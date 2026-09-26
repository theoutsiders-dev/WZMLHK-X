from asyncio import sleep
from logging import getLogger
from os import path as ospath, walk
from re import match as re_match, sub as re_sub
from time import time

from aioshutil import rmtree
from natsort import natsorted
from PIL import Image
from html import unescape as html_unescape
from pyrogram import enums
from pyrogram.errors import BadRequest, FloodWait, RPCError

try:
    from pyrogram.errors import FloodPremiumWait
except ImportError:
    FloodPremiumWait = FloodWait
from aiofiles.os import (
    path as aiopath,
    remove,
    rename,
)
from pyrogram.types import (
    InputMediaDocument,
    InputMediaPhoto,
    InputMediaVideo,
)
from tenacity import (
    RetryError,
    retry,
    retry_if_exception,
    stop_after_attempt,
    wait_exponential,
)

from ....core.config_manager import Config
from ....core.tg_client import TgClient
from ...ext_utils.bot_utils import sync_to_async
from ...ext_utils.files_utils import get_base_name, is_archive
from ...ext_utils.status_utils import get_readable_file_size, get_readable_time
from ...telegram_helper.message_utils import send_message
from ...ext_utils.media_utils import (
    get_audio_thumbnail,
    get_document_type,
    get_media_info,
    get_multiple_frames_thumbnail,
    get_video_thumbnail,
    get_md5_hash,
    is_video_split,
)
from ...ext_utils.tmdb_utils import get_poster_thumb
from ...telegram_helper.message_utils import delete_message

LOGGER = getLogger(__name__)


def _should_retry(error):
    if isinstance(error, BadRequest):
        if "ENTITY_BOUNDS_INVALID" in str(error):
            return False
    return True


class TelegramUploader:
    def __init__(self, listener, path):
        self._last_uploaded = 0
        self._processed_bytes = 0
        self._listener = listener
        self._path = path
        self._client = None
        self._start_time = time()
        self._total_files = 0
        self._thumb = self._listener.thumb or f"thumbnails/{listener.user_id}.jpg"
        self._msgs_dict = {}
        self._corrupted = 0
        self._is_corrupted = False
        self._media_dict = {"videos": {}, "documents": {}}
        self._last_msg_in_group = False
        self._up_path = ""
        self._lprefix = ""
        self._lsuffix = ""
        self._lcaption = ""
        self._lfont = ""
        self._bot_pm = False
        self._media_group = False
        self._is_private = False
        self._sent_msg = None
        self._log_msg = None
        self._user_session = self._listener.user_transmission
        self._error = ""

    @staticmethod
    def _is_entity_bounds_error(error):
        return "ENTITY_BOUNDS_INVALID" in str(error)

    @staticmethod
    def _plain_caption(caption):
        if not caption:
            return ""
        text = re_sub(r"<[^>]+>", "", caption)
        return html_unescape(text)

    async def _send_with_caption_fallback(self, sender, **kwargs):
        try:
            kwargs["parse_mode"] = enums.ParseMode.HTML
            return await sender(**kwargs)
        except BadRequest as err:
            if not self._is_entity_bounds_error(err):
                raise

            LOGGER.warning(
                "ENTITY_BOUNDS_INVALID for %s; retrying with plain-text caption",
                self._up_path,
            )

            kwargs["caption"] = self._plain_caption(kwargs.get("caption", ""))
            kwargs["parse_mode"] = enums.ParseMode.DISABLED

            return await sender(**kwargs)

    async def _upload_progress(self, current, _):
        if self._listener.is_cancelled:
            if self._user_session:
                TgClient.user.stop_transmission()
            else:
                self._listener.client.stop_transmission()
        chunk_size = current - self._last_uploaded
        self._last_uploaded = current
        self._processed_bytes += chunk_size

    async def _user_settings(self):
        settings_map = {
            "MEDIA_GROUP": ("_media_group", False),
            "BOT_PM": ("_bot_pm", False),
            "LEECH_PREFIX": ("_lprefix", ""),
            "LEECH_SUFFIX": ("_lsuffix", ""),
            "LEECH_CAPTION": ("_lcaption", ""),
            "LEECH_FONT": ("_lfont", ""),
        }

        self._smart_autorename = getattr(self._listener, "smart_autorename", False)
        self._auto_thumbnail = getattr(self._listener, "auto_thumbnail", True)

        for key, (attr, default) in settings_map.items():
            setattr(
                self,
                attr,
                self._listener.user_dict.get(key) or getattr(Config, key, default),
            )

        if self._thumb != "none" and not await aiopath.exists(self._thumb):
            self._thumb = None

    async def _msg_to_reply(self):
        if self._listener.up_dest:
            msg_link = (
                self._listener.message.link if self._listener.is_super_chat else ""
            )
            msg = f"""➲ <b><u>Leech Started :</u></b>
┃
┠ <b>User :</b> {self._listener.user.mention} ( #ID{self._listener.user_id} ){f"\n┠ <b>Message Link :</b> <a href='{msg_link}'>Click Here</a>" if msg_link else ""}
┖ <b>Source :</b> <a href='{self._listener.source_url}'>Click Here</a>"""
            try:
                await TgClient.bot.resolve_peer(self._listener.up_dest)
                self._log_msg = await TgClient.bot.send_message(
                    chat_id=self._listener.up_dest,
                    text=msg,
                    disable_web_page_preview=True,
                    message_thread_id=self._listener.chat_thread_id,
                    disable_notification=True,
                )
                self._sent_msg = self._log_msg
                if self._user_session:
                    self._sent_msg = await TgClient.user.get_messages(
                        chat_id=self._sent_msg.chat.id,
                        message_ids=self._sent_msg.id,
                    )
                else:
                    self._is_private = self._sent_msg.chat.type.name == "PRIVATE"
                if self._listener.leech_dest:
                    try:
                        leech_dest = self._listener.leech_dest
                        if not isinstance(leech_dest, int):
                            if "|" in str(leech_dest):
                                leech_dest, _ = str(leech_dest).split("|", 1)
                            if leech_dest.lstrip("-").isdigit():
                                leech_dest = int(leech_dest)
                        await self._log_msg.copy(chat_id=leech_dest)
                    except Exception as e:
                        if not self._listener.is_cancelled:
                            LOGGER.error(
                                f"Failed to copy 'Leech Started' message to {self._listener.leech_dest}: {e}"
                            )
                            await send_message(
                                self._listener.user_id,
                                f"Failed to send 'Leech Started' message to {self._listener.leech_dest}\n{e}",
                            )
            except Exception as e:
                await self._listener.on_upload_error(str(e))
                return False

        elif self._user_session:
            self._sent_msg = await TgClient.user.get_messages(
                chat_id=self._listener.message.chat.id, message_ids=self._listener.mid
            )
            if self._sent_msg is None:
                self._sent_msg = await TgClient.user.send_message(
                    chat_id=self._listener.message.chat.id,
                    text="Deleted Cmd Message! Don't delete the cmd message again!",
                    disable_web_page_preview=True,
                    disable_notification=True,
                )
        else:
            self._sent_msg = self._listener.message
        return True

    async def _prepare_file(self, pre_file_, dirpath):
        orig_filenames = self._listener.file_details.get("orig_filenames", {})
        orig_filename = orig_filenames.get(
            pre_file_
        ) or self._listener.file_details.get("orig_filename")
        cap_file_ = (
            orig_filename if (orig_filename and self._smart_autorename) else pre_file_
        )
        file_ = pre_file_

        if self._lprefix:
            cap_file_ = self._lprefix.replace(r"\s", " ") + file_
            self._lprefix = re_sub(r"<.*?>", "", self._lprefix).replace(r"\s", " ")
            if not file_.startswith(self._lprefix):
                file_ = f"{self._lprefix}{file_}"

        if self._lsuffix:
            split_match = re_match(
                r"(?i)(?P<stem>.*?)(?P<ext>\.(?:mkv|mp4|webm|avi|flv|mov|m4v|3gp|ts|m2ts|wmv|asf))(?P<split>\.0*\d+)$",
                cap_file_,
            )
            if split_match:
                cap_file_ = f"{split_match.group('stem')}{self._lsuffix.replace(r'\\s', ' ')}{split_match.group('ext')}{split_match.group('split')}"
            else:
                name, ext = ospath.splitext(cap_file_)
                cap_file_ = name + self._lsuffix.replace(r"\s", " ") + ext
            self._lsuffix = re_sub(r"<.*?>", "", self._lsuffix).replace(r"\s", " ")

        cap_mono = (
            f"<{Config.LEECH_FONT}>{cap_file_}</{Config.LEECH_FONT}>"
            if Config.LEECH_FONT
            else cap_file_
        )
        if self._smart_autorename:
            cap_mono = f"<blockquote>{cap_mono}</blockquote>"
        if self._lcaption:
            self._lcaption = re_sub(
                r"(\\\||\\\{|\\\}|\\s)",
                lambda m: {r"\|": "%%", r"\{": "&%&", r"\}": "$%$", r"\s": " "}[
                    m.group(0)
                ],
                self._lcaption,
            )

            parts = self._lcaption.split("|")
            parts[0] = re_sub(
                r"\{([^}]+)\}", lambda m: f"{{{m.group(1).lower()}}}", parts[0]
            )
            up_path = ospath.join(dirpath, pre_file_)

            # Use original metadata if preserved during split
            base_name_match = re_match(
                r"^(.+?)(?:\.[0-9]+|\.part[0-9]+\..+)$", pre_file_
            )
            orig_name = base_name_match.group(1) if base_name_match else pre_file_

            if orig_name in self._listener.file_details.get("media_info", {}):
                dur, qual, lang, subs = self._listener.file_details["media_info"][
                    orig_name
                ]
            else:
                dur, qual, lang, subs = await get_media_info(up_path, True)
            display_orig = (
                orig_filename
                or self._listener.file_details.get("orig_filename")
                or pre_file_
            )
            display_filename = file_ if self._smart_autorename else cap_file_
            smart_meta = (
                self._listener.file_details.get("smart_metadata", {}).get(pre_file_)
                or self._listener.file_details.get("smart_metadata", {}).get(orig_name)
                or {}
            )
            if not smart_meta and (self._smart_autorename or orig_filename):
                try:
                    from ...ext_utils.smart_autorename import parse_smart_filename

                    ctx = parse_smart_filename(display_orig)
                    smart_meta = {
                        "show_name": ctx.title or "",
                        "season": f"{ctx.season:02d}" if ctx.season is not None else "",
                        "episode": f"{ctx.episode_start:02d}"
                        if ctx.episode_start is not None
                        else "",
                        "title": "",
                        "year": str(ctx.year or ""),
                        "source": ctx.ott or "",
                        "codec": ctx.filename_codec or "",
                    }
                except Exception:
                    smart_meta = {}

            def _html_safe(value):
                from html import escape

                return escape(str(value or ""))

            cap_mono = parts[0].format(
                filename=_html_safe(display_filename),
                orig_filename=_html_safe(display_orig),
                smart_filename=_html_safe(pre_file_),
                size=_html_safe(get_readable_file_size(await aiopath.getsize(up_path))),
                duration=_html_safe(get_readable_time(dur)),
                quality=_html_safe(qual),
                languages=_html_safe(lang),
                subtitles=_html_safe(subs),
                md5_hash=_html_safe(await sync_to_async(get_md5_hash, up_path)),
                mime_type=_html_safe(
                    self._listener.file_details.get("mime_type", "text/plain")
                ),
                prefilename=_html_safe(display_orig),
                precaption=self._listener.file_details.get("caption", ""),
                show_name=_html_safe(smart_meta.get("show_name", "")),
                season=_html_safe(smart_meta.get("season", "")),
                episode=_html_safe(smart_meta.get("episode", "")),
                title=_html_safe(smart_meta.get("title", "")),
                year=_html_safe(smart_meta.get("year", "")),
                source=_html_safe(smart_meta.get("source", "")),
                codec=_html_safe(smart_meta.get("codec", "")),
            )

            for part in parts[1:]:
                if not part:
                    continue
                args = part.split(":")
                if len(args) > 2 and args[-1].isdigit():
                    count = int(args[-1])
                    search_str = ":".join(args[:-2])
                    replace_str = args[-2]
                elif len(args) >= 2:
                    count = -1
                    search_str = ":".join(args[:-1])
                    replace_str = args[-1]
                else:
                    count = -1
                    search_str = args[0]
                    replace_str = ""
                cap_mono = cap_mono.replace(search_str, replace_str, count)
            cap_mono = re_sub(
                r"%%|&%&|\$%\$",
                lambda m: {"%%": "|", "&%&": "{", "$%$": "}"}[m.group()],
                cap_mono,
            )

        if len(file_) > 255:
            if is_archive(file_):
                name = get_base_name(file_)
                ext = file_.split(name, 1)[1]
            elif match := re_match(r".+(?=\..+\.0*\d+$)|.+(?=\.part\d+\..+$)", file_):
                name = match.group(0)
                ext = file_.split(name, 1)[1]
            elif len(fsplit := ospath.splitext(file_)) > 1:
                name = fsplit[0]
                ext = fsplit[1]
            else:
                name = file_
                ext = ""
            if self._lsuffix:
                ext = f"{self._lsuffix}{ext}"
            name = name[: 255 - len(ext)]
            file_ = f"{name}{ext}"
        elif self._lsuffix:
            name, ext = ospath.splitext(file_)
            file_ = f"{name}{self._lsuffix}{ext}"

        if pre_file_ != file_:
            new_path = ospath.join(dirpath, file_)
            await rename(self._up_path, new_path)
            self._up_path = new_path

        return cap_mono

    def _get_input_media(self, subkey, key):
        rlist = []
        for msg in self._media_dict[key][subkey]:
            if key == "videos":
                input_media = InputMediaVideo(
                    media=msg.video.file_id, caption=msg.caption
                )
            else:
                input_media = InputMediaDocument(
                    media=msg.document.file_id, caption=msg.caption
                )
            rlist.append(input_media)
        return rlist

    async def _send_screenshots(self, dirpath, outputs):
        inputs = [
            InputMediaPhoto(ospath.join(dirpath, p), p.rsplit("/", 1)[-1])
            for p in outputs
        ]
        for i in range(0, len(inputs), 10):
            batch = inputs[i : i + 10]
            if Config.BOT_PM:
                await TgClient.bot.send_media_group(
                    chat_id=self._listener.user_id,
                    media=batch,
                    disable_notification=True,
                )
            self._sent_msg = (
                await self._sent_msg.reply_media_group(
                    media=batch,
                    quote=True,
                    disable_notification=True,
                )
            )[-1]

    async def _send_media_group(self, subkey, key, msgs):
        for index, msg in enumerate(msgs):
            if self._listener.hybrid_leech or not self._user_session:
                msgs[index] = await self._listener.client.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
            else:
                msgs[index] = await TgClient.user.get_messages(
                    chat_id=msg[0], message_ids=msg[1]
                )
        msgs_list = await msgs[0].reply_to_message.reply_media_group(
            media=self._get_input_media(subkey, key),
            quote=True,
            disable_notification=True,
        )
        for msg in msgs:
            if msg.link in self._msgs_dict:
                del self._msgs_dict[msg.link]
            await delete_message(msg)
        del self._media_dict[key][subkey]
        if self._listener.is_super_chat or self._listener.up_dest:
            for m in msgs_list:
                self._msgs_dict[m.link] = m.caption
        self._sent_msg = msgs_list[-1]

    async def _copy_media(self):
        try:
            if self._bot_pm:
                await TgClient.bot.copy_message(
                    chat_id=self._listener.user_id,
                    from_chat_id=self._sent_msg.chat.id,
                    message_id=self._sent_msg.id,
                    reply_to_message_id=(
                        self._listener.pm_msg.id if self._listener.pm_msg else None
                    ),
                )
        except Exception as err:
            if not self._listener.is_cancelled:
                LOGGER.error(f"Failed To Send in BotPM:\n{str(err)}")

    async def upload(self):
        await self._user_settings()
        res = await self._msg_to_reply()
        if not res:
            return
        is_log_del = False
        for dirpath, _, files in natsorted(await sync_to_async(walk, self._path)):
            if dirpath.strip().endswith("/yt-dlp-thumb"):
                continue
            if dirpath.strip().endswith("_mltbss"):
                await self._send_screenshots(dirpath, files)
                await rmtree(dirpath, ignore_errors=True)
                continue
            for file_ in natsorted(files):
                self._error = ""
                self._up_path = f_path = ospath.join(dirpath, file_)
                if not await aiopath.exists(self._up_path):
                    LOGGER.error(f"{self._up_path} not exists! Continue uploading!")
                    continue
                try:
                    f_size = await aiopath.getsize(self._up_path)
                    self._total_files += 1
                    if f_size == 0:
                        LOGGER.error(
                            f"{self._up_path} size is zero, telegram don't upload zero size files"
                        )
                        self._corrupted += 1
                        continue
                    if self._listener.is_cancelled:
                        return
                    cap_mono = await self._prepare_file(file_, dirpath)
                    if self._last_msg_in_group:
                        group_lists = [
                            x for v in self._media_dict.values() for x in v.keys()
                        ]
                        match = re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", f_path)
                        if not match or match and match.group(0) not in group_lists:
                            for key, value in list(self._media_dict.items()):
                                for subkey, msgs in list(value.items()):
                                    if len(msgs) > 1:
                                        await self._send_media_group(subkey, key, msgs)
                    if self._listener.hybrid_leech and self._listener.user_transmission:
                        self._user_session = f_size > 2097152000
                        if self._user_session:
                            self._sent_msg = await TgClient.user.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                        else:
                            self._sent_msg = await self._listener.client.get_messages(
                                chat_id=self._sent_msg.chat.id,
                                message_ids=self._sent_msg.id,
                            )
                    self._last_msg_in_group = False
                    self._last_uploaded = 0
                    uploaded = False
                    uploaded = await self._upload_file(cap_mono, file_, f_path)
                    if self._log_msg and not is_log_del and Config.CLEAN_LOG_MSG:
                        await delete_message(self._log_msg)
                        is_log_del = True
                    if self._listener.is_cancelled:
                        return
                    if (
                        not self._is_corrupted
                        and (self._listener.is_super_chat or self._listener.up_dest)
                        and not self._is_private
                    ):
                        self._msgs_dict[self._sent_msg.link] = file_
                    await sleep(1)
                except Exception as err:
                    if isinstance(err, RetryError):
                        LOGGER.info(
                            f"Total Attempts: {err.last_attempt.attempt_number}"
                        )
                        err = err.last_attempt.exception()
                    LOGGER.error(f"{err}. Path: {self._up_path}", exc_info=True)
                    self._error = str(err)
                    self._corrupted += 1
                    if self._listener.is_cancelled:
                        return
                if (
                    uploaded
                    and not self._listener.is_cancelled
                    and await aiopath.exists(self._up_path)
                ):
                    await remove(self._up_path)
        for key, value in list(self._media_dict.items()):
            for subkey, msgs in list(value.items()):
                if len(msgs) > 1:
                    try:
                        await self._send_media_group(subkey, key, msgs)
                    except Exception as e:
                        LOGGER.info(
                            f"While sending media group at the end of task. Error: {e}"
                        )
        if self._listener.is_cancelled:
            return
        if self._total_files == 0:
            await self._listener.on_upload_error(
                "No files to upload. In case you have filled EXCLUDED_EXTENSIONS, then check if all files have those extensions or not."
            )
            return
        if self._total_files <= self._corrupted:
            await self._listener.on_upload_error(
                f"Files Corrupted or unable to upload. {self._error or 'Check logs!'}"
            )
            return
        LOGGER.info(f"Leech Completed: {self._listener.name}")
        await self._listener.on_upload_complete(
            None, self._msgs_dict, self._total_files, self._corrupted
        )
        return

    @retry(
        wait=wait_exponential(multiplier=2, min=4, max=8),
        stop=stop_after_attempt(3),
        retry=retry_if_exception(_should_retry),
    )
    async def _upload_file(self, cap_mono, file, o_path, force_document=False):
        if self._sent_msg is None:
            LOGGER.error("Cannot upload: _sent_msg is None")
            await self._listener.on_upload_error(
                "Upload failed: Message not initialized"
            )
            return

        if not hasattr(self._sent_msg, "chat") or self._sent_msg.chat is None:
            LOGGER.error("Cannot upload: _sent_msg.chat is None")
            await self._listener.on_upload_error(
                "Upload failed: Invalid message object"
            )
            return

        if (
            self._thumb is not None
            and not await aiopath.exists(self._thumb)
            and self._thumb != "none"
        ):
            self._thumb = None
        thumb = self._thumb
        self._is_corrupted = False
        try:
            is_vsplit = is_video_split(self._up_path)
            base_name_match = re_match(r"^(.+?)(?:\.[0-9]+|\.part[0-9]+\..+)$", file)
            orig_name = base_name_match.group(1) if base_name_match else file

            if orig_name in self._listener.file_details.get("document_type", {}):
                is_video, is_audio, is_image = self._listener.file_details[
                    "document_type"
                ][orig_name]
            else:
                is_video, is_audio, is_image = await get_document_type(self._up_path)

            if not is_image and thumb is None:
                file_name = ospath.splitext(file)[0]
                thumb_path = f"{self._path}/yt-dlp-thumb/{file_name}.jpg"
                if await aiopath.isfile(thumb_path):
                    thumb = thumb_path
                elif await aiopath.isfile(thumb_path.replace("/yt-dlp-thumb", "")):
                    thumb = thumb_path.replace("/yt-dlp-thumb", "")
                elif is_audio and not is_video:
                    thumb = await get_audio_thumbnail(self._up_path)
                elif (is_video or is_vsplit) and self._auto_thumbnail:
                    auto_thumb = await get_poster_thumb(
                        file, getattr(self._listener, "as_doc", False) or is_vsplit
                    )
                    if auto_thumb and await aiopath.isfile(auto_thumb):
                        thumb = auto_thumb

            LOGGER.debug(
                "Telegram upload caption: path=%s caption_length=%d",
                self._up_path,
                len(cap_mono or ""),
            )

            if (
                self._listener.as_doc
                or force_document
                or (not is_video and not is_audio and not is_image)
            ):
                key = "documents"
                if is_video and not is_vsplit and thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, None)

                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._send_with_caption_fallback(
                    self._sent_msg.reply_document,
                    document=self._up_path,
                    quote=True,
                    thumb=thumb,
                    caption=cap_mono,
                    disable_content_type_detection=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_video:
                key = "videos"
                duration = (await get_media_info(self._up_path))[0]
                if thumb is None and self._listener.thumbnail_layout:
                    thumb = await get_multiple_frames_thumbnail(
                        self._up_path,
                        self._listener.thumbnail_layout,
                        self._listener.screen_shots,
                    )
                if thumb is None:
                    thumb = await get_video_thumbnail(self._up_path, duration)
                if thumb is not None and thumb != "none":
                    with Image.open(thumb) as img:
                        width, height = img.size
                else:
                    width = 480
                    height = 320
                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._send_with_caption_fallback(
                    self._sent_msg.reply_video,
                    video=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    width=width,
                    height=height,
                    thumb=thumb,
                    supports_streaming=True,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            elif is_audio:
                key = "audios"
                duration, artist, title = await get_media_info(self._up_path)
                if self._listener.is_cancelled:
                    return
                if thumb == "none":
                    thumb = None
                self._sent_msg = await self._send_with_caption_fallback(
                    self._sent_msg.reply_audio,
                    audio=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    duration=duration,
                    performer=artist,
                    title=title,
                    thumb=thumb,
                    disable_notification=True,
                    progress=self._upload_progress,
                )
            else:
                key = "photos"
                if self._listener.is_cancelled:
                    return
                self._sent_msg = await self._send_with_caption_fallback(
                    self._sent_msg.reply_photo,
                    photo=self._up_path,
                    quote=True,
                    caption=cap_mono,
                    disable_notification=True,
                    progress=self._upload_progress,
                )

            if (
                not self._listener.is_cancelled
                and self._media_group
                and (self._sent_msg.video or self._sent_msg.document)
            ):
                key = "documents" if self._sent_msg.document else "videos"
                if match := re_match(r".+(?=\.0*\d+$)|.+(?=\.part\d+\..+$)", o_path):
                    pname = match.group(0)
                    if pname in self._media_dict[key].keys():
                        self._media_dict[key][pname].append(
                            [self._sent_msg.chat.id, self._sent_msg.id]
                        )
                    else:
                        self._media_dict[key][pname] = [
                            [self._sent_msg.chat.id, self._sent_msg.id]
                        ]
                    msgs = self._media_dict[key][pname]
                    if len(msgs) == 10:
                        await self._send_media_group(pname, key, msgs)
                    else:
                        self._last_msg_in_group = True

            if self._sent_msg:
                await self._copy_media()
                if self._listener.leech_dest:
                    try:
                        leech_dest = self._listener.leech_dest
                        if not isinstance(leech_dest, int):
                            if "|" in str(leech_dest):
                                leech_dest, _ = str(leech_dest).split("|", 1)
                            if leech_dest.lstrip("-").isdigit():
                                leech_dest = int(leech_dest)
                        await TgClient.bot.copy_message(
                            chat_id=leech_dest,
                            from_chat_id=self._sent_msg.chat.id,
                            message_id=self._sent_msg.id,
                        )
                    except Exception as e:
                        if not self._listener.is_cancelled:
                            LOGGER.error(
                                f"Failed to forward to {self._listener.leech_dest}: {e}"
                            )
                            await send_message(
                                self._listener.user_id,
                                f"Failed to forward to {self._listener.leech_dest}\n{e}",
                            )

            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return True
        except (FloodWait, FloodPremiumWait) as f:
            LOGGER.warning(str(f))
            await sleep(f.value * 1.3)
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            return await self._upload_file(cap_mono, file, o_path)
        except Exception as err:
            if (
                self._thumb is None
                and thumb is not None
                and await aiopath.exists(thumb)
            ):
                await remove(thumb)
            err_type = "RPCError: " if isinstance(err, RPCError) else ""
            LOGGER.error(f"{err_type}{err}. Path: {self._up_path}", exc_info=True)
            if isinstance(err, BadRequest) and key != "documents":
                LOGGER.error(f"Retrying As Document. Path: {self._up_path}")
                return await self._upload_file(cap_mono, file, o_path, True)
            raise err

    @property
    def speed(self):
        try:
            return self._processed_bytes / (time() - self._start_time)
        except ZeroDivisionError:
            return 0

    @property
    def processed_bytes(self):
        return self._processed_bytes

    async def cancel_task(self):
        self._listener.is_cancelled = True
        LOGGER.info(f"Cancelling Upload: {self._listener.name}")
        await self._listener.on_upload_error("your upload has been stopped!")
