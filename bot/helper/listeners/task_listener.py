from asyncio import gather, sleep
from html import escape
from time import time
from mimetypes import guess_type
from contextlib import suppress
from os import path as ospath, walk
from pyrogram.enums import ButtonStyle

from aiofiles.os import listdir, remove, path as aiopath
from requests import utils as rutils

from ... import (
    intervals,
    task_dict,
    task_dict_lock,
    LOGGER,
    non_queued_up,
    non_queued_dl,
    queued_up,
    queued_dl,
    queue_dict_lock,
    same_directory_lock,
    DOWNLOAD_DIR,
)
from ...modules.metadata import apply_metadata_title
from ..common import TaskConfig
from ...core.tg_client import TgClient
from ...core.config_manager import Config
from ...core.torrent_manager import TorrentManager
from ..ext_utils.bot_utils import sync_to_async
from ..ext_utils.links_utils import encode_slink
from ..ext_utils.db_handler import database
from ..ext_utils.files_utils import (
    clean_download,
    clean_target,
    create_recursive_symlink,
    get_path_size,
    join_files,
    remove_excluded_files,
    move_and_merge,
)
from ..ext_utils.links_utils import is_gdrive_id
from ..ext_utils.status_utils import get_readable_file_size, get_readable_time
from ..ext_utils.task_manager import (
    check_running_tasks,
    check_blacklisted_keywords,
    start_from_queued,
)
from ..mirror_leech_utils.uphoster_utils.multi_upload import MultiUphosterUpload
from ..mirror_leech_utils.gdrive_utils.upload import GoogleDriveUpload
from ..mirror_leech_utils.rclone_utils.transfer import RcloneTransferHelper
from ..mirror_leech_utils.status_utils.uphoster_status import UphosterStatus
from ..mirror_leech_utils.status_utils.gdrive_status import (
    GoogleDriveStatus,
)
from ..mirror_leech_utils.status_utils.queue_status import QueueStatus
from ..mirror_leech_utils.status_utils.rclone_status import RcloneStatus
from ..mirror_leech_utils.status_utils.telegram_status import TelegramStatus
from ..mirror_leech_utils.status_utils.yt_status import YtStatus
from ..mirror_leech_utils.upload_utils.telegram_uploader import TelegramUploader
from ..mirror_leech_utils.youtube_utils.youtube_upload import YouTubeUpload
from ..telegram_helper.button_build import ButtonMaker
from ..telegram_helper.message_utils import (
    delete_message,
    delete_status,
    send_message,
    update_status_message,
)


class TaskListener(TaskConfig):
    def __init__(self):
        super().__init__()
        self.avg_download_speed = 0
        self.avg_upload_speed = 0

    async def clean(self):
        with suppress(Exception):
            if st := intervals["status"]:
                for intvl in list(st.values()):
                    intvl.cancel()
            intervals["status"].clear()
            await gather(TorrentManager.aria2.purgeDownloadResult(), delete_status())

    def clear(self):
        self.subname = ""
        self.subsize = 0
        self.files_to_proceed = []
        self.proceed_count = 0
        self.progress = True

    async def remove_from_same_dir(self):
        async with task_dict_lock:
            if (
                self.folder_name
                and self.same_dir
                and self.mid in self.same_dir[self.folder_name]["tasks"]
            ):
                self.same_dir[self.folder_name]["tasks"].remove(self.mid)
                self.same_dir[self.folder_name]["total"] -= 1

    async def on_download_start(self):
        self.download_start_time = time()
        mode_name = "Leech" if self.is_leech else "Mirror"
        if self.bot_pm and self.is_super_chat:
            self.pm_msg = await send_message(
                self.user_id,
                f"""➲ <b><u>Task Started :</u></b>
┃
┖ <b>Link:</b> <a href='{self.source_url}'>Click Here</a>
""",
            )
        if Config.LINKS_LOG_ID:
            await send_message(
                Config.LINKS_LOG_ID,
                f"""➲  <b><u>{mode_name} Started:</u></b>
 ┃
 ┠ <b>User :</b> {self.tag} ( #ID{self.user_id} )
 ┠ <b>Message Link :</b> <a href='{self.message.link}'>Click Here</a>
 ┗ <b>Link:</b> <a href='{self.source_url}'>Click Here</a>
 """,
            )
        if (
            self.is_super_chat
            and (Config.INCOMPLETE_TASK_NOTIFIER or Config.INCOMPLETE_TASK_RESUME)
            and Config.DATABASE_URL
        ):
            await database.add_incomplete_task(
                self.message.chat.id, self.message.link, self.tag, self.message.id
            )

    async def on_download_complete(self):
        await sleep(2)
        if self.is_cancelled:
            return
        multi_links = False
        if (
            self.folder_name
            and self.same_dir
            and self.mid in self.same_dir[self.folder_name]["tasks"]
        ):
            async with same_directory_lock:
                while True:
                    async with task_dict_lock:
                        if self.mid not in self.same_dir[self.folder_name]["tasks"]:
                            return
                        if (
                            self.same_dir[self.folder_name]["total"] <= 1
                            or len(self.same_dir[self.folder_name]["tasks"]) > 1
                        ):
                            if self.same_dir[self.folder_name]["total"] > 1:
                                self.same_dir[self.folder_name]["tasks"].remove(
                                    self.mid
                                )
                                self.same_dir[self.folder_name]["total"] -= 1
                                spath = f"{self.dir}{self.folder_name}"
                                des_id = list(self.same_dir[self.folder_name]["tasks"])[
                                    0
                                ]
                                des_path = f"{DOWNLOAD_DIR}{des_id}{self.folder_name}"
                                LOGGER.info(f"Moving files from {self.mid} to {des_id}")
                                await move_and_merge(spath, des_path, self.mid)
                                multi_links = True
                            break
                    await sleep(1)
        async with task_dict_lock:
            if self.is_cancelled:
                return
            if self.mid not in task_dict:
                return
            download = task_dict[self.mid]
            self.name = download.name()
            gid = download.gid()
        LOGGER.info(f"Download completed: {self.name}")

        if not (self.is_torrent or self.is_qbit):
            self.seed = False

        if multi_links:
            self.seed = False
            await self.on_upload_error(
                f"{self.name} Downloaded!\n\nWaiting for other tasks to finish..."
            )
            return
        elif self.same_dir:
            self.seed = False

        if self.folder_name:
            self.name = self.folder_name.strip("/").split("/", 1)[0]

        if not await aiopath.exists(f"{self.dir}/{self.name}"):
            try:
                files = await listdir(self.dir)
                self.name = files[-1]
                if self.name == "yt-dlp-thumb":
                    self.name = files[0]
            except Exception as e:
                await self.on_upload_error(str(e))
                return

        dl_path = f"{self.dir}/{self.name}"
        self.size = await get_path_size(dl_path)
        self.download_size = self.size
        self.is_file = await aiopath.isfile(dl_path)

        is_bl, bl_kw = await check_blacklisted_keywords(self, self.name)
        if is_bl:
            await self.on_download_error(
                f"Task cancelled! Name contains blacklisted keyword: <code>{bl_kw}</code>"
            )
            return

        download_time = time() - getattr(self, "download_start_time", time())
        self.avg_download_speed = (
            self.download_size / download_time if download_time > 0 else 0
        )

        if self.seed:
            up_dir = self.up_dir = f"{self.dir}10000"
            up_path = f"{self.up_dir}/{self.name}"
            await create_recursive_symlink(self.dir, self.up_dir)
            LOGGER.info(f"Shortcut created: {dl_path} -> {up_path}")
        else:
            up_dir = self.dir
            up_path = dl_path

        await remove_excluded_files(self.up_dir or self.dir, self.excluded_extensions)

        if not Config.QUEUE_ALL:
            async with queue_dict_lock:
                if self.mid in non_queued_dl:
                    non_queued_dl.remove(self.mid)
            await start_from_queued()

        if self.join and not self.is_file:
            await join_files(up_path)

        if self.extract and not self.is_nzb:
            up_path = await self.proceed_extract(up_path, gid)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()
            await remove_excluded_files(up_dir, self.excluded_extensions)

        if self.ffmpeg_cmds:
            up_path = await self.proceed_ffmpeg(
                up_path,
                gid,
            )
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if (
            (hasattr(self, "metadata_dict") and self.metadata_dict)
            or (hasattr(self, "audio_metadata_dict") and self.audio_metadata_dict)
            or (hasattr(self, "video_metadata_dict") and self.video_metadata_dict)
        ):
            up_path = await apply_metadata_title(
                self,
                up_path,
                gid,
                getattr(self, "metadata_dict", {}),
                getattr(self, "audio_metadata_dict", {}),
                getattr(self, "video_metadata_dict", {}),
            )
            if self.is_cancelled:
                return

            self.name = up_path.replace(f"{up_dir.rstrip('/')}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_path)
            self.clear()

        if self.is_leech and self.is_file:
            fname = ospath.basename(up_path)
            self.file_details["filename"] = fname
            self.file_details["mime_type"] = (guess_type(fname))[
                0
            ] or "application/octet-stream"

        if self.name_swap:
            up_path = await self.substitute(up_path)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]

        if self.screen_shots:
            up_path = await self.generate_screenshots(up_path)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)

        if self.convert_audio or self.convert_video:
            up_path = await self.convert_media(
                up_path,
                gid,
            )
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if self.sample_video:
            up_path = await self.generate_sample_video(up_path, gid)
            if self.is_cancelled:
                return
            self.is_file = await aiopath.isfile(up_path)
            self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
            self.size = await get_path_size(up_dir)
            self.clear()

        if self.compress:
            up_path = await self.proceed_compress(
                up_path,
                gid,
            )
            self.is_file = await aiopath.isfile(up_path)
            if self.is_cancelled:
                return
            self.clear()

        self.name = up_path.replace(f"{up_dir}/", "").split("/", 1)[0]
        self.size = await get_path_size(up_dir)

        if (
            self.is_leech
            and self.smart_autorename
            and not self.compress
            and not self.custom_name
            and not self.name_swap
        ):
            try:
                from ..ext_utils.smart_autorename import (
                    SmartAutoRename,
                    SmartRenameLengthError,
                    SmartRenameMetadataError,
                )

                smart_renamer = SmartAutoRename()
                prefix = self.user_dict.get("LEECH_PREFIX") or Config.LEECH_PREFIX or ""
                suffix = self.user_dict.get("LEECH_SUFFIX") or Config.LEECH_SUFFIX or ""

                if await aiopath.isfile(up_path):
                    orig_name = ospath.basename(up_path)
                    up_path, smart_meta = await smart_renamer.rename_get_meta(
                        up_path,
                        prefix=prefix,
                        suffix=suffix,
                    )
                    if ospath.basename(up_path) != orig_name:
                        self.file_details["orig_filename"] = orig_name
                        self.file_details.setdefault("orig_filenames", {})[
                            ospath.basename(up_path)
                        ] = orig_name
                    if smart_meta:
                        self.file_details.setdefault("smart_metadata", {})[
                            ospath.basename(up_path)
                        ] = smart_meta
                    self.name = ospath.basename(up_path)
                elif await aiopath.isdir(up_path):
                    orig_dict = self.file_details.setdefault("orig_filenames", {})
                    meta_dict = self.file_details.setdefault("smart_metadata", {})
                    for root, _, files in await sync_to_async(list, walk(up_path)):
                        for file_ in files:
                            file_path = ospath.join(root, file_)
                            orig_file_name = file_
                            try:
                                (
                                    new_file_path,
                                    smart_meta,
                                ) = await smart_renamer.rename_get_meta(
                                    file_path,
                                    prefix=prefix,
                                    suffix=suffix,
                                )
                                new_file_name = ospath.basename(new_file_path)
                                if new_file_name != orig_file_name:
                                    orig_dict[new_file_name] = orig_file_name
                                if smart_meta:
                                    meta_dict[new_file_name] = smart_meta
                            except Exception as exc:
                                LOGGER.warning(
                                    f"Smart Autorename skipped for {file_}: {exc}"
                                )

                self.size = await get_path_size(up_path)
            except SmartRenameLengthError as exc:
                await send_message(self.message, str(exc))
                return await self.on_upload_error(str(exc))
            except SmartRenameMetadataError as exc:
                LOGGER.warning(f"Smart Autorename skipped: {exc}")
            except Exception as exc:
                LOGGER.error(f"Smart Autorename failed: {exc}", exc_info=True)

        if self.is_leech and not self.compress:
            await self.proceed_split(up_path, gid)
            if self.is_cancelled:
                return
            self.clear()

        self.subproc = None

        add_to_queue, event = await check_running_tasks(self, "up")
        await start_from_queued()
        if add_to_queue:
            LOGGER.info(f"Added to Queue/Upload: {self.name}")
            async with task_dict_lock:
                task_dict[self.mid] = QueueStatus(self, gid, "Up")
            await event.wait()
            if self.is_cancelled:
                return
            LOGGER.info(f"Start from Queued/Upload: {self.name}")

        self.upload_start_time = time()

        self.size = await get_path_size(up_dir)

        if self.is_yt:
            LOGGER.info(f"Up to yt Name: {self.name}")
            yt = YouTubeUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = YtStatus(self, yt, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                sync_to_async(yt.upload),
            )
            del yt
        elif self.is_leech:
            LOGGER.info(f"Leech Name: {self.name}")
            tg = TelegramUploader(self, up_dir)
            async with task_dict_lock:
                task_dict[self.mid] = TelegramStatus(self, tg, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                tg.upload(),
            )
            del tg
        elif self.is_uphoster:
            LOGGER.info(f"Uphoster Upload Name: {self.name}")
            uphoster_service = self.user_dict.get("UPHOSTER_SERVICE", "gofile")
            services = uphoster_service.split(",")
            ddl = MultiUphosterUpload(self, up_path, services)
            async with task_dict_lock:
                task_dict[self.mid] = UphosterStatus(self, ddl, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                ddl.upload(),
            )
            del ddl
        elif is_gdrive_id(self.up_dest):
            LOGGER.info(f"Gdrive Upload Name: {self.name}")
            drive = GoogleDriveUpload(self, up_path)
            async with task_dict_lock:
                task_dict[self.mid] = GoogleDriveStatus(self, drive, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                sync_to_async(drive.upload),
            )
            del drive
        else:
            LOGGER.info(f"Rclone Upload Name: {self.name}")
            RCTransfer = RcloneTransferHelper(self)
            async with task_dict_lock:
                task_dict[self.mid] = RcloneStatus(self, RCTransfer, gid, "up")
            await gather(
                update_status_message(self.message.chat.id),
                RCTransfer.upload(up_path),
            )
            del RCTransfer
        return

    async def on_upload_complete(
        self, link, files, folders, mime_type, rclone_path="", dir_id=""
    ):
        upload_time = time() - getattr(self, "upload_start_time", time())
        self.avg_upload_speed = self.size / upload_time if upload_time > 0 else 0
        if (
            self.is_super_chat
            and (Config.INCOMPLETE_TASK_NOTIFIER or Config.INCOMPLETE_TASK_RESUME)
            and Config.DATABASE_URL
        ):
            await database.rm_complete_task(self.message.link)
        msg = (
            f"<b><i>{escape(self.name)}</i></b>\n│"
            f"\n┟ <b>Task Size</b> → {get_readable_file_size(self.size)}"
            f"\n┠ <b>Time Taken</b> → {get_readable_time(time() - self.message.date.timestamp())}"
            f"\n┠ <b>In / Out Mode</b> → {self.mode[0]} | {self.mode[1]}"
        )

        if not getattr(self, "is_clone", False):
            msg += (
                f"\n┠ <b>Avg DL Speed</b> → {get_readable_file_size(self.avg_download_speed)}/s"
                f"\n┠ <b>Avg UL Speed</b> → {get_readable_file_size(self.avg_upload_speed)}/s"
            )
        LOGGER.info(f"Task Done: {self.name}")
        if self.is_yt:
            buttons = ButtonMaker()
            if mime_type == "Folder/Playlist":
                msg += "\n┠ <b>Type</b> → Playlist"
                msg += f"\n┖ <b>Total Videos</b> → {files}"
                if link:
                    buttons.url_button(
                        "🔗 View Playlist", link, style=ButtonStyle.PRIMARY
                    )
                user_message = f"{self.tag}\nYour playlist ({files} videos) has been uploaded to YouTube successfully!"
            else:
                msg += "\n┖ <b>Type</b> → Video"
                if link:
                    buttons.url_button("🔗 View Video", link, style=ButtonStyle.PRIMARY)
                user_message = (
                    f"{self.tag}\nYour video has been uploaded to YouTube successfully!"
                )

            msg += f"\n\n<b>Task By: </b>{self.tag}"

            button = buttons.build_menu(1) if link else None

            await send_message(self.user_id, msg, button)
            if Config.LEECH_DUMP_CHAT:
                await send_message(int(Config.LEECH_DUMP_CHAT), msg, button)
            await send_message(self.message, user_message, button)

        elif self.is_leech:
            msg += f"\n<b>Total Files: </b>{folders}"
            if mime_type != 0:
                msg += f"\n┠ <b>Corrupted Files</b> → {mime_type}"
            msg += f"\n┖ <b>Task By</b> → {self.tag}\n\n"

            if self.bot_pm or self.private_output:
                pmsg = msg
                # pmsg += "〶 <b><u>Action Performed :</u></b>\n"
                # pmsg += "⋗ <i>File(s) have been sent to User PM</i>\n\n"
                if self.is_super_chat:
                    pm_button = ButtonMaker()
                    pm_button.url_button(
                        "📥 View in Bot PM", f"https://t.me/{TgClient.BNAME}"
                    )
                    await send_message(self.message, pmsg, pm_button.build_menu(1))

            if not files and not self.is_super_chat:
                await send_message(self.message, msg)
            else:
                log_chat = (
                    self.user_id
                    if (self.bot_pm or self.private_output)
                    else self.message
                )
                msg += "〶 <b><u>Files List :</u></b>\n"
                fmsg = ""
                for index, (link, name) in enumerate(files.items(), start=1):
                    chat_id, msg_id = link.split("/")[-2:]
                    fmsg += f"{index}. <a href='{link}'>{name}</a>"
                    if Config.MEDIA_STORE and (
                        self.is_super_chat or Config.LEECH_DUMP_CHAT
                    ):
                        if chat_id.isdigit():
                            chat_id = f"-100{chat_id}"
                        flink = f"https://t.me/{TgClient.BNAME}?start={encode_slink('file' + chat_id + '&&' + msg_id)}"
                        fmsg += f"\n┖ <b>Get Media</b> → <a href='{flink}'>Store Link</a> | <a href='https://t.me/share/url?url={flink}'>Share Link</a>"
                    fmsg += "\n"
                    if len(fmsg.encode() + msg.encode()) > 4000:
                        await send_message(log_chat, msg + fmsg)
                        await sleep(1)
                        fmsg = ""
                if fmsg != "":
                    await send_message(log_chat, msg + fmsg)
        else:
            msg += f"\n│\n┟ <b>Type</b> → {mime_type}"
            if mime_type == "Folder":
                msg += f"\n┠ <b>SubFolders</b> → {folders}"
                msg += f"\n┠ <b>Files</b> → {files}"

            multi_link_msg = ""
            multi_links = []
            if isinstance(link, dict) and not self.is_yt:
                # MultiUphoster result
                for service, result in link.items():
                    if "error" in result:
                        multi_link_msg += (
                            f"{service.capitalize()}: Error - {result['error']}\n"
                        )
                    elif result.get("link"):
                        multi_links.append(
                            (f"{service.capitalize()} Link", result["link"])
                        )
                multi_link_msg = multi_link_msg.strip()
                link = None  # Disable single link button logic

            if (
                link
                or rclone_path
                and Config.RCLONE_SERVE_URL
                and not self.private_link
                or multi_links
            ):
                buttons = ButtonMaker()
                if link and Config.SHOW_CLOUD_LINK:
                    buttons.url_button("☁️ Cloud Link", link, style=ButtonStyle.PRIMARY)
                elif multi_links:
                    for name, url in multi_links:
                        buttons.url_button(name, url)
                else:
                    msg += f"\n\nPath: <code>{rclone_path}</code>"
                if rclone_path and Config.RCLONE_SERVE_URL and not self.private_link:
                    remote, rpath = rclone_path.split(":", 1)
                    url_path = rutils.quote(f"{rpath}")
                    if Config.RCLONE_USE_REMOTE_PREFIX:
                        share_url = f"{Config.RCLONE_SERVE_URL}/{remote}/{url_path}"
                    else:
                        share_url = f"{Config.RCLONE_SERVE_URL}/{url_path}"
                    if mime_type == "Folder":
                        share_url += "/"
                    buttons.url_button(
                        "🔗 Rclone Link", share_url, style=ButtonStyle.PRIMARY
                    )
                if not rclone_path and dir_id:
                    INDEX_URL = ""
                    if self.private_link:
                        INDEX_URL = self.user_dict.get("INDEX_URL", "") or ""
                    elif Config.INDEX_URL:
                        INDEX_URL = Config.INDEX_URL
                    if INDEX_URL and self.name:
                        safe_name = rutils.quote(self.name.strip("/"))
                        share_url = f"{INDEX_URL}/{safe_name}"
                        if mime_type == "Folder":
                            share_url += "/"
                        buttons.url_button(
                            "⚡ Index Link", share_url, style=ButtonStyle.PRIMARY
                        )
                        if mime_type.startswith(("image", "video", "audio")):
                            share_urls = f"{share_url}?a=view"
                            buttons.url_button(
                                "🌐 View Link", share_urls, style=ButtonStyle.PRIMARY
                            )
                button = buttons.build_menu(2)
            else:
                if not multi_link_msg:
                    msg += f"\n┃\n┠ Path: <code>{rclone_path}</code>"
                button = None
            msg += f"\n┃\n┖ <b>Task By</b> → {self.tag}\n\n"
            group_msg = (
                msg
                # msg + "〶 <b><u>Action Performed :</u></b>\n"
                # "⋗ <i>Cloud link(s) have been sent to User PM</i>\n\n"
            )

            if multi_link_msg:
                group_msg += multi_link_msg + "\n"
                msg += multi_link_msg + "\n"

            if (self.bot_pm or self.private_output) and self.is_super_chat:
                await send_message(self.user_id, msg, button)

            if hasattr(Config, "MIRROR_LOG_ID") and Config.MIRROR_LOG_ID:
                await send_message(Config.MIRROR_LOG_ID, msg, button)

            if (self.bot_pm or self.private_output) and self.is_super_chat:
                pm_button = ButtonMaker()
                pm_button.url_button(
                    "📥 View in Bot PM", f"https://t.me/{TgClient.BNAME}"
                )
                group_btn = pm_button.build_menu(1)
            else:
                group_btn = button
            await send_message(self.message, group_msg, group_btn)
        if self.seed:
            await clean_target(self.up_dir)
            async with queue_dict_lock:
                if self.mid in non_queued_up:
                    non_queued_up.remove(self.mid)
            await start_from_queued()
            return

        if self.pm_msg and (not Config.DELETE_LINKS or Config.CLEAN_LOG_MSG):
            await delete_message(self.pm_msg)

        await clean_download(self.dir)
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        async with queue_dict_lock:
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()

    async def on_download_error(self, error, button=None, is_limit=False):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        await self.remove_from_same_dir()
        error_msg = (
            error
            if isinstance(error, str)
            and (
                error.startswith("File/Folder is already available")
                or "<code>" in error
                or "<b>" in error
            )
            else escape(str(error))
        )
        task_ref = (
            self.name
            if self.name
            else (
                (self.link[:50] + "...")
                if isinstance(self.link, str) and len(self.link) > 50
                else self.link
            )
        )
        name_info = f"<b><i>{escape(str(task_ref))}</i></b>\n│\n" if task_ref else ""

        msg = (
            f"""〶 <b><i><u>Limit Breached:</u></i></b>

{name_info}┎ <b>Task Size</b> → {get_readable_file_size(self.size)}
┠ <b>In / Out Mode</b> → {self.mode[0]} | {self.mode[1]}
{error}"""
            if is_limit
            else f"""{name_info}<i><b>〶 Download Stopped!</b></i>

┎ <b>Due To</b> → {error_msg}
┠ <b>Task Size</b> → {get_readable_file_size(self.size)}
┠ <b>Time Taken</b> → {get_readable_time(time() - self.message.date.timestamp())}
┠ <b>In / Out Mode</b> → {self.mode[0]} | {self.mode[1]}
┖ <b>Task By</b> → {self.tag}"""
        )

        await send_message(self.message, msg, button)
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and (Config.INCOMPLETE_TASK_NOTIFIER or Config.INCOMPLETE_TASK_RESUME)
            and Config.DATABASE_URL
            and not intervals.get("stopAll", False)
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)

    async def on_upload_error(self, error):
        async with task_dict_lock:
            if self.mid in task_dict:
                del task_dict[self.mid]
            count = len(task_dict)
        task_ref = (
            self.name
            if self.name
            else (
                (self.link[:50] + "...")
                if isinstance(self.link, str) and len(self.link) > 50
                else self.link
            )
        )
        task_info = f"<b><i>{escape(str(task_ref))}</i></b>\n│\n" if task_ref else ""
        await send_message(self.message, f"{task_info}{self.tag} {escape(str(error))}")
        if count == 0:
            await self.clean()
        else:
            await update_status_message(self.message.chat.id)

        if (
            self.is_super_chat
            and (Config.INCOMPLETE_TASK_NOTIFIER or Config.INCOMPLETE_TASK_RESUME)
            and Config.DATABASE_URL
            and not intervals.get("stopAll", False)
        ):
            await database.rm_complete_task(self.message.link)

        async with queue_dict_lock:
            if self.mid in queued_dl:
                queued_dl[self.mid].set()
                del queued_dl[self.mid]
            if self.mid in queued_up:
                queued_up[self.mid].set()
                del queued_up[self.mid]
            if self.mid in non_queued_dl:
                non_queued_dl.remove(self.mid)
            if self.mid in non_queued_up:
                non_queued_up.remove(self.mid)

        await start_from_queued()
        await sleep(3)
        await clean_download(self.dir)
        if self.up_dir:
            await clean_download(self.up_dir)
        if self.thumb and await aiopath.exists(self.thumb):
            await remove(self.thumb)
