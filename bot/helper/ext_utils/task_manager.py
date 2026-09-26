from asyncio import Event
from time import time

from ... import (
    LOGGER,
    bot_cache,
    non_queued_dl,
    non_queued_up,
    queue_dict_lock,
    queued_dl,
    queued_up,
    user_data,
)
from ...core.config_manager import Config
from ..mirror_leech_utils.gdrive_utils.search import GoogleDriveSearch
from ..telegram_helper.filters import CustomFilters
from ..telegram_helper.tg_utils import check_botpm, forcesub, verify_token
from .bot_utils import get_telegraph_list, sync_to_async, safe_int
from .files_utils import get_base_name, check_storage_threshold
from .links_utils import is_gdrive_id, is_rclone_path
from .status_utils import get_readable_time, get_readable_file_size, get_specific_tasks


async def stop_duplicate_check(listener):
    if (
        isinstance(listener.up_dest, int)
        or listener.is_leech
        or listener.select
        or not listener.stop_duplicate
        or listener.same_dir
    ):
        return False, None

    min_size = (
        listener.user_dict.get("STOP_DUPLICATE_MIN_SIZE")
        or (
            "STOP_DUPLICATE_MIN_SIZE" not in listener.user_dict
            and Config.STOP_DUPLICATE_MIN_SIZE
        )
        or 0
    )
    if min_size and getattr(listener, "size", 0):
        min_size_bytes = min_size * 1024 * 1024
        if listener.size < min_size_bytes:
            LOGGER.info(
                f"Skipping stop duplicate check for '{listener.name}' because file size ({get_readable_file_size(listener.size)}) is less than minimum limit ({min_size}MB)"
            )
            return False, None

    name = listener.name

    if listener.compress:
        name = f"{name}.zip"
    elif listener.extract:
        try:
            name = get_base_name(name)
        except Exception:
            name = None

    if not name:
        return False, None

    # GDrive stop duplicate check
    if is_gdrive_id(listener.up_dest):
        if listener.up_dest.startswith("mtp:"):
            return False, None
        LOGGER.info(f"Checking File/Folder if already in Drive: {name}")
        telegraph_content, contents_no = await sync_to_async(
            GoogleDriveSearch(stop_dup=True, no_multi=listener.is_clone).drive_list,
            name,
            listener.up_dest,
            listener.user_id,
        )
        if telegraph_content:
            msg = f"File/Folder is already available in Drive.\nHere are {contents_no} list results:"
            button = await get_telegraph_list(telegraph_content)
            return msg, button

    # Rclone stop duplicate check using rc_search cached index
    elif is_rclone_path(listener.up_dest):
        LOGGER.info(f"Checking File/Folder if already in Rclone Dest: {name}")
        msg, button = await _rclone_stop_duplicate_check(name)
        if msg:
            return msg, button

    return False, None


async def _rclone_stop_duplicate_check(name):
    """Check if a file/folder with the same name exists on the rclone remote
    using the global cached index from rc_search. Returns inline results
    styled like /list output. Uses plain text since callers may escape HTML."""
    from ...modules.rc_search import (
        search_files,
        format_size,
        RCLONE_SERVE_URL,
        REMOTE_BASE_PATH,
        RCLONE_REMOTE,
    )
    from ...core.config_manager import BinConfig
    from .bot_utils import cmd_exec

    files = await sync_to_async(search_files)
    if not files:
        return False, None

    matched = [f for f in files if f["Name"].lower() == name.lower()]

    if not matched:
        return False, None

    msg = "File/Folder is already available in Rclone Destination.\n"
    msg += f"┠ <b>Duplicates Found:</b> {len(matched)}"

    for i, f in enumerate(matched[:10], 1):
        is_dir = f.get("IsDir", False)
        path = f["Path"]
        if is_dir:
            size = "Folder"
        else:
            size = format_size(f.get("Size", 0))

        msg += f"\n┠ <b>{i}.</b> {f['Name']}\n"
        msg += f"┠ <b>Size:</b> {size}"

        links = []
        if RCLONE_SERVE_URL:
            remote = ""
            if RCLONE_REMOTE and ":" in RCLONE_REMOTE:
                remote = RCLONE_REMOTE.split(":", 1)[0]

            url_path = path
            if REMOTE_BASE_PATH:
                if url_path.startswith(f"{REMOTE_BASE_PATH}/"):
                    url_path = url_path[len(REMOTE_BASE_PATH) + 1 :]
                rpath = f"{REMOTE_BASE_PATH}/{url_path}"
            else:
                rpath = url_path

            from requests import utils as rutils

            url_path_quoted = rutils.quote(f"{rpath}")

            from ...core.config_manager import Config

            if getattr(Config, "RCLONE_USE_REMOTE_PREFIX", False):
                public_link = f"{RCLONE_SERVE_URL}/{remote}/{url_path_quoted}"
            else:
                public_link = f"{RCLONE_SERVE_URL}/{url_path_quoted}"

            if is_dir:
                public_link += "/"
            links.append(f'<a href="{public_link}">Rclone Link</a>')

        # Get native cloud link via rclone link (best effort)
        if RCLONE_REMOTE:
            try:
                full_path = f"{RCLONE_REMOTE}{path}"
                cmd = [
                    BinConfig.RCLONE_NAME,
                    "link",
                    "--config",
                    "rclone.conf",
                    full_path,
                ]
                res, _, code = await cmd_exec(cmd)
                if code == 0 and res.strip():
                    links.append(f'<a href="{res.strip()}">Cloud Link</a>')
            except Exception:
                pass

        if links:
            msg += f"\n┠ <b>Links:</b> {' | '.join(links)}"

    if len(matched) > 10:
        msg += f"\n┠ ... and {len(matched) - 10} more."

    return msg, None


async def check_running_tasks(listener, state="dl"):
    all_limit = safe_int(Config.QUEUE_ALL)
    state_limit = (
        safe_int(Config.QUEUE_DOWNLOAD)
        if state == "dl"
        else safe_int(Config.QUEUE_UPLOAD)
    )
    event = None
    is_over_limit = False
    async with queue_dict_lock:
        if state == "up" and listener.mid in non_queued_dl:
            non_queued_dl.remove(listener.mid)
        if (
            (all_limit or state_limit)
            and not listener.force_run
            and not (listener.force_upload and state == "up")
            and not (listener.force_download and state == "dl")
        ):
            dl_count = len(non_queued_dl)
            up_count = len(non_queued_up)
            t_count = dl_count if state == "dl" else up_count
            is_over_limit = (
                all_limit
                and dl_count + up_count >= all_limit
                and (not state_limit or t_count >= state_limit)
            ) or (state_limit and t_count >= state_limit)
            if is_over_limit:
                event = Event()
                if state == "dl":
                    queued_dl[listener.mid] = event
                else:
                    queued_up[listener.mid] = event
        if not is_over_limit:
            if state == "up":
                non_queued_up.add(listener.mid)
            else:
                non_queued_dl.add(listener.mid)

    return is_over_limit, event


async def start_dl_from_queued(mid: int):
    queued_dl[mid].set()
    del queued_dl[mid]
    non_queued_dl.add(mid)


async def start_up_from_queued(mid: int):
    queued_up[mid].set()
    del queued_up[mid]
    non_queued_up.add(mid)


async def start_from_queued():
    if all_limit := safe_int(Config.QUEUE_ALL):
        dl_limit = safe_int(Config.QUEUE_DOWNLOAD)
        up_limit = safe_int(Config.QUEUE_UPLOAD)
        async with queue_dict_lock:
            dl = len(non_queued_dl)
            up = len(non_queued_up)
            all_ = dl + up
            if all_ < all_limit:
                f_tasks = all_limit - all_
                if queued_up and (not up_limit or up < up_limit):
                    for index, mid in enumerate(list(queued_up.keys()), start=1):
                        await start_up_from_queued(mid)
                        f_tasks -= 1
                        if f_tasks == 0 or (up_limit and index >= up_limit - up):
                            break
                if queued_dl and (not dl_limit or dl < dl_limit) and f_tasks != 0:
                    for index, mid in enumerate(list(queued_dl.keys()), start=1):
                        await start_dl_from_queued(mid)
                        if (dl_limit and index >= dl_limit - dl) or index == f_tasks:
                            break
        return

    if up_limit := Config.QUEUE_UPLOAD:
        async with queue_dict_lock:
            up = len(non_queued_up)
            if queued_up and up < up_limit:
                f_tasks = up_limit - up
                for index, mid in enumerate(list(queued_up.keys()), start=1):
                    await start_up_from_queued(mid)
                    if index == f_tasks:
                        break
    else:
        async with queue_dict_lock:
            if queued_up:
                for mid in list(queued_up.keys()):
                    await start_up_from_queued(mid)

    if dl_limit := Config.QUEUE_DOWNLOAD:
        async with queue_dict_lock:
            dl = len(non_queued_dl)
            if queued_dl and dl < dl_limit:
                f_tasks = dl_limit - dl
                for index, mid in enumerate(list(queued_dl.keys()), start=1):
                    await start_dl_from_queued(mid)
                    if index == f_tasks:
                        break
    else:
        async with queue_dict_lock:
            if queued_dl:
                for mid in list(queued_dl.keys()):
                    await start_dl_from_queued(mid)


async def limit_checker(listener, yt_playlist=0):
    LOGGER.info("Checking Size Limit...")
    if await CustomFilters.sudo("", listener.message):
        LOGGER.info("SUDO User. Skipping Size Limit...")
        return

    size = listener.size

    async def recurr_limits(limits):
        nonlocal yt_playlist, size
        limit_exceeded = ""
        for condition, attr, name in limits:
            if condition and (limit := getattr(Config, attr, 0)):
                if attr == "PLAYLIST_LIMIT":
                    if yt_playlist >= limit:
                        limit_exceeded = f"┠ <b>{name} Limit Count</b> → {limit}"
                else:
                    byte_limit = limit * 1024**3
                    if size >= byte_limit:
                        limit_exceeded = f"┠ <b>{name} Limit</b> → {get_readable_file_size(byte_limit)}"

                LOGGER.info(
                    f"{name} Limit Breached: {listener.name} & Size: {get_readable_file_size(size)}"
                )
                break
        return limit_exceeded

    limits = [
        (listener.is_torrent or listener.is_qbit, "TORRENT_LIMIT", "Torrent"),
        (listener.is_mega, "MEGA_LIMIT", "Mega"),
        (listener.is_gdrive, "GD_DL_LIMIT", "GDriveDL"),
        (listener.is_clone, "CLONE_LIMIT", "Clone"),
        (listener.is_jd, "JD_LIMIT", "JDownloader"),
        (listener.is_nzb, "NZB_LIMIT", "SABnzbd"),
        (listener.is_seedr, "SEEDR_LIMIT", "Seedr"),
        (listener.is_rclone, "RC_DL_LIMIT", "RCloneDL"),
        (listener.is_ytdlp, "YTDLP_LIMIT", "YT-DLP"),
        (bool(yt_playlist), "PLAYLIST_LIMIT", "Playlist"),
        (True, "DIRECT_LIMIT", "Direct"),
    ]
    limit_exceeded = await recurr_limits(limits)

    if not limit_exceeded:
        extra_limits = [
            (listener.is_leech, "LEECH_LIMIT", "Leech"),
            (listener.compress, "ARCHIVE_LIMIT", "Archive"),
            (listener.extract, "EXTRACT_LIMIT", "Extract"),
        ]
        limit_exceeded = await recurr_limits(extra_limits)

        if Config.STORAGE_LIMIT and not listener.is_clone:
            limit = Config.STORAGE_LIMIT * 1024**3
            if not await check_storage_threshold(
                size, limit, any([listener.compress, listener.extract])
            ):
                limit_exceeded = f"┠ <b>Threshold Storage Limit</b> → {get_readable_file_size(limit)}"

    if limit_exceeded:
        return limit_exceeded + f"\n┖ <b>Task By</b> → {listener.tag}"


"""
class UsageChecks: # TODO: Dynamic Check for All Task

class DailyUsageChecks:
"""


async def user_interval_check(user_id):
    bot_cache.setdefault("time_interval", {})
    if (time_interval := bot_cache["time_interval"].get(user_id, False)) and (
        time() - time_interval
    ) < (UTI := Config.USER_TIME_INTERVAL):
        return UTI - (time() - time_interval)
    bot_cache["time_interval"][user_id] = time()
    return None


async def pre_task_check(message):
    LOGGER.info("Running Pre Task Checks ...")
    msg = []
    button = None
    if await CustomFilters.sudo("", message):
        return msg, button
    user_id = (message.from_user or message.sender_chat).id
    if Config.RSS_CHAT and user_id == int(Config.RSS_CHAT):
        return msg, button
    user_dict = user_data.get(user_id, {})
    if message.chat.type != message.chat.type.BOT:
        if ids := Config.FORCE_SUB_IDS:
            _msg, button = await forcesub(message, ids, button)
            if _msg:
                msg.append(_msg)
        if Config.BOT_PM or user_dict.get("BOT_PM"):  # or config_dict['SAFE_MODE']:
            _msg, button = await check_botpm(message, button)
            if _msg:
                msg.append(_msg)
    if (uti := Config.USER_TIME_INTERVAL) and (
        ut := await user_interval_check(user_id)
    ):
        msg.append(
            f"┠ <b>Waiting Time</b> → {get_readable_time(ut)}\n┠ <i>User's Time Interval Restrictions</i> → {get_readable_time(uti)}"
        )
    bmax_tasks = safe_int(user_dict.get("bmax_tasks", Config.BOT_MAX_TASKS))
    if bmax_tasks > 0 and len(await get_specific_tasks("All", False)) >= bmax_tasks:
        msg.append(
            f"┠ Max Concurrent Bot's Tasks Limit exceeded.\n┠ Bot Tasks Limit : {bmax_tasks} task"
        )

    maxtask = safe_int(user_dict.get("maxtask", Config.USER_MAX_TASKS))
    if maxtask > 0 and len(await get_specific_tasks("All", user_id)) >= maxtask:
        msg.append(
            f"┠ Max Concurrent User's Task(s) Limit exceeded! \n┠ User Task Limit : {maxtask} tasks"
        )

    token_msg, button = await verify_token(user_id, button)
    if token_msg is not None:
        msg.append(token_msg)

    if msg:
        username = message.from_user.mention
        final_msg = f"⌬ <b>Task Checks :</b>\n│\n┟ <b>Name</b> → {username}\n┃\n"
        for i, m_part in enumerate(msg, 1):
            final_msg += f"{m_part}\n"
        if button is not None:
            button = button.build_menu(2)
        return final_msg, button

    return None, None


async def check_blacklisted_keywords(listener, name_or_link=None):
    if not name_or_link:
        name_or_link = listener.name or listener.link
    if not name_or_link or not listener.blacklisted_keywords:
        return False, None

    import re
    from urllib.parse import unquote

    text = unquote(str(name_or_link)).replace("+", " ").lower()
    for kw in listener.blacklisted_keywords:
        if not kw:
            continue
        kw_clean = kw.lower().strip()
        pattern = (
            r"(^|[\s_.\-+=!\[\]()/\\])"
            + re.escape(kw_clean)
            + r"($|[\s_.\-+=!\[\]()/\\])"
        )
        if re.search(pattern, text):
            return True, kw
    return False, None
