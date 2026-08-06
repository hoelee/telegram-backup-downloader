"""Reliable Telegram channel backup downloader.

Configuration is read from config.json in the current working directory.
"""
import asyncio
import json
import logging
import os
import re
import shutil
import signal
import sys
from datetime import datetime, timedelta, timezone
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiofiles
import aiosqlite
from telethon import TelegramClient, events, utils

CONFIG_FILE = Path("config.json")
BASE_DIR = Path("channels")
LOG_DIR = Path("logs")
BASE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

with CONFIG_FILE.open("r", encoding="utf-8") as config_file:
    CONFIG = json.load(config_file)

API_ID = CONFIG.get("api_id")
API_HASH = CONFIG.get("api_hash")
PHONE = CONFIG.get("phone_number")
SESSION_NAME = CONFIG.get("session_name")
CHANNELS = CONFIG.get("channels", [])
PARALLEL_DOWNLOADS = CONFIG.get("parallel_downloads", 3)
DOWNLOAD_TIMEOUT = CONFIG.get("download_timeout_seconds", 600)
DOWNLOAD_RETRY = CONFIG.get("download_retry_count", 3)
QUEUE_MAX_SIZE = CONFIG.get("queue_max_size", 5000)
MIN_DISK_SPACE_GB = CONFIG.get("min_disk_space_gb", 6)
MIN_DISK_SPACE = MIN_DISK_SPACE_GB * 1024**3
STATUS_PORT = CONFIG.get("status_port", 0)
RESYNC_INTERVAL = CONFIG.get("resync_interval_minutes", 60)
MAX_LIFETIME_RETRIES = CONFIG.get("max_lifetime_retries", 20)
CHANNEL_AUTO_DISABLE_AFTER = CONFIG.get("channel_auto_disable_after", 5)
MEDIA_RECORD_TTL_DAYS = CONFIG.get("media_record_ttl_days", 90)
RETRY_DROP_LOG = CONFIG.get("retry_drop_log", True)
DB_PATH = CONFIG.get("db_path", "telegram_state.db")
START_TIME = datetime.now(timezone.utc)

logger = logging.getLogger("telegram_archiver")
logger.setLevel(logging.INFO)
logger.propagate = False
if not logger.handlers:
    formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s")
    file_handler = RotatingFileHandler(LOG_DIR / "app.log", maxBytes=10 * 1024 * 1024,
                                       backupCount=10, encoding="utf-8")
    console_handler = logging.StreamHandler(sys.stdout)
    file_handler.setFormatter(formatter)
    console_handler.setFormatter(formatter)
    logger.addHandler(file_handler)
    logger.addHandler(console_handler)

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS channel_state (
    channel_id TEXT PRIMARY KEY, last_message_id INTEGER DEFAULT 0
);
CREATE TABLE IF NOT EXISTS downloaded_media (
    channel_id TEXT, message_id INTEGER, file_name TEXT, file_size INTEGER,
    downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS logged_messages (
    channel_id TEXT, message_id INTEGER, PRIMARY KEY(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS failed_downloads (
    channel_id TEXT, message_id INTEGER, last_error TEXT, retry_count INTEGER,
    PRIMARY KEY(channel_id, message_id)
);
CREATE TABLE IF NOT EXISTS channel_fail_count (
    channel_id TEXT PRIMARY KEY, consecutive_fails INTEGER DEFAULT 0, last_fail TEXT
);
"""

INVALID_CHARS = r'[<>:"/\\|?*]'
MY_TZ = timezone(timedelta(hours=8))
db = None
download_allowed = None
download_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
priority_download_queue = asyncio.Queue()
shutdown_event = asyncio.Event()
shutdown_lock = asyncio.Lock()
CONFIG_LOCK = asyncio.Lock()
MANUAL_IN_PROGRESS = set()
MONITORED_CHANNEL_IDS = set()
CHANNEL_ENTITY_MAP = {}
CHANNEL_OBJECT_MAP = {}
active_tasks = set()
worker_tasks = []
worker_busy_count = 0
config_task = None
sync_task = None
disk_task = None
resync_task = None
status_task = None
supervisor_task = None

client = TelegramClient(SESSION_NAME, API_ID, API_HASH, auto_reconnect=True,
                        connection_retries=None, retry_delay=5)


def validate_config(cfg):
    required = {
        "api_id": int,
        "api_hash": str,
        "phone_number": str,
        "session_name": str,
        "channels": list,
    }
    for key, expected_type in required.items():
        if key not in cfg:
            raise ValueError(f"Missing required config key: {key}")
        if not isinstance(cfg[key], expected_type) or (expected_type is str and not cfg[key].strip()):
            raise ValueError(f"Config key '{key}' must be a non-empty {expected_type.__name__}")
    if cfg["api_id"] <= 0:
        raise ValueError("Config key 'api_id' must be a positive integer")
    if not all(isinstance(channel, (str, int)) for channel in cfg["channels"]):
        raise ValueError("Config key 'channels' must contain strings or integers")


def sanitize_filename(name):
    return re.sub(INVALID_CHARS, "_", name).strip().rstrip(".")[:200] or "unnamed"


def parse_channel_id(raw_id):
    if isinstance(raw_id, str) and raw_id.lstrip("-").isdigit():
        return int(raw_id)
    return raw_id


def get_channel_paths(channel_name):
    root = BASE_DIR / sanitize_filename(channel_name)
    paths = {"root": root, "photo": root / "photo", "video": root / "video",
             "document": root / "document", "messages": root / "messages.txt",
             "metadata": root / "metadata.jsonl"}
    for key in ("root", "photo", "video", "document"):
        paths[key].mkdir(parents=True, exist_ok=True)
    return paths


def detect_media_type(message):
    if message.photo:
        return "photo"
    if message.video:
        return "video"
    if message.document:
        return "document"
    return None


async def init_db():
    global db
    db = await aiosqlite.connect(DB_PATH)
    await db.execute("PRAGMA journal_mode=WAL")
    await db.execute("PRAGMA synchronous=NORMAL")
    await db.executescript(CREATE_TABLES_SQL)
    await db.commit()
    try:
        await db.execute("ALTER TABLE downloaded_media ADD COLUMN downloaded_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP")
        await db.commit()
    except Exception:
        # SQLite raises if the column exists; older SQLite builds also reject
        # CURRENT_TIMESTAMP as an ALTER TABLE default, handled below.
        pass
    async with db.execute("PRAGMA table_info(downloaded_media)") as cur:
        columns = {row[1] for row in await cur.fetchall()}
    if "downloaded_at" not in columns:
        await db.execute("ALTER TABLE downloaded_media ADD COLUMN downloaded_at TEXT")
        await db.execute("UPDATE downloaded_media SET downloaded_at=CURRENT_TIMESTAMP WHERE downloaded_at IS NULL")
        await db.commit()


async def get_last_message_id(channel_id):
    async with db.execute("SELECT last_message_id FROM channel_state WHERE channel_id=?", (channel_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 0


async def update_last_message_id(channel_id, message_id):
    await db.execute("""INSERT INTO channel_state(channel_id,last_message_id) VALUES (?,?)
        ON CONFLICT(channel_id) DO UPDATE SET last_message_id=MAX(last_message_id,excluded.last_message_id)""",
                     (channel_id, message_id))
    await db.commit()


async def media_exists(channel_id, message_id):
    async with db.execute("SELECT 1 FROM downloaded_media WHERE channel_id=? AND message_id=?", (channel_id, message_id)) as cur:
        return await cur.fetchone() is not None


async def add_media_record(channel_id, message_id, file_name, file_size):
    await db.execute("""INSERT OR IGNORE INTO downloaded_media
        (channel_id,message_id,file_name,file_size) VALUES (?,?,?,?)""",
                     (channel_id, message_id, file_name, file_size))
    await db.commit()


async def text_logged(channel_id, message_id):
    async with db.execute("SELECT 1 FROM logged_messages WHERE channel_id=? AND message_id=?", (channel_id, message_id)) as cur:
        return await cur.fetchone() is not None


async def add_logged_message(channel_id, message_id):
    await db.execute("INSERT OR IGNORE INTO logged_messages VALUES (?,?)", (channel_id, message_id))
    await db.commit()


async def add_failed_download(channel_id, message_id, error):
    await db.execute("""INSERT INTO failed_downloads(channel_id,message_id,last_error,retry_count)
        VALUES (?,?,?,1) ON CONFLICT(channel_id,message_id) DO UPDATE SET
        retry_count=retry_count+1,last_error=excluded.last_error""", (channel_id, message_id, error[:1000]))
    await db.commit()
    async with db.execute("SELECT retry_count FROM failed_downloads WHERE channel_id=? AND message_id=?", (channel_id, message_id)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 1


async def increment_channel_fail(channel_id, error):
    await db.execute("""INSERT INTO channel_fail_count(channel_id,consecutive_fails,last_fail)
        VALUES (?,1,?) ON CONFLICT(channel_id) DO UPDATE SET
        consecutive_fails=consecutive_fails+1,last_fail=excluded.last_fail""", (channel_id, error[:500]))
    await db.commit()
    async with db.execute("SELECT consecutive_fails FROM channel_fail_count WHERE channel_id=?", (channel_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 1


async def reset_channel_fail(channel_id):
    await db.execute("DELETE FROM channel_fail_count WHERE channel_id=?", (channel_id,))
    await db.commit()


async def get_channel_fail_count(channel_id):
    async with db.execute("SELECT consecutive_fails FROM channel_fail_count WHERE channel_id=?", (channel_id,)) as cur:
        row = await cur.fetchone()
        return row[0] if row else 0


async def save_config():
    async with CONFIG_LOCK:
        async with aiofiles.open(CONFIG_FILE, "w", encoding="utf-8") as file:
            await file.write(json.dumps(CONFIG, indent=2, ensure_ascii=False) + "\n")


async def append_message_log(path, text):
    async with aiofiles.open(path, "a", encoding="utf-8") as file:
        await file.write(text)


async def append_metadata(path, data):
    async with aiofiles.open(path, "a", encoding="utf-8") as file:
        await file.write(json.dumps(data, ensure_ascii=False) + "\n")


async def resolve_channels():
    MONITORED_CHANNEL_IDS.clear()
    CHANNEL_ENTITY_MAP.clear()
    CHANNEL_OBJECT_MAP.clear()
    overrides = CONFIG.get("channel_overrides", {})
    for channel in CHANNELS:
        channel_key = str(parse_channel_id(channel))
        if not overrides.get(channel_key, {}).get("turnon", True):
            logger.info("[RESOLVE] Skipping disabled channel %s", channel)
            continue
        try:
            entity = await client.get_entity(parse_channel_id(channel))
            entity_id = str(utils.get_peer_id(entity))
            title = getattr(entity, "title", entity_id)
            MONITORED_CHANNEL_IDS.add(entity_id)
            CHANNEL_ENTITY_MAP[entity_id] = title
            CHANNEL_OBJECT_MAP[entity_id] = entity
            await reset_channel_fail(entity_id)
        except Exception as error:
            fail_count = await increment_channel_fail(channel_key, str(error))
            logger.error("[RESOLVE ERROR] Failed to resolve channel %r: %s", channel, error)
            if CHANNEL_AUTO_DISABLE_AFTER > 0 and fail_count >= CHANNEL_AUTO_DISABLE_AFTER:
                logger.warning("[CHANNEL DISABLED] %r failed %d consecutive resolutions", channel, fail_count)
                CONFIG.setdefault("channel_overrides", {}).setdefault(channel_key, {})["turnon"] = False
                await save_config()


def apply_channel_overrides():
    overrides = CONFIG.get("channel_overrides", {})
    for entity_id in list(MONITORED_CHANNEL_IDS):
        if not overrides.get(entity_id, {}).get("turnon", True):
            MONITORED_CHANNEL_IDS.discard(entity_id)
            CHANNEL_ENTITY_MAP.pop(entity_id, None)
            CHANNEL_OBJECT_MAP.pop(entity_id, None)
            logger.info("[OVERRIDE] Disabled channel %s", entity_id)


async def reload_config():
    global CONFIG, CHANNELS, MIN_DISK_SPACE_GB, MIN_DISK_SPACE, STATUS_PORT, RESYNC_INTERVAL
    global MAX_LIFETIME_RETRIES, CHANNEL_AUTO_DISABLE_AFTER, MEDIA_RECORD_TTL_DAYS, RETRY_DROP_LOG
    try:
        async with CONFIG_LOCK:
            async with aiofiles.open(CONFIG_FILE, "r", encoding="utf-8") as file:
                updated = json.loads(await file.read())
            validate_config(updated)
            manual_changed = updated.get("manual_downloads", {}) != CONFIG.get("manual_downloads", {})
            CONFIG = updated
        CHANNELS = CONFIG["channels"]
        MIN_DISK_SPACE_GB = CONFIG.get("min_disk_space_gb", 6)
        MIN_DISK_SPACE = MIN_DISK_SPACE_GB * 1024**3
        STATUS_PORT = CONFIG.get("status_port", 0)
        RESYNC_INTERVAL = CONFIG.get("resync_interval_minutes", 60)
        MAX_LIFETIME_RETRIES = CONFIG.get("max_lifetime_retries", 20)
        CHANNEL_AUTO_DISABLE_AFTER = CONFIG.get("channel_auto_disable_after", 5)
        MEDIA_RECORD_TTL_DAYS = CONFIG.get("media_record_ttl_days", 90)
        RETRY_DROP_LOG = CONFIG.get("retry_drop_log", True)
        apply_channel_overrides()
        await resolve_channels()
        if manual_changed:
            await process_manual_downloads()
        logger.info("[CONFIG] Reloaded")
    except Exception as error:
        logger.error("[CONFIG] Reload failed: %s", error)
        raise


async def config_watcher():
    try:
        last_mtime = CONFIG_FILE.stat().st_mtime_ns
    except OSError:
        last_mtime = 0
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(10)
            mtime = CONFIG_FILE.stat().st_mtime_ns
            if mtime != last_mtime:
                last_mtime = mtime
                await reload_config()
        except asyncio.CancelledError:
            break
        except Exception as error:
            logger.error("[CONFIG WATCHER] %s", error)


async def remove_manual_download(channel_id, message_id):
    entries = CONFIG.get("manual_downloads", {}).get(str(channel_id), [])
    normalized = [item for item in entries if str(item) != str(message_id)]
    if len(normalized) != len(entries):
        CONFIG["manual_downloads"][str(channel_id)] = normalized
        await save_config()


async def process_manual_downloads():
    for raw_channel_id, message_ids in CONFIG.get("manual_downloads", {}).items():
        try:
            entity = await client.get_entity(parse_channel_id(raw_channel_id))
            entity_id = str(utils.get_peer_id(entity))
            title = getattr(entity, "title", entity_id)
            for message_id in message_ids:
                key = f"{entity_id}:{message_id}"
                if key in MANUAL_IN_PROGRESS:
                    continue
                message = await client.get_messages(entity, ids=int(message_id))
                if not message:
                    logger.warning("[MANUAL] Message %s not found in %s", message_id, raw_channel_id)
                    continue
                MANUAL_IN_PROGRESS.add(key)
                await priority_download_queue.put((message, title, entity_id, True, raw_channel_id))
        except Exception as error:
            logger.error("[MANUAL] Could not queue channel %s: %s", raw_channel_id, error)


async def download_media(message, paths, channel_id):
    if message.out or not (media_type := detect_media_type(message)) or await media_exists(channel_id, message.id):
        return
    if not download_allowed.is_set():
        logger.info("[PAUSED] Waiting for disk space: message %s", message.id)
        await download_allowed.wait()
    if shutdown_event.is_set():
        return
    entity = CHANNEL_OBJECT_MAP.get(channel_id)
    if not entity:
        return
    file_name = sanitize_filename((message.file.name if message.file else None) or
                                  f"{message.id}{(message.file.ext if message.file else '') or ''}")
    final_name = f"{message.id}_{file_name}"
    target_path = paths[media_type] / final_name
    temp_path = paths[media_type] / f"{final_name}.part"
    if target_path.exists():
        await add_media_record(channel_id, message.id, final_name, target_path.stat().st_size)
        return
    last_error = "Unknown error"
    for attempt in range(1, DOWNLOAD_RETRY + 1):
        if shutdown_event.is_set() or not client.is_connected():
            break
        try:
            temp_path.unlink(missing_ok=True)
            fresh = await client.get_messages(entity, ids=message.id)
            if not fresh:
                raise RuntimeError(f"Message {message.id} not found")
            await asyncio.wait_for(fresh.download_media(file=temp_path), timeout=DOWNLOAD_TIMEOUT)
            if not temp_path.exists() or temp_path.stat().st_size <= 0:
                raise RuntimeError("Download is missing or empty")
            size = temp_path.stat().st_size
            os.replace(temp_path, target_path)
            await add_media_record(channel_id, message.id, final_name, size)
            await db.execute("DELETE FROM failed_downloads WHERE channel_id=? AND message_id=?", (channel_id, message.id))
            await db.commit()
            logger.info("[DOWNLOAD OK] %s (%d bytes)", final_name, size)
            return
        except Exception as error:
            last_error = str(error)
            logger.warning("[DOWNLOAD ERROR] %s attempt %d/%d: %s", final_name, attempt, DOWNLOAD_RETRY, error)
        finally:
            if temp_path.exists() and temp_path.stat().st_size == 0:
                temp_path.unlink(missing_ok=True)
        await asyncio.sleep(min(5 * attempt, 5))
    if not shutdown_event.is_set():
        retry_count = await add_failed_download(channel_id, message.id, last_error)
        logger.error("[FAILED] %s after %d attempts (lifetime %d)", final_name, DOWNLOAD_RETRY, retry_count)
        if MAX_LIFETIME_RETRIES > 0 and retry_count >= MAX_LIFETIME_RETRIES:
            logger.error("[DROP] %s exceeded %d lifetime retries", final_name, MAX_LIFETIME_RETRIES)
            if RETRY_DROP_LOG:
                await append_metadata(LOG_DIR / "dropped_downloads.jsonl", {
                    "timestamp": datetime.now(timezone.utc).isoformat(), "channel_id": channel_id,
                    "message_id": message.id, "file_name": final_name, "error": last_error,
                    "retry_count": retry_count,
                })
            await db.execute("DELETE FROM failed_downloads WHERE channel_id=? AND message_id=?", (channel_id, message.id))
            await db.commit()


async def process_message(message, entity_title, channel_id):
    paths = get_channel_paths(entity_title)
    sender = await message.get_sender()
    first = getattr(sender, "first_name", "") or "" if sender else ""
    last = getattr(sender, "last_name", "") or "" if sender else ""
    sender_name = f"{first} {last}".strip() or "Unknown"
    username = getattr(sender, "username", "") or "" if sender else ""
    timestamp = message.date.astimezone(MY_TZ).strftime("%Y-%m-%d %I:%M:%S %p")
    if message.message and not await text_logged(channel_id, message.id):
        media_name = None
        media_size = None
        media_info = ""
        if message.media and message.file:
            media_name = sanitize_filename(message.file.name or f"{message.id}{message.file.ext or ''}")
            media_size = getattr(message.file, "size", 0) or 0
            media_info = f"\n\n[Media]\nFilename: {media_name}\nSize: {media_size / 1024**2:.3f} MB"
        text = (f"[{timestamp}]\nSender Name: {sender_name}\nUsername: {username}\nMessage ID: {message.id}"
                f"{media_info}\n\n{message.message}\n{'-' * 50}\n")
        await append_message_log(paths["messages"], text)
        await append_metadata(paths["metadata"], {"timestamp": timestamp, "message_id": message.id,
            "sender_name": sender_name, "username": username, "text": message.message,
            "media_filename": media_name, "media_size_bytes": media_size})
        await add_logged_message(channel_id, message.id)
    if message.media:
        await download_media(message, paths, channel_id)
    await update_last_message_id(channel_id, message.id)


async def disk_monitor():
    last_warn_time = 0.0
    while not shutdown_event.is_set():
        try:
            free = shutil.disk_usage(BASE_DIR).free
            if free < MIN_DISK_SPACE:
                download_allowed.clear()
                now = asyncio.get_running_loop().time()
                if now - last_warn_time > 300:
                    logger.warning("[DISK] Critical: %.2f GB free (threshold %s GB). Downloads paused automatically.",
                                   free / 1024**3, MIN_DISK_SPACE_GB)
                    last_warn_time = now
            elif not download_allowed.is_set():
                logger.info("[DISK] Space recovered: %.2f GB free. Resuming downloads.", free / 1024**3)
                download_allowed.set()
        except asyncio.CancelledError:
            break
        except Exception as error:
            logger.error("[DISK MONITOR] %s", error)
        await asyncio.sleep(30)


async def worker(worker_id):
    global worker_busy_count
    logger.info("[WORKER %d] Started", worker_id)
    while not (shutdown_event.is_set() and download_queue.empty() and priority_download_queue.empty()):
        queue_used = None
        task = None
        item = None
        try:
            try:
                item = priority_download_queue.get_nowait()
                queue_used = priority_download_queue
            except asyncio.QueueEmpty:
                item = await asyncio.wait_for(download_queue.get(), timeout=1)
                queue_used = download_queue
            message, title, channel_id, is_manual, raw_channel_id = item
            worker_busy_count += 1
            task = asyncio.create_task(process_message(message, title, channel_id))
            active_tasks.add(task)
            await task
            if is_manual:
                await remove_manual_download(raw_channel_id, message.id)
                MANUAL_IN_PROGRESS.discard(f"{channel_id}:{message.id}")
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            break
        except Exception:
            logger.exception("[WORKER %d]", worker_id)
        finally:
            if item is not None:
                worker_busy_count = max(0, worker_busy_count - 1)
            if task:
                active_tasks.discard(task)
            if queue_used:
                queue_used.task_done()
    logger.info("[WORKER %d] Exited", worker_id)


@client.on(events.NewMessage)
async def new_message_handler(event):
    channel_id = str(utils.get_peer_id(event.message.peer_id))
    if channel_id not in MONITORED_CHANNEL_IDS:
        return
    try:
        await download_queue.put((event.message, CHANNEL_ENTITY_MAP[channel_id], channel_id, False, None))
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("[NEW MESSAGE] Could not queue message %s", event.message.id)


async def initial_sync():
    active_syncs = []
    try:
        for entity_id, entity in CHANNEL_OBJECT_MAP.items():
            last_id = await get_last_message_id(entity_id)
            active_syncs.append({"entity_id": entity_id, "title": CHANNEL_ENTITY_MAP[entity_id],
                "iterator": client.iter_messages(entity, min_id=max(0, last_id - 5), reverse=True).__aiter__(), "count": 0})
        while active_syncs and not shutdown_event.is_set() and client.is_connected():
            for sync in list(active_syncs):
                exhausted = False
                for _ in range(15):
                    try:
                        message = await sync["iterator"].__anext__()
                        await download_queue.put((message, sync["title"], sync["entity_id"], False, None))
                        sync["count"] += 1
                    except StopAsyncIteration:
                        exhausted = True
                        break
                    except Exception as error:
                        logger.error("[SYNC] %s: %s", sync["title"], error)
                        exhausted = True
                        break
                if exhausted:
                    logger.info("[SYNC DONE] %s queued %d", sync["title"], sync["count"])
                    active_syncs.remove(sync)
            await asyncio.sleep(0.01)
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("[SYNC CRITICAL ERROR]")


async def cleanup_part_files():
    count = 0
    for part in BASE_DIR.rglob("*.part"):
        try:
            part.unlink()
            count += 1
        except Exception as error:
            logger.warning("[STARTUP CLEANUP] Could not delete %s: %s", part, error)
    if count:
        logger.info("[STARTUP CLEANUP] Removed %d stale .part files.", count)


async def db_cleanup():
    if MEDIA_RECORD_TTL_DAYS > 0:
        cutoff = (datetime.now(timezone.utc) - timedelta(days=MEDIA_RECORD_TTL_DAYS)).isoformat()
        cur = await db.execute("DELETE FROM downloaded_media WHERE downloaded_at < ?", (cutoff,))
        pruned = cur.rowcount
        await db.commit()
        if pruned:
            logger.info("[DB CLEANUP] Pruned %d expired media records (TTL=%dd).", pruned, MEDIA_RECORD_TTL_DAYS)
    await db.execute("VACUUM")
    await db.commit()
    logger.info("[DB CLEANUP] VACUUM done.")


async def periodic_resync():
    if RESYNC_INTERVAL <= 0:
        return
    while not shutdown_event.is_set():
        try:
            await asyncio.sleep(RESYNC_INTERVAL * 60)
        except asyncio.CancelledError:
            break
        if not shutdown_event.is_set():
            logger.info("[RESYNC] Starting periodic re-sync...")
            await initial_sync()
            logger.info("[RESYNC] Periodic re-sync complete.")


async def connection_supervisor():
    global sync_task
    backoff = [5, 10, 30, 60, 120, 300]
    attempt = 0
    while not shutdown_event.is_set():
        try:
            logger.info("[SUPERVISOR] Monitoring connection...")
            await client.run_until_disconnected()
            if shutdown_event.is_set():
                break
            logger.warning("[SUPERVISOR] Disconnected unexpectedly.")
        except asyncio.CancelledError:
            break
        except Exception as error:
            logger.error("[SUPERVISOR] Connection error: %s", error)
        delay = backoff[min(attempt, len(backoff) - 1)]
        logger.info("[SUPERVISOR] Reconnecting in %ds (attempt %d)...", delay, attempt + 1)
        await asyncio.sleep(delay)
        attempt += 1
        try:
            await client.connect()
            await resolve_channels()
            if sync_task and not sync_task.done():
                sync_task.cancel()
                await asyncio.gather(sync_task, return_exceptions=True)
            sync_task = asyncio.create_task(initial_sync())
            attempt = 0
            logger.info("[SUPERVISOR] Reconnected and re-syncing.")
        except Exception as error:
            logger.error("[SUPERVISOR] Reconnect attempt failed: %s", error)


async def status_server():
    try:
        from aiohttp import web
    except ImportError:
        logger.error("[STATUS] aiohttp is required when status_port is enabled")
        return

    async def status(_request):
        free = shutil.disk_usage(BASE_DIR).free / 1024**3
        async with db.execute("SELECT COUNT(*) FROM failed_downloads") as cur:
            failed = await cur.fetchone()
        channels = {}
        for channel_id, title in CHANNEL_ENTITY_MAP.items():
            channels[channel_id] = {"title": title, "enabled": channel_id in MONITORED_CHANNEL_IDS,
                                    "fail_count": await get_channel_fail_count(channel_id)}
        db_size = Path(DB_PATH).stat().st_size / 1024**2 if Path(DB_PATH).exists() else 0
        return web.json_response({"uptime_seconds": int((datetime.now(timezone.utc) - START_TIME).total_seconds()),
            "connected": client.is_connected(), "downloads_paused": not download_allowed.is_set(),
            "workers": {"total": len(worker_tasks), "busy": worker_busy_count},
            "queue": {"normal": download_queue.qsize(), "priority": priority_download_queue.qsize()},
            "disk_free_gb": round(free, 2), "failed_downloads_count": failed[0],
            "db_size_mb": round(db_size, 2), "channels": channels})

    async def logs(request):
        try:
            lines = max(1, min(int(request.query.get("lines", "100")), 5000))
        except ValueError:
            lines = 100
        try:
            content = (LOG_DIR / "app.log").read_text(encoding="utf-8").splitlines()
            return web.Response(text="\n".join(content[-lines:]) + ("\n" if content else ""), content_type="text/plain")
        except FileNotFoundError:
            return web.Response(text="", content_type="text/plain")

    async def reload_endpoint(_request):
        await reload_config()
        return web.json_response({"status": "reloaded"})

    async def cleanup_endpoint(_request):
        await db_cleanup()
        return web.json_response({"status": "cleaned"})

    async def set_channel(request):
        channel_id, enabled = request.match_info["id"], request.match_info["action"] == "enable"
        CONFIG.setdefault("channel_overrides", {}).setdefault(channel_id, {})["turnon"] = enabled
        await save_config()
        if enabled:
            await resolve_channels()
        else:
            apply_channel_overrides()
        return web.json_response({"status": "enabled" if enabled else "disabled", "channel_id": channel_id})

    app = web.Application()
    app.router.add_get("/health", lambda _request: web.json_response({"status": "ok"}))
    app.router.add_get("/status", status)
    app.router.add_get("/logs", logs)
    app.router.add_post("/reload", reload_endpoint)
    app.router.add_post("/db/cleanup", cleanup_endpoint)
    app.router.add_post("/channel/{id}/{action:enable|disable}", set_channel)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.TCPSite(runner, "0.0.0.0", STATUS_PORT)
    await site.start()
    logger.info("[STATUS] Listening on port %d", STATUS_PORT)
    try:
        await shutdown_event.wait()
    finally:
        await runner.cleanup()


async def shutdown():
    async with shutdown_lock:
        if shutdown_event.is_set():
            return
        shutdown_event.set()
        logger.info("[SHUTDOWN] Graceful shutdown started")
        for task in (config_task, sync_task, disk_task, resync_task, status_task, supervisor_task):
            if task and not task.done():
                task.cancel()
        if client.is_connected():
            try:
                await asyncio.wait_for(client.disconnect(), timeout=10)
            except Exception:
                logger.exception("[DISCONNECT ERROR]")
        for queue in (download_queue, priority_download_queue):
            while not queue.empty():
                try:
                    queue.get_nowait()
                    queue.task_done()
                except asyncio.QueueEmpty:
                    break
        if active_tasks:
            await asyncio.gather(*active_tasks, return_exceptions=True)
        for task in worker_tasks:
            task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)
        if db:
            await db.commit()
            await db.close()
        logger.info("[SHUTDOWN COMPLETE]")


async def main():
    global config_task, sync_task, disk_task, resync_task, status_task, supervisor_task, download_allowed
    validate_config(CONFIG)
    download_allowed = asyncio.Event()
    download_allowed.set()
    await init_db()
    await db_cleanup()
    await cleanup_part_files()
    await client.start(phone=PHONE)
    me = await client.get_me()
    logger.info("[CONNECTED] Logged in as %s", me.first_name)
    await resolve_channels()
    await process_manual_downloads()
    for index in range(PARALLEL_DOWNLOADS):
        worker_tasks.append(asyncio.create_task(worker(index + 1)))
    disk_task = asyncio.create_task(disk_monitor())
    sync_task = asyncio.create_task(initial_sync())
    config_task = asyncio.create_task(config_watcher())
    supervisor_task = asyncio.create_task(connection_supervisor())
    resync_task = asyncio.create_task(periodic_resync())
    if STATUS_PORT > 0:
        status_task = asyncio.create_task(status_server())
    logger.info("[READY] Workers: %d, Status port: %s", PARALLEL_DOWNLOADS, STATUS_PORT or "disabled")
    while not shutdown_event.is_set():
        await asyncio.sleep(5)


def handle_exit(*_args):
    loop.call_soon_threadsafe(lambda: asyncio.create_task(shutdown()))


if __name__ == "__main__":
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    signal.signal(signal.SIGINT, handle_exit)
    if hasattr(signal, "SIGTERM"):
        signal.signal(signal.SIGTERM, handle_exit)
    try:
        loop.run_until_complete(main())
    finally:
        loop.run_until_complete(shutdown())
        loop.close()
