import asyncio
import json
import logging
import os
import re
import signal
import sys
import shutil
from datetime import datetime, timezone, timedelta
from logging.handlers import RotatingFileHandler
from pathlib import Path

import aiofiles
import aiosqlite

from telethon import TelegramClient, events, utils
from telethon.errors import (
    FloodWaitError,
    FileReferenceExpiredError,
    RPCError,
)

# =========================
# LOAD CONFIG & CONSTANTS
# =========================

with open("config.json", "r", encoding="utf-8") as f:
    CONFIG = json.load(f)

API_ID = CONFIG["api_id"]
API_HASH = CONFIG["api_hash"]
PHONE = CONFIG["phone_number"]
SESSION_NAME = CONFIG["session_name"]

CHANNELS = CONFIG["channels"]

PARALLEL_DOWNLOADS = CONFIG.get("parallel_downloads", 3)
DOWNLOAD_TIMEOUT = CONFIG.get("download_timeout_seconds", 600)
DOWNLOAD_RETRY = CONFIG.get("download_retry_count", 3)
QUEUE_MAX_SIZE = CONFIG.get("queue_max_size", 5000)

MIN_DISK_SPACE = 6 * 1024 * 1024 * 1024  # 6GB in bytes

BASE_DIR = Path("channels")
LOG_DIR = Path("logs")

BASE_DIR.mkdir(exist_ok=True)
LOG_DIR.mkdir(exist_ok=True)

# =========================
# LOGGING
# =========================

logger = logging.getLogger("telegram_archiver")
logger.setLevel(logging.INFO)

formatter = logging.Formatter(
    "%(asctime)s [%(levelname)s] %(message)s"
)

file_handler = RotatingFileHandler(
    LOG_DIR / "app.log",
    maxBytes=10 * 1024 * 1024,
    backupCount=10,
    encoding="utf-8"
)

file_handler.setFormatter(formatter)

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(formatter)

logger.addHandler(file_handler)
logger.addHandler(console_handler)

# =========================
# SQLITE
# =========================

DB_FILE = "telegram_state.db"

CREATE_TABLES_SQL = """
CREATE TABLE IF NOT EXISTS channel_state (
    channel_id TEXT PRIMARY KEY,
    last_message_id INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS downloaded_media (
    channel_id TEXT,
    message_id INTEGER,
    file_name TEXT,
    file_size INTEGER,
    PRIMARY KEY(channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS logged_messages (
    channel_id TEXT,
    message_id INTEGER,
    PRIMARY KEY(channel_id, message_id)
);

CREATE TABLE IF NOT EXISTS failed_downloads (
    channel_id TEXT,
    message_id INTEGER,
    last_error TEXT,
    retry_count INTEGER,
    PRIMARY KEY(channel_id,message_id)
);
"""

db = None

# =========================
# GLOBALS & SAFEGUARDS
# =========================

INVALID_CHARS = r'[<>:"/\\\\|?*]'

client = TelegramClient(
    SESSION_NAME,
    API_ID,
    API_HASH,
    auto_reconnect=True,
    connection_retries=None,
    retry_delay=5,
)

download_queue = asyncio.Queue(maxsize=QUEUE_MAX_SIZE)
priority_download_queue = asyncio.Queue()

shutdown_event = asyncio.Event()
shutdown_lock = asyncio.Lock()
CONFIG_LOCK = asyncio.Lock()

# Will be initialized inside main() to tie it to the correct event loop
download_allowed = None 

CONFIG_FILE = "config.json"
MANUAL_IN_PROGRESS = set()
active_tasks = set()
worker_tasks = []

config_task = None
sync_task = None
disk_task = None

MONITORED_CHANNEL_IDS = set()
CHANNEL_ENTITY_MAP = {}
CHANNEL_OBJECT_MAP = {}

MY_TZ = timezone(timedelta(hours=8))

# =========================
# HELPERS
# =========================

def sanitize_filename(name):
    name = re.sub(INVALID_CHARS, "_", name)
    name = name.strip().rstrip(".")
    return name[:200]

def parse_channel_id(raw_id):
    if isinstance(raw_id, str) and raw_id.lstrip('-').isdigit():
        return int(raw_id)
    return raw_id

# =========================
# DATABASE OPERATIONS
# =========================

async def init_db():
    global db
    db = await aiosqlite.connect(DB_FILE)
    await db.execute("PRAGMA journal_mode=WAL;")
    await db.execute("PRAGMA synchronous=NORMAL;")
    await db.executescript(CREATE_TABLES_SQL)
    await db.commit()
    
async def add_logged_message(channel_id, message_id):
    await db.execute("INSERT OR IGNORE INTO logged_messages VALUES (?,?)", (channel_id, message_id))
    await db.commit()
    
async def resolve_channels():
    MONITORED_CHANNEL_IDS.clear()
    CHANNEL_ENTITY_MAP.clear()
    CHANNEL_OBJECT_MAP.clear()

    for channel in CHANNELS:
        try:
            parsed_channel = parse_channel_id(channel)
            entity = await client.get_entity(parsed_channel)
            entity_id = str(utils.get_peer_id(entity))
            entity_title = getattr(entity, "title", entity_id)

            MONITORED_CHANNEL_IDS.add(entity_id)
            CHANNEL_ENTITY_MAP[entity_id] = entity_title
            CHANNEL_OBJECT_MAP[entity_id] = entity
        except Exception as e:
            logger.error(f"[RESOLVE ERROR] Failed to resolve channel '{channel}': {e}")
            
async def get_last_message_id(channel_id):
    async with db.execute("SELECT last_message_id FROM channel_state WHERE channel_id=?", (channel_id,)) as cursor:
        row = await cursor.fetchone()
        return row[0] if row else 0

async def update_last_message_id(channel_id, message_id):
    await db.execute(
        """
        INSERT INTO channel_state(channel_id, last_message_id)
        VALUES (?, ?) ON CONFLICT(channel_id)
        DO UPDATE SET last_message_id = MAX(last_message_id, excluded.last_message_id)
        """, (channel_id, message_id)
    )
    await db.commit()

async def media_exists(channel_id, message_id):
    async with db.execute("SELECT 1 FROM downloaded_media WHERE channel_id=? AND message_id=?", (channel_id, message_id)) as cursor:
        return await cursor.fetchone() is not None

async def add_media_record(channel_id, message_id, file_name, file_size):
    await db.execute(
        "INSERT OR IGNORE INTO downloaded_media (channel_id, message_id, file_name, file_size) VALUES (?, ?, ?, ?)",
        (channel_id, message_id, file_name, file_size)
    )
    await db.commit()
    
async def add_failed_download(channel_id, message_id, error):
    await db.execute(
        """
        INSERT INTO failed_downloads (channel_id, message_id, last_error, retry_count)
        VALUES (?, ?, ?, 1) ON CONFLICT(channel_id,message_id)
        DO UPDATE SET retry_count = retry_count + 1, last_error = excluded.last_error
        """, (channel_id, message_id, error[:1000])
    )
    await db.commit()
    
async def text_logged(channel_id, message_id):
    async with db.execute("SELECT 1 FROM logged_messages WHERE channel_id=? AND message_id=?", (channel_id, message_id)) as cursor:
        return await cursor.fetchone() is not None

async def save_config():
    async with CONFIG_LOCK:
        async with aiofiles.open(CONFIG_FILE, "w", encoding="utf-8") as f:
            await f.write(json.dumps(CONFIG, indent=2, ensure_ascii=False))

def get_channel_paths(channel_name):
    safe_name = sanitize_filename(channel_name)
    root = BASE_DIR / safe_name
    folders = {
        "root": root,
        "photo": root / "photo",
        "video": root / "video",
        "document": root / "document",
        "messages": root / "messages.txt",
        "metadata": root / "metadata.jsonl",
    }
    for p in folders.values():
        if isinstance(p, Path) and p.suffix == "":
            p.mkdir(parents=True, exist_ok=True)
    return folders

async def append_message_log(file_path, text):
    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(text)

async def append_metadata(file_path, data):
    async with aiofiles.open(file_path, "a", encoding="utf-8") as f:
        await f.write(json.dumps(data, ensure_ascii=False) + "\n")

def detect_media_type(message):
    if message.photo: return "photo"
    if message.video: return "video"
    if message.document: return "document"
    return None

# =========================
# CORE PROCESSING
# =========================

async def download_media(message, paths, channel_id):
    if message.out: return
    media_type = detect_media_type(message)
    if not media_type: return
    if await media_exists(channel_id, message.id): return

    # === DISK SPACE SAFEGUARD ===
    if not download_allowed.is_set():
        logger.info(f"⏳ [PAUSED] Waiting for disk space to process media for message ID: {message.id}")
        await download_allowed.wait()
        if shutdown_event.is_set(): return
    # ============================

    entity = CHANNEL_OBJECT_MAP.get(channel_id)
    if not entity: return

    file_name = message.file.name if message.file else None
    if not file_name:
        ext = message.file.ext if message.file else ""
        file_name = f"{message.id}{ext}"

    file_name = sanitize_filename(file_name)
    final_name = f"{message.id}_{file_name}"
    target_dir = paths[media_type]

    temp_path = target_dir / f"{final_name}.part"
    target_path = target_dir / final_name

    if target_path.exists():
        size = target_path.stat().st_size
        await add_media_record(channel_id, message.id, final_name, size)
        return

    logger.info(f"[DOWNLOAD] {final_name}")
    success = False
    last_error = "Unknown"
    
    for attempt in range(1, DOWNLOAD_RETRY + 1):
        if shutdown_event.is_set() or not client.is_connected():
            break

        try:
            temp_path.unlink(missing_ok=True)
            fresh_message = await client.get_messages(entity=entity, ids=message.id)
            if not fresh_message:
                raise Exception(f"Message {message.id} not found")

            await asyncio.wait_for(fresh_message.download_media(file=temp_path), timeout=DOWNLOAD_TIMEOUT)

            if not temp_path.exists():
                raise Exception("Temp file missing")

            size = temp_path.stat().st_size
            if size <= 0:
                raise Exception("Downloaded file is 0 bytes")

            os.replace(temp_path, target_path)
            await add_media_record(channel_id, message.id, final_name, size)
            logger.info(f"[DOWNLOAD OK] {final_name} ({size} bytes)")
            success = True
            break

        except Exception as e:
            last_error = str(e)
            logger.exception(f"[DOWNLOAD ERROR] {final_name}: {e}")
        finally:
            if temp_path.exists():
                try:
                    if temp_path.stat().st_size == 0:
                        temp_path.unlink(missing_ok=True)
                except Exception:
                    pass

        await asyncio.sleep(min(5 * attempt, 5))

    if not success and not shutdown_event.is_set():
        await add_failed_download(channel_id, message.id, last_error)
        logger.error(f"[FAILED] {final_name} after {DOWNLOAD_RETRY} attempts")
    elif success:
        await db.execute("DELETE FROM failed_downloads WHERE channel_id=? AND message_id=?", (channel_id, message.id))
        await db.commit()

async def process_message(message, entity_title, channel_id):
    paths = get_channel_paths(entity_title)
    sender = await message.get_sender()
    sender_name = "Unknown"
    username = ""

    if sender:
        first = getattr(sender, "first_name", "") or ""
        last = getattr(sender, "last_name", "") or ""
        sender_name = f"{first} {last}".strip() or "Unknown"
        username = getattr(sender, "username", "") or ""

    timestamp = message.date.astimezone(MY_TZ).strftime("%Y-%m-%d %I:%M:%S %p")

    if message.message:
        if not await text_logged(channel_id, message.id):
            media_info = ""
            media_name = None
            media_size = None

            if message.media and message.file:
                media_name = sanitize_filename(message.file.name or f"{message.id}{message.file.ext or ''}")
                media_size = getattr(message.file, "size", 0) or 0
                media_size_mb = media_size / (1024 * 1024)
                media_info = f"\n\n[Media]\nFilename: {media_name}\nSize: {media_size_mb:.3f} MB"

            text_log = (
                f"[{timestamp}]\nSender Name: {sender_name}\nUsername: {username}\n"
                f"Message ID: {message.id}\n{media_info}\n\n{message.message}\n{'-'*50}\n"
            )

            await append_message_log(paths["messages"], text_log)
            await append_metadata(paths["metadata"], {
                "timestamp": timestamp, "message_id": message.id, "sender_name": sender_name,
                "username": username, "text": message.message, "media_filename": media_name, "media_size_bytes": media_size
            })
            await add_logged_message(channel_id, message.id)

    if message.media:
        await download_media(message, paths, channel_id)

    await update_last_message_id(channel_id, message.id)

# =========================
# BACKGROUND MONITORS
# =========================

async def disk_monitor():
    """Continuously monitors disk space and pauses downloads if below 6GB."""
    loop = asyncio.get_running_loop()
    
    while not shutdown_event.is_set():
        try:
            free_space = shutil.disk_usage(BASE_DIR).free
            
            if free_space < MIN_DISK_SPACE:
                download_allowed.clear()
                logger.warning(f"🚨 DISK SPACE CRITICAL: Only {free_space / 1024**3:.2f} GB left! (< 6GB)")
                
                print("\n" + "="*60)
                print("🛑 DOWNLOADS PAUSED DUE TO LOW DISK SPACE.")
                print(f"Current Free Space: {free_space / 1024**3:.2f} GB")
                print("Please free up storage space on the drive.")
                print("="*60 + "\n")
                
                # Asynchronously wait for terminal input without blocking the main event loop
                await loop.run_in_executor(None, input, "👉 Press [ENTER] here once space is cleared to re-check...\n")
                
                logger.info("Re-checking disk space...")
            else:
                if not download_allowed.is_set():
                    logger.info("✅ Disk space sufficient. Resuming operations.")
                    download_allowed.set()
                    
            await asyncio.sleep(5)
        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.error(f"[DISK MONITOR] Error: {e}")
            await asyncio.sleep(5)

# =========================
# WORKERS & QUEUES
# =========================

async def worker(worker_id):
    logger.info(f"[WORKER {worker_id}] Started")
    while True:
        if shutdown_event.is_set() and download_queue.empty() and priority_download_queue.empty():
            break

        task = None
        queue_used = None

        try:
            try:
                item = priority_download_queue.get_nowait()
                queue_used = priority_download_queue
            except asyncio.QueueEmpty:
                item = await asyncio.wait_for(download_queue.get(), timeout=1)
                queue_used = download_queue

            message, entity_title, channel_id, is_manual, raw_channel_id = item

            task = asyncio.create_task(process_message(message, entity_title, channel_id))
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
            logger.exception(f"[WORKER {worker_id}]")
        finally:
            if task:
                active_tasks.discard(task)
            if queue_used:
                queue_used.task_done()

    logger.info(f"[WORKER {worker_id}] Exited")

# (Omitted config watchers and specific round-robin code below for brevity, use your existing functions)
# ... [Insert reload_config, config_watcher, apply_channel_overrides, remove_manual_download, process_manual_downloads, initial_sync, new_message_handler as written previously] ...
# NOTE: Make sure to include all of them when running. I am keeping the response focused on the modifications and the main block below!

# =========================
# INITIAL SYNC (ROUND-ROBIN)
# =========================

async def initial_sync():
    try:
        active_syncs = []
        for entity_id, entity in CHANNEL_OBJECT_MAP.items():
            if shutdown_event.is_set() or not client.is_connected():
                return
            try:
                entity_title = CHANNEL_ENTITY_MAP[entity_id]
                last_message_id = await get_last_message_id(entity_id)
                min_id = max(0, last_message_id - 5)
                
                logger.info(f"[SYNC INIT] {entity_title} (last_id={last_message_id}, sync min_id={min_id})")
                iterator = client.iter_messages(entity, min_id=min_id, reverse=True).__aiter__()
                active_syncs.append({"entity_id": entity_id, "entity_title": entity_title, "iterator": iterator, "count": 0})
            except Exception:
                logger.exception(f"[SYNC INIT ERROR] Failed to initialize iterator for {entity_id}")

        chunk_size = 15
        while active_syncs and not shutdown_event.is_set():
            if not client.is_connected():
                break
                
            for sync in list(active_syncs):
                if shutdown_event.is_set() or not client.is_connected(): break
                exhausted = False
                for _ in range(chunk_size):
                    try:
                        message = await sync["iterator"].__anext__()
                        await download_queue.put((message, sync["entity_title"], sync["entity_id"], False, None))
                        sync["count"] += 1
                    except StopAsyncIteration:
                        exhausted = True
                        break
                    except Exception as e:
                        logger.error(f"[SYNC ITER ERROR] Channel {sync['entity_title']}: {e}")
                        exhausted = True
                        break
                
                if exhausted:
                    logger.info(f"[SYNC DONE] {sync['entity_title']} complete. Queued: {sync['count']}")
                    active_syncs.remove(sync)
                    
            await asyncio.sleep(0.01)
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("[SYNC CRITICAL ERROR]")

# =========================
# SHUTDOWN
# =========================

async def shutdown():
    async with shutdown_lock:
        if shutdown_event.is_set(): return
        shutdown_event.set()
        logger.info("[SHUTDOWN] Graceful shutdown started")

        global config_task, sync_task, disk_task
        if config_task and not config_task.done(): config_task.cancel()
        if sync_task and not sync_task.done(): sync_task.cancel()
        if disk_task and not disk_task.done(): disk_task.cancel()

        try:
            if client.is_connected(): await asyncio.wait_for(client.disconnect(), timeout=10)
        except Exception:
            logger.exception("[DISCONNECT ERROR]")

        while not download_queue.empty():
            try: download_queue.get_nowait(); download_queue.task_done()
            except asyncio.QueueEmpty: break
                
        while not priority_download_queue.empty():
            try: priority_download_queue.get_nowait(); priority_download_queue.task_done()
            except asyncio.QueueEmpty: break
        
        if active_tasks: await asyncio.gather(*active_tasks, return_exceptions=True)
        for task in worker_tasks: task.cancel()
        await asyncio.gather(*worker_tasks, return_exceptions=True)

        if db:
            await db.commit()
            await db.close()

        logger.info("[SHUTDOWN COMPLETE]")

# =========================
# MAIN
# =========================

async def main():
    global config_task, sync_task, disk_task, download_allowed
    
    # Initialize the global event tracker inside the running loop
    download_allowed = asyncio.Event()
    download_allowed.set()

    await init_db()
    await client.start(phone=PHONE)
    me = await client.get_me()
    logger.info(f"[CONNECTED] {me.first_name}")
    
    await resolve_channels()
    
    for i in range(PARALLEL_DOWNLOADS):
        worker_tasks.append(asyncio.create_task(worker(i + 1)))
    
    logger.info("[LISTENER ACTIVE]")
    
    # Track background tasks
    disk_task = asyncio.create_task(disk_monitor())
    sync_task = asyncio.create_task(initial_sync())
    
    listener_task = asyncio.create_task(client.run_until_disconnected())
    await listener_task
    
    while not shutdown_event.is_set():
        await asyncio.sleep(5)

# =========================
# SIGNAL & ENTRY
# =========================

def handle_exit(*args):
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