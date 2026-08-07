# Telegram Backup Downloader v5

![Docker Image](https://img.shields.io/docker/v/hoelee/telegram-backup-downloader)
![Platforms](https://img.shields.io/badge/platforms-linux%2Famd64%2C%20linux%2Farm64-blue)
[![Docker Hub](https://img.shields.io/badge/Docker%20Hub-hoelee%2Ftelegram--backup--downloader-2496ED?logo=docker&logoColor=white)](https://hub.docker.com/r/hoelee/telegram-backup-downloader)
[![GitHub](https://img.shields.io/badge/GitHub-hoelee%2Ftelegram--backup--downloader-181717?logo=github)](https://github.com/hoelee/telegram-backup-downloader)

Downloads text, photos, videos, and documents from configured Telegram channels. Message text is stored in `messages.txt` and JSONL metadata; media is sorted into per-channel folders. SQLite tracks completed work, failed downloads, and sync progress.

> **Platform support:** Docker images are built for `linux/amd64` and `linux/arm64` only. Legacy 32-bit ARM (`arm/v7`) is not supported because the `cryptg` Telethon extension has no prebuilt wheel for it. This covers servers, desktops, NAS devices, Raspberry Pi 4/5, and Apple Silicon via emulation.

## Prerequisites

- Python 3.12 or Docker
- A Telegram API ID and API hash from [my.telegram.org](https://my.telegram.org)
- A Telegram account that can access every configured channel

## Local Quick Start

1. Create a virtual environment and install dependencies: `python -m venv .venv` then `.venv\Scripts\pip install -r requirements.txt` on Windows, or `.venv/bin/pip install -r requirements.txt` on Linux/macOS.
2. Copy `config.example.json` to `config.json` and enter your Telegram values.
3. Run `python downloadv5.py`. The first run requests Telegram login verification if no session file exists.

Backups are written to `channels/`; logs are written to `logs/app.log`. Stop gracefully with Ctrl+C.

## Docker

### Option A: Docker Compose (recommended)

The example `docker-compose.yml` uses the pre-built image from [Docker Hub](https://hub.docker.com/r/hoelee/telegram-backup-downloader) and supports `linux/amd64` and `linux/arm64`.

**Before running**, create the folders and config file so the container can write to them. The compose file mounts local paths for config, session, data, channels, and logs:

```bash
mkdir -p data channels logs
cp config.example.json config.json
# edit config.json with your Telegram values
```

> On Linux, if the container runs as a non-root user, make sure the current user has read+write access to these folders (the example compose runs as `root`).

Then start the stack:

```bash
docker compose up -d
docker compose logs -f telegram-backup
```

This is the `docker-compose.yml`:

```yaml
version: "3.9"

services:
  telegram-backup:
    image: hoelee/telegram-backup-downloader:latest
    container_name: telegram-backup
    restart: unless-stopped
    user: root
    volumes:
      - ./config.json:/app/config.json
      - ./telegram_session.session:/app/telegram_session.session
      - ./data:/app/data
      - ./channels:/app/channels
      - ./logs:/app/logs
    ports:
      - "8080:8080"
    environment:
      - TZ=Asia/Kuala_Lumpur
    healthcheck:
      test: ["CMD", "python", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8080/health', timeout=3)"]
      interval: 30s
      timeout: 5s
      retries: 3
      start_period: 15s
```

Set `status_port` to `8080` in `config.json` to use the health check and expose the status API.

### Option B: docker run

```bash
mkdir -p data channels logs
cp config.example.json config.json

docker run -d \
  --name telegram-backup \
  --restart unless-stopped \
  -v $(pwd)/config.json:/app/config.json \
  -v $(pwd)/telegram_session.session:/app/telegram_session.session \
  -v $(pwd)/data:/app/data \
  -v $(pwd)/channels:/app/channels \
  -v $(pwd)/logs:/app/logs \
  -p 8080:8080 \
  -e TZ=Asia/Kuala_Lumpur \
  hoelee/telegram-backup-downloader:latest
```

Follow logs with `docker logs -f telegram-backup`.

### Option C: Build from source

```bash
cp config.example.json config.json
docker compose -f docker-compose.yml up -d --build
docker compose logs -f telegram-backup
```

## Configuration

| Key | Required | Default | Description |
|---|---:|---:|---|
| `api_id` | Yes | - | Numeric Telegram API ID. |
| `api_hash` | Yes | - | Telegram API hash. |
| `phone_number` | Yes | - | Account phone number in international format. |
| `session_name` | Yes | - | Telethon session basename. |
| `channels` | Yes | - | Channel usernames or numeric peer IDs. |
| `parallel_downloads` | No | `3` | Number of media workers. |
| `download_timeout_seconds` | No | `600` | Per-download timeout. |
| `download_retry_count` | No | `3` | Attempts made in one processing pass. |
| `max_lifetime_retries` | No | `20` | Total failed passes before an item is dropped; `0` disables the cap. |
| `queue_max_size` | No | `5000` | Normal download queue capacity. |
| `min_disk_space_gb` | No | `6` | Pause media downloads below this free space. |
| `channel_auto_disable_after` | No | `5` | Consecutive resolution failures before disabling a channel; `0` disables this. |
| `media_record_ttl_days` | No | `90` | Retention for completed media DB records; `0` disables pruning. |
| `retry_drop_log` | No | `true` | Write discarded downloads to `logs/dropped_downloads.jsonl`. |
| `resync_interval_minutes` | No | `60` | Periodic backfill interval; `0` disables it. |
| `status_port` | No | `0` | HTTP port; `0` disables the server. |
| `db_path` | No | `telegram_state.db` | SQLite state database path. |
| `channel_overrides` | No | `{}` | Per-channel controls. Only `turnon` is supported. Use `turnon: false` to skip a channel without removing it from `channels`. Example: `{"-100321012345":{"turnon":false}}`. Change is detected by config watcher — no restart needed. |
| `channel_last_message_id_overrides` | No | `{}` | One-time per-channel sync cursor overrides. With `updateonce: true`, sets the channel's SQLite `last_message_id` to the specified value, then automatically changes `updateonce` to `false` in `config.json`. Use numeric Telegram peer IDs that are included in `channels`. |
| `manual_downloads` | No | `{}` | Message IDs to prioritize, keyed by channel ID. Useful to force-retry specific messages. Example: `{"-1001":[42,43,44]}`. Change is detected by config watcher — no restart needed. |

`config.json` must be mounted read-write. The program writes back to it directly — for example, when `channel_last_message_id_overrides` runs with `updateonce: true`, the downloader updates the database and then flips `updateonce` to `false` in the same file. A read-only mount (`:ro`) would break this and any other live config update.

`config.json` is watched every 10 seconds. Changes to channels, overrides, cursor overrides, and manual downloads are applied without restarting. Do not set `status_port`, `db_path`, worker count, or API credentials expecting a live process to rebind/recreate those resources; restart after changing them.

### One-Time Sync Cursor Override

Use `channel_last_message_id_overrides` to resume a channel from a known message ID or re-download messages after a cursor correction. The channel must be present in `channels`. When the running downloader detects `updateonce: true`, it updates the database cursor and writes `updateonce: false` only after the database change succeeds.

```json
"channel_last_message_id_overrides": {
  "-1004295572354": {
    "updateonce": true,
    "last_message_id": 21321
  }
}
```

The next sync starts from a small overlap before that cursor, so existing message and media records prevent duplicate output.

## HTTP API

The API listens on `0.0.0.0:<status_port>` with no authentication. Only expose it on a trusted network.

| Method | Endpoint | Description |
|---|---|---|
| GET | `/health` | Returns `{"status":"ok"}`. |
| GET | `/status` | Connection, queue, workers, disk, database, failure, and channel status. |
| GET | `/logs?lines=100` | Last 1-5000 lines of `logs/app.log`. |
| POST | `/reload` | Reloads `config.json`. |
| POST | `/db/cleanup` | Prunes expired records and vacuums SQLite. |
| POST | `/channel/{id}/enable` | Enables a channel override and resolves channels. |
| POST | `/channel/{id}/disable` | Disables a channel override. |

## Troubleshooting

- **Login problems:** delete only the session file if you intentionally need to authenticate again, then restart.
- **No downloads:** verify the account belongs to or can view the channel, and confirm the channel override is enabled.
- **Downloads paused:** inspect `/status` or the log for disk-space warnings. Downloads resume automatically after space is recovered.
- **Repeated failed media:** inspect `logs/app.log`; items reaching the lifetime cap are recorded in `logs/dropped_downloads.jsonl` when enabled.
- **Port unavailable:** set `status_port` to another free port, update compose port mapping if needed, and restart.
- **Database location in Docker:** set `db_path` to `data/telegram_state.db` if you want the database in the mounted `data` directory.
