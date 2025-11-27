#!/usr/bin/env python3
import os
import boto3
import logging
from datetime import datetime
import subprocess
import tempfile
import json
import urllib.request
import urllib.error

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(message)s")
logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  ENV vars (S3 + Postgres)
# ─────────────────────────────────────────────
S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL")
S3_BUCKET = os.getenv("S3_BUCKET_NAME")
S3_PREFIX = os.getenv("S3_BUCKET_PREFIX", "TechHelp_backups")
S3_REGION = os.getenv("S3_REGION", "ru-1")
S3_ACCESS = os.getenv("S3_ACCESS_KEY")
S3_SECRET = os.getenv("S3_SECRET_KEY")

PG_HOST = os.getenv("POSTGRES_HOST", "supportbot-postgres")
PG_DB = os.getenv("POSTGRES_DB")
PG_USER = os.getenv("POSTGRES_USER")
PG_PASS = os.getenv("POSTGRES_PASSWORD")

BACKUP_RETENTION_DAYS = int(os.getenv("BACKUP_RETENTION_DAYS", 30))

# ─────────────────────────────────────────────
#  Telegram уведомления
# ─────────────────────────────────────────────
# MAIN_GROUP_ID — ID главной группы
MAIN_GROUP_ID_RAW = os.getenv("MAIN_GROUP_ID")

# Токен для отправки уведомлений:
TELEGRAM_BOT_TOKEN = (
    os.getenv("BACKUP_BOT_TOKEN")
    or os.getenv("BOT_TOKEN")
    or os.getenv("TELEGRAM_BOT_TOKEN")
)


def send_telegram_message(text: str) -> None:
    """
    Отправка сообщения в Telegram-группу MAIN_GROUP_ID.
    Если переменные не заданы — просто логируем и выходим.
    """
    if not TELEGRAM_BOT_TOKEN:
        logger.warning("TELEGRAM_BOT_TOKEN не задан, уведомление не отправлено")
        return

    if not MAIN_GROUP_ID_RAW:
        logger.warning("MAIN_GROUP_ID не задан, уведомление не отправлено")
        return

    try:
        chat_id = int(MAIN_GROUP_ID_RAW)
    except ValueError:
        logger.error(f"Некорректный MAIN_GROUP_ID: {MAIN_GROUP_ID_RAW}")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": chat_id,
        "text": text,
        "parse_mode": "HTML",
        "disable_notification": True,
        # если захочешь слать в конкретный топик:
        # "message_thread_id": int(os.getenv("MAIN_GROUP_TOPIC_ID", "0")) or None,
    }

    data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(
        url,
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )

    try:
        with urllib.request.urlopen(req, timeout=10) as resp:
            if resp.status != 200:
                logger.error(f"Ошибка отправки в Telegram: HTTP {resp.status}")
            else:
                logger.info("Уведомление в Telegram отправлено")
    except urllib.error.URLError as e:
        logger.error(f"Ошибка отправки в Telegram: {e}")


# ─────────────────────────────────────────────
#  Основная логика бэкапа
# ─────────────────────────────────────────────
def run_pg_dump():
    logger.info("Starting pg_dump backup")

    timestamp = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    filename = f"pg_backup_{timestamp}.sql.gz"
    tmpfile = os.path.join(tempfile.gettempdir(), filename)

    env = os.environ.copy()
    env["PGPASSWORD"] = PG_PASS

    cmd = [
        "pg_dump",
        "-h", PG_HOST,
        "-U", PG_USER,
        "-d", PG_DB,
        "-F", "p",
        "-Z", "9",
        "-f", tmpfile,
    ]

    logger.info("Running pg_dump.")
    subprocess.run(cmd, check=True, env=env)
    logger.info(f"Backup created: {tmpfile}")
    return tmpfile, filename


def upload_to_s3(file_path, file_name):
    logger.info("Uploading backup to S3.")

    s3 = boto3.client(
        "s3",
        endpoint_url=S3_ENDPOINT_URL,
        region_name=S3_REGION,
        aws_access_key_id=S3_ACCESS,
        aws_secret_access_key=S3_SECRET,
    )

    key = f"{S3_PREFIX}/{file_name}"
    s3.upload_file(file_path, S3_BUCKET, key)

    logger.info(f"Uploaded to s3://{S3_BUCKET}/{key}")
    return key


def main():
    start_ts = datetime.utcnow()
    try:
        fp, name = run_pg_dump()
        s3_key = upload_to_s3(fp, name)
        os.remove(fp)
        logger.info("Backup finished successfully")

        # ✅ Уведомление об успешном бэкапе
        msg = (
            "✅ <b>Бэкап БД выполнен</b>\n"
            f"⏱ Время: <code>{start_ts.isoformat()}</code>\n"
            f"🗂 Файл: <code>{name}</code>\n"
            f"📦 S3 ключ: <code>{s3_key}</code>"
        )
        send_telegram_message(msg)

    except Exception as e:
        logger.error(f"Backup failed: {e}", exc_info=True)

        # ❌ Уведомление об ошибке
        msg = (
            "❌ <b>Ошибка бэкапа БД</b>\n"
            f"⏱ Время: <code>{start_ts.isoformat()}</code>\n"
            f"Ошибка: <code>{e}</code>"
        )
        send_telegram_message(msg)


if __name__ == "__main__":
    main()
