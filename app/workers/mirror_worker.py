"""
Mirror Worker — обработчик очереди Redis Streams.
5 параллельных воркеров, backoff, retry, DLQ, anti-429 throttle.
"""

import asyncio
import json
import logging
from typing import Dict, Any

from aiogram import Bot
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramBadRequest,
    TelegramAPIError,
)

from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP, MAX_RETRIES

logger = logging.getLogger(__name__)

# =============================
# НАСТРОЙКИ ПРОИЗВОДИТЕЛЬНОСТИ
# =============================
WORKER_COUNT = 5
RATE_LIMIT_DELAY = 0.05   # 50 ms между отправками чтобы избегать 429
BACKOFF_BASE = 1.8
BACKOFF_START = 1.0


# ==================================================================
# UNIVERSAL TELEGRAM SENDER
# ==================================================================
async def send_payload(bot: Bot, payload: Dict[str, Any]):
    """
    Универсальная отправка сообщения любого типа из очереди.
    payload: {
        "type": text/photo/video/document/voice
        "text": "...",
        "file_id": "...",
        "caption": "...",
        "target_chat_id": int,
        "target_thread_id": int | None,
        "pin": bool,
        ...
    }
    """
    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")
    pin = payload.get("pin", False)

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    sent = None

    try:
        if msg_type == "text":
            sent = await bot.send_message(
                chat_id=chat_id,
                text=payload["text"],
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )

        elif msg_type == "photo":
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "video":
            sent = await bot.send_video(
                chat_id=chat_id,
                video=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "document":
            sent = await bot.send_document(
                chat_id=chat_id,
                document=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "voice":
            sent = await bot.send_voice(
                chat_id=chat_id,
                voice=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )
        else:
            raise ValueError(f"Неизвестный тип сообщения: {msg_type}")

        if pin and sent:
            try:
                await bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent.message_id,
                    disable_notification=True
                )
            except Exception as e:
                logger.warning(f"PIN ошибка: {e}")

        return True

    except TelegramRetryAfter as e:
        # Потоковая блокировка Telegram API
        logger.warning(f"⏳ 429 Retry-After: {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False

    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest: {e}")
        return "fatal"

    except TelegramAPIError as e:
        logger.error(f"⚠️ API Error: {e}")
        return False

    except Exception as e:
        logger.error(f"❌ Ошибка send_payload: {e}", exc_info=True)
        return False


# ==================================================================
# PROCESS ONE MESSAGE
# ==================================================================
async def process_message(message_id: str, payload: Dict[str, Any]):
    bot = Bot(token=payload["bot_token"])

    try:
        ok = await send_payload(bot, payload)

        # Успех → ACK
        if ok is True:
            logger.info(
                f"✅ Отправлено: {payload['type']} → chat={payload['target_chat_id']} "
            )
            return True

        # Фатальная ошибка → DLQ
        if ok == "fatal":
            await redis_streams.send_to_dlq(payload, "fatal_error")
            return True

        # Нефатальная ошибка → retry
        attempt = payload.get("attempt", 0)

        if attempt >= MAX_RETRIES:
            await redis_streams.send_to_dlq(payload, "max_retries_reached")
            logger.error(f"💀 DLQ: {payload}")
            return True

        delay = BACKOFF_START * (BACKOFF_BASE ** attempt)
        logger.info(
            f"🔁 RETRY {attempt+1}/{MAX_RETRIES}, delay={delay:.2f}s"
        )

        await asyncio.sleep(delay)
        payload["attempt"] = attempt + 1
        await redis_streams.enqueue(payload)

        return True

    except Exception as e:
        logger.error(f"❌ Ошибка process_message: {e}", exc_info=True)
        return False

    finally:
        await bot.session.close()


# ==================================================================
# WORKER TASK — ОДИН ВОРКЕР
# ==================================================================
async def worker_task(worker_name: str):
    logger.info(f"🚀 Worker {worker_name} стартует...")

    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=worker_name,
                streams={STREAM_KEY: ">"},
                count=20,
                block=2000
            )

            if not resp:
                continue

            for _, messages in resp:
                for msg_id, raw in messages:

                    try:
                        payload = json.loads(raw["payload"])
                    except Exception:
                        logger.error("❌ Некорректный payload, ACK")
                        await redis_streams.ack(msg_id)
                        continue

                    ok = await process_message(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)

                    # Anti-429
                    await asyncio.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logger.error(f"❌ Worker {worker_name} ERROR: {e}", exc_info=True)
            await asyncio.sleep(2)


# ==================================================================
# MAIN WORKER LOOP (RUN 5 WORKERS)
# ==================================================================
async def worker_loop():
    logger.info(f"🔥 Запускаем {WORKER_COUNT} воркеров Redis Streams...")

    tasks = []

    for i in range(WORKER_COUNT):
        name = f"mirror_worker_{i+1}"
        tasks.append(asyncio.create_task(worker_task(name)))

    # Ждём завершения всех (обычно никогда)
    await asyncio.gather(*tasks)


# ==================================================================
# ENTRYPOINT
# ==================================================================
async def mirror_worker():
    try:
        await worker_loop()
    except asyncio.CancelledError:
        logger.info("⛔ Worker остановлен")
    except Exception as e:
        logger.error(f"❌ Worker crash: {e}", exc_info=True)
    finally:
        await redis_streams.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    asyncio.run(mirror_worker())
