import asyncio
import json
import logging
from typing import Dict, Any

import aioredis
from aiogram import Bot
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramBadRequest,
    TelegramAPIError,
)

from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP, MAX_RETRIES

logger = logging.getLogger(__name__)

CONSUMER = "mirror_worker_1"

BACKOFF_BASE = 1.8       # коэффициент экспоненциальной задержки
BACKOFF_START = 1.0      # стартовая задержка в секундах


# ==================================================================
# UNIVERSAL TELEGRAM SENDER — отправляет ВСЕ типы сообщений
# ==================================================================
async def send_payload(bot: Bot, payload: Dict[str, Any]):
    """
    Универсальная отправка сообщения любого типа.
    Поддерживает:
    text / photo / video / document / voice
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
        # TEXT
        if msg_type == "text":
            sent = await bot.send_message(
                chat_id=chat_id,
                text=payload["text"],
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )

        # PHOTO
        elif msg_type == "photo":
            sent = await bot.send_photo(
                chat_id=chat_id,
                photo=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        # VIDEO
        elif msg_type == "video":
            sent = await bot.send_video(
                chat_id=chat_id,
                video=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        # DOCUMENT
        elif msg_type == "document":
            sent = await bot.send_document(
                chat_id=chat_id,
                document=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        # VOICE
        elif msg_type == "voice":
            sent = await bot.send_voice(
                chat_id=chat_id,
                voice=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        # UNKNOWN TYPE
        else:
            raise ValueError(f"Неизвестный msg_type: {msg_type}")

        # PIN
        if sent and pin:
            try:
                await bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent.message_id,
                    disable_notification=True
                )
            except Exception as e:
                logger.warning(f"PIN ошибка: {e}")

        return True

    # Telegram просит подождать (rate limit)
    except TelegramRetryAfter as e:
        logger.warning(f"429 RETRY AFTER {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False

    except TelegramBadRequest as e:
        # ошибка отправки — не retryable
        logger.error(f"BadRequest → {e}")
        return "fatal"

    except TelegramAPIError as e:
        logger.error(f"Telegram API error: {e}")
        return False

    except Exception as e:
        logger.error(f"send_payload error: {e}")
        return False


# ==================================================================
# PROCESS ONE MESSAGE
# ==================================================================
async def process_message(message_id: str, payload: Dict[str, Any]):
    bot = Bot(token=payload["bot_token"])

    result = await send_payload(bot, payload)

    # Успех → ACK
    if result is True:
        return True

    # Фатальная ошибка → DLQ
    if result == "fatal":
        await redis_streams.send_to_dlq(payload, "fatal_send_error")
        return True

    # RETRY
    attempt = payload.get("attempt", 0)

    if attempt >= MAX_RETRIES:
        await redis_streams.send_to_dlq(payload, "max_retries_reached")
        return True

    # Экспоненциальная задержка
    delay = BACKOFF_START * (BACKOFF_BASE ** attempt)
    logger.info(f"🔁 RETRY attempt={attempt+1}, delay={delay:.2f}s")

    await asyncio.sleep(delay)

    payload["attempt"] = attempt + 1
    await redis_streams.enqueue(payload)

    return True


# ==================================================================
# MAIN WORKER LOOP
# ==================================================================
async def worker_loop():
    redis = redis_streams.redis

    logger.info("🚀 Mirror Worker запущен")

    # создаём группу, если нет
    await redis_streams.init()

    while True:
        try:
            # читаем pending + новые сообщения
            resp = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_KEY: ">"},
                count=20,
                block=3000  # 3 секунды
            )

            if not resp:
                continue

            for stream, messages in resp:
                for msg_id, fields in messages:

                    try:
                        payload = json.loads(fields["payload"])
                    except Exception as e:
                        logger.error(f"Некорректный payload: {fields}")
                        await redis_streams.ack(msg_id)
                        continue

                    ok = await process_message(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)

        except Exception as e:
            logger.error(f"WorkerLoop ERROR: {e}", exc_info=True)
            await asyncio.sleep(2)


# ==================================================================
# ENTRYPOINT
# ==================================================================
if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    asyncio.run(worker_loop())
