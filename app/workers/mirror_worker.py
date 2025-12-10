# app/workers/mirror_worker.py
"""
Воркер для обработки очереди сообщений из Redis Streams.
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

CONSUMER = "mirror_worker_1"

BACKOFF_BASE = 1.8
BACKOFF_START = 1.0


# ==================================================================
# UNIVERSAL TELEGRAM SENDER
# ==================================================================
async def send_payload(bot: Bot, payload: Dict[str, Any]):
    """Универсальная отправка сообщения любого типа."""
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

    except TelegramRetryAfter as e:
        logger.warning(f"⏰ 429 RETRY AFTER {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False
    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest → {e}")
        return "fatal"
    except TelegramAPIError as e:
        logger.error(f"⚠️ Telegram API error: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ send_payload error: {e}", exc_info=True)
        return False


# ==================================================================
# PROCESS ONE MESSAGE
# ==================================================================
async def process_message(message_id: str, payload: Dict[str, Any]):
    """Обработка одного сообщения из очереди."""
    bot = Bot(token=payload["bot_token"])

    try:
        result = await send_payload(bot, payload)

        if result is True:
            logger.debug(f"✅ Отправлено: {payload['type']} → {payload['target_chat_id']}")
            return True

        if result == "fatal":
            await redis_streams.send_to_dlq(payload, "fatal_send_error")
            logger.error(f"💀 В DLQ: fatal_send_error")
            return True

        attempt = payload.get("attempt", 0)
        if attempt >= MAX_RETRIES:
            await redis_streams.send_to_dlq(payload, "max_retries_reached")
            logger.error(f"💀 В DLQ: max_retries (attempts={attempt})")
            return True

        delay = BACKOFF_START * (BACKOFF_BASE ** attempt)
        logger.info(f"🔁 RETRY attempt={attempt+1}/{MAX_RETRIES}, delay={delay:.2f}s")
        await asyncio.sleep(delay)

        payload["attempt"] = attempt + 1
        await redis_streams.enqueue(payload)
        return True

    except Exception as e:
        logger.error(f"❌ process_message error: {e}", exc_info=True)
        return False
    finally:
        await bot.session.close()


# ==================================================================
# WORKER LOOP
# ==================================================================
async def worker_loop():
    """Основной цикл воркера."""
    await redis_streams.connect()
    await redis_streams.init()

    logger.info("🚀 Mirror Worker запущен")
    logger.info(f"📡 Stream: {STREAM_KEY}, Group: {GROUP}, Consumer: {CONSUMER}")

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_KEY: ">"},
                count=20,
                block=3000
            )

            if not resp:
                continue

            for stream_data in resp:
                messages = stream_data[1]
                
                for msg_id, fields in messages:
                    try:
                        payload = json.loads(fields["payload"])
                    except Exception as e:
                        logger.error(f"❌ Некорректный payload: {e}")
                        await redis_streams.ack(msg_id)
                        continue

                    ok = await process_message(msg_id, payload)
                    if ok:
                        await redis_streams.ack(msg_id)

        except Exception as e:
            logger.error(f"❌ WorkerLoop ERROR: {e}", exc_info=True)
            await asyncio.sleep(2)


# ==================================================================
# ENTRYPOINT для standalone запуска
# ==================================================================
async def mirror_worker():
    """
    Точка входа для воркера.
    Может быть запущена как отдельный процесс или как задача в main event loop.
    """
    try:
        await worker_loop()
    except asyncio.CancelledError:
        logger.info("⏹️ Mirror worker остановлен")
    except Exception as e:
        logger.error(f"❌ Mirror worker crashed: {e}", exc_info=True)
        raise
    finally:
        await redis_streams.disconnect()
        logger.info("✅ Redis Streams отключен")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    asyncio.run(mirror_worker())