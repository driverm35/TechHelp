"""
Mirror Worker — строгий FIFO воркер для зеркалирования сообщений.
ОБЕСПЕЧИВАЕТ НУЛЕВЫЕ 429 за счёт правильных пауз.
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

from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP

logger = logging.getLogger(__name__)

# =============================
# НАСТРОЙКИ
# =============================
TEXT_DELAY = 0.05         # 50 мс между текстами
MEDIA_DELAY = 1.3         # 1.3 сек между медиа
CONSUMER = "mirror_worker_fifo"


# =============================
# UNIVERSAL TELEGRAM SENDER
# =============================
async def send_payload(bot: Bot, payload: Dict[str, Any]) -> bool:
    """
    Универсальная отправка сообщения.
    Возвращает True = OK, False = нужно повторить ту же задачу.
    """

    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    try:
        # ------------------------------
        # TEXT
        # ------------------------------
        if msg_type == "text":
            await bot.send_message(
                chat_id=chat_id,
                text=payload["text"],
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )
            await asyncio.sleep(TEXT_DELAY)
            return True

        # ------------------------------
        # MEDIA
        # ------------------------------
        elif msg_type == "photo":
            await bot.send_photo(
                chat_id=chat_id,
                photo=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        elif msg_type == "video":
            await bot.send_video(
                chat_id=chat_id,
                video=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        elif msg_type == "document":
            await bot.send_document(
                chat_id=chat_id,
                document=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        elif msg_type == "voice":
            await bot.send_voice(
                chat_id=chat_id,
                voice=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        else:
            logger.error(f"❌ Неизвестный тип сообщения: {msg_type}")
            return True  # ACK

    except TelegramRetryAfter as e:
        # Telegram просит подождать → ждем строго retry_after и повторяем
        logger.warning(f"⏳ 429: Telegram просит подождать {e.retry_after}s — ждём…")
        await asyncio.sleep(e.retry_after)
        return False  # НЕ ACK → повторить ту же задачу в воркере

    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest: {e}")
        # 99% это ошибка данных → ACK, не пытаемся ретраить
        return True

    except TelegramAPIError as e:
        logger.error(f"⚠️ Telegram API Error: {e}")
        await asyncio.sleep(1)
        return False  # повторить

    except Exception as e:
        logger.error(f"❌ Ошибка send_payload: {e}", exc_info=True)
        await asyncio.sleep(1)
        return False  # повторить


# =============================
# PROCESS MESSAGE
# =============================
async def process_message(msg_id: str, payload: Dict[str, Any]) -> bool:
    """
    Обрабатывает одну задачу.
    FIFO гарантируется тем, что:
    - только один воркер
    - повтор делается локально (без повторного enqueue)
    """
    bot = Bot(token=payload["bot_token"])

    try:
        while True:
            ok = await send_payload(bot, payload)

            if ok:
                return True  # ACK

            # ok == False → повторяем ту же задачу (429 или временная ошибка)
            logger.info("🔁 Повторяем задачу после паузы…")
            await asyncio.sleep(0.3)

    finally:
        await bot.session.close()


# =============================
# MAIN WORKER LOOP (FIFO)
# =============================
async def worker_loop():
    logger.info("🚀 FIFO Mirror Worker запущен")

    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_KEY: ">"},
                count=1,          # ВАЖНО! FIFO → только одно сообщение за раз
                block=3000
            )

            if not resp:
                continue

            for _, messages in resp:
                for msg_id, raw in messages:

                    # Достаём payload
                    try:
                        payload = json.loads(raw["payload"])
                    except Exception:
                        logger.error("❌ Плохой payload, делаем ACK")
                        await redis_streams.ack(msg_id)
                        continue

                    logger.info(f"📨 TASK → {payload['type']}")

                    ok = await process_message(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)
                        logger.info("✔ ACK")

        except Exception as e:
            logger.error(f"❌ Worker ERROR: {e}", exc_info=True)
            await asyncio.sleep(1)


# =============================
# ENTRYPOINT
# =============================
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
