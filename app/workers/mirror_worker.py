"""
Mirror Worker — строгий FIFO воркер для Redis Streams.
Гарантирует порядок сообщений, без повторных enqueue.
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
RATE_LIMIT_DELAY = 0.05   # 50 ms — безопасный anti-429 throttle
WORKER_CONSUMER = "fifo_worker_1"


# ==================================================================
# UNIVERSAL TELEGRAM SENDER
# ==================================================================
async def send_payload(bot: Bot, payload: Dict[str, Any]):
    """
    Универсальная отправка — строго последовательная.
    РЕТРАЕВ НЕТ. Если 429 — просто ждём и отправляем повторно.
    """

    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")
    pin = payload.get("pin", False)

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    while True:  # 🔁 Отправляем, пока не получится
        try:
            # ----------- TEXT ----------
            if msg_type == "text":
                sent = await bot.send_message(
                    chat_id=chat_id,
                    text=payload["text"],
                    parse_mode="HTML",
                    disable_web_page_preview=True,
                    **kwargs
                )

            # ----------- PHOTO ----------
            elif msg_type == "photo":
                sent = await bot.send_photo(
                    chat_id=chat_id,
                    photo=payload["file_id"],
                    caption=payload.get("caption"),
                    parse_mode="HTML",
                    **kwargs
                )

            # ----------- VIDEO ----------
            elif msg_type == "video":
                sent = await bot.send_video(
                    chat_id=chat_id,
                    video=payload["file_id"],
                    caption=payload.get("caption"),
                    parse_mode="HTML",
                    **kwargs
                )

            # ----------- DOCUMENT ----------
            elif msg_type == "document":
                sent = await bot.send_document(
                    chat_id=chat_id,
                    document=payload["file_id"],
                    caption=payload.get("caption"),
                    parse_mode="HTML",
                    **kwargs
                )

            # ----------- VOICE ----------
            elif msg_type == "voice":
                sent = await bot.send_voice(
                    chat_id=chat_id,
                    voice=payload["file_id"],
                    caption=payload.get("caption"),
                    parse_mode="HTML",
                    **kwargs
                )

            else:
                raise ValueError(f"Неизвестный тип: {msg_type}")

            # ------- pin -------
            if pin:
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
            logger.warning(f"⏳ 429 Too Many Requests, ждём {e.retry_after}s…")
            await asyncio.sleep(e.retry_after)
            continue  # повторяем ту же отправку

        except TelegramBadRequest as e:
            # Фатальная ошибка — ACK и НЕ повторяем
            logger.error(f"❌ BadRequest (пропускаем): {e}")
            return True

        except TelegramAPIError as e:
            logger.error(f"⚠️ API ошибка, попробуем снова через 1 секунду: {e}")
            await asyncio.sleep(1)

        except Exception as e:
            logger.error(f"❌ Ошибка send_payload: {e}", exc_info=True)
            await asyncio.sleep(1)


# ==================================================================
# PROCESS ONE MESSAGE
# ==================================================================
async def process_message(message_id: str, payload: Dict[str, Any]):
    bot = Bot(token=payload["bot_token"])

    try:
        await send_payload(bot, payload)
        return True

    except Exception as e:
        logger.error(f"❌ process_message ошибка: {e}", exc_info=True)
        return True  # даже если ошибка — ACK, чтобы не зависли

    finally:
        await bot.session.close()


# ==================================================================
# FIFO WORKER (1 воркер = строгий порядок)
# ==================================================================
async def worker_loop():
    logger.info("🚀 FIFO Worker стартует...")
    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=WORKER_CONSUMER,
                streams={STREAM_KEY: ">"},
                count=1,      # ⚠️ только ОДНА задача за раз — строгий порядок
                block=5000
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

                    # После отправки: ACK
                    if ok:
                        await redis_streams.ack(msg_id)

                    # Throttle
                    await asyncio.sleep(RATE_LIMIT_DELAY)

        except Exception as e:
            logger.error(f"❌ Worker ERROR: {e}", exc_info=True)
            await asyncio.sleep(2)


# ==================================================================
# ENTRYPOINT
# ==================================================================
async def mirror_worker():
    try:
        await worker_loop()
    except asyncio.CancelledError:
        logger.info("⛔ FIFO worker остановлен")
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
