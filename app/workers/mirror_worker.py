"""
Mirror Worker — 10 параллельных воркеров для зеркалирования сообщений.
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
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP

logger = logging.getLogger(__name__)

# =============================
# НАСТРОЙКИ
# =============================
TEXT_DELAY = 0.05
MEDIA_DELAY = 1.3
CONSUMER = "mirror_worker_fifo"


# =============================
# UNIVERSAL TELEGRAM SENDER
# =============================
async def send_payload(bot: Bot, payload: Dict[str, Any]) -> bool:
    """Универсальная отправка. True = OK, False = повторить."""
    
    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    try:
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

        elif msg_type == "status_buttons":
    
            ticket_id = payload["ticket_id"]
    
            kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="🟡 В работе",
                        callback_data=f"status_work:{ticket_id}",
                    ),
                    InlineKeyboardButton(
                        text="⚪️ Закрыть",
                        callback_data=f"status_close:{ticket_id}",
                    ),
                    InlineKeyboardButton(
                        text="📊 Отправить опрос",
                        callback_data=f"send_feedback_button:{ticket_id}",
                    ),
                ]]
            )

            msg = await bot.send_message(
                chat_id=chat_id,
                text="🎛 <b>Управление статусом:</b>",
                reply_markup=kb,
                parse_mode="HTML",
                **kwargs
            )

            # Закрепляем если нужно
            if payload.get("pin"):
                try:
                    await bot.pin_chat_message(
                        chat_id=chat_id,
                        message_id=msg.message_id,
                        disable_notification=True,
                    )
                except Exception:
                    pass
                
            await asyncio.sleep(TEXT_DELAY)
            return True

        else:
            logger.error(f"❌ Неизвестный тип: {msg_type}")
            return True

    except TelegramRetryAfter as e:
        logger.warning(f"⏳ 429: ждём {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False

    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest: {e}")
        return True

    except TelegramAPIError as e:
        logger.error(f"⚠️ API Error: {e}")
        await asyncio.sleep(1)
        return False

    except Exception as e:
        logger.error(f"❌ Ошибка: {e}", exc_info=True)
        await asyncio.sleep(1)
        return False


# =============================
# PROCESS MESSAGE
# =============================
async def process_message(msg_id: str, payload: Dict[str, Any]) -> bool:
    """Обрабатывает одну задачу с retry."""
    bot = Bot(token=payload["bot_token"])

    try:
        while True:
            ok = await send_payload(bot, payload)
            if ok:
                return True
            
            logger.info("🔁 Повторяем задачу...")
            await asyncio.sleep(0.3)

    finally:
        await bot.session.close()


# =============================
# WORKER LOOP (один воркер)
# =============================
async def worker_loop(worker_id: int):
    """Один воркер из пула."""
    consumer_name = f"{CONSUMER}_{worker_id}"
    logger.info(f"🚀 Worker #{worker_id} запущен")

    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=consumer_name,
                streams={STREAM_KEY: ">"},
                count=1,
                block=3000
            )

            if not resp:
                continue

            for _, messages in resp:
                for msg_id, raw in messages:
                    try:
                        payload = json.loads(raw["payload"])
                    except Exception:
                        logger.error(f"❌ Worker {worker_id}: плохой payload")
                        await redis_streams.ack(msg_id)
                        continue

                    logger.info(f"📨 Worker {worker_id}: {payload['type']}")

                    ok = await process_message(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)
                        logger.info(f"✔ Worker {worker_id}: ACK")

        except Exception as e:
            logger.error(f"❌ Worker {worker_id}: {e}", exc_info=True)
            await asyncio.sleep(1)


# =============================
# POOL MANAGER
# =============================
async def mirror_worker():
    """Запускает 10 параллельных воркеров."""
    NUM_WORKERS = 10
    
    tasks = []
    try:
        for i in range(1, NUM_WORKERS + 1):
            task = asyncio.create_task(worker_loop(i))
            tasks.append(task)
        
        logger.info(f"🚀 Запущено {NUM_WORKERS} воркеров")
        await asyncio.gather(*tasks)
        
    except asyncio.CancelledError:
        logger.info("⛔ Останавливаем воркеры...")
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)
        
    except Exception as e:
        logger.error(f"❌ Pool crash: {e}", exc_info=True)
        
    finally:
        await redis_streams.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )
    asyncio.run(mirror_worker())