"""
Mirror Worker — 10 параллельных воркеров с гарантией порядка FIFO per ticket.
"""

import asyncio
import json
import logging
from typing import Dict, Any, Tuple
from collections import defaultdict

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

# Глобальный буфер для упорядочивания сообщений
# {ticket_id: {sequence_id: (msg_id, payload)}}
ticket_buffers: Dict[int, Dict[int, Tuple[str, Dict]]] = defaultdict(dict)
# {ticket_id: expected_next_sequence_id}
ticket_next_seq: Dict[int, int] = {}


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
                inline_keyboard=[
                    [
                        InlineKeyboardButton(
                            text="Отправить опрос",
                            callback_data=f"send_feedback_button:{ticket_id}",
                        ),
                    ],
                    [
                        InlineKeyboardButton(
                            text="🟡 В работе",
                            callback_data=f"status_work:{ticket_id}",
                        ),
                        InlineKeyboardButton(
                            text="⚪️ Закрыть",
                            callback_data=f"status_close:{ticket_id}",
                        )
                    ]
                ]
            )

            msg = await bot.send_message(
                chat_id=chat_id,
                text="<b>Управление статусом:</b>",
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
# PROCESS WITH ORDERING
# =============================
async def send_message_safe(payload: Dict[str, Any]) -> bool:
    """Отправляет сообщение с retry."""
    bot = Bot(token=payload["bot_token"])
    try:
        while True:
            ok = await send_payload(bot, payload)
            if ok:
                return True
            await asyncio.sleep(0.3)
    finally:
        await bot.session.close()


async def process_message_ordered(msg_id: str, payload: Dict[str, Any]) -> bool:
    """
    Обрабатывает сообщение с учётом порядка sequence_id.
    Если пришло не по порядку — буферизует.
    """
    
    ticket_id = payload.get("ticket_id")
    sequence_id = payload.get("sequence_id")
    
    # Если нет sequence_id — обрабатываем сразу (не требует порядка)
    if sequence_id is None or ticket_id is None:
        return await send_message_safe(payload)
    
    # Инициализация для тикета
    if ticket_id not in ticket_next_seq:
        ticket_next_seq[ticket_id] = sequence_id
        ticket_buffers[ticket_id] = {}
    
    expected = ticket_next_seq[ticket_id]
    
    # Если меньше ожидаемого — уже обработано (дубликат)
    if sequence_id < expected:
        logger.warning(f"⚠️ Дубликат seq={sequence_id} для ticket={ticket_id} (ждём {expected})")
        return True  # ACK
    
    # Если больше ожидаемого — буферизуем
    if sequence_id > expected:
        logger.debug(f"📦 Буферизуем seq={sequence_id} для ticket={ticket_id} (ждём {expected})")
        ticket_buffers[ticket_id][sequence_id] = (msg_id, payload)
        return False  # НЕ ACK — повторим позже
    
    # ✅ Это нужное сообщение (sequence_id == expected) — обрабатываем
    logger.debug(f"✅ Обрабатываем seq={sequence_id} для ticket={ticket_id}")
    
    ok = await send_message_safe(payload)
    if not ok:
        return False  # retry
    
    # Увеличиваем счётчик
    ticket_next_seq[ticket_id] += 1
    
    # Проверяем буфер — может там следующие уже есть?
    while True:
        next_seq = ticket_next_seq[ticket_id]
        
        if next_seq not in ticket_buffers[ticket_id]:
            break
        
        # Есть следующее — обрабатываем из буфера
        buffered_msg_id, buffered_payload = ticket_buffers[ticket_id].pop(next_seq)
        
        logger.debug(f"📤 Обрабатываем из буфера seq={next_seq} для ticket={ticket_id}")
        
        ok = await send_message_safe(buffered_payload)
        if not ok:
            # Вернули в буфер
            ticket_buffers[ticket_id][next_seq] = (buffered_msg_id, buffered_payload)
            break
        
        # ACK для буферизованного
        try:
            await redis_streams.ack(buffered_msg_id)
            logger.debug(f"✔ ACK буферизованного seq={next_seq}")
        except Exception as e:
            logger.error(f"❌ Ошибка ACK буферизованного: {e}")
        
        ticket_next_seq[ticket_id] += 1
    
    return True


# =============================
# WORKER LOOP
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

                    seq = payload.get('sequence_id', '?')
                    logger.info(f"📨 Worker {worker_id}: {payload.get('type')} ticket={payload.get('ticket_id')} seq={seq}")

                    ok = await process_message_ordered(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)
                        logger.info(f"✔ Worker {worker_id}: ACK seq={seq}")

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