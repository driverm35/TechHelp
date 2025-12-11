"""
Mirror Worker — FIFO per ticket. 10 parallel workers.

Особенности:
- История тикета передаётся строго в порядке sequence_id
- Живые сообщения (без sequence_id) отправляются сразу
- Уведомления «Начата/Завершена пересылка» — только в главный топик клиента
- Ни одна ClientSession не остаётся открытой (async with Bot)
"""

import asyncio
import json
import logging
import time
from collections import defaultdict
from typing import Dict, Any, Tuple, Optional

from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramAPIError
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP
from app.config import settings

logger = logging.getLogger(__name__)

# -------------------------
# Настройки
# -------------------------
TEXT_DELAY = 0.04
MEDIA_DELAY = 0.9
WORKER_TIMEOUT = 60
CONSUMER = "mirror_worker_fifo"

# -------------------------
# Внутренние структуры FIFO
# -------------------------
ticket_buffers: Dict[int, Dict[int, Tuple[str, Dict]]] = defaultdict(dict)
ticket_next_seq: Dict[int, int] = {}
ticket_processing: Dict[int, bool] = defaultdict(lambda: False)

ticket_stats: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
    "start_time": None,
    "count": 0,
})

ticket_in_progress: Dict[int, Optional[int]] = defaultdict(lambda: None)


# ================================================================
# Уведомления в главный топик клиента
# ================================================================
async def notify_main_group(bot_token: str, main_chat_id: int, main_thread_id: int, text: str):
    """Отправляет системное уведомление в главный топик клиента."""
    if not main_chat_id or not main_thread_id:
        return

    async with Bot(token=bot_token) as bot:
        try:
            await bot.send_message(
                chat_id=main_chat_id,
                message_thread_id=main_thread_id,
                text=text,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка уведомления в главный топик: {e}")


# ================================================================
# Универсальная отправка сообщения
# ================================================================
async def send_payload(bot: Bot, payload: Dict[str, Any]) -> bool:
    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    try:
        # TEXT
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

        # PHOTO
        elif msg_type == "photo":
            await bot.send_photo(
                chat_id=chat_id,
                photo=payload["file_id"],
                caption=payload.get("caption") or None,
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        # VIDEO
        elif msg_type == "video":
            await bot.send_video(
                chat_id=chat_id,
                video=payload["file_id"],
                caption=payload.get("caption") or None,
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        # DOCUMENT
        elif msg_type == "document":
            await bot.send_document(
                chat_id=chat_id,
                document=payload["file_id"],
                caption=payload.get("caption") or None,
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        # VOICE
        elif msg_type == "voice":
            await bot.send_voice(
                chat_id=chat_id,
                voice=payload["file_id"],
                caption=payload.get("caption") or None,
                parse_mode="HTML",
                **kwargs
            )
            await asyncio.sleep(MEDIA_DELAY)
            return True

        # STATUS BUTTONS
        elif msg_type == "status_buttons":
            ticket_id = payload["ticket_id"]

            kb = InlineKeyboardMarkup(inline_keyboard=[
                [
                    InlineKeyboardButton(
                        text="Отправить опрос",
                        callback_data=f"send_feedback_button:{ticket_id}"
                    )
                ],
                [
                    InlineKeyboardButton(
                        text="🟡 В работе",
                        callback_data=f"status_work:{ticket_id}"
                    ),
                    InlineKeyboardButton(
                        text="⚪️ Закрыть",
                        callback_data=f"status_close:{ticket_id}"
                    )
                ]
            ])

            msg = await bot.send_message(
                chat_id=chat_id,
                text="<b>Управление статусом:</b>",
                reply_markup=kb,
                parse_mode="HTML",
                **kwargs
            )

            if payload.get("pin"):
                try:
                    await bot.pin_chat_message(
                        chat_id=chat_id,
                        message_id=msg.message_id,
                        disable_notification=True
                    )
                except Exception:
                    pass

            await asyncio.sleep(TEXT_DELAY)
            return True

        else:
            logger.error(f"❌ Неизвестный тип сообщения: {msg_type}")
            return True

    except TelegramRetryAfter as e:
        logger.warning(f"⏳ 429 {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False

    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest: {e}")
        return True

    except TelegramAPIError as e:
        logger.error(f"⚠️ Telegram API error: {e}")
        await asyncio.sleep(1)
        return False

    except Exception as e:
        logger.error(f"❌ Ошибка отправки: {e}", exc_info=True)
        await asyncio.sleep(1)
        return False


# ================================================================
# Обёртка с ретраями
# ================================================================
async def send_message_safe(payload: Dict[str, Any]) -> bool:
    async with Bot(token=payload["bot_token"]) as bot:
        while True:
            ok = await send_payload(bot, payload)
            if ok:
                return True
            await asyncio.sleep(0.3)


# ================================================================
# FIFO per ticket
# ================================================================
async def process_message_ordered(msg_id: str, payload: Dict[str, Any]) -> bool:
    ticket_id = payload.get("ticket_id")
    seq = payload.get("sequence_id")

    # LIVE message (нет sequence)
    if ticket_id is None or seq is None:
        return await send_message_safe(payload)

    # INIT
    if ticket_id not in ticket_next_seq:
        ticket_next_seq[ticket_id] = seq
        ticket_buffers[ticket_id] = {}

        ticket_stats[ticket_id]["start_time"] = time.time()
        ticket_stats[ticket_id]["count"] = 0
        ticket_processing[ticket_id] = True

        # уведомление только в главный топик
        await notify_main_group(
            bot_token=payload["bot_token"],
            main_chat_id=settings.main_group_id,
            main_thread_id=payload.get("main_thread_id"),
            text=f"📤 <b>Начата пересылка истории</b>\nТикет #{ticket_id}"
        )

    expected = ticket_next_seq[ticket_id]

    # OUT OF ORDER
    if seq < expected:
        logger.warning(f"⚠️ Дубликат seq={seq} (ожидается {expected}) ticket={ticket_id}")
        return True

    if seq > expected:
        ticket_buffers[ticket_id][seq] = (msg_id, payload)
        logger.info(f"📦 Буфер: seq={seq} ждём {expected}")
        return False

    # PROCESS CURRENT
    ok = await send_message_safe(payload)
    if not ok:
        return False

    ticket_stats[ticket_id]["count"] += 1
    ticket_next_seq[ticket_id] += 1

    # PROCESS BUFFER
    while True:
        next_seq = ticket_next_seq[ticket_id]
        item = ticket_buffers[ticket_id].pop(next_seq, None)
        if not item:
            break

        buffered_msg_id, buffered_payload = item
        ok2 = await send_message_safe(buffered_payload)

        if not ok2:
            ticket_buffers[ticket_id][next_seq] = item
            break

        ticket_stats[ticket_id]["count"] += 1
        ticket_next_seq[ticket_id] += 1

        await redis_streams.ack(buffered_msg_id)

    # FINISH
    if not ticket_buffers[ticket_id]:
        total = ticket_stats[ticket_id]["count"]
        elapsed = round(time.time() - ticket_stats[ticket_id]["start_time"], 2)

        logger.info(f"🎉 Тикет #{ticket_id} завершён: {total} сообщений, {elapsed}s")

        await notify_main_group(
            bot_token=payload["bot_token"],
            main_chat_id=settings.main_group_id,
            main_thread_id=payload.get("main_thread_id"),
            text=(
                f"📬 <b>Пересылка завершена</b>\n"
                f"Тикет #{ticket_id}\n"
                f"• Сообщений: <b>{total}</b>\n"
                f"• Время: <b>{elapsed} сек</b>"
            )
        )

        # очищаем состояние
        del ticket_next_seq[ticket_id]
        del ticket_buffers[ticket_id]
        del ticket_stats[ticket_id]
        ticket_processing[ticket_id] = False

    return True


# ================================================================
# Worker loop
# ================================================================
async def worker_loop(worker_id: int):
    consumer = f"{CONSUMER}_{worker_id}"
    logger.info(f"🚀 Worker #{worker_id} запущен")

    last_activity = time.time()
    ticket_in_progress[worker_id] = None

    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            resp = await redis_streams.redis.xreadgroup(
                groupname=GROUP,
                consumername=consumer,
                streams={STREAM_KEY: ">"},
                count=1,
                block=3000,
            )

            if not resp:
                continue

            for _, messages in resp:
                for msg_id, raw in messages:
                    last_activity = time.time()

                    try:
                        payload = json.loads(raw["payload"])
                    except Exception:
                        await redis_streams.ack(msg_id)
                        continue

                    ticket_id = payload.get("ticket_id")
                    seq = payload.get("sequence_id")

                    ticket_in_progress[worker_id] = ticket_id

                    ok = await process_message_ordered(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)

                    ticket_in_progress[worker_id] = None

        except Exception as e:
            logger.error(f"❌ Ошибка worker #{worker_id}: {e}", exc_info=True)
            ticket_in_progress[worker_id] = None
            await asyncio.sleep(1)


# ================================================================
# Manager
# ================================================================
async def mirror_worker():
    NUM_WORKERS = 10
    tasks = []

    try:
        for i in range(1, NUM_WORKERS + 1):
            tasks.append(asyncio.create_task(worker_loop(i)))

        logger.info(f"🚀 Запущено {NUM_WORKERS} воркеров")
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        for t in tasks:
            t.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    finally:
        await redis_streams.disconnect()


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s"
    )

    asyncio.run(mirror_worker())
