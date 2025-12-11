"""
Mirror Worker — 10 параллельных воркеров с гарантией FIFO per ticket.
"""

import asyncio
import json
import logging
import time
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
WORKER_TIMEOUT = 60  # если воркер висит >60 сек — перезапуск
CONSUMER = "mirror_worker_fifo"

# =============================
# ГЛОБАЛЬНЫЕ СТРУКТУРЫ
# =============================
ticket_buffers: Dict[int, Dict[int, Tuple[str, Dict]]] = defaultdict(dict)
ticket_next_seq: Dict[int, int] = {}

ticket_stats: Dict[int, Dict[str, Any]] = defaultdict(lambda: {
    "start_time": None,
    "count": 0,
})


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

            if payload.get("pin"):
                try:
                    await bot.pin_chat_message(
                        chat_id=chat_id, message_id=msg.message_id,
                        disable_notification=True
                    )
                except Exception:
                    pass

            await asyncio.sleep(TEXT_DELAY)
            return True

        else:
            logger.error(f"❌ Неизвестный тип: {msg_type}")
            return True

    except TelegramRetryAfter as e:
        logger.warning(f"⏳ 429: ждем {e.retry_after}s")
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
        logger.error(f"❌ Ошибка отправки: {e}", exc_info=True)
        await asyncio.sleep(1)
        return False


# =============================
# SAFE WRAPPER
# =============================
async def send_message_safe(payload: Dict[str, Any]) -> bool:
    """Отправляет сообщение с ретраями."""
    bot = Bot(token=payload["bot_token"])
    try:
        while True:
            ok = await send_payload(bot, payload)
            if ok:
                return True
            await asyncio.sleep(0.3)
    finally:
        await bot.session.close()


# =============================
# PROCESS MESSAGE ORDERED
# =============================
async def process_message_ordered(msg_id: str, payload: Dict[str, Any]) -> bool:
    ticket_id = payload.get("ticket_id")
    sequence_id = payload.get("sequence_id")

    # Без sequence — обычная отправка
    if ticket_id is None or sequence_id is None:
        return await send_message_safe(payload)

    # =============================
    # ИНИЦИАЛИЗАЦИЯ ТИКЕТА
    # =============================
    if ticket_id not in ticket_next_seq:
        ticket_next_seq[ticket_id] = sequence_id
        ticket_buffers[ticket_id] = {}

        ticket_stats[ticket_id]["start_time"] = time.time()
        ticket_stats[ticket_id]["count"] = 0

        logger.info(f"🚀 Тикет #{ticket_id}: НАЧАЛАСЬ пересылка")

        # уведомление в главный топик
        try:
            from app.config import settings
            bot = Bot(token=payload["bot_token"])
            await bot.send_message(
                chat_id=settings.main_group_id,
                message_thread_id=payload.get("main_thread_id"),
                text=f"📤 <b>Начата пересылка истории</b>\nТикет #{ticket_id}",
                parse_mode="HTML",
            )
            await bot.session.close()
        except Exception as e:
            logger.error(f"❌ Ошибка нотификации начала: {e}")

    expected = ticket_next_seq[ticket_id]

    # =============================
    # OUT OF ORDER
    # =============================
    if sequence_id < expected:
        logger.warning(
            f"⚠️ Дубликат seq={sequence_id} для ticket={ticket_id} (ожидается {expected})"
        )
        return True

    if sequence_id > expected:
        logger.info(
            f"📦 Буферизуем seq={sequence_id} (ждем {expected}) ticket={ticket_id}"
        )
        ticket_buffers[ticket_id][sequence_id] = (msg_id, payload)
        return False

    # =============================
    # PROCESS CURRENT SEQUENCE
    # =============================
    logger.info(
        f"➡️ Отправка seq={sequence_id} ticket={ticket_id} (ожидали {expected})"
    )

    ok = await send_message_safe(payload)
    if ok:
        ticket_stats[ticket_id]["count"] += 1
    if not ok:
        return False

    ticket_next_seq[ticket_id] += 1

    # =============================
    # PROCESS BUFFER
    # =============================
    while True:
        next_seq = ticket_next_seq[ticket_id]

        if next_seq not in ticket_buffers[ticket_id]:
            break

        buffered_msg_id, buffered_payload = ticket_buffers[ticket_id].pop(next_seq)
        logger.info(
            f"📤 Из буфера: seq={next_seq} ticket={ticket_id}"
        )

        ok = await send_message_safe(buffered_payload)
        if ok:
            ticket_stats[ticket_id]["count"] += 1
        if not ok:
            ticket_buffers[ticket_id][next_seq] = (buffered_msg_id, buffered_payload)
            break

        try:
            await redis_streams.ack(buffered_msg_id)
        except Exception as e:
            logger.error(f"❌ Ошибка ACK буферного сообщения: {e}")

        ticket_next_seq[ticket_id] += 1

    # =============================
    # FINISH
    # =============================
    if not ticket_buffers[ticket_id]:
        total = ticket_stats[ticket_id]["count"]
        elapsed = round(time.time() - ticket_stats[ticket_id]["start_time"], 2)

        logger.info(
            f"🎉 Тикет #{ticket_id}: ПЕРЕСЫЛКА ЗАВЕРШЕНА — {total} сообщений, {elapsed} сек"
        )

        # уведомление в главный топик
        try:
            from app.config import settings
            bot = Bot(token=payload["bot_token"])
            await bot.send_message(
                chat_id=settings.main_group_id,
                message_thread_id=payload.get("main_thread_id"),
                text=(
                    f"📬 <b>Пересылка завершена</b>\n"
                    f"Тикет #{ticket_id}\n"
                    f"• Сообщений: <b>{total}</b>\n"
                    f"• Время: <b>{elapsed} сек</b>"
                ),
                parse_mode="HTML",
            )
            await bot.session.close()
        except Exception as e:
            logger.error(f"❌ Ошибка финальной нотификации: {e}")

    return True


# =============================
# WORKER LOOP + WATCHDOG
# =============================
async def worker_loop(worker_id: int):
    consumer_name = f"{CONSUMER}_{worker_id}"
    logger.info(f"🚀 Worker #{worker_id} ЗАПУЩЕН")

    last_activity = time.time()

    await redis_streams.connect()
    await redis_streams.init()

    while True:
        try:
            # WATCHDOG
            if time.time() - last_activity > WORKER_TIMEOUT:
                logger.error(f"⛔ Worker #{worker_id} завис >{WORKER_TIMEOUT}s — перезапуск")
                raise RuntimeError("Worker hang detected")

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
                    last_activity = time.time()

                    try:
                        payload = json.loads(raw["payload"])
                    except Exception:
                        logger.error(f"❌ Worker #{worker_id}: плохой payload")
                        await redis_streams.ack(msg_id)
                        continue

                    seq = payload.get("sequence_id", "?")
                    ticket = payload.get("ticket_id")
                    logger.info(f"📨 Worker #{worker_id}: ticket={ticket} seq={seq}")

                    ok = await process_message_ordered(msg_id, payload)

                    if ok:
                        await redis_streams.ack(msg_id)
                        logger.info(f"✔ ACK #{worker_id}: seq={seq}")

        except Exception as e:
            logger.error(f"❌ Ошибка Worker #{worker_id}: {e}", exc_info=True)
            await asyncio.sleep(1)


# =============================
# POOL MANAGER
# =============================
async def mirror_worker():
    NUM_WORKERS = 10
    tasks = []

    try:
        for i in range(1, NUM_WORKERS + 1):
            t = asyncio.create_task(worker_loop(i))
            tasks.append(t)

        logger.info(f"🚀 Запущено {NUM_WORKERS} воркеров")
        await asyncio.gather(*tasks)

    except asyncio.CancelledError:
        logger.info("⛔ Завершение: останавливаем воркеров…")
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
