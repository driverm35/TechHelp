import asyncio
import json
import logging
from typing import Dict, Any

import aiohttp

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from aiogram import Bot
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest, TelegramAPIError

from app.utils.redis_streams import redis_streams, STREAM_KEY, GROUP, MAX_RETRIES

logger = logging.getLogger(__name__)

CONSUMER = "worker-1"
BACKOFF_BASE = 1.5             # коэффициент экспоненциальной задержки
BACKOFF_START = 1.0            # секунды


# ─────────────────────────────────────────────────────────────
#  Health-check API (FastAPI)
# ─────────────────────────────────────────────────────────────
app = FastAPI(title="Mirror Worker")


@app.get("/health")
async def health():
    stats = await redis_streams.health()
    return JSONResponse({
        "status": "ok",
        "stats": stats
    })


# ─────────────────────────────────────────────────────────────
#  UNIVERSAL SENDER — отправляет ВСЕ типы сообщений
# ─────────────────────────────────────────────────────────────
async def send_payload(bot: Bot, payload: Dict[str, Any]):
    msg_type = payload["type"]
    chat_id = payload["target_chat_id"]
    thread_id = payload.get("target_thread_id")
    pin = payload.get("pin", False)

    kwargs = {}
    if thread_id:
        kwargs["message_thread_id"] = thread_id

    sent_message = None

    try:
        if msg_type == "text":
            sent_message = await bot.send_message(
                chat_id=chat_id,
                text=payload["text"],
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )

        elif msg_type == "photo":
            sent_message = await bot.send_photo(
                chat_id=chat_id,
                photo=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "video":
            sent_message = await bot.send_video(
                chat_id=chat_id,
                video=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "document":
            sent_message = await bot.send_document(
                chat_id=chat_id,
                document=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        elif msg_type == "voice":
            sent_message = await bot.send_voice(
                chat_id=chat_id,
                voice=payload["file_id"],
                caption=payload.get("caption"),
                parse_mode="HTML",
                **kwargs
            )

        else:
            raise ValueError(f"Неизвестный тип сообщения: {msg_type}")

        # PIN
        if pin and sent_message:
            try:
                await bot.pin_chat_message(
                    chat_id=chat_id,
                    message_id=sent_message.message_id,
                    disable_notification=True
                )
            except TelegramBadRequest as e:
                logger.warning(f"PIN error: {e}")

        return True

    except TelegramRetryAfter as e:
        # Telegram просит подождать
        logger.warning(f"429 RETRY AFTER {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        return False

    except TelegramBadRequest as e:
        # Нельзя отправить — это не временная ошибка
        logger.error(f"BadRequest → {e}")
        return "fatal"

    except TelegramAPIError as e:
        logger.error(f"Telegram API error: {e}")
        return False

    except Exception as e:
        logger.error(f"Unknown error: {e}")
        return False


# ─────────────────────────────────────────────────────────────
#  PROCESS MESSAGE (main handler)
# ─────────────────────────────────────────────────────────────
async def process_message(message_id: str, payload: Dict[str, Any]) -> bool:
    bot = Bot(token=payload["bot_token"])

    result = await send_payload(bot, payload)

    if result is True:
        return True

    retry = payload.get("attempt", 0)

    if result == "fatal":
        await redis_streams.send_to_dlq(payload, "fatal_error")
        return True

    if retry >= MAX_RETRIES:
        await redis_streams.send_to_dlq(payload, "max_retries_exceeded")
        return True

    # RETRY с экспоненциальной задержкой
    retry_delay = BACKOFF_START * (BACKOFF_BASE ** retry)
    await asyncio.sleep(retry_delay)

    payload["attempt"] = retry + 1
    await redis_streams.enqueue(payload)

    logger.info(f"🔁 RETRY #{retry+1} для payload {payload}")
    return True


# ─────────────────────────────────────────────────────────────
#  WORKER LOOP
# ─────────────────────────────────────────────────────────────
async def worker_loop():
    redis = redis_streams.redis

    logger.info("🚀 Worker started")

    while True:
        try:
            # читаем сообщения из pending + новые
            response = await redis.xreadgroup(
                groupname=GROUP,
                consumername=CONSUMER,
                streams={STREAM_KEY: ">"},
                count=10,
                block=3000
            )

            if not response:
                continue

            for stream_key, messages in response:
                for mid, data in messages:

                    try:
                        payload = json.loads(data["payload"])
                    except Exception:
                        logger.error(f"Некорректный payload: {data}")
                        await redis_streams.ack(mid)
                        continue

                    ok = await process_message(mid, payload)

                    if ok:
                        await redis_streams.ack(mid)

        except Exception as e:
            logger.error(f"Worker error: {e}", exc_info=True)
            await asyncio.sleep(2)


# ─────────────────────────────────────────────────────────────
#  ENTRYPOINT
# ─────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import uvicorn

    loop = asyncio.get_event_loop()
    loop.create_task(worker_loop())

    uvicorn.run(app, host="0.0.0.0", port=8081)
