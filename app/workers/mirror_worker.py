import asyncio
import json
from aiogram.exceptions import TelegramRetryAfter, TelegramBadRequest
from app.bot import bot
from app.utils.redis_streams import redis_streams


MAX_ATTEMPTS = 5


async def process_task(task_id, payload):
    attempt = payload.get("attempt", 0)

    try:
        await bot.copy_message(
            chat_id=payload["target_chat_id"],
            from_chat_id=payload["source_chat_id"],
            message_id=payload["source_message_id"],
            message_thread_id=payload.get("target_thread_id"),
        )

        await redis_streams.ack(task_id)
        await redis_streams.delete(task_id)
        return

    except TelegramRetryAfter as e:
        await asyncio.sleep(e.retry_after)

    except TelegramBadRequest:
        # Нельзя копировать — удаляем
        await redis_streams.ack(task_id)
        await redis_streams.delete(task_id)
        return

    except Exception:
        pass

    # RETRY section
    if attempt >= MAX_ATTEMPTS:
        await redis_streams.ack(task_id)
        return

    payload["attempt"] = attempt + 1
    await redis_streams.enqueue(payload)
    await redis_streams.ack(task_id)


async def worker_loop():
    await redis_streams.connect()
    print("🔥 Mirror Worker started")

    while True:
        messages = await redis_streams.read("consumer1")

        if not messages:
            continue

        for stream, tasks in messages:
            for task_id, fields in tasks:
                payload = json.loads(fields["data"])
                await process_task(task_id, payload)


if __name__ == "__main__":
    asyncio.run(worker_loop())
