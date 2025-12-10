import json
from app.config import settings


STREAM_NAME = "supportbot:mirror"


class MirrorQueue:

    @staticmethod
    async def enqueue(redis, payload: dict):
        """
        Добавляет задачу в Redis Stream.
        """
        await redis.xadd(
            STREAM_NAME,
            {"data": json.dumps(payload)},
            approx=False
        )
