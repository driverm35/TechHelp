# app/utils/redis_streams.py
import json
import logging
from typing import Dict, Any, Optional

from redis.asyncio import Redis

logger = logging.getLogger(__name__)

# ==================================================================
# КОНФИГУРАЦИЯ
# ==================================================================
STREAM_KEY = "supportbot:mirror"
DLQ_KEY = "supportbot:dlq"
GROUP = "mirror_group"
MAX_RETRIES = 5


# ==================================================================
# REDIS STREAMS MANAGER
# ==================================================================
class RedisStreamsManager:
    def __init__(self, redis_url: str = "redis://redis:6379/0"):
        self.redis_url = redis_url
        self.redis: Optional[Redis] = None

    async def connect(self):
        """Подключение к Redis"""
        if self.redis is None:
            self.redis = await Redis.from_url(
                self.redis_url,
                encoding="utf-8",
                decode_responses=True
            )
            logger.info("✅ Redis connected")

    async def disconnect(self):
        """Отключение от Redis"""
        if self.redis:
            await self.redis.close()
            self.redis = None
            logger.info("❌ Redis disconnected")

    async def init(self):
        """Инициализация: создание consumer group"""
        try:
            await self.redis.xgroup_create(
                name=STREAM_KEY,
                groupname=GROUP,
                id="0",
                mkstream=True
            )
            logger.info(f"✅ Consumer group '{GROUP}' создана")
        except Exception as e:
            if "BUSYGROUP" in str(e):
                logger.debug(f"Consumer group '{GROUP}' уже существует")
            else:
                logger.error(f"Ошибка создания группы: {e}")

    async def enqueue(self, payload: Dict[str, Any]):
        """
        Добавить сообщение в очередь.
        
        Args:
            payload: Словарь с данными для отправки
                     ОБЯЗАТЕЛЬНО: bot_token, type, target_chat_id
                     ОПЦИОНАЛЬНО: target_thread_id, text, file_id, caption, pin
        """
        if not self.redis:
            await self.connect()

        payload_json = json.dumps(payload, ensure_ascii=False)

        msg_id = await self.redis.xadd(
            name=STREAM_KEY,
            fields={"payload": payload_json}
        )

        logger.debug(f"➕ Enqueued: {msg_id} → {payload.get('type')} to {payload.get('target_chat_id')}")
        return msg_id

    async def ack(self, message_id: str):
        """Подтвердить обработку сообщения"""
        await self.redis.xack(STREAM_KEY, GROUP, message_id)
        logger.debug(f"✅ ACK: {message_id}")

    async def send_to_dlq(self, payload: Dict[str, Any], reason: str):
        """Отправить в Dead Letter Queue"""
        dlq_data = {
            "payload": json.dumps(payload, ensure_ascii=False),
            "reason": reason
        }
        await self.redis.xadd(DLQ_KEY, fields=dlq_data)
        logger.warning(f"💀 DLQ: {reason} → {payload.get('ticket_id', 'N/A')}")

    async def health(self) -> Dict[str, Any]:
        """Статистика для health-check"""
        try:
            pending = await self.redis.xpending(STREAM_KEY, GROUP)
            stream_len = await self.redis.xlen(STREAM_KEY)
            dlq_len = await self.redis.xlen(DLQ_KEY)

            return {
                "stream_length": stream_len,
                "pending_messages": pending["pending"] if pending else 0,
                "dlq_length": dlq_len,
            }
        except Exception as e:
            logger.error(f"Health check error: {e}")
            return {"error": str(e)}


# ==================================================================
# SINGLETON INSTANCE
# ==================================================================
redis_streams = RedisStreamsManager()