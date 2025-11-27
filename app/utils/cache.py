# app/utils/cache.py
import json
import logging
from typing import Any, Optional, Union
from datetime import timedelta
import redis.asyncio as redis

from app.config import settings

logger = logging.getLogger(__name__)


class CacheService:
    """
    Сервис кеширования
    Используется для:
    1. Уменьшения запросов к БД (списки техников, маппинги топиков)
    2. Throttling и rate limiting
    3. Быстрого поиска тикетов по топикам
    """

    def __init__(self):
        self.redis_client: Optional[redis.Redis] = None
        self._connected = True

    async def connect(self):
        """Подключение к Redis с fallback на dev режим."""
        # В dev режиме Redis не используется
        if not settings.use_redis:
            logger.info("⚠️ DEV режим - Redis кеш отключен")
            self._connected = False
            return

        try:
            self.redis_client = redis.from_url(
                settings.redis_url,
                decode_responses=True,
                max_connections=20,
                socket_keepalive=True,
                socket_connect_timeout=5,
                retry_on_timeout=True,
            )
            await self.redis_client.ping()
            self._connected = True
            logger.info("✅ Подключение к Redis кешу установлено")
        except Exception as e:
            logger.warning(f"⚠️ Redis недоступен: {e}")
            logger.info("💡 Работа продолжается без кеширования")
            self._connected = False

    async def disconnect(self):
        """Корректное отключение от Redis."""
        if self.redis_client:
            await self.redis_client.close()
            self._connected = False
            logger.info("Redis кеш отключен")

    # ═══════════════════════════════════════════════════════════
    # Базовые операции
    # ═══════════════════════════════════════════════════════════

    async def get(self, key: str) -> Optional[Any]:
        """Получить значение из кеша."""
        if not self._connected:
            return None

        try:
            value = await self.redis_client.get(key)
            if value:
                return json.loads(value)
            return None
        except Exception as e:
            logger.error(f"Ошибка получения из кеша {key}: {e}")
            return None

    async def set(
        self,
        key: str,
        value: Any,
        expire: Union[int, timedelta] = None
    ) -> bool:
        """Записать значение в кеш."""
        if not self._connected:
            return False

        try:
            serialized_value = json.dumps(value, default=str)

            if isinstance(expire, timedelta):
                expire = int(expire.total_seconds())

            await self.redis_client.set(key, serialized_value, ex=expire)
            return True
        except Exception as e:
            logger.error(f"Ошибка записи в кеш {key}: {e}")
            return False

    async def delete(self, key: str) -> bool:
        """Удалить ключ из кеша."""
        if not self._connected:
            return False

        try:
            deleted = await self.redis_client.delete(key)
            return deleted > 0
        except Exception as e:
            logger.error(f"Ошибка удаления из кеша {key}: {e}")
            return False

    async def delete_pattern(self, pattern: str) -> int:
        """Удалить все ключи по паттерну."""
        if not self._connected:
            return 0

        try:
            keys = await self.redis_client.keys(pattern)
            if not keys:
                return 0

            deleted = await self.redis_client.delete(*keys)
            return int(deleted)
        except Exception as e:
            logger.error(f"Ошибка удаления ключей по шаблону {pattern}: {e}")
            return 0

    async def exists(self, key: str) -> bool:
        """Проверить существование ключа."""
        if not self._connected:
            return False

        try:
            return await self.redis_client.exists(key)
        except Exception as e:
            logger.error(f"Ошибка проверки существования в кеше {key}: {e}")
            return False

    async def expire(self, key: str, seconds: int) -> bool:
        """Установить TTL для ключа."""
        if not self._connected:
            return False

        try:
            return await self.redis_client.expire(key, seconds)
        except Exception as e:
            logger.error(f"Ошибка установки TTL для {key}: {e}")
            return False

    async def increment(self, key: str, amount: int = 1) -> Optional[int]:
        """Инкремент значения."""
        if not self._connected:
            return None

        try:
            return await self.redis_client.incrby(key, amount)
        except Exception as e:
            logger.error(f"Ошибка инкремента {key}: {e}")
            return None

    # ═══════════════════════════════════════════════════════════
    # ТЕХНИКИ - кешируем список (меняется редко, читается часто)
    # ═══════════════════════════════════════════════════════════

    async def get_technicians(self) -> Optional[list[dict]]:
        """
        Получить список техников из кеша.

        Кешируется на 10 минут, т.к. техники добавляются редко,
        но список запрашивается при каждом создании тикета.
        """
        return await self.get("technicians:active")

    async def set_technicians(self, technicians: list[dict]) -> bool:
        """Закешировать список техников на 10 минут."""
        return await self.set("technicians:active", technicians, expire=600)

    async def invalidate_technicians(self) -> bool:
        """Сбросить кеш техников после изменений."""
        return await self.delete("technicians:active")

    async def get_technician_group(self, tech_id: int) -> Optional[int]:
        """
        Получить group_chat_id техника.

        Критично для зеркалирования - запрашивается при каждом сообщении.
        """
        key = f"tech:{tech_id}:group"
        return await self.get(key)

    async def set_technician_group(
        self,
        tech_id: int,
        group_chat_id: int
    ) -> bool:
        """Закешировать group_chat_id техника на 30 минут."""
        key = f"tech:{tech_id}:group"
        return await self.set(key, group_chat_id, expire=1800)

    # ═══════════════════════════════════════════════════════════
    # ТОПИКИ - критичный маппинг для быстрого поиска
    # ═══════════════════════════════════════════════════════════

    async def get_ticket_by_main_thread(
        self,
        main_chat_id: int,
        main_thread_id: int
    ) -> Optional[int]:
        """
        Получить ticket_id по топику главной группы.

        Используется при каждом сообщении из главной группы.
        Кеш на 1 час - топики не удаляются, только закрываются.
        """
        key = f"thread:main:{main_chat_id}:{main_thread_id}"
        return await self.get(key)

    async def set_ticket_by_main_thread(
        self,
        main_chat_id: int,
        main_thread_id: int,
        ticket_id: int
    ) -> bool:
        """Закешировать маппинг главного топика."""
        key = f"thread:main:{main_chat_id}:{main_thread_id}"
        return await self.set(key, ticket_id, expire=3600)

    async def get_ticket_by_tech_thread(
        self,
        tech_chat_id: int,
        tech_thread_id: int
    ) -> Optional[int]:
        """
        Получить ticket_id по топику техника.

        Используется при каждом сообщении от техника.
        """
        key = f"thread:tech:{tech_chat_id}:{tech_thread_id}"
        return await self.get(key)

    async def set_ticket_by_tech_thread(
        self,
        tech_chat_id: int,
        tech_thread_id: int,
        ticket_id: int
    ) -> bool:
        """Закешировать маппинг топика техника."""
        key = f"thread:tech:{tech_chat_id}:{tech_thread_id}"
        return await self.set(key, ticket_id, expire=3600)

    async def get_tech_thread_by_ticket(
        self,
        ticket_id: int,
        tech_id: int
    ) -> Optional[dict]:
        """
        Получить tech_thread по ticket_id и tech_id.

        Нужно для зеркалирования сообщений.
        Возвращает: {"tech_chat_id": ..., "tech_thread_id": ...}
        """
        key = f"ticket:{ticket_id}:tech:{tech_id}:thread"
        return await self.get(key)

    async def set_tech_thread_by_ticket(
        self,
        ticket_id: int,
        tech_id: int,
        tech_chat_id: int,
        tech_thread_id: int
    ) -> bool:
        """Закешировать TechThread."""
        key = f"ticket:{ticket_id}:tech:{tech_id}:thread"
        data = {
            "tech_chat_id": tech_chat_id,
            "tech_thread_id": tech_thread_id
        }
        return await self.set(key, data, expire=3600)

    async def invalidate_ticket_threads(self, ticket_id: int) -> int:
        """Сбросить все кеши топиков для тикета."""
        pattern = f"ticket:{ticket_id}:*"
        return await self.delete_pattern(pattern)

    # ═══════════════════════════════════════════════════════════
    # АКТИВНЫЕ ТИКЕТЫ КЛИЕНТА - для быстрого доступа
    # ═══════════════════════════════════════════════════════════

    async def get_active_ticket(self, user_id: int) -> Optional[int]:
        """
        Получить ID активного тикета клиента.

        Кешируется на 5 минут для быстрого ответа при новых сообщениях.
        """
        key = f"user:{user_id}:active_ticket"
        return await self.get(key)

    async def set_active_ticket(self, user_id: int, ticket_id: int) -> bool:
        """Закешировать активный тикет клиента."""
        key = f"user:{user_id}:active_ticket"
        return await self.set(key, ticket_id, expire=300)

    async def clear_active_ticket(self, user_id: int) -> bool:
        """Очистить активный тикет (при закрытии)."""
        key = f"user:{user_id}:active_ticket"
        return await self.delete(key)

    async def get_ticket_messages_cached(
        self,
        ticket_id: int
    ) -> list[dict] | None:
        """Получить кешированные сообщения тикета."""
        key = f"messages:ticket:{ticket_id}"
        return await self.get(key)

    async def invalidate_ticket_messages(self, ticket_id: int) -> bool:
        """Сбросить кеш сообщений тикета."""
        key = f"messages:ticket:{ticket_id}"
        return await self.delete(key)
    # ═══════════════════════════════════════════════════════════
    # СТАТИСТИКА - для дашбордов и отчетов
    # ═══════════════════════════════════════════════════════════

    async def get_ticket_stats(
        self,
        tech_id: Optional[int] = None
    ) -> Optional[dict]:
        """Получить статистику тикетов (кеш на 5 минут)."""
        key = f"stats:tickets:{tech_id or 'all'}"
        return await self.get(key)

    async def set_ticket_stats(
        self,
        stats: dict,
        tech_id: Optional[int] = None
    ) -> bool:
        """Закешировать статистику тикетов."""
        key = f"stats:tickets:{tech_id or 'all'}"
        return await self.set(key, stats, expire=300)

    async def increment_daily_tickets(self) -> int:
        """
        Инкремент счетчика тикетов за сегодня.

        Используется для мониторинга нагрузки.
        """
        from datetime import datetime
        date_key = datetime.utcnow().strftime("%Y-%m-%d")
        key = f"stats:daily:{date_key}:tickets"

        count = await self.increment(key)
        if count == 1:
            # Устанавливаем TTL на 7 дней для первой записи
            await self.expire(key, 604800)
        return count

    # ═══════════════════════════════════════════════════════════
    # RATE LIMITING - защита от спама
    # ═══════════════════════════════════════════════════════════

    async def check_rate_limit(
        self,
        user_id: int,
        action: str = "message",
        limit: int = 5,
        window: int = 10
    ) -> bool:
        """
        Проверить rate limit.

        Args:
            user_id: ID пользователя
            action: Тип действия (message, callback, etc)
            limit: Максимум действий
            window: Окно времени в секундах

        Returns:
            True если лимит НЕ превышен
        """
        if not self._connected:
            return True

        key = f"rate:{user_id}:{action}"

        try:
            current = await self.redis_client.incr(key)

            if current == 1:
                await self.redis_client.expire(key, window)

            return current <= limit
        except Exception as e:
            logger.error(f"Ошибка rate limit для {user_id}: {e}")
            return True

    async def reset_rate_limit(self, user_id: int, action: str) -> bool:
        """Сбросить rate limit для пользователя."""
        key = f"rate:{user_id}:{action}"
        return await self.delete(key)

    # ═══════════════════════════════════════════════════════════
    # СЕССИИ FSM - временные данные
    # ═══════════════════════════════════════════════════════════

    async def get_user_session(
        self,
        user_id: int,
        session_key: str
    ) -> Optional[Any]:
        """Получить данные сессии пользователя."""
        key = f"session:{user_id}:{session_key}"
        return await self.get(key)

    async def set_user_session(
        self,
        user_id: int,
        session_key: str,
        data: Any,
        expire: int = 1800
    ) -> bool:
        """Сохранить данные сессии (по умолчанию на 30 минут)."""
        key = f"session:{user_id}:{session_key}"
        return await self.set(key, data, expire)

    async def delete_user_session(
        self,
        user_id: int,
        session_key: str
    ) -> bool:
        """Удалить данные сессии."""
        key = f"session:{user_id}:{session_key}"
        return await self.delete(key)

    # ═══════════════════════════════════════════════════════════
    # УТИЛИТЫ
    # ═══════════════════════════════════════════════════════════

    async def get_keys(self, pattern: str = "*") -> list:
        """Получить список ключей по паттерну (для отладки)."""
        if not self._connected:
            return []

        try:
            keys = await self.redis_client.keys(pattern)
            return [key for key in keys]
        except Exception as e:
            logger.error(f"Ошибка получения ключей по паттерну {pattern}: {e}")
            return []

    async def flush_all(self) -> bool:
        """ОПАСНО: Очистить весь кеш."""
        if not self._connected:
            return False

        try:
            await self.redis_client.flushall()
            logger.warning("🗑️ Весь кеш очищен!")
            return True
        except Exception as e:
            logger.error(f"Ошибка очистки кеша: {e}")
            return False

    async def get_cache_info(self) -> dict:
        """Получить информацию о состоянии кеша."""
        if not self._connected:
            return {"connected": False}

        try:
            info = await self.redis_client.info()
            return {
                "connected": True,
                "used_memory_human": info.get("used_memory_human"),
                "connected_clients": info.get("connected_clients"),
                "total_keys": await self.redis_client.dbsize(),
            }
        except Exception as e:
            logger.error(f"Ошибка получения информации о кеше: {e}")
            return {"connected": False, "error": str(e)}

    async def get_topic_title(self, chat_id: int, thread_id: int) -> str | None:
        """Получить закешированное название топика."""
        key = f"topic_title:{chat_id}:{thread_id}"
        return await self.get(key)

    async def set_topic_title(
        self,
        chat_id: int,
        thread_id: int,
        title: str
    ) -> bool:
        """Сохранить название топика в кеш."""
        key = f"topic_title:{chat_id}:{thread_id}"
        return await self.set(key, title, expire=86400)

class RateLimitCache:
    """
    Класс для совместимости со старым API.
    Делегирует вызовы глобальному экземпляру cache.
    """

    @staticmethod
    async def is_rate_limited(
        user_id: int,
        action: str,
        limit: int,
        window: int
    ) -> bool:
        """
        Проверить rate limit (совместимость).

        Returns:
            True если лимит ПРЕВЫШЕН (инверсия от check_rate_limit!)
        """
        is_allowed = await cache.check_rate_limit(
            user_id=user_id,
            action=action,
            limit=limit,
            window=window
        )
        # Инверсия: старый API возвращал True если заблокирован
        return not is_allowed

    @staticmethod
    async def reset_rate_limit(user_id: int, action: str) -> bool:
        """Сбросить rate limit (совместимость)."""
        return await cache.reset_rate_limit(user_id, action)


class UserCache:
    """Хелперы для кеширования данных пользователя."""

    @staticmethod
    async def get_user_data(user_id: int) -> Optional[dict]:
        """Получить данные пользователя."""
        key = f"user:{user_id}:data"
        return await cache.get(key)

    @staticmethod
    async def set_user_data(
        user_id: int,
        data: dict,
        expire: int = 3600
    ) -> bool:
        """Сохранить данные пользователя."""
        key = f"user:{user_id}:data"
        return await cache.set(key, data, expire)

    @staticmethod
    async def delete_user_data(user_id: int) -> bool:
        """Удалить данные пользователя."""
        key = f"user:{user_id}:data"
        return await cache.delete(key)

    @staticmethod
    async def get_user_session(
        user_id: int,
        session_key: str
    ) -> Optional[Any]:
        """Получить сессию пользователя."""
        return await cache.get_user_session(user_id, session_key)

    @staticmethod
    async def set_user_session(
        user_id: int,
        session_key: str,
        data: Any,
        expire: int = 1800
    ) -> bool:
        """Сохранить сессию пользователя."""
        return await cache.set_user_session(
            user_id,
            session_key,
            data,
            expire
        )

    @staticmethod
    async def delete_user_session(
        user_id: int,
        session_key: str
    ) -> bool:
        """Удалить сессию пользователя."""
        return await cache.delete_user_session(user_id, session_key)


class SystemCache:
    """Системный кеш для статистики и мониторинга."""

    @staticmethod
    async def get_system_stats() -> Optional[dict]:
        """Получить системную статистику."""
        return await cache.get("system:stats")

    @staticmethod
    async def set_system_stats(
        stats: dict,
        expire: int = 300
    ) -> bool:
        """Сохранить системную статистику."""
        return await cache.set("system:stats", stats, expire)

    @staticmethod
    async def get_daily_stats(date: str) -> Optional[dict]:
        """Получить статистику за день."""
        key = f"stats:daily:{date}"
        return await cache.get(key)

    @staticmethod
    async def set_daily_stats(date: str, stats: dict) -> bool:
        """Сохранить статистику за день."""
        key = f"stats:daily:{date}"
        return await cache.set(key, stats, 86400)

# Глобальный экземпляр кеша
cache = CacheService()


def cache_key(*parts) -> str:
    """Вспомогательная функция для создания ключей."""
    return ":".join(str(part) for part in parts)

async def cached_function(key: str, expire: int = 300):
    """Декоратор для кеширования результатов функций."""
    def decorator(func):
        async def wrapper(*args, **kwargs):
            cache_result = await cache.get(key)
            if cache_result is not None:
                return cache_result

            result = await func(*args, **kwargs)
            await cache.set(key, result, expire)
            return result

        return wrapper
    return decorator