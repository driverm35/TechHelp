import logging
from aiogram import Bot, Dispatcher, types, BaseMiddleware
from aiogram.enums import ChatType
from aiogram.fsm.storage.redis import RedisStorage
from aiogram.fsm.storage.memory import MemoryStorage
import redis.asyncio as redis
from typing import Callable, Dict, Any, Awaitable

from app.bot.middlewares.global_error import GlobalErrorMiddleware
from app.bot.middlewares.logging import LoggingMiddleware
from app.bot.middlewares.throttling import ThrottlingMiddleware
from app.config import settings
from app.utils.cache import cache
from app.utils.permissions import is_group_admin
from app.utils.redis_streams import redis_streams 

from app.bot.handlers import (
    admin,
    start,
    user_bot,
    user_poll,
    tech_group,
    main_group,
    tech_mirror,
    service_messages,
)

logger = logging.getLogger(__name__)


async def debug_callback_handler(callback: types.CallbackQuery):
    logger.info("🔍 DEBUG CALLBACK:")
    logger.info(f"  - Data: {callback.data}")
    logger.info(f"  - User: {callback.from_user.id}")
    logger.info(f"  - Username: {callback.from_user.username}")


class GroupCallbacksGuardMiddleware(BaseMiddleware):
    """Middleware: запрещает нажимать кнопки в группах неадминам."""

    async def __call__(
        self,
        handler: Callable[[types.CallbackQuery, Dict[str, Any]], Awaitable[Any]],
        event: types.CallbackQuery,
        data: Dict[str, Any]
    ) -> Any:
        if not event.message:
            return await handler(event, data)

        chat = event.message.chat

        if chat.type not in {ChatType.GROUP, ChatType.SUPERGROUP}:
            return await handler(event, data)

        bot = data.get("bot")

        if await is_group_admin(bot, chat.id, event.from_user.id):
            return await handler(event, data)

        try:
            await event.answer(
                "⛔ Только администраторы этой группы могут нажимать эти кнопки.",
                show_alert=True,
            )
        except Exception:
            pass

        return None


async def setup_bot() -> tuple[Bot, Dispatcher]:
    """Настройка и инициализация бота."""

    # ✅ 1. Инициализируем Redis Streams
    try:
        await redis_streams.connect()
        await redis_streams.init()
        logger.info("✅ Redis Streams инициализирован")
    except Exception as e:
        logger.error(f"❌ Redis Streams не инициализирован: {e}")
        # В зависимости от важности - можно raise или продолжить
        if settings.app_env.lower() in ("prod", "production"):
            raise
        else:
            logger.warning("⚠️ Продолжаем без Redis Streams (DEV режим)")

    # ✅ 2. Подключаем кеш
    try:
        await cache.connect()
        if cache._connected:
            logger.info("✅ Кеш инициализирован и подключен")
        else:
            logger.info("ℹ️ Кеш инициализирован (без Redis)")
    except Exception as e:
        logger.warning(f"⚠️ Кеш не инициализирован: {e}")

    from aiogram.client.default import DefaultBotProperties
    from aiogram.enums import ParseMode

    bot = Bot(
        token=settings.bot_token,
        default=DefaultBotProperties(parse_mode=ParseMode.HTML)
    )

    # FSM Storage
    storage = None
    if settings.use_redis:
        try:
            redis_client = redis.from_url(
                settings.redis_url,
                decode_responses=True
            )
            await redis_client.ping()
            storage = RedisStorage(redis_client)
            logger.info("✅ Подключено к Redis для FSM storage")
        except Exception as e:
            logger.warning(f"⚠️ Redis для FSM недоступен: {e}")
            logger.info("💡 Используется MemoryStorage")
            storage = MemoryStorage()
    else:
        storage = MemoryStorage()
        logger.info("ℹ️ Используется MemoryStorage (dev режим)")

    dp = Dispatcher(storage=storage)

    dp.message.middleware(GlobalErrorMiddleware())
    dp.callback_query.middleware(GlobalErrorMiddleware())
    dp.message.middleware(LoggingMiddleware())
    dp.callback_query.middleware(LoggingMiddleware())

    # Guard для callback в группах
    dp.callback_query.middleware(GroupCallbacksGuardMiddleware())

    admin.register_handlers(dp)
    tech_group.register_handlers(dp)
    main_group.register_handlers(dp)
    tech_mirror.register_handlers(dp)
    start.register_handlers(dp)
    user_poll.register_handlers(dp)
    user_bot.register_handlers(dp)
    service_messages.register_handlers(dp)

    logger.info("🛡️ GlobalErrorMiddleware активирован")
    logger.info("✅ Бот успешно настроен")

    return bot, dp


async def shutdown_bot():
    """Корректное завершение работы бота."""
    try:
        await redis_streams.disconnect()
        logger.info("✅ Redis Streams отключен")
    except Exception as e:
        logger.error(f"❌ Ошибка отключения Redis Streams: {e}")

    try:
        await cache.close()
        logger.info("✅ Соединения с кешем закрыты")
    except Exception as e:
        logger.error(f"❌ Ошибка закрытия кеша: {e}")