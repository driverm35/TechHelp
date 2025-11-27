import logging
import time
from typing import Callable, Dict, Any, Awaitable
from aiogram import BaseMiddleware
from aiogram.types import Message, CallbackQuery, TelegramObject

logger = logging.getLogger(__name__)


class LoggingMiddleware(BaseMiddleware):

    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any]
    ) -> Any:

        start_time = time.time()

        # 🔹 Получаем информацию о обработчике
        handler_name = self._get_handler_name(handler, data)

        try:
            # Логируем входящее событие
            if isinstance(event, Message):
                user_info = f"@{event.from_user.username}" if event.from_user.username else f"ID:{event.from_user.id}"
                text = event.text or event.caption or "[медиа]"
                chat_type = event.chat.type if event.chat else "unknown"
                logger.info(
                    f"📩 Сообщение от {user_info} в {chat_type}: {text[:50]}... "
                    f"→ {handler_name}"
                )

            elif isinstance(event, CallbackQuery):
                user_info = f"@{event.from_user.username}" if event.from_user.username else f"ID:{event.from_user.id}"
                logger.info(
                    f"🔘 Callback от {user_info}: {event.data} "
                    f"→ {handler_name}"
                )

            # Выполняем обработчик
            result = await handler(event, data)

            # Логируем время выполнения
            execution_time = time.time() - start_time
            if execution_time > 1.0:
                logger.warning(
                    f"⏱️ Медленная операция в {handler_name}: {execution_time:.2f}s"
                )
            else:
                logger.debug(
                    f"✅ {handler_name} выполнен за {execution_time:.3f}s"
                )

            return result

        except Exception as e:
            execution_time = time.time() - start_time
            logger.error(
                f"❌ Ошибка в {handler_name} за {execution_time:.2f}s: {e}",
                exc_info=True
            )
            raise

    def _get_handler_name(self, handler: Callable, data: Dict[str, Any]) -> str:
        """
        Получить читаемое имя обработчика.

        Args:
            handler: Функция-обработчик
            data: Данные из middleware

        Returns:
            Строка с именем обработчика и модулем
        """
        try:
            # Пытаемся получить оригинальный callback из данных
            if "handler" in data and hasattr(data["handler"], "callback"):
                func = data["handler"].callback
            else:
                func = handler

            # Получаем имя функции
            func_name = getattr(func, "__name__", "unknown")

            # Получаем модуль
            module = getattr(func, "__module__", None)

            if module:
                # Упрощаем путь модуля
                if module.startswith("app.bot.handlers."):
                    # app.bot.handlers.user_bot → user_bot
                    module_short = module.replace("app.bot.handlers.", "")
                elif module.startswith("app."):
                    # app.bot.middlewares.logging → middlewares.logging
                    module_short = module.replace("app.", "")
                else:
                    module_short = module

                return f"{module_short}.{func_name}"
            else:
                return func_name

        except Exception as e:
            logger.debug(f"Не удалось получить имя обработчика: {e}")
            return "unknown_handler"