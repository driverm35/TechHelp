from __future__ import annotations
from typing import Any, Awaitable, Callable, Dict
from aiogram import BaseMiddleware
from aiogram.types import TelegramObject
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import AsyncSessionLocal

class DBSessionMiddleware(BaseMiddleware):
    """
    Открывает AsyncSession на время обработки одного апдейта
    и гарантированно закрывает её (возвращает коннект в пул).
    Достаётся в хэндлерах через параметр `session: AsyncSession`.
    """
    async def __call__(
        self,
        handler: Callable[[TelegramObject, Dict[str, Any]], Awaitable[Any]],
        event: TelegramObject,
        data: Dict[str, Any],
    ) -> Any:
        async with AsyncSessionLocal() as session:  # контекст => close() гарантирован
            data["session"] = session
            try:
                return await handler(event, data)
            finally:
                # Явное закрытие на случай, если кто-то захватил соединение
                await session.close()
