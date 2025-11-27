from __future__ import annotations
import functools
from typing import Any, Awaitable, Callable, TypeVar, ParamSpec
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.database import AsyncSessionLocal

P = ParamSpec("P")
R = TypeVar("R")

def with_session(func: Callable[P, Awaitable[R]]) -> Callable[P, Awaitable[R]]:
    """
    Декоратор, который автоматически создаёт AsyncSession, если её не передали.

    Пример:
        @with_session
        async def get_user(session: AsyncSession, user_id: int): ...

        await get_user(user_id=123)                  # создаст сессию сам
        async with AsyncSessionLocal() as s:
            await get_user(session=s, user_id=123)   # использует существующую
    """
    @functools.wraps(func)
    async def wrapper(*args: P.args, **kwargs: P.kwargs) -> R:
        session: AsyncSession | None = kwargs.get("session")
        if session is not None:
            return await func(*args, **kwargs)
        async with AsyncSessionLocal() as s:
            kwargs["session"] = s
            result = await func(*args, **kwargs)
            # По умолчанию коммит не делаем — CRUD решает сам
            return result
    return wrapper
