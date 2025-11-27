from __future__ import annotations
from aiogram import BaseMiddleware
from aiogram.types import Update
from typing import Callable, Awaitable, Any
import logging

log = logging.getLogger("updates")

class LoggingMiddleware(BaseMiddleware):
    async def __call__(self, handler: Callable[[Update, dict[str, Any]], Awaitable[Any]], event: Update, data: dict[str, Any]) -> Any:
        log.info("update", extra={"update": event.event_type})
        return await handler(event, data)
