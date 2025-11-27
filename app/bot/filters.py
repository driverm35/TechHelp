from __future__ import annotations
from aiogram.filters import BaseFilter
from aiogram.types import Message
from app.config import settings

class IsMainGroup(BaseFilter):
    async def __call__(self, m: Message) -> bool:
        return m.chat and m.chat.id == settings.main_group_id
