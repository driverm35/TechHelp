from __future__ import annotations
from aiogram.types import Message
from app.cache.redis import cache_get, cache_set

FWD_TTL = 2 * 60 * 60  # 2 hours

def _fwd_key(msg: Message) -> str:
    return f"fwd:{msg.chat.id}:{msg.message_id}"

async def is_forwarded(msg: Message) -> bool:
    return (await cache_get(_fwd_key(msg))) is not None

async def mark_forwarded(msg: Message) -> None:
    await cache_set(_fwd_key(msg), 1, ttl=FWD_TTL)

def is_service_or_command(msg: Message) -> bool:
    if msg.text and msg.text.startswith("/"):
        return True
    if msg.is_automatic_forward is True:
        return True
    if msg.new_chat_members or msg.left_chat_member or msg.pinned_message:
        return True
    return False
