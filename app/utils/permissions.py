# app/bot/utils/permissions.py
from __future__ import annotations

import logging
from aiogram import Bot
from aiogram.enums import ChatMemberStatus

from app.config import settings

logger = logging.getLogger(__name__)


async def is_group_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    """
    Проверка, является ли пользователь админом конкретной группы.

    Право есть если:
      • он в списке глобальных админов (settings.is_admin)
      • или он creator / administrator в этом чате
    """
    # Глобальные админы всегда проходят
    if settings.is_admin(user_id):
        return True

    try:
        member = await bot.get_chat_member(chat_id, user_id)
    except Exception as e:
        logger.warning(
            f"⚠️ Не удалось получить статус участника {user_id} в чате {chat_id}: {e}"
        )
        return False

    return member.status in {ChatMemberStatus.ADMINISTRATOR, ChatMemberStatus.CREATOR}
