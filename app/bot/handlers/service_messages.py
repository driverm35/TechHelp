# app/bot/handlers/service_messages.py
"""
Обработчик системных сообщений в группах.
Автоматически удаляет служебные сообщения для чистоты чатов.
"""
from __future__ import annotations

import logging
import asyncio
from aiogram import Dispatcher, F, Bot
from aiogram.types import Message
from aiogram.enums import ChatType
from aiogram.exceptions import TelegramBadRequest

from app.config import settings

logger = logging.getLogger(__name__)


async def _safe_delete_message(
    bot: Bot,
    chat_id: int,
    message_id: int,
    delay: float = 0.0
) -> bool:
    """
    Безопасно удалить сообщение с опциональной задержкой.

    Args:
        bot: Экземпляр бота
        chat_id: ID чата
        message_id: ID сообщения
        delay: Задержка перед удалением (секунды)

    Returns:
        True если удалено успешно
    """
    try:
        if delay > 0:
            await asyncio.sleep(delay)

        await bot.delete_message(chat_id=chat_id, message_id=message_id)
        logger.debug(f"🗑 Удалено служебное сообщение {message_id} в чате {chat_id}")
        return True
    except TelegramBadRequest as e:
        if "message to delete not found" in str(e).lower():
            logger.debug(f"⚠️ Сообщение {message_id} уже удалено")
        elif "not enough rights" in str(e).lower():
            logger.warning(f"⚠️ Нет прав на удаление сообщения {message_id}")
        else:
            logger.warning(f"⚠️ Не удалось удалить сообщение {message_id}: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка удаления сообщения {message_id}: {e}")
        return False


async def handle_service_messages(message: Message, bot: Bot) -> None:
    logger.info(
        f"[service] message_id={message.message_id}, "
        f"chat_id={message.chat.id}, "
        f"text={repr(message.text)}"
    )
    """
    Обработка и удаление системных сообщений в группах.

    Удаляет ВСЕ служебные сообщения (включая от бота):
    - Сообщения о закреплении
    - Изменения топиков
    - Добавление/удаление участников
    - Изменение названия/фото чата
    - Другие служебные сообщения
    """
    # Проверяем, включено ли автоудаление
    if not settings.auto_delete_service_messages:
        return

    # Список причин для удаления
    should_delete = False
    reason = ""

    # 1. Сообщения о закреплении (включая от бота!)
    if message.pinned_message and settings.delete_pinned_messages:
        should_delete = True
        reason = "pinned_message"

    # 2. Изменения форум-топиков
    elif settings.delete_topic_changes and any([
        message.forum_topic_created,
        message.forum_topic_closed,
        message.forum_topic_reopened,
        message.forum_topic_edited,
        message.general_forum_topic_hidden,
        message.general_forum_topic_unhidden,
    ]):
        should_delete = True
        reason = "forum_topic_change"

    # 3. Проверка на service_type для других изменений топиков
    elif hasattr(message, 'service_type') and message.service_type:
        should_delete = True
        reason = f"service_type_{message.service_type}"

    # 4. Проверка текста на типичные фразы (от бота тоже!)
    elif message.text and any([
        "changed the" in message.text.lower(),
        "изменил" in message.text.lower(),
        "изменила" in message.text.lower(),
        "renamed" in message.text.lower(),
        "переименовал" in message.text.lower(),
        "переименовала" in message.text.lower(),
        "pinned" in message.text.lower(),
        "закрепил" in message.text.lower(),
        "закрепила" in message.text.lower(),
    ]):
        should_delete = True
        reason = "topic_rename_text"

    # 5. Новые участники
    elif message.new_chat_members and settings.delete_new_chat_members:
        should_delete = True
        reason = "new_chat_members"

    # 6. Участник покинул чат
    elif message.left_chat_member and settings.delete_left_chat_member:
        should_delete = True
        reason = "left_chat_member"

    # 7. Изменение названия чата
    elif message.new_chat_title and settings.delete_chat_title_changes:
        should_delete = True
        reason = "new_chat_title"

    # 8. Изменение фото чата
    elif settings.delete_chat_photo_changes and any([
        message.new_chat_photo,
        message.delete_chat_photo,
    ]):
        should_delete = True
        reason = "chat_photo_change"

    # 9. Другие служебные сообщения
    elif any([
        message.group_chat_created,
        message.supergroup_chat_created,
        message.channel_chat_created,
        message.migrate_to_chat_id,
        message.migrate_from_chat_id,
        message.message_auto_delete_timer_changed,
        message.video_chat_scheduled,
        message.video_chat_started,
        message.video_chat_ended,
        message.video_chat_participants_invited,
        message.web_app_data,
        message.proximity_alert_triggered,
    ]):
        should_delete = True
        reason = "other_service_message"

    # Удаляем сообщение
    if should_delete:
        # Минимальная задержка для изменений топиков (чтобы не было видно мельканий)
        if reason in ["forum_topic_change", "topic_rename_text"]:
            delay = 0.05  # Очень быстро удаляем изменения топиков
        elif reason == "pinned_message":
            delay = 0.1  # Быстро удаляем закрепления
        else:
            delay = 0.3

        asyncio.create_task(
            _safe_delete_message(
                bot,
                message.chat.id,
                message.message_id,
                delay=delay
            )
        )

        from_who = "от бота" if message.from_user and message.from_user.is_bot else "от пользователя"
        logger.debug(
            f"🗑 Запланировано удаление служебного сообщения ({from_who}): {reason} "
            f"в чате {message.chat.id}"
        )


def register_handlers(dp: Dispatcher) -> None:
    """Регистрация обработчиков служебных сообщений."""
    logger.info("🔧 === НАЧАЛО регистрации обработчиков service_messages.py ===")

    if not settings.auto_delete_service_messages:
        logger.info("ℹ️ Автоудаление служебных сообщений ОТКЛЮЧЕНО")
        logger.info("🔧 === КОНЕЦ регистрации обработчиков service_messages.py ===")
        return

    # Регистрируем обработчик для всех служебных сообщений в группах
    # БЕЗ фильтра на ботов - удаляем ВСЕ системные сообщения
    dp.message.register(
        handle_service_messages,
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    )

    logger.info("✅ Зарегистрирован обработчик удаления служебных сообщений")
    logger.info("   🤖 Удаляются сообщения от ВСЕХ (включая ботов)")
    logger.info(f"   📌 Закрепления: {'✅' if settings.delete_pinned_messages else '❌'}")
    logger.info(f"   🗂 Топики: {'✅' if settings.delete_topic_changes else '❌'}")
    logger.info(f"   👥 Участники: {'✅' if settings.delete_new_chat_members else '❌'}")
    logger.info(f"   👋 Выход: {'✅' if settings.delete_left_chat_member else '❌'}")
    logger.info(f"   📝 Название: {'✅' if settings.delete_chat_title_changes else '❌'}")
    logger.info(f"   🖼 Фото: {'✅' if settings.delete_chat_photo_changes else '❌'}")
    logger.info("🔧 === КОНЕЦ регистрации обработчиков service_messages.py ===")
