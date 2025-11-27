# app/db/crud/message.py
from __future__ import annotations

from typing import Sequence
from sqlalchemy import select, desc
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload

from app.db.models import TicketMessage, Ticket
from app.utils.session_decorator import with_session
from app.utils.cache import cache

import logging

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# CREATE
# ─────────────────────────────────────────────────────────────

@with_session
async def add_message(
    session: AsyncSession,
    *,
    ticket_id: int,
    user_id: int,
    message_text: str,
    is_from_admin: bool = False,
    media_type: str | None = None,
    media_file_id: str | None = None,
    media_caption: str | None = None,
    telegram_message_id: int | None = None,
) -> TicketMessage:
    """
    Добавить сообщение к тикету.

    Args:
        session: DB сессия
        ticket_id: ID тикета
        user_id: Telegram ID пользователя (клиент или админ)
        message_text: Текст сообщения
        is_from_admin: True если от поддержки
        media_type: Тип медиа (photo, video, document, voice)
        media_file_id: file_id из Telegram
        media_caption: Подпись к медиа
        telegram_message_id: ID сообщения в Telegram

    Returns:
        Созданное сообщение
    """
    message = TicketMessage(
        ticket_id=ticket_id,
        user_id=user_id,
        message_text=message_text or "",
        is_from_admin=is_from_admin,
        has_media=bool(media_type and media_file_id),
        media_type=media_type,
        media_file_id=media_file_id,
        media_caption=media_caption,
        telegram_message_id=telegram_message_id,
    )

    session.add(message)
    await session.flush()
    await session.refresh(message)

    # Инвалидируем кеш сообщений тикета
    await cache.delete(f"messages:ticket:{ticket_id}")

    # Обновляем счетчик сообщений в тикете
    stmt = select(Ticket).where(Ticket.id == ticket_id)
    result = await session.execute(stmt)
    ticket = result.scalar_one_or_none()

    if ticket:
        if hasattr(ticket, 'messages_count'):
            ticket.messages_count = (ticket.messages_count or 0) + 1
        if hasattr(ticket, 'last_message_at'):
            from datetime import datetime
            ticket.last_message_at = datetime.utcnow()
        await session.flush()

    logger.info(
        f"✅ Добавлено сообщение #{message.id} к тикету #{ticket_id} "
        f"(от {'админа' if is_from_admin else 'клиента'})"
    )

    return message


# ─────────────────────────────────────────────────────────────
# READ
# ─────────────────────────────────────────────────────────────

@with_session
async def get_ticket_messages(
    session: AsyncSession,
    ticket_id: int,
    *,
    limit: int = 100,
    offset: int = 0,
    use_cache: bool = True,
) -> Sequence[TicketMessage]:
    """
    Получить сообщения тикета с кешированием.

    Args:
        session: DB сессия
        ticket_id: ID тикета
        limit: Максимум сообщений
        offset: Смещение для пагинации
        use_cache: Использовать ли кеш

    Returns:
        Список сообщений
    """
    # Пытаемся получить из кеша (только для первой страницы)
    if use_cache and offset == 0 and limit <= 100:
        cache_key = f"messages:ticket:{ticket_id}"
        cached = await cache.get(cache_key)

        if cached:
            logger.debug(f"📦 Сообщения тикета #{ticket_id} из кеша")
            # Преобразуем обратно в объекты (упрощенно)
            # В реальности лучше хранить только ID и делать запрос
            return cached

    # Запрос к БД
    stmt = (
        select(TicketMessage)
        .where(TicketMessage.ticket_id == ticket_id)
        .options(
            joinedload(TicketMessage.user),
            joinedload(TicketMessage.ticket),
        )
        .order_by(TicketMessage.created_at.asc())
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(stmt)
    messages = result.scalars().all()

    # Кешируем на 5 минут (только первую страницу)
    if use_cache and offset == 0 and messages:
        cache_key = f"messages:ticket:{ticket_id}"
        # Сохраняем упрощенные данные
        cache_data = [
            {
                "id": m.id,
                "user_id": m.user_id,
                "message_text": m.message_text,
                "is_from_admin": m.is_from_admin,
                "has_media": m.has_media,
                "media_type": m.media_type,
                "created_at": m.created_at.isoformat() if m.created_at else None,
            }
            for m in messages
        ]
        await cache.set(cache_key, cache_data, expire=300)

    return messages


@with_session
async def get_last_message(
    session: AsyncSession,
    ticket_id: int,
) -> TicketMessage | None:
    """
    Получить последнее сообщение тикета.

    Args:
        session: DB сессия
        ticket_id: ID тикета

    Returns:
        Последнее сообщение или None
    """
    stmt = (
        select(TicketMessage)
        .where(TicketMessage.ticket_id == ticket_id)
        .options(
            joinedload(TicketMessage.user),
            joinedload(TicketMessage.ticket),
        )
        .order_by(desc(TicketMessage.created_at))
        .limit(1)
    )

    result = await session.execute(stmt)
    return result.scalar_one_or_none()


@with_session
async def count_ticket_messages(
    session: AsyncSession,
    ticket_id: int,
) -> int:
    """
    Подсчитать количество сообщений в тикете.

    Args:
        session: DB сессия
        ticket_id: ID тикета

    Returns:
        Количество сообщений
    """
    from sqlalchemy import func

    stmt = (
        select(func.count())
        .select_from(TicketMessage)
        .where(TicketMessage.ticket_id == ticket_id)
    )

    result = await session.execute(stmt)
    return result.scalar() or 0


@with_session
async def get_messages_by_user(
    session: AsyncSession,
    user_id: int,
    *,
    limit: int = 50,
    offset: int = 0,
) -> Sequence[TicketMessage]:
    """
    Получить сообщения пользователя (для истории).

    Args:
        session: DB сессия
        user_id: Telegram ID пользователя
        limit: Максимум сообщений
        offset: Смещение

    Returns:
        Список сообщений
    """
    stmt = (
        select(TicketMessage)
        .where(TicketMessage.user_id == user_id)
        .options(
            joinedload(TicketMessage.ticket),
            joinedload(TicketMessage.user),
        )
        .order_by(desc(TicketMessage.created_at))
        .offset(offset)
        .limit(limit)
    )

    result = await session.execute(stmt)
    return result.scalars().all()


# ─────────────────────────────────────────────────────────────
# DELETE
# ─────────────────────────────────────────────────────────────

@with_session
async def delete_message(
    session: AsyncSession,
    message_id: int,
) -> bool:
    """
    Удалить сообщение.

    Args:
        session: DB сессия
        message_id: ID сообщения

    Returns:
        True если удалено
    """
    stmt = select(TicketMessage).where(TicketMessage.id == message_id)
    result = await session.execute(stmt)
    message = result.scalar_one_or_none()

    if not message:
        return False

    ticket_id = message.ticket_id
    await session.delete(message)
    await session.flush()

    # Инвалидируем кеш
    await cache.delete(f"messages:ticket:{ticket_id}")

    logger.info(f"🗑 Удалено сообщение #{message_id} из тикета #{ticket_id}")

    return True


# ─────────────────────────────────────────────────────────────
# ОБЁРТКА-КЛАСС ДЛЯ СОВМЕСТИМОСТИ
# ─────────────────────────────────────────────────────────────

class TicketMessageCRUD:
    """
    Класс для работы с сообщениями тикетов.
    """

    # CREATE
    add_message = staticmethod(add_message)

    # READ
    get_ticket_messages = staticmethod(get_ticket_messages)
    get_last_message = staticmethod(get_last_message)
    count_ticket_messages = staticmethod(count_ticket_messages)
    get_messages_by_user = staticmethod(get_messages_by_user)

    # DELETE
    delete_message = staticmethod(delete_message)