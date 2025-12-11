# app/db/crud/tech.py
from __future__ import annotations
import logging
from typing import Sequence, Optional
from datetime import datetime, time
from sqlalchemy import func, select, desc
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import joinedload
from zoneinfo import ZoneInfo

from app.config import settings
from app.db.models import Technician, Feedback, TechThread
from app.utils.session_decorator import with_session

logger = logging.getLogger(__name__)

# --- READ ---

@with_session
async def get_technicians(
    session: AsyncSession,
    active_only: bool = True,
) -> Sequence[Technician]:
    stmt = select(Technician)
    if active_only:
        stmt = stmt.where(Technician.is_active.is_(True))
    stmt = stmt.order_by(Technician.name.asc())
    res = await session.execute(stmt)
    return res.scalars().all()


@with_session
async def get_technician_by_id(
    session: AsyncSession,
    tech_id: int,
) -> Technician | None:
    stmt = select(Technician).where(Technician.id == tech_id)
    res = await session.execute(stmt)
    return res.scalars().first()


@with_session
async def get_technician_by_name(
    session: AsyncSession,
    name: str,
) -> Technician | None:
    stmt = select(Technician).where(Technician.name.ilike(name))
    res = await session.execute(stmt)
    return res.scalars().first()


@with_session
async def get_active_names(session: AsyncSession) -> list[str]:
    # ВАЖНО: session передаём ИМЕННО как keyword, чтобы не ломать with_session
    techs = await get_technicians(session=session, active_only=True)
    return [t.name for t in techs]


# --- WRITE/UPSERT ---

@with_session
async def upsert_technician(
    session: AsyncSession,
    *,
    name: str,
    tg_user_id: int | None = None,
    group_chat_id: int | None = None,
    is_active: bool = True,
) -> Technician:
    tech = await get_technician_by_name(session=session, name=name)
    if tech:
        changed = False
        if tg_user_id is not None and tech.tg_user_id != tg_user_id:
            tech.tg_user_id = tg_user_id
            changed = True
        if group_chat_id is not None and tech.group_chat_id != group_chat_id:
            tech.group_chat_id = group_chat_id
            changed = True
        if tech.is_active != is_active:
            tech.is_active = is_active
            changed = True
        if changed:
            await session.flush()
        return tech

    tech = Technician(
        name=name,
        tg_user_id=tg_user_id,
        group_chat_id=group_chat_id,
        is_active=is_active,
    )
    session.add(tech)
    await session.flush()
    return tech


@with_session
async def set_technician_group_by_name(
    session: AsyncSession,
    name: str,
    group_chat_id: int,
) -> Technician | None:
    tech = await get_technician_by_name(session=session, name=name)
    if not tech:
        return None
    if tech.group_chat_id != group_chat_id:
        tech.group_chat_id = group_chat_id
        await session.flush()
    return tech


@with_session
async def deactivate_technician_by_id(
    session: AsyncSession,
    tech_id: int,
) -> bool:
    """Деактивировать техника по ID."""
    tech = await get_technician_by_id(session=session, tech_id=tech_id)
    if not tech:
        return False
    if tech.is_active:
        tech.is_active = False
        await session.flush()
    return True


async def get_technician_stats(
    session: AsyncSession,
    tech_id: int,
    limit: int = 10,
    offset: int = 0,
) -> tuple[list[dict], int, float]:
    """
    Получить статистику по технику.

    Args:
        session: Сессия БД
        tech_id: ID техника
        limit: Количество записей на странице
        offset: Смещение для пагинации

    Returns:
        Кортеж: (список записей, общее количество, средняя оценка)

    Запись: {
        'ticket_id': int,
        'avg_rating': float,
        'created_at': datetime,
    }
    """
    # Подзапрос для расчета средней оценки по каждому тикету
    subquery = (
        select(
            Feedback.ticket_id,
            Feedback.created_at,
            ((Feedback.q1 + Feedback.q2 + Feedback.q3 + Feedback.q4 + Feedback.q5) / 5.0).label("avg_rating")
        )
        .where(Feedback.tech_id == tech_id)
        .order_by(desc(Feedback.created_at))
        .subquery()
    )

    # Запрос с пагинацией
    stmt_paginated = (
        select(
            subquery.c.ticket_id,
            subquery.c.avg_rating,
            subquery.c.created_at,
        )
        .limit(limit)
        .offset(offset)
    )

    result = await session.execute(stmt_paginated)
    records = [
        {
            "ticket_id": row.ticket_id,
            "avg_rating": float(row.avg_rating),
            "created_at": row.created_at,
        }
        for row in result.all()
    ]

    # Общее количество записей
    count_stmt = select(func.count()).select_from(
        select(Feedback.id).where(Feedback.tech_id == tech_id).subquery()
    )
    total_result = await session.execute(count_stmt)
    total_count = total_result.scalar() or 0

    # Общая средняя оценка
    avg_stmt = select(
        func.avg((Feedback.q1 + Feedback.q2 + Feedback.q3 + Feedback.q4 + Feedback.q5) / 5.0)
    ).where(Feedback.tech_id == tech_id)

    avg_result = await session.execute(avg_stmt)
    overall_avg = avg_result.scalar()
    overall_avg = float(overall_avg) if overall_avg is not None else 0.0

    return records, total_count, overall_avg


async def update_technician_name(
    session: AsyncSession,
    tech_id: int,
    new_name: str,
) -> bool:
    """
    Обновить имя техника.

    Args:
        session: Сессия БД
        tech_id: ID техника
        new_name: Новое имя

    Returns:
        True если обновлено успешно
    """
    tech = await get_technician_by_id(session=session, tech_id=tech_id)

    if not tech:
        return False

    tech.name = new_name
    await session.flush()

    return True

def _parse_time_str(value: str | None) -> Optional[time]:
    if not value:
        return None
    try:
        parts = value.strip().split(":")
        if len(parts) == 1:
            h = int(parts[0])
            m = 0
        else:
            h = int(parts[0])
            m = int(parts[1])
        if not (0 <= h <= 23 and 0 <= m <= 59):
            return None
        return time(hour=h, minute=m)
    except Exception:
        return None


def _time_in_interval(now: time, start: time, end: time) -> bool:
    """
    Проверка попадания времени в интервал [start, end).

    Поддерживает "ночные" смены, например 22:00–06:00.
    """
    if start <= end:
        return start <= now < end
    else:
        # через полночь
        return now >= start or now < end

@with_session
async def get_auto_assign_technician_for_now(
    session: AsyncSession,
    *,
    now: datetime | None = None,
) -> Technician | None:
    """
    Найти техника с включённым автоназначением по текущему времени.

    Сейчас берём ПЕРВОГО подходящего активного техника.
    При необходимости можно усложнить (по нагрузке, round-robin и т.д.).
    """
    if now is None:
        now = datetime.now(ZoneInfo(settings.timezone))
    now_t = now.time()

    stmt = (
        select(Technician)
        .where(
            Technician.is_active.is_(True),
            Technician.is_auto_assign.is_(True),
            Technician.auto_assign_start_hour.is_not(None),
            Technician.auto_assign_end_hour.is_not(None),
        )
        .order_by(Technician.id.asc())
    )
    res = await session.execute(stmt)
    techs = res.scalars().all()

    for tech in techs:
        start = _parse_time_str(tech.auto_assign_start_hour)
        end = _parse_time_str(tech.auto_assign_end_hour)
        if not start or not end:
            continue

        if _time_in_interval(now_t, start, end):
            logger.info(
                "⏱ Выбран техник по автоназначению: %s (ID=%s) для времени %s",
                tech.name,
                tech.id,
                now_t,
            )
            return tech

    return None

@with_session
async def get_or_create_tech_thread(
    session: AsyncSession,
    *,
    ticket_id: int,
    user_id: int,
    tech_id: int,
    tech_chat_id: int,
    tech_thread_id: int,
) -> TechThread:
    """
    Атомарно получить или создать TechThread для (ticket_id, tech_id).

    Опирается на UNIQUE-индекс:
        uq_tech_threads_ticket_tech (ticket_id, tech_id)

    Поведение:
      1) Если запись уже есть — возвращаем её, при необходимости обновляя
         tech_chat_id / tech_thread_id.
      2) Если записи нет — создаём новую.
      3) Если параллельно кто-то успел создать (IntegrityError) —
         перечитываем существующую и возвращаем её.
    """

    # Базовый запрос для поиска
    base_stmt = (
        select(TechThread)
        .where(
            TechThread.ticket_id == ticket_id,
            TechThread.tech_id == tech_id,
        )
        .options(
            joinedload(TechThread.ticket),
            joinedload(TechThread.technician),
        )
    )

    # 1) Пытаемся найти уже существующую запись
    res = await session.execute(base_stmt)
    thread = res.scalar_one_or_none()
    if thread:
        updated = False

        if thread.tech_chat_id != tech_chat_id:
            thread.tech_chat_id = tech_chat_id
            updated = True

        if thread.tech_thread_id != tech_thread_id:
            thread.tech_thread_id = tech_thread_id
            updated = True

        if updated:
            await session.flush()

        return thread

    # 2) Нет записи — пробуем создать в вложенной транзакции
    #    (чтобы аккуратно ловить IntegrityError, не ломая внешний транзакшн)
    async with session.begin_nested():
        thread = TechThread(
            ticket_id=ticket_id,
            user_id=user_id,
            tech_id=tech_id,
            tech_chat_id=tech_chat_id,
            tech_thread_id=tech_thread_id,
        )
        session.add(thread)

        try:
            await session.flush()
        except IntegrityError:
            # Кто-то параллельно создал такую же запись
            logger.warning(
                "Race on get_or_create_tech_thread(ticket_id=%s, tech_id=%s): "
                "IntegrityError on insert, re-selecting existing row",
                ticket_id,
                tech_id,
                exc_info=True,
            )
            # вложенная транзакция будет откатана автоматически
        else:
            # всё ок, запись создалась
            await session.refresh(thread)
            return thread

    # 3) Если мы здесь — была IntegrityError, перечитываем существующую запись
    res = await session.execute(base_stmt)
    thread = res.scalar_one()

    # На всякий случай синхронизируем chat/thread ID, если они отличаются
    updated = False
    if thread.tech_chat_id != tech_chat_id:
        thread.tech_chat_id = tech_chat_id
        updated = True
    if thread.tech_thread_id != tech_thread_id:
        thread.tech_thread_id = tech_thread_id
        updated = True
    if updated:
        await session.flush()

    return thread


async def find_existing_tech_topic_for_client(
    session: AsyncSession,
    client_tg_id: int,
    tech_id: int
) -> TechThread | None:
    """
    Найти последний тех-топик для пары (client, technician).
    Используется при повторном назначении техники.
    """
    stmt = (
        select(TechThread)
        .where(
            TechThread.user_id == client_tg_id,
            TechThread.tech_id == tech_id
        )
        .order_by(TechThread.id.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()