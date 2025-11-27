# app/db/crud/ticket.py
from __future__ import annotations

from datetime import datetime
from typing import Sequence, Iterable

from sqlalchemy import select, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload, joinedload
from app.db.crud.message import TicketMessageCRUD

from app.db.models import (
    Ticket,
    TicketStatus,
    TechThread,
    Feedback,
    Event,
    Actor,
    Technician,
)
from app.utils.session_decorator import with_session


# ─────────────────────────────────────────────────────────────
# ВСПОМОГАТЕЛЬНОЕ
# ─────────────────────────────────────────────────────────────

def _normalize_statuses(
    statuses: Iterable[TicketStatus | str] | None,
) -> list[TicketStatus]:
    """
    Принимает список enum-ов или строк и приводит их к TicketStatus.
    Удобно для мест, где мы иногда передаём строки из UI.
    """
    if not statuses:
        return []
    result: list[TicketStatus] = []
    for s in statuses:
        if isinstance(s, TicketStatus):
            result.append(s)
        else:
            result.append(TicketStatus(str(s)))
    return result


# ─────────────────────────────────────────────────────────────
# TICKET CRUD
# ─────────────────────────────────────────────────────────────

@with_session
async def create_ticket(
    session: AsyncSession,
    *,
    client_tg_id: int,
    main_chat_id: int,
    main_thread_id: int | None = None,
    assigned_tech_id: int | None = None,
    actor: Actor = Actor.CLIENT,
    initial_comment: str | None = None,
    extra: dict | None = None,
) -> Ticket:
    """
    Создать новый тикет.

    Минимальный набор:
      - client_tg_id  — Telegram ID клиента
      - main_chat_id  — ID главного чата (группа/форум, где живёт основной топик)
      - main_thread_id — ID топика (thread) в этом чате, если есть

    При создании сразу пишем Event 'ticket_created' с initial_comment (если есть).
    """
    ticket = Ticket(
        client_tg_id=client_tg_id,
        main_chat_id=main_chat_id,
        main_thread_id=main_thread_id,
        assigned_tech_id=assigned_tech_id,
        status=TicketStatus.NEW,
    )
    session.add(ticket)
    await session.flush()  # получаем ticket.id

    payload: dict = {}
    if initial_comment:
        payload["text"] = initial_comment
    if extra:
        payload.update(extra)

    await add_event(
        session=session,
        ticket_id=ticket.id,
        actor=actor,
        action="ticket_created",
        payload=payload or None,
    )

    # ticket уже в сессии, но на всякий случай обновим
    await session.refresh(ticket)
    return ticket


@with_session
async def get_ticket_by_id(
    session: AsyncSession,
    ticket_id: int,
    *,
    with_client: bool = True,
    with_tech: bool = True,
    with_threads: bool = True,
) -> Ticket | None:
    """
    Получить тикет по ID с опциональной предзагрузкой:
      - client
      - assigned_tech
      - tech_threads
    """
    stmt = select(Ticket).where(Ticket.id == ticket_id)

    options = []
    if with_client:
        options.append(joinedload(Ticket.client))
    if with_tech:
        options.append(joinedload(Ticket.assigned_tech))
    if with_threads:
        options.append(selectinload(Ticket.tech_threads))

    if options:
        stmt = stmt.options(*options)

    res = await session.execute(stmt)
    return res.scalar_one_or_none()


@with_session
async def get_tickets_for_client(
    session: AsyncSession,
    client_tg_id: int,
    *,
    statuses: Sequence[TicketStatus | str] | None = None,
    limit: int = 50,
    offset: int = 0,
) -> Sequence[Ticket]:
    """
    Тикеты конкретного клиента с фильтром по статусам и пагинацией.
    """
    stmt = (
        select(Ticket)
        .where(Ticket.client_tg_id == client_tg_id)
        .order_by(Ticket.created_at.desc())
        .options(
            joinedload(Ticket.client),
            joinedload(Ticket.assigned_tech),
        )
    )

    norm = _normalize_statuses(statuses)
    if norm:
        stmt = stmt.where(Ticket.status.in_(norm))

    stmt = stmt.offset(offset).limit(limit)

    res = await session.execute(stmt)
    return res.scalars().all()


@with_session
async def count_tickets_for_client(
    session: AsyncSession,
    client_tg_id: int,
    *,
    statuses: Sequence[TicketStatus | str] | None = None,
) -> int:
    """
    Количество тикетов клиента с опциональным фильтром по статусу.
    """
    stmt = select(func.count()).select_from(Ticket).where(
        Ticket.client_tg_id == client_tg_id
    )

    norm = _normalize_statuses(statuses)
    if norm:
        stmt = stmt.where(Ticket.status.in_(norm))

    res = await session.execute(stmt)
    return int(res.scalar() or 0)


@with_session
async def get_tickets_for_admin(
    session: AsyncSession,
    *,
    statuses: Sequence[TicketStatus | str] | None = None,
    assigned_tech_id: int | None = None,
    client_tg_id: int | None = None,
    limit: int = 50,
    offset: int = 0,
) -> Sequence[Ticket]:
    """
    Универсальный список тикетов для админских экранов:
      - фильтр по статусам
      - фильтр по назначенному технику
      - фильтр по клиенту
    """
    stmt = (
        select(Ticket)
        .options(
            joinedload(Ticket.client),
            joinedload(Ticket.assigned_tech),
        )
        .order_by(Ticket.created_at.desc())
    )

    conditions = []
    norm = _normalize_statuses(statuses)
    if norm:
        conditions.append(Ticket.status.in_(norm))
    if assigned_tech_id is not None:
        conditions.append(Ticket.assigned_tech_id == assigned_tech_id)
    if client_tg_id is not None:
        conditions.append(Ticket.client_tg_id == client_tg_id)

    for cond in conditions:
        stmt = stmt.where(cond)

    stmt = stmt.offset(offset).limit(limit)
    res = await session.execute(stmt)
    return res.scalars().all()


@with_session
async def count_tickets(
    session: AsyncSession,
    *,
    statuses: Sequence[TicketStatus | str] | None = None,
) -> int:
    """
    Подсчёт тикетов с опциональным фильтром по нескольким статусам.
    """
    stmt = select(func.count()).select_from(Ticket)
    norm = _normalize_statuses(statuses)
    if norm:
        stmt = stmt.where(Ticket.status.in_(norm))

    res = await session.execute(stmt)
    return int(res.scalar() or 0)


@with_session
async def get_open_tickets_count(session: AsyncSession) -> int:
    """
    Количество "незавершённых" тикетов.
    В текущей логике: NEW + WORK.
    """
    res = await session.execute(
        select(func.count())
        .select_from(Ticket)
        .where(Ticket.status.in_([TicketStatus.NEW, TicketStatus.WORK]))
    )
    return int(res.scalar() or 0)


@with_session
async def set_ticket_status(
    session: AsyncSession,
    *,
    ticket_id: int,
    status: TicketStatus | str,
    actor: Actor | None = None,
    reason: str | None = None,
) -> Ticket | None:
    """
    Обновить статус тикета.
    Если статус становится CLOSED — проставляем closed_at (если ещё не было).
    При наличии actor пишем Event 'status_changed'.
    """
    ticket = await get_ticket_by_id(
        session=session,            # 🔹 KWARG
        ticket_id=ticket_id,
        with_client=False,
        with_tech=False,
        with_threads=False,
    )
    if not ticket:
        return None

    new_status = TicketStatus(status)
    if ticket.status == new_status:
        return ticket

    ticket.status = new_status
    if new_status is TicketStatus.CLOSED and ticket.closed_at is None:
        ticket.closed_at = datetime.utcnow()

    await session.flush()

    if actor is not None:
        payload = {"reason": reason} if reason else None
        await add_event(
            session=session,        # 🔹 KWARG
            ticket_id=ticket.id,
            actor=actor,
            action="status_changed",
            payload=payload,
        )

    await session.refresh(ticket)
    return ticket


@with_session
async def close_ticket(
    session: AsyncSession,
    *,
    ticket_id: int,
    actor: Actor = Actor.STAFF,
    reason: str | None = None,
) -> Ticket | None:
    """
    Удобный шорткат: закрыть тикет.
    """
    return await set_ticket_status(
        session=session,            # 🔹 KWARG
        ticket_id=ticket_id,
        status=TicketStatus.CLOSED,
        actor=actor,
        reason=reason,
    )



@with_session
async def assign_ticket_to_technician(
    session: AsyncSession,
    *,
    ticket_id: int,
    tech_id: int | None,
    actor: Actor = Actor.STAFF,
    reason: str | None = None,
) -> Ticket | None:
    """
    Назначить (или снять) техника для тикета.
    При этом пишется Event 'assigned_tech_changed'.
    """
    ticket = await get_ticket_by_id(
        session=session,            # 🔹 KWARG
        ticket_id=ticket_id,
        with_client=False,
        with_tech=False,
        with_threads=False,
    )
    if not ticket:
        return None

    old_tech_id = ticket.assigned_tech_id
    if old_tech_id == tech_id:
        return ticket

    # Опционально можно проверить, что техник существует
    if tech_id is not None:
        res = await session.execute(
            select(Technician.id).where(Technician.id == tech_id)
        )
        if res.scalar_one_or_none() is None:
            # не существующий техник — считаем ошибкой
            return None

    ticket.assigned_tech_id = tech_id
    await session.flush()

    await add_event(
        session=session,            # 🔹 KWARG
        ticket_id=ticket.id,
        actor=actor,
        action="assigned_tech_changed",
        payload={
            "old_tech_id": old_tech_id,
            "new_tech_id": tech_id,
            "reason": reason,
        },
    )

    await session.refresh(ticket)
    return ticket



# ─────────────────────────────────────────────────────────────
# TECH THREADS
# ─────────────────────────────────────────────────────────────

@with_session
async def create_tech_thread(
    session: AsyncSession,
    *,
    ticket_id: int,
    user_id: int,  # 🔹 ДОБАВИТЬ
    tech_chat_id: int,
    tech_thread_id: int,
    tech_id: int | None = None,
) -> TechThread:
    """
    Создать зеркальную тему тикета в тех. группе/топике.
    Один тикет может иметь несколько TechThread (разные группы/техи).
    """
    thread = TechThread(
        ticket_id=ticket_id,
        user_id=user_id,  # 🔹 ДОБАВИТЬ
        tech_id=tech_id,
        tech_chat_id=tech_chat_id,
        tech_thread_id=tech_thread_id,
    )
    session.add(thread)
    await session.flush()
    await session.refresh(thread)
    return thread

@with_session
async def get_tech_thread_by_user_and_tech(
    session: AsyncSession,
    *,
    user_id: int,
    tech_id: int,
) -> TechThread | None:
    """
    Найти TechThread по user_id и tech_id.

    Используется для поиска существующего топика клиента у конкретного техника.
    """
    stmt = (
        select(TechThread)
        .where(
            TechThread.user_id == user_id,
            TechThread.tech_id == tech_id,
        )
        .options(
            joinedload(TechThread.ticket),
            joinedload(TechThread.technician),
        )
        .order_by(TechThread.created_at.desc())  # Берем последний
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()

@with_session
async def get_all_tech_threads_for_ticket(
    session: AsyncSession,
    ticket_id: int,
) -> Sequence[TechThread]:
    """
    Получить все TechThread для тикета.

    Используется для обновления названий всех топиков.
    """
    stmt = (
        select(TechThread)
        .where(TechThread.ticket_id == ticket_id)
        .options(
            joinedload(TechThread.technician),
        )
    )
    res = await session.execute(stmt)
    return res.scalars().all()

@with_session
async def get_ticket_by_thread(
    session: AsyncSession,
    *,
    tech_chat_id: int,
    tech_thread_id: int,
) -> Ticket | None:
    """
    Найти тикет по связанной тех-теме (группа + thread_id).

    Это то, что нужно, чтобы из сообщения в топике группы понять, к какому тикету оно относится.
    """
    stmt = (
        select(Ticket)
        .join(TechThread, TechThread.ticket_id == Ticket.id)
        .where(
            TechThread.tech_chat_id == tech_chat_id,
            TechThread.tech_thread_id == tech_thread_id,
        )
        .options(
            joinedload(Ticket.client),
            joinedload(Ticket.assigned_tech),
            selectinload(Ticket.tech_threads),
        )
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


# ─────────────────────────────────────────────────────────────
# FEEDBACK CRUD
# ─────────────────────────────────────────────────────────────

@with_session
async def create_feedback(
    session: AsyncSession,
    *,
    ticket_id: int,
    q1: int,
    q2: int,
    q3: int,
    q4: int,
    q5: int,
    comment: str | None = None,
    tech_id: int | None = None,
) -> Feedback:
    """
    Создать отзыв по тикету.
    tech_id можно не указывать (например, если тикет вели несколько техников).
    """
    fb = Feedback(
        ticket_id=ticket_id,
        tech_id=tech_id,
        q1=q1,
        q2=q2,
        q3=q3,
        q4=q4,
        q5=q5,
        comment=comment,
    )
    session.add(fb)
    await session.flush()
    await session.refresh(fb)
    return fb


@with_session
async def get_feedback_for_ticket(
    session: AsyncSession,
    ticket_id: int,
) -> Feedback | None:
    """
    Получить последний отзыв по тикету (если их несколько).
    """
    stmt = (
        select(Feedback)
        .where(Feedback.ticket_id == ticket_id)
        .order_by(Feedback.created_at.desc())
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


@with_session
async def list_feedbacks_for_technician(
    session: AsyncSession,
    tech_id: int,
    *,
    limit: int = 50,
    offset: int = 0,
) -> Sequence[Feedback]:
    """
    Список отзывов по конкретному технику.
    """
    stmt = (
        select(Feedback)
        .where(Feedback.tech_id == tech_id)
        .order_by(Feedback.created_at.desc())
        .offset(offset)
        .limit(limit)
    )
    res = await session.execute(stmt)
    return res.scalars().all()


# ─────────────────────────────────────────────────────────────
# EVENTS (лог действий по тикету)
# ─────────────────────────────────────────────────────────────

@with_session
async def add_event(
    session: AsyncSession,
    *,
    ticket_id: int,
    actor: Actor,
    action: str,
    payload: dict | None = None,
) -> Event:
    """
    Универсальный логгер действий по тикету.

    Примеры action:
      - 'ticket_created'
      - 'client_message'
      - 'tech_message'
      - 'status_changed'
      - 'assigned_tech_changed'
      - 'feedback_created'
      - и т.п.
    """
    ev = Event(
        ticket_id=ticket_id,
        actor=actor,
        action=action,
        payload=payload,
    )
    session.add(ev)
    await session.flush()
    await session.refresh(ev)
    return ev


@with_session
async def list_events_for_ticket(
    session: AsyncSession,
    ticket_id: int,
    *,
    limit: int = 100,
    offset: int = 0,
) -> Sequence[Event]:
    """
    Получить историю событий по тикету (для админки/отладки).
    """
    stmt = (
        select(Event)
        .where(Event.ticket_id == ticket_id)
        .order_by(Event.ts.asc())
        .offset(offset)
        .limit(limit)
    )
    res = await session.execute(stmt)
    return res.scalars().all()


# ─────────────────────────────────────────────────────────────
# ОБЁРТКА-КОМПАТ ДЛЯ СТАРОГО ИМЕНИ TicketCRUD
# ─────────────────────────────────────────────────────────────

class TicketCRUD:
    """
    Совместимая обёртка над функциями выше.

    Старый огромный класс TicketCRUD из другого проекта здесь НЕ восстановлен,
    мы просто даём удобные статические методы под новую схему.
    """

    # создание/чтение тикетов
    create_ticket = staticmethod(create_ticket)
    get_ticket_by_id = staticmethod(get_ticket_by_id)
    get_tickets_for_client = staticmethod(get_tickets_for_client)
    count_tickets_for_client = staticmethod(count_tickets_for_client)
    get_tickets_for_admin = staticmethod(get_tickets_for_admin)
    count_tickets = staticmethod(count_tickets)
    get_open_tickets_count = staticmethod(get_open_tickets_count)

    # статус/назначение
    set_ticket_status = staticmethod(set_ticket_status)
    close_ticket = staticmethod(close_ticket)
    assign_ticket_to_technician = staticmethod(assign_ticket_to_technician)

    # threads
    create_tech_thread = staticmethod(create_tech_thread)
    get_ticket_by_thread = staticmethod(get_ticket_by_thread)
    get_tech_thread_by_user_and_tech = staticmethod(get_tech_thread_by_user_and_tech)
    get_all_tech_threads_for_ticket = staticmethod(get_all_tech_threads_for_ticket)

    # feedback
    create_feedback = staticmethod(create_feedback)
    get_feedback_for_ticket = staticmethod(get_feedback_for_ticket)
    list_feedbacks_for_technician = staticmethod(list_feedbacks_for_technician)

    # events
    add_event = staticmethod(add_event)
    list_events_for_ticket = staticmethod(list_events_for_ticket)