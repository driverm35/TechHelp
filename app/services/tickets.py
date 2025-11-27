from __future__ import annotations
from typing import Optional
from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import User, Ticket, Technician, TicketStatus

async def get_or_create_user(session: AsyncSession, tg_id: int, username: str | None, full_name: str | None) -> User:
    u = await session.get(User, tg_id)
    if u:
        changed = False
        if username and u.username != username:
            u.username = username; changed = True
        if full_name and u.full_name != full_name:
            u.full_name = full_name; changed = True
        if changed:
            await session.flush()
        return u
    u = User(tg_id=tg_id, username=username, full_name=full_name)
    session.add(u)
    await session.flush()
    return u

async def find_open_ticket(session: AsyncSession, client_tg_id: int) -> Optional[Ticket]:
    q = select(Ticket).where(Ticket.client_tg_id == client_tg_id, Ticket.status != TicketStatus.CLOSED).order_by(Ticket.id.desc())
    res = await session.execute(q)
    return res.scalars().first()

async def create_ticket(session: AsyncSession, client_tg_id: int, main_chat_id: int, main_thread_id: int | None) -> Ticket:
    t = Ticket(client_tg_id=client_tg_id, main_chat_id=main_chat_id, main_thread_id=main_thread_id, status=TicketStatus.NEW)
    session.add(t)
    await session.flush()
    return t

async def assign_technician(session: AsyncSession, ticket_id: int, tech_name: str) -> Optional[Technician]:
    q = select(Technician).where(Technician.name.ilike(tech_name))
    tech = (await session.execute(q)).scalars().first()
    if not tech:
        return None
    await session.execute(update(Ticket).where(Ticket.id == ticket_id).values(assigned_tech_id=tech.id, status=TicketStatus.WORK))
    await session.flush()
    return tech

async def close_ticket(session: AsyncSession, ticket_id: int) -> None:
    await session.execute(update(Ticket).where(Ticket.id == ticket_id).values(status=TicketStatus.CLOSED))
    await session.flush()
