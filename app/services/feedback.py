from __future__ import annotations
from sqlalchemy.ext.asyncio import AsyncSession
from app.db.models import Feedback

async def save_feedback(session: AsyncSession, ticket_id: int, q1: int, q2: int, q3: int, comment: str | None):
    fb = Feedback(ticket_id=ticket_id, q1=q1, q2=q2, q3=q3, comment=comment or None)
    session.add(fb)
    await session.flush()
    return fb
