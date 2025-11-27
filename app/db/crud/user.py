from __future__ import annotations

import logging
from datetime import datetime
from typing import Optional

from sqlalchemy import select, or_, func
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.exc import IntegrityError

from app.db.models import User, Ticket
from app.utils.validators import sanitize_telegram_name

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────────────────────
# READ
# ─────────────────────────────────────────────────────────────────────────────

async def get_user_by_telegram_id(db: AsyncSession, telegram_id: int) -> Optional[User]:
    """
    Возвращает пользователя по tg_id с предзагрузкой:
      - user.topics
      - user.tickets (+ назначенный техник)
    """
    res = await db.execute(
        select(User)
        .options(
            selectinload(User.topics),
            selectinload(User.tickets).selectinload(Ticket.assigned_tech),
        )
        .where(User.tg_id == telegram_id)
    )
    return res.scalar_one_or_none()


async def get_user_by_username(db: AsyncSession, username: str | None) -> Optional[User]:
    """
    Поиск по username (без @), регистронезависимый.
    """
    if not username:
        return None
    normalized = username.lstrip("@").lower()

    res = await db.execute(
        select(User)
        .options(
            selectinload(User.topics),
            selectinload(User.tickets).selectinload(Ticket.assigned_tech),
        )
        .where(func.lower(User.username) == normalized)
    )
    return res.scalar_one_or_none()


async def get_users_count(db: AsyncSession, search: Optional[str] = None) -> int:
    """
    Подсчёт пользователей с опциональным поиском по:
      - first_name ILIKE
      - last_name  ILIKE
      - username   ILIKE
      - tg_id      (точное совпадение, если search — число)
    """
    stmt = select(func.count()).select_from(User)

    if search:
        pattern = f"%{search}%"
        conditions = [
            User.first_name.ilike(pattern),
            User.last_name.ilike(pattern),
            User.username.ilike(pattern),
        ]
        if search.isdigit():
            try:
                conditions.append(User.tg_id == int(search))
            except ValueError:
                pass
        stmt = stmt.where(or_(*conditions))

    return int((await db.execute(stmt)).scalar_one())


# ─────────────────────────────────────────────────────────────────────────────
# WRITE
# ─────────────────────────────────────────────────────────────────────────────

async def create_user_no_commit(
    db: AsyncSession,
    telegram_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> User:
    """
    Создаёт пользователя без коммита (для батчей). Делает flush(), чтобы получить PK.
    """
    safe_first = sanitize_telegram_name(first_name)
    safe_last = sanitize_telegram_name(last_name)

    user = User(
        tg_id=telegram_id,
        username=(username.lstrip("@") if username else None),
        first_name=safe_first,
        last_name=safe_last,
        last_seen=datetime.utcnow(),
    )
    db.add(user)
    await db.flush()  # получить PK/зафиксировать в сессии
    logger.info("✅ Подготовлен пользователь %s (без коммита)", telegram_id)
    return user


async def create_user(
    db: AsyncSession,
    telegram_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> User:
    """
    Создаёт пользователя с коммитом. Если уже существует — вернёт существующего.
    """
    safe_first = sanitize_telegram_name(first_name)
    safe_last = sanitize_telegram_name(last_name)

    user = User(
        tg_id=telegram_id,
        username=(username.lstrip("@") if username else None),
        first_name=safe_first,
        last_name=safe_last,
        last_seen=datetime.utcnow(),
    )
    db.add(user)
    try:
        await db.commit()
        await db.refresh(user)
        logger.info("✅ Создан пользователь %s", telegram_id)
        return user
    except IntegrityError:
        await db.rollback()
        existing = await get_user_by_telegram_id(db, telegram_id)
        if existing:
            logger.info("ℹ️ Пользователь %s уже существовал — вернули существующего", telegram_id)
            return existing
        raise


async def get_or_create_user(
    db: AsyncSession,
    telegram_id: int,
    username: str | None = None,
    first_name: str | None = None,
    last_name: str | None = None,
) -> User:
    """
    Быстрый хелпер: найти или создать.
    Обновляет username/имя, если изменились.
    """
    user = await get_user_by_telegram_id(db, telegram_id)
    if user:
        changed = False
        # аккуратно обновим базовые поля
        if username is not None:
            new_username = username.lstrip("@")
            if user.username != new_username:
                user.username = new_username
                changed = True
        sf = sanitize_telegram_name(first_name)
        sl = sanitize_telegram_name(last_name)
        if sf is not None and sf != user.first_name:
            user.first_name = sf
            changed = True
        if sl is not None and sl != user.last_name:
            user.last_name = sl
            changed = True

        user.last_seen = datetime.utcnow()
        if changed:
            await db.flush()
        # не обязательно коммитить немедленно (зависит от вызова)
        return user

    return await create_user(db, telegram_id, username=username, first_name=first_name, last_name=last_name)


async def update_user(db: AsyncSession, user: User, **kwargs) -> User:
    """
    Мягкое обновление полей: username/first_name/last_name/last_seen и любых безопасных полей модели.
    """
    for field, value in kwargs.items():
        if field in ("first_name", "last_name"):
            value = sanitize_telegram_name(value)
        if field == "username" and value is not None:
            value = value.lstrip("@")
        if hasattr(user, field):
            setattr(user, field, value)

    if "last_seen" not in kwargs:
        user.last_seen = datetime.utcnow()

    await db.commit()
    await db.refresh(user)
    return user
