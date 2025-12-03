# app/db/models.py
from __future__ import annotations

from datetime import datetime
from enum import Enum

from sqlalchemy import (
    BigInteger,
    JSON,
    ForeignKey,
    Index,
    UniqueConstraint,
    text,
)
from sqlalchemy.orm import Mapped, mapped_column, relationship
from sqlalchemy import Enum as SAEnum  # переносимый ENUM (native_enum=False)
from sqlalchemy.types import DateTime, Boolean, Integer, String

from app.db.database import Base


# ───────────────────
# Переносимые ENUM'ы
# ───────────────────

class TicketStatus(str, Enum):
    NEW = "NEW"
    WORK = "WORK"
    CLOSED = "CLOSED"


class Actor(str, Enum):
    CLIENT = "CLIENT"
    TECH = "TECH"
    STAFF = "STAFF"
    SYSTEM = "SYSTEM"


TicketStatusType = SAEnum(TicketStatus, name="ticketstatus", native_enum=False, create_constraint=True)
ActorType = SAEnum(Actor, name="actor", native_enum=False, create_constraint=True)

now_sql = text("CURRENT_TIMESTAMP")


class User(Base):
    """
    Клиент бота. Ключ — tg_id.
    """
    __tablename__ = "users"

    tg_id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    username: Mapped[str | None] = mapped_column(String, nullable=True)
    # 🛠️ фикс аннотаций:
    first_name: Mapped[str | None] = mapped_column(String(255), nullable=True)
    last_name:  Mapped[str | None] = mapped_column(String(255), nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)
    last_seen:  Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Коллекции → selectin (2 запроса: users + связанные)
    tickets: Mapped[list["Ticket"]] = relationship(
        back_populates="client",
        cascade="save-update",
        passive_deletes=True,
        lazy="selectin",
    )

    topics: Mapped[list["UserTopic"]] = relationship(
        back_populates="user",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
    )


class UserTopic(Base):
    """
    Привязка клиента к теме/топику в чате.
    """
    __tablename__ = "user_topics"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    tech_group_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.tg_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    # Одиночная ссылка → selectin или joined (selectin универсальнее для батчей)
    user: Mapped["User"] = relationship(
        back_populates="topics",
        lazy="selectin",
    )


class Technician(Base):
    """
    Справочник техников.
    """
    __tablename__ = "technicians"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    name: Mapped[str] = mapped_column(String, nullable=False)
    tg_user_id: Mapped[int | None] = mapped_column(BigInteger, nullable=False)
    group_chat_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True, server_default=text("true"))
    is_auto_assign: Mapped[bool] = mapped_column(Boolean, default=False, server_default=text("false"))
    auto_assign_start_hour: Mapped[str | None] = mapped_column(String, nullable=True)
    auto_assign_end_hour: Mapped[str | None] = mapped_column(String, nullable=True)

    # Коллекции → selectin
    tickets: Mapped[list["Ticket"]] = relationship(
        back_populates="assigned_tech",
        passive_deletes=True,
        lazy="selectin",
    )
    threads: Mapped[list["TechThread"]] = relationship(
        back_populates="technician",
        passive_deletes=True,
        lazy="selectin",
    )

    __table_args__ = (
        UniqueConstraint("name", name="uq_technicians_name"),
    )


class Ticket(Base):
    """
    Тикет / обращение.
    """
    __tablename__ = "tickets"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    client_tg_id: Mapped[int] = mapped_column(
        ForeignKey("users.tg_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    first_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)
    status: Mapped[TicketStatus] = mapped_column(
        TicketStatusType,
        default=TicketStatus.NEW,
        nullable=False,
    )

    main_chat_id:   Mapped[int] = mapped_column(BigInteger, nullable=False)
    main_thread_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True)

    assigned_tech_id: Mapped[int | None] = mapped_column(
        ForeignKey("technicians.id"),
        nullable=True,
        index=True,
    )

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)
    closed_at:  Mapped[datetime | None] = mapped_column(DateTime, nullable=True)

    # Одиночные ссылки → joined (удобно для отображения списка тикетов с клиентом/техом)
    client: Mapped["User"] = relationship(
        back_populates="tickets",
        lazy="joined",
    )
    assigned_tech: Mapped["Technician | None"] = relationship(
        back_populates="tickets",
        lazy="joined",
    )

    # Коллекция → selectin
    tech_threads: Mapped[list["TechThread"]] = relationship(
        back_populates="ticket",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
    )
    messages: Mapped[list["TicketMessage"]] = relationship(
        back_populates="ticket",
        cascade="all, delete-orphan",
        passive_deletes=True,
        lazy="selectin",
        order_by="TicketMessage.created_at.asc()"  # Сортировка по времени
    )

    __table_args__ = (
        Index("ix_tickets_client_status", "client_tg_id", "status"),
        Index("ix_tickets_assigned_status", "assigned_tech_id", "status"),
    )

class TicketMessage(Base):
    """История сообщений по тикету."""
    __tablename__ = "ticket_messages"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    ticket_id: Mapped[int] = mapped_column(
        ForeignKey("tickets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.tg_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    message_text: Mapped[str] = mapped_column(String, nullable=False)
    is_from_admin: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)

    # Для медиа файлов
    has_media: Mapped[bool] = mapped_column(Boolean, default=False, nullable=False)
    media_type: Mapped[str | None] = mapped_column(String(20), nullable=True)
    media_file_id: Mapped[str | None] = mapped_column(String(255), nullable=True)
    media_caption: Mapped[str | None] = mapped_column(String, nullable=True)

    # Telegram message_id для отслеживания
    telegram_message_id: Mapped[int | None] = mapped_column(BigInteger, nullable=True, index=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)

    # Связи
    ticket: Mapped["Ticket"] = relationship(back_populates="messages", lazy="joined")
    user: Mapped["User"] = relationship(lazy="joined")

    @property
    def is_user_message(self) -> bool:
        """Сообщение от клиента."""
        return not self.is_from_admin

    @property
    def is_admin_message(self) -> bool:
        """Сообщение от поддержки."""
        return self.is_from_admin

    def __repr__(self):
        text_preview = self.message_text[:30] if self.message_text else ""
        return (
            f"<TicketMessage(id={self.id}, ticket_id={self.ticket_id}, "
            f"is_admin={self.is_from_admin}, text='{text_preview}...')>"
        )


class TechThread(Base):
    """
    Зеркальная тема тикета в группе техника.
    """
    __tablename__ = "tech_threads"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)

    ticket_id: Mapped[int] = mapped_column(
        ForeignKey("tickets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    user_id: Mapped[int] = mapped_column(
        ForeignKey("users.tg_id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    tech_id: Mapped[int | None] = mapped_column(
        ForeignKey("technicians.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    tech_chat_id:   Mapped[int] = mapped_column(BigInteger, nullable=False)
    tech_thread_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)

    # Одиночные ссылки → joined (нам часто нужно сразу знать ticket/technician)
    ticket: Mapped["Ticket"] = relationship(
        back_populates="tech_threads",
        lazy="joined",
    )
    technician: Mapped["Technician | None"] = relationship(
        back_populates="threads",
        lazy="joined",
    )

    __table_args__ = (
        UniqueConstraint("ticket_id", "tech_id", name="uq_tech_threads_ticket_tech"),
    )


class Feedback(Base):
    """
    Оценка работы ТП. Может быть без техника (NULL).
    """
    __tablename__ = "feedbacks"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticket_id: Mapped[int] = mapped_column(
        ForeignKey("tickets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )
    tech_id: Mapped[int | None] = mapped_column(
        ForeignKey("technicians.id", ondelete="SET NULL"),
        index=True,
        nullable=True,
    )

    q1: Mapped[int] = mapped_column(Integer, nullable=False)
    q2: Mapped[int] = mapped_column(Integer, nullable=False)
    q3: Mapped[int] = mapped_column(Integer, nullable=False)
    q4: Mapped[int] = mapped_column(Integer, nullable=False)
    q5: Mapped[int] = mapped_column(Integer, nullable=False)
    comment: Mapped[str | None] = mapped_column(String, nullable=True)

    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)

    # Часто нужен тикет/техник вместе с фидбеком → joined
    ticket: Mapped["Ticket"] = relationship(lazy="joined")
    technician: Mapped["Technician | None"] = relationship(lazy="joined")


class Event(Base):
    """
    Аудит/лог событий по тикету.
    """
    __tablename__ = "events"

    id: Mapped[int] = mapped_column(primary_key=True, autoincrement=True)
    ticket_id: Mapped[int] = mapped_column(
        ForeignKey("tickets.id", ondelete="CASCADE"),
        index=True,
        nullable=False,
    )

    actor:   Mapped[Actor] = mapped_column(ActorType, nullable=False)
    action:  Mapped[str]   = mapped_column(String, nullable=False)
    payload: Mapped[dict | None] = mapped_column(JSON, nullable=True)

    ts: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow, server_default=now_sql)
