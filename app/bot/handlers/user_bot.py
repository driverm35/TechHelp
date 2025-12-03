# app/bot/handlers/user_bot.py
from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from aiogram import Dispatcher, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.enums import ChatType
from aiogram.types import Message
from aiogram.exceptions import TelegramBadRequest
from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.models import TicketStatus, Actor, Ticket, User
from app.db.crud.user import get_or_create_user
from app.db.crud.ticket import TicketCRUD, add_event, get_tech_thread_by_user_and_tech
from app.db.crud.tech import get_technicians, get_auto_assign_technician_for_now
from app.db.crud.message import TicketMessageCRUD
from app.db.database import db_manager
from app.services.gspread_client import find_in_column_j_across_sheets


logger = logging.getLogger(__name__)

# ─────────────────────────────────────────────
#  Google Sheets
# ─────────────────────────────────────────────


async def get_client_data_from_sheets(tg_id: int) -> Optional[Dict[str, Any]]:
    return await find_in_column_j_across_sheets(
        spreadsheet=settings.gspread_spreadsheet,
        value=tg_id,
    )


# ─────────────────────────────────────────────
#  Хелперы для топика/шапки
# ─────────────────────────────────────────────

def _status_emoji(status: TicketStatus) -> str:
    return {
        TicketStatus.NEW: "🟢",
        TicketStatus.WORK: "🟡",
        TicketStatus.CLOSED: "⚪️",
    }.get(status, "⚪️")


def _build_topic_title(user: User, status: TicketStatus, assigned: bool) -> str:
    """
    Имя топика:
      🟢 [-] Имя (@username)
    Пока тикет никому не передан — добавляем тег [-].
    """
    parts: list[str] = [_status_emoji(status)]
    if not assigned:
        parts.append("[-]")

    name_bits: list[str] = []
    if user.first_name:
        name_bits.append(user.first_name)
    if user.last_name:
        name_bits.append(user.last_name)
    title = " ".join(name_bits) or user.username or str(user.tg_id)

    parts.append(title)
    if user.username:
        parts.append(f"(@{user.username})")

    return " ".join(parts)


def _build_client_header(user: User, sheet: Optional[Dict[str, Any]]) -> str:
    """
    Текст шапки клиента: работает строго с колонками по индексам A–M.
    """
    if sheet is None:
        return (
            "<b>Новый клиент</b>\n\n"
            f"TG: <a href=\"tg://user?id={user.tg_id}\">{user.first_name or user.username or user.tg_id}</a>"
            + (f"\nUsername: @{user.username}" if user.username else "")
        )

    lines = ["<b>Клиент по базе</b>"]

    if sheet.get("fio"):
        lines.append(f"ФИО: {sheet['fio']}")

    if sheet.get("city"):
        lines.append(f"Город: {sheet['city']}")

    if sheet.get("model"):
        lines.append(f"Модель: {sheet['model']}")

    if sheet.get("serial"):
        lines.append(f"Серийный номер: <code>{sheet['serial']}</code>")

    if sheet.get("warranty_date"):
        lines.append(f"Дата активации гарантии: {sheet['warranty_date']}")

    if sheet.get("order_date"):
        lines.append(f"Дата заказа: {sheet['order_date']}")

    if sheet.get("platform"):
        lines.append(f"Площадка: {sheet['platform']}")

    if sheet.get("phone"):
        lines.append(f"Телефон: <code>{sheet['phone']}</code>")

    lines.append("")
    lines.append(
        f"TG: <a href=\"tg://user?id={user.tg_id}\">{user.first_name or user.username or user.tg_id}</a>"
    )

    if user.username:
        lines.append(f"Username: @{user.username}")

    return "\n".join(lines)

def _extract_consonants(name: str, count: int = 3) -> str:
    """Извлечь первые N согласных букв из имени (как в main_group)."""
    consonants_ru = "БВГДЖЗЙКЛМНПРСТФХЦЧШЩбвгджзйклмнпрстфхцчшщ"
    consonants_en = "BCDFGHJKLMNPQRSTVWXYZbcdfghjklmnpqrstvwxyz"

    result: list[str] = []
    for ch in name:
        if ch in consonants_ru or ch in consonants_en:
            result.append(ch.upper())
            if len(result) >= count:
                break

    # если согласных мало — добираем первыми буквами
    if len(result) < 2:
        result = [c.upper() for c in name[:count] if c.isalpha()]

    return "".join(result[:count]) or "???"


def _build_main_topic_title_with_tech(
    user: User,
    status: TicketStatus,
    tech_tag: str | None = None,
) -> str:
    """
    Название топика в главной группе по шаблону main_group.py:
      🟢 [ТСТ] Имя (@username)
      или без тега, если tech_tag=None.
    """
    emoji = _status_emoji(status)
    parts: list[str] = [emoji]

    if tech_tag is not None:
        parts.append(f"[{tech_tag}]")

    name_bits: list[str] = []
    if user.first_name:
        name_bits.append(user.first_name)
    if user.last_name:
        name_bits.append(user.last_name)

    title = " ".join(name_bits) or user.username or f"User{user.tg_id}"
    parts.append(title)

    if user.username:
        parts.append(f"(@{user.username})")

    full = " ".join(parts)
    return full[:125] + "..." if len(full) > 128 else full


async def _build_technicians_keyboard(
    ticket_id: int,
    session: AsyncSession,
) -> InlineKeyboardMarkup:
    """
    Клавиатура техников:
        [Тех1] [Тех2]
        [Тех3] [Тех4]
    callback_data: assign_tech:<ticket_id>:<tech_id>
    """
    technicians = await get_technicians(session=session, active_only=True)

    kb = InlineKeyboardBuilder()
    for tech in technicians:
        kb.button(
            text=tech.name,
            callback_data=f"assign_tech:{ticket_id}:{tech.id}",
        )
    # по 2 кнопки в ряд
    kb.adjust(2)
    return kb.as_markup()


# ─────────────────────────────────────────────
#  Поиск/создание тикета и топика
# ─────────────────────────────────────────────

async def _get_last_ticket_for_client(
    session: AsyncSession,
    client_tg_id: int,
) -> Optional[Ticket]:
    """
    Последний тикет клиента (по created_at DESC).
    """
    from sqlalchemy import select, desc

    stmt = (
        select(Ticket)
        .where(Ticket.client_tg_id == client_tg_id)
        .order_by(desc(Ticket.created_at))
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _ensure_topic_and_ticket(
    message: Message,
    bot: Bot,
    session: AsyncSession,
    user: User,
) -> tuple[Ticket, int, bool]:
    """
    Гарантирует, что у пользователя есть:

      • Топик в группе поддержки
      • Актуальный тикет (status != CLOSED)

    Возвращает:
      (ticket, topic_id, is_new_ticket)

    Если предыдущий тикет был CLOSED — создаём новый, но в ТОМ ЖЕ топике:
      • переименовываем топик под новый тикет
      • шлём новую шапку и клавиатуру
    """
    support_chat_id = settings.main_group_id

    last_ticket = await _get_last_ticket_for_client(session=session, client_tg_id=user.tg_id)
    topic_id: Optional[int] = None
    ticket: Optional[Ticket] = None
    is_new_ticket = False

    if (
        last_ticket
        and last_ticket.main_chat_id == support_chat_id
        and last_ticket.main_thread_id
    ):
        topic_id = last_ticket.main_thread_id
        if last_ticket.status != TicketStatus.CLOSED:
            ticket = last_ticket

    # Если нет актуального тикета — создаём новый
    if ticket is None:
        if topic_id is None:
            # Топика ещё нет — создаём
            topic_title = _build_topic_title(user, TicketStatus.NEW, assigned=False)
            try:
                topic = await bot.create_forum_topic(
                    chat_id=support_chat_id,
                    name=topic_title,
                )
            except TelegramBadRequest as e:
                logger.error("Не удалось создать форум-топик: %s", e)
                raise

            topic_id = topic.message_thread_id
        else:
            # Топик есть (старый тикет закрыт) — переименуем под новый тикет
            topic_title = _build_topic_title(user, TicketStatus.NEW, assigned=False)
            try:
                await bot.edit_forum_topic(
                    chat_id=support_chat_id,
                    message_thread_id=topic_id,
                    name=topic_title,
                )
            except TelegramBadRequest as e:
                logger.warning("Не удалось переименовать форум-топик: %s", e)

        # создаём новый тикет с привязкой к этому топику
        ticket = await TicketCRUD.create_ticket(
            session=session,
            client_tg_id=user.tg_id,
            main_chat_id=support_chat_id,
            main_thread_id=topic_id,
            actor=Actor.CLIENT,
        )
        is_new_ticket = True

        # Попробуем автоназначить техника по его часам
        try:
            auto_tech = await get_auto_assign_technician_for_now(session=session)
        except Exception as e:
            logger.error("❌ Ошибка подбора техника для автоназначения: %s", e)
            auto_tech = None

        if auto_tech:
            ticket.assigned_tech_id = auto_tech.id
            await session.flush()
            logger.info(
                "🤖 Автоматически назначен техник %s (ID=%s) на тикет #%s",
                auto_tech.name,
                auto_tech.id,
                ticket.id,
            )

            # Обновляем название топика в главной группе: добавляем [ТЕГ]
            try:
                tag = _extract_consonants(auto_tech.name)
                new_title = _build_main_topic_title_with_tech(
                    user=user,
                    status=ticket.status,
                    tech_tag=tag,
                )
                await bot.edit_forum_topic(
                    chat_id=support_chat_id,
                    message_thread_id=topic_id,
                    name=new_title,
                )
                logger.info(
                    "📝 Название топика обновлено для автоназначенного техника: %s",
                    new_title,
                )
            except TelegramBadRequest as e:
                logger.warning(
                    "⚠️ Не удалось обновить название топика при автоназначении: %s",
                    e,
                )
            # Создаём тех-топик и отправляем шапку/кнопки/первое сообщение в группу техника
            try:
                from app.db.crud.tech import get_technician_by_id, get_or_create_tech_thread
                from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

                tech = await get_technician_by_id(session=session, tech_id=auto_tech.id)
                if tech and tech.group_chat_id:
                    # формируем название топика для техники (без тега)
                    tech_topic_title = _build_main_topic_title_with_tech(
                        user=user,
                        status=ticket.status,
                        tech_tag=None,
                    )
                    try:
                        topic = await bot.create_forum_topic(
                            chat_id=tech.group_chat_id,
                            name=tech_topic_title,
                        )
                    except TelegramBadRequest as e:
                        logger.error(
                            "❌ Не удалось создать тех-топик при автоназначении: %s",
                            e,
                        )
                        topic = None

                    tech_thread = None
                    if topic:
                        tech_thread = await get_or_create_tech_thread(
                            session=session,
                            ticket_id=ticket.id,
                            user_id=ticket.client_tg_id,
                            tech_id=tech.id,
                            tech_chat_id=tech.group_chat_id,
                            tech_thread_id=topic.message_thread_id,
                        )

                    # отправляем шапку
                    if tech_thread:
                        try:
                            sheet_data = await get_client_data_from_sheets(user.tg_id)
                            header_text = _build_client_header(user, sheet_data)
                            await bot.send_message(
                                chat_id=tech_thread.tech_chat_id,
                                message_thread_id=tech_thread.tech_thread_id,
                                text=header_text,
                                parse_mode="HTML",
                                disable_web_page_preview=True,
                            )
                            logger.info("✅ Шапка клиента отправлена в тех-топик (автоназначение)")
                        except Exception as e:
                            logger.error("❌ Ошибка при отправке шапки клиента в тех-топик: %s", e)

                        # кнопки управления статусом
                        try:
                            status_kb = InlineKeyboardMarkup(
                                inline_keyboard=[[
                                    InlineKeyboardButton(
                                        text="🟡 В работе",
                                        callback_data=f"status_work:{ticket.id}",
                                    ),
                                    InlineKeyboardButton(
                                        text="⚪️ Закрыть",
                                        callback_data=f"status_close:{ticket.id}",
                                    ),
                                ]]
                            )
                            status_msg = await bot.send_message(
                                chat_id=tech_thread.tech_chat_id,
                                message_thread_id=tech_thread.tech_thread_id,
                                text="🎛 <b>Управление статусом:</b>",
                                reply_markup=status_kb,
                                parse_mode="HTML",
                            )
                            try:
                                await bot.pin_chat_message(
                                    chat_id=tech_thread.tech_chat_id,
                                    message_id=status_msg.message_id,
                                    disable_notification=True,
                                )
                                logger.info("📌 Кнопки статусов закреплены в тех-топике (автоназначение)")
                            except Exception as e:
                                logger.warning(
                                    "⚠️ Не удалось закрепить кнопки статусов в тех-топике: %s",
                                    e,
                                )
                        except Exception as e:
                            logger.error("❌ Ошибка отправки кнопок статусов в тех-топик: %s", e)

                        # копируем первое сообщение клиента в тех-топик и сохраняем
                        try:
                            sent_msg = await bot.copy_message(
                                chat_id=tech_thread.tech_chat_id,
                                message_thread_id=tech_thread.tech_thread_id,
                                from_chat_id=message.chat.id,
                                message_id=message.message_id,
                            )
                            logger.info("✅ Первое сообщение отправлено в тех-группу (автоназначение)")
                            try:
                                await bot.pin_chat_message(
                                    chat_id=tech_thread.tech_chat_id,
                                    message_id=sent_msg.message_id,
                                    disable_notification=True,
                                )
                            except Exception:
                                pass

                            await TicketMessageCRUD.add_message(
                                session=session,
                                ticket_id=ticket.id,
                                user_id=user.tg_id,
                                message_text=message.text or message.caption or "[медиа]",
                                is_from_admin=False,
                                telegram_message_id=sent_msg.message_id,
                            )
                        except TelegramBadRequest as e:
                            logger.warning("⚠️ Не удалось скопировать сообщение в тех-группу: %s", e)
                        except Exception as e:
                            logger.error("❌ Ошибка при копировании первого сообщения в тех-топик: %s", e)
            except Exception:
                logger.exception("❌ Непредвиденная ошибка при создании тех-топика при автоназначении")

            # Если были изменения при автоназначении — зафиксируем их немедленно,
            # чтобы последующие вебхуки (сообщения в главной группе) видели назначение
            try:
                await session.commit()
            except Exception as e:
                logger.warning("⚠️ Не удалось закоммитить сессию после автоназначения: %s", e)


    assert topic_id is not None
    return ticket, topic_id, is_new_ticket


# ─────────────────────────────────────────────
#  Отправка в топик
# ─────────────────────────────────────────────

async def _send_header_and_first_message(
    *,
    bot: Bot,
    session: AsyncSession,
    user: User,
    ticket: Ticket,
    topic_id: int,
    message: Message,
) -> None:
    """
    Для нового тикета:
      1) шапка с данными клиента
      2) копия сообщения клиента с клавиатурой выбора техника
      3) сохранение сообщения в БД
      4) кнопки управления статусом
      5) закрепление сообщений
    """
    from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

    sheet_data = await get_client_data_from_sheets(user.tg_id)
    header_text = _build_client_header(user, sheet_data)
    try:
        status_keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                    [InlineKeyboardButton(
                        text="🟡 В работе",
                        callback_data=f"status_work:{ticket.id}"
                    ),
                    InlineKeyboardButton(
                        text="⚪️ Закрыть",
                        callback_data=f"status_close:{ticket.id}"
                    )
                ]
            ]
        )

        status_msg = await bot.send_message(
            chat_id=settings.main_group_id,
            message_thread_id=topic_id,
            text="<b>Управление статусом:</b>",
            reply_markup=status_keyboard,
            parse_mode="HTML"
        )

        # Закрепляем кнопки статусов
        try:
            await bot.pin_chat_message(
                chat_id=settings.main_group_id,
                message_id=status_msg.message_id,
                disable_notification=True,
            )
            logger.info("📌 Кнопки статусов закреплены")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось закрепить кнопки статусов: {e}")

    except Exception as e:
        logger.error(f"❌ Ошибка отправки кнопок статусов: {e}")

    # 1) шапка в главной группе
    await bot.send_message(
        chat_id=settings.main_group_id,
        message_thread_id=topic_id,
        text=header_text,
        parse_mode="HTML",
        disable_web_page_preview=True,
    )

    # 2) Кого назначаем техником — клавиатура
    kb_tech = await _build_technicians_keyboard(ticket.id, session)
    await bot.send_message(
        chat_id=settings.main_group_id,
        message_thread_id=topic_id,
        text="Техники для назначения:",
        parse_mode="HTML",
        disable_web_page_preview=True,
        reply_markup=kb_tech,
    )

    media_type = None
    media_file_id = None
    media_caption = None

    if message.photo:
        media_type = "photo"
        media_file_id = message.photo[-1].file_id
        media_caption = message.caption
    elif message.video:
        media_type = "video"
        media_file_id = message.video.file_id
        media_caption = message.caption
    elif message.document:
        media_type = "document"
        media_file_id = message.document.file_id
        media_caption = message.caption
    elif message.voice:
        media_type = "voice"
        media_file_id = message.voice.file_id

    message_text = message.text or message.caption or "[медиа]"

    try:
        sent_msg = await bot.copy_message(
            chat_id=settings.main_group_id,
            message_thread_id=topic_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        logger.info("✅ Первое сообщение отправлено в главную группу")

        # ЗАКРЕПЛЯЕМ первое сообщение
        try:
            await bot.pin_chat_message(
                chat_id=settings.main_group_id,
                message_id=sent_msg.message_id,
                disable_notification=True,
            )
            logger.info("📌 Первое сообщение закреплено")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось закрепить сообщение: {e}")

        # 3) Сохраняем в БД
        await TicketMessageCRUD.add_message(
            session=session,
            ticket_id=ticket.id,
            user_id=user.tg_id,
            message_text=message_text,
            is_from_admin=False,
            media_type=media_type,
            media_file_id=media_file_id,
            media_caption=media_caption,
            telegram_message_id=sent_msg.message_id,
        )

    except TelegramBadRequest as e:
        if "can't be copied" in str(e).lower():
            text = message.text or message.caption or "[медиа]"
            sent_msg = await bot.send_message(
                chat_id=settings.main_group_id,
                message_thread_id=topic_id,
                text=f"Сообщение от клиента:\n\n{text}",
            )

            # Закрепляем
            try:
                await bot.pin_chat_message(
                    chat_id=settings.main_group_id,
                    message_id=sent_msg.message_id,
                    disable_notification=True,
                )
            except Exception:
                pass

            await TicketMessageCRUD.add_message(
                session=session,
                ticket_id=ticket.id,
                user_id=user.tg_id,
                message_text=text,
                is_from_admin=False,
                telegram_message_id=sent_msg.message_id,
            )
        else:
            logger.error(f"❌ Ошибка отправки первого сообщения: {e}")
            raise


    # Логируем как Event
    await add_event(
        session=session,
        ticket_id=ticket.id,
        actor=Actor.CLIENT,
        action="client_message",
        payload={
            "telegram_message_id": message.message_id,
            "chat_id": message.chat.id,
            "text": message_text,
            "is_first": True,
        },
    )


async def _forward_message_to_topic(
    *,
    bot: Bot,
    session: AsyncSession,
    user: User,
    ticket: Ticket,
    topic_id: int,
    message: Message,
) -> None:
    """
    Обычная пересылка сообщения клиента в его топик главной группы.
    Если назначен техник - дублируем в его группу.
    Сохраняем сообщение в БД.
    """

    # Определяем медиа
    media_type = None
    media_file_id = None
    media_caption = None

    if message.photo:
        media_type = "photo"
        media_file_id = message.photo[-1].file_id
        media_caption = message.caption
    elif message.video:
        media_type = "video"
        media_file_id = message.video.file_id
        media_caption = message.caption
    elif message.document:
        media_type = "document"
        media_file_id = message.document.file_id
        media_caption = message.caption
    elif message.voice:
        media_type = "voice"
        media_file_id = message.voice.file_id

    message_text = message.text or message.caption or "[медиа]"

    # Пересылаем в главную группу
    try:
        sent_msg = await bot.copy_message(
            chat_id=settings.main_group_id,
            message_thread_id=topic_id,
            from_chat_id=message.chat.id,
            message_id=message.message_id,
        )
        logger.info(
            "✅ Сообщение переслано в главную группу (топик %s)", topic_id
        )

        # Сохраняем в БД
        await TicketMessageCRUD.add_message(
            session=session,
            ticket_id=ticket.id,
            user_id=user.tg_id,
            message_text=message_text,
            is_from_admin=False,
            media_type=media_type,
            media_file_id=media_file_id,
            media_caption=media_caption,
            telegram_message_id=sent_msg.message_id,
        )

    except TelegramBadRequest as e:
        if "can't be copied" in str(e).lower():
            logger.warning(
                "⚠️ Сообщение %s нельзя скопировать (тип: %s)",
                message.message_id,
                message.content_type,
            )
        else:
            logger.error("❌ Ошибка пересылки в главную группу: %s", e)
    except Exception as e:
        logger.error("❌ Ошибка пересылки в главную группу: %s", e)

    # Если тикет не назначен — ничего больше не делаем
    if not ticket.assigned_tech_id:
        # Лог события только о пересылке в main (ниже тоже логируется)
        await add_event(
            session=session,
            ticket_id=ticket.id,
            actor=Actor.CLIENT,
            action="client_message",
            payload={
                "telegram_message_id": message.message_id,
                "chat_id": message.chat.id,
                "text": message_text,
                "is_first": False,
            },
        )
        return

    # Пытаемся найти существующий тех-топик
    tech_thread = await get_tech_thread_by_user_and_tech(
        session=session,
        user_id=ticket.client_tg_id,
        tech_id=ticket.assigned_tech_id,
    )

    # Ленивая генерация тех-топика для автоназначенного техника
    if not tech_thread:
        from app.db.crud.tech import get_technician_by_id, get_or_create_tech_thread
        from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton

        tech = await get_technician_by_id(
            session=session,
            tech_id=ticket.assigned_tech_id,
        )
        if not tech:
            logger.warning(
                "❌ Автоназначенный техник не найден в БД: tech_id=%s ticket_id=%s",
                ticket.assigned_tech_id,
                ticket.id,
            )
            return

        if not tech.group_chat_id:
            logger.warning(
                "❌ У техника нет group_chat_id: tech_id=%s ticket_id=%s",
                tech.id,
                ticket.id,
            )
            return

        # 👤 Формируем нормальное имя топика техника (как в main_group: без тега)
        tech_topic_title = _build_main_topic_title_with_tech(
            user=user,
            status=ticket.status,
            tech_tag=None,  # в топике техника тега нет
        )

        # Создаём новый топик в группе техника
        try:
            topic = await bot.create_forum_topic(
                chat_id=tech.group_chat_id,
                name=tech_topic_title,
            )
        except TelegramBadRequest as e:
            logger.error(
                "❌ Не удалось создать тех-топик для автоназначения: %s",
                e,
                exc_info=True,
            )
            return

        # ✅ Создаём / получаем запись тех-топика в БД (без двойного insert)
        tech_thread = await get_or_create_tech_thread(
            session=session,
            ticket_id=ticket.id,
            user_id=ticket.client_tg_id,
            tech_id=tech.id,
            tech_chat_id=tech.group_chat_id,
            tech_thread_id=topic.message_thread_id,
        )

        logger.info(
            "✅ Создан тех-топик по автоназначению: "
            "ticket_id=%s tech_id=%s group=%s topic_id=%s",
            ticket.id,
            tech.id,
            tech_thread.tech_chat_id,
            tech_thread.tech_thread_id,
        )

        # 📋 Шапка клиента из таблицы — сразу в тех-топик
        try:
            sheet_data = await get_client_data_from_sheets(user.tg_id)
            header_text = _build_client_header(user, sheet_data)

            await bot.send_message(
                chat_id=tech_thread.tech_chat_id,
                message_thread_id=tech_thread.tech_thread_id,
                text=header_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            logger.info("✅ Шапка клиента отправлена в тех-топик (автоназначение)")
        except Exception as e:
            logger.error("❌ Ошибка отправки шапки клиента в тех-топик: %s", e)

        # 🎛 Кнопки управления статусом заявки в тех-топик + пин
        try:
            status_kb = InlineKeyboardMarkup(
                inline_keyboard=[[
                    InlineKeyboardButton(
                        text="🟡 В работе",
                        callback_data=f"status_work:{ticket.id}",
                    ),
                    InlineKeyboardButton(
                        text="⚪️ Закрыть",
                        callback_data=f"status_close:{ticket.id}",
                    ),
                ]]
            )
            status_msg = await bot.send_message(
                chat_id=tech_thread.tech_chat_id,
                message_thread_id=tech_thread.tech_thread_id,
                text="🎛 <b>Управление статусом:</b>",
                reply_markup=status_kb,
                parse_mode="HTML",
            )
            try:
                await bot.pin_chat_message(
                    chat_id=tech_thread.tech_chat_id,
                    message_id=status_msg.message_id,
                    disable_notification=True,
                )
                logger.info("📌 Кнопки статусов закреплены в тех-топике (автоназначение)")
            except Exception as e:
                logger.warning(
                    "⚠️ Не удалось закрепить кнопки статусов в тех-топике: %s",
                    e,
                )
        except Exception as e:
            logger.error(
                "❌ Ошибка отправки кнопок статусов в тех-топик: %s",
                e,
            )

    # Логируем событие
    await add_event(
        session=session,
        ticket_id=ticket.id,
        actor=Actor.CLIENT,
        action="client_message",
        payload={
            "telegram_message_id": message.message_id,
            "chat_id": message.chat.id,
            "text": message_text,
            "is_first": False,
        },
    )


# ─────────────────────────────────────────────
#  Хэндлер для user-bot
# ─────────────────────────────────────────────

async def handle_any_user_message(
    message: Message,
    bot: Bot,
    state: FSMContext,
) -> None:
    """
    Любое сообщение от пользователя в личке бота.

    Логика:
      1) Берём любое сообщение от пользователя.
      2) Проверяем, есть ли он в БД — если нет, создаём / обновляем.
      3) Проверяем, есть ли топик и актуальный тикет:
         • если тикет закрыт или его нет — создаём новый тикет (🟢 [-] ...),
           при необходимости создаём/переименовываем топик.
      4) Если тикет новый:
         • шапка с данными из Google Sheets / "новый клиент"
         • копия сообщения клиента с клавиатурой техников.
      5) Если тикет не новый и не закрыт:
         • просто пересылаем сообщение в его топик.
    """
    if not message.from_user:
        return

    # 🔒 Админов здесь игнорируем, чтобы для них не создавались тикеты/топики
    if settings.is_admin(message.from_user.id):
        # Можно вообще молча игнорить, либо что-то ответить:
        # await message.answer("Это пользовательский бот поддержки, для работы админа используйте группу.")
        return

    current_state = await state.get_state()
    if current_state is not None:
        return

    async with db_manager.session() as db:  # 🔹 сами берём AsyncSession
        # 1) юзер в БД
        user = await get_or_create_user(
            db,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

        # 2) тикет + топик
        ticket, topic_id, is_new_ticket = await _ensure_topic_and_ticket(
            message=message,
            bot=bot,
            session=db,
            user=user,
        )

        # 3) отправка
        if is_new_ticket:
            await _send_header_and_first_message(
                bot=bot,
                session=db,
                user=user,
                ticket=ticket,
                topic_id=topic_id,
                message=message,
            )
        else:
            if ticket.status == TicketStatus.CLOSED:
                # До сюда по идее не дойдём, но на всякий случай
                logger.warning("Сообщение по закрытому тикету %s", ticket.id)
                return

            await _forward_message_to_topic(
                bot=bot,
                session=db,
                user=user,
                ticket=ticket,
                topic_id=topic_id,
                message=message,
            )


def register_handlers(dp: Dispatcher) -> None:
    """
    Регистрация обработчиков для пользовательского бота.
    Включаем один хэндлер: любое сообщение в личке.
    """
    dp.message.register(
        handle_any_user_message,
        F.chat.type == ChatType.PRIVATE,
        # StateFilter(None),
    )
