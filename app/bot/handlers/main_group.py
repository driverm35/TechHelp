# app/bot/handlers/main_group.py
from __future__ import annotations
import logging
import asyncio

from aiogram import Dispatcher, F, Bot
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.db.database import db_manager
from app.db.models import Ticket, TechThread, TicketStatus, Technician
from app.db.crud.ticket import get_all_tech_threads_for_ticket
from app.db.crud.tech import get_technicians, get_technician_by_id
from app.utils.cache import cache


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Вспомогательные функции
# ─────────────────────────────────────────────

def _extract_consonants(name: str, count: int = 3) -> str:
    """Извлечь первые N согласных букв из имени."""
    consonants_ru = "БВГДЖЗЙКЛМНПРСТФХЦЧШЩбвгджзйклмнпрстфхцчшщ"
    consonants_en = "BCDFGHJKLMNPQRSTVWXYZbcdfghjklmnpqrstvwxyz"

    result = []
    for char in name:
        if char in consonants_ru or char in consonants_en:
            result.append(char.upper())
            if len(result) >= count:
                break

    # Если не хватает согласных, возьмем первые буквы
    if len(result) < 2:
        result = [c.upper() for c in name[:count] if c.isalpha()]

    return "".join(result[:count]) or "???"


def _status_emoji(status: TicketStatus) -> str:
    """Получить эмодзи статуса."""
    return {
        TicketStatus.NEW: "🟢",
        TicketStatus.WORK: "🟡",
        TicketStatus.CLOSED: "⚪️",
    }.get(status, "⚪️")


def _build_topic_title(
    status: TicketStatus,
    client_name: str,
    client_username: str | None = None,
    tech_tag: str | None = None,
) -> str:
    """
    Построить название топика по единому шаблону.

    Args:
        status: Статус тикета
        client_name: Имя клиента
        client_username: Username клиента
        tech_tag: Тег техника (только для главной группы)

    Returns:
        Название топика

    Примеры:
        - Главная группа: "🟢 [ПВЛ] Иван (@ivan)"
        - Группа техника: "🟢 Иван (@ivan)"
        - Без техника: "🟢 [-] Иван (@ivan)"
    """
    emoji = _status_emoji(status)

    parts = [emoji]

    # Добавляем тег (для главной группы)
    if tech_tag is not None:
        parts.append(f"[{tech_tag}]")

    # Имя клиента
    parts.append(client_name)

    # Username если есть
    if client_username:
        parts.append(f"(@{client_username})")

    title = " ".join(parts)

    # Telegram ограничивает 128 символов
    if len(title) > 128:
        title = title[:125] + "..."

    return title


def _get_status_control_keyboard(ticket_id: int) -> InlineKeyboardMarkup:
    """
    Клавиатура управления статусом тикета.

    Args:
        ticket_id: ID тикета

    Returns:
        Клавиатура с кнопками статусов
    """
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="🟡 В работе",
                    callback_data=f"status_work:{ticket_id}"
                ),
                InlineKeyboardButton(
                    text="⚪️ Закрыть",
                    callback_data=f"status_close:{ticket_id}"
                ),
            ],
        ]
    )


async def _update_all_topic_titles(
    bot: Bot,
    ticket: Ticket,
    db: AsyncSession,
) -> None:
    """Обновить названия всех топиков с проверкой кеша."""
    if not ticket.client:
        logger.error(f"❌ У тикета {ticket.id} нет загруженного client")
        return

    client_name = (
        ticket.client.first_name
        or ticket.client.username
        or f"User{ticket.client.tg_id}"
    )
    client_username = ticket.client.username

    # ─────────────────────────────────────────────
    # 1. Обновляем топик в главной группе
    # ─────────────────────────────────────────────

    # ВАЖНО: В главном топике ВСЕГДА показываем тег техника, если он назначен
    tech_tag = "-"  # По умолчанию, если техник не назначен

    if ticket.assigned_tech_id:
        tech = await get_technician_by_id(session=db, tech_id=ticket.assigned_tech_id)
        if tech:
            tech_tag = _extract_consonants(tech.name)
            logger.debug(f"   Техник: {tech.name} → тег [{tech_tag}]")
        else:
            logger.warning(f"⚠️ Техник #{ticket.assigned_tech_id} не найден в БД")
            tech_tag = "???"

    main_title = _build_topic_title(
        status=ticket.status,
        client_name=client_name,
        client_username=client_username,
        tech_tag=tech_tag,  #  Всегда передаем тег (даже если это "-")
    )

    logger.debug(f"📝 Главная группа: новое название '{main_title}'")

    # Проверяем кеш
    cached_title = await cache.get_topic_title(
        ticket.main_chat_id,
        ticket.main_thread_id
    )

    if cached_title != main_title:
        try:
            await bot.edit_forum_topic(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id,
                name=main_title
            )
            logger.info(f"✅ Обновлено название топика в главной группе: '{main_title}'")

            # Сохраняем в кеш
            await cache.set_topic_title(
                ticket.main_chat_id,
                ticket.main_thread_id,
                main_title
            )
        except TelegramBadRequest as e:
            if "TOPIC_NOT_MODIFIED" in str(e):
                await cache.set_topic_title(
                    ticket.main_chat_id,
                    ticket.main_thread_id,
                    main_title
                )
                logger.debug("ℹ️ Название топика главной группы уже актуально (кеш обновлен)")
            else:
                logger.error(f"❌ Ошибка обновления главного топика: {e}")
    else:
        logger.debug("ℹ️ Название топика главной группы не изменилось (пропускаем)")

    # ─────────────────────────────────────────────
    # 2. Обновляем ВСЕ топики техников для этого тикета
    # ─────────────────────────────────────────────

    tech_threads = await get_all_tech_threads_for_ticket(session=db, ticket_id=ticket.id)

    for tech_thread in tech_threads:
        # 🔹 В топике техника тег НЕ показываем
        tech_title = _build_topic_title(
            status=ticket.status,
            client_name=client_name,
            client_username=client_username,
            tech_tag=None,  # В топике техника тега нет
        )

        logger.debug(
            f"📝 Топик техника #{tech_thread.tech_id}: новое название '{tech_title}' "
            f"(группа {tech_thread.tech_chat_id}, топик {tech_thread.tech_thread_id})"
        )

        # Проверяем кеш
        cached_tech_title = await cache.get_topic_title(
            tech_thread.tech_chat_id,
            tech_thread.tech_thread_id
        )

        if cached_tech_title != tech_title:
            try:
                await bot.edit_forum_topic(
                    chat_id=tech_thread.tech_chat_id,
                    message_thread_id=tech_thread.tech_thread_id,
                    name=tech_title
                )
                logger.info(
                    f"✅ Обновлено название топика у техника #{tech_thread.tech_id}: '{tech_title}' "
                    f"(группа {tech_thread.tech_chat_id}, топик {tech_thread.tech_thread_id})"
                )

                await cache.set_topic_title(
                    tech_thread.tech_chat_id,
                    tech_thread.tech_thread_id,
                    tech_title
                )
            except TelegramBadRequest as e:
                if "TOPIC_NOT_MODIFIED" in str(e):
                    await cache.set_topic_title(
                        tech_thread.tech_chat_id,
                        tech_thread.tech_thread_id,
                        tech_title
                    )
                    logger.debug(f"ℹ️ Название топика техника #{tech_thread.tech_id} уже актуально")
                else:
                    logger.error(f"❌ Ошибка обновления топика техника: {e}")
        else:
            logger.debug(f"ℹ️ Название топика техника #{tech_thread.tech_id} не изменилось")


async def _pin_message_in_topic(
    bot: Bot,
    chat_id: int,
    thread_id: int,
    message_id: int,
) -> bool:
    """
    Закрепить сообщение в топике.

    Args:
        bot: Экземпляр бота
        chat_id: ID чата
        thread_id: ID топика
        message_id: ID сообщения

    Returns:
        True если успешно
    """
    try:
        await bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message_id,
            disable_notification=True,
        )
        logger.info(f"📌 Закреплено сообщение {message_id} в топике {thread_id}")
        return True
    except TelegramBadRequest as e:
        logger.warning(f"⚠️ Не удалось закрепить сообщение: {e}")
        return False


async def _get_ticket_by_thread(
    session: AsyncSession,
    chat_id: int,
    thread_id: int
) -> Ticket | None:
    """Получить тикет по chat_id и thread_id с кешем (берём последний, если их несколько)."""
    # 1) Пробуем из кеша
    ticket_id = await cache.get_ticket_by_main_thread(chat_id, thread_id)

    if ticket_id:
        logger.debug(f"📦 Тикет #{ticket_id} получен из кеша")
        stmt = select(Ticket).where(Ticket.id == ticket_id)
        res = await session.execute(stmt)
        # тут по первичному ключу всё равно одна строка
        return res.scalars().first()

    # 2) Ищем в БД по chat_id + thread_id
    stmt = (
        select(Ticket)
        .where(
            Ticket.main_chat_id == chat_id,
            Ticket.main_thread_id == thread_id,
        )
        .order_by(Ticket.id.desc())  # или created_at.desc(), если есть
        .limit(1)
    )
    res = await session.execute(stmt)
    ticket = res.scalars().first()

    if ticket:
        await cache.set_ticket_by_main_thread(chat_id, thread_id, ticket.id)

        # Логируем, если вдруг нашлось больше одной записи (для диагностики)
        try:
            stmt_count = (
                select(Ticket)
                .where(
                    Ticket.main_chat_id == chat_id,
                    Ticket.main_thread_id == thread_id,
                )
            )
            res_count = await session.execute(stmt_count)
            all_tickets = res_count.scalars().all()
            if len(all_tickets) > 1:
                logger.warning(
                    "⚠️ Для main_chat_id=%s, main_thread_id=%s найдено %s тикета. "
                    "Используем последний с id=%s",
                    chat_id, thread_id, len(all_tickets), ticket.id,
                )
        except Exception:
            pass

    return ticket



async def _get_tech_thread(
    session: AsyncSession,
    ticket_id: int,
    tech_id: int
) -> TechThread | None:
    """Получить TechThread по ticket_id и tech_id."""
    stmt = (
        select(TechThread)
        .where(
            TechThread.ticket_id == ticket_id,
            TechThread.tech_id == tech_id
        )
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()

async def _reopen_tech_topic(
    bot: Bot,
    tech_chat_id: int,
    tech_thread_id: int,
) -> None:
    """Переоткрыть топик в группе техника, если он закрыт."""
    try:
        await bot.reopen_forum_topic(
            chat_id=tech_chat_id,
            message_thread_id=tech_thread_id,
        )
        logger.info(f"✅ Топик {tech_thread_id} переоткрыт в группе {tech_chat_id}")
    except TelegramBadRequest as e:
        # Если уже открыт — Телеграм может вернуть ошибку, логируем как debug
        logger.debug(f"ℹ️ Не удалось переоткрыть топик {tech_thread_id}: {e}")

async def _close_tech_topic(
    bot: Bot,
    tech_chat_id: int,
    tech_thread_id: int
) -> None:
    """Закрыть топик в группе техника."""
    try:
        await bot.close_forum_topic(
            chat_id=tech_chat_id,
            message_thread_id=tech_thread_id
        )
        logger.info(f"✅ Топик {tech_thread_id} закрыт в группе {tech_chat_id}")
    except TelegramBadRequest as e:
        logger.warning(f"⚠️ Не удалось закрыть топик: {e}")


async def _create_tech_topic(
    bot: Bot,
    tech: Technician,
    topic_name: str
) -> int | None:
    """Создать топик в группе техника."""
    if not tech.group_chat_id:
        logger.error(f"❌ У техника {tech.name} нет привязанной группы")
        return None

    try:
        topic = await bot.create_forum_topic(
            chat_id=tech.group_chat_id,
            name=topic_name
        )
        logger.info(f"✅ Создан топик '{topic_name}' в группе {tech.group_chat_id}")
        return topic.message_thread_id
    except TelegramBadRequest as e:
        logger.error(f"❌ Не удалось создать топик: {e}")
        return None

async def _get_client_header_text(ticket: Ticket) -> str:
    """
    Получить текст шапки с данными клиента.

    Args:
        ticket: Тикет (должен быть загружен с client)

    Returns:
        HTML-форматированный текст шапки
    """
    if not ticket.client:
        return "<b>Клиент</b>\n\nИнформация недоступна"

    user = ticket.client

    # Пытаемся получить данные из Google Sheets
    try:
        from app.bot.handlers.user_bot import get_client_data_from_sheets
        sheet_data = await get_client_data_from_sheets(user.tg_id)
    except Exception:
        sheet_data = None

    if sheet_data is None:
        # Новый клиент - только данные из телеги
        lines = [
            "<b>Новый клиент</b>",
            "",
            f"TG: <a href=\"tg://user?id={user.tg_id}\">{user.first_name or user.username or user.tg_id}</a>",
        ]
        if user.username:
            lines.append(f"Username: @{user.username}")
        return "\n".join(lines)

    # Клиент найден в гугл-таблице
    lines = ["<b>Клиент по базе</b>"]

    def g(*keys: str) -> str | None:
        for k in keys:
            if k in sheet_data and sheet_data[k]:
                return str(sheet_data[k])
        return None

    fio = g("ФИО", "fio")
    if fio:
        lines.append(f"ФИО: {fio}")

    city = g("Город", "город", "city")
    if city:
        lines.append(f"Город: {city}")

    model = g("Модель", "модель", "model")
    if model:
        lines.append(f"Модель: {model}")

    serial = g("Серийный номер", "серийный номер", "serial")
    if serial:
        lines.append(f"Серийный номер: <code>{serial}</code>")

    warranty_date = g("Дата активации гарантии", "warranty_date")
    if warranty_date:
        lines.append(f"Дата активации гарантии: {warranty_date}")

    order_date = g("Дата Заказа", "Дата заказа", "order_date")
    if order_date:
        lines.append(f"Дата заказа: {order_date}")

    platform = g("Площадка", "platform")
    if platform:
        lines.append(f"Площадка: {platform}")

    phone = g("Телефон", "phone")
    if phone:
        lines.append(f"Телефон: <code>{phone}</code>")

    lines.append("")
    lines.append(
        f"TG: <a href=\"tg://user?id={user.tg_id}\">{user.first_name or user.username or user.tg_id}</a>"
    )
    if user.username:
        lines.append(f"Username: @{user.username}")

    return "\n".join(lines)


async def _copy_ticket_history_to_tech(
    bot: Bot,
    ticket: Ticket,
    tech_chat_id: int,
    tech_thread_id: int,
    db: AsyncSession,
) -> int:
    """
    Скопировать всю историю тикета в топик техника.

    Args:
        bot: Экземпляр бота
        ticket: Тикет (должен быть загружен с client и messages)
        tech_chat_id: ID группы техника
        tech_thread_id: ID топика в группе техника
        db: Сессия БД

    Returns:
        Количество скопированных сообщений
    """
    copied_count = 0

    try:
        # 1. Отправляем шапку с данными клиента
        header_text = await _get_client_header_text(ticket)

        try:
            await bot.send_message(
                chat_id=tech_chat_id,
                message_thread_id=tech_thread_id,
                text=header_text,
                parse_mode="HTML",
                disable_web_page_preview=True,
            )
            logger.info("✅ Шапка клиента отправлена в топик техника")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки шапки: {e}")

        # 2. Получаем все сообщения тикета из БД
        from sqlalchemy import select as sql_select
        from app.db.models import TicketMessage

        stmt = (
            sql_select(TicketMessage)
            .where(TicketMessage.ticket_id == ticket.id)
            .order_by(TicketMessage.created_at)
        )
        result = await db.execute(stmt)
        messages = result.scalars().all()

        if not messages:
            logger.info("ℹ️ История сообщений пуста")
            return copied_count

        logger.info(f"📋 Найдено {len(messages)} сообщений для копирования")

        # 3. Копируем каждое сообщение
        for msg in messages:
            try:
                # Формируем префикс отправителя
                if msg.is_from_admin:
                    prefix = "🛠️ <b>Поддержка:</b>\n"
                else:
                    prefix = "👤 <b>Клиент:</b>\n"

                # Если есть медиа
                if msg.has_media and msg.media_file_id:
                    caption = f"{prefix}{msg.media_caption or ''}" if msg.media_caption else prefix.rstrip()

                    # Ограничиваем длину caption
                    if len(caption) > 1000:
                        caption = caption[:997] + "..."

                    if msg.media_type == "photo":
                        await bot.send_photo(
                            chat_id=tech_chat_id,
                            message_thread_id=tech_thread_id,
                            photo=msg.media_file_id,
                            caption=caption,
                            parse_mode="HTML",
                        )
                    elif msg.media_type == "video":
                        await bot.send_video(
                            chat_id=tech_chat_id,
                            message_thread_id=tech_thread_id,
                            video=msg.media_file_id,
                            caption=caption,
                            parse_mode="HTML",
                        )
                    elif msg.media_type == "document":
                        await bot.send_document(
                            chat_id=tech_chat_id,
                            message_thread_id=tech_thread_id,
                            document=msg.media_file_id,
                            caption=caption,
                            parse_mode="HTML",
                        )
                    elif msg.media_type == "voice":
                        await bot.send_voice(
                            chat_id=tech_chat_id,
                            message_thread_id=tech_thread_id,
                            voice=msg.media_file_id,
                            caption=caption,
                            parse_mode="HTML",
                        )
                    else:
                        # Неизвестный тип медиа - отправляем как текст
                        text = f"{prefix}{msg.message_text}"
                        await bot.send_message(
                            chat_id=tech_chat_id,
                            message_thread_id=tech_thread_id,
                            text=text[:4000],  # Ограничение Telegram
                            parse_mode="HTML",
                        )
                else:
                    # Обычное текстовое сообщение
                    text = f"{prefix}{msg.message_text}"

                    # Ограничиваем длину
                    if len(text) > 4000:
                        text = text[:3997] + "..."

                    await bot.send_message(
                        chat_id=tech_chat_id,
                        message_thread_id=tech_thread_id,
                        text=text,
                        parse_mode="HTML",
                    )

                copied_count += 1

                # Небольшая задержка чтобы не словить rate limit
                if copied_count % 10 == 0:
                    await asyncio.sleep(0.5)

            except TelegramBadRequest as e:
                logger.warning(f"⚠️ Не удалось скопировать сообщение: {e}")
            except Exception as e:
                logger.error(f"❌ Ошибка копирования сообщения: {e}")

        logger.info(f"✅ Скопировано {copied_count} сообщений из {len(messages)}")

        # 4. Отправляем разделитель
        try:
            await bot.send_message(
                chat_id=tech_chat_id,
                message_thread_id=tech_thread_id,
                text="📍 <b>Конец истории</b>",
                parse_mode="HTML",
            )
        except Exception:
            pass

    except Exception as e:
        logger.error(f"❌ Ошибка копирования истории: {e}", exc_info=True)

    return copied_count

# ─────────────────────────────────────────────
#  Обработка сообщений из топиков
# ─────────────────────────────────────────────

async def handle_main_group_message(message: Message, bot: Bot) -> None:
    """Обработка сообщений из топиков главной группы."""
    if not message.message_thread_id:
        return

    if any([
        message.forum_topic_created,
        message.forum_topic_closed,
        message.forum_topic_edited,
        message.forum_topic_reopened,
        message.general_forum_topic_hidden,
        message.general_forum_topic_unhidden,
        message.new_chat_members,
        message.left_chat_member,
        message.new_chat_title,
        message.new_chat_photo,
        message.delete_chat_photo,
        message.group_chat_created,
        message.supergroup_chat_created,
        message.channel_chat_created,
        message.migrate_to_chat_id,
        message.migrate_from_chat_id,
        message.pinned_message,
        message.message_auto_delete_timer_changed,
        message.video_chat_scheduled,
        message.video_chat_started,
        message.video_chat_ended,
        message.video_chat_participants_invited,
    ]):
        logger.debug("⏭ Пропускаем системное сообщение в главной группе")
        return

    if message.forum_topic_created or message.forum_topic_closed or message.forum_topic_edited:
        return

    if message.text and message.text.startswith("/"):
        return

    if any([
        message.new_chat_members,
        message.left_chat_member,
        message.new_chat_title,
        message.new_chat_photo,
        message.delete_chat_photo,
        message.group_chat_created,
        message.supergroup_chat_created,
        message.channel_chat_created,
        message.migrate_to_chat_id,
        message.migrate_from_chat_id,
        message.pinned_message,
    ]):
        return

    async with db_manager.session() as db:
        ticket = await _get_ticket_by_thread(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not ticket:
            logger.warning(f"⚠️ Тикет не найден для топика {message.message_thread_id}")
            return

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

        # Пересылаем клиенту
        try:
            await bot.copy_message(
                chat_id=ticket.client_tg_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            logger.info(f"✅ Сообщение переслано клиенту {ticket.client_tg_id}")

            # Сохраняем в БД
            from app.db.crud.message import TicketMessageCRUD

            await TicketMessageCRUD.add_message(
                session=db,
                ticket_id=ticket.id,
                user_id=message.from_user.id,
                message_text=message_text,
                is_from_admin=True,
                media_type=media_type,
                media_file_id=media_file_id,
                media_caption=media_caption,
                telegram_message_id=message.message_id,
            )

        except TelegramBadRequest as e:
            if "can't be copied" in str(e).lower():
                logger.warning(f"⚠️ Сообщение {message.message_id} нельзя скопировать")
            else:
                logger.error(f"❌ Не удалось переслать сообщение клиенту: {e}")
        except Exception as e:
            logger.error(f"❌ Не удалось переслать сообщение клиенту: {e}")

        # Зеркалирование в группу техника
        if ticket.assigned_tech_id:
            tech_thread = await _get_tech_thread(db, ticket.id, ticket.assigned_tech_id)

            if tech_thread:
                try:
                    await bot.copy_message(
                        chat_id=tech_thread.tech_chat_id,
                        message_id=message.message_id,
                        from_chat_id=message.chat.id,
                        message_thread_id=tech_thread.tech_thread_id
                    )
                    logger.info("✅ Сообщение зеркалировано в группу техника")
                except TelegramBadRequest as e:
                    if "can't be copied" in str(e).lower():
                        logger.warning(f"⚠️ Сообщение {message.message_id} нельзя скопировать")
                    else:
                        logger.error(f"❌ Не удалось зеркалировать: {e}")
                except Exception as e:
                    logger.error(f"❌ Не удалось зеркалировать: {e}")


# ─────────────────────────────────────────────
#  Команда /tech
# ─────────────────────────────────────────────

async def cmd_tech(message: Message, bot: Bot) -> None:
    """Команда /tech - показать клавиатуру для выбора техника."""
    if not settings.is_admin(message.from_user.id):
        return

    if not message.message_thread_id:
        return

    if message.chat.id != settings.main_group_id:
        return

    async with db_manager.session() as db:
        ticket = await _get_ticket_by_thread(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not ticket:
            await message.reply("❌ Тикет не найден для этого топика.")
            return

        technicians = await get_technicians(session=db, active_only=True)

        if not technicians:
            await message.reply("❌ Нет доступных техников.")
            return

    builder = InlineKeyboardBuilder()
    for tech in technicians:
        text = tech.name

        if ticket.assigned_tech_id == tech.id:
            text = f"✅ {tech.name}"

        if not tech.group_chat_id:
            text = f"⚠️ {tech.name} (нет группы)"

        builder.button(
            text=text,
            callback_data=f"assign_tech:{ticket.id}:{tech.id}"
        )

    builder.adjust(2)

    await message.reply(
        "👥 <b>Выберите техника для этого тикета:</b>",
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


# ─────────────────────────────────────────────
#  Callback: назначение техника
# ─────────────────────────────────────────────

async def callback_assign_tech(call: CallbackQuery, bot: Bot) -> None:
    """Обработка назначения техника на тикет (с переназначением зеркалирования и переименованием топиков)."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, ticket_id_str, tech_id_str = call.data.split(":", maxsplit=2)
        ticket_id = int(ticket_id_str)
        tech_id = int(tech_id_str)
    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка парсинга callback_data: {e}")
        await call.answer("❌ Некорректные данные.", show_alert=True)
        return

    async with db_manager.session() as db:
        try:
            from sqlalchemy.orm import selectinload
            from app.db.crud.ticket import get_tech_thread_by_user_and_tech, get_all_tech_threads_for_ticket

            # Загружаем тикет с клиентом и текущим техником
            stmt = (
                select(Ticket)
                .options(
                    selectinload(Ticket.client),
                    selectinload(Ticket.assigned_tech),
                )
                .where(Ticket.id == ticket_id)
            )
            result = await db.execute(stmt)
            ticket = result.scalar_one_or_none()

            if not ticket:
                await call.answer("❌ Тикет не найден.", show_alert=True)
                return

            tech = await get_technician_by_id(session=db, tech_id=tech_id)
            if not tech:
                await call.answer("❌ Техник не найден.", show_alert=True)
                return

            if not tech.group_chat_id:
                await call.answer(
                    f"⚠️ У техника {tech.name} нет привязанной группы.\n"
                    f"Используйте /join в группе техника.",
                    show_alert=True,
                )
                return

            # Если уже этот техник — ничего не делаем
            if ticket.assigned_tech_id == tech_id:
                await call.answer("✅ Этот техник уже назначен", show_alert=False)
                return

            if not ticket.client:
                logger.error(f"❌ У тикета {ticket.id} нет связанного клиента")
                await call.answer("❌ Ошибка: клиент не найден.", show_alert=True)
                return

            client_name = (
                ticket.client.first_name
                or ticket.client.username
                or f"User{ticket.client.tg_id}"
            )
            client_username = ticket.client.username
            tag = _extract_consonants(tech.name)

            # Название для топика техника (без тега)
            tech_topic_name = _build_topic_title(
                status=ticket.status,
                client_name=client_name,
                client_username=client_username,
                tech_tag=None,
            )

            # 1) Отключаем зеркалирование от прежнего техника (если был)
            if ticket.assigned_tech_id and ticket.assigned_tech_id != tech_id:
                old_thread = await _get_tech_thread(
                    db,
                    ticket.id,
                    ticket.assigned_tech_id,
                )
                if old_thread:
                    # закрываем старый тех-топик и удаляем запись
                    await _close_tech_topic(
                        bot,
                        old_thread.tech_chat_id,
                        old_thread.tech_thread_id,
                    )
                    await db.delete(old_thread)
                    logger.info(
                        f"🗑 Удален TechThread для старого техника {ticket.assigned_tech_id} "
                        f"(тикет #{ticket.id})"
                    )

            # 2) Ищем/создаём тех-топик для НОВОГО техника
            #   — логика: один тех-топик на связку (клиент, техник),
            #     при новом тикете — просто "перепривязываем" его.
            existing_thread = await get_tech_thread_by_user_and_tech(
                session=db,
                user_id=ticket.client_tg_id,
                tech_id=tech_id,
            )

            tech_thread_id: int | None = None

            if existing_thread:
                # Переиспользуем существующий топик этого техника для этого клиента
                tech_thread_id = existing_thread.tech_thread_id

                existing_thread.ticket_id = ticket.id
                await db.flush()

                # Переоткрываем топик на всякий случай
                await _reopen_tech_topic(
                    bot,
                    existing_thread.tech_chat_id,
                    tech_thread_id,
                )

                # Обновляем его название под текущий тикет
                try:
                    await bot.edit_forum_topic(
                        chat_id=existing_thread.tech_chat_id,
                        message_thread_id=tech_thread_id,
                        name=tech_topic_name,
                    )
                    logger.info(
                        f"✅ Название существующего топика техника обновлено: {tech_topic_name}"
                    )
                except TelegramBadRequest as e:
                    logger.warning(f"⚠️ Не удалось обновить название тех-топика: {e}")

                logger.info(
                    f"♻️ Переиспользован тех-топик {tech_thread_id} "
                    f"для клиента {ticket.client_tg_id} и техника {tech.id}"
                )
            else:
                # Создаём новый тех-топик
                tech_thread_id = await _create_tech_topic(
                    bot,
                    tech,
                    tech_topic_name,
                )
                if not tech_thread_id:
                    await call.answer(
                        "❌ Не удалось создать топик в группе техника.",
                        show_alert=True,
                    )
                    return

                tech_thread = TechThread(
                    ticket_id=ticket.id,
                    user_id=ticket.client_tg_id,
                    tech_id=tech.id,
                    tech_chat_id=tech.group_chat_id,
                    tech_thread_id=tech_thread_id,
                )
                db.add(tech_thread)
                await db.flush()

                # Копируем историю тикета в новый тех-топик
                try:
                    copied = await _copy_ticket_history_to_tech(
                        bot=bot,
                        ticket=ticket,
                        tech_chat_id=tech.group_chat_id,
                        tech_thread_id=tech_thread_id,
                        db=db,
                    )
                    logger.info(
                        f"📋 Скопировано {copied} сообщений в новый топик техника "
                        f"(тикет #{ticket.id}, техник {tech.id})"
                    )
                except Exception as e:
                    logger.error(f"❌ Ошибка копирования истории: {e}")

                # Отправляем и закрепляем кнопки статуса в тех-топике
                try:
                    status_msg = await bot.send_message(
                        chat_id=tech.group_chat_id,
                        message_thread_id=tech_thread_id,
                        text="🎛 <b>Управление статусом:</b>",
                        reply_markup=_get_status_control_keyboard(ticket.id),
                        parse_mode="HTML",
                    )
                    await _pin_message_in_topic(
                        bot,
                        tech.group_chat_id,
                        tech_thread_id,
                        status_msg.message_id,
                    )
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки кнопок статуса в тех-топик: {e}")

            # 3) Обновляем назначенного техника у тикета
            ticket.assigned_tech_id = tech.id
            await db.commit()

            logger.info(
                f"✅ Техник {tech.name} (ID={tech.id}) назначен на тикет #{ticket.id}"
            )

            # 4) Обновляем название ВСЕХ топиков (главный + все тех-топики тикета)
            await _update_all_topic_titles(bot, ticket, db)

            # 5) Скрываем клавиатуру выбора техника
            try:
                await call.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest as e:
                logger.warning(f"⚠️ Не удалось скрыть клавиатуру /tech: {e}")

        except Exception as e:
            logger.error(f"❌ Ошибка при назначении техника: {e}", exc_info=True)
            await db.rollback()
            await call.answer(
                "❌ Произошла ошибка при назначении техника.", show_alert=True
            )
            return

    # Уведомление в чат
    try:
        await call.message.answer(
            f"✅ <b>Техник {tech.name} назначен</b>\n\n"
            f"📁 Топик: #{tech_thread_id}\n"
            f"🏷 Тег: [{tag}]",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Не удалось отправить подтверждение в группу: {e}")

    try:
        await call.answer(f"✅ {tech.name} назначен")
    except Exception:
        pass


# ─────────────────────────────────────────────
#  Callback: изменение статуса
# ─────────────────────────────────────────────

async def callback_change_status(call: CallbackQuery, bot: Bot) -> None:
    """Обработка изменения статуса через кнопки."""

    # 🔹 Проверка прав (админы или техники могут менять статус)
    is_admin = settings.is_admin(call.from_user.id)

    try:
        action, ticket_id_str = call.data.split(":", maxsplit=1)
        ticket_id = int(ticket_id_str)

        # Определяем новый статус
        new_status_map = {
            "status_new": TicketStatus.NEW,
            "status_work": TicketStatus.WORK,
            "status_close": TicketStatus.CLOSED,
        }

        new_status = new_status_map.get(action)

        if not new_status:
            await call.answer("❌ Неизвестный статус", show_alert=True)
            return

    except (ValueError, IndexError) as e:
        logger.error(f"Ошибка парсинга callback_data: {e}")
        await call.answer("❌ Некорректные данные.", show_alert=True)
        return

    async with db_manager.session() as db:
        try:
            from sqlalchemy.orm import selectinload

            # 🔹 Загружаем тикет со ВСЕМИ необходимыми relationships
            stmt = (
                select(Ticket)
                .options(
                    selectinload(Ticket.client),
                    selectinload(Ticket.assigned_tech)
                )
                .where(Ticket.id == ticket_id)
            )
            result = await db.execute(stmt)
            ticket = result.scalar_one_or_none()

            if not ticket:
                await call.answer("❌ Тикет не найден.", show_alert=True)
                return

            # 🔹 Определяем, является ли пользователь техником
            current_tech = None
            if not is_admin:
                # Ищем техника по tg_user_id
                all_techs = await get_technicians(session=db, active_only=True)
                for t in all_techs:
                    if t.tg_user_id == call.from_user.id:
                        current_tech = t
                        break

                if not current_tech:
                    await call.answer("⛔ Нет прав на изменение статуса", show_alert=True)
                    return

                # Проверяем, назначен ли этот техник на тикет
                if ticket.assigned_tech_id != current_tech.id:
                    await call.answer("⛔ Вы не назначены на этот тикет", show_alert=True)
                    return

            if ticket.status == new_status:
                status_names = {
                    TicketStatus.NEW: "Новый",
                    TicketStatus.WORK: "В работе",
                    TicketStatus.CLOSED: "Закрыт",
                }
                await call.answer(f"✅ Уже в статусе '{status_names[new_status]}'")
                return

            old_status = ticket.status

            # Если техник меняет статус НА "В работе" и еще не назначен - назначаем его
            if current_tech and new_status == TicketStatus.WORK and not ticket.assigned_tech_id:
                ticket.assigned_tech_id = current_tech.id
                logger.info(
                    f"✅ Техник {current_tech.name} автоматически назначен на тикет #{ticket.id} "
                    f"при переводе в статус WORK"
                )

            # Обновляем статус
            ticket.status = new_status
            await db.commit()

            logger.info(
                f"📊 Тикет #{ticket.id} переведен из {old_status.value} "
                f"в статус {new_status.value} пользователем {call.from_user.id}"
            )

            # Перезагружаем тикет с relationships после коммита
            await db.refresh(ticket)
            stmt = (
                select(Ticket)
                .options(
                    selectinload(Ticket.client),
                    selectinload(Ticket.assigned_tech)
                )
                .where(Ticket.id == ticket_id)
            )
            result = await db.execute(stmt)
            ticket = result.scalar_one_or_none()

            if not ticket:
                logger.error("❌ Не удалось перезагрузить тикет после коммита")
                await call.answer("❌ Произошла ошибка.", show_alert=True)
                return

            # 🟢 Если переводим из CLOSED в NEW/WORK — надо переоткрыть топики
            if new_status in (TicketStatus.NEW, TicketStatus.WORK):
                # 1) Главная группа
                if ticket.main_chat_id and ticket.main_thread_id:
                    try:
                        await bot.reopen_forum_topic(
                            chat_id=ticket.main_chat_id,
                            message_thread_id=ticket.main_thread_id,
                        )
                        logger.info(
                            f"✅ Переоткрыт топик {ticket.main_thread_id} "
                            f"в главной группе {ticket.main_chat_id}"
                        )
                    except TelegramBadRequest as e:
                        logger.debug(
                            f"ℹ️ Не удалось переоткрыть главный топик "
                            f"{ticket.main_thread_id}: {e}"
                        )

                # 2) Все тех-топики этого тикета
                tech_threads = await get_all_tech_threads_for_ticket(
                    session=db,
                    ticket_id=ticket.id,
                )

                for tech_thread in tech_threads:
                    try:
                        await _reopen_tech_topic(
                            bot,
                            tech_thread.tech_chat_id,
                            tech_thread.tech_thread_id,
                        )
                    except Exception as e:
                        logger.error(
                            f"❌ Ошибка переоткрытия тех-топика "
                            f"{tech_thread.tech_thread_id} в группе {tech_thread.tech_chat_id}: {e}"
                        )

            # Логируем перед обновлением названий
            logger.info(f"🔄 Обновление названий топиков для тикета #{ticket.id}")
            logger.info(f"   Главная группа: {ticket.main_chat_id}/{ticket.main_thread_id}")

            if ticket.assigned_tech_id:
                tech_name = current_tech.name if current_tech else "?"
                logger.info(f"   Назначен техник: #{ticket.assigned_tech_id} ({tech_name})")
            else:
                logger.info("   Техник не назначен")

            # Обновляем названия топиков
            await _update_all_topic_titles(bot, ticket, db)

            # Если закрываем - закрываем топики и отправляем опрос
            if new_status == TicketStatus.CLOSED:
                # Отправляем опрос клиенту
                try:
                    from app.bot.handlers.user_poll import start_feedback_poll

                    await start_feedback_poll(
                        bot=bot,
                        user_id=ticket.client_tg_id,
                        ticket_id=ticket.id,
                        tech_id=ticket.assigned_tech_id
                    )
                    logger.info(f"✅ Опрос отправлен клиенту {ticket.client_tg_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка отправки опроса: {e}")

                # Закрываем топик в главной группе
                try:
                    await bot.close_forum_topic(
                        chat_id=ticket.main_chat_id,
                        message_thread_id=ticket.main_thread_id
                    )
                    logger.info(f"✅ Закрыт топик в главной группе {ticket.main_thread_id}")
                except Exception as e:
                    logger.error(f"❌ Ошибка закрытия главного топика: {e}")

                # 🔹 Закрываем ВСЕ топики техников для этого клиента

                tech_threads = await get_all_tech_threads_for_ticket(
                    session=db,
                    ticket_id=ticket.id
                )

                for tech_thread in tech_threads:
                    try:
                        await _close_tech_topic(
                            bot,
                            tech_thread.tech_chat_id,
                            tech_thread.tech_thread_id
                        )
                        logger.info(
                            f"✅ Закрыт топик техника {tech_thread.tech_thread_id} "
                            f"в группе {tech_thread.tech_chat_id}"
                        )
                    except Exception as e:
                        logger.error(f"❌ Ошибка закрытия топика техника: {e}")

            status_emoji_map = {
                TicketStatus.NEW: "🟢",
                TicketStatus.WORK: "🟡",
                TicketStatus.CLOSED: "⚪️",
            }

            await call.answer(f"{status_emoji_map[new_status]} Статус обновлен")

        except Exception as e:
            logger.error(f"❌ Ошибка изменения статуса: {e}", exc_info=True)
            await db.rollback()
            await call.answer("❌ Произошла ошибка.", show_alert=True)

# ─────────────────────────────────────────────
#  Регистрация обработчиков
# ─────────────────────────────────────────────

def register_handlers(dp: Dispatcher) -> None:
    """Регистрация обработчиков для главной группы."""
    logger.info("🔧 === НАЧАЛО регистрации обработчиков main_group.py ===")

    # Команда /tech
    dp.message.register(
        cmd_tech,
        Command("tech"),
        F.chat.id == settings.main_group_id,
    )

    # Обработка сообщений
    dp.message.register(
        handle_main_group_message,
        F.chat.id == settings.main_group_id,
        F.message_thread_id,
    )



    # Callbacks
    dp.callback_query.register(
        callback_assign_tech,
        F.data.startswith("assign_tech:"),
    )

    dp.callback_query.register(
        callback_change_status,
        F.data.startswith("status_"),
    )

    logger.info("✅ Зарегистрированы обработчики для главной группы")
    logger.info("🔧 === КОНЕЦ регистрации обработчиков main_group.py ===")