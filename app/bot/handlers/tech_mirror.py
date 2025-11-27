# app/bot/handlers/tech_mirror.py
from __future__ import annotations
import logging

from aiogram import Dispatcher, F, Bot
from aiogram.enums import ChatType
from aiogram.filters import Command
from aiogram.types import Message, InlineQuery, InlineQueryResultArticle, InputTextMessageContent
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from aiogram.exceptions import TelegramBadRequest
from sqlalchemy.orm import selectinload

from app.bot.handlers.main_group import _update_all_topic_titles

from app.config import settings
from app.db.database import db_manager
from app.db.models import TechThread, Ticket, TicketStatus


logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Вспомогательные функции
# ─────────────────────────────────────────────

def _status_emoji(status: TicketStatus) -> str:
    """Получить эмодзи статуса."""
    return {
        TicketStatus.NEW: "🟢",
        TicketStatus.WORK: "🟡",
        TicketStatus.CLOSED: "⚪️",
    }.get(status, "⚪️")


async def _get_tech_thread_by_location(
    session: AsyncSession,
    tech_chat_id: int,
    tech_thread_id: int
) -> TechThread | None:
    """Получить TechThread по местоположению в группе техника."""
    stmt = (
        select(TechThread)
        .where(
            TechThread.tech_chat_id == tech_chat_id,
            TechThread.tech_thread_id == tech_thread_id
        )
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _get_ticket_with_client(
    session: AsyncSession,
    ticket_id: int
) -> Ticket | None:
    """Получить тикет с предзагрузкой клиента."""
    from sqlalchemy.orm import selectinload

    stmt = (
        select(Ticket)
        .options(selectinload(Ticket.client))
        .where(Ticket.id == ticket_id)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _send_feedback_poll(bot: Bot, ticket_id: int, client_tg_id: int, tech_id: int | None = None) -> None:
    """
    Инициировать опрос клиента после закрытия тикета.

    Args:
        bot: Экземпляр бота
        ticket_id: ID тикета
        client_tg_id: Telegram ID клиента
        tech_id: ID техника (может быть None)
    """
    try:
        from app.bot.handlers.user_poll import start_feedback_poll

        await start_feedback_poll(
            bot=bot,
            user_id=client_tg_id,
            ticket_id=ticket_id,
            tech_id=tech_id
        )

        logger.info(f"✅ Опрос инициирован для клиента {client_tg_id}, тикет #{ticket_id}")
    except Exception as e:
        logger.error(f"❌ Не удалось инициировать опрос: {e}")


# ─────────────────────────────────────────────
#  Зеркалирование сообщений из группы техника
# ─────────────────────────────────────────────

async def handle_tech_group_message(message: Message, bot: Bot) -> None:
    """
    Обработка сообщений из топиков группы техника.

    Логика:
    1. Найти TechThread по tech_chat_id + tech_thread_id
    2. Переслать в главную группу (топик тикета)
    3. Переслать клиенту
    4. Не пересылать команды
    """
    # Игнорируем сообщения не из топиков
    if not message.message_thread_id:
        return

    # Игнорируем служебные сообщения
    if any([
        message.forum_topic_created,
        message.forum_topic_closed,
        message.forum_topic_edited,
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

    # Проверяем команды
    if message.text and message.text.startswith("/"):
        # Служебные команды - не пересылаем
        if message.text.lower().startswith((
            "/s", "/i",
            "/work", "/done"
        )):
            return
        # Остальные команды тоже не пересылаем
        return

    async with db_manager.session() as db:
        # Находим TechThread
        tech_thread = await _get_tech_thread_by_location(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not tech_thread:
            logger.debug(
                f"TechThread не найден для группы {message.chat.id}, "
                f"топик {message.message_thread_id}"
            )
            return

        # Получаем тикет с клиентом
        ticket = await _get_ticket_with_client(db, tech_thread.ticket_id)

        if not ticket:
            logger.warning(
                f"⚠️ Тикет #{tech_thread.ticket_id} не найден для TechThread"
            )
            return

        if not ticket.client:
            logger.error(
                f"❌ У тикета #{ticket.id} нет связанного клиента"
            )
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

        # 1. Пересылаем в главную группу
        try:
            await bot.copy_message(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            logger.info(
                f"✅ Сообщение техника переслано в главную группу "
                f"(топик {ticket.main_thread_id})"
            )
            # Сохраняем в БД (от техника)
            from app.db.crud.message import TicketMessageCRUD

            await TicketMessageCRUD.add_message(
                session=db,
                ticket_id=ticket.id,
                user_id=message.from_user.id,  # ID техника
                message_text=message_text,
                is_from_admin=True,  # От поддержки/техника
                media_type=media_type,
                media_file_id=media_file_id,
                media_caption=media_caption,
                telegram_message_id=message.message_id,
            )

        except TelegramBadRequest as e:
            if "can't be copied" in str(e).lower():
                logger.warning(
                    f"⚠️ Сообщение {message.message_id} нельзя скопировать "
                    f"(тип: {message.content_type})"
                )
            else:
                logger.error(f"❌ Ошибка пересылки в главную группу: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка пересылки в главную группу: {e}")

        # 2. Пересылаем клиенту
        try:
            await bot.copy_message(
                chat_id=ticket.client_tg_id,
                from_chat_id=message.chat.id,
                message_id=message.message_id
            )
            logger.info(
                f"✅ Сообщение техника переслано клиенту {ticket.client_tg_id}"
            )
        except TelegramBadRequest as e:
            if "can't be copied" in str(e).lower():
                logger.warning(
                    f"⚠️ Сообщение {message.message_id} нельзя скопировать клиенту "
                    f"(тип: {message.content_type})"
                )
            else:
                logger.error(f"❌ Ошибка пересылки клиенту: {e}")
        except Exception as e:
            logger.error(f"❌ Ошибка пересылки клиенту: {e}")


# ─────────────────────────────────────────────
#  Служебные команды (не пересылаются клиенту)
# ─────────────────────────────────────────────

async def cmd_staff(message: Message, bot: Bot) -> None:
    """
    Команда /s - служебная заметка.

    Отправляется только в главную группу, НЕ клиенту.
    Использование: /s <текст>
    """
    if not message.message_thread_id:
        return

    if not message.text:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "💼 Используйте: <code>/s текст заметки</code>",
            parse_mode="HTML"
        )
        return

    staff_text = parts[1]

    async with db_manager.session() as db:
        tech_thread = await _get_tech_thread_by_location(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not tech_thread:
            return

        ticket = await _get_ticket_with_client(db, tech_thread.ticket_id)

        if not ticket:
            return

        sender_name = (
            message.from_user.first_name
            or message.from_user.username
            or "Специалист"
        )

        formatted_text = (
            f"💼 <b>{sender_name}:</b> {staff_text}"
        )

        try:
            await bot.send_message(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id,
                text=formatted_text,
                parse_mode="HTML"
            )
            logger.info(f"✅ Служебная заметка от {sender_name} отправлена")
            await message.reply("✅")
        except Exception as e:
            logger.error(f"❌ Ошибка отправки заметки: {e}")
            await message.reply("❌")


async def cmd_internal(message: Message) -> None:
    """
    Команда /i - внутренняя заметка.

    Видна только в группе техника.
    """
    if not message.message_thread_id:
        return

    await message.reply("📝")


# ─────────────────────────────────────────────
#  Команды изменения статуса
# ─────────────────────────────────────────────

async def cmd_work(message: Message, bot: Bot) -> None:
    """
    Команда /work - перевести тикет в работу.

    Меняет статус на WORK (🟡) во всех топиках.
    """
    if not message.message_thread_id:
        return

    async with db_manager.session() as db:
        tech_thread = await _get_tech_thread_by_location(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not tech_thread:
            await message.reply("❌ Топик не связан с тикетом")
            return

        ticket = await _get_ticket_with_client(db, tech_thread.ticket_id)

        if not ticket:
            return

        if ticket.status == TicketStatus.WORK:
            await message.reply("✅ Уже в работе")
            return

        # Обновляем статус
        ticket.status = TicketStatus.WORK
        await db.commit()

        logger.info(f"🟡 Тикет #{ticket.id} переведен в работу")

        # Обновляем названия топиков (главный + все тех-топики)
        # Перезагружаем тикет с нужными связями
        stmt = (
            select(Ticket)
            .options(
                selectinload(Ticket.client),
                selectinload(Ticket.assigned_tech),
            )
            .where(Ticket.id == ticket.id)
        )
        result = await db.execute(stmt)
        ticket_reloaded = result.scalar_one_or_none()

        if ticket_reloaded:
            await _update_all_topic_titles(bot, ticket_reloaded, db)

        await message.reply("🟡 В работе")


async def cmd_done(message: Message, bot: Bot) -> None:
    """
    Команда /done - закрыть тикет.

    Меняет статус на CLOSED (⚪️) и отправляет опрос клиенту.
    """
    if not message.message_thread_id:
        return

    async with db_manager.session() as db:
        tech_thread = await _get_tech_thread_by_location(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not tech_thread:
            await message.reply("❌ Топик не связан с тикетом")
            return

        ticket = await _get_ticket_with_client(db, tech_thread.ticket_id)

        if not ticket:
            return

        if ticket.status == TicketStatus.CLOSED:
            await message.reply("✅ Уже закрыт")
            return

        # Обновляем статус
        ticket.status = TicketStatus.CLOSED
        await db.commit()

        logger.info(f"⚪️ Тикет #{ticket.id} закрыт")

        # 🔹 Отправляем опрос клиенту (с tech_id)
        await _send_feedback_poll(
            bot=bot,
            ticket_id=ticket.id,
            client_tg_id=ticket.client_tg_id,
            tech_id=ticket.assigned_tech_id
        )

        # Обновляем эмодзи в топиках
        # Перезагружаем тикет с нужными связями
        stmt = (
            select(Ticket)
            .options(
                selectinload(Ticket.client),
                selectinload(Ticket.assigned_tech)
            )
            .where(Ticket.id == ticket.id)
        )
        result = await db.execute(stmt)
        ticket_reloaded = result.scalar_one_or_none()

        if ticket_reloaded:
            await _update_all_topic_titles(bot, ticket_reloaded, db)

        # Закрываем топики
        try:
            await bot.close_forum_topic(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id
            )

            await bot.close_forum_topic(
                chat_id=message.chat.id,
                message_thread_id=message.message_thread_id
            )

            await message.reply("⚪️ Закрыт")
        except Exception as e:
            logger.error(f"❌ Ошибка закрытия топиков: {e}")
            await message.reply("⚪️ Закрыт")


# ─────────────────────────────────────────────
#  Inline режим
# ─────────────────────────────────────────────

async def inline_query_handler(inline_query: InlineQuery) -> None:
    """
    Обработка inline запросов для подсказок по командам.
    """
    results = [
        InlineQueryResultArticle(
            id="staff",
            title="💼 /s - Служебная заметка",
            description="Отправить заметку только в главную группу (не клиенту)",
            input_message_content=InputTextMessageContent(
                message_text="/s "
            )
        ),
        InlineQueryResultArticle(
            id="internal",
            title="📝 /i - Внутренняя заметка",
            description="Заметка только для вашей группы",
            input_message_content=InputTextMessageContent(
                message_text="/i "
            )
        ),
        InlineQueryResultArticle(
            id="work",
            title="🟡 /work - В работу",
            description="Перевести тикет в статус 'В работе'",
            input_message_content=InputTextMessageContent(
                message_text="/work"
            )
        ),
        InlineQueryResultArticle(
            id="done",
            title="⚪️ /done - Закрыть",
            description="Закрыть тикет и отправить опрос клиенту",
            input_message_content=InputTextMessageContent(
                message_text="/done"
            )
        ),
    ]

    # Фильтруем результаты по запросу
    query = inline_query.query.lower()
    if query:
        results = [
            r for r in results
            if query in r.title.lower() or query in r.description.lower()
        ]

    await inline_query.answer(
        results,
        cache_time=1,
        is_personal=True
    )


# ─────────────────────────────────────────────
#  Регистрация обработчиков
# ─────────────────────────────────────────────

def register_handlers(dp: Dispatcher) -> None:
    """Регистрация обработчиков для зеркалирования из групп техников."""
    logger.info("🔧 === НАЧАЛО регистрации обработчиков tech_mirror.py ===")

    # Inline режим
    dp.inline_query.register(inline_query_handler)

    # Служебные команды (обрабатываются первыми)
    dp.message.register(
        cmd_staff,
        Command("staff", "s", "lead", "l"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
        F.message_thread_id,
    )

    dp.message.register(
        cmd_internal,
        Command("internal", "i"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
        F.message_thread_id,
    )

    # Команды статусов
    dp.message.register(
        cmd_work,
        Command("work"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
        F.message_thread_id,
    )

    dp.message.register(
        cmd_done,
        Command("done"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
        F.message_thread_id,
    )

    # Зеркалирование обычных сообщений
    # Важно: НЕ из главной группы
    dp.message.register(
        handle_tech_group_message,
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
        F.chat.id != settings.main_group_id,
        F.message_thread_id,
        F.text | F.photo | F.video | F.document | F.voice,
    )

    logger.info("✅ Зарегистрированы обработчики для зеркалирования из групп техников")
    logger.info("🔧 === КОНЕЦ регистрации обработчиков tech_mirror.py ===")