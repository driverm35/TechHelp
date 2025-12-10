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
from app.db.crud.user import get_or_create_user
from app.utils.redis_streams import redis_streams

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Вспомогательные функции
# ─────────────────────────────────────────────

async def _pin_message_in_topic(
    bot: Bot,
    chat_id: int,
    message_id: int,
) -> bool:
    """
    Закрепить сообщение в чате/топике.

    В Telegram форумы используют общий метод pin_chat_message,
    но сообщение закрепляется и в конкретном топике.
    """
    try:
        await bot.pin_chat_message(
            chat_id=chat_id,
            message_id=message_id,
            disable_notification=True,
        )
        logger.info(f"📌 Закреплено сообщение {message_id} в чате {chat_id}")
        return True
    except TelegramBadRequest as e:
        logger.warning(f"⚠️ Не удалось закрепить сообщение: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Ошибка закрепления сообщения: {e}")
        return False


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
    """Обработка сообщений из топиков группы техника."""
    
    if not message.message_thread_id:
        return

    # Игнорируем служебные
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
        if message.text.lower().startswith((
            "/s", "/i", "feed", "/f",
            "/work", "/done"
        )):
            return
        return

    async with db_manager.session() as db:
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

        await get_or_create_user(
            db=db,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

        # ✅ ИСПРАВЛЕНО: 1. Пересылаем в главную группу через Redis
        try:
            main_payload = {
                "bot_token": bot.token,
                "type": "text" if not media_type else media_type,
                "target_chat_id": ticket.main_chat_id,
                "target_thread_id": ticket.main_thread_id,
                "ticket_id": ticket.id,
            }
            
            if media_type:
                main_payload["file_id"] = media_file_id
                if media_caption:
                    main_payload["caption"] = media_caption
            else:
                main_payload["text"] = message_text
            
            await redis_streams.enqueue(main_payload)
            logger.info(
                f"✅ Сообщение техника добавлено в очередь для главной группы "
                f"(топик {ticket.main_thread_id})"
            )
            
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

        except Exception as e:
            logger.error(f"❌ Ошибка пересылки в главную группу: {e}")

        # ✅ ИСПРАВЛЕНО: 2. Пересылаем клиенту через Redis
        try:
            client_payload = {
                "bot_token": bot.token,
                "type": "text" if not media_type else media_type,
                "target_chat_id": ticket.client_tg_id,
                "ticket_id": ticket.id,
            }
            
            if media_type:
                client_payload["file_id"] = media_file_id
                if media_caption:
                    client_payload["caption"] = media_caption
            else:
                client_payload["text"] = message_text
            
            await redis_streams.enqueue(client_payload)
            logger.info(
                f"✅ Сообщение техника добавлено в очередь для клиента {ticket.client_tg_id}"
            )
        except Exception as e:
            logger.error(f"❌ Ошибка пересылки клиенту: {e}")


# ─────────────────────────────────────────────
#  Служебные команды (не пересылаются клиенту)
# ─────────────────────────────────────────────

async def cmd_staff(message: Message, bot: Bot) -> None:
    """
    Команда /s - служебная заметка.

    ✅ Отправляется:
      • в ТЕКУЩИЙ тех-топик (и закрепляется там)
      • в главный топик тикета (и тоже закрепляется)

    НЕ отправляется клиенту.
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

    staff_text = parts[1].strip()
    if not staff_text:
        await message.reply(
            "💼 Используйте: <code>/s текст заметки</code>",
            parse_mode="HTML"
        )
        return

    async with db_manager.session() as db:
        # Находим TechThread по текущему тех-топику
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
            await message.reply("❌ Тикет не найден")
            return

        sender_name = (
            message.from_user.first_name
            or message.from_user.username
            or "Специалист"
        )

        # Единый текст заметки (будет одинаковым везде и в БД)
        formatted_text = f"💼 <b>{sender_name}:</b> {staff_text}"

        # Убедимся, что техник есть в users
        from app.db.crud.user import get_or_create_user  # уже есть в модуле, но на всякий
        await get_or_create_user(
            db=db,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

        # 1) Отправляем заметку в ТЕКУЩИЙ тех-топик и закрепляем
        try:
            tech_msg = await bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=message.message_thread_id,
                text=formatted_text,
                parse_mode="HTML"
            )
            await _pin_message_in_topic(
                bot=bot,
                chat_id=message.chat.id,
                message_id=tech_msg.message_id,
            )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки заметки в тех-группу: {e}")

        # 2) Отправляем заметку в ГЛАВНУЮ группу (топик тикета) и закрепляем
        try:
            main_msg = await bot.send_message(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id,
                text=formatted_text,
                parse_mode="HTML"
            )
            await _pin_message_in_topic(
                bot=bot,
                chat_id=ticket.main_chat_id,
                message_id=main_msg.message_id,
            )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки заметки в главную группу: {e}")

        # 3) Логируем заметку в историю тикета, чтобы можно было восстановить
        try:
            from app.db.crud.message import TicketMessageCRUD  # :contentReference[oaicite:0]{index=0}

            await TicketMessageCRUD.add_message(
                session=db,
                ticket_id=ticket.id,
                user_id=message.from_user.id,
                message_text=formatted_text,
                is_from_admin=True,
                telegram_message_id=None,  # можно не привязывать
            )
        except Exception as e:
            logger.error(f"❌ Не удалось сохранить служебную заметку в БД: {e}")

        await db.commit()

    # Не отвечаем в топик
    return


async def cmd_feedback(message: Message, bot: Bot) -> None:
    """
    /feed, /f — вручную отправить клиенту опрос по тикету.

    Ограничения:
      ✔ работает только в тех-топике
      ✔ тикет должен быть в статусе CLOSED
      ✔ опрос не отправляется повторно
    """

    # 1. Команда возможна только в топике
    if not message.message_thread_id:
        return

    # 2. Проверяем, что текст — команда
    if not message.text or not message.text.lower().startswith(("/feed", "/f")):
        return

    async with db_manager.session() as db:

        # 3. Ищем тех-топик
        tech_thread = await _get_tech_thread_by_location(
            db,
            message.chat.id,
            message.message_thread_id
        )

        if not tech_thread:
            await message.reply("❌ Этот топик не связан с тикетом")
            return

        # 4. Получаем тикет с клиентом
        ticket = await _get_ticket_with_client(db, tech_thread.ticket_id)

        if not ticket:
            await message.reply("❌ Тикет не найден", parse_mode="HTML")
            return

        if not ticket.client:
            await message.reply("❌ У тикета нет клиента", parse_mode="HTML")
            return

        # 5. Проверка: тикет должен быть закрыт
        if ticket.status != TicketStatus.CLOSED:
            await message.reply(
                "⚠️ Опрос можно отправить только для <b>закрытого</b> тикета.",
                parse_mode="HTML"
            )
            return

        # 6. Проверка на повторную отправку опроса
        #    Чтобы не спамить клиенту
        feedback_key = f"feedback_sent:{ticket.id}"
        from app.utils.cache import cache

        already = await cache.get(feedback_key)
        if already:
            await message.reply(
                "ℹ️ Опрос уже был отправлен ранее.",
                parse_mode="HTML"
            )
            return

        # 7. Отправляем опрос
        try:
            await _send_feedback_poll(
                bot=bot,
                ticket_id=ticket.id,
                client_tg_id=ticket.client_tg_id,
                tech_id=ticket.assigned_tech_id
            )

            # Запоминаем факт отправки (TTL = 7 дней)
            await cache.set(feedback_key, True, ttl=7*24*3600)

            await message.reply("📨 Опрос отправлен клиенту.", parse_mode="HTML")

        except Exception as e:
            logger.error(f"❌ Ошибка отправки опроса вручную: {e}")
            await message.reply(
                "❌ Ошибка при отправке опроса.",
                parse_mode="HTML"
            )

    # Не отвечаем в tech-topic
    return


async def cmd_internal(message: Message, bot: Bot) -> None:
    """
    Команда /i - внутренняя заметка.

    ✅ Видна только в группе техника:
      • отправляем сообщение в текущий тех-топик
      • закрепляем его
      • пишем в БД, чтобы при смене техника можно было восстановить
    """
    if not message.message_thread_id:
        return

    if not message.text:
        return

    parts = message.text.split(maxsplit=1)
    if len(parts) < 2:
        await message.reply(
            "📝 Используйте: <code>/i текст заметки</code>",
            parse_mode="HTML"
        )
        return

    internal_text = parts[1].strip()
    if not internal_text:
        await message.reply(
            "📝 Используйте: <code>/i текст заметки</code>",
            parse_mode="HTML"
        )
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
            await message.reply("❌ Тикет не найден")
            return

        sender_name = (
            message.from_user.first_name
            or message.from_user.username
            or "Специалист"
        )

        # Отдельный формат для внутренних заметок
        # Важно: текст попадет в БД именно в таком виде
        formatted_text = f"📝 <b>Внутренняя заметка ({sender_name}):</b> {internal_text}"

        # Убедимся, что техник есть в users
        from app.db.crud.user import get_or_create_user
        await get_or_create_user(
            db=db,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

        # 1) Отправляем заметку в ТЕКУЩИЙ тех-топик
        try:
            internal_msg = await bot.send_message(
                chat_id=message.chat.id,
                message_thread_id=message.message_thread_id,
                text=formatted_text,
                parse_mode="HTML"
            )
            # 2) Закрепляем её
            await _pin_message_in_topic(
                bot=bot,
                chat_id=message.chat.id,
                message_id=internal_msg.message_id,
            )
        except Exception as e:
            logger.error(f"❌ Ошибка отправки внутренней заметки: {e}")

        # 3) Сохраняем как сообщение тикета (но оно нигде, кроме тех-групп, не показывается)
        try:
            from app.db.crud.message import TicketMessageCRUD

            await TicketMessageCRUD.add_message(
                session=db,
                ticket_id=ticket.id,
                user_id=message.from_user.id,
                message_text=formatted_text,
                is_from_admin=True,
                telegram_message_id=None,
            )
        except Exception as e:
            logger.error(f"❌ Не удалось сохранить внутреннюю заметку в БД: {e}")

        await db.commit()

    # Не отвечаем в топик
    return

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
    # Команда отправки опроса
    dp.message.register(
        cmd_feedback,
        Command("feed", "f"),
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