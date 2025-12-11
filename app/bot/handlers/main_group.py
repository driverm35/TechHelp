# app/bot/handlers/main_group.py

from __future__ import annotations
import asyncio
import logging

from aiogram import Dispatcher, F, Bot
from aiogram.enums import ChatType
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.exceptions import (
    TelegramRetryAfter,
    TelegramBadRequest,
)
from aiogram.utils.keyboard import InlineKeyboardBuilder
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload

from app.config import settings
from app.db.database import db_manager
from app.db.models import Ticket, TechThread, TicketStatus, Technician, User
from app.db.crud.ticket import (
    get_all_tech_threads_for_ticket
    
)
from app.db.crud.tech import (
    get_technicians,
    get_technician_by_id,
    find_existing_tech_topic_for_client
)
from app.db.crud.user import get_or_create_user
from app.utils.cache import cache
from app.utils.redis_streams import redis_streams

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


def _get_tech_tag(tech: Technician | None) -> str:
    """
    Получить тег техника из согласных букв его имени.
    
    Args:
        tech: Объект техника или None
        
    Returns:
        Тег техника (например, "ПВЛ") или "???" если техник None
    """
    if tech is None:
        return "???"
    
    return _extract_consonants(tech.name, count=3)


def _build_topic_title(
    user: User,
    status: TicketStatus,
    assigned: bool,
    tech_tag: str | None = None,
) -> str:
    """
    Построить название топика по единому шаблону.

    Args:
        user: Объект пользователя (клиента)
        status: Статус тикета
        assigned: Назначен ли техник
        tech_tag: Тег техника (только для главной группы). Если None - тег не добавляется

    Returns:
        Название топика

    Примеры:
        - Главная группа с техником: "🟢 [ПВЛ] Иван (@ivan)"
        - Главная группа без техника: "🟢 [-] Иван (@ivan)"
        - Группа техника: "🟢 Иван (@ivan)"
    """
    emoji = _status_emoji(status)
    
    parts = [emoji]

    # Добавляем тег (для главной группы)
    if tech_tag is not None:
        parts.append(f"[{tech_tag}]")

    # Формируем имя клиента
    name_bits = []
    if user.first_name:
        name_bits.append(user.first_name)
    if user.last_name:
        name_bits.append(user.last_name)
    
    client_name = " ".join(name_bits) or user.username or str(user.tg_id)
    parts.append(client_name)

    # Username если есть
    if user.username:
        parts.append(f"(@{user.username})")

    title = " ".join(parts)

    # Telegram ограничивает 128 символов
    if len(title) > 128:
        title = title[:125] + "..."

    return title


async def _update_all_topic_titles(
    bot: Bot,
    ticket: Ticket,
    db: AsyncSession,
) -> None:
    """Обновляет названия топиков в главной группе и всех тех-групп.
    Сравнение ведётся ТОЛЬКО с БД, кеш игнорируется полностью.
    """

    if not ticket.client:
        logger.error(f"❌ У тикета {ticket.id} нет загруженного client")
        return

    # -----------------------------------
    # 1. Определяем есть ли назначенный техник
    # -----------------------------------
    has_tech = ticket.assigned_tech_id is not None

    # -----------------------------------------------------
    # 2. Формируем итоговое имя главного топика
    # -----------------------------------------------------
    main_title = _build_topic_title(
        user=ticket.client,
        status=ticket.status,
        assigned=has_tech,
        tech_tag=_get_tech_tag(await get_technician_by_id(session=db, tech_id=ticket.assigned_tech_id)) if has_tech else "-",
    )

    logger.debug(f"📝 Проверка главного топика: '{main_title}'")

    # -----------------------------------------------------
    # 3. Обновляем главный топик (он хранится в самом Ticket)
    # -----------------------------------------------------
    if ticket.main_chat_id and ticket.main_thread_id:
        try:
            await bot.edit_forum_topic(
                chat_id=ticket.main_chat_id,
                message_thread_id=ticket.main_thread_id,
                name=main_title
            )
            logger.info(f"✅ Обновлено название главного топика → {main_title}")
        except TelegramBadRequest as e:
            if "TOPIC_NOT_MODIFIED" not in str(e):
                logger.warning(f"⚠️ Ошибка изменения главного топика: {e}")
            else:
                logger.debug("ℹ️ Главное название уже корректное — обновление не требуется.")
    else:
        logger.warning(f"⚠️ У тикета {ticket.id} нет main_chat_id или main_thread_id")

    # -----------------------------------------------------
    # 4. Обновляем ВСЕ тех-топики
    # -----------------------------------------------------
    tech_threads = await get_all_tech_threads_for_ticket(session=db, ticket_id=ticket.id)

    if not tech_threads:
        logger.debug(f"ℹ️ У тикета {ticket.id} нет тех-топиков")
        return

    # Имя топика у техника всегда assigned=True (без [-] в начале)
    tech_title = _build_topic_title(
        user=ticket.client,
        status=ticket.status,
        assigned=True,
        tech_tag=None,
    )

    for thread in tech_threads:
        logger.debug(
            f"🛠 Проверка тех-топика {thread.tech_chat_id}/{thread.tech_thread_id} "
            f"→ '{tech_title}'"
        )

        # Проверяем, нужно ли обновлять название
        needs_update = False
        
        # Если в модели есть поле tech_thread_name
        if hasattr(thread, 'tech_thread_name'):
            needs_update = thread.tech_thread_name != tech_title
        else:
            # Если нет поля - всегда пытаемся обновить
            needs_update = True

        if needs_update:
            try:
                await bot.edit_forum_topic(
                    chat_id=thread.tech_chat_id,
                    message_thread_id=thread.tech_thread_id,
                    name=tech_title
                )
                logger.info(
                    f"✅ Обновлено название тех-топика {thread.tech_id} → '{tech_title}'"
                )
                
                # Обновляем в БД, если поле существует
                if hasattr(thread, 'tech_thread_name'):
                    thread.tech_thread_name = tech_title
                    await db.flush()
                    
            except TelegramBadRequest as e:
                if "TOPIC_NOT_MODIFIED" not in str(e):
                    logger.warning(f"⚠️ Ошибка изменения тех-топика: {e}")
                else:
                    logger.debug(f"ℹ️ Топик техника #{thread.tech_id} уже имеет корректное название")
        else:
            logger.debug(
                f"ℹ️ Топик техника #{thread.tech_id} уже имеет корректное название"
            )

    await db.commit()


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
    """Создать топик в группе техника (только если группа - форум)."""
    if not tech.group_chat_id:
        logger.error(f"❌ У техника {tech.name} нет привязанной группы")
        return None

    try:
        chat = await bot.get_chat(tech.group_chat_id)
    except TelegramBadRequest as e:
        logger.error(f"❌ Не удалось получить чат {tech.group_chat_id} для техника {tech.name}: {e}")
        return None

    # Проверяем, что это супергруппа с включенными темами
    is_forum = getattr(chat, "is_forum", False)
    if not (chat.type == ChatType.SUPERGROUP and is_forum):
        logger.error(
            f"❌ Чат {tech.group_chat_id} для техника {tech.name} не является форумом: "
            f"type={chat.type}, is_forum={is_forum}"
        )
        return None

    try:
        topic = await bot.create_forum_topic(
            chat_id=tech.group_chat_id,
            name=topic_name,
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


async def _copy_message_direct(
    bot: Bot,
    source_message: Message,
    target_chat_id: int,
    target_thread_id: int | None = None,
) -> bool:
    """
    Копирует сообщение напрямую (без очереди).
    
    Args:
        bot: Экземпляр бота
        source_message: Исходное сообщение
        target_chat_id: ID целевого чата
        target_thread_id: ID целевого топика (опционально)
        
    Returns:
        True если успешно
    """
    kwargs = {}
    if target_thread_id:
        kwargs["message_thread_id"] = target_thread_id

    try:
        if source_message.photo:
            await bot.send_photo(
                chat_id=target_chat_id,
                photo=source_message.photo[-1].file_id,
                caption=source_message.caption,
                parse_mode="HTML",
                **kwargs
            )
        elif source_message.video:
            await bot.send_video(
                chat_id=target_chat_id,
                video=source_message.video.file_id,
                caption=source_message.caption,
                parse_mode="HTML",
                **kwargs
            )
        elif source_message.document:
            await bot.send_document(
                chat_id=target_chat_id,
                document=source_message.document.file_id,
                caption=source_message.caption,
                parse_mode="HTML",
                **kwargs
            )
        elif source_message.voice:
            await bot.send_voice(
                chat_id=target_chat_id,
                voice=source_message.voice.file_id,
                caption=source_message.caption,
                parse_mode="HTML",
                **kwargs
            )
        elif source_message.audio:
            await bot.send_audio(
                chat_id=target_chat_id,
                audio=source_message.audio.file_id,
                caption=source_message.caption,
                parse_mode="HTML",
                **kwargs
            )
        elif source_message.video_note:
            await bot.send_video_note(
                chat_id=target_chat_id,
                video_note=source_message.video_note.file_id,
                **kwargs
            )
        else:
            # Текст
            text = source_message.text or source_message.caption or "[медиа]"
            await bot.send_message(
                chat_id=target_chat_id,
                text=text,
                parse_mode="HTML",
                disable_web_page_preview=True,
                **kwargs
            )
        
        return True

    except TelegramRetryAfter as e:
        logger.warning(f"⏳ 429: ждём {e.retry_after}s")
        await asyncio.sleep(e.retry_after)
        
        # Повторная попытка
        try:
            if source_message.photo:
                await bot.send_photo(chat_id=target_chat_id, photo=source_message.photo[-1].file_id, caption=source_message.caption, parse_mode="HTML", **kwargs)
            elif source_message.video:
                await bot.send_video(chat_id=target_chat_id, video=source_message.video.file_id, caption=source_message.caption, parse_mode="HTML", **kwargs)
            elif source_message.document:
                await bot.send_document(chat_id=target_chat_id, document=source_message.document.file_id, caption=source_message.caption, parse_mode="HTML", **kwargs)
            elif source_message.voice:
                await bot.send_voice(chat_id=target_chat_id, voice=source_message.voice.file_id, caption=source_message.caption, parse_mode="HTML", **kwargs)
            elif source_message.audio:
                await bot.send_audio(chat_id=target_chat_id, audio=source_message.audio.file_id, caption=source_message.caption, parse_mode="HTML", **kwargs)
            elif source_message.video_note:
                await bot.send_video_note(chat_id=target_chat_id, video_note=source_message.video_note.file_id, **kwargs)
            else:
                text = source_message.text or source_message.caption or "[медиа]"
                await bot.send_message(chat_id=target_chat_id, text=text, parse_mode="HTML", disable_web_page_preview=True, **kwargs)
            return True
        except Exception as retry_error:
            logger.error(f"❌ Повторная отправка провалилась: {retry_error}")
            return False

    except TelegramBadRequest as e:
        logger.error(f"❌ BadRequest при копировании: {e}")
        return False

    except Exception as e:
        logger.error(f"❌ Ошибка копирования: {e}", exc_info=True)
        return False
    

async def _copy_ticket_history_to_tech(
    bot: Bot,
    ticket: Ticket,
    tech_chat_id: int,
    tech_thread_id: int,
    db: AsyncSession,
) -> int:
    """
    Копирует историю тикета в топик техника.
    Порядок: история → разделитель → шапка клиента → кнопки
    """

    copied_count = 0

    try:
        # 1. Получаем историю сообщений
        from sqlalchemy import select as sql_select
        from app.db.models import TicketMessage

        stmt = (
            sql_select(TicketMessage)
            .where(TicketMessage.ticket_id == ticket.id)
            .order_by(TicketMessage.id)  # ✅ ВАЖНО: сортировка по ID, не created_at
        )
        result = await db.execute(stmt)
        messages = result.scalars().all()

        # Определяем последний sequence_id
        last_seq_id = messages[-1].id if messages else 0

        if not messages:
            logger.info("ℹ️ История сообщений пуста")
        else:
            logger.info(f"📋 История содержит {len(messages)} сообщений (seq: {messages[0].id} → {last_seq_id})")

            # 2. Обрабатываем каждое сообщение
            for msg in messages:
                try:
                    text = msg.message_text or ""
                    text_stripped = text.lstrip()

                    is_staff_note = text_stripped.startswith("💼 ")
                    is_internal_note = text_stripped.startswith("📝 ")
                    should_pin = is_staff_note or is_internal_note

                    # Формируем отображаемый текст (с префиксами)
                    if msg.is_from_admin and not should_pin:
                        prefix = "🛠️ <b>Поддержка:</b>\n"
                    elif not msg.is_from_admin and not should_pin:
                        prefix = "👤 <b>Клиент:</b>\n"
                    else:
                        prefix = ""

                    final_text = f"{prefix}{text}".strip()

                    payload = {
                        "bot_token": bot.token,
                        "target_chat_id": tech_chat_id,
                        "target_thread_id": tech_thread_id,
                        "ticket_id": ticket.id,
                        "sequence_id": msg.id,  # ✅ ID из БД
                        "attempt": 0,
                        "pin": False,
                    }

                    # --- Медиа ---
                    if msg.has_media and msg.media_file_id:

                        caption = msg.media_caption or text or ""
                        caption = f"{prefix}{caption}".strip() if prefix else caption

                        if msg.media_type == "photo":
                            payload.update({
                                "type": "photo",
                                "file_id": msg.media_file_id,
                                "caption": caption,
                            })

                        elif msg.media_type == "video":
                            payload.update({
                                "type": "video",
                                "file_id": msg.media_file_id,
                                "caption": caption,
                            })

                        elif msg.media_type == "document":
                            payload.update({
                                "type": "document",
                                "file_id": msg.media_file_id,
                                "caption": caption,
                            })

                        elif msg.media_type == "voice":
                            payload.update({
                                "type": "voice",
                                "file_id": msg.media_file_id,
                                "caption": caption,
                            })

                        else:
                            # fallback → просто как текст
                            payload.update({
                                "type": "text",
                                "text": final_text
                            })

                    else:
                        # --- Обычный текст ---
                        payload.update({
                            "type": "text",
                            "text": final_text
                        })

                    await redis_streams.enqueue(payload)
                    copied_count += 1

                except Exception as e:
                    logger.error(f"❌ Ошибка упаковки сообщения #{msg.id}: {e}")

            logger.info(f"✅ В очередь поставлено {copied_count} сообщений истории")

        # ========================================
        # 3. Разделитель (sequence_id = last + 1)
        # ========================================
        await redis_streams.enqueue({
            "bot_token": bot.token,
            "type": "text",
            "text": "📍 <b>Конец истории</b>",
            "target_chat_id": tech_chat_id,
            "target_thread_id": tech_thread_id,
            "ticket_id": ticket.id,
            "sequence_id": last_seq_id + 1,  # Порядок в конце
            "pin": False,
            "attempt": 0
        })

        # ========================================
        # 4. Шапка клиента (sequence_id = last + 2)
        # ========================================
        header_text = await _get_client_header_text(ticket)
        await redis_streams.enqueue({
            "bot_token": bot.token,
            "type": "text",
            "text": header_text,
            "target_chat_id": tech_chat_id,
            "target_thread_id": tech_thread_id,
            "ticket_id": ticket.id,
            "sequence_id": last_seq_id + 2,  # Порядок в конце
            "pin": True,
            "attempt": 0
        })

        # ========================================
        # 5. Кнопки (sequence_id = last + 3)
        # ========================================
        await redis_streams.enqueue({
            "bot_token": bot.token,
            "type": "status_buttons",
            "ticket_id": ticket.id,
            "target_chat_id": tech_chat_id,
            "target_thread_id": tech_thread_id,
            "sequence_id": last_seq_id + 3,  # Порядок в конце
            "pin": True,
            "attempt": 0
        })

        logger.info("📨 Шапка и кнопки отправлены в очередь (в конце)")

    except Exception as e:
        logger.error(f"❌ Ошибка при копировании истории: {e}", exc_info=True)

    return copied_count


# ─────────────────────────────────────────────
#  Обработка сообщений из топиков
# ─────────────────────────────────────────────

async def handle_main_group_message(message: Message, bot: Bot) -> None:
    """Обработка сообщений из топиков главной группы."""
    logger.info(
        "📨 handle_main_group_message: chat=%s thread=%s from=%s is_bot=%s content_type=%s",
        message.chat.id,
        message.message_thread_id,
        getattr(message.from_user, "id", None),
        getattr(message.from_user, "is_bot", None),
        message.content_type,
    )

    if not message.message_thread_id:
        logger.debug("ℹ️ Пропускаем сообщение без thread_id в главной группе")
        return

    # Системные сообщения - пытаемся удалить
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
        logger.info("⭐ Пытаемся удалить системное сообщение в главной группе")
        try:
            await message.delete()
        except TelegramBadRequest as e:
            logger.debug("Не смогли удалить системное сообщение: %s", e)
        return

    if message.text and message.text.startswith("/"):
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
            media_caption = None
        elif message.audio:
            media_type = "audio"
            media_file_id = message.audio.file_id
            media_caption = message.caption

        message_text = message.text or message.caption or "[медиа]"
        
        await get_or_create_user(
            db=db,
            telegram_id=message.from_user.id,
            username=message.from_user.username,
            first_name=message.from_user.first_name,
            last_name=message.from_user.last_name,
        )

        # ========================================
        # 1. Сохраняем в БД
        # ========================================
        try:
            from app.db.crud.message import TicketMessageCRUD

            msg_record = await TicketMessageCRUD.add_message(
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
            
            await db.flush()
            sequence_id = msg_record.id
            logger.debug(f"📝 Сохранено сообщение #{sequence_id}")

        except Exception as e:
            logger.error(f"❌ Не удалось сохранить в БД: {e}")
            return

        # ========================================
        # 2. ПРЯМОЕ копирование клиенту
        # ========================================
        success = await _copy_message_direct(
            bot=bot,
            source_message=message,
            target_chat_id=ticket.client_tg_id,
        )
        
        if success:
            logger.info(f"✅ Сообщение #{sequence_id} скопировано клиенту {ticket.client_tg_id}")
        else:
            logger.error(f"❌ Не удалось скопировать сообщение #{sequence_id} клиенту {ticket.client_tg_id}")

        # ========================================
        # 3. ПРЯМОЕ копирование в группу техника
        # ========================================
        if ticket.assigned_tech_id:
            tech_thread = await _get_tech_thread(db, ticket.id, ticket.assigned_tech_id)

            if not tech_thread:
                try:
                    from app.db.crud.ticket import get_tech_thread_by_user_and_tech
                    tech_thread = await get_tech_thread_by_user_and_tech(
                        session=db,
                        user_id=ticket.client_tg_id,
                        tech_id=ticket.assigned_tech_id,
                    )
                    if tech_thread:
                        logger.info(
                            "ℹ️ TechThread найден фолбеком: ticket=%s tech=%s",
                            ticket.id,
                            ticket.assigned_tech_id,
                        )
                        await cache.set_tech_thread_by_ticket(
                            ticket.id,
                            ticket.assigned_tech_id,
                            tech_thread.tech_chat_id,
                            tech_thread.tech_thread_id,
                        )
                except Exception as e:
                    logger.exception("❌ Ошибка при поиске TechThread: %s", e)

            if tech_thread and getattr(tech_thread, 'tech_chat_id', None) and getattr(tech_thread, 'tech_thread_id', None):
                success = await _copy_message_direct(
                    bot=bot,
                    source_message=message,
                    target_chat_id=tech_thread.tech_chat_id,
                    target_thread_id=tech_thread.tech_thread_id,
                )
                
                if success:
                    logger.info(
                        f"✅ Сообщение #{sequence_id} скопировано в группу техника "
                        f"(chat={tech_thread.tech_chat_id} thread={tech_thread.tech_thread_id})"
                    )
                else:
                    logger.error(
                        f"❌ Не удалось скопировать сообщение #{sequence_id} в группу техника"
                    )
            else:
                logger.debug(
                    f"ℹ️ TechThread не найден для ticket={ticket.id} tech={ticket.assigned_tech_id}"
                )
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


async def find_existing_tech_topic_for_client(
    db: AsyncSession,
    client_tg_id: int,
    tech_id: int
) -> TechThread | None:
    """
    Возвращает старый тех-топик этого техника для данного клиента.
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
    res = await db.execute(stmt)
    return res.scalar_one_or_none()


async def enqueue_ticket_messages_to_tech(
    db: AsyncSession,
    ticket: Ticket,
    tech_chat_id: int,
    tech_thread_id: int,
    bot_token: str,
):
    """
    Отправляет в Redis Streams ВСЕ сообщения текущего тикета
    (НЕ историю старых тикетов этого клиента).
    """
    from sqlalchemy import select
    from app.db.models import TicketMessage
    from app.utils.redis_streams import redis_streams

    stmt = (
        select(TicketMessage)
        .where(TicketMessage.ticket_id == ticket.id)
        .order_by(TicketMessage.created_at)
    )
    result = await db.execute(stmt)
    messages = result.scalars().all()

    for msg in messages:

        payload = {
            "bot_token": bot_token,
            "attempt": 0,
            "type": None,
            "target_chat_id": tech_chat_id,
            "target_thread_id": tech_thread_id,
            "pin": False
        }

        # -------- TEXT --------
        if not msg.has_media:
            payload["type"] = "text"
            payload["text"] = msg.message_text or ""
            await redis_streams.enqueue(payload)
            continue

        # -------- MEDIA --------
        payload["file_id"] = msg.media_file_id
        payload["caption"] = msg.media_caption or msg.message_text or ""

        payload["type"] = msg.media_type  # photo / video / voice / document
        await redis_streams.enqueue(payload)


# ─────────────────────────────────────────────
#  Callback: назначение техника
# ─────────────────────────────────────────────
async def callback_assign_tech(call: CallbackQuery, bot: Bot) -> None:
    """Обработка назначения техника на тикет."""
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
            # Загружаем тикет с клиентом
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

            # Если уже стоит этот техник — ничего не делаем
            if ticket.assigned_tech_id == tech_id:
                await call.answer("✅ Этот техник уже назначен", show_alert=False)
                return

            if not ticket.client:
                logger.error(f"❌ У тикета {ticket.id} нет связанного клиента")
                await call.answer("❌ Ошибка: клиент не найден.", show_alert=True)
                return

            tag = _extract_consonants(tech.name)
          
            tech_title = _build_topic_title(
                user=ticket.client,
                status=ticket.status,
                assigned=True,
                tech_tag=None,
            )

            # ========================================
            # 1) Удаляем связь со старым техником
            # ========================================
            if ticket.assigned_tech_id and ticket.assigned_tech_id != tech_id:
                old_thread = await _get_tech_thread(
                    db,
                    ticket.id,
                    ticket.assigned_tech_id,
                )
                if old_thread:
                    await _close_tech_topic(
                        bot,
                        old_thread.tech_chat_id,
                        old_thread.tech_thread_id,
                    )
                    await db.delete(old_thread)
                    logger.info(
                        f"🗑 Удален TechThread для старого техника {ticket.assigned_tech_id}"
                    )

            # ========================================
            # 2) Ищем существующий топик у НОВОГО техника
            # ========================================
            existing_thread = await find_existing_tech_topic_for_client(
                db=db,
                client_tg_id=ticket.client_tg_id,
                tech_id=tech.id
            )

            tech_thread_id = None

            if existing_thread:
                # ========================================
                # Путь А: Переиспользуем старый топик
                # ========================================
                tech_thread_id = existing_thread.tech_thread_id

                # Привязываем к текущему тикету
                existing_thread.ticket_id = ticket.id
                existing_thread.tech_thread_name = tech_title
                await db.flush()

                # Переоткрываем
                await _reopen_tech_topic(
                    bot,
                    existing_thread.tech_chat_id,
                    tech_thread_id,
                )

                # Переименовываем
                try:
                    await bot.edit_forum_topic(
                        chat_id=existing_thread.tech_chat_id,
                        message_thread_id=tech_thread_id,
                        name=tech_title
                    )
                except TelegramBadRequest:
                    pass
                
                logger.info(
                    f"♻️ Переиспользован топик {tech_thread_id} "
                    f"для клиента {ticket.client_tg_id} и техника {tech.id}"
                )
                
                # ✅ История уже есть в старом топике - ничего не копируем

            else:
                # ========================================
                # Путь Б: Создаём новый топик
                # ========================================
                tech_thread_id = await _create_tech_topic(
                    bot,
                    tech,
                    tech_title,
                )
                if not tech_thread_id:
                    await call.answer(
                        "❌ Не удалось создать топик в группе техника.",
                        show_alert=True
                    )
                    return

                # Проверяем существование TechThread для этого ticket+tech
                stmt_check = select(TechThread).where(
                    TechThread.ticket_id == ticket.id,
                    TechThread.tech_id == tech.id
                )
                existing_record = await db.execute(stmt_check)
                tech_thread_obj = existing_record.scalar_one_or_none()

                if tech_thread_obj:
                    # Уже есть запись — обновляем
                    tech_thread_obj.tech_chat_id = tech.group_chat_id
                    tech_thread_obj.tech_thread_id = tech_thread_id
                    tech_thread_obj.tech_thread_name = tech_title
                    logger.info(f"♻️ Обновлен TechThread для ticket={ticket.id} tech={tech.id}")
                else:
                    # Создаём новую запись
                    tech_thread = TechThread(
                        ticket_id=ticket.id,
                        user_id=ticket.client_tg_id,
                        tech_id=tech.id,
                        tech_chat_id=tech.group_chat_id,
                        tech_thread_id=tech_thread_id,
                        tech_thread_name=tech_title,
                    )
                    db.add(tech_thread)
                    logger.info(f"✅ Создан новый TechThread для ticket={ticket.id} tech={tech.id}")
    
                await db.flush()

                # ========================================
                # Копируем историю ЧЕРЕЗ ВОРКЕР (только для нового топика)
                # ========================================
                stmt_with_messages = (
                    select(Ticket)
                    .options(
                        selectinload(Ticket.client),
                        selectinload(Ticket.messages),
                    )
                    .where(Ticket.id == ticket.id)
                )
                result = await db.execute(stmt_with_messages)
                ticket_with_messages = result.scalar_one_or_none()

                if ticket_with_messages:
                    copied = await _copy_ticket_history_to_tech(
                        bot=bot,
                        ticket=ticket_with_messages,
                        tech_chat_id=tech.group_chat_id,
                        tech_thread_id=tech_thread_id,
                        db=db,
                    )
                    logger.info(
                        f"📋 История из {copied} сообщений добавлена в очередь воркера "
                        f"(тикет #{ticket.id}, техник {tech.id})"
                    )
    
            # ========================================
            # 3) Обновляем assigned_tech_id
            # ========================================
            ticket.assigned_tech_id = tech.id
            await db.commit()

            logger.info(
                f"✅ Техник {tech.name} (ID={tech.id}) назначен на тикет #{ticket.id}"
            )

            # ========================================
            # 4) Обновляем названия всех топиков
            # ========================================
            await _update_all_topic_titles(bot, ticket, db)

            # ========================================
            # 5) Скрываем клавиатуру выбора
            # ========================================
            try:
                await call.message.edit_reply_markup(reply_markup=None)
            except TelegramBadRequest as e:
                logger.warning(f"⚠️ Не удалось скрыть клавиатуру: {e}")

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
            f"🔖 Топик: #{tech_thread_id}\n"
            f"🏷 Тег: [{tag}]",
            parse_mode="HTML",
        )
    except Exception as e:
        logger.error(f"Не удалось отправить подтверждение: {e}")

    try:
        await call.answer(f"✅ {tech.name} назначен")
    except Exception:
        pass


# ─────────────────────────────────────────────
#  Callback: изменение статуса
# ─────────────────────────────────────────────

async def callback_change_status(call: CallbackQuery, bot: Bot) -> None:
    """Обработка изменения статуса тикета (NEW / WORK / CLOSED).
    Даже если статус не меняется — топики обязаны обновиться!
    """

    is_admin = settings.is_admin(call.from_user.id)

    # ----------------------------
    # 1. Парсим callback
    # ----------------------------
    try:
        action, ticket_id_str = call.data.split(":", maxsplit=1)
        ticket_id = int(ticket_id_str)

        map_status = {
            "status_new": TicketStatus.NEW,
            "status_work": TicketStatus.WORK,
            "status_close": TicketStatus.CLOSED,
        }

        new_status = map_status.get(action)
        if not new_status:
            await call.answer("❌ Неизвестный статус", show_alert=True)
            return

    except Exception as e:
        logger.error(f"❌ Ошибка парсинга callback_data: {e}")
        await call.answer("❌ Ошибка данных.", show_alert=True)
        return

    # ----------------------------
    # 2. Загружаем тикет
    # ----------------------------
    async with db_manager.session() as db:
        try:

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

            # ----------------------------
            # 3. Определяем теха (если не админ)
            # ----------------------------
            current_tech = None

            if not is_admin:
                techs = await get_technicians(session=db, active_only=True)
                for t in techs:
                    if t.tg_user_id == call.from_user.id:
                        current_tech = t
                        break

                if not current_tech:
                    await call.answer("⛔ У вас нет прав", show_alert=True)
                    return

                if ticket.assigned_tech_id and ticket.assigned_tech_id != current_tech.id:
                    await call.answer("⛔ Вы не назначены на тикет", show_alert=True)
                    return

            old_status = ticket.status

            # ============================================================
            # 4. Если статус НЕ меняется → всё равно обновляем названия!
            # ============================================================
            if ticket.status == new_status:
                logger.info(
                    f"ℹ️ Статус тикета #{ticket.id} уже {new_status}, но обновляем топики"
                )

                await _update_all_topic_titles(bot, ticket, db)

                emoji = {
                    TicketStatus.NEW: "🟢",
                    TicketStatus.WORK: "🟡",
                    TicketStatus.CLOSED: "⚪️",
                }[new_status]

                await call.answer(f"{emoji} Статус уже установлен\n🔄 Топики обновлены", show_alert=True)
                return

            # ============================================================
            # 5. Статус действительно меняется → обновляем
            # ============================================================
            # Техник сам берёт тикет → назначаем его
            if current_tech and new_status == TicketStatus.WORK and not ticket.assigned_tech_id:
                ticket.assigned_tech_id = current_tech.id
                logger.info(f"🔧 Автоназначение техника {current_tech.name} на тикет #{ticket.id}")

            # Обновляем статус
            ticket.status = new_status
            await db.commit()
            await db.refresh(ticket)

            # ============================================================
            # 6. Открытие топиков при переходе в NEW/WORK
            # ============================================================
            if new_status in (TicketStatus.NEW, TicketStatus.WORK):

                # Главный топик
                try:
                    await bot.reopen_forum_topic(
                        chat_id=ticket.main_chat_id,
                        message_thread_id=ticket.main_thread_id
                    )
                except TelegramBadRequest:
                    pass

                # Тех-топики
                tech_threads = await get_all_tech_threads_for_ticket(session=db, ticket_id=ticket.id)
                for th in tech_threads:
                    try:
                        await bot.reopen_forum_topic(
                            chat_id=th.tech_chat_id,
                            message_thread_id=th.tech_thread_id
                        )
                    except TelegramBadRequest:
                        pass

            # ============================================================
            # 7. Обновляем РЕАЛЬНО все названия топиков
            # ============================================================
            await _update_all_topic_titles(bot, ticket, db)

            # ============================================================
            # 8. Закрытие тикета
            # ============================================================
            if new_status == TicketStatus.CLOSED:
                # Закрываем главный топик
                try:
                    await bot.close_forum_topic(
                        chat_id=ticket.main_chat_id,
                        message_thread_id=ticket.main_thread_id,
                    )
                except Exception:
                    pass

                # Закрываем тех-топики
                tech_threads = await get_all_tech_threads_for_ticket(session=db, ticket_id=ticket.id)
                for th in tech_threads:
                    try:
                        await bot.close_forum_topic(
                            chat_id=th.tech_chat_id,
                            message_thread_id=th.tech_thread_id,
                        )
                    except Exception:
                        pass

            emoji = {
                TicketStatus.NEW: "🟢",
                TicketStatus.WORK: "🟡",
                TicketStatus.CLOSED: "⚪️",
            }[new_status]
            await call.answer(f"{emoji} Статус изменён", show_alert=True)
        except Exception as e:
            logger.error(f"❌ Ошибка callback_change_status:", exc_info=True)
            await db.rollback()
            await call.answer("❌ Ошибка.", show_alert=True)


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