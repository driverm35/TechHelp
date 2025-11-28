# app/bot/handlers/tech_group.py
from __future__ import annotations
import logging
from aiogram import Dispatcher, F, Bot
from aiogram.enums import ChatType
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
from aiogram.exceptions import TelegramBadRequest
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.config import settings
from app.db.database import db_manager
from app.db.crud.user import get_or_create_user
from app.db.crud.tech import (
    get_technicians,
    get_technician_by_id,
)
from sqlalchemy import select
from app.db.models import Technician

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  Вспомогательные функции
# ─────────────────────────────────────────────

def _is_main_group(chat_id: int) -> bool:
    """Проверка, является ли группа главной."""
    return chat_id == settings.main_group_id


async def _get_tech_by_group_id(session, group_id: int) -> Technician | None:
    """Получить техника по group_chat_id."""
    stmt = (
        select(Technician)
        .where(
            Technician.group_chat_id == group_id,
            Technician.is_active.is_(True)
        )
        .limit(1)
    )
    res = await session.execute(stmt)
    return res.scalar_one_or_none()


async def _clear_other_techs_from_group(session, group_id: int, except_tech_id: int | None = None) -> None:
    """Очистить group_chat_id у всех техников этой группы, кроме указанного."""
    stmt = select(Technician).where(Technician.group_chat_id == group_id)
    if except_tech_id:
        stmt = stmt.where(Technician.id != except_tech_id)

    res = await session.execute(stmt)
    techs = res.scalars().all()

    for tech in techs:
        tech.group_chat_id = None
        logger.info(f"🔄 Очищена группа у техника {tech.name} (ID: {tech.id})")


async def _make_user_admin(bot: Bot, chat_id: int, user_id: int) -> bool:
    """Попытаться сделать пользователя администратором группы."""
    try:
        await bot.promote_chat_member(
            chat_id=chat_id,
            user_id=user_id,
            can_manage_chat=True,
            can_delete_messages=True,
            can_manage_video_chats=False,
            can_restrict_members=True,
            can_promote_members=False,
            can_change_info=False,
            can_invite_users=True,
            can_post_messages=False,
            can_edit_messages=False,
            can_pin_messages=True,
            can_manage_topics=True,
        )
        logger.info(f"✅ Пользователь {user_id} назначен админом в группе {chat_id}")
        return True
    except TelegramBadRequest as e:
        error_msg = str(e).lower()

        # Пользователь не является участником группы
        if any(phrase in error_msg for phrase in [
            "user not found",
            "user is not a member",
            "participant_id_invalid",
            "user_not_participant"
        ]):
            logger.warning(
                f"⚠️ Пользователь {user_id} еще не присоединился к группе {chat_id}"
            )
            return False

        # Бот не имеет прав на назначение администраторов
        if "not enough rights" in error_msg or "chat_admin_required" in error_msg:
            logger.error(
                f"❌ У бота недостаточно прав для назначения администраторов в группе {chat_id}"
            )
            return False

        # Другие ошибки
        logger.error(f"❌ Ошибка при назначении админа: {e}")
        return False
    except Exception as e:
        logger.error(f"❌ Неожиданная ошибка при назначении админа: {e}")
        return False


# ─────────────────────────────────────────────
#  Команда /join
# ─────────────────────────────────────────────

async def cmd_join(message: Message, bot: Bot) -> None:
    """
    Команда /join - привязать техника к этой группе.

    Логика:
    1. Проверка, что команда от админа бота
    2. Проверка, что это не главная группа
    3. Показать клавиатуру со всеми техниками
    4. Дождаться выбора техника
    """
    # Проверка: только админы бота
    if not settings.is_admin(message.from_user.id):
        logger.warning(f"⛔ Попытка использовать /join от не-админа: {message.from_user.id}")
        return

    # Проверка: только группы
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("⛔ Эта команда работает только в группах.")
        return

    # Проверка: не главная группа
    if _is_main_group(message.chat.id):
        await message.answer("⛔ Эта команда не работает в главной группе поддержки.")
        return

    # Получаем список активных техников
    async with db_manager.session() as db:
        technicians = await get_technicians(session=db, active_only=True)

        if not technicians:
            await message.answer("❌ Нет доступных техников.")
            return

        # Проверим, есть ли уже техник в этой группе
        current_tech = await _get_tech_by_group_id(db, message.chat.id)

    # Проверка прав бота
    try:
        bot_member = await bot.get_chat_member(message.chat.id, bot.id)
        if not bot_member.can_promote_members:
            await message.answer(
                "⚠️ <b>У бота нет прав на назначение администраторов</b>\n\n"
                "Дайте боту права:\n"
                "1. Зайдите в настройки группы\n"
                "2. Администраторы → Найдите бота\n"
                "3. Включите: <i>Добавление администраторов</i>",
                parse_mode="HTML"
            )
            return
    except Exception as e:
        logger.warning(f"Не удалось проверить права бота: {e}")

    # Строим клавиатуру
    builder = InlineKeyboardBuilder()
    for tech in technicians:
        # Показываем, если у техника уже есть группа
        text = tech.name
        if tech.group_chat_id:
            if tech.group_chat_id == message.chat.id:
                text = f"✅ {tech.name} (текущий)"
            else:
                text = f"🔒 {tech.name} (занят)"

        builder.button(
            text=text,
            callback_data=f"tech_join:{message.chat.id}:{tech.id}",
        )

    # Кнопка отмены
    builder.button(
        text="❌ Отмена",
        callback_data=f"tech_join_cancel:{message.chat.id}",
    )

    builder.adjust(1)  # По 1 кнопке в ряд

    info_text = (
        "👥 <b>Привязка техника к группе</b>\n\n"
        "Выберите техника, который будет закреплен за этой группой:\n"
    )

    if current_tech:
        info_text += f"\n📌 Текущий техник: <b>{current_tech.name}</b>"

    await message.answer(
        info_text,
        reply_markup=builder.as_markup(),
        parse_mode="HTML"
    )


async def callback_tech_join(call: CallbackQuery, bot: Bot) -> None:
    """Обработка выбора техника при /join."""
    logger.info(f"🔧 callback_tech_join: data={call.data}, user={call.from_user.id}")

    if not settings.is_admin(call.from_user.id):
        logger.warning(f"⛔ Не админ: {call.from_user.id}")
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, group_id_str, tech_id_str = call.data.split(":", maxsplit=2)
        group_id = int(group_id_str)
        tech_id = int(tech_id_str)
        logger.info(f"🔧 Parsed: group_id={group_id}, tech_id={tech_id}")
    except Exception as e:
        logger.error(f"❌ Ошибка парсинга: {e}")
        await call.answer("❌ Некорректные данные.", show_alert=True)
        return

    # Проверка, что callback вызван из той же группы
    logger.info(f"🔧 Chat check: message.chat.id={call.message.chat.id}, group_id={group_id}")
    if call.message.chat.id != group_id:
        logger.warning(f"❌ Группы не совпадают: {call.message.chat.id} != {group_id}")
        await call.answer("❌ Ошибка: группа не совпадает.", show_alert=True)
        return

    logger.info("✅ Проверки пройдены, работаем с БД...")

    async with db_manager.session() as db:
        tech = await get_technician_by_id(session=db, tech_id=tech_id)

        if not tech:
            await call.answer("❌ Техник не найден.", show_alert=True)
            return

        # Создаём или получаем пользователя для техника
        from app.db.crud.user import get_or_create_user

        try:
            tech_user_info = await bot.get_chat(tech.tg_user_id)
            await get_or_create_user(
                db=db,
                telegram_id=tech.tg_user_id,
                username=tech_user_info.username,
                first_name=tech_user_info.first_name,
                last_name=tech_user_info.last_name,
            )
        except Exception as e:
            logger.warning(f"⚠️ Не удалось получить информацию о технике {tech.tg_user_id}: {e}")
            # Создаём с минимальными данными
            await get_or_create_user(
                db=db,
                telegram_id=tech.tg_user_id,
                first_name=tech.name,
            )

        # TOGGLE ЛОГИКА: если техник уже закреплен за этой группой - открепляем
        if tech.group_chat_id == group_id:
            tech.group_chat_id = None
            await db.commit()

            logger.info(
                f"🗑 Техник {tech.name} (ID: {tech.id}) откреплен от группы {group_id}"
            )

            success_text = (
                f"✅ <b>Техник {tech.name} откреплен от группы</b>\n\n"
                f"Используйте /join чтобы назначить нового техника."
            )

            try:
                await call.message.edit_text(
                    success_text,
                    parse_mode="HTML"
                )
            except Exception as e:
                logger.warning(f"Не удалось отредактировать сообщение: {e}")
                await call.message.answer(success_text, parse_mode="HTML")

            await call.answer("🗑 Техник откреплен")
            return

        # Проверяем, есть ли у техника другая группа
        if tech.group_chat_id and tech.group_chat_id != group_id:
            await call.answer(
                f"⚠️ Техник {tech.name} уже закреплен за другой группой.",
                show_alert=True
            )
            return

        # Закрепляем: очищаем всех других техников из этой группы
        await _clear_other_techs_from_group(db, group_id, except_tech_id=tech_id)

        # Привязываем техника к группе
        tech.group_chat_id = group_id
        await db.commit()
        await db.refresh(tech)

        logger.info(
            f"✅ {tech.name} (ID: {tech.id}) привязан к группе {group_id}"
        )

    # Пытаемся сделать техника админом (если он уже в группе)
    admin_status = await _make_user_admin(bot, group_id, tech.tg_user_id)
    logger.info(f"Сделали админом: {admin_status}")

    success_text = (
        f"✅ <b>{tech.name} закреплен за этой группой</b>\n\n"
    )

    try:
        await call.message.edit_text(
            success_text,
            parse_mode="HTML"
        )
    except Exception as e:
        logger.warning(f"Не удалось отредактировать сообщение: {e}")
        await call.message.answer(success_text, parse_mode="HTML")

    await call.answer("✅ Техник закреплен")


async def callback_tech_join_cancel(call: CallbackQuery) -> None:
    """Отмена привязки техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        await call.message.delete()
    except Exception:
        try:
            await call.message.edit_text("❌ Отменено.")
        except Exception:
            pass

    await call.answer("Отменено")


# ─────────────────────────────────────────────
#  Команда /kick
# ─────────────────────────────────────────────

async def cmd_kick(message: Message, bot: Bot) -> None:
    """
    Команда /kick - открепить техника от этой группы.

    Логика:
    1. Проверка, что команда от админа бота
    2. Проверка, что это не главная группа
    3. Найти техника по group_chat_id
    4. Очистить у него group_chat_id
    5. Написать подтверждение
    """
    # Проверка: только админы бота
    if not settings.is_admin(message.from_user.id):
        logger.warning(f"⛔ Попытка использовать /kick от не-админа: {message.from_user.id}")
        return

    # Проверка: только группы
    if message.chat.type not in (ChatType.GROUP, ChatType.SUPERGROUP):
        await message.answer("⛔ Эта команда работает только в группах.")
        return

    # Проверка: не главная группа
    if _is_main_group(message.chat.id):
        await message.answer("⛔ Эта команда не работает в главной группе поддержки.")
        return

    # Ищем техника, привязанного к этой группе
    async with db_manager.session() as db:
        tech = await _get_tech_by_group_id(db, message.chat.id)

        if not tech:
            await message.answer(
                "❌ К этой группе не привязан ни один техник.\n\n"
                "Используйте /join для привязки техника."
            )
            return

        tech_name = tech.name
        tech_id = tech.id

        # Очищаем привязку к группе
        tech.group_chat_id = None
        await db.commit()

        logger.info(
            f"🗑 Техник {tech_name} (ID: {tech_id}) откреплен от группы {message.chat.id}"
        )

    await message.answer(
        f"✅ Техник <b>{tech_name}</b> откреплен от этой группы.",
        parse_mode="HTML"
    )


# ─────────────────────────────────────────────
#  Регистрация обработчиков
# ─────────────────────────────────────────────

def register_handlers(dp: Dispatcher) -> None:
    """Регистрация обработчиков для управления группами техников."""
    logger.info("🔧 === НАЧАЛО регистрации обработчиков tech_group.py ===")

    # Команды работают только в группах
    dp.message.register(
        cmd_join,
        Command("join"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    )

    dp.message.register(
        cmd_kick,
        Command("kick"),
        F.chat.type.in_({ChatType.GROUP, ChatType.SUPERGROUP}),
    )

    # Callback для выбора техника
    dp.callback_query.register(
        callback_tech_join,
        F.data.startswith("tech_join:"),
    )

    dp.callback_query.register(
        callback_tech_join_cancel,
        F.data.startswith("tech_join_cancel:"),
    )

    logger.info("✅ Зарегистрированы обработчики для /join и /kick")
    logger.info("🔧 === КОНЕЦ регистрации обработчиков tech_group.py ===")