# app/bot/handlers/admin.py
from __future__ import annotations
import logging
import re

from dataclasses import dataclass
from aiogram import Dispatcher, F
from aiogram.enums import ContentType
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import Message, CallbackQuery
from datetime import time

from app.config import settings
from app.db.database import db_manager
from app.db.models import Technician
from app.db.crud.tech import (
    get_technicians,
    get_technician_by_id,
    upsert_technician,
    deactivate_technician_by_id,
)
from app.bot.keyboards import admin_kb as admin_kb

logger = logging.getLogger(__name__)


class AdminTechStates(StatesGroup):
    waiting_contact = State()
    waiting_manual_name = State()
    waiting_manual_tg_id = State()
    waiting_new_name = State()
    waiting_auto_hours = State()

@dataclass
class TechAddContext:
    menu_msg_id: int | None = None
    name: str | None = None


async def _load_technicians_text_and_kb() -> tuple[str, list[Technician], object]:
    """Возвращает текст и клавиатуру списка техников."""
    async with db_manager.session() as db:
        techs = await get_technicians(session=db, active_only=True)
        if techs:
            text = "👨‍🔧 <b>Управление техниками</b>\n\nВыберите техника или добавьте нового:"
        else:
            text = (
                "👨‍🔧 <b>Управление техниками</b>\n\n"
                "Пока нет ни одного техника. Нажмите «➕ Добавить техника»."
            )
        kb = admin_kb.get_technicians_menu_keyboard(technicians=techs)
    return text, techs, kb


async def _back_to_tech_menu(call: CallbackQuery, state: FSMContext) -> None:
    """Универсальный возврат в список техников."""
    await state.clear()
    text, _, kb = await _load_technicians_text_and_kb()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


async def handle_admin_technicians_menu(
    call: CallbackQuery, state: FSMContext
) -> None:
    """Обработчик инлайн-кнопки «Техники» из главного меню админа."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Доступно только администраторам.", show_alert=True)
        return
    await state.clear()
    text, _, kb = await _load_technicians_text_and_kb()
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


async def admin_add_tech_start(call: CallbackQuery, state: FSMContext) -> None:
    """Начало процесса добавления техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    ctx = TechAddContext(menu_msg_id=call.message.message_id)
    await state.update_data(tech_add_ctx=ctx.__dict__)

    kb = admin_kb.get_add_tech_method_keyboard()
    text = (
        "➕ <b>Добавление техника</b>\n\n"
        "Выберите способ:\n"
        "• 📎 Отправить контакт техника\n"
        "• ⌨️ Ввести данные вручную"
    )
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


async def admin_add_tech_contact_choice(
    call: CallbackQuery, state: FSMContext
) -> None:
    """Выбор способа добавления через контакт."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    data = await state.get_data()
    ctx_dict = data.get("tech_add_ctx") or {}
    ctx = TechAddContext(**ctx_dict)
    ctx.menu_msg_id = call.message.message_id
    await state.update_data(tech_add_ctx=ctx.__dict__)
    await state.set_state(AdminTechStates.waiting_contact)

    kb = admin_kb.get_back_button_keyboard()
    text = (
        "📎 <b>Отправьте контакт техника</b>\n\n"
        "Используйте кнопку «Поделиться контактом» в Telegram.\n\n"
        "После получения контакта техник будет автоматически добавлен."
    )
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


async def admin_add_tech_contact_message(msg: Message, state: FSMContext) -> None:
    """Обработка сообщения с контактом."""

    if not settings.is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет прав.")
        return

    logger.info(f"🔍 Проверка контакта: {msg.contact}")

    if not msg.contact:
        await msg.answer("📎 Отправьте, пожалуйста, контакт техника.")
        return

    contact = msg.contact
    tg_user_id = contact.user_id
    name = (
        " ".join(
            part for part in [contact.first_name, contact.last_name] if part
        ).strip()
        or "Техник"
    )

    logger.info(f"✅ Обрабатываем контакт: {name} (ID: {tg_user_id})")

    async with db_manager.session() as db:
        tech = await upsert_technician(
            session=db,
            name=name,
            tg_user_id=tg_user_id,
            is_active=True,
        )
        await db.commit()

    logger.info(
        "✅ Добавлен техник через контакт: %s (%s)", tech.name, tech.tg_user_id
    )

    data = await state.get_data()
    ctx_dict = data.get("tech_add_ctx") or {}
    ctx = TechAddContext(**ctx_dict)
    text, _, kb = await _load_technicians_text_and_kb()

    # 🗑️ Удаляем сообщение с контактом
    try:
        await msg.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение с контактом: %s", e)

    if ctx.menu_msg_id:
        try:
            await msg.bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=ctx.menu_msg_id,
                text=text + f"\n\n✅ Техник <b>{tech.name}</b> добавлен.",
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            await msg.answer(
                f"✅ Техник <b>{tech.name}</b> добавлен.",
                parse_mode="HTML",
                reply_markup=kb,
            )
    else:
        await msg.answer(
            f"✅ Техник <b>{tech.name}</b> добавлен.",
            parse_mode="HTML",
            reply_markup=kb,
        )
    await state.clear()


async def admin_add_tech_manual_choice(
    call: CallbackQuery, state: FSMContext
) -> None:
    """Выбор способа добавления вручную."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    data = await state.get_data()
    ctx_dict = data.get("tech_add_ctx") or {}
    ctx = TechAddContext(**ctx_dict)
    ctx.menu_msg_id = call.message.message_id
    await state.update_data(tech_add_ctx=ctx.__dict__)
    await state.set_state(AdminTechStates.waiting_manual_name)

    kb = admin_kb.get_back_button_keyboard()
    text = (
        "⌨️ <b>Добавление техника вручную</b>\n\n"
        "Введите имя техника (как оно будет отображаться в кнопке):"
    )
    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()


async def admin_add_tech_manual_name(msg: Message, state: FSMContext) -> None:
    """Получение имени техника при добавлении вручную."""
    if not settings.is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет прав.")
        return

    name = (msg.text or "").strip()

    # 🗑️ Удаляем сообщение пользователя
    try:
        await msg.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение: %s", e)

    if len(name) < 2:
        await msg.answer("Имя слишком короткое, введите от 2 символов.")
        return

    data = await state.get_data()
    ctx_dict = data.get("tech_add_ctx") or {}
    ctx = TechAddContext(**ctx_dict)
    ctx.name = name
    await state.update_data(tech_add_ctx=ctx.__dict__)
    await state.set_state(AdminTechStates.waiting_manual_tg_id)

    kb = admin_kb.get_back_button_keyboard()
    text = (
        f"Имя техника: <b>{name}</b>\n\n"
        "Теперь отправьте <b>Telegram ID</b> техника (числом)."
    )

    if ctx.menu_msg_id:
        try:
            await msg.bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=ctx.menu_msg_id,
                text=text,
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            await msg.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")


async def admin_add_tech_manual_tg_id(msg: Message, state: FSMContext) -> None:
    """Получение Telegram ID техника при добавлении вручную."""
    if not settings.is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет прав.")
        return

    tg_text = (msg.text or "").strip()

    # 🗑️ Удаляем сообщение пользователя
    try:
        await msg.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение: %s", e)

    try:
        tg_user_id = int(tg_text)
    except ValueError:
        await msg.answer("❌ Telegram ID должен быть числом. Попробуйте ещё раз.")
        return

    data = await state.get_data()
    ctx_dict = data.get("tech_add_ctx") or {}
    ctx = TechAddContext(**ctx_dict)

    if not ctx.name:
        await msg.answer("❌ Не найдено имя техника. Начните заново.")
        await state.clear()
        return

    async with db_manager.session() as db:
        tech = await upsert_technician(
            session=db,
            name=ctx.name,
            tg_user_id=tg_user_id,
            is_active=True,
        )
        await db.commit()

    logger.info("✅ Добавлен техник вручную: %s (%s)", tech.name, tech.tg_user_id)

    text, _, kb = await _load_technicians_text_and_kb()

    if ctx.menu_msg_id:
        try:
            await msg.bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=ctx.menu_msg_id,
                text=text + f"\n\n✅ Техник <b>{tech.name}</b> добавлен.",
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            await msg.answer(
                f"✅ Техник <b>{tech.name}</b> добавлен.",
                parse_mode="HTML",
                reply_markup=kb,
            )
    else:
        await msg.answer(
            f"✅ Техник <b>{tech.name}</b> добавлен.",
            parse_mode="HTML",
            reply_markup=kb,
        )
    await state.clear()


async def admin_edit_tech_hours_start(
    call: CallbackQuery,
    state: FSMContext,
) -> None:
    """
    Старт редактирования часов автонаправления для техника.
    callback_data: admin_edit_tech_hours:{tech_id}
    """
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, tech_id_str = call.data.split(":", maxsplit=1)
        tech_id = int(tech_id_str)
    except (ValueError, IndexError):
        await call.answer("❌ Некорректные данные.", show_alert=True)
        return

    async with db_manager.session() as db:
        tech = await get_technician_by_id(session=db, tech_id=tech_id)
        if not tech:
            await call.answer("❌ Техник не найден.", show_alert=True)
            return

        current_status = "включено" if tech.is_auto_assign else "выключено"
        current_hours = (
            f"{tech.auto_assign_start_hour}–{tech.auto_assign_end_hour}"
            if tech.auto_assign_start_hour and tech.auto_assign_end_hour
            else "не заданы"
        )

        text = [
            f"🕐 <b>Автонаправление для техника</b> <b>{tech.name}</b>",
            "",
            f"Сейчас: <b>{current_status}</b>",
            f"Часы: <code>{current_hours}</code>",
            "",
            "Отправьте часы в формате:",
            "<code>09:00-18:00</code> или <code>9-18</code>",
            "",
            "Чтобы <b>выключить</b> автоназначение — отправьте:",
            "<code>0</code>, <code>off</code> или <code>выкл</code>.",
        ]

        prompt_msg = await call.message.answer("\n".join(text), parse_mode="HTML")

    await state.set_state(AdminTechStates.waiting_auto_hours)
    await state.update_data(
        tech_id=tech_id,
        auto_hours_msg_id=prompt_msg.message_id,  # запоминаем msg_id промпта
    )
    await call.answer()



async def admin_edit_tech_hours_finish(
    message: Message,
    state: FSMContext,
) -> None:
    """
    Принимаем строку с часами, сохраняем в Technician.
    После этого:
    - удаляем сообщение с промптом "Автонаправление для техника"
    - удаляем сообщение администратора с временем
    - открываем карточку техника с кнопками
    """
    data = await state.get_data()
    tech_id = data.get("tech_id")
    prompt_msg_id = data.get("auto_hours_msg_id")

    if not tech_id:
        await message.answer("❌ Неизвестный техник. Попробуйте ещё раз через меню.")
        await state.clear()
        return

    raw = (message.text or "").strip().lower()

    # Варианты отключения
    if raw in {"0", "off", "выкл", "нет"}:
        async with db_manager.session() as db:
            tech = await get_technician_by_id(session=db, tech_id=tech_id)
            if not tech:
                await message.answer("❌ Техник не найден.")
                await state.clear()
                return

            tech.is_auto_assign = False
            tech.auto_assign_start_hour = None
            tech.auto_assign_end_hour = None
            await db.commit()

            # Получаем статистику для карточки
            from app.db.crud.tech import get_technician_stats

            records, total_count, overall_avg = await get_technician_stats(
                session=db,
                tech_id=tech_id,
                limit=10,
                offset=0,
            )
            total_pages = max(1, (total_count + 10 - 1) // 10)

        # Удаляем "служебные" сообщения
        try:
            await message.delete()
        except Exception as e:
            logger.warning("Не удалось удалить сообщение с часами: %s", e)

        if prompt_msg_id:
            try:
                await message.bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=prompt_msg_id,
                )
            except Exception as e:
                logger.warning("Не удалось удалить промпт автонаправления: %s", e)

        # Формируем карточку
        text = _build_tech_stats_text(
            tech=tech,
            records=records,
            overall_avg=overall_avg,
            current_page=1,
            total_pages=total_pages,
        )
        text += "\n\n🛑 Автонаправление выключено."

        kb = admin_kb.get_technician_view_keyboard(
            tech_id=tech.id,
            stats_page=1,
            total_pages=total_pages,
        )

        await message.answer(text, reply_markup=kb, parse_mode="HTML")
        await state.clear()
        return

    # Парсим интервал
    parsed = _parse_hours_interval(raw)
    if not parsed:
        await message.answer(
            "❌ Не могу распознать время.\n"
            "Примеры: <code>9-18</code>, <code>09:00-18:00</code>\n"
            "Или отправьте <code>0</code> чтобы выключить.",
            parse_mode="HTML",
        )
        return

    start_t, end_t = parsed
    start_str = start_t.strftime("%H:%M")
    end_str = end_t.strftime("%H:%M")

    async with db_manager.session() as db:
        tech = await get_technician_by_id(session=db, tech_id=tech_id)
        if not tech:
            await message.answer("❌ Техник не найден.")
            await state.clear()
            return

        tech.is_auto_assign = True
        tech.auto_assign_start_hour = start_str
        tech.auto_assign_end_hour = end_str
        await db.commit()

        # Получаем статистику для карточки
        from app.db.crud.tech import get_technician_stats

        records, total_count, overall_avg = await get_technician_stats(
            session=db,
            tech_id=tech_id,
            limit=10,
            offset=0,
        )
        total_pages = max(1, (total_count + 10 - 1) // 10)

    # Удаляем "служебные" сообщения
    try:
        await message.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение с часами: %s", e)

    if prompt_msg_id:
        try:
            await message.bot.delete_message(
                chat_id=message.chat.id,
                message_id=prompt_msg_id,
            )
        except Exception as e:
            logger.warning("Не удалось удалить промпт автонаправления: %s", e)

    # Формируем карточку
    text = _build_tech_stats_text(
        tech=tech,
        records=records,
        overall_avg=overall_avg,
        current_page=1,
        total_pages=total_pages,
    )
    text += (
        "\n\n✅ Автонаправление включено.\n"
        f"Техник: <b>{tech.name}</b>\n"
        f"Часы: <code>{start_str}-{end_str}</code>"
    )

    kb = admin_kb.get_technician_view_keyboard(
        tech_id=tech.id,
        stats_page=1,
        total_pages=total_pages,
    )

    await message.answer(text, reply_markup=kb, parse_mode="HTML")
    await state.clear()



_HOURS_RE = re.compile(
    r"^\s*(\d{1,2})(?::?(\d{2}))?\s*-\s*(\d{1,2})(?::?(\d{2}))?\s*$"
)


def _parse_hours_interval(text: str) -> tuple[time, time] | None:
    """
    Парсим строку вида '9-18', '09-18', '09:00-18:30' → (time, time).
    Возвращаем None, если формат кривой.
    """
    m = _HOURS_RE.match(text)
    if not m:
        return None

    h1, m1, h2, m2 = m.groups()
    h1 = int(h1)
    h2 = int(h2)
    m1 = int(m1) if m1 is not None else 0
    m2 = int(m2) if m2 is not None else 0

    if not (0 <= h1 <= 23 and 0 <= h2 <= 23 and 0 <= m1 <= 59 and 0 <= m2 <= 59):
        return None

    try:
        t1 = time(hour=h1, minute=m1)
        t2 = time(hour=h2, minute=m2)
    except ValueError:
        return None

    if t1 == t2:
        # интервал нулевой длины — бессмысленно
        return None

    return t1, t2


async def admin_view_technician(call: CallbackQuery, state: FSMContext) -> None:
    """Просмотр информации о технике с статистикой."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        parts = call.data.split(":")
        tech_id = int(parts[1])
        page = int(parts[2]) if len(parts) > 2 else 1
    except Exception:
        await call.answer("❌ Некорректный ID техника.", show_alert=True)
        return

    async with db_manager.session() as db:
        from app.db.crud.tech import get_technician_stats

        tech = await get_technician_by_id(session=db, tech_id=tech_id)

        if not tech:
            await call.answer("❌ Техник не найден.", show_alert=True)
            return

        # Получаем статистику с пагинацией
        per_page = 10
        offset = (page - 1) * per_page

        records, total_count, overall_avg = await get_technician_stats(
            session=db,
            tech_id=tech_id,
            limit=per_page,
            offset=offset,
        )

        total_pages = max(1, (total_count + per_page - 1) // per_page)
        page = min(page, total_pages)  # Корректируем страницу

    # Формируем текст
    text = _build_tech_stats_text(
        tech=tech,
        records=records,
        overall_avg=overall_avg,
        current_page=page,
        total_pages=total_pages,
    )

    # Формируем клавиатуру
    kb = admin_kb.get_technician_view_keyboard(
        tech_id=tech.id,
        stats_page=page,
        total_pages=total_pages,
    )

    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")

    await call.answer()


async def admin_delete_tech_confirm(call: CallbackQuery, state: FSMContext) -> None:
    """Показать подтверждение удаления техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, tech_id_str = call.data.split(":", maxsplit=1)
        tech_id = int(tech_id_str)
    except Exception:
        await call.answer("❌ Некорректный ID техника.", show_alert=True)
        return

    async with db_manager.session() as db:
        tech = await get_technician_by_id(session=db, tech_id=tech_id)

    if not tech:
        await call.answer("❌ Техник не найден.", show_alert=True)
        return

    kb = admin_kb.get_technician_delete_confirm_keyboard(tech.id)

    text = (
        "⚠️ <b>Подтверждение удаления</b>\n\n"
        f"Вы уверены, что хотите удалить техника?\n\n"
        f"Имя: <b>{tech.name}</b>\n"
        f"Telegram ID: <code>{tech.tg_user_id}</code>\n\n"
        "❗️ Техник будет деактивирован и больше не будет отображаться в списке."
    )

    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")

    await call.answer()


async def admin_delete_tech_execute(call: CallbackQuery, state: FSMContext) -> None:
    """Выполнить удаление техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, tech_id_str = call.data.split(":", maxsplit=1)
        tech_id = int(tech_id_str)
    except Exception:
        await call.answer("❌ Некорректный ID техника.", show_alert=True)
        return

    async with db_manager.session() as db:

        tech = await get_technician_by_id(session=db, tech_id=tech_id)
        if not tech:
            await call.answer("❌ Техник не найден.", show_alert=True)
            return

        tech_name = tech.name
        success = await deactivate_technician_by_id(session=db, tech_id=tech_id)
        await db.commit()

    if success:
        logger.info("🗑 Техник удален (деактивирован): %s (ID: %s)", tech_name, tech_id)

        text, _, kb = await _load_technicians_text_and_kb()

        try:
            await call.message.edit_text(
                text + f"\n\n✅ Техник <b>{tech_name}</b> удален.",
                reply_markup=kb,
                parse_mode="HTML"
            )
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            await call.message.answer(
                f"✅ Техник <b>{tech_name}</b> удален.",
                parse_mode="HTML",
                reply_markup=kb,
            )

        await call.answer("✅ Техник удален", show_alert=False)
    else:
        await call.answer("❌ Ошибка при удалении техника.", show_alert=True)

async def admin_back_to_tech_menu(call: CallbackQuery, state: FSMContext) -> None:
    """Возврат в меню техников."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await _back_to_tech_menu(call, state)


async def admin_back_to_main_menu(call: CallbackQuery, state: FSMContext) -> None:
    """Возврат в главное меню админа."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return
    await state.clear()

    text = "😎 <b>АДМИН ПАНЕЛЬ</b>\n\nВыберите действие:"
    kb = admin_kb.get_main_menu_keyboard()

    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")
    await call.answer()

def _format_rating_stars(rating: float) -> str:
    """
    Преобразовать числовую оценку в звезды.

    Args:
        rating: Оценка от 0 до 5

    Returns:
        Строка со звездами
    """
    full_stars = int(rating)
    half_star = (rating - full_stars) >= 0.5
    empty_stars = 5 - full_stars - (1 if half_star else 0)

    result = "⭐️" * full_stars
    if half_star:
        result += "✨"
    result += "☆" * empty_stars

    return result


def _build_tech_stats_text(
    tech: Technician,
    records: list[dict],
    overall_avg: float,
    current_page: int,
    total_pages: int,
) -> str:
    """
    Сформировать текст карточки техника со статистикой.

    Args:
        tech: Техник
        records: Список записей статистики
        overall_avg: Общая средняя оценка
        current_page: Текущая страница
        total_pages: Всего страниц

    Returns:
        HTML-форматированный текст
    """
    status = "🟢 активен" if tech.is_active else "🔴 отключен"

    # Шапка карточки
    lines = [
        "👨‍🔧 <b>Карточка техника</b>",
        "",
        f"<b>Имя:</b> {tech.name}",
        f"<b>Telegram ID:</b> <code>{tech.tg_user_id}</code>",
        f"<b>Статус:</b> {status}",
        "",
    ]
    # Блок про автоназначение
    if tech.is_auto_assign:
        if tech.auto_assign_start_hour and tech.auto_assign_end_hour:
            auto_status = "🟢 включено"
            auto_hours = f"{tech.auto_assign_start_hour}–{tech.auto_assign_end_hour}"
        else:
            auto_status = "🟡 включено, но время не задано"
            auto_hours = "—"
    else:
        auto_status = "🔴 выключено"
        auto_hours = "—"

    lines.extend(
        [
            f"<b>Автонаправление:</b> {auto_status}",
            f"<b>Часы автоназначения:</b> <code>{auto_hours}</code>",
            "",
        ]
    )
    # Общая статистика
    if overall_avg > 0:
        stars = _format_rating_stars(overall_avg)
        lines.append(
            f"📊 <b>Средняя оценка:</b> {overall_avg:.2f}/5.0 {stars}"
        )
        lines.append("")
    else:
        lines.append("📊 <b>Отзывов пока нет</b>")
        lines.append("")

    # История оценок
    if records:
        lines.append(f"📋 <b>История оценок</b> (стр. {current_page}/{total_pages}):")
        lines.append("")

        for record in records:
            ticket_id = record["ticket_id"]
            avg_rating = record["avg_rating"]
            created_at = record["created_at"]

            stars = _format_rating_stars(avg_rating)
            date_str = created_at.strftime("%d.%m.%Y")

            # Оформляем через blockquote
            lines.append(
                f"<blockquote>"
                f"Тикет <b>#{ticket_id}</b> • {date_str}\n"
                f"Оценка: {avg_rating:.1f}/5.0 {stars}"
                f"</blockquote>"
            )
    else:
        if overall_avg == 0:
            lines.append("<i>История оценок пуста</i>")

    return "\n".join(lines)



async def admin_tech_page_navigation(call: CallbackQuery, state: FSMContext) -> None:
    """Навигация по страницам статистики техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    # Используем ту же функцию просмотра
    await admin_view_technician(call, state)


async def admin_edit_tech_name_start(call: CallbackQuery, state: FSMContext) -> None:
    """Начать изменение имени техника."""
    if not settings.is_admin(call.from_user.id):
        await call.answer("⛔ Нет прав", show_alert=True)
        return

    try:
        _, tech_id_str = call.data.split(":", maxsplit=1)
        tech_id = int(tech_id_str)
    except Exception:
        await call.answer("❌ Некорректный ID техника.", show_alert=True)
        return

    async with db_manager.session() as db:
        tech = await get_technician_by_id(session=db, tech_id=tech_id)

    if not tech:
        await call.answer("❌ Техник не найден.", show_alert=True)
        return

    # Сохраняем контекст
    await state.update_data(
        edit_tech_id=tech_id,
        edit_tech_menu_msg_id=call.message.message_id,
    )
    await state.set_state(AdminTechStates.waiting_new_name)

    kb = admin_kb.get_cancel_edit_keyboard(tech_id)

    text = (
        f"✏️ <b>Изменение имени техника</b>\n\n"
        f"Текущее имя: <b>{tech.name}</b>\n\n"
        f"Введите новое имя:"
    )

    try:
        await call.message.edit_text(text, reply_markup=kb, parse_mode="HTML")
    except Exception as e:
        logger.warning("Не удалось отредактировать сообщение: %s", e)
        await call.message.answer(text, reply_markup=kb, parse_mode="HTML")

    await call.answer()


async def admin_edit_tech_name_finish(msg: Message, state: FSMContext) -> None:
    """Завершить изменение имени техника."""
    if not settings.is_admin(msg.from_user.id):
        await msg.answer("⛔ Нет прав.")
        return

    new_name = (msg.text or "").strip()

    # Удаляем сообщение пользователя
    try:
        await msg.delete()
    except Exception as e:
        logger.warning("Не удалось удалить сообщение: %s", e)

    if len(new_name) < 2:
        await msg.answer("❌ Имя слишком короткое. Минимум 2 символа.")
        return

    if len(new_name) > 64:
        await msg.answer("❌ Имя слишком длинное. Максимум 64 символа.")
        return

    data = await state.get_data()
    tech_id = data.get("edit_tech_id")
    menu_msg_id = data.get("edit_tech_menu_msg_id")

    if not tech_id:
        await msg.answer("❌ Ошибка: техник не найден.")
        await state.clear()
        return

    async with db_manager.session() as db:
        from app.db.crud.tech import update_technician_name

        success = await update_technician_name(
            session=db,
            tech_id=tech_id,
            new_name=new_name,
        )
        await db.commit()

    if not success:
        await msg.answer("❌ Не удалось обновить имя техника.")
        await state.clear()
        return

    logger.info(f"✅ Имя техника #{tech_id} изменено на: {new_name}")

    await state.clear()

    # Возвращаемся к карточке техника
    async with db_manager.session() as db:
        from app.db.crud.tech import get_technician_stats

        tech = await get_technician_by_id(session=db, tech_id=tech_id)

        if not tech:
            await msg.answer("❌ Техник не найден.")
            return

        # Получаем статистику
        records, total_count, overall_avg = await get_technician_stats(
            session=db,
            tech_id=tech_id,
            limit=10,
            offset=0,
        )

        total_pages = max(1, (total_count + 10 - 1) // 10)

    # Формируем текст
    text = _build_tech_stats_text(
        tech=tech,
        records=records,
        overall_avg=overall_avg,
        current_page=1,
        total_pages=total_pages,
    )

    text += f"\n\n✅ Имя изменено на: <b>{new_name}</b>"

    kb = admin_kb.get_technician_view_keyboard(
        tech_id=tech.id,
        stats_page=1,
        total_pages=total_pages,
    )

    if menu_msg_id:
        try:
            await msg.bot.edit_message_text(
                chat_id=msg.chat.id,
                message_id=menu_msg_id,
                text=text,
                reply_markup=kb,
                parse_mode="HTML",
            )
        except Exception as e:
            logger.warning("Не удалось отредактировать сообщение: %s", e)
            await msg.answer(text, reply_markup=kb, parse_mode="HTML")
    else:
        await msg.answer(text, reply_markup=kb, parse_mode="HTML")

def register_handlers(dp: Dispatcher) -> None:
    """Регистрация всех обработчиков для админ-модуля техников."""
    # Открытие меню техников
    dp.callback_query.register(
        handle_admin_technicians_menu,
        F.data == "admin_technicians",
    )

    # Добавление техника
    dp.callback_query.register(
        admin_add_tech_start,
        F.data == "admin_add_tech",
    )
    dp.callback_query.register(
        admin_add_tech_contact_choice,
        F.data == "admin_add_tech_contact",
    )
    dp.callback_query.register(
        admin_add_tech_manual_choice,
        F.data == "admin_add_tech_manual",
    )

    # Контакт
    dp.message.register(
        admin_add_tech_contact_message,
        AdminTechStates.waiting_contact,
        F.content_type == ContentType.CONTACT,
    )

    # Вручную
    dp.message.register(
        admin_add_tech_manual_name,
        AdminTechStates.waiting_manual_name,
        F.text,
    )
    dp.message.register(
        admin_add_tech_manual_tg_id,
        AdminTechStates.waiting_manual_tg_id,
        F.text,
    )

    # Просмотр техника
    dp.callback_query.register(
        admin_view_technician,
        F.data.startswith("admin_tech:"),
    )

    # Удаление техника
    dp.callback_query.register(
        admin_delete_tech_confirm,
        F.data.startswith("admin_delete_tech:"),
    )
    dp.callback_query.register(
        admin_delete_tech_execute,
        F.data.startswith("admin_delete_tech_confirm:"),
    )

    # Навигация
    dp.callback_query.register(
        admin_back_to_tech_menu,
        F.data == "admin_back_to_tech_menu",
    )
    dp.callback_query.register(
        admin_back_to_main_menu,
        F.data == "admin_back_to_menu",
    )
    # Просмотр техника
    dp.callback_query.register(
        admin_view_technician,
        F.data.startswith("admin_tech:"),
    )

    # Пагинация статистики
    dp.callback_query.register(
        admin_tech_page_navigation,
        F.data.startswith("admin_tech_page:"),
    )

    # Изменение имени
    dp.callback_query.register(
        admin_edit_tech_name_start,
        F.data.startswith("admin_edit_tech_name:"),
    )

    dp.message.register(
        admin_edit_tech_name_finish,
        AdminTechStates.waiting_new_name,
        F.text,
    )
    # Кнопка "🕐 Часы автонаправления"
    dp.callback_query.register(
        admin_edit_tech_hours_start,
        F.data.startswith("admin_edit_tech_hours:"),
    )
    # Приём текста с часами
    dp.message.register(
        admin_edit_tech_hours_finish,
        AdminTechStates.waiting_auto_hours,
    )