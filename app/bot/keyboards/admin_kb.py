# app/bot/keyboards/admin_kb.py
from __future__ import annotations

from typing import Sequence

from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder

from app.db.models import Technician


def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    """Главное меню для админа."""
    builder = InlineKeyboardBuilder()

    builder.button(
        text="👨‍🔧 Техники",
        callback_data="admin_technicians",
    )

    builder.adjust(1)

    return builder.as_markup()


def get_technicians_menu_keyboard(
    technicians: Sequence[Technician],
) -> InlineKeyboardMarkup:
    """Клавиатура управления техниками."""
    builder = InlineKeyboardBuilder()

    # 1) Кнопка "Добавить техника"
    builder.button(
        text="➕ Добавить техника",
        callback_data="admin_add_tech",
    )

    # 2) Список техников
    for tech in technicians:
        builder.button(
            text=tech.name,
            callback_data=f"admin_tech:{tech.id}",
        )

    # 3) Кнопка "Назад"
    builder.button(
        text="⬅️ Назад",
        callback_data="admin_back_to_menu",
    )

    # Всё по одному в строке
    builder.adjust(1)

    return builder.as_markup()


def get_add_tech_method_keyboard() -> InlineKeyboardMarkup:
    """Выбор способа добавления техника."""
    builder = InlineKeyboardBuilder()

    builder.button(
        text="📎 Отправить контакт",
        callback_data="admin_add_tech_contact",
    )
    builder.button(
        text="⌨️ Добавить вручную",
        callback_data="admin_add_tech_manual",
    )
    builder.button(
        text="⬅️ Назад",
        callback_data="admin_back_to_tech_menu",
    )

    builder.adjust(1)

    return builder.as_markup()


def get_back_button_keyboard() -> InlineKeyboardMarkup:
    """Простая клавиатура с кнопкой 'Назад'."""
    builder = InlineKeyboardBuilder()

    builder.button(
        text="⬅️ Назад",
        callback_data="admin_back_to_tech_menu",
    )

    return builder.as_markup()


def get_technician_view_keyboard(
    tech_id: int,
    stats_page: int = 1,
    total_pages: int = 1,
) -> InlineKeyboardMarkup:
    """
    Клавиатура для просмотра техника.

    Args:
        tech_id: ID техника
        stats_page: Текущая страница статистики
        total_pages: Всего страниц
    """
    builder = InlineKeyboardBuilder()

    # Пагинация статистики (если больше 1 страницы)
    if total_pages > 1:
        nav_buttons = []

        # Кнопка "Назад" по страницам
        if stats_page > 1:
            nav_buttons.append(
                {
                    "text": "⬅️",
                    "callback_data": f"admin_tech_page:{tech_id}:{stats_page - 1}"
                }
            )

        # Индикатор страницы
        nav_buttons.append(
            {
                "text": f"{stats_page}/{total_pages}",
                "callback_data": "noop"
            }
        )

        # Кнопка "Вперед" по страницам
        if stats_page < total_pages:
            nav_buttons.append(
                {
                    "text": "➡️",
                    "callback_data": f"admin_tech_page:{tech_id}:{stats_page + 1}"
                }
            )

        # Добавляем ряд пагинации
        for btn in nav_buttons:
            builder.button(text=btn["text"], callback_data=btn["callback_data"])

        # Корректируем последний ряд
        builder.adjust(len(nav_buttons))

    # Кнопки управления
    builder.button(
        text="✏️ Изменить имя",
        callback_data=f"admin_edit_tech_name:{tech_id}",
    )
    builder.button(
        text="🗑 Удалить",
        callback_data=f"admin_delete_tech:{tech_id}",
    )
    builder.button(
        text="⬅️ Назад",
        callback_data="admin_back_to_tech_menu",
    )

    builder.adjust(1)
    return builder.as_markup()


def get_technician_delete_confirm_keyboard(tech_id: int) -> InlineKeyboardMarkup:
    """Клавиатура подтверждения удаления техника."""
    builder = InlineKeyboardBuilder()

    builder.button(
        text="✅ Да, удалить",
        callback_data=f"admin_delete_tech_confirm:{tech_id}",
    )
    builder.button(
        text="❌ Отмена",
        callback_data=f"admin_tech:{tech_id}",
    )

    builder.adjust(1)
    return builder.as_markup()


def get_cancel_edit_keyboard(tech_id: int) -> InlineKeyboardMarkup:
    """Клавиатура отмены редактирования."""
    builder = InlineKeyboardBuilder()

    builder.button(
        text="❌ Отмена",
        callback_data=f"admin_tech:{tech_id}",
    )

    return builder.as_markup()