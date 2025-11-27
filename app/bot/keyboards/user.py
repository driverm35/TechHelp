from aiogram.types import InlineKeyboardMarkup
from aiogram.utils.keyboard import InlineKeyboardBuilder


def get_main_menu_keyboard() -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="🎫 Мои тикеты", callback_data="get_all_tickets")
    kb.button(text="➕ Создать тикет", callback_data="create_new_ticket")
    kb.adjust(1)
    return kb.as_markup()

def stars_kb(prefix: str) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for i in range(1,6):
        kb.button(text="⭐"*i, callback_data=f"{prefix}:{i}")
    kb.button(text="Отказаться", callback_data="cancel_feedback")
    kb.adjust(1)
    return kb.as_markup()