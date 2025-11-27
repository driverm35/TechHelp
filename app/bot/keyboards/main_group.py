from __future__ import annotations
from aiogram.utils.keyboard import InlineKeyboardBuilder
from aiogram.types import InlineKeyboardMarkup

def technicians_kb(names: list[str]) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    for n in names:
        kb.button(text=f"Назначить: {n.title()}", callback_data=f"assign:{n.lower()}")
    kb.adjust(1)
    return kb.as_markup()

def close_ticket_kb(ticket_id: int) -> InlineKeyboardMarkup:
    kb = InlineKeyboardBuilder()
    kb.button(text="Закрыть тикет", callback_data=f"close:{ticket_id}")
    return kb.as_markup()


