from __future__ import annotations
import logging
from datetime import datetime

from aiogram import Router, Dispatcher
from aiogram.fsm.context import FSMContext
from aiogram.types import Message
from aiogram.filters import CommandStart


from app.config import settings
from app.db.crud.user import get_user_by_telegram_id, create_user
from app.db.database import db_manager
from app.bot.keyboards import admin_kb as admin_kb

from app.db.models import User

start_router = Router(name="start_router")
logger = logging.getLogger(__name__)


async def cmd_start(
    msg: Message,
    state: FSMContext,
    db_user: User | None = None,
) -> None:
    """
    Обработчик /start.

    - Берёт/создаёт пользователя в БД по tg_id
    - Обновляет username / first_name / last_name при изменении
    - Обновляет last_seen
    - Показывает меню для админа или юзера
    """
    logger.info(f"🚀 START: Обработка /start от {msg.from_user.id}")

    async with db_manager.session() as db:
        user = db_user or await get_user_by_telegram_id(db, msg.from_user.id)

        if user:
            logger.info(f"✅ Пользователь найден: {user.tg_id}")

            profile_updated = False

            # username без @
            new_username = msg.from_user.username.lstrip("@") if msg.from_user.username else None
            if user.username != new_username:
                old_username = user.username
                user.username = new_username
                logger.info(f"📝 Username обновлен: '{old_username}' → '{user.username}'")
                profile_updated = True

            if user.first_name != msg.from_user.first_name:
                old_first_name = user.first_name
                user.first_name = msg.from_user.first_name
                logger.info(f"📝 Имя обновлено: '{old_first_name}' → '{user.first_name}'")
                profile_updated = True

            if user.last_name != msg.from_user.last_name:
                old_last_name = user.last_name
                user.last_name = msg.from_user.last_name
                logger.info(f"📝 Фамилия обновлена: '{old_last_name}' → '{user.last_name}'")
                profile_updated = True

            # В новой модели есть last_seen, а не last_activity/updated_at
            user.last_seen = datetime.utcnow()

            # Если что-то реально меняли — можно сделать flush или commit
            await db.commit()
            await db.refresh(user)
            if profile_updated:
                logger.info(f"💾 Профиль пользователя {user.tg_id} обновлен")
        else:
            user = await create_user(
                db,
                telegram_id=msg.from_user.id,
                username=msg.from_user.username,
                first_name=msg.from_user.first_name,
                last_name=msg.from_user.last_name,
            )
            logger.info(f"🆕 Новый пользователь создан: {user.tg_id}")

        # Проверяем на админа по tg_id
        is_admin = settings.is_admin(user.tg_id)

    # Меню уже вне контекста БД
    if is_admin:
        menu_text = "😎 АДМИН ПАНЕЛЬ"
        reply_markup = admin_kb.get_main_menu_keyboard()
    else:
        menu_text = "Это техподдержка. Чем мы можем помочь?"
        reply_markup = None

    await msg.answer(
        menu_text,
        reply_markup=reply_markup,
        parse_mode="HTML",
    )

    await state.clear()


def register_handlers(dp: Dispatcher) -> None:
    logger.info("🔧 === НАЧАЛО регистрации обработчиков start.py ===")

    dp.message.register(
        cmd_start,
        CommandStart(),
    )
    logger.info("✅ Зарегистрирован cmd_start")

    logger.info("🔧 === КОНЕЦ регистрации обработчиков start.py ===")
