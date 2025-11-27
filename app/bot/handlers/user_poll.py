# app/bot/handlers/user_poll.py
"""
Обработчик опросов для оценки работы технической поддержки.
Использует FSM состояния и inline-клавиатуру для сбора обратной связи.
"""
from __future__ import annotations

import logging
from aiogram import Dispatcher, F, Bot
from aiogram.fsm.context import FSMContext
from aiogram.fsm.state import StatesGroup, State
from aiogram.types import CallbackQuery, Message, InlineKeyboardMarkup, InlineKeyboardButton
from aiogram.enums import ChatType

from app.db.database import db_manager
from app.db.crud.ticket import create_feedback
from app.utils.cache import cache

logger = logging.getLogger(__name__)


# ─────────────────────────────────────────────
#  FSM Состояния
# ─────────────────────────────────────────────

class FeedbackStates(StatesGroup):
    """Состояния опроса обратной связи."""
    question_1 = State()
    question_2 = State()
    question_3 = State()
    question_4 = State()
    question_5 = State()
    waiting_comment = State()


# ─────────────────────────────────────────────
#  Тексты вопросов
# ─────────────────────────────────────────────

QUESTIONS = {
    1: "Насколько специалист был внимателен, корректен и готов отвечать на вопросы?",
    2: "Насколько понятно специалист объяснил проблему и варианты её решения?",
    3: "Насколько быстро была проведена диагностика и ремонт?",
    4: "Насколько хорошо принтер работает после вмешательства специалиста?",
    5: "Насколько вам понравилось, как специалист объяснил стоимость работ и деталей?",
}


# ─────────────────────────────────────────────
#  Клавиатуры
# ─────────────────────────────────────────────

def _get_rating_keyboard(question_num: int) -> InlineKeyboardMarkup:
    """
    Клавиатура для оценки (1-5 звезд + кнопка отказа).

    Args:
        question_num: Номер вопроса (1-5)

    Returns:
        Клавиатура с кнопками оценок
    """
    stars = ["⭐️", "⭐️⭐️", "⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️", "⭐️⭐️⭐️⭐️⭐️"]

    return InlineKeyboardMarkup(
        inline_keyboard=[
            # Ряд 1: 1 звезда
            [
                InlineKeyboardButton(
                    text=f"{stars[0]} 1",
                    callback_data=f"poll_rate:{question_num}:1"
                ),
            ],
            # Ряд 2: 2 звезды
            [
                InlineKeyboardButton(
                    text=f"{stars[1]} 2",
                    callback_data=f"poll_rate:{question_num}:2"
                ),
            ],
            # Ряд 3: 3 звезды
            [
                InlineKeyboardButton(
                    text=f"{stars[2]} 3",
                    callback_data=f"poll_rate:{question_num}:3"
                ),
            ],
            # Ряд 4: 4 звезды
            [
                InlineKeyboardButton(
                    text=f"{stars[3]} 4",
                    callback_data=f"poll_rate:{question_num}:4"
                ),
            ],
            # Ряд 5: 5 звезд
            [
                InlineKeyboardButton(
                    text=f"{stars[4]} 5",
                    callback_data=f"poll_rate:{question_num}:5"
                ),
            ],
            # Ряд 6: отказаться
            [
                InlineKeyboardButton(
                    text="❌ Отказаться от опроса",
                    callback_data="poll_decline"
                ),
            ],
        ]
    )


def _get_comment_keyboard() -> InlineKeyboardMarkup:
    """Клавиатура для этапа комментария."""
    return InlineKeyboardMarkup(
        inline_keyboard=[
            [
                InlineKeyboardButton(
                    text="✅ Пропустить комментарий",
                    callback_data="poll_skip_comment"
                ),
            ],
            [
                InlineKeyboardButton(
                    text="❌ Отменить опрос",
                    callback_data="poll_decline"
                ),
            ],
        ]
    )


# ─────────────────────────────────────────────
#  Вспомогательные функции
# ─────────────────────────────────────────────

async def _save_answer_to_cache(
    user_id: int,
    ticket_id: int,
    question_num: int,
    rating: int
) -> bool:
    """
    Сохранить ответ в кеш.

    Args:
        user_id: ID пользователя
        ticket_id: ID тикета
        question_num: Номер вопроса (1-5)
        rating: Оценка (1-5)

    Returns:
        True если успешно
    """
    cache_key = f"poll:{user_id}:{ticket_id}:q{question_num}"
    return await cache.set(cache_key, rating, expire=3600)  # 1 час


async def _get_answers_from_cache(
    user_id: int,
    ticket_id: int
) -> dict[str, int]:
    """
    Получить все ответы из кеша.

    Args:
        user_id: ID пользователя
        ticket_id: ID тикета

    Returns:
        Словарь с ответами {q1: rating, q2: rating, ...}
    """
    answers = {}
    for i in range(1, 6):
        cache_key = f"poll:{user_id}:{ticket_id}:q{i}"
        rating = await cache.get(cache_key)
        if rating is not None:
            answers[f"q{i}"] = int(rating)
    return answers


async def _clear_poll_cache(user_id: int, ticket_id: int) -> None:
    """Очистить кеш опроса."""
    for i in range(1, 6):
        cache_key = f"poll:{user_id}:{ticket_id}:q{i}"
        await cache.delete(cache_key)


async def _get_ticket_info_from_cache(user_id: int) -> dict | None:
    """Получить информацию о тикете из кеша."""
    cache_key = f"poll:{user_id}:ticket_info"
    return await cache.get(cache_key)


async def _save_ticket_info_to_cache(
    user_id: int,
    ticket_id: int,
    tech_id: int | None
) -> bool:
    """Сохранить информацию о тикете в кеш."""
    cache_key = f"poll:{user_id}:ticket_info"
    data = {"ticket_id": ticket_id, "tech_id": tech_id}
    return await cache.set(cache_key, data, expire=3600)


# ─────────────────────────────────────────────
#  Инициация опроса
# ─────────────────────────────────────────────

async def start_feedback_poll(
    bot: Bot,
    user_id: int,
    ticket_id: int,
    tech_id: int | None = None
) -> None:
    """
    Начать опрос обратной связи для клиента.

    Args:
        bot: Экземпляр бота
        user_id: Telegram ID клиента
        ticket_id: ID тикета
        tech_id: ID техника (может быть None)
    """
    try:
        # Сохраняем информацию о тикете в кеш
        await _save_ticket_info_to_cache(user_id, ticket_id, tech_id)

        # Отправляем первый вопрос
        text = (
            "📊 <b>Оценка работы поддержки</b>\n\n"
            f"<b>Вопрос 1/5</b>\n\n"
            f"{QUESTIONS[1]}"
        )

        await bot.send_message(
            chat_id=user_id,
            text=text,
            reply_markup=_get_rating_keyboard(1),
            parse_mode="HTML"
        )

        logger.info(f"✅ Опрос инициирован для пользователя {user_id}, тикет #{ticket_id}")

    except Exception as e:
        logger.error(f"❌ Ошибка инициации опроса: {e}")


# ─────────────────────────────────────────────
#  Обработчики ответов
# ─────────────────────────────────────────────

async def handle_rating(call: CallbackQuery, state: FSMContext, bot: Bot) -> None:
    """Обработка оценки по вопросу."""
    try:
        # Парсим callback_data: poll_rate:question_num:rating
        _, question_str, rating_str = call.data.split(":")
        question_num = int(question_str)
        rating = int(rating_str)

        # Получаем информацию о тикете
        ticket_info = await _get_ticket_info_from_cache(call.from_user.id)

        if not ticket_info:
            await call.message.edit_text(
                "❌ Сессия опроса истекла. Пожалуйста, начните заново.",
                reply_markup=None
            )
            await call.answer()
            return

        ticket_id = ticket_info["ticket_id"]

        # Сохраняем ответ в кеш
        await _save_answer_to_cache(
            call.from_user.id,
            ticket_id,
            question_num,
            rating
        )

        logger.info(
            f"📝 Ответ на вопрос {question_num}: {rating} "
            f"(пользователь {call.from_user.id}, тикет #{ticket_id})"
        )

        # Переходим к следующему вопросу или к комментарию
        if question_num < 5:
            next_question = question_num + 1
            text = (
                "📊 <b>Оценка работы поддержки</b>\n\n"
                f"<b>Вопрос {next_question}/5</b>\n\n"
                f"{QUESTIONS[next_question]}"
            )

            await call.message.edit_text(
                text=text,
                reply_markup=_get_rating_keyboard(next_question),
                parse_mode="HTML"
            )
        else:
            # Все 5 вопросов пройдены - предлагаем комментарий
            text = (
                "✅ <b>Спасибо за ваши оценки!</b>\n\n"
                "Хотите оставить комментарий или пожелание?\n\n"
                "Напишите его в следующем сообщении или нажмите "
                "<b>«Пропустить комментарий»</b>."
            )

            await call.message.edit_text(
                text=text,
                reply_markup=_get_comment_keyboard(),
                parse_mode="HTML"
            )

            # Устанавливаем состояние ожидания комментария
            await state.set_state(FeedbackStates.waiting_comment)
            await state.update_data(
                ticket_id=ticket_id,
                tech_id=ticket_info.get("tech_id"),
                # сохраняем message_id сообщения с опросом/клавой
                poll_message_id=call.message.message_id,
            )

        await call.answer()

    except Exception as e:
        logger.error(f"❌ Ошибка обработки оценки: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)


async def handle_decline(call: CallbackQuery, state: FSMContext) -> None:
    """Обработка отказа от опроса - немедленное закрытие."""
    try:
        # Получаем информацию о тикете
        ticket_info = await _get_ticket_info_from_cache(call.from_user.id)

        if ticket_info:
            # Очищаем кеш
            await _clear_poll_cache(call.from_user.id, ticket_info["ticket_id"])
            cache_key = f"poll:{call.from_user.id}:ticket_info"
            await cache.delete(cache_key)

        # Очищаем состояние
        await state.clear()

        # 🔹 УДАЛЯЕМ сообщение с опросом
        try:
            await call.message.delete()
            logger.info(f"✅ Сообщение опроса удалено для пользователя {call.from_user.id}")
        except Exception as e:
            logger.warning(f"⚠️ Не удалось удалить сообщение опроса: {e}")
            # Если не удалось удалить - редактируем
            await call.message.edit_text(
                "Вы отказались от опроса. Спасибо за обращение в поддержку! 👋",
                reply_markup=None
            )

        logger.info(f"ℹ️ Пользователь {call.from_user.id} отказался от опроса")

        await call.answer("Опрос отменен")

    except Exception as e:
        logger.error(f"❌ Ошибка обработки отказа: {e}")
        await call.answer()


async def handle_skip_comment(call: CallbackQuery, state: FSMContext) -> None:
    """Обработка пропуска комментария."""
    try:
        # Получаем данные из состояния
        data = await state.get_data()
        ticket_id = data.get("ticket_id")
        tech_id = data.get("tech_id")

        if not ticket_id:
            await call.message.edit_text(
                "❌ Ошибка: данные опроса не найдены.",
                reply_markup=None
            )
            await call.answer()
            return

        # Получаем ответы из кеша
        answers = await _get_answers_from_cache(call.from_user.id, ticket_id)

        if len(answers) != 5:
            await call.message.edit_text(
                "❌ Ошибка: не все вопросы отвечены.",
                reply_markup=None
            )
            await call.answer()
            return

        # Сохраняем в БД без комментария
        await _save_feedback_to_db(
            ticket_id=ticket_id,
            tech_id=tech_id,
            answers=answers,
            comment=None
        )

        # Очищаем кеш и состояние
        await _clear_poll_cache(call.from_user.id, ticket_id)
        cache_key = f"poll:{call.from_user.id}:ticket_info"
        await cache.delete(cache_key)
        await state.clear()

        await call.message.edit_text(
            "✅ <b>Спасибо за обратную связь!</b>\n\n"
            "Ваша оценка помогает нам становиться лучше! 🌟",
            reply_markup=None,
            parse_mode="HTML"
        )

        logger.info(f"✅ Опрос завершен без комментария (тикет #{ticket_id})")

        await call.answer()

    except Exception as e:
        logger.error(f"❌ Ошибка сохранения опроса: {e}")
        await call.answer("❌ Произошла ошибка", show_alert=True)


async def handle_comment(message: Message, state: FSMContext, bot: Bot) -> None:
    """Обработка комментария."""
    try:
        # Проверяем состояние
        current_state = await state.get_state()
        if current_state != FeedbackStates.waiting_comment:
            return

        # Получаем данные
        data = await state.get_data()
        ticket_id = data.get("ticket_id")
        tech_id = data.get("tech_id")
        poll_message_id = data.get("poll_message_id")  # 🆕 id сообщения с опросом

        if not ticket_id:
            await message.answer("❌ Ошибка: данные опроса не найдены.")
            return

        # Получаем ответы из кеша
        answers = await _get_answers_from_cache(message.from_user.id, ticket_id)

        if len(answers) != 5:
            await message.answer("❌ Ошибка: не все вопросы отвечены.")
            return

        # Получаем комментарий (ограничиваем 500 символов)
        comment = message.text.strip()[:500] if message.text else None

        # Сохраняем в БД с комментарием
        await _save_feedback_to_db(
            ticket_id=ticket_id,
            tech_id=tech_id,
            answers=answers,
            comment=comment
        )

        # Очищаем кеш и состояние
        await _clear_poll_cache(message.from_user.id, ticket_id)
        cache_key = f"poll:{message.from_user.id}:ticket_info"
        await cache.delete(cache_key)
        await state.clear()

        # 🧹 Пытаемся удалить сообщение с опросом/кнопками
        if poll_message_id:
            try:
                await bot.delete_message(
                    chat_id=message.chat.id,
                    message_id=poll_message_id,
                )
                logger.info(
                    f"✅ Сообщение опроса {poll_message_id} удалено "
                    f"в чате {message.chat.id}"
                )
            except Exception as e:
                logger.warning(
                    f"⚠️ Не удалось удалить сообщение опроса "
                    f"{poll_message_id} в чате {message.chat.id}: {e}"
                )

        # 🧹 Пытаемся удалить комментарий пользователя
        # В приватном чате Telegram, скорее всего, не даст удалить — будет тихий фейл.
        try:
            await bot.delete_message(
                chat_id=message.chat.id,
                message_id=message.message_id,
            )
            logger.info(
                f"✅ Сообщение с комментарием пользователя "
                f"{message.message_id} удалено в чате {message.chat.id}"
            )
        except Exception as e:
            # В ЛС это нормально — бот не имеет права удалять сообщения пользователя.
            logger.debug(
                f"ℹ️ Не удалось удалить комментарий пользователя "
                f"{message.message_id} в чате {message.chat.id}: {e}"
            )

        # Отправляем финальное сообщение "Спасибо..."
        await bot.send_message(
            chat_id=message.chat.id,
            text=(
                "✅ <b>Спасибо за обратную связь!</b>\n\n"
                "Ваша оценка и комментарий помогают нам становиться лучше! 🌟"
            ),
            parse_mode="HTML",
        )

        logger.info(f"✅ Опрос завершен с комментарием (тикет #{ticket_id})")

    except Exception as e:
        logger.error(f"❌ Ошибка обработки комментария: {e}")
        await message.answer("❌ Произошла ошибка при сохранении комментария.")



async def _save_feedback_to_db(
    ticket_id: int,
    tech_id: int | None,
    answers: dict[str, int],
    comment: str | None
) -> None:
    """Сохранить отзыв в базу данных."""
    async with db_manager.session() as db:
        try:
            await create_feedback(
                session=db,
                ticket_id=ticket_id,
                tech_id=tech_id,
                q1=answers.get("q1", 0),
                q2=answers.get("q2", 0),
                q3=answers.get("q3", 0),
                q4=answers.get("q4", 0),
                q5=answers.get("q5", 0),
                comment=comment
            )

            logger.info(
                f"💾 Отзыв сохранен в БД: тикет #{ticket_id}, "
                f"оценки: {answers}, комментарий: {'да' if comment else 'нет'}"
            )

        except Exception as e:
            logger.error(f"❌ Ошибка сохранения отзыва в БД: {e}")
            raise


# ─────────────────────────────────────────────
#  Регистрация обработчиков
# ─────────────────────────────────────────────

def register_handlers(dp: Dispatcher) -> None:
    logger.info("🔧 === НАЧАЛО регистрации обработчиков user_poll.py ===")

    # Обработка оценок
    dp.callback_query.register(
        handle_rating,
        F.data.startswith("poll_rate:"),
        F.message.chat.type == ChatType.PRIVATE,
    )

    # Обработка отказа
    dp.callback_query.register(
        handle_decline,
        F.data == "poll_decline",
        F.message.chat.type == ChatType.PRIVATE,
    )

    # Обработка пропуска комментария
    dp.callback_query.register(
        handle_skip_comment,
        F.data == "poll_skip_comment",
        F.message.chat.type == ChatType.PRIVATE,
    )

    # А тут всё норм — это Message, у него есть chat
    dp.message.register(
        handle_comment,
        FeedbackStates.waiting_comment,
        F.chat.type == ChatType.PRIVATE,
        F.text,
    )

    logger.info("✅ Зарегистрированы обработчики опросов")
    logger.info("🔧 === КОНЕЦ регистрации обработчиков user_poll.py ===")