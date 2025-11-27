from aiogram.fsm.state import StatesGroup, State

class FeedbackStates(StatesGroup):
    waiting_q1 = State()
    waiting_q2 = State()
    waiting_q3 = State()
    waiting_comment = State()
