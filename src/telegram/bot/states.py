from aiogram.fsm.state import State, StatesGroup

class Flow(StatesGroup):
    waiting_date_range = State()  # ждём ввод интервала дат после выбора бренда
    waiting_reason = State()      # после нажатия mark ждём текст причины
