from aiogram import Bot, Dispatcher
from aiogram.fsm.storage.memory import MemoryStorage

from src.telegram.bot.handlers.start import router as start_router
from src.telegram.bot.handlers.browse import router as browse_router
from src.telegram.bot.handlers.mark import router as mark_router

def build_bot_app(bot_token: str):
    bot = Bot(token=bot_token)
    dp = Dispatcher(storage=MemoryStorage())
    dp.include_router(start_router)
    dp.include_router(browse_router)
    dp.include_router(mark_router)
    return bot, dp
