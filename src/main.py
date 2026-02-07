#!/usr/bin/env python3
"""
🚀 TG Ad Tracker PRO - БД  + СТАТИСТИКА ЧЕРЕЗ TELEGRAM API
✅ БД: Posts, Channels, Brands, Channels_Brands
✅ БОТ: /start, /help, /stats, /detailed_stats_brand, /detailed_stats_post, /today
"""
import asyncio
from aiogram import Bot, Dispatcher
from config import BOT_TOKEN
from database.db import init_db
from handlers.commands import router as commands_router
from handlers.brand import router as brand_router
from handlers.post_stats import router as post_router
from prometheus_client import start_http_server

async def main():
    await init_db()
    print("🚀 TG Ad Tracker PRO - Новая БД")
    start_http_server(8000)
    
    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher()
    
    # ✅ Подключаем роутеры
    dp.include_router(commands_router)
    dp.include_router(brand_router)
    dp.include_router(post_router)
    
    print("🤖 Bot запущен!")
    await dp.start_polling(bot)

if __name__ == "__main__":
    asyncio.run(main())


# import asyncio
# import sqlite3
# import os
# import signal
# from datetime import date, datetime

# from dotenv import load_dotenv
# from telethon import TelegramClient
# from telethon.tl.types import InputPeerEmpty
# from telethon.tl.functions.channels import CheckSearchPostsFloodRequest, SearchPostsRequest
# from telethon.tl.functions.messages import GetMessagesViewsRequest

# from prometheus_client import start_http_server, Counter, Gauge
# from apscheduler.schedulers.asyncio import AsyncIOScheduler
# import aiosqlite

# from aiogram import Bot, Dispatcher, F
# from aiogram.filters import Command
# from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
# from aiogram.fsm.storage.memory import MemoryStorage

