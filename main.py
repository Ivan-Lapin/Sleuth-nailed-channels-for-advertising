#!/usr/bin/env python3
"""
🚀 TG Ad Tracker PRO - НОВАЯ АРХИТЕКТУРА БД (3 таблицы)
✅ channel_stats: канал+бренд статистика
✅ daily_mentions: ежедневная история
✅ search_log: мониторинг лимитов
✅ Telegram Bot + Prometheus + Scheduler
"""

import asyncio
import sqlite3
import os
import signal
from datetime import date, datetime
from collections import defaultdict
from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import InputPeerEmpty
from telethon.tl.functions.channels import CheckSearchPostsFloodRequest, SearchPostsRequest
from telethon.errors import FloodWaitError
from prometheus_client import start_http_server, Counter, Gauge, Histogram
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiosqlite

# 🛠️ НАСТРОЙКИ
load_dotenv()
API_ID = int(os.getenv("TELEGRAM_API_ID", "31063618"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "64120db1d95785c5c4d2f61c8a1cc621")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = "data/tg_global_ads.db"
BRANDS = ["FOREO", "BORK", "Remez", "Dreame", "L&L Skin", "D'alba", "NFO"]
SEARCH_LIMIT = 50

# 📊 МЕТРИКИ PROMETHEUS
SEARCH_REQUESTS = Counter('tg_search_requests_total', 'Search requests', ['brand', 'status'])
RELEVANT_CHANNELS = Gauge('tg_relevant_channels_total', 'Relevant channels >5', ['brand'])
REMAINING_SEARCHES = Gauge('tg_remaining_searches', 'Remaining daily searches')
TOTAL_MENTIONS = Counter('tg_total_mentions', 'Total mentions found', ['brand'])
SEARCH_DURATION = Histogram('tg_search_duration_seconds', 'Search duration')

scheduler = AsyncIOScheduler()

# 🗄️ НОВАЯ АРХИТЕКТУРА БД (3 таблицы)
def init_db():
    """🚀 НОВАЯ структура БД"""
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)
    
    # 1. 📊 channel_stats: статистика канал+бренд
    conn.execute('''CREATE TABLE IF NOT EXISTS channel_stats (
        channel_id INTEGER,
        brand TEXT,
        title TEXT,
        username TEXT,
        total_mentions INTEGER DEFAULT 1,
        first_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        last_seen TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (channel_id, brand)
    )''')
    
    # 2. 📅 daily_mentions: ежедневные упоминания
    conn.execute('''CREATE TABLE IF NOT EXISTS daily_mentions (
        date TEXT,
        channel_id INTEGER,
        brand TEXT,
        message_id INTEGER,
        message_text TEXT,
        peer_username TEXT,
        PRIMARY KEY (date, channel_id, brand, message_id)
    )''')
    
    # 3. 📈 search_log: логи поиска
    conn.execute('''CREATE TABLE IF NOT EXISTS search_log (
        timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        brand TEXT,
        status TEXT,
        total_results INTEGER,
        remaining_limits INTEGER,
        duration_sec REAL,
        PRIMARY KEY (timestamp, brand)
    )''')
    
    # 🏎️ Индексы для скорости
    conn.execute('CREATE INDEX IF NOT EXISTS idx_channel_brand ON channel_stats(channel_id, brand)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_daily_date ON daily_mentions(date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_search_brand ON search_log(brand)')
    
    conn.commit()
    conn.close()
    print(f"✅ НОВАЯ БД готова: {DB_PATH}")

async def save_channel_mention(channel_id, title, brand, username, message_id, message_text=""):
    """✅ Сохранение в 2 таблицы"""
    today = date.today().strftime('%Y-%m-%d')
    
    async with aiosqlite.connect(DB_PATH) as conn:
        # 1. 📅 Ежедневное упоминание
        await conn.execute('''INSERT OR IGNORE INTO daily_mentions 
                            (date, channel_id, brand, message_id, message_text, peer_username)
                            VALUES (?, ?, ?, ?, ?, ?)''',
                         (today, channel_id, brand, message_id, message_text[:500], username))
        
        # 2. 📊 Обновляем статистику
        await conn.execute('''INSERT INTO channel_stats 
                            (channel_id, brand, title, username, total_mentions, last_seen)
                            VALUES (?, ?, ?, ?, 1, CURRENT_TIMESTAMP)
                            ON CONFLICT(channel_id, brand) DO UPDATE SET
                            total_mentions = total_mentions + 1,
                            last_seen = CURRENT_TIMESTAMP,
                            title = excluded.title,
                            username = excluded.username''',
                         (channel_id, brand, title, username))
        
        await conn.commit()

async def log_search(brand, status, total_results, remaining_limits, duration):
    """📈 Логирование поиска"""
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute('''INSERT INTO search_log 
                            (brand, status, total_results, remaining_limits, duration_sec)
                            VALUES (?, ?, ?, ?, ?)''',
                         (brand, status, total_results, remaining_limits, duration))
        await conn.commit()

def get_relevant_channels(brand_filter=None, min_mentions=5):
    """🎯 Релевантные каналы (>min_mentions)"""
    conn = sqlite3.connect(DB_PATH)
    if brand_filter:
        cursor = conn.execute(
            'SELECT channel_id, title, username, total_mentions, brand FROM channel_stats '
            'WHERE brand=? AND total_mentions >= ? ORDER BY total_mentions DESC LIMIT 20',
            (brand_filter, min_mentions)
        )
    else:
        cursor = conn.execute(
            'SELECT channel_id, title, username, total_mentions, brand FROM channel_stats '
            'WHERE total_mentions >= ? ORDER BY total_mentions DESC LIMIT 20',
            (min_mentions,)
        )
    result = cursor.fetchall()
    conn.close()
    return result

def get_stats():
    """📊 Полная статистика"""
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute('SELECT COUNT(*) FROM channel_stats').fetchone()[0]
    relevant = conn.execute('SELECT COUNT(*) FROM channel_stats WHERE total_mentions >= 5').fetchone()[0]
    today_mentions = conn.execute(
        'SELECT COUNT(*) FROM daily_mentions WHERE date = ?',
        (date.today().strftime('%Y-%m-%d'),)
    ).fetchone()[0]
    conn.close()
    return total, relevant, today_mentions

def get_brand_stats(brand):
    """📈 Статистика по бренду"""
    conn = sqlite3.connect(DB_PATH)
    total = conn.execute('SELECT COUNT(*) FROM channel_stats WHERE brand=?', (brand,)).fetchone()[0]
    relevant = conn.execute('SELECT COUNT(*) FROM channel_stats WHERE brand=? AND total_mentions >= 5', (brand,)).fetchone()[0]
    conn.close()
    return total, relevant

# 🔍 ГЛОБАЛЬНЫЙ ПОИСК
async def global_brand_search(client, brand):
    """🚀 Глобальный поиск + graceful лимиты"""
    SEARCH_REQUESTS.labels(brand=brand, status="start").inc()
    print(f"\n🔍 Глобальный поиск '{brand}'...")
    start_time = asyncio.get_event_loop().time()
    
    try:
        # ✅ GRACEFUL CHECK ЛИМИТОВ 
        flood_remains = 10 
        try:
            from telethon.tl.functions.channels import CheckSearchPostsFloodRequest
            flood = await client(CheckSearchPostsFloodRequest())
            flood_remains = flood.remains
            REMAINING_SEARCHES.set(flood.remains)
            print(f"   📊 Лимиты: {flood.remains}/{flood.total_daily}")
        except ImportError:
            print("   ⚠️ CheckSearchPostsFloodRequest недоступен, используем default=10")
        except Exception as e:
            print(f"   ⚠️ Лимиты недоступны: {e}, продолжаем...")
        
        if flood_remains <= 0:
            print("❌ Лимит исчерпан!")
            return 0, 0
        
        result = await client(SearchPostsRequest(
            query=brand,
            limit=SEARCH_LIMIT,
            offset_rate=0,
            offset_peer=InputPeerEmpty(),
            offset_id=0
        ))
        
        duration = asyncio.get_event_loop().time() - start_time
        SEARCH_DURATION.observe(duration)
        
        print(f"📋 {len(result.messages)} сообщений из {len(result.chats)} каналов")
        
        # ✅ Обработка 
        processed = 0
        for msg in result.messages[:20]:
            if hasattr(msg.peer_id, 'channel_id') and msg.peer_id.channel_id:
                chat = next((c for c in result.chats if c.id == msg.peer_id.channel_id), None)
                if chat:
                    processed += 1
                    print(f"💾 @{getattr(chat, 'username', 'private')} - {chat.title}")
                    await save_channel_mention(
                        chat.id, chat.title or '', brand,
                        getattr(chat, 'username', ''),
                        msg.id, msg.message or ''
                    )
        
        # ✅ Логирование
        await log_search(brand, 'success', len(result.chats), flood_remains, duration)
        TOTAL_MENTIONS.labels(brand=brand).inc(processed)
        
        total, relevant = get_brand_stats(brand)
        RELEVANT_CHANNELS.labels(brand=brand).set(relevant)
        SEARCH_REQUESTS.labels(brand=brand, status="success").inc()
        
        print(f"✅ {brand}: {total} каналов, {relevant} релевантных")
        return total, relevant
        
    except Exception as e:
        duration = asyncio.get_event_loop().time() - start_time
        await log_search(brand, 'error', 0, 0, duration)
        SEARCH_REQUESTS.labels(brand=brand, status="error").inc()
        print(f"❌ {brand}: {e}")
        return 0, 0


async def test_search():
    """🧪 Быстрый тест"""
    print("🧪 Тест FOREO...")
    init_db()
    
    async with TelegramClient('tg_session', API_ID, API_HASH) as client:
        await global_brand_search(client, "FOREO")
        total, relevant, today = get_stats()
        print(f"📈 {total} каналов, {relevant} релевантных")

# 🤖 TELEGRAM BOT
async def start_bot():
    """🎯 Полнофункциональный бот"""
    if not BOT_TOKEN:
        print("⚠️ BOT_TOKEN не найден")
        return
    
    try:
        from aiogram import Bot, Dispatcher
        from aiogram.filters import Command
        from aiogram.types import Message
        from aiogram.fsm.storage.memory import MemoryStorage
        
        bot = Bot(token=BOT_TOKEN)
        dp = Dispatcher(storage=MemoryStorage())
        
        @dp.message(Command("start"))
        async def start_cmd(message: Message):
            await message.answer(
                "🚀 TG Ad Tracker PRO\n\n"
                "/stats - полная статистика\n"
                "/relevant - топ каналов\n"
                "/brand FOREO - по бренду\n"
                "/growth - рост за день\n"
                "/logs - последние поиски"
            )
        
        @dp.message(Command("stats"))
        async def stats_cmd(message: Message):
            total, relevant, today = get_stats()
            text = f"""📊 СТАТИСТИКА:
📝 Всего: {total} каналов
🎯 Релевантных: {relevant} (>5 упоминаний)
🆕 Сегодня: {today} новых"""
            await message.answer(text)
        
        @dp.message(Command("relevant"))
        async def relevant_cmd(message: Message):
            channels = get_relevant_channels()
            if not channels:
                await message.answer("📭 Нет релевантных каналов")
                return
            
            text = "🎯 ТОП РЕЛЕВАНТНЫХ (>5 упоминаний):\n\n"
            for ch_id, title, username, count, brand in channels[:10]:
                text += f"@{username or 'private'} | {brand}\n"
                text += f"  {title[:40]}... x{count}\n\n"
            await message.answer(text)
        
        @dp.message(Command("brand"))
        async def brand_cmd(message: Message):
            brand = message.text.split()[-1] if len(message.text.split()) > 1 else "FOREO"
            total, relevant = get_brand_stats(brand)
            text = f"📊 {brand}:\n📝 {total} каналов\n🎯 {relevant} релевантных"
            await message.answer(text)
        
        @dp.message(Command("growth"))
        async def growth_cmd(message: Message):
            conn = sqlite3.connect(DB_PATH)
            today = date.today().strftime('%Y-%m-%d')
            growth = conn.execute(
                'SELECT brand, COUNT(*) FROM daily_mentions WHERE date=? GROUP BY brand ORDER BY COUNT(*) DESC LIMIT 5',
                (today,)
            ).fetchall()
            conn.close()
            
            text = f"📈 РОСТ СЕГОДНЯ ({today}):\n\n"
            for brand, count in growth:
                text += f"{brand}: +{count} упоминаний\n"
            await message.answer(text)
        
        print("🤖 Bot запущен!")
        await dp.start_polling(bot)
        
    except Exception as e:
        print(f"❌ Bot: {e}")

# 🚀 MAIN
async def main():
    SKIP_PROMETHEUS = os.getenv('SKIP_PROMETHEUS', '0') == '1'
    
    print("🚀 TG Ad Tracker PRO")
    print(f"📅 {date.today()}")
    
    init_db()
    if not SKIP_PROMETHEUS:
        start_http_server(8008)
        print("📊 http://localhost:8008/metrics")
    
    # Тест
    await test_search()
    
    # Production
    async with TelegramClient('tg_session', API_ID, API_HASH) as client:
        scheduler.start()
        asyncio.create_task(start_bot())
        
        try:
            await asyncio.Future()
        except KeyboardInterrupt:
            print("\n👋 Остановлен")
        finally:
            scheduler.shutdown()

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        print("\n👋 Завершён")
