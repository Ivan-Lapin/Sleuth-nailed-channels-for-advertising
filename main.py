#!/usr/bin/env python3
"""
🚀 TG Ad Tracker PRO - БД  + СТАТИСТИКА ЧЕРЕЗ TELEGRAM API
✅ БД: Posts, Channels, Brands, Channels_Brands
✅ БОТ: /start, /help, /stats, /detailed_stats_brand, /detailed_stats_post, /today
"""

import asyncio
import sqlite3
import os
import signal
from datetime import date, datetime

from dotenv import load_dotenv
from telethon import TelegramClient
from telethon.tl.types import InputPeerEmpty
from telethon.tl.functions.channels import CheckSearchPostsFloodRequest, SearchPostsRequest
from prometheus_client import start_http_server, Counter, Gauge
from apscheduler.schedulers.asyncio import AsyncIOScheduler
import aiosqlite

from aiogram import Bot, Dispatcher, F
from aiogram.filters import Command
from aiogram.types import Message, InlineKeyboardMarkup, InlineKeyboardButton, CallbackQuery
from aiogram.fsm.storage.memory import MemoryStorage

# 🛠️ НАСТРОЙКИ
load_dotenv()
API_ID = int(os.getenv("TELEGRAM_API_ID", "31063618"))
API_HASH = os.getenv("TELEGRAM_API_HASH", "64120db1d95785c5c4d2f61c8a1cc621")
BOT_TOKEN = os.getenv("BOT_TOKEN", "")
DB_PATH = "data/tg_global_ads.db"

BRANDS = ["FOREO", "Bork", "Remez", "Dreame", "L&L Skin", "D'alba", "NFO"]
SEARCH_LIMIT = 100

# 📊 МЕТРИКИ
SEARCH_REQUESTS = Counter('tg_search_requests_total', 'Search requests', ['brand', 'status'])
RELEVANT_CHANNELS = Gauge('tg_relevant_channels_total', 'Relevant channels >2', ['brand'])
REMAINING_SEARCHES = Gauge('tg_remaining_searches', 'Remaining daily searches')
TOTAL_MENTIONS = Counter('tg_total_mentions', 'Total mentions found', ['brand'])

scheduler = AsyncIOScheduler()
shutdown_event = asyncio.Event()


def _handle_signal():
    if not shutdown_event.is_set():
        shutdown_event.set()


# 🗄️ БД (4 таблицы)
def init_db():
    os.makedirs("data", exist_ok=True)
    conn = sqlite3.connect(DB_PATH)

    conn.execute('''CREATE TABLE IF NOT EXISTS Posts (
        ID_post TEXT PRIMARY KEY,      -- t.me/username/123
        ID_channel INTEGER,
        Brand_ID INTEGER,
        Date TEXT,
        message_text TEXT
    )''')

    conn.execute('''CREATE TABLE IF NOT EXISTS Channels (
        ID_channel INTEGER PRIMARY KEY,
        name TEXT,
        username TEXT
    )''')

    conn.execute('''CREATE TABLE IF NOT EXISTS Brands (
        ID_brand INTEGER PRIMARY KEY AUTOINCREMENT,
        name TEXT UNIQUE
    )''')

    conn.execute('''CREATE TABLE IF NOT EXISTS Channels_Brands (
        ID_channel INTEGER,
        ID_brand INTEGER,
        mention_count INTEGER DEFAULT 1,
        PRIMARY KEY (ID_channel, ID_brand)
    )''')

    conn.execute('CREATE INDEX IF NOT EXISTS idx_posts_channel ON Posts(ID_channel)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_posts_date ON Posts(Date)')
    conn.execute('CREATE INDEX IF NOT EXISTS idx_channels_brands ON Channels_Brands(ID_channel, ID_brand)')

    for brand in BRANDS:
        conn.execute("INSERT OR IGNORE INTO Brands (name) VALUES (?)", (brand,))

    conn.commit()
    conn.close()
    print(f"✅ Новая БД готова: {DB_PATH}")


async def get_or_create_channel_id(channel_id, name, username):
    async with aiosqlite.connect(DB_PATH) as conn:
        await conn.execute(
            "INSERT OR IGNORE INTO Channels (ID_channel, name, username) VALUES (?, ?, ?)",
            (channel_id, name, username),
        )
        await conn.commit()
        return channel_id


async def get_brand_id(brand_name):
    async with aiosqlite.connect(DB_PATH) as conn:
        cursor = await conn.execute("SELECT ID_brand FROM Brands WHERE name = ?", (brand_name,))
        result = await cursor.fetchone()
        return result[0] if result else None


async def save_channel_mention(channel_id, title, brand, username, message_id, message_text):
    """Сохранение поста и счётчиков в БД, ID_post = t.me/username/123"""
    channel_id_int = channel_id
    brand_id = await get_brand_id(brand)
    if not brand_id:
        return

    if username:
        post_id = f"t.me/{username}/{message_id}"
    else:
        post_id = f"t.me/c/{channel_id_int}/{message_id}"

    async with aiosqlite.connect(DB_PATH) as conn:
        await get_or_create_channel_id(channel_id_int, title, username)

        # при сохранении
        db_today = date.today().strftime('%Y-%m-%d')

        await conn.execute(
            '''INSERT OR IGNORE INTO Posts
            (ID_post, ID_channel, Brand_ID, Date, message_text)
            VALUES (?, ?, ?, ?, ?)''',
            (post_id, channel_id_int, brand_id, db_today, message_text[:500]),
        )


        await conn.execute(
            '''INSERT INTO Channels_Brands (ID_channel, ID_brand, mention_count)
               VALUES (
                   ?, ?,
                   COALESCE(
                       (SELECT mention_count + 1
                        FROM Channels_Brands
                        WHERE ID_channel = ? AND ID_brand = ?),
                       1
                   )
               )
               ON CONFLICT(ID_channel, ID_brand) DO UPDATE SET
                   mention_count = Channels_Brands.mention_count + 1
            ''',
            (channel_id_int, brand_id, channel_id_int, brand_id),
        )

        await conn.commit()


def get_relevant_channels(brand_name=None, min_mentions=3):
    conn = sqlite3.connect(DB_PATH)
    if brand_name:
        cursor = conn.execute(
            '''
            SELECT c.ID_channel, c.name, c.username, cb.mention_count, b.name as brand
            FROM Channels_Brands cb
            JOIN Channels c ON cb.ID_channel = c.ID_channel
            JOIN Brands b ON cb.ID_brand = b.ID_brand
            WHERE b.name = ? AND cb.mention_count >= ?
            ORDER BY cb.mention_count DESC LIMIT 20
            ''',
            (brand_name, min_mentions),
        )
    else:
        cursor = conn.execute(
            '''
            SELECT c.ID_channel, c.name, c.username, cb.mention_count, b.name as brand
            FROM Channels_Brands cb
            JOIN Channels c ON cb.ID_channel = c.ID_channel
            JOIN Brands b ON cb.ID_brand = b.ID_brand
            WHERE cb.mention_count >= ?
            ORDER BY cb.mention_count DESC LIMIT 20
            ''',
            (min_mentions,),
        )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_relevant_posts_for_brand(brand_name: str, min_mentions: int = 3, limit: int = 30):
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.execute(
        '''
        SELECT
            p.ID_post,
            c.ID_channel,
            c.name,
            c.username,
            b.name
        FROM Channels_Brands cb
        JOIN Brands b   ON cb.ID_brand = b.ID_brand
        JOIN Channels c ON cb.ID_channel = c.ID_channel
        JOIN Posts p    ON p.ID_channel = c.ID_channel AND p.Brand_ID = b.ID_brand
        WHERE b.name = ? AND cb.mention_count >= ?
        ORDER BY cb.mention_count DESC, p.Date DESC
        LIMIT ?
        ''',
        (brand_name, min_mentions, limit),
    )
    rows = cursor.fetchall()
    conn.close()
    return rows


def get_stats():
    conn = sqlite3.connect(DB_PATH)
    db_today = date.today().strftime('%Y-%m-%d')
    total_channels = conn.execute('SELECT COUNT(DISTINCT ID_channel) FROM Channels').fetchone()[0]
    total_brands = conn.execute('SELECT COUNT(*) FROM Brands').fetchone()[0]
    relevant = conn.execute('SELECT COUNT(*) FROM Channels_Brands WHERE mention_count > 2').fetchone()[0]
    today_posts = conn.execute(
        'SELECT COUNT(*) FROM Posts WHERE Date = ?', db_today).fetchone()[0]
    conn.close()
    return total_channels, total_brands, relevant, today_posts


# 📈 Статистика поста через Telegram API
async def get_post_stats_via_telegram(username: str, message_id: int) -> str:
    client = TelegramClient('full_stats', API_ID, API_HASH)
    await client.start()
    try:
        channel = await client.get_entity(f"@{username}")
        message = await client.get_messages(channel, ids=message_id)

        if not message or not getattr(message, "views", None):
            return "❌ Пост не найден или статистика недоступна"

        lines = []
        lines.append(f"📱 Пост #{message.id} в @{username}")
        lines.append("")
        lines.append(f"👁 Просмотры: {message.views}")
        lines.append(f"🔄 Пересылки: {message.forwards}")

        if getattr(message, "reactions", None) and message.reactions.results:
            lines.append(f"👍 Реакции ({message.reactions.results_count}):")
            for reaction in message.reactions.results:
                emoji = getattr(reaction.reaction, "emoticon", str(reaction.reaction))
                lines.append(f"   {emoji}: {reaction.count}")
        else:
            lines.append("👍 Реакции: 0")

        replies_count = getattr(message.replies, "replies", 0) if message.replies else 0
        lines.append(f"💬 Комментарии: {replies_count}")

        text = message.text or ""
        preview = text[:100] + ("..." if len(text) > 100 else "")
        lines.append(f"📝 Текст ({len(text)} символов): {preview}")

        age_hours = (datetime.now(message.date.tzinfo) - message.date).total_seconds() / 3600
        dt = message.date  # datetime
        date_str = dt.strftime('%d.%m.%Y')
        time_str = dt.strftime('%H:%M')

        lines.append(f"⏰ Дата: {date_str}")
        lines.append(f"🕒 Время: {time_str}")
        lines.append(f"   Время жизни: {age_hours:.1f} ч")

        return "\n".join(lines)
    finally:
        await client.disconnect()



# 🔍 Глобальный поиск
async def global_brand_search(client, brand):
    SEARCH_REQUESTS.labels(brand=brand, status="start").inc()
    print(f"\n🔍 Глобальный поиск '{brand}'...")
    try:
        flood_remains = 10
        try:
            flood = await client(CheckSearchPostsFloodRequest())
            flood_remains = flood.remains
            REMAINING_SEARCHES.set(flood.remains)
            print(f"   📊 Лимиты: {flood.remains}/{flood.total_daily}")
        except Exception:
            print("   ⚠️ Лимиты недоступны, продолжаем...")

        if flood_remains <= 0:
            print("❌ Лимит исчерпан!")
            return 0, 0

        result = await client(
            SearchPostsRequest(
                query=brand,
                limit=SEARCH_LIMIT,
                offset_rate=0,
                offset_peer=InputPeerEmpty(),
                offset_id=0,
            )
        )

        print(f"📋 {len(result.messages)} сообщений из {len(result.chats)} каналов")

        processed = 0
        for msg in result.messages:
            if hasattr(msg.peer_id, "channel_id") and msg.peer_id.channel_id:
                chat = next((c for c in result.chats if c.id == msg.peer_id.channel_id), None)
                if chat:
                    processed += 1
                    print(f"💾 @{getattr(chat, 'username', 'private')} - {chat.title}")
                    await save_channel_mention(
                        chat.id,
                        chat.title or "",
                        brand,
                        getattr(chat, "username", ""),
                        msg.id,
                        msg.message or "",
                    )

        TOTAL_MENTIONS.labels(brand=brand).inc(processed)
        SEARCH_REQUESTS.labels(brand=brand, status="success").inc()
        print(f"✅ {brand}: {processed} обработано")
        return processed, len(get_relevant_channels(brand))
    except Exception as e:
        SEARCH_REQUESTS.labels(brand=brand, status="error").inc()
        print(f"❌ {brand}: {e}")
        return 0, 0


def get_brand_aggregate_stats(brand_name: str):
    """Статистика по бренду для /stats (после выбора бренда)."""
    conn = sqlite3.connect(DB_PATH)
    row = conn.execute(
        '''
        SELECT
            COUNT(DISTINCT c.ID_channel) AS channels_cnt,
            COUNT(DISTINCT p.ID_post)    AS posts_cnt,
            SUM(CASE WHEN cb.mention_count > 2 THEN 1 ELSE 0 END) AS relevant_channels_cnt
        FROM Channels_Brands cb
        JOIN Brands   b ON cb.ID_brand = b.ID_brand
        JOIN Channels c ON cb.ID_channel = c.ID_channel
        JOIN Posts    p ON p.ID_channel = c.ID_channel AND p.Brand_ID = b.ID_brand
        WHERE b.name = ?
        ''',
        (brand_name,)
    ).fetchone()
    conn.close()
    if not row:
        return 0, 0, 0
    return row  # (channels_cnt, posts_cnt, relevant_channels_cnt)


def get_detailed_brand_channels(brand_name: str, min_mentions: int = 2, max_posts_per_channel: int = 10):
    """
    Для /detailed_stats_brand:
    возвращает список:
    [
      (channel_name, username, mention_count, [(post_id, date, text_preview), ...]),
      ...
    ]
    только по релевантным каналам (mention_count > min_mentions).
    """
    conn = sqlite3.connect(DB_PATH)
    # получаем релевантные каналы
    channels = conn.execute(
        '''
        SELECT c.ID_channel, c.name, c.username, cb.mention_count
        FROM Channels_Brands cb
        JOIN Brands   b ON cb.ID_brand = b.ID_brand
        JOIN Channels c ON cb.ID_channel = c.ID_channel
        WHERE b.name = ? AND cb.mention_count > ?
        ORDER BY cb.mention_count DESC
        ''',
        (brand_name, min_mentions)
    ).fetchall()

    result = []
    for ch_id, ch_name, username, mention_count in channels:
        posts = conn.execute(
            '''
            SELECT ID_post, Date, message_text
            FROM Posts
            WHERE ID_channel = ?
              AND Brand_ID = (SELECT ID_brand FROM Brands WHERE name = ?)
            ORDER BY Date ASC
            LIMIT ?
            ''',
            (ch_id, brand_name, max_posts_per_channel)
        ).fetchall()
        result.append((ch_name, username, mention_count, posts))

    conn.close()
    return result



# 🤖 Бот
async def start_bot():
    
    if not BOT_TOKEN:
        print("⚠️ BOT_TOKEN не найден")
        return

    bot = Bot(token=BOT_TOKEN)
    dp = Dispatcher(storage=MemoryStorage())

    # 1) /start — только приветствие + перечень команд
    @dp.message(Command("start"))
    async def start_cmd(message: Message):
        await message.answer(
            "🚀 TG Ad Tracker\n\n"
            "Доступные команды:\n"
            "• /help – подробная инструкция\n"
            "• /stats – общая статистика и статистика по бренду\n"
            "• /detailed_stats_brand – подробная статистика по бренду (каналы + посты)\n"
            "• /detailed_stats_post – подробная статистика по одному посту\n"
            "• /today – посты, найденные сегодня"
        )

    # 2) /help — детальная инструкция
    @dp.message(Command("help"))
    async def help_cmd(message: Message):
        text = (
            "ℹ️ Инструкция по TG Ad Tracker:\n\n"
            "1. Сервис регулярно выполняет глобальный поиск по брендам и сохраняет:\n"
            "   • каналы (Channels),\n"
            "   • посты (Posts),\n"
            "   • связи канал–бренд с количеством упоминаний (Channels_Brands).\n\n"
            "2. Релевантный канал — это канал, где бренд упоминается более 2 раз.\n\n"
            "Команды:\n"
            "• /stats – показывает общую статистику по всей базе и позволяет выбрать бренд,\n"
            "  после выбора бренда показывает статистику по этому бренду.\n"
            "• /detailed_stats_brand – позволяет выбрать бренд и увидеть по нему список\n"
            "  релевантных каналов с постами (до 10 постов на канал).\n"
            "• /detailed_stats_post – вы вводите @ник канала и ID поста, бот показывает\n"
            "  подробную статистику этого поста по данным Telegram.\n"
            "• /today – выводит последние посты, найденные сегодня.\n"
        )
        await message.answer(text)

    # 3) /stats — общая статистика + выбор бренда -> агрегаты по бренду
    @dp.message(Command("stats"))
    async def stats_cmd(message: Message):
        total_ch, total_br, relevant, today = get_stats()
        text = (
            "📊 ОБЩАЯ СТАТИСТИКА БАЗЫ:\n\n"
            f"1️⃣ Каналов в базе: {total_ch}\n"
            f"2️⃣ Брендов в базе: {total_br}\n"
            f"3️⃣ Постов в базе: {today} (за сегодня отдельно, общее число можно добавить позже)\n"
            f"4️⃣ Релевантных каналов (упоминаний > 2): {relevant}\n\n"
            "Теперь выберите бренд, чтобы посмотреть его статистику."
        )

        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=brand, callback_data=f"statsbrand_{brand}")]
                for brand in BRANDS
            ]
        )
        await message.answer(text, reply_markup=keyboard)

    @dp.callback_query(F.data.startswith("statsbrand_"))
    async def stats_brand_callback(callback: CallbackQuery):
        brand = callback.data.replace("statsbrand_", "", 1)
        ch_cnt, posts_cnt, rel_cnt = get_brand_aggregate_stats(brand)

        text = (
            f"📊 СТАТИСТИКА ПО БРЕНДУ: {brand}\n\n"
            f"1️⃣ Каналов с этим брендом: {ch_cnt}\n"
            f"2️⃣ Постов с этим брендом: {posts_cnt}\n"
            f"3️⃣ Релевантных каналов (упоминаний > 2): {rel_cnt}\n"
        )

        await callback.message.edit_text(text)
        await callback.answer()

    # 4) /detailed_stats_brand — выбор бренда -> список релевантных каналов + посты
    @dp.message(Command("detailed_stats_brand"))
    async def detailed_stats_brand_cmd(message: Message):
        keyboard = InlineKeyboardMarkup(
            inline_keyboard=[
                [InlineKeyboardButton(text=brand, callback_data=f"detbrand_{brand}")]
                for brand in BRANDS
            ]
        )
        await message.answer("🔍 Выберите бренд для подробной статистики:", reply_markup=keyboard)

    @dp.callback_query(F.data.startswith("detbrand_"))
    async def detailed_brand_callback(callback: CallbackQuery):
        brand = callback.data.replace("detbrand_", "", 1)
        channels = get_detailed_brand_channels(brand, min_mentions=2, max_posts_per_channel=10)

        if not channels:
            await callback.message.edit_text(f"📭 Для бренда {brand} пока нет релевантных каналов")
            await callback.answer()
            return

        lines = [f"📊 ПОДРОБНАЯ СТАТИСТИКА ПО БРЕНДУ {brand}:\n"]
        for ch_idx, (ch_name, username, mention_count, posts) in enumerate(channels, start=1):
            
            lines.append(
                f"{ch_idx}) @{username or 'private'} — {ch_name} (x{mention_count})"
            )
            if not posts:
                lines.append("   • Постов не найдено.")
                continue

            for p_id, p_date, p_text in posts:
                preview = (p_text or "")[:50].replace("\n", " ")
                lines.append(f"   • {p_date} | ID: {p_id} | {preview}…")

            lines.append("") 

        await callback.message.edit_text("\n".join(lines))
        await callback.answer()


    # 5) /detailed_stats_post — пользователь вводит @ник и id поста, бот даёт статистику
    @dp.message(Command("detailed_stats_post"))
    async def detailed_stats_post_cmd(message: Message):
        await message.answer(
            "✏️ Отправьте в одном сообщении ник и ID поста в формате:\n"
            "`@channel_username 12345`\n"
            "Пример: `@diamo_tutto_per_scontato 805889`",
            parse_mode="Markdown"
        )

    @dp.message(F.text.regexp(r"^@[\w\d_]+\s+\d+$"))
    async def detailed_stats_post_input(message: Message):
        # Парсим строку вида "@username 12345"
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("❌ Формат неверный. Пример: `@mychannel 12345`", parse_mode="Markdown")
            return

        username = parts[0].lstrip("@")
        msg_id = int(parts[1])

        stats = await get_post_stats_via_telegram(username, msg_id)
        await message.answer(stats)

    # 6) /today — как раньше, просто список постов за сегодня
    @dp.message(Command("today"))
    async def today_cmd(message: Message):
        today = date.today().strftime('%d.%m.%Y')
        conn = sqlite3.connect(DB_PATH)
        posts = conn.execute(
            'SELECT p.ID_post, c.name, c.username, b.name FROM Posts p '
            'JOIN Channels c ON p.ID_channel = c.ID_channel '
            'JOIN Brands b ON p.Brand_ID = b.ID_brand '
            'WHERE p.Date = ? ORDER BY p.ID_post DESC LIMIT 10',
            (today,),
        ).fetchall()
        conn.close()

        if not posts:
            await message.answer("📅 Сегодня постов нет")
            return

        text = f"🆕 ПОСТЫ ЗА СЕГОДНЯ ({today}):\n\n"
        for post_id, name, username, brand in posts:
            text += f"@{username or 'private'} ({brand})\n"
            text += f"   {name[:40]}...\n"
            text += f"   🔗 {post_id}\n\n"
        await message.answer(text)
        
    async def _runner():
        try:
            print("🤖 Bot запущен!")
            await dp.start_polling(bot)
        finally:
            await bot.session.close()

    # Запускаем polling в отдельной задаче и ждём shutdown_event
    polling_task = asyncio.create_task(_runner())

    await shutdown_event.wait()           # ждём Ctrl+C / SIGTERM
    print("🔻 Останавливаем бота...")

    polling_task.cancel()
    try:
        await polling_task
    except asyncio.CancelledError:
        pass

    print("✅ Бот остановлен")



# 🚀 MAIN
async def main():
    print("🚀 TG Ad Tracker PRO - Новая БД")
    init_db()
    start_http_server(8000)
    print("📊 http://localhost:8000/metrics")

    loop = asyncio.get_running_loop()
    for s in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(s, _handle_signal)

    async with TelegramClient('tg_session', API_ID, API_HASH) as client:
        print("✅ Telegram подключен!")

        # первоначальный прогон по брендам
        # for brand in BRANDS:
        #     await global_brand_search(client, brand)
        #     await asyncio.sleep(5)

        # бот отдельной задачей
        bot_task = asyncio.create_task(start_bot())

        # ждём сигнала завершения
        await shutdown_event.wait()
        print("🛑 Получен сигнал завершения...")

        # дожидаемся остановки бота (start_bot сам слушает shutdown_event)
        try:
            await bot_task
        except asyncio.CancelledError:
            pass

    print("👋 Завершено корректно.")



if __name__ == "__main__":
    asyncio.run(main())
