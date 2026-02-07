# /detailed_stats_post
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message
from telethon_client import get_post_stats
router = Router()

# 5) /detailed_stats_post — пользователь вводит @ник и id поста, бот даёт статистику
@router.message(Command("detailed_stats_post"))
async def detailed_stats_post_cmd(message: Message):
        await message.answer(
            "✏️ Отправьте в одном сообщении ник и ID поста в формате:\n"
            "`@channel_username 12345`\n"
            "Пример: `@diamo_tutto_per_scontato 805889`",
            parse_mode=None
        )

@router.message(F.text.regexp(r"^@[\w\d_]+\s+\d+$"))
async def detailed_stats_post_input(message: Message):
        # Парсим строку вида "@username 12345"
        parts = message.text.split()
        if len(parts) != 2 or not parts[1].isdigit():
            await message.answer("❌ Формат неверный. Пример: `@mychannel 12345`", parse_mode=None)
            return

        username = parts[0].lstrip("@")
        msg_id = int(parts[1])

        stats = await get_post_stats(username, msg_id)
        await message.answer(stats)

    # 6) /today — как раньше, просто список постов за сегодня
@router.message(Command("today"))
async def today_cmd(message: Message):
        db_today = date.today().strftime('%Y-%m-%d')
        display_today = date.today().strftime('%d.%m.%Y')

        conn = sqlite3.connect(DB_PATH)
        posts = conn.execute(
            'SELECT p.ID_post, c.name, c.username, b.name, p.Date FROM Posts p '
            'JOIN Channels c ON p.ID_channel = c.ID_channel '
            'JOIN Brands b ON p.Brand_ID = b.ID_brand '
            'WHERE p.Date = ? ORDER BY p.ID_post DESC LIMIT 10',
            (db_today,),
        ).fetchall()
        conn.close()

        if not posts:
            await message.answer("📅 Сегодня постов нет")
            return

        text = f"🆕 ПОСТЫ ЗА СЕГОДНЯ ({display_today}):\n\n"
        for post_id, name, username, brand, p_date in posts:
            try:
                y, m, d = p_date.split("-")
                display_date = f"{d}.{m}.{y}"
            except Exception:
                display_date = p_date

            text += f"{display_date} @{username or 'private'} ({brand})\n"
            text += f"   {name[:40]}...\n"
            text += f"   🔗 {post_id}\n\n"

        await message.answer(text)