# /start, /help, /stats, /today
import aiosqlite
import datetime
from aiogram import F, Router
from aiogram.filters import Command
from aiogram.types import Message, CallbackQuery
router = Router()

# 1) /start — только приветствие + перечень команд
@router.message(Command("start"))
async def start_cmd(message: Message):
        await message.answer(
            "🚀 TG Ad Tracker\n\n"
            "Доступные команды:\n"
            "• /help – подробная инструкция\n"
            "• /stats – общая статистика и статистика по бренду\n"
            "• /detailed_stats_brand – подробная статистика по бренду\n"
            "• /detailed_stats_post – подробная статистика по одному посту\n"
            "• /today – посты, найденные сегодня"
        )

    # 2) /help — детальная инструкция
@router.message(Command("help"))
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
    # @dp.message(Command("stats"))
    # async def stats_cmd(message: Message):
    #     total_ch, total_br, relevant, today = get_stats()
    #     text = (
    #         "📊 ОБЩАЯ СТАТИСТИКА БАЗЫ:\n\n"
    #         f"1️⃣ Каналов в базе: {total_ch}\n"
    #         f"2️⃣ Брендов в базе: {total_br}\n"
    #         f"3️⃣ Постов в базе: {today} (за сегодня отдельно, общее число можно добавить позже)\n"
    #         f"4️⃣ Релевантных каналов (упоминаний > 2): {relevant}\n\n"
    #         "Теперь выберите бренд, чтобы посмотреть его статистику."
    #     )

    #     keyboard = InlineKeyboardMarkup(
    #         inline_keyboard=[
    #             [InlineKeyboardButton(text=brand, callback_data=f"statsbrand_{brand}")]
    #             for brand in BRANDS
    #         ]
    #     )
    #     await message.answer(text, reply_markup=keyboard)

    # @dp.callback_query(F.data.startswith("statsbrand_"))
    # async def stats_brand_callback(callback: CallbackQuery):
    #     brand = callback.data.replace("statsbrand_", "", 1)
    #     ch_cnt, posts_cnt, rel_cnt = get_brand_aggregate_stats(brand)

    #     text = (
    #         f"📊 СТАТИСТИКА ПО БРЕНДУ: {brand}\n\n"
    #         f"1️⃣ Каналов с этим брендом: {ch_cnt}\n"
    #         f"2️⃣ Постов с этим брендом: {posts_cnt}\n"
    #         f"3️⃣ Релевантных каналов (упоминаний > 2): {rel_cnt}\n"
    #     )

    #     await callback.message.edit_text(text)
    #     await callback.answer()
    
    
@router.callback_query(F.data.startswith("ad_"))
async def mark_ad_channel(callback: CallbackQuery):
        _, username, brand = callback.data.split("_", 2)
        marked_date = datetime.today().strftime('%Y-%m-%d')
        user_id = callback.from_user.id
        
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                '''INSERT INTO Advertising_Channels (username, channel_name, brand, marked_date, marked_by)
                SELECT ?, c.name, ?, ?, ? FROM Channels c WHERE c.username = ? OR c.ID_channel = ?''',
                (username, brand, marked_date, str(user_id), username, username)
            )
            await conn.commit()
        
        await callback.answer(f"✅ @{username} помечен как рекламный для {brand}")

@router.callback_query(F.data.startswith("ignore_"))
async def ignore_channel(callback: CallbackQuery):
        _, username, brand = callback.data.split("_", 2)
        
        async with aiosqlite.connect(DB_PATH) as conn:
            await conn.execute(
                "INSERT OR IGNORE INTO Blacklist (username, brand) VALUES (?, ?)",
                (username, brand)
            )
            await conn.commit()
        
        await callback.answer(f"❌ @{username} исключён из рекомендаций для {brand}")