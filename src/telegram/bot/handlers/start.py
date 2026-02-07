from aiogram import Router, F
from aiogram.types import Message, CallbackQuery
from src.telegram.bot.keyboards import brands_kb
from src.services.report_service import get_brand_list

router = Router()

@router.message(F.text.in_({"/start", "/menu"}))
async def cmd_start(message: Message, pool, settings):
    async with pool.acquire() as conn:
        brands = await get_brand_list(conn)
    await message.answer("Выберите бренд:", reply_markup=brands_kb(brands))

@router.callback_query(F.data == "nav:brands")
async def nav_brands(cb: CallbackQuery, pool, settings):
    async with pool.acquire() as conn:
        brands = await get_brand_list(conn)
    await cb.message.edit_text("Выберите бренд:", reply_markup=brands_kb(brands))
    await cb.answer()
