# ⌨️ Все клавиатуры
from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from config import BRANDS

def get_brands_keyboard(prefix="brand_"):
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text=brand, callback_data=f"{prefix}{brand}")]
        for brand in BRANDS
    ])
