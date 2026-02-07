from aiogram.types import InlineKeyboardMarkup, InlineKeyboardButton
from datetime import date, timedelta

def brands_kb(brands: list[dict]) -> InlineKeyboardMarkup:
    buttons = [
        [InlineKeyboardButton(text=b["name"], callback_data=f"brand:{b['id']}")]
        for b in brands
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def range_prompt_kb() -> InlineKeyboardMarkup:
    # когда ждём ввод интервала: только навигация назад
    buttons = [
        [InlineKeyboardButton(text="⬅️ Назад к брендам", callback_data="nav:brands")]
    ]
    return InlineKeyboardMarkup(inline_keyboard=buttons)

def channels_list_kb(items: list[dict], brand_id: int, start_ymd: str, end_ymd: str, offset: int, limit: int) -> InlineKeyboardMarkup:
    """
    start_ymd/end_ymd — строка YYYYMMDD (компактная, чтобы влезать в callback)
    """
    buttons = []
    for it in items:
        title = it["name"]
        status = it["status"]
        posts_count = it["posts_count"]
        btn_text = f"{title} ({posts_count}) [{status}]"
        buttons.append([InlineKeyboardButton(
            text=btn_text[:60],
            callback_data=f"ch:{brand_id}:{start_ymd}:{end_ymd}:{it['id_channel']}"
        )])

    nav_row = []
    if offset > 0:
        nav_row.append(InlineKeyboardButton(
            text="⬅️",
            callback_data=f"page:{brand_id}:{start_ymd}:{end_ymd}:{max(0, offset-limit)}"
        ))
    if len(items) == limit:
        nav_row.append(InlineKeyboardButton(
            text="➡️",
            callback_data=f"page:{brand_id}:{start_ymd}:{end_ymd}:{offset+limit}"
        ))
    if nav_row:
        buttons.append(nav_row)

    buttons.append([InlineKeyboardButton(
        text="⬅️ Назад к вводу интервала",
        callback_data=f"nav:range:{brand_id}"
    )])

    return InlineKeyboardMarkup(inline_keyboard=buttons)

def channel_actions_kb(brand_id: int, start_ymd: str, end_ymd: str, channel_id: int, post_url: str | None) -> InlineKeyboardMarkup:
    buttons = []
    if post_url:
        buttons.append([InlineKeyboardButton(text="Открыть последний пост", url=post_url)])
    buttons.append([
        InlineKeyboardButton(text="✅ Potential", callback_data=f"mark:potential:{brand_id}:{start_ymd}:{end_ymd}:{channel_id}"),
        InlineKeyboardButton(text="⛔ Blacklist", callback_data=f"mark:blacklist:{brand_id}:{start_ymd}:{end_ymd}:{channel_id}"),
    ])
    buttons.append([InlineKeyboardButton(
        text="⬅️ Назад к списку",
        callback_data=f"nav:list:{brand_id}:{start_ymd}:{end_ymd}"
    )])
    return InlineKeyboardMarkup(inline_keyboard=buttons)