from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import re

from src.telegram.bot.keyboards import range_prompt_kb, channels_list_kb, channel_actions_kb
from src.telegram.bot.states import Flow

from src.services.report_service import get_channels_for_brand_and_range  

router = Router()
PAGE_LIMIT = 20


def _parse_range(text: str) -> tuple[date, date]:
    """
    Принимаем 'YYYY-MM-DD - YYYY-MM-DD' (можно без пробелов, можно тире/дефисы разные).
    """
    s = (text or "").strip().replace("—", "-").replace("–", "-")
    if " - " in s:
        left, right = [p.strip() for p in s.split(" - ", 1)]
    else:
        m = re.findall(r"\d{4}-\d{2}-\d{2}", s)
        if len(m) != 2:
            raise ValueError("bad format")
        left, right = m[0], m[1]

    start = date.fromisoformat(left)
    end = date.fromisoformat(right)
    if start > end:
        raise ValueError("start after end")
    return start, end


def _to_utc_bounds(start: date, end: date, tz_name: str) -> tuple[datetime, datetime]:
    """
    Возвращаем (start_utc, end_utc_exclusive)
    """
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(start, time.min, tzinfo=tz)
    end_local_excl = datetime.combine(end + timedelta(days=1), time.min, tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local_excl.astimezone(ZoneInfo("UTC"))


def _ymd(d: date) -> str:
    return d.strftime("%Y%m%d")


def _ymd_to_date(s: str) -> date:
    return date(int(s[0:4]), int(s[4:6]), int(s[6:8]))


@router.callback_query(F.data.startswith("brand:"))
async def pick_brand(cb: CallbackQuery, state: FSMContext):
    brand_id = int(cb.data.split(":")[1])

    await state.update_data(brand_id=brand_id)
    await state.set_state(Flow.waiting_date_range)

    await cb.message.edit_text(
        "Введите интервал дат в формате:\n"
        "`YYYY-MM-DD - YYYY-MM-DD`\n"
        "Например: `2026-02-01 - 2026-02-06`",
        reply_markup=range_prompt_kb(),
        parse_mode="Markdown",
    )
    await cb.answer()


@router.callback_query(F.data.startswith("nav:range:"))
async def nav_range(cb: CallbackQuery, state: FSMContext):
    # вернуться к вводу интервала
    brand_id = int(cb.data.split(":")[2])
    await state.update_data(brand_id=brand_id)
    await state.set_state(Flow.waiting_date_range)
    await cb.message.edit_text(
        "Введите интервал дат в формате:\n"
        "`YYYY-MM-DD - YYYY-MM-DD`\n"
        "Например: `2026-02-01 - 2026-02-06`",
        reply_markup=range_prompt_kb(),
        parse_mode="Markdown",
    )
    await cb.answer()


@router.message(Flow.waiting_date_range)
async def range_entered(message: Message, state: FSMContext, pool, settings):
    data = await state.get_data()
    brand_id = data.get("brand_id")
    if not brand_id:
        await message.answer("Сначала выберите бренд командой /start")
        return

    try:
        start_d, end_d = _parse_range(message.text or "")
    except ValueError:
        await message.answer("Не понял формат. Пример: `2026-02-01 - 2026-02-06`", parse_mode="Markdown")
        return

    # разумный лимит, чтобы не тянуть огромные периоды
    if (end_d - start_d).days > 60:
        await message.answer("Слишком большой период. Для MVP максимум 60 дней.")
        return

    start_utc, end_utc_excl = _to_utc_bounds(start_d, end_d, settings.tz)
    start_ymd, end_ymd = _ymd(start_d), _ymd(end_d)

    async with pool.acquire() as conn:
        items = await get_channels_for_brand_and_range(
            conn,
            brand_id=brand_id,
            start_utc=start_utc,
            end_utc_excl=end_utc_excl,
            limit=PAGE_LIMIT,
            offset=0,
        )

    title = f"Каналы с упоминаниями за {start_d.isoformat()} — {end_d.isoformat()} (brand_id={brand_id})"
    await message.answer(
        title,
        reply_markup=channels_list_kb(items, brand_id, start_ymd, end_ymd, offset=0, limit=PAGE_LIMIT),
        disable_web_page_preview=True
    )


@router.callback_query(F.data.startswith("page:"))
async def paginate(cb: CallbackQuery, pool, settings):
    _, brand_id, start_ymd, end_ymd, offset = cb.data.split(":")
    brand_id = int(brand_id)
    offset = int(offset)

    start_d = _ymd_to_date(start_ymd)
    end_d = _ymd_to_date(end_ymd)
    start_utc, end_utc_excl = _to_utc_bounds(start_d, end_d, settings.tz)

    async with pool.acquire() as conn:
        items = await get_channels_for_brand_and_range(
            conn,
            brand_id=brand_id,
            start_utc=start_utc,
            end_utc_excl=end_utc_excl,
            limit=PAGE_LIMIT,
            offset=offset,
        )

    title = f"Каналы с упоминаниями за {start_d.isoformat()} — {end_d.isoformat()} (brand_id={brand_id})"
    await cb.message.edit_text(
        title,
        reply_markup=channels_list_kb(items, brand_id, start_ymd, end_ymd, offset=offset, limit=PAGE_LIMIT),
        disable_web_page_preview=True
    )
    await cb.answer()


@router.callback_query(F.data.startswith("nav:list:"))
async def back_to_list(cb: CallbackQuery, pool, settings):
    _, _, brand_id, start_ymd, end_ymd = cb.data.split(":")
    brand_id = int(brand_id)

    start_d = _ymd_to_date(start_ymd)
    end_d = _ymd_to_date(end_ymd)
    start_utc, end_utc_excl = _to_utc_bounds(start_d, end_d, settings.tz)

    async with pool.acquire() as conn:
        items = await get_channels_for_brand_and_range(
            conn,
            brand_id=brand_id,
            start_utc=start_utc,
            end_utc_excl=end_utc_excl,
            limit=PAGE_LIMIT,
            offset=0,
        )

    title = f"Каналы с упоминаниями за {start_d.isoformat()} — {end_d.isoformat()} (brand_id={brand_id})"
    await cb.message.edit_text(
        title,
        reply_markup=channels_list_kb(items, brand_id, start_ymd, end_ymd, offset=0, limit=PAGE_LIMIT),
        disable_web_page_preview=True
    )
    await cb.answer()


@router.callback_query(F.data.startswith("ch:"))
async def open_channel_card(cb: CallbackQuery, pool, settings):
    _, brand_id, start_ymd, end_ymd, channel_id = cb.data.split(":")
    brand_id = int(brand_id)
    channel_id = int(channel_id)

    start_d = _ymd_to_date(start_ymd)
    end_d = _ymd_to_date(end_ymd)

    async with pool.acquire() as conn:
        row = await conn.fetchrow("""
            SELECT c.id_channel, c.name, c.username, cb.status, cb.marked_reason,
                   (SELECT p.post_url FROM posts p
                    WHERE p.brand_id=$1 AND p.channel_id=$2
                      AND p.posted_at >= $3 AND p.posted_at < $4
                    ORDER BY p.posted_at DESC LIMIT 1) AS post_url
            FROM channels c
            JOIN channels_brands cb ON cb.channel_id=c.id_channel AND cb.brand_id=$1
            WHERE c.id_channel=$2
        """, brand_id, channel_id,
        *_to_utc_bounds(start_d, end_d, settings.tz))

    if not row:
        await cb.message.answer("Канал не найден в БД. Возможно, обновите поиск.")
        await cb.answer()
        return

    post_url = row["post_url"]
    txt = (
        f"Канал: {row['name']}\n"
        f"@{row['username'] or 'private'}\n"
        f"Статус: {row['status']}\n"
        f"Причина: {row['marked_reason'] or '-'}\n"
        f"Период: {start_d.isoformat()} — {end_d.isoformat()}"
    )
    await cb.message.edit_text(
        txt,
        reply_markup=channel_actions_kb(brand_id, start_ymd, end_ymd, channel_id, post_url),
        disable_web_page_preview=True
    )
    await cb.answer()
