from aiogram import Router, F
from aiogram.types import CallbackQuery, Message
from aiogram.fsm.context import FSMContext
from datetime import datetime, date, time, timedelta
from zoneinfo import ZoneInfo
import re

from src.telegram.bot.keyboards import range_prompt_kb, channels_list_kb, channel_actions_kb, posts_list_kb
from src.telegram.bot.states import Flow

from src.services.report_service import get_channels_for_brand_and_range  

from src.db.repositories.posts import PostsRepo
from src.db.repositories.brands import BrandsRepo


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
async def pick_brand(cb: CallbackQuery, state: FSMContext, pool):
    brand_id = int(cb.data.split(":")[1])
    
    async with pool.acquire() as conn:
        brand = await conn.fetchrow("SELECT id, name FROM brands WHERE id=$1", brand_id)

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
    if (end_d - start_d).days > 90:
        await message.answer("Слишком большой период.")
        return

    start_utc, end_utc_excl = _to_utc_bounds(start_d, end_d, settings.tz)
    start_ymd, end_ymd = _ymd(start_d), _ymd(end_d)

    async with pool.acquire() as conn:
        brands_repo = BrandsRepo(conn)
        brand_name = await brands_repo.get_by_id(brand_id)

        items = await get_channels_for_brand_and_range(
            conn,
            brand_id=brand_id,
            start_day=start_utc,
            end_day=end_utc_excl,
            tz_name=settings.tz,
            limit=PAGE_LIMIT,
            offset=0,
        )


    title = (
        f"Каналы с упоминаниями за "
        f"{start_d.isoformat()} — {end_d.isoformat()} "
        f"({brand_name})"
    )

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
            start_day=start_utc,
            end_day=end_utc_excl,
            tz_name=settings.tz,
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
            start_day=start_utc,
            end_day=end_utc_excl,
            tz_name=settings.tz,
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
            SELECT c.id_channel, c.name, c.username, cb.status, cb.marked_reason, c.subscribers,
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
    
    subs = row["subscribers"] or 0
    status = row["status"]

    lines = [
        f"Канал: {row['name']}",
        f"@{row['username'] or 'private'}",
        f"👥 Подписчики: {subs}",
        f"Статус: {status}",
    ]

    if status in ("potential", "blacklist") and row["marked_reason"]:
        lines.append(f"Причина: {row['marked_reason']}")

    lines.append(f"Период: {start_d.isoformat()} — {end_d.isoformat()}")

    txt = "\n".join(lines)
    
    await cb.message.edit_text(
        txt,
        reply_markup=channel_actions_kb(brand_id, start_ymd, end_ymd, channel_id),
        disable_web_page_preview=True
    )
    await cb.answer()
    
@router.callback_query(F.data.startswith("posts:"))
async def show_posts(cb: CallbackQuery, pool, settings):
    _, brand_id, start_ymd, end_ymd, channel_id, offset = cb.data.split(":")
    brand_id = int(brand_id)
    channel_id = int(channel_id)
    offset = int(offset)

    start_d = _ymd_to_date(start_ymd)
    end_d = _ymd_to_date(end_ymd)
    start_utc, end_utc_excl = _to_utc_bounds(start_d, end_d, settings.tz)

    limit = 10

    async with pool.acquire() as conn:
        repo = PostsRepo(conn)
        rows = await repo.select_posts_for_channel_brand_range(
            channel_id=channel_id,
            brand_id=brand_id,
            start_day=start_utc,
            end_day=end_utc_excl,
            stats_end_date=end_d,
            limit=limit,
            offset=offset,
        )


        ch = await conn.fetchrow(
            "SELECT name, username FROM channels WHERE id_channel=$1",
            channel_id
        )

    if not rows:
        text = "Постов в выбранном периоде не найдено."
    else:
        ch_name = ch["name"] if ch else str(channel_id)
        ch_user = ch["username"] if ch else None
        lines = [f"Посты канала: {ch_name} (@{ch_user or 'private'})"]
        lines.append(f"Период: {start_d.isoformat()} — {end_d.isoformat()}\n")

        for r in rows:
            base_dt = r.get("posted_at") or r.get("created_at")
            if base_dt is None:
                life_str = "-"
            else:
                life_start = max(base_dt, start_utc)
                delta = end_utc_excl - life_start
                if delta.total_seconds() < 0:
                    delta = timedelta(0)
                life_str = f"{delta.days}д {delta.seconds // 3600}ч"
            
            views = r.get("views")
            forwards = r.get("forwards")
            stat_date = r.get("stat_date")

            views_str = str(views) if views is not None else "—"
            forwards_str = str(forwards) if forwards is not None else "—"
            stat_date_str = stat_date.isoformat() if stat_date else "—"


            lines.append(
                f"{r['post_url']}\n"
                f"👁 {views_str}  🔁 {forwards_str}  📅 {stat_date_str}  ⏳ {life_str}\n"
            )



        text = "\n".join(lines)

    await cb.message.edit_text(
        text,
        reply_markup=posts_list_kb(
            brand_id=brand_id,
            start_ymd=start_ymd,
            end_ymd=end_ymd,
            channel_id=channel_id,
            offset=offset,
            limit=limit,
            got=len(rows),
        ),
        disable_web_page_preview=True,
    )
    await cb.answer()


