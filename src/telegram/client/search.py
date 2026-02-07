from datetime import timezone
from telethon.tl.functions.channels import SearchPostsRequest, CheckSearchPostsFloodRequest
from telethon.tl.types import InputPeerEmpty

from src.utils.dates import today_range_utc
from src.utils.text import exact_word_match
from src.db.repositories.posts import PostsRepo
from src.db.repositories.brands import BrandsRepo
from src.telegram.client.throttling import soft_sleep

def _is_today(msg_dt_utc, start_utc, end_utc) -> bool:
    if msg_dt_utc is None:
        return False
    if msg_dt_utc.tzinfo is None:
        msg_dt_utc = msg_dt_utc.replace(tzinfo=timezone.utc)
    return start_utc <= msg_dt_utc <= end_utc

async def global_brand_search(client, conn, brand_name: str, settings) -> dict:
    """
    Глобальный поиск по бренду.
    Сохраняем только посты за сегодня (Europe/Moscow) и только точные упоминания.
    """
    report = {
        "brand": brand_name,
        "processed_msgs": 0,
        "saved_posts": 0,
        "unique_channels": 0,
        "skipped_not_today": 0,
        "skipped_not_exact": 0,
        "flood_remains": None,
        "flood_total": None,
    }

    # лимиты поиска
    try:
        flood = await client(CheckSearchPostsFloodRequest())
        report["flood_remains"] = getattr(flood, "remains", None)
        report["flood_total"] = getattr(flood, "total_daily", None)
        if report["flood_remains"] is not None and report["flood_remains"] <= 0:
            return report
    except Exception:
        pass

    start_utc, end_utc = today_range_utc(settings.tz)

    result = await client(SearchPostsRequest(
        query=brand_name,
        limit=settings.search_limit,
        offset_rate=0,
        offset_peer=InputPeerEmpty(),
        offset_id=0,
    ))

    chats_by_id = {c.id: c for c in getattr(result, "chats", [])}
    seen_channels: set[int] = set()

    brands_repo = BrandsRepo(conn)
    brand_id = await brands_repo.get_brand_id(brand_name)
    posts_repo = PostsRepo(conn)

    for msg in getattr(result, "messages", []):
        msg_dt = getattr(msg, "date", None)
        if not _is_today(msg_dt, start_utc, end_utc):
            report["skipped_not_today"] += 1
            continue

        text = (getattr(msg, "message", "") or "")
        if not exact_word_match(text, brand_name):
            report["skipped_not_exact"] += 1
            continue

        peer = getattr(msg, "peer_id", None)
        channel_id = getattr(peer, "channel_id", None) if peer else None
        if not channel_id:
            continue

        chat = chats_by_id.get(channel_id)
        if not chat:
            continue

        username = getattr(chat, "username", None)
        title = getattr(chat, "title", "") or ""
        post_url = f"https://t.me/{username}/{msg.id}" if username else None
        subscribers = getattr(chat, "participants_count", None)

        await posts_repo.upsert_channel(channel_id, title, username, subscribers)
        inserted_post_id = await posts_repo.insert_post(
            channel_id=channel_id,
            brand_id=brand_id,
            message_id=msg.id,
            post_url=post_url,
            posted_at=msg_dt if msg_dt.tzinfo else msg_dt.replace(tzinfo=timezone.utc),
        )
        if inserted_post_id:
            report["saved_posts"] += 1
            await posts_repo.upsert_channels_brand_mention(channel_id, brand_id, msg_dt)

        report["processed_msgs"] += 1
        seen_channels.add(channel_id)

        await soft_sleep(settings.sleep_between_messages_sec)

    report["unique_channels"] = len(seen_channels)
    return report
