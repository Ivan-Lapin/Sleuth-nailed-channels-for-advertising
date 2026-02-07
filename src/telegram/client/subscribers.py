import asyncio
from telethon.errors import FloodWaitError
from telethon.tl.functions.channels import GetFullChannelRequest

from src.db.repositories.channels import ChannelsRepo

async def update_channels_subscribers(
    client,
    conn,
    *,
    batch_limit: int = 50,
    sleep_sec: float = 1.0,
) -> dict:
    """
    Обновляем subscribers ТОЛЬКО для публичных каналов (username есть).
    Самый стабильный и безопасный MVP-вариант.
    """
    repo = ChannelsRepo(conn)
    peer = await client.get_input_entity("GWM_StockOption")
    full = await client(GetFullChannelRequest(peer))
    subs = full.full_chat.participants_count
    channels = await repo.list_channels_for_subs_update(limit=batch_limit)
    print("FULL:", full.full_chat)


    report = {
        "total_candidates": len(channels),
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "flood_waits": 0,
    }

    for ch in channels:
        channel_id = ch["id_channel"]
        username = ch.get("username")

        if not username:
            report["skipped"] += 1
            continue

        try:
            peer = await client.get_input_entity(username)
            full = await client(GetFullChannelRequest(peer))

            subs = getattr(full.full_chat, "participants_count", None)
            if not subs:
                report["skipped"] += 1
            else:
                await repo.update_subscribers(channel_id, int(subs))
                report["updated"] += 1

            await asyncio.sleep(sleep_sec)

        except FloodWaitError as e:
            report["flood_waits"] += 1
            await asyncio.sleep(e.seconds + 1)

        except Exception as e:
            report["errors"] += 1
            print("❌ subs error:", channel_id, username, e)

    return report
