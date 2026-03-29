import asyncio

from telethon.errors import FloodWaitError
from telethon.tl.types import InputPeerChannel
from telethon.tl.functions.channels import GetFullChannelRequest

from src.db.repositories.channels import ChannelsRepo


async def update_channels_subscribers(
    client,
    conn,
    *,
    batch_limit: int = 500,
    sleep_sec: float = 1.0,
) -> dict:

    repo = ChannelsRepo(conn)
    channels = await repo.list_channels_for_subs_update(limit=batch_limit)

    report = {
        "total_candidates": len(channels),
        "updated": 0,
        "skipped": 0,
        "errors": 0,
        "flood_waits": 0,
    }

    for ch in channels:
        channel_id = ch["id_channel"]
        access_hash = ch.get("access_hash")

        # ❗ если нет access_hash — пропускаем
        if not access_hash:
            report["skipped"] += 1
            continue

        try:
            peer = InputPeerChannel(int(channel_id), int(access_hash))
            full = await client(GetFullChannelRequest(peer))

            subs = getattr(full.full_chat, "participants_count", None)

            if subs is None:
                report["skipped"] += 1
            else:
                await repo.update_subscribers(channel_id, int(subs))
                report["updated"] += 1

            await asyncio.sleep(sleep_sec)

        except FloodWaitError as e:
            report["flood_waits"] += 1
            print(f"⏳ FloodWait {e.seconds}s — stopping updater")
            break

        except Exception as e:
            report["errors"] += 1
            print("❌ subs error:", channel_id, e)

    return report
