import asyncio
import re
from dotenv import load_dotenv

from telethon import TelegramClient
from telethon.errors import FloodWaitError, UsernameInvalidError, UsernameNotOccupiedError
from telethon.tl.functions.channels import GetFullChannelRequest
from telethon.tl.types import InputPeerChannel

from src.config import load_settings
from src.db.pool import create_pool
from src.db.repositories.channels import ChannelsRepo

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,30}[A-Za-z0-9]$")


async def main():
    load_dotenv()
    settings = load_settings()
    pool = await create_pool(settings.pg_dsn)

    # ВАЖНО: отдельное имя сессии, чтобы не конфликтовать с другими скриптами
    async with TelegramClient("fill_access_hash", settings.api_id, settings.api_hash) as client:
        async with pool.acquire() as conn:
            repo = ChannelsRepo(conn)

            limit = 200  
            channels = await repo.list_channels_missing_access_hash(limit=limit)

            report = {
                "total": len(channels),
                "resolved": 0,
                "hash_updated": 0,
                "subs_updated": 0,
                "skipped": 0,
                "bad_username": 0,
                "id_mismatch": 0,
                "flood_waits": 0,
                "errors": 0,
            }

            print(f"🔎 Candidates: {len(channels)}")

            for i, ch in enumerate(channels, start=1):
                channel_id = int(ch["id_channel"])
                username = (ch.get("username") or "").lstrip("@").strip()
                subs_in_db = ch.get("subscribers") or 0

                if not username or not USERNAME_RE.match(username):
                    report["skipped"] += 1
                    await repo.null_username(channel_id)
                    report["bad_username"] += 1
                    continue

                try:
                    # 1) Resolve username -> получаем id + access_hash
                    ent = await client.get_entity(username)  # <-- это ResolveUsernameRequest
                    ent_id = int(getattr(ent, "id", 0) or 0)
                    ent_hash = getattr(ent, "access_hash", None)

                    if not ent_id or ent_hash is None:
                        report["skipped"] += 1
                        continue

                    report["resolved"] += 1

                    # username мог быть переиспользован и вести на другой канал
                    if ent_id != channel_id:
                        report["id_mismatch"] += 1
                        # безопаснее занулить username, чтобы больше не пытаться
                        await repo.null_username(channel_id)
                        continue

                    # 2) Сохраняем access_hash
                    await repo.update_access_hash(channel_id, int(ent_hash))
                    report["hash_updated"] += 1

                    # 3) Обновляем subscribers только если 0/NULL
                    if subs_in_db == 0:
                        peer = InputPeerChannel(channel_id, int(ent_hash))
                        full = await client(GetFullChannelRequest(peer))
                        subs = getattr(full.full_chat, "participants_count", None)
                        if subs is not None:
                            await repo.update_subscribers_if_zero(channel_id, int(subs))
                            report["subs_updated"] += 1

                    if i % 10 == 0:
                        print(f"Progress {i}/{len(channels)} report={report}")

                    await asyncio.sleep(2.0)  # обязательно: снижает шанс FloodWait

                except (UsernameInvalidError, UsernameNotOccupiedError):
                    report["bad_username"] += 1
                    await repo.null_username(channel_id)

                except FloodWaitError as e:
                    report["flood_waits"] += 1
                    print(f"⏳ FloodWait {e.seconds}s — stopping for now")
                    break

                except Exception as e:
                    report["errors"] += 1
                    print("❌ error:", {"channel_id": channel_id, "username": username, "err": str(e)})

            print("📊 Final report:", report)


if __name__ == "__main__":
    asyncio.run(main())
