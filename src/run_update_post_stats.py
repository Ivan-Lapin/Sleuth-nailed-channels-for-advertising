import asyncio
from datetime import datetime, timedelta, date

from dotenv import load_dotenv
from telethon.errors import FloodWaitError

from src.config import load_settings
from src.db.pool import create_pool
from src.telegram.client.telethon_client import create_telethon_client
from src.db.repositories.posts import PostsRepo
from src.db.repositories.post_stats import PostStatsRepo
from src.telegram.client.post_stats_fetcher import fetch_post_stats

SLEEP_BETWEEN_REQ_SEC = 0.35


async def main():
    load_dotenv()
    settings = load_settings()
    print("⚙️ Settings loaded")

    pool = await create_pool(settings.pg_dsn)
    print("✅ DB pool created")

    client = create_telethon_client(
        session_name="post_stats_updater",
        api_id=settings.api_id,
        api_hash=settings.api_hash,
    )

    since_utc = datetime.utcnow() - timedelta(days=7)
    stat_day = date.today()

    updated = 0
    skipped = 0
    errors = 0
    flood_waits = 0

    async with client:
        print("✅ Telethon client started")

        async with pool.acquire() as conn:
            posts_repo = PostsRepo(conn)
            stats_repo = PostStatsRepo(conn)

            # ВАЖНО: выбирать только те посты, где access_hash есть и не 0
            posts = await posts_repo.list_posts_for_stats_update(
                since_utc=since_utc,
                limit=2000,
            )
            print(f"✅ List of posts: {len(posts)}")

            total = len(posts)
            for i, p in enumerate(posts, start=1):
                if i % 50 == 0:
                    print(f"Progress {i}/{total} updated={updated} skipped={skipped} errors={errors}")

                try:
                    # если access_hash всё равно отсутствует — пропускаем
                    ah = p.get("access_hash")
                    if ah is None or int(ah) == 0:
                        skipped += 1
                        continue

                    stats = await fetch_post_stats(
                        client,
                        channel_id=int(p["channel_id"]),
                        access_hash=int(ah),
                        message_id=int(p["message_id"]),
                    )

                    if not stats:
                        skipped += 1
                        continue

                    await stats_repo.upsert_post_stats(
                        post_id=int(p["post_id"]),
                        stat_date=stat_day,
                        views=int(stats.views),
                        forwards=int(stats.forwards or 0),
                    )

                    updated += 1
                    await asyncio.sleep(SLEEP_BETWEEN_REQ_SEC)

                except FloodWaitError as e:
                    flood_waits += 1
                    print(f"⏳ FloodWait {e.seconds}s — stopping updater for now")
                    break

                except Exception as e:
                    errors += 1
                    print("❌ stats error:", p, e)

    print({
        "total_candidates": len(posts),
        "updated": updated,
        "skipped": skipped,
        "errors": errors,
        "flood_waits": flood_waits,
    })


if __name__ == "__main__":
    asyncio.run(main())
