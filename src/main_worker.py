import asyncio
from dotenv import load_dotenv

from src.config import load_settings
from src.db.pool import create_pool
from src.db.schema import init_db
from src.telegram.client.telethon_client import create_telethon_client
from src.services.search_service import run_daily_search
from src.scheduler.daily import sleep_until_evening
from src.telegram.client.throttling import soft_sleep

async def main():
    load_dotenv()
    settings = load_settings()

    pool = await create_pool(settings.pg_dsn)
    await init_db(pool, settings.brands)

    client = create_telethon_client(settings.session_name, settings.api_id, settings.api_hash)

    async with client:
        while True:
            await sleep_until_evening(settings.tz, settings.worker_hour_local)

            reports = await run_daily_search(client, pool, settings)
            for r in reports:
                print(r)

            await soft_sleep(10)

if __name__ == "__main__":
    asyncio.run(main())
