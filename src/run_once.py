import asyncio
from dotenv import load_dotenv

from src.config import load_settings
from src.db.pool import create_pool
from src.db.schema import init_db
from src.telegram.client.telethon_client import create_telethon_client
from src.services.search_service import run_daily_search

async def main():
    load_dotenv()
    settings = load_settings()

    print("⚙️ Settings loaded")
    print("Brands:", settings.brands)

    pool = await create_pool(settings.pg_dsn)
    await init_db(pool, settings.brands)

    client = create_telethon_client(
        settings.session_name + "_once",
        settings.api_id,
        settings.api_hash,
    )

    async with client:
        print("🔍 Running search NOW...")
        reports = await run_daily_search(client, pool, settings)
        print("📊 Reports:")
        for r in reports:
            print(r)

if __name__ == "__main__":
    asyncio.run(main())
