import asyncio
from dotenv import load_dotenv

from src.config import load_settings
from src.db.pool import create_pool
from src.db.schema import init_db
from src.telegram.client.telethon_client import create_telethon_client
from src.telegram.client.subscribers import update_channels_subscribers

async def main():
    load_dotenv()
    settings = load_settings()

    pool = await create_pool(settings.pg_dsn)
    await init_db(pool, settings.brands)

    client = create_telethon_client(
        settings.session_name + "_sub",
        settings.api_id,
        settings.api_hash,
    )

    async with client:
        async with pool.acquire() as conn:
            print("👥 Updating subscribers...")
            report = await update_channels_subscribers(
                client,
                conn,
                batch_limit=20,  
                sleep_sec=1.0,    
            )
            print("📊 Subscribers report:", report)

if __name__ == "__main__":
    asyncio.run(main())
