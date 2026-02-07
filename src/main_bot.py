import asyncio
from dotenv import load_dotenv

from src.config import load_settings
from src.db.pool import create_pool
from src.db.schema import init_db
from src.telegram.bot.app import build_bot_app

async def main():
    load_dotenv()
    settings = load_settings()

    pool = await create_pool(settings.pg_dsn)
    await init_db(pool, settings.brands)

    bot, dp = build_bot_app(settings.bot_token)
    await dp.start_polling(bot, pool=pool, settings=settings)

if __name__ == "__main__":
    asyncio.run(main())
