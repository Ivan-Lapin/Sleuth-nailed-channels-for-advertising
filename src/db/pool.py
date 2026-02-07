import asyncpg

async def create_pool(pg_dsn: str) -> asyncpg.Pool:
    return await asyncpg.create_pool(dsn=pg_dsn, min_size=1, max_size=10)
