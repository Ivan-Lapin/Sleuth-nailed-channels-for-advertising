import asyncpg
from src.telegram.client.search import global_brand_search

async def run_daily_search(client, pool: asyncpg.Pool, settings) -> list[dict]:
    reports: list[dict] = []
    async with pool.acquire() as conn:
        brands = await conn.fetch("SELECT name FROM brands ORDER BY id")
        for b in brands:
            
            rep = await global_brand_search(client, conn, b["name"], settings)
            reports.append(rep)
    return reports
