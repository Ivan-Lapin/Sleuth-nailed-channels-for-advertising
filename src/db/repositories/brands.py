import asyncpg

class BrandsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def list_brands(self) -> list[dict]:
        rows = await self.conn.fetch("SELECT id, name FROM brands ORDER BY name")
        return [dict(r) for r in rows]

    async def get_brand_id(self, name: str) -> int:
        row = await self.conn.fetchrow("SELECT id FROM brands WHERE name=$1", name)
        if not row:
            raise ValueError(f"Brand not found: {name}")
        return int(row["id"])
