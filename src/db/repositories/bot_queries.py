import asyncpg
from datetime import datetime

class BotQueriesRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def list_channels_by_brand_range(
        self,
        brand_id: int,
        start_utc: datetime,
        end_utc_excl: datetime,
        limit: int = 20,
        offset: int = 0,
    ) -> list[dict]:
        rows = await self.conn.fetch("""
            SELECT
              c.id_channel,
              c.name,
              c.username,
              c.subscribers,
              cb.status,
              cb.mention_count,
              MAX(p.posted_at) AS last_posted_at,
              COUNT(*) AS posts_in_range
            FROM posts p
            JOIN channels c ON c.id_channel = p.channel_id
            JOIN channels_brands cb ON cb.channel_id = c.id_channel AND cb.brand_id = p.brand_id
            WHERE p.brand_id = $1
              AND p.posted_at >= $2
              AND p.posted_at <  $3
            GROUP BY c.id_channel, c.name, c.username, c.subscribers, cb.status, cb.mention_count
            ORDER BY posts_in_range DESC, last_posted_at DESC
            LIMIT $4 OFFSET $5;
        """, brand_id, start_utc, end_utc_excl, limit, offset)

        return [dict(r) for r in rows]
