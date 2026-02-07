import asyncpg
from datetime import datetime, timezone

class ChannelsBrandsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def mark_status(self, channel_id: int, brand_id: int, status: str, reason: str | None) -> None:
        now = datetime.now(timezone.utc)
        await self.conn.execute("""
            UPDATE channels_brands
            SET status=$3,
                marked_reason=$4,
                marked_at=$5
            WHERE channel_id=$1 AND brand_id=$2
        """, channel_id, brand_id, status, reason, now)

    async def list_channels_for_brand_and_day(
        self, brand_id: int, day_start_utc, day_end_utc, limit: int = 50, offset: int = 0
    ) -> list[dict]:
        # Для интерфейса: каналы, где были посты в этот день, + count + last_post_url
        rows = await self.conn.fetch("""
            WITH day_posts AS (
                SELECT p.channel_id,
                       COUNT(*) AS posts_count,
                       MAX(p.posted_at) AS last_posted_at
                FROM posts p
                WHERE p.brand_id=$1
                  AND p.posted_at >= $2
                  AND p.posted_at <= $3
                GROUP BY p.channel_id
            ),
            last_posts AS (
                SELECT DISTINCT ON (p.channel_id)
                       p.channel_id,
                       p.post_url,
                       p.posted_at
                FROM posts p
                WHERE p.brand_id=$1
                  AND p.posted_at >= $2
                  AND p.posted_at <= $3
                ORDER BY p.channel_id, p.posted_at DESC
            )
            SELECT c.id_channel,
                   c.name,
                   c.username,
                   cb.status,
                   cb.marked_reason,
                   dp.posts_count,
                   lp.post_url,
                   dp.last_posted_at
            FROM day_posts dp
            JOIN channels c ON c.id_channel = dp.channel_id
            JOIN channels_brands cb ON cb.channel_id = dp.channel_id AND cb.brand_id=$1
            LEFT JOIN last_posts lp ON lp.channel_id = dp.channel_id
            ORDER BY dp.last_posted_at DESC
            LIMIT $4 OFFSET $5
        """, brand_id, day_start_utc, day_end_utc, limit, offset)
        return [dict(r) for r in rows]
    
    async def list_channels_for_brand_and_range(self, brand_id, start_utc, end_utc_excl, limit=50, offset=0):
        rows = await self.conn.fetch("""
            SELECT
            c.id_channel,
            c.name,
            c.username,
            c.subscribers,
            cb.status,
            COUNT(p.id) AS posts_count,
            MAX(p.posted_at) AS last_posted_at
            FROM posts p
            JOIN channels c ON c.id_channel = p.channel_id
            JOIN channels_brands cb ON cb.channel_id = c.id_channel AND cb.brand_id = p.brand_id
            WHERE p.brand_id = $1
            AND p.posted_at >= $2
            AND p.posted_at <  $3
            GROUP BY c.id_channel, c.name, c.username, c.subscribers, cb.status
            ORDER BY posts_count DESC, last_posted_at DESC
            LIMIT $4 OFFSET $5
        """, brand_id, start_utc, end_utc_excl, limit, offset)

        return [dict(r) for r in rows]

