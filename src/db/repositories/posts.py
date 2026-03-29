import asyncpg
from datetime import datetime, date

class PostsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def upsert_channel(
        self,
        channel_id: int,
        title: str,
        username: str | None,
        subscribers: int | None,
        access_hash: int | None,
    ) -> None:
        await self.conn.execute("""
            INSERT INTO channels (id_channel, name, username, subscribers, access_hash)
            VALUES ($1, $2, $3, COALESCE($4, 0), $5)
            ON CONFLICT (id_channel) DO UPDATE
            SET name = EXCLUDED.name,
                username = EXCLUDED.username,
                subscribers = CASE
                    WHEN EXCLUDED.subscribers > 0 THEN EXCLUDED.subscribers
                    ELSE channels.subscribers
                END,
                access_hash = COALESCE(EXCLUDED.access_hash, channels.access_hash);
        """, channel_id, title, username, subscribers, access_hash)


    async def insert_post(self, channel_id: int, brand_id: int, message_id: int,
                          post_url: str | None, posted_at: datetime) -> int | None:
        row = await self.conn.fetchrow("""
            INSERT INTO posts (channel_id, brand_id, message_id, post_url, posted_at)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (channel_id, message_id, brand_id) DO NOTHING
            RETURNING id
        """, channel_id, brand_id, message_id, post_url, posted_at)
        return int(row["id"]) if row else None

    async def upsert_channels_brand_mention(self, channel_id: int, brand_id: int, posted_at: datetime) -> None:
        await self.conn.execute("""
            INSERT INTO channels_brands (channel_id, brand_id, mention_count, last_mention_at)
            VALUES ($1, $2, 1, $3)
            ON CONFLICT (channel_id, brand_id) DO UPDATE
            SET mention_count = channels_brands.mention_count + 1,
                last_mention_at = GREATEST(channels_brands.last_mention_at, EXCLUDED.last_mention_at)
        """, channel_id, brand_id, posted_at)
        
    
    async def select_posts_for_channel_brand_range(
        self,
        *,
        channel_id: int,
        brand_id: int,
        start_day: datetime,
        end_day: datetime,
        stats_end_date: date,
        limit: int = 10,
        offset: int = 0,
    ) -> list[dict]:
        rows = await self.conn.fetch("""
            SELECT
            p.id AS post_id,
            p.post_url,
            p.posted_at,
            p.created_at,
            s.views AS views,
            s.forwards AS forwards,
            s.stat_date
            FROM posts p
            LEFT JOIN LATERAL (
            SELECT views, forwards, stat_date
            FROM post_stats
            WHERE post_id = p.id
                AND stat_date <= $5
            ORDER BY stat_date DESC
            LIMIT 1
            ) s ON TRUE
            WHERE p.brand_id = $1
            AND p.channel_id = $2
            AND p.posted_at >= $3
            AND p.posted_at <  $4
            ORDER BY p.posted_at DESC
            LIMIT $6 OFFSET $7
        """, brand_id, channel_id, start_day, end_day, stats_end_date, limit, offset)

        return [dict(r) for r in rows]
    
    async def list_posts_for_stats_update(
        self,
        *,
        since_utc: datetime,
        limit: int = 500,
        only_public: bool = True,
    ) -> list[dict]:
        """
        Выбираем посты, для которых хотим обновить post_stats.
        MVP-критерий: посты за последние N дней.
        Дополнительно (если only_public=True) фильтруем только публичные каналы (username не null),
        иначе telethon часто не сможет достать сообщение.
        """
        if only_public:
            rows = await self.conn.fetch("""
                SELECT p.id AS post_id, p.channel_id, p.message_id, c.access_hash
                FROM posts p
                JOIN channels c ON c.id_channel = p.channel_id
                WHERE p.posted_at >= $1
                AND c.access_hash IS NOT NULL
                AND c.access_hash <> 0
                ORDER BY p.posted_at DESC
                LIMIT $2
            """, since_utc, limit)
        else:
            rows = await self.conn.fetch("""
                SELECT
                  p.id AS post_id,
                  p.channel_id,
                  p.message_id
                FROM posts p
                WHERE p.posted_at >= $1
                ORDER BY p.posted_at DESC
                LIMIT $2
            """, since_utc, limit)

        return [dict(r) for r in rows]

        
