import asyncpg
from datetime import datetime

class PostsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def upsert_channel(self, channel_id: int, title: str, username: str | None, subscribers: int | None) -> None:
        await self.conn.execute("""
            INSERT INTO channels (id_channel, name, username, subscribers)
            VALUES ($1, $2, $3, COALESCE($4, 0))
            ON CONFLICT (id_channel) DO UPDATE
            SET name = EXCLUDED.name,
                username = EXCLUDED.username,
                subscribers = CASE
                WHEN EXCLUDED.subscribers > 0 THEN EXCLUDED.subscribers
                ELSE channels.subscribers
            END
        """, channel_id, title, username, subscribers)

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
