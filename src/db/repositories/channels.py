# src/db/repositories/channels.py
import asyncpg
from datetime import datetime
from typing import Optional
from typing import Any


class ChannelsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def list_channels_for_subs_update(self, limit: int = 200) -> list[dict]:
        # MVP-логика: обновляем только тех, у кого subscribers = 0
        rows = await self.conn.fetch("""
            SELECT id_channel, username, access_hash
            FROM channels
            WHERE access_hash IS NOT NULL
            ORDER BY subscribers ASC
            LIMIT $1;
        """, limit)
        return [dict(r) for r in rows]

    async def update_subscribers(self, channel_id: int, subscribers: int) -> None:
        await self.conn.execute("""
            UPDATE channels
            SET subscribers = $2
            WHERE id_channel = $1
        """, channel_id, subscribers)
        
    async def null_username(self, channel_id: int) -> None:
        await self.conn.execute("UPDATE channels SET username=NULL WHERE id_channel=$1", channel_id)
        
    async def list_channels_missing_access_hash(self, *, limit: int = 100) -> list[dict[str, Any]]:
        rows = await self.conn.fetch("""
            SELECT id_channel, username, subscribers
            FROM channels
            WHERE access_hash IS NULL
              AND username IS NOT NULL
              AND username <> ''
            ORDER BY created_at DESC
            LIMIT $1
        """, limit)
        return [dict(r) for r in rows]

    async def update_access_hash(self, channel_id: int, access_hash: int) -> None:
        await self.conn.execute("""
            UPDATE channels
            SET access_hash = $2
            WHERE id_channel = $1
        """, channel_id, access_hash)

    async def update_subscribers_if_zero(self, channel_id: int, subs: int) -> None:
        await self.conn.execute("""
            UPDATE channels
            SET subscribers = $2
            WHERE id_channel = $1
              AND (subscribers IS NULL OR subscribers = 0)
        """, channel_id, subs)

    async def null_username(self, channel_id: int) -> None:
        await self.conn.execute("""
            UPDATE channels
            SET username = NULL
            WHERE id_channel = $1
        """, channel_id)

