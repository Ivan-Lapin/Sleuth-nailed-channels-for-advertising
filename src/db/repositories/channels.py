# src/db/repositories/channels.py
import asyncpg
from datetime import datetime
from typing import Optional

class ChannelsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def list_channels_for_subs_update(self, limit: int = 200) -> list[dict]:
        # MVP-логика: обновляем только тех, у кого subscribers = 0
        rows = await self.conn.fetch("""
            SELECT id_channel, username
            FROM channels
            WHERE subscribers = 0
            AND username IS NOT NULL
            AND username <> ''
            ORDER BY created_at DESC
            LIMIT $1

        """, limit)
        return [dict(r) for r in rows]

    async def update_subscribers(self, channel_id: int, subscribers: int) -> None:
        await self.conn.execute("""
            UPDATE channels
            SET subscribers = $2
            WHERE id_channel = $1
        """, channel_id, subscribers)
