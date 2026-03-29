import asyncpg
from datetime import date

class PostStatsRepo:
    def __init__(self, conn: asyncpg.Connection):
        self.conn = conn

    async def upsert_post_stats(
        self,
        *,
        post_id: int,
        stat_date: date,
        views: int,
        forwards: int,
    ) -> None:
        """
        Пишем статистику на конкретную дату.
        Если за эту дату уже есть строка — обновляем.
        """
        await self.conn.execute("""
            INSERT INTO post_stats (post_id, stat_date, views, forwards)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT (post_id, stat_date) DO UPDATE
            SET views = EXCLUDED.views,
                forwards = EXCLUDED.forwards
        """, post_id, stat_date, views, forwards)

    async def get_latest_stats_for_post(self, post_id: int) -> dict | None:
        """
        (Опционально) получить последний слепок статистики для поста.
        Удобно для дебага.
        """
        row = await self.conn.fetchrow("""
            SELECT stat_date, views, forwards
            FROM post_stats
            WHERE post_id = $1
            ORDER BY stat_date DESC
            LIMIT 1
        """, post_id)
        return dict(row) if row else None
