# 🗄️ ВСЕ БД функции для PostgreSQL
# Совместимо с asyncpg для Telethon + psycopg2 для синхронных вызовов

import os
from time import strftime
import asyncpg
import psycopg2
from psycopg2.extras import RealDictCursor
from datetime import date, datetime, timedelta
from typing import List, Optional, Tuple, Dict, Any
import asyncio

from config import PG_DSN, BRANDS  # DSN вида "postgresql://user:pass@localhost/dbname"

# Глобальный пул соединений для asyncpg (эффективно для Telethon)
pool: Optional[asyncpg.Pool] = None

async def init_db_pool():
    """Инициализация пула соединений asyncpg"""
    global pool
    pool = await asyncpg.create_pool(
        PG_DSN,
        min_size=1,
        max_size=20,
        command_timeout=60
    )
    print("✅ PostgreSQL pool готов")

async def init_schema():
    """Создание/миграция схемы БД"""
    async with pool.acquire() as conn:
        async with conn.transaction():
        
            await conn.execute("""
                CREATE TYPE channels_status AS ENUM ('potential', 'blacklist', 'undefined');
            """)
            
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS brands (
                    id SERIAL PRIMARY KEY,
                    name TEXT UNIQUE NOT NULL
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channels (
                    id_channel BIGINT PRIMARY KEY,
                    name TEXT NOT NULL,
                    username TEXT,
                    subscribers INTEGER NOT NULL DEFAULT 0,
                    marked_reason TEXT,
                    marked_at TIMESTAMPTZ DEFAULT NOW(),
                    created_at TIMESTAMPTZ DEFAULT NOW()
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS posts (
                    id BIGSERIAL PRIMARY KEY,
                    channel_id BIGINT NOT NULL REFERENCES channels(id_channel) ON DELETE CASCADE,
                    brand_id INTEGER NOT NULL REFERENCES brands(id),
                    message_id INTEGER NOT NULL,
                    post_url TEXT,
                    posted_at TIMESTAMPTZ NOT NULL,
                    is_available BOOLEAN DEFAULT TRUE,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(channel_id, message_id, brand_id)
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS post_stats (
                    id BIGSERIAL PRIMARY KEY,
                    post_id BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
                    stat_date DATE NOT NULL,
                    views INTEGER NOT NULL DEFAULT 0,
                    forwards INTEGER NOT NULL DEFAULT 0,
                    created_at TIMESTAMPTZ DEFAULT NOW(),
                    UNIQUE(post_id, stat_date)
                );
            """)

            await conn.execute("""
                CREATE TABLE IF NOT EXISTS channels_brands (
                    channel_id BIGINT NOT NULL REFERENCES channels(id_channel),
                    brand_id INTEGER NOT NULL REFERENCES brands(id),
                    mention_count INTEGER NOT NULL DEFAULT 0,
                    status channels_status DEFAULT 'undefined',
                    last_mention_at TIMESTAMPTZ,
                    PRIMARY KEY (channel_id, brand_id)
                );
            """)

            # Индексы
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_brand_date ON posts(brand_id, posted_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_posts_channel_date ON posts(channel_id, posted_at);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_post_stats_date ON post_stats(stat_date);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_channels_brands_channel ON channels_brands(channel_id);")
            await conn.execute("CREATE INDEX IF NOT EXISTS idx_channels_brands_brand ON channels_brands(brand_id);")

            # Заполняем бренды
            for brand in BRANDS:
                await conn.execute("INSERT INTO brands (name) VALUES ($1) ON CONFLICT (name) DO NOTHING;", brand)

            print("✅ PostgreSQL схема готова")

async def get_or_create_channel(channel_id: int, name: str, username: str = None, subscribers: int = 0) -> int:
    """Возвращает channel_id (создаёт/обновляет канал)"""
    async with pool.acquire() as conn:
        await conn.execute("""
            INSERT INTO channels (id_channel, name, username, subscribers) 
            VALUES ($1, $2, $3, $4) 
            ON CONFLICT (id_channel) DO UPDATE SET 
                name = EXCLUDED.name, 
                username = EXCLUDED.username;
                subscribers = EXCLUDED.subscribers;
        """, channel_id, name, username, subscribers)
        return channel_id

async def get_brand_id(brand_name: str) -> Optional[int]:
    async with pool.acquire() as conn:
        row = await conn.fetchrow("SELECT id FROM brands WHERE name = $1;", brand_name)
        return row['id'] if row else None

async def save_channel_mention(channel_id: int, title: str, brand: str, username: str, 
                              message_id: int, posted_at: datetime, subscribers: int = 0):
    """Сохранение упоминания бренда в посте"""
    brand_id = await get_brand_id(brand)
    if not brand_id:
        return

    # Формируем post_url
    if username:
        post_url = f"t.me/{username}/{message_id}"
    else:
        post_url = f"t.me/c/{channel_id}/{message_id}"

    async with pool.acquire() as conn:
        await get_or_create_channel(channel_id, title, username, subscribers)

        # Сохраняем пост
        post_id = await conn.fetchval("""
            INSERT INTO posts (channel_id, brand_id, message_id, post_url, posted_at) 
            VALUES ($1, $2, $3, $4, $5) 
            ON CONFLICT DO NOTHING 
            RETURNING id;
        """, channel_id, brand_id, message_id, post_url, posted_at)

        if post_id:
            # Обновляем channels_brands (только если пост новый)
            await conn.execute("""
                INSERT INTO channels_brands (channel_id, brand_id) 
                VALUES ($1, $2) 
                ON CONFLICT (channel_id, brand_id) DO NOTHING;
            """, channel_id, brand_id)

        await conn.execute("COMMIT;")

# def get_relevant_channels(brand_name: str = None, min_mentions: int = 3) -> List[Dict]:
#     """Синхронная функция для бота: топ каналов по упоминаниям"""
#     with psycopg2.connect(PG_DSN) as conn:
#         with conn.cursor(cursor_factory=RealDictCursor) as cur:
#             if brand_name:
#                 cur.execute("""
#                     SELECT c.id_channel, c.name, c.username, cb.mention_count, b.name as brand
#                     FROM channels_brands cb
#                     JOIN channels c ON cb.channel_id = c.id_channel
#                     JOIN brands b ON cb.brand_id = b.id
#                     WHERE b.name = %s AND cb.mention_count >= %s
#                     ORDER BY cb.mention_count DESC LIMIT 20;
#                 """, (brand_name, min_mentions))
#             else:
#                 cur.execute("""
#                     SELECT c.id_channel, c.name, c.username, cb.mention_count, b.name as brand
#                     FROM channels_brands cb
#                     JOIN channels c ON cb.channel_id = c.id_channel
#                     JOIN brands b ON cb.brand_id = b.id
#                     WHERE cb.mention_count >= %s
#                     ORDER BY cb.mention_count DESC LIMIT 20;
#                 """, (min_mentions,))
            
#             return [dict(row) for row in cur.fetchall()]

def get_relevant_posts_for_brand(brand_name: str, min_mentions: int = 3, limit: int = 30) -> List[Dict]:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT p.post_url as id_post, c.id_channel, c.name, c.username, b.name
                FROM channels_brands cb
                JOIN brands b ON cb.brand_id = b.id
                JOIN channels c ON cb.channel_id = c.id_channel
                JOIN posts p ON p.channel_id = c.id_channel AND p.brand_id = b.id
                WHERE b.name = %s AND cb.mention_count >= %s
                ORDER BY cb.mention_count DESC, p.posted_at DESC
                LIMIT %s;
            """, (brand_name, min_mentions, limit))
            return [dict(row) for row in cur.fetchall()]

def get_stats() -> Tuple[int, int, int, int]:
    """Общая статистика"""
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(DISTINCT id_channel) FROM channels;")
            total_channels = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM brands;")
            total_brands = cur.fetchone()[0]
            
            cur.execute("SELECT COUNT(*) FROM channels_brands WHERE mention_count > 2;")
            relevant = cur.fetchone()[0]
            
            db_today = date.today().strftime('%Y-%m-%d')
            cur.execute("SELECT COUNT(*) FROM posts WHERE DATE(posted_at) = %s;", (db_today,))
            today_posts = cur.fetchone()[0]
            
            return total_channels, total_brands, relevant, today_posts

def get_detailed_brand_channels(brand_name: str, min_mentions: int = 2, max_posts_per_channel: int = 10) -> List[Tuple]:
    """Детализация каналов по бренду с постами"""
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            # Получаем каналы
            cur.execute("""
                SELECT c.id_channel, c.name, c.username, cb.mention_count
                FROM channels_brands cb
                JOIN brands b ON cb.brand_id = b.id
                JOIN channels c ON cb.channel_id = c.id_channel
                WHERE b.name = %s AND cb.mention_count > %s
                ORDER BY cb.mention_count DESC;
            """, (brand_name, min_mentions))
            channels = cur.fetchall()

            result = []
            for row in channels:
                ch_id, ch_name, username, mention_count = row['id_channel'], row['name'], row['username'], row['mention_count']
                
                # Посты канала
                cur.execute("""
                    SELECT post_url, posted_at
                    FROM posts
                    WHERE channel_id = %s AND brand_id = (SELECT id FROM brands WHERE name = %s)
                    ORDER BY posted_at DESC
                    LIMIT %s;
                """, (ch_id, brand_name, max_posts_per_channel))
                posts = cur.fetchall()
                
                post_ids = [(p['post_url'], p['posted_at'].strftime('%Y-%m-%d'), 0) for p in posts]  # message_id пока 0
                result.append((ch_name, username, mention_count, post_ids))
            
            return result

def get_brand_aggregate_stats(brand_name: str) -> Tuple[int, int, int]:
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor() as cur:
            cur.execute("""
                SELECT
                    COUNT(DISTINCT c.id_channel) AS channels_cnt,
                    COUNT(DISTINCT p.id) AS posts_cnt,
                    COUNT(CASE WHEN cb.mention_count > 2 THEN 1 END) AS relevant_channels_cnt
                FROM channels_brands cb
                JOIN brands b ON cb.brand_id = b.id
                JOIN channels c ON cb.channel_id = c.id_channel
                JOIN posts p ON p.channel_id = c.id_channel AND p.brand_id = b.id
                WHERE b.name = %s;
            """, (brand_name,))
            row = cur.fetchone()
            return row if row else (0, 0, 0)

async def save_post_stats(post_url: str, views: int, forwards: int):
    """Сохранение статистики поста за день"""
    async with pool.acquire() as conn:
        # Находим post_id по post_url (для совместимости)
        post_id = await conn.fetchval("""
            SELECT id FROM posts 
            WHERE post_url = $1;
        """, post_url)
        
        if post_id:
            today = date.today()
            await conn.execute("""
                INSERT INTO post_stats (post_id, stat_date, views, forwards)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (post_id, stat_date) DO UPDATE SET
                    views = EXCLUDED.views,
                    forwards = EXCLUDED.forwards;
            """, post_id, today, views, forwards)
            await conn.execute("COMMIT;")

async def update_channel_subscribers(channel_id: int, subscribers: int):
    """Обновление подписчиков канала"""
    async with pool.acquire() as conn:
        await conn.execute("""
            UPDATE channels 
            SET subscribers = $1 
            WHERE id_channel = $2;
        """, subscribers, channel_id)
        await conn.execute("COMMIT;")

def get_posts_for_bot(brand_id: int, date_from: str, date_to: str, channel_id: int = None) -> List[Dict]:
    """Для бота: посты бренда по диапазону дат"""
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            if channel_id:
                cur.execute("""
                    SELECT 
                        p.id, p.message_id, p.posted_at, p.post_url,
                        COALESCE(ps.views, 0) as views,
                        COALESCE(ps.forwards, 0) as forwards,
                        c.username
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id_channel
                    LEFT JOIN post_stats ps ON ps.post_id = p.id AND ps.stat_date = DATE(p.posted_at)
                    WHERE p.brand_id = %s AND p.channel_id = %s
                      AND p.posted_at BETWEEN %s AND %s
                    ORDER BY p.posted_at DESC;
                """, (brand_id, channel_id, date_from, date_to))
            else:
                cur.execute("""
                    SELECT 
                        p.id, p.message_id, p.posted_at, p.post_url,
                        COALESCE(ps.views, 0) as views,
                        COALESCE(ps.forwards, 0) as forwards,
                        c.username, c.name
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id_channel
                    LEFT JOIN post_stats ps ON ps.post_id = p.id AND ps.stat_date = DATE(p.posted_at)
                    WHERE p.brand_id = %s
                      AND p.posted_at BETWEEN %s AND %s
                    ORDER BY p.posted_at DESC;
                """, (brand_id, date_from, date_to))
            
            return [dict(row) for row in cur.fetchall()]
        
def get_posts_for_yesterday():
    """Получение постов за предыдущий день"""
    yesterday = (date.today - timedelta(days=1).strftime('%Y-%m-%d'))    
    with psycopg2.connect(PG_DSN) as conn:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute("""
                SELECT p.id, p.message_id
                FROM posts p
                JOIN channels c ON p.channels_id = c.id_channel
                WHERE 
                ;
                """
            )
            return [dict(row) for row in cur.fetchall()]
        
async def add_post_statistic(post_id: int, views: int, forwards: int):
    yesterday = date.today() - timedelta(days=1)
    async with pool.acquire() as conn:
        await conn.execute(
            """
            INSERT INTO post_stats(post_id, stat_date, views, forwards)
            VALUES ($1, $2, $3, $4)
            ON CONFLICT DO NOTHING
            """, post_id, yesterday, views, forwards
        )
        
        await conn.execute("COMMIT;")
           
# Graceful shutdown
async def close_pool():
    global pool
    if pool:
        await pool.close()
