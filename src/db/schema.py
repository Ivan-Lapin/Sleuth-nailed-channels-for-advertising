import asyncpg

DDL = """
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'channels_status') THEN
        CREATE TYPE channels_status AS ENUM ('potential', 'blacklist', 'undefined');
    END IF;
END $$;


CREATE TABLE IF NOT EXISTS brands (
    id SERIAL PRIMARY KEY,
    name TEXT UNIQUE NOT NULL
);

CREATE TABLE IF NOT EXISTS channels (
    id_channel BIGINT PRIMARY KEY,
    name TEXT NOT NULL,
    username TEXT,
    subscribers INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS posts (
    id BIGSERIAL PRIMARY KEY,
    channel_id BIGINT NOT NULL REFERENCES channels(id_channel) ON DELETE CASCADE,
    brand_id INTEGER NOT NULL REFERENCES brands(id),
    message_id INTEGER NOT NULL,
    post_url TEXT,
    posted_at TIMESTAMPTZ NOT NULL,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(channel_id, message_id, brand_id)
);

CREATE TABLE IF NOT EXISTS post_stats (
    id BIGSERIAL PRIMARY KEY,
    post_id BIGINT NOT NULL REFERENCES posts(id) ON DELETE CASCADE,
    stat_date DATE NOT NULL,
    views INTEGER NOT NULL DEFAULT 0,
    forwards INTEGER NOT NULL DEFAULT 0,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    UNIQUE(post_id, stat_date)
);

CREATE TABLE IF NOT EXISTS channels_brands (
    channel_id BIGINT NOT NULL REFERENCES channels(id_channel) ON DELETE CASCADE,
    brand_id INTEGER NOT NULL REFERENCES brands(id) ON DELETE CASCADE,
    mention_count INTEGER NOT NULL DEFAULT 0,
    last_mention_at TIMESTAMPTZ,
    status channels_status DEFAULT 'undefined',
    marked_reason TEXT,
    marked_at TIMESTAMPTZ,
    PRIMARY KEY (channel_id, brand_id)
);

CREATE INDEX IF NOT EXISTS idx_posts_brand_date ON posts(brand_id, posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_posts_channel_date ON posts(channel_id, posted_at DESC);
CREATE INDEX IF NOT EXISTS idx_post_stats_date ON post_stats(stat_date);
CREATE INDEX IF NOT EXISTS idx_channels_username ON channels(username);
CREATE INDEX IF NOT EXISTS idx_cb_brand_status ON channels_brands(brand_id, status);
CREATE INDEX IF NOT EXISTS idx_cb_brand_last ON channels_brands(brand_id, last_mention_at DESC);
"""

async def init_db(pool: asyncpg.Pool, brands: list[str]) -> None:
    async with pool.acquire() as conn:
        async with conn.transaction():
            await conn.execute(DDL)
            for b in brands:
                await conn.execute(
                    "INSERT INTO brands(name) VALUES($1) ON CONFLICT(name) DO NOTHING",
                    b,
                )
