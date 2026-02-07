# stats_collector.py
# 🔥 Ночной сборщик статистики постов (запуск в 00:01 UTC через cron)
# Работает параллельно с ботом и поиском упоминаний

import asyncio
import logging
from datetime import date, timedelta
from typing import List, Dict

import asyncpg
import psycopg2
from psycopg2.extras import RealDictCursor
from telethon import TelegramClient
from telethon.errors import FloodWaitError, ChannelPrivateError

from config import PG_DSN, API_ID, API_HASH, MAIN_SESSION
from db import pool, init_db_pool  # твой пул из db.py

# Настройка логирования
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler('logs/stats_collector.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class StatsCollector:
    def __init__(self):
        self.stats_today = {
            'processed': 0,
            'errors': 0,
            'views_total': 0,
            'forwards_total': 0
        }

    async def get_posts_without_stats(self) -> List[Dict]:
        """Получает посты без статистики за вчера (смотрим последние N дней)"""
        yesterday = date.today() - timedelta(days=1)
        
        with psycopg2.connect(PG_DSN) as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cur:
                cur.execute("""
                    SELECT p.id, p.channel_id, p.message_id, c.username, p.posted_at, p.post_url
                    FROM posts p
                    JOIN channels c ON p.channel_id = c.id_channel
                    LEFT JOIN post_stats ps ON ps.post_id = p.id AND ps.stat_date = %s
                    WHERE ps.post_id IS NULL  -- нет статистики за вчера
                    ORDER BY p.posted_at DESC;
                """, (yesterday))
                return [dict(row) for row in cur.fetchall()]

    async def get_post_stats_telethon(self, client: TelegramClient, 
                                    channel_id: int, message_id: int, 
                                    username: str = None) -> Dict[str, int]:
        """Получает статистику поста через Telethon"""
        try:
            # Пробуем сначала по username, потом по channel_id
            entity = username or channel_id
            messages = await client.get_messages(entity, ids=message_id)
            
            if not messages:
                return {'views': 0, 'forwards': 0, 'error': 'Post not found'}
            
            msg = messages[0]
            return {
                'views': msg.views or 0,
                'forwards': msg.forwards or 0,
                'error': None
            }
        except FloodWaitError as e:
            logger.warning(f"Flood wait {e.seconds}s for post {message_id}")
            await asyncio.sleep(e.seconds)
            return await self.get_post_stats_telethon(client, channel_id, message_id, username)
        except ChannelPrivateError:
            return {'views': 0, 'forwards': 0, 'error': 'Channel private'}
        except Exception as e:
            logger.error(f"Error getting stats for {channel_id}/{message_id}: {e}")
            return {'views': 0, 'forwards': 0, 'error': str(e)}

    async def save_post_stats(self, post_id: int, views: int, forwards: int):
        """Сохраняет статистику поста за вчера"""
        yesterday = date.today() - timedelta(days=1)
        async with pool.acquire() as conn:
            await conn.execute("""
                INSERT INTO post_stats (post_id, stat_date, views, forwards)
                VALUES ($1, $2, $3, $4)
                ON CONFLICT (post_id, stat_date) DO UPDATE SET
                    views = EXCLUDED.views,
                    forwards = EXCLUDED.forwards;
            """, post_id, yesterday, views, forwards)
            await conn.execute("COMMIT;")

    async def update_channel_subs_if_needed(self, client: TelegramClient, channel_id: int):
        """Обновляет подписчиков канала (раз в неделю)"""
        try:
            entity = await client.get_entity(channel_id)
            async with pool.acquire() as conn:
                await conn.execute("""
                    UPDATE channels SET subscribers = $1 WHERE id_channel = $2;
                """, entity.participants_count or 0, channel_id)
                await conn.execute("COMMIT;")
        except Exception as e:
            logger.debug(f"Could not update subs for {channel_id}: {e}")

    async def collect_stats(self):
        """Основной метод сбора статистики"""
        logger.info("🚀 Запуск ночного сбора статистики")
        
        # Инициализация пула БД
        await init_db_pool()
        
        # Получаем посты без статистики
        posts = await self.get_posts_without_stats(days_back=14)
        logger.info(f"📊 Найдено {len(posts)} постов без статистики за вчера")
        
        if not posts:
            logger.info("✅ Все посты имеют статистику, выходим")
            return

        async with TelegramClient(MAIN_SESSION, API_ID, API_HASH) as client:
            await client.start()
            logger.info("✅ Telethon клиент запущен")
            
            # Обновляем подписчиков для всех каналов (раз в неделю)
            unique_channels = list(set(post['channel_id'] for post in posts))
            logger.info(f"📈 Обновляем подписчиков для {len(unique_channels)} каналов")
            
            for i, channel_id in enumerate(unique_channels[:50]):  # лимит 50 каналов
                if i % 10 == 0:
                    await asyncio.sleep(1)  # антифлуд
                await self.update_channel_subs_if_needed(client, channel_id)

            # Собираем статистику постов батчами по 20
            for i in range(0, len(posts), 20):
                batch = posts[i:i+20]
                logger.info(f"📈 Обрабатываем батч {i//20 + 1}/{len(posts)//20 + 1}")
                
                tasks = []
                for post in batch:
                    task = self.process_single_post(client, post)
                    tasks.append(task)
                
                results = await asyncio.gather(*tasks, return_exceptions=True)
                
                # Сохраняем результаты
                for j, result in enumerate(results):
                    if isinstance(result, Exception):
                        logger.error(f"Ошибка в батче {i+j}: {result}")
                        self.stats_today['errors'] += 1
                    else:
                        post = batch[j]
                        self.stats_today['processed'] += 1
                        self.stats_today['views_total'] += result['views']
                        self.stats_today['forwards_total'] += result['forwards']

                # Антифлуд между батчами
                await asyncio.sleep(2)

        # Финальный отчёт
        self.print_final_report()

    async def process_single_post(self, client: TelegramClient, post: Dict):
        """Обрабатывает один пост"""
        stats = await self.get_post_stats_telethon(
            client, 
            post['channel_id'], 
            post['message_id'], 
            post['username']
        )
        
        if stats['error']:
            logger.debug(f"Пост {post['id']}: {stats['error']}")
            return {'views': 0, 'forwards': 0}
        
        await self.save_post_stats(post['id'], stats['views'], stats['forwards'])
        logger.debug(f"✅ {post['post_url']}: {stats['views']} views, {stats['forwards']} forwards")
        
        return stats

    def print_final_report(self):
        """Печатает финальный отчёт"""
        logger.info("📊" + "="*50)
        logger.info(f"✅ Обработано постов: {self.stats_today['processed']}")
        logger.info(f"❌ Ошибок: {self.stats_today['errors']}")
        logger.info(f"👁  Общий прирост просмотров: {self.stats_today['views_total']:,}")
        logger.info(f"🔄 Общий прирост репостов: {self.stats_today['forwards_total']:,}")
        logger.info("📊" + "="*50)

async def main():
    """Главная функция"""
    collector = StatsCollector()
    await collector.collect_stats()

if __name__ == "__main__":
    asyncio.run(main())
