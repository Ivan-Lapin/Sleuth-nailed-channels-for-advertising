# 🔍 Telethon функции
from telethon import TelegramClient
from telethon.tl.functions.channels import CheckSearchPostsFloodRequest, SearchPostsRequest
from database import get_relevant_channels, save_channel_mention
from config import API_ID, API_HASH, MAIN_SESSION, SEARCH_LIMIT, SESSIONS_DIR

# 📈 Статистика канала через Telegram API
async def get_channel_stats(channel_username: str):
    """Возвращает {'id_channel': int, 'title': str, 'subscribers': int}"""
    async with TelegramClient(MAIN_SESSION, API_ID, API_HASH) as client:
        await client.start()
        
        try:
        
            entity = await client.get_entity(f"@{channel_username}")
            
            return {
                'title': entity.title,
                'id_channel': getattr(entity, 'channel_id', entity.id),
                'subscribers': getattr(entity, 'participants_count', 0)
            }
            
        except Exception as e:
            return {'subscribers': 0, 'error': str(e)}


# 📈 Статистика поста через Telegram API
async def get_post_stats(username: str, message_id: int) -> dict:
    """
    channel_identifier: '@username' или channel_id (int)
    Возвращает всегда: {'views': int, 'forwards': int, 'error': str|None}
    """
    async with TelegramClient(MAIN_SESSION, API_ID, API_HASH) as client:
        
        await client.start()
        try:
            channel = await client.get_entity(f"@{username}")
            messages = await client.get_messages(channel, ids=message_id)

            if not messages:
                return {'views': 0, 'forwards': 0, 'error': 'Пост не найден'}

            msg = messages[0]
            if not msg.views:
                return {'error': 'Статистика недоступна'}
            
            return {str(msg.id): { 
                'views': msg.views or 0,
                'forwards': msg.forwards or 0
            }}
        
        except Exception as e:
                return {'error': str(e)}

# 🔍 Глобальный поиск
async def global_brand_search(client, brand):
    """Ищет упоминания бренда и сохраняет в БД"""

    print(f"\n🔍 Глобальный поиск '{brand}'...")
    try:
        flood_remains = 10
        try:
            flood = await client(CheckSearchPostsFloodRequest())
            print(f"   📊 Лимиты: {flood.remains}/{flood.total_daily}")
            if flood.remains <= 0:
                print("❌ Лимит поиска исчерпан!")
                return 0, 0
        except Exception:
            print("   ⚠️ Лимиты недоступны, продолжаем...")

        if flood_remains <= 0:
            print("❌ Лимит исчерпан!")
            return 0, 0

        result = await client(
            SearchPostsRequest(
                query=brand,
                limit=SEARCH_LIMIT,
                offset_rate=0,
                offset_peer=InputPeerEmpty(),
                offset_id=0,
            )
        )

        print(f"📋 {len(result.messages)} сообщений из {len(result.chats)} каналов")

        processed = 0
        for msg in result.messages:
            if hasattr(msg.peer_id, "channel_id") and msg.peer_id.channel_id:
                chat = next((c for c in result.chats if c.id == msg.peer_id.channel_id), None)
                if chat:
                    processed += 1
                    print(f"💾 @{getattr(chat, 'username', 'private')} - {chat.title}")
                    await save_channel_mention(
                        chat.id,
                        chat.title or "",
                        brand,
                        getattr(chat, "username", ""),
                        msg.id,
                        msg.message or "",
                    )
        print(f"✅ {brand}: {processed} обработано")
        return processed, len(get_relevant_channels(brand))
    except Exception as e:
        print(f"❌ {brand}: {e}")
        return 0, 0
