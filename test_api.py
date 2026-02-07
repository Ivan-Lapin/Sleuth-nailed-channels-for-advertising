import asyncio
import logging
import os
import sys
from pathlib import Path

# ВКЛЮЧАЕМ ЛОГИ Telethon
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('telethon_debug.log'),
        logging.StreamHandler(sys.stdout)
    ]
)

print("🚀 СТАРТ СКРИПТА!")
print(f"📂 Рабочая папка: {Path.cwd()}")

from telethon import TelegramClient
from telethon.tl.functions.messages import GetMessagesViewsRequest
from telethon.errors import SessionPasswordNeededError, FloodWaitError

# ТВОИ API данные
API_ID = 31063618
API_HASH = "64120db1d95785c5c4d2f61c8a1cc621"
SESSION_NAME = "test_views_debug"

async def test_views_debug():
    print("🔄 Создаём клиент...")
    
    client = TelegramClient(SESSION_NAME, API_ID, API_HASH)
    
    print("🔄 Подключаемся...")
    try:
        await client.start()
        me = await client.get_me()
        print(f"✅ АВТОРИЗОВАН: @{me.username} (ID: {me.id})")
        
    except SessionPasswordNeededError:
        print("❌ НУЖЕН ПАРОЛЬ 2FA!")
        return
    except Exception as e:
        print(f"❌ ОШИБКА ПОДКЛЮЧЕНИЯ: {e}")
        return
    
    try:
        # ТЕСТ 1: @durov (гарантированно публичный)
        print("\n" + "="*70)
        print("🧪 ТЕСТ 1: @durov")
        print("="*70)
        
        channel = await client.get_entity("durov")
        print(f"📢 Канал: {channel.title}")
        print(f"👥 Подписчики: {getattr(channel, 'participants_count', 'N/A')}")
        
        # Последние 3 поста
        messages = await client.get_messages(channel, limit=3)
        print(f"📬 Постов найдено: {len(messages)}")
        
        for msg in messages:
            print(f"  Пост ID {msg.id}: {msg.date}")
        
        if messages:
            post_ids = [msg.id for msg in messages]
            print(f"🆔 ID для API: {post_ids}")
            
            print("👁️ Запрашиваем просмотры...")
            views_stats = await client(GetMessagesViewsRequest(
                peer=channel,
                id=post_ids,
                increment=False
            ))
            
            print(f"✅ views_stats.views: {len(views_stats.views)} объектов")
            for i, msg_view in enumerate(views_stats.views):
                print(f"  📊 Пост {msg_view.msg_id}:")
                print(f"     views: {getattr(msg_view, 'views', 'None')}")
                print(f"     forwards: {getattr(msg_view, 'forwards', 'None')}")
                print(f"     vars: {vars(msg_view)}")
        
    except Exception as e:
        print(f"❌ ОШИБКА ТЕСТА: {e}")
        import traceback
        traceback.print_exc()
    
    finally:
        print("\n🔌 Отключаемся...")
        await client.disconnect()
        print("🏁 СКРИПТ ЗАВЕРШЁН!")

if __name__ == "__main__":
    print("🐍 Python версия:", sys.version)
    print("📦 Запуск...")
    try:
        asyncio.run(test_views_debug())
    except KeyboardInterrupt:
        print("\n⏹️ Остановлен пользователем")
    except Exception as e:
        print(f"💥 КРИТИЧЕСКАЯ ОШИБКА: {e}")
        import traceback
        traceback.print_exc()
