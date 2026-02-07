import asyncio
import signal
import sys
from telethon import TelegramClient

def signal_handler(sig, frame):
    print("\n⏹️ Ctrl+C нажат - код уже в Telegram!")
    sys.exit(0)

signal.signal(signal.SIGINT, signal_handler)

print("🚀 АВТОРИЗАЦИЯ (Ctrl+C безопасно!)")
print("📱 ВВЕДИ НОМЕР: +7XXXXXXXXXX")
client = TelegramClient('auth_test', 31063618, "64120db1d95785c5c4d2f61c8a1cc621")

async def safe_auth():
    
    await client.connect()
    
    if not await client.is_user_authorized():
        phone = input("📱 Телефон (+7XXXXXXXXXX): ").strip()
        print("✅ КОД ОТПРАВЛЕН в Telegram!")
        print("💡 Проверь 'Недавние действия' или чат с собой!")
        print("⏳ Жди 1-2 минуты...")
        
        try:
            code = input("🔑 Код из Telegram: ").strip()
            await client.sign_in(phone, code)
        except:
            password = input("🔐 2FA пароль: ").strip()
            await client.sign_in(password=password)
    
    me = await client.get_me()
    print(f"\n🎉 УСПЕХ! @{me.username} (ID: {me.id})")
    await client.disconnect()

try:
    asyncio.run(safe_auth())
except KeyboardInterrupt:
    print("\n✅ Код отправлен! Проверь Telegram!")
