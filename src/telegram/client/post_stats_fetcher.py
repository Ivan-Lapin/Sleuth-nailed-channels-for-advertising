import re
from dataclasses import dataclass
from telethon import TelegramClient
from telethon.tl.types import Message
from telethon.errors import UsernameNotOccupiedError, UsernameInvalidError
from telethon.tl.types import InputPeerChannel
from telethon.errors import FloodWaitError

USERNAME_RE = re.compile(r"^[A-Za-z][A-Za-z0-9_]{3,30}[A-Za-z0-9]$")

@dataclass
class TgPostStats:
    views: int
    forwards: int | None

async def fetch_post_stats(client, channel_id, access_hash, message_id):

    if not channel_id or not access_hash or not message_id:
        return None

    try:
        peer = InputPeerChannel(int(channel_id), int(access_hash))
        msg = await client.get_messages(peer, ids=message_id)

        if not msg:
            return None

        views = int(getattr(msg, "views", 0) or 0)
        forwards = int(getattr(msg, "forwards", 0) or 0)

        return TgPostStats(views=views, forwards=forwards)

    except FloodWaitError as e:
        print(f"⏳ FloodWait {e.seconds}s in stats fetcher")
        raise

    except Exception:
        return None
