import os
from dataclasses import dataclass
from dotenv import load_dotenv
from pathlib import Path 


def _parse_brands(raw: str) -> list[str]:
    """
    Парсит строку брендов из .env:
    """
    if not raw:
        return []
    return [b.strip() for b in raw.split(",") if b.strip()]

@dataclass(frozen=True)
class Settings:
    pg_dsn: str
    bot_token: str
    api_id: int
    api_hash: str
    session_name: str = "collector"

    # search tuning
    search_limit: int = 100
    tz: str = "Europe/Amsterdam"
    worker_hour_local: int = 23

    # throttling
    sleep_between_brands_sec: float = 2.0
    sleep_between_messages_sec: float = 0.0  # можно >0 если страшно по лимитам
    
    brands: list[str] = None

def load_settings() -> Settings:
    
    brands_raw = os.environ.get("BRANDS", "")
    brands = _parse_brands(brands_raw)
    
    return Settings(
        pg_dsn=os.environ["PG_DSN"],
        bot_token=os.environ["BOT_TOKEN"],
        api_id=int(os.environ["TG_API_ID"]),
        api_hash=os.environ["TG_API_HASH"],
        session_name=os.environ.get("TG_SESSION", "collector"),
        search_limit=int(os.environ.get("SEARCH_LIMIT", "100")),
        tz=os.environ.get("TZ", "Europe/Amsterdam"),
        worker_hour_local=int(os.environ.get("WORKER_HOUR_LOCAL", "23")),
        sleep_between_brands_sec=float(os.environ.get("SLEEP_BETWEEN_BRANDS_SEC", "2.0")),
        sleep_between_messages_sec=float(os.environ.get("SLEEP_BETWEEN_MESSAGES_SEC", "0.0")),
        brands=brands,
    )
