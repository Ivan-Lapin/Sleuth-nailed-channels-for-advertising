import asyncio
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

async def sleep_until_evening(tz_name: str, hour: int) -> None:
    tz = ZoneInfo(tz_name)
    now = datetime.now(tz)
    target = now.replace(hour=hour, minute=0, second=0, microsecond=0)
    if target <= now:
        target += timedelta(days=1)
    await asyncio.sleep((target - now).total_seconds())
