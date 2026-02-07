import asyncio

async def soft_sleep(seconds: float) -> None:
    if seconds and seconds > 0:
        await asyncio.sleep(seconds)
