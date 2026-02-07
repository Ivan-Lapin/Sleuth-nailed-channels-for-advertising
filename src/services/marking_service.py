from src.db.repositories.channels_brands import ChannelsBrandsRepo

async def mark_channel(conn, channel_id: int, brand_id: int, status: str, reason: str | None):
    repo = ChannelsBrandsRepo(conn)
    await repo.mark_status(channel_id, brand_id, status, reason)
