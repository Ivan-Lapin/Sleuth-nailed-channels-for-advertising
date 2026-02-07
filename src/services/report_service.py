from datetime import date
from src.db.repositories.brands import BrandsRepo
from src.db.repositories.channels_brands import ChannelsBrandsRepo
from src.utils.dates import day_range_utc, range_to_utc  # range_to_utc добавим

async def get_brand_list(conn):
    return await BrandsRepo(conn).list_brands()

async def get_channels_for_brand_and_day(
    conn,
    brand_id: int,
    day: date,
    tz_name: str,
    limit: int = 50,
    offset: int = 0,
):
    start_utc, end_utc_excl = day_range_utc(day, tz_name)
    repo = ChannelsBrandsRepo(conn)
    return await repo.list_channels_for_brand_and_range(
        brand_id, start_utc, end_utc_excl, limit=limit, offset=offset
    )

async def get_channels_for_brand_and_range(
    conn,
    brand_id: int,
    start_day: date,
    end_day: date,
    tz_name: str,
    limit: int = 50,
    offset: int = 0,
):
    start_utc, end_utc_excl = range_to_utc(start_day, end_day, tz_name)
    repo = ChannelsBrandsRepo(conn)
    return await repo.list_channels_for_brand_and_range(
        brand_id, start_utc, end_utc_excl, limit=limit, offset=offset
    )
