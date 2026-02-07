from __future__ import annotations
from dataclasses import dataclass
from datetime import datetime, time, date, timedelta
from zoneinfo import ZoneInfo

@dataclass(frozen=True)
class DateRange:
    start: date
    end: date  
    
def parse_date_range(text: str) -> DateRange:
    """
    '2026-02-01 - 2026-02-06' -> DateRange(date(2026,2,1), date(2026,2,6))
    """
    s = text.strip().replace("—", "-").replace("–", "-")
    parts = [p.strip() for p in s.split("-")]

    # ожидаем: [YYYY,MM,DD, YYYY,MM,DD] если split по '-'
    # поэтому делаем более надёжно через разделитель " - "
    if " - " in s:
        left, right = [p.strip() for p in s.split(" - ", 1)]
    else:
        # запасной вариант: если человек написал без пробелов
        # попробуем найти две даты регуляркой
        import re
        m = re.findall(r"\d{4}-\d{2}-\d{2}", s)
        if len(m) != 2:
            raise ValueError("Bad format")
        left, right = m[0], m[1]

    start = date.fromisoformat(left)
    end = date.fromisoformat(right)
    if start > end:
        raise ValueError("Start date is after end date")

    return DateRange(start=start, end=end)

def daterange_to_datetimes_utc(dr: DateRange, tz_name: str) -> tuple[datetime, datetime]:
    """
    Возвращает (start_utc, end_utc_exclusive)
    """
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(dr.start, time.min, tzinfo=tz)
    end_local_excl = datetime.combine(dr.end + timedelta(days=1), time.min, tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local_excl.astimezone(ZoneInfo("UTC"))

def today_range_utc(tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz)
    today_dr = DateRange(now_local.date(), now_local.date())
    return daterange_to_datetimes_utc(today_dr, tz_name)

def date_from_days_ago(days_ago: int, tz_name: str) -> date:
    tz = ZoneInfo(tz_name)
    now_local = datetime.now(tz).date()
    return now_local - timedelta(days=days_ago)

def range_to_utc(start_day: date, end_day: date, tz_name: str) -> tuple[datetime, datetime]:
    tz = ZoneInfo(tz_name)
    start_local = datetime.combine(start_day, time.min, tzinfo=tz)
    end_local_excl = datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=tz)
    return start_local.astimezone(ZoneInfo("UTC")), end_local_excl.astimezone(ZoneInfo("UTC"))

def day_range_utc(day: date, tz_name: str) -> tuple[datetime, datetime]:
    return range_to_utc(day, day, tz_name)

