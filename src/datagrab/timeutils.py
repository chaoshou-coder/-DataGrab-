from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

BEIJING_TZ = ZoneInfo("Asia/Shanghai")


def beijing_now() -> datetime:
    return datetime.now(tz=BEIJING_TZ)


def parse_date(value: str) -> datetime:
    dt = datetime.fromisoformat(value)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=BEIJING_TZ)
    return dt


def to_beijing(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=ZoneInfo("UTC"))
    return dt.astimezone(BEIJING_TZ)


def format_date_for_path(dt: datetime) -> str:
    return to_beijing(dt).strftime("%Y%m%d")


@dataclass(frozen=True)
class DateRange:
    start: datetime
    end: datetime

    def clip_end(self, end: datetime) -> "DateRange":
        return DateRange(self.start, min(self.end, end))


def default_date_range(days: int = 365) -> DateRange:
    end = beijing_now()
    start = end - timedelta(days=days)
    return DateRange(start=start, end=end)
