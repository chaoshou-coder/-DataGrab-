from __future__ import annotations

from datetime import datetime, timezone
from typing import Iterable

import pandas as pd
from zoneinfo import ZoneInfo

UTC = timezone.utc
NY_TZ = ZoneInfo("America/New_York")
VALID_INTERVALS = ("1m", "5m", "15m", "1d")
EXPECTED_COLUMNS = ("datetime", "open", "high", "low", "close", "volume")
COUNT_CHECK_INTERVALS = {"1m", "5m", "15m"}


def parse_dt(raw: str) -> datetime:
    """Parse an ISO-format datetime string into a UTC-aware datetime.

    Args:
        raw: ISO-format datetime string.

    Returns:
        A timezone-aware datetime in UTC.
    """
    dt = datetime.fromisoformat(raw)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def parse_symbols(raw: str) -> list[str]:
    """Parse a comma-separated string of symbols into uppercase tokens.

    Args:
        raw: Comma-separated symbol string (e.g. ``"XAUUSD,XAGUSD"``).

    Returns:
        List of trimmed, uppercase symbol strings.
    """
    return [x.strip().upper() for x in raw.split(",") if x.strip()]


def parse_intervals(raw: str, valid_intervals: Iterable[str] = VALID_INTERVALS) -> list[str]:
    """Parse and normalize a comma-separated interval string.

    Accepts shorthand notations (e.g. ``"1"`` -> ``"1d"``,
    ``"5min"`` -> ``"5m"``) and validates against the set of allowed
    intervals. Returns all valid intervals when *raw* is empty.

    Args:
        raw: Comma-separated interval string (e.g. ``"1m,5m,1d"``).
        valid_intervals: Allowed interval values.

    Returns:
        List of normalized interval strings.

    Raises:
        ValueError: If any interval is not in the valid set.
    """
    val_set = set(valid_intervals)
    vals = [x.strip().lower() for x in raw.split(",") if x.strip()]
    normalized: list[str] = []
    for value in vals:
        token = value
        if token.isdigit():
            candidate = f"{token}d"
            if candidate in val_set:
                token = candidate
        elif token.endswith("min") and token[:-3].isdigit():
            candidate = f"{token[:-3]}m"
            if candidate in val_set:
                token = candidate
        elif token.endswith("day") and token[:-3].isdigit():
            candidate = f"{token[:-3]}d"
            if candidate in val_set:
                token = candidate
        normalized.append(token)
    if not normalized:
        return list(valid_intervals)
    invalid = [v for v in normalized if v not in val_set]
    if invalid:
        raise ValueError(f"invalid intervals: {', '.join(invalid)}")
    return normalized


def to_minute_floor(dt: datetime, minutes: int) -> datetime:
    """Floor a datetime to the nearest lower *minutes* boundary.

    Args:
        dt: The datetime to floor.
        minutes: The minute-granularity for flooring.

    Returns:
        A naive UTC datetime floored to the given minute boundary.
    """
    ts = pd.Timestamp(dt)
    if ts.tzinfo is None:
        base = ts.to_pydatetime().replace(tzinfo=None)
    else:
        base = ts.tz_convert("UTC").to_pydatetime().replace(tzinfo=None)
    return base - pd.Timedelta(
        minutes=base.minute % minutes,
        seconds=base.second,
        microseconds=base.microsecond,
    ).to_pytimedelta()


def to_hour_floor(dt: datetime, floor_up: bool = False) -> datetime:
    """Floor (or ceiling) a datetime to the nearest hour boundary.

    Args:
        dt: The datetime to round.
        floor_up: If ``True``, round up to the next hour when *dt* is not
            already on an hour boundary.

    Returns:
        A datetime with minutes, seconds, and microseconds zeroed out.
    """
    original = dt
    floored = dt.replace(second=0, microsecond=0, minute=0)
    if floor_up and floored < original:
        return floored + pd.Timedelta(hours=1).to_pytimedelta()
    return floored


def to_naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to a naive (tzinfo-free) UTC datetime.

    Args:
        dt: A datetime, optionally timezone-aware.

    Returns:
        A naive datetime representing the equivalent UTC time.
    """
    if dt.tzinfo is None:
        return dt
    return dt.astimezone(UTC).replace(tzinfo=None)


def iter_year_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split a date range into yearly windows.

    Each window starts at the beginning of a calendar year and ends at the
    start of the next year, except for the final window which ends at *end*.

    Args:
        start: Start of the range (converted to naive UTC).
        end: End of the range (converted to naive UTC).

    Returns:
        List of ``(window_start, window_end)`` tuples. Empty if
        ``start >= end``.
    """
    windows: list[tuple[datetime, datetime]] = []
    current = to_naive_utc(start)
    limit = to_naive_utc(end)
    if current >= limit:
        return windows
    while current < limit:
        year_end = datetime(current.year + 1, 1, 1, tzinfo=None)
        if year_end > limit:
            year_end = limit
        windows.append((current, year_end))
        current = year_end
    return windows


def build_expected_index(start: datetime, end: datetime, interval: str) -> pd.DatetimeIndex:
    """Build the expected minute-level DatetimeIndex for a window and interval.

    Args:
        start: Window start datetime.
        end: Window end datetime.
        interval: Interval string (e.g. ``"1m"``, ``"5m"``, ``"15m"``).

    Returns:
        A ``pd.DatetimeIndex`` with the expected timestamps (left-inclusive).
    """
    step = int(interval[:-1])
    start_floor = to_minute_floor(start, step)
    end_floor = to_minute_floor(end, 1)
    return pd.date_range(start=start_floor, end=end_floor, freq=f"{step}min", inclusive="left")


def build_daily_bars_ny_close(base_1m: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 1-minute bars into daily bars using the 17:00 New York close.

    Each daily bar is keyed to the New York 17:00 session close. OHLCV values
    are aggregated from the underlying 1-minute data.

    Args:
        base_1m: DataFrame with columns
            ``datetime, open, high, low, close, volume``.

    Returns:
        DataFrame with the same columns, aggregated to daily bars. Returns an
        empty DataFrame if the input is empty or has no valid datetimes.
    """
    if base_1m.empty:
        return pd.DataFrame(columns=list(EXPECTED_COLUMNS))
    if "datetime" not in base_1m.columns:
        return pd.DataFrame(columns=list(EXPECTED_COLUMNS))

    utc_dt = pd.to_datetime(base_1m["datetime"], errors="coerce", utc=True)
    valid_dt = utc_dt.notna()
    if not valid_dt.any():
        return pd.DataFrame(columns=list(EXPECTED_COLUMNS))

    m1 = base_1m.loc[valid_dt, ["open", "high", "low", "close", "volume"]].copy()
    for col in ("open", "high", "low", "close", "volume"):
        m1[col] = pd.to_numeric(m1[col], errors="coerce")

    ny_dt = utc_dt.loc[valid_dt].dt.tz_convert(NY_TZ)
    ny_naive = ny_dt.dt.tz_localize(None)
    after_close = (ny_naive.dt.hour >= 17).astype(int)
    session_date = ny_naive.dt.floor("D") + pd.to_timedelta(after_close, unit="D")
    session_close_naive = session_date + pd.Timedelta(hours=17)
    m1["session_close"] = session_close_naive.dt.tz_localize(NY_TZ)

    grouped = m1.groupby("session_close", sort=True)[["open", "high", "low", "close", "volume"]]
    bars = grouped.agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": lambda s: s.sum(min_count=1),
        }
    )
    bars = bars.rename_axis(index="datetime").reset_index()
    bars["datetime"] = bars["datetime"].dt.tz_convert("UTC").dt.tz_localize(None).astype("datetime64[ns]")
    return bars[list(EXPECTED_COLUMNS)]
