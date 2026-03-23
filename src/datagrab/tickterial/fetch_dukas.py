"""Dukascopy downloader using dukascopy-python / dukascopy-node.

This module provides a high-performance tick download backend using the
Dukascopy data feed, which is significantly more active than tickterial.
See: dukascopy-python or dukascopy-node on npm/pypi.
"""

from __future__ import annotations

import asyncio
import logging
import time
from datetime import datetime, timezone

import pandas as pd

from .exceptions import FetchError

logger = logging.getLogger(__name__)

_DUKAS_AVAILABLE = False
_download_func = None
_read_func = None

try:
    from dukascopy import (
        History,
        Interval,
        from_datetime,
        to_datetime,
    )

    _DUKAS_AVAILABLE = True
    _download_func = "dukascopy_python"
except Exception:
    _DUKAS_AVAILABLE = False


DUKAS_AVAILABLE = _DUKAS_AVAILABLE


def _to_utc(dt: datetime) -> datetime:
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def _dukascopy_interval(interval: str) -> Interval | None:
    mapping = {
        "1m": Interval.M1,
        "5m": Interval.M5,
        "15m": Interval.M15,
        "30m": Interval.M30,
        "1h": Interval.H1,
        "4h": Interval.H4,
        "1d": Interval.D1,
    }
    return mapping.get(interval.strip().lower())


async def fetch_ticks_async(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    """Fetch ticks from Dukascopy using dukascopy-python.

    Args:
        symbol: Instrument symbol (e.g. "XAUUSD", "XAGUSD").
        window_start: Start of the download window.
        window_end: End of the download window.

    Returns:
        DataFrame with datetime, price, volume columns.
    """
    if not DUKAS_AVAILABLE:
        raise FetchError("dukascopy-python is not installed: pip install dukascopy")

    if window_start >= window_end:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    start_naive = _to_utc(window_start).replace(tzinfo=None)
    end_naive = _to_utc(window_end).replace(tzinfo=None)

    logger.info("dukascopy downloading symbol=%s %s -> %s", symbol, start_naive.isoformat(), end_naive.isoformat())

    try:
        from dukascopy import History

        hist = History(
            symbol=symbol,
            start=from_datetime(start_naive),
            end=from_datetime(end_naive),
            interval=Interval.TICK,
        )

        ticks_data: list[dict] = []
        async for row in hist:
            ticks_data.append(
                {
                    "datetime": row[0],
                    "price": float(row[1]) if row[1] is not None else None,
                    "volume": float(row[2]) if row[2] is not None else 0.0,
                }
            )

        if not ticks_data:
            return pd.DataFrame(columns=["datetime", "price", "volume"])

        df = pd.DataFrame(ticks_data)
        df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
        df["datetime"] = df["datetime"].dt.tz_convert("UTC").dt.tz_localize(None)
        df = df.dropna(subset=["datetime", "price"])
        df = df.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
        return df[["datetime", "price", "volume"]]

    except Exception as exc:
        raise FetchError(f"dukascopy fetch failed for {symbol}: {exc}") from exc


def fetch_ticks(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
) -> pd.DataFrame:
    """Synchronous wrapper of :func:`fetch_ticks_async`."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(fetch_ticks_async(symbol, window_start, window_end))
    raise FetchError(
        "fetch_ticks called within a running asyncio loop; "
        "call fetch_ticks_async directly instead"
    )
