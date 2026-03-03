"""Concurrent tick data fetching from Dukascopy via tickterial."""

from __future__ import annotations

import logging
import math
import random
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone

import pandas as pd

from .common import to_minute_floor as floor_to_minute
from .exceptions import FetchError

try:
    from tickterial import Tickloader
except ImportError:
    Tickloader = None  # type: ignore

UTC = timezone.utc
DEFAULT_ADAPTIVE_FAIL_RATE = 0.30
logger = logging.getLogger(__name__)


def to_float(value: object) -> float | None:
    """Convert arbitrary value to finite float, or None."""
    if value is None:
        return None
    try:
        value_f = float(value)
    except (TypeError, ValueError):
        return None
    if math.isfinite(value_f):
        return value_f
    return None


def extract_price(row: dict[str, object]) -> float | None:
    """Extract best-effort price from a tick dict (last > price > mid)."""
    for key in ("last", "price"):
        candidate = to_float(row.get(key))
        if candidate is not None:
            return candidate
    ask = to_float(row.get("ask"))
    bid = to_float(row.get("bid"))
    if ask is None and bid is None:
        return None
    if ask is None:
        return bid
    if bid is None:
        return ask
    return (ask + bid) * 0.5


def extract_volume(row: dict[str, object]) -> float | None:
    """Extract tick volume from ask-vol / bid-vol fields."""
    keys = ("ask-vol", "bid-vol", "ask_vol", "bid_vol")
    values = [to_float(row.get(k)) for k in keys]
    if all(v is None for v in values):
        return None
    return sum(v for v in values if v is not None)


@dataclass(frozen=True)
class HourTaskResult:
    """Result of downloading one hour of tick data."""
    cursor: datetime
    ticks: list[tuple[datetime, float, float | None]]
    retries: int


def _fetch_hour_ticks(
    symbol: str,
    hour_start: datetime,
    window_start: datetime,
    window_end: datetime,
    cache_dir: str,
    max_retries: int,
    retry_delay: float,
    retry_jitter_ms: int,
    source_timestamp_shift_hours: float,
) -> HourTaskResult:
    """Fetch ticks for a single hour slot with retries."""
    rows: list[tuple[datetime, float, float | None]] = []
    hour_end = hour_start + timedelta(hours=1)
    loader = Tickloader(pack=False, cachedir=cache_dir)
    attempt = 0
    raw_count = 0
    parsed_ts_count = 0
    while True:
        try:
            raw = list(loader.download(symbol, hour_start, utcoffset=0) or [])
            raw_count = len(raw)
            break
        except (FetchError, RuntimeError, OSError, ValueError, TimeoutError, ConnectionError) as exc:
            attempt += 1
            if attempt > max_retries:
                raise FetchError(
                    f"tick download failed for {symbol} {hour_start.isoformat()} after {max_retries} retries: {exc}"
                ) from exc
            base_delay = min(60.0, retry_delay * (2 ** (attempt - 1)))
            jitter = retry_jitter_ms / 1000.0
            delay = base_delay + random.uniform(0.0, jitter)
            logger.warning(
                "retry %d/%d for %s %s after error: %s; sleep %.1fs",
                attempt,
                max_retries,
                symbol,
                hour_start.isoformat(),
                exc,
                delay,
            )
            time.sleep(delay)

    for item in raw:
        if not isinstance(item, dict):
            continue
        ts = to_float(item.get("timestamp"))
        if ts is None:
            continue
        parsed_ts_count += 1
        try:
            tick_dt = datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None) + timedelta(
                hours=source_timestamp_shift_hours
            )
        except (OSError, OverflowError, TypeError):
            continue
        if tick_dt < hour_start or tick_dt >= hour_end:
            continue
        if tick_dt < window_start or tick_dt >= window_end:
            continue
        price = extract_price(item)
        if price is None:
            continue
        volume = extract_volume(item)
        rows.append((tick_dt, price, volume))
    if raw_count > 0 and parsed_ts_count > 0 and not rows:
        raise FetchError(
            f"download returned {raw_count} rows but no valid ticks remained for {symbol} {hour_start.isoformat()}"
        )
    return HourTaskResult(cursor=hour_start, ticks=rows, retries=max(0, attempt))


def fetch_ticks(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    max_retries: int,
    retry_delay: float,
    download_workers: int,
    batch_size: int,
    batch_pause_ms: int,
    retry_jitter_ms: int,
    cache_dir: str,
    source_timestamp_shift_hours: float,
) -> pd.DataFrame:
    """Fetch all ticks for a time window using concurrent hourly batches.

    Args:
        symbol: Instrument identifier (e.g. ``XAUUSD``).
        window_start: Inclusive start of the time window (UTC or naive-UTC).
        window_end: Exclusive end of the time window.
        max_retries: Max retries per hour slot.
        retry_delay: Base delay between retries in seconds.
        download_workers: Max concurrent workers.
        batch_size: Hour slots per batch.
        batch_pause_ms: Pause between batches in milliseconds.
        retry_jitter_ms: Random jitter added to delays.
        cache_dir: Tickterial local cache directory.
        source_timestamp_shift_hours: Offset applied to raw timestamps.

    Returns:
        DataFrame with columns ``[datetime, price, volume]``, sorted, deduplicated.

    Raises:
        FetchError: If any batch has unrecoverable failures.
    """
    if window_start.tzinfo is None:
        start_naive = window_start
    else:
        start_naive = window_start.astimezone(UTC).replace(tzinfo=None)
    if window_end.tzinfo is None:
        end_naive = window_end
    else:
        end_naive = window_end.astimezone(UTC).replace(tzinfo=None)
    if start_naive >= end_naive:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    cursor = floor_to_minute(start_naive, 60)
    if cursor < start_naive:
        cursor += timedelta(hours=1)

    hours: list[datetime] = []
    while cursor < end_naive:
        hours.append(cursor)
        cursor += timedelta(hours=1)

    if not hours:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    current_workers = max(1, download_workers)
    current_batch_size = max(1, batch_size)
    pause_seconds = max(0.0, batch_pause_ms / 1000.0)
    all_rows: list[tuple[datetime, float, float | None]] = []
    batch_errors: list[str] = []
    total_batches = (len(hours) + current_batch_size - 1) // current_batch_size

    for batch_index in range(total_batches):
        batch = hours[batch_index * current_batch_size : (batch_index + 1) * current_batch_size]
        batch_total = len(batch)
        batch_fails = 0
        batch_retries = 0
        logger.info(
            "[batch %d/%d] start %s size %d",
            batch_index + 1,
            total_batches,
            batch[0].isoformat(),
            batch_total,
        )

        with ThreadPoolExecutor(max_workers=current_workers) as executor:
            futures = {
                executor.submit(
                    _fetch_hour_ticks,
                    symbol,
                    hour,
                    start_naive,
                    end_naive,
                    cache_dir,
                    max_retries,
                    retry_delay,
                    retry_jitter_ms,
                    source_timestamp_shift_hours,
                ): hour
                for hour in batch
            }
            for future in as_completed(futures):
                hour = futures[future]
                try:
                    result = future.result()
                except (FetchError, RuntimeError, ValueError, TimeoutError) as exc:
                    batch_fails += 1
                    logger.warning("hour failed: %s %s %s", symbol, hour.isoformat(), exc)
                    batch_errors.append(f"{hour.isoformat()}:{exc}")
                    continue
                batch_retries += result.retries
                all_rows.extend(result.ticks)

        fail_ratio = batch_fails / batch_total if batch_total else 0.0
        logger.info(
            "[batch %d/%d] done: success=%d failed=%d retries=%d fail_rate=%.1f workers=%d pause_ms=%d",
            batch_index + 1,
            total_batches,
            batch_total - batch_fails,
            batch_fails,
            batch_retries,
            fail_ratio,
            current_workers,
            int(pause_seconds * 1000),
        )

        if fail_ratio > DEFAULT_ADAPTIVE_FAIL_RATE and current_batch_size > 1:
            next_workers = max(1, current_workers - 1)
            if next_workers != current_workers:
                logger.warning("adaptive throttle: reduce workers %d -> %d", current_workers, next_workers)
                current_workers = next_workers
            next_pause = min(30_000, int(pause_seconds * 1000.0 * 2))
            if next_pause > int(pause_seconds * 1000):
                logger.warning(
                    "adaptive throttle: increase batch pause %dms -> %dms",
                    int(pause_seconds * 1000),
                    next_pause,
                )
                pause_seconds = next_pause / 1000.0

        if batch_fails == 0 and current_workers < download_workers:
            recovery_workers = min(download_workers, current_workers + 1)
            if recovery_workers != current_workers:
                logger.info("adaptive recover: increase workers %d -> %d", current_workers, recovery_workers)
                current_workers = recovery_workers
            recovery_pause = max(max(1, batch_pause_ms) / 1000.0, pause_seconds * 0.75)
            if recovery_pause < pause_seconds:
                logger.info(
                    "adaptive recover: reduce pause %dms -> %dms",
                    int(pause_seconds * 1000),
                    int(recovery_pause * 1000),
                )
                pause_seconds = recovery_pause

        if batch_index < total_batches - 1 and pause_seconds > 0:
            jitter = retry_jitter_ms / 1000.0
            time.sleep(pause_seconds + random.uniform(0.0, jitter))

    if batch_errors:
        first_errors = "; ".join(batch_errors[:5])
        if len(batch_errors) > 5:
            first_errors += f"; ... (+{len(batch_errors) - 5} more)"
        raise FetchError(f"tick download failed for {symbol}: {len(batch_errors)} hour windows failed: {first_errors}")

    if not all_rows:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    out = pd.DataFrame(all_rows, columns=["datetime", "price", "volume"]).sort_values("datetime")
    if out.empty:
        return pd.DataFrame(columns=["datetime", "price", "volume"])
    out = out.drop_duplicates(subset=["datetime"])
    return out.reset_index(drop=True)
