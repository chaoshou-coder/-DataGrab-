#!/usr/bin/env python
"""Process .tick-data/XAGUSD cache into OHLCV CSV outputs.

Reads bi5 files directly from cache (no network). Scans cache, aggregates
ticks into 1m/5m/15m/1d CSV. Compatible with repair/check tooling.
"""

from __future__ import annotations

import argparse
import struct
import sys
from datetime import datetime, timedelta, timezone
from lzma import LZMADecompressor, FORMAT_AUTO
from pathlib import Path

import pandas as pd

_PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from datagrab.tickterial.aggregate import (
    build_1m_bars,
    build_daily_bars_ny_close,
    build_multi_interval_bars,
)
from datagrab.tickterial.common import EXPECTED_COLUMNS
from datagrab.tickterial.runner import write_csv

UTC = timezone.utc
BI5_FORMAT = "!3i2f"  # tm, askp, bidp, askv, bidv
XAGUSD_POINT = 1e3


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Convert XAGUSD tick cache to OHLCV CSV (cache-only, no network)")
    p.add_argument("--cache-dir", default=str(_PROJECT_ROOT / ".tick-data"), help="tick cache root")
    p.add_argument("--output", default=r"E:\stock_data\DateGrab\tickterial_csv\XAGUSD", help="output directory")
    p.add_argument("--intervals", default="1m,5m,15m,1d")
    p.add_argument("--source-timestamp-shift-hours", type=float, default=8.0)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--limit-years", type=int, default=0)
    p.add_argument("--resume", action="store_true", help="skip windows that already have 1m output")
    return p.parse_args()


def scan_cache_hours(symbol_dir: Path) -> list[datetime]:
    """Parse XAGUSD_YYYY-MM-DD_HH filenames -> sorted hour datetimes."""
    hours: list[datetime] = []
    for f in symbol_dir.iterdir():
        if not f.is_file():
            continue
        parts = f.stem.split("_")  # XAGUSD_2016-01-04_00
        if len(parts) != 3 or parts[0] != "XAGUSD":
            continue
        try:
            date_part, hour_part = parts[1], parts[2]
            y, mo, d = (int(x) for x in date_part.split("-"))
            h = int(hour_part)
            dt = datetime(y, mo, d, h, 0, 0, tzinfo=None)
            hours.append(dt)
        except (ValueError, IndexError):
            continue
    return sorted(set(hours))


def iter_year_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    windows: list[tuple[datetime, datetime]] = []
    cur = start
    while cur < end:
        y_end = datetime(cur.year + 1, 1, 1, tzinfo=None)
        if y_end > end:
            y_end = end
        windows.append((cur, y_end))
        cur = y_end
    return windows


def read_bi5_hour(
    cache_path: Path,
    hour_start: datetime,
    window_start: datetime,
    window_end: datetime,
    shift_hours: float,
) -> list[tuple[datetime, float, float | None]]:
    """Read one bi5 file from cache, decompress, parse. No network."""
    if not cache_path.is_file() or cache_path.stat().st_size == 0:
        return []
    try:
        raw = cache_path.read_bytes()
    except OSError:
        return []
    try:
        data = LZMADecompressor(FORMAT_AUTO).decompress(raw)
    except Exception:
        return []
    daystamp = datetime(hour_start.year, hour_start.month, hour_start.day, hour_start.hour)
    hour_end = hour_start + timedelta(hours=1)
    rows: list[tuple[datetime, float, float | None]] = []
    for tm, askp, bidp, askv, bidv in struct.iter_unpack(BI5_FORMAT, data):
        try:
            ts = (daystamp + timedelta(milliseconds=tm)).timestamp()
            tick_dt = datetime.fromtimestamp(ts, tz=UTC).replace(tzinfo=None) + timedelta(hours=shift_hours)
        except (OSError, OverflowError, TypeError):
            continue
        if tick_dt < hour_start or tick_dt >= hour_end:
            continue
        if tick_dt < window_start or tick_dt >= window_end:
            continue
        ask = askp / XAGUSD_POINT
        bid = bidp / XAGUSD_POINT
        price = (ask + bid) / 2.0
        vol = round(askv * 1e6) + round(bidv * 1e6) if (askv or bidv) else None
        rows.append((tick_dt, price, float(vol) if vol is not None else None))
    return rows


def get_cache_path(cache_root: Path, hour: datetime) -> Path:
    """Match tickterial cache path: {cache}/{symbol}/{symbol}_{YYYY-MM-DD}_{HH}"""
    return cache_root / "XAGUSD" / f"XAGUSD_{hour:%Y-%m-%d}_{hour.hour:02d}"


def main() -> int:
    args = parse_args()
    cache_root = Path(args.cache_dir).resolve()
    symbol_dir = cache_root / "XAGUSD"
    if not symbol_dir.is_dir():
        print(f"cache dir not found: {symbol_dir}")
        return 2

    all_hours = scan_cache_hours(symbol_dir)
    if not all_hours:
        print("no cache files found")
        return 2

    first_ts, last_ts = all_hours[0], all_hours[-1]
    print(f"cache coverage: {first_ts.date()} {first_ts.hour:02d}:00 -> {last_ts.date()} {last_ts.hour:02d}:00")
    print(f"total cached hours: {len(all_hours)}")

    windows = iter_year_windows(first_ts, last_ts + timedelta(hours=1))
    if args.limit_years > 0:
        windows = windows[: args.limit_years]
    intervals = [x.strip().lower() for x in args.intervals.split(",") if x.strip()] or ["1m", "5m", "15m", "1d"]

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)

    for win_start, win_end in windows:
        window_hours = [h for h in all_hours if win_start <= h < win_end]
        if not window_hours:
            continue
        tag = f"{win_start:%Y%m%d}_{win_end:%Y%m%d}"
        expected_hours = int((win_end - win_start).total_seconds() // 3600)
        missing = expected_hours - len(window_hours)
        print(f"window {tag}: {len(window_hours)} cached hours, {missing} missing")

        if args.dry_run:
            continue

        if args.resume:
            existing_1m = output_dir / f"XAGUSD_1m_{tag}.csv"
            if existing_1m.exists() and existing_1m.stat().st_size > 0:
                print(f"  skip {tag}: already exists (resume)")
                continue

        all_rows: list[tuple[datetime, float, float | None]] = []
        for h in window_hours:
            path = get_cache_path(cache_root, h)
            rows = read_bi5_hour(path, h, win_start, win_end, args.source_timestamp_shift_hours)
            all_rows.extend(rows)

        if not all_rows:
            print(f"  skip {tag}: no ticks")
            continue

        ticks = (
            pd.DataFrame(all_rows, columns=["datetime", "price", "volume"])
            .drop_duplicates(subset=["datetime"])
            .sort_values("datetime")
            .reset_index(drop=True)
        )

        bars_1m = build_1m_bars(ticks, win_start, win_end)
        to_write: dict[str, pd.DataFrame] = {"1m": bars_1m}
        if "5m" in intervals:
            to_write["5m"] = build_multi_interval_bars(bars_1m, 5, win_start, win_end)
        if "15m" in intervals:
            to_write["15m"] = build_multi_interval_bars(bars_1m, 15, win_start, win_end)
        if "1d" in intervals:
            to_write["1d"] = build_daily_bars_ny_close(bars_1m)

        for iv, df in to_write.items():
            if iv not in intervals:
                continue
            path = output_dir / f"XAGUSD_{iv}_{tag}.csv"
            write_csv(path, df[list(EXPECTED_COLUMNS)])
            print(f"  wrote {path.name} ({len(df)} rows)")

    print("done")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
