"""CLI orchestration for tickterial download pipeline."""

from __future__ import annotations

import argparse
import csv
import logging
import math
import random
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .aggregate import (
    build_1m_bars,
    build_daily_bars_ny_close,
    build_multi_interval_bars,
    check_interval_integrity,
    check_ny_close_alignment,
    check_ohlc_consistency,
)
from .common import (
    EXPECTED_COLUMNS,
    iter_year_windows as common_iter_year_windows,
    parse_dt,
    parse_intervals,
    parse_symbols,
)
from .exceptions import AggregationError, FetchError
from .fetch import Tickloader, fetch_ticks

UTC = timezone.utc
logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class FailedWindow:
    """Record of a window that failed to download."""
    symbol: str
    start: datetime
    end: datetime
    reason: str


def parse_args() -> argparse.Namespace:
    """Build CLI argument parser for the standalone download command."""
    parser = argparse.ArgumentParser(description="download XAU/XAG tick data via tickterial")
    parser.add_argument("--symbols", default="XAUUSD,XAGUSD", help="symbols, comma separated")
    parser.add_argument("--start", required=True, help="start datetime, e.g. 2016-01-01T00:00:00")
    parser.add_argument("--end", required=True, help="end datetime, e.g. 2026-01-01T00:00:00")
    parser.add_argument("--output", default="tickterial_csv", help="output directory")
    parser.add_argument("--cache-dir", default=".tick-data", help="tickterial cache directory")
    parser.add_argument("--intervals", default="1m,5m,15m,1d", help="output intervals, default 1m,5m,15m,1d")
    parser.add_argument("--max-retries", type=int, default=6, help="retry times per hourly tick call")
    parser.add_argument("--retry-delay", type=float, default=1.5, help="base delay seconds between retries")
    parser.add_argument("--download-workers", type=int, default=4, help="max concurrent hourly tick workers")
    parser.add_argument("--batch-size", type=int, default=8, help="hourly tasks per batch")
    parser.add_argument("--batch-pause-ms", type=int, default=1000, help="pause milliseconds between batches")
    parser.add_argument("--retry-jitter-ms", type=int, default=300, help="jitter milliseconds added to retry and batch sleeps")
    parser.add_argument("--source-timestamp-shift-hours", type=float, default=8.0, help="shift raw tick timestamps to UTC")
    parser.add_argument("--resume-failures", default="", help="retry only windows in a previous failures_mvp.csv")
    parser.add_argument("--validate", action="store_true", help="run OHLCV consistency checks")
    parser.add_argument("--strict-validate", action=argparse.BooleanOptionalAction, default=True, help="fail window when integrity checks fail")
    parser.add_argument("--window-retries", type=int, default=1, help="retry full window on failure, default 1")
    parser.add_argument("--log-level", default="INFO", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"], help="log level")
    parser.add_argument("--suppress-tickloader-info", action="store_true", help="suppress tickterial.tickloader INFO logs")
    parser.add_argument("--force", action="store_true", help="overwrite existing yearly CSV outputs")
    return parser.parse_args()


def configure_logging(log_level: str, suppress_tickloader_info: bool) -> None:
    """Configure stdlib logging for the download pipeline."""
    root = logging.getLogger()
    root.setLevel(log_level.upper())
    if not root.handlers:
        handler = logging.StreamHandler()
        formatter = logging.Formatter("%(levelname)s:%(name)s:%(message)s")
        handler.setFormatter(formatter)
        root.addHandler(handler)

    tickterial_tickloader_logger = logging.getLogger("tickterial.tickloader")
    if suppress_tickloader_info:
        tickterial_tickloader_logger.setLevel(logging.WARNING)
    else:
        tickterial_tickloader_logger.setLevel(logging.INFO)


def read_failed_windows(path: str) -> set[tuple[str, str]]:
    """Read a failures_mvp.csv and return the set of ``(symbol, start:end)`` keys."""
    if not path:
        return set()
    p = Path(path)
    if not p.exists():
        return set()
    targets: set[tuple[str, str]] = set()
    with p.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            symbol = (row.get("symbol") or "").strip().upper()
            start = (row.get("start") or "").strip()
            end = (row.get("end") or "").strip()
            interval = (row.get("interval") or "").strip()
            if symbol and start and end and interval == "mvp":
                targets.add((symbol, f"{start}:{end}"))
    return targets


def iter_year_windows(start: datetime, end: datetime) -> list[tuple[datetime, datetime]]:
    """Split a date range into per-year windows."""
    return common_iter_year_windows(start, end)


def _with_atomic_write(path: Path, rows: list[dict[str, str]], fieldnames: list[str]) -> None:
    """Write CSV rows atomically via tmp + replace."""
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    try:
        with tmp_path.open("w", encoding="utf-8", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(rows)
    except PermissionError as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(f"cannot write temp csv, file may be in use: {tmp_path}") from exc
    try:
        tmp_path.replace(path)
    except PermissionError as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(f"cannot replace target file, it may be locked by another process: {path}") from exc


def append_failure(path: Path, entry: FailedWindow) -> None:
    """Append a failed window record to the failures CSV."""
    entries: list[dict[str, str]] = []
    if path.exists():
        try:
            with path.open("r", encoding="utf-8", newline="") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if row:
                        entries.append(row)
        except PermissionError as exc:
            raise RuntimeError(f"cannot read failure log, it may be locked: {path}") from exc
    entries.append(
        {
            "version": "1",
            "symbol": entry.symbol,
            "interval": "mvp",
            "start": entry.start.isoformat(),
            "end": entry.end.isoformat(),
            "asset_type": "commodity",
            "adjust": "none",
            "reason": entry.reason,
            "created_at": datetime.now(tz=UTC).isoformat(),
        }
    )
    fieldnames = ["version", "symbol", "interval", "start", "end", "asset_type", "adjust", "reason", "created_at"]
    _with_atomic_write(path, entries, fieldnames)


def write_csv(path: Path, bars: pd.DataFrame) -> None:
    """Write OHLCV bars to CSV atomically."""
    path.parent.mkdir(parents=True, exist_ok=True)
    out = bars.copy()
    out["datetime"] = pd.to_datetime(out["datetime"]).dt.strftime("%Y-%m-%d %H:%M:%S")
    tmp_path = path.with_suffix(f"{path.suffix}.tmp")
    try:
        out.to_csv(tmp_path, index=False, columns=EXPECTED_COLUMNS)
    except PermissionError as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(f"cannot write csv to temp file, it may be in use: {tmp_path}") from exc
    try:
        tmp_path.replace(path)
    except PermissionError as exc:
        if tmp_path.exists():
            tmp_path.unlink()
        raise RuntimeError(f"cannot replace target csv, it may be in use: {path}") from exc


def run(args: argparse.Namespace | None = None) -> int:
    """Main entry point for the download pipeline.

    Args:
        args: Pre-parsed CLI arguments, or ``None`` to parse from ``sys.argv``.

    Returns:
        Exit code: 0 on success, 1 if any window failed, 2 on invalid input.
    """
    if Tickloader is None:
        logger.error("tickterial package not available, run `pip install tickterial` first.")
        return 2

    args = args or parse_args()
    symbols = parse_symbols(args.symbols)
    intervals = parse_intervals(args.intervals)
    start = parse_dt(args.start)
    end = parse_dt(args.end)
    if start >= end:
        logger.error("start must be earlier than end")
        return 2
    if args.download_workers < 1:
        logger.error("--download-workers must be >= 1")
        return 2
    if args.batch_size < 1:
        logger.error("--batch-size must be >= 1")
        return 2
    if args.batch_pause_ms < 0:
        logger.error("--batch-pause-ms must be >= 0")
        return 2
    if args.max_retries < 0:
        logger.error("--max-retries must be >= 0")
        return 2
    if not math.isfinite(args.source_timestamp_shift_hours):
        logger.error("--source-timestamp-shift-hours must be finite")
        return 2
    if args.window_retries < 0:
        logger.error("--window-retries must be >= 0")
        return 2
    configure_logging(args.log_level, args.suppress_tickloader_info)

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    fail_csv = output_dir / "failures_mvp.csv"
    retry = read_failed_windows(args.resume_failures) if args.resume_failures else set()
    window_attempts = max(1, args.window_retries + 1)

    has_failure = False
    for symbol in symbols:
        for win_start, win_end in iter_year_windows(start, end):
            win_key = f"{win_start.isoformat()}:{win_end.isoformat()}"
            if retry and (symbol, win_key) not in retry:
                continue

            window_tag = f"{win_start:%Y%m%d}_{win_end:%Y%m%d}"
            logger.info("processing %s %s", symbol, window_tag)

            outputs: dict[str, Path] = {
                interval: output_dir / f"{symbol}_{interval}_{window_tag}.csv"
                for interval in intervals
            }
            if not args.force and all(path.exists() for path in outputs.values()):
                logger.info("skip existing: %s %s", symbol, window_tag)
                continue

            last_error: Exception | None = None
            completed = False
            for attempt in range(1, window_attempts + 1):
                try:
                    if window_attempts > 1:
                        logger.info("window attempt %d/%d: %s %s", attempt, window_attempts, symbol, window_tag)

                    ticks = fetch_ticks(
                        symbol,
                        win_start,
                        win_end,
                        args.max_retries,
                        args.retry_delay,
                        args.download_workers,
                        args.batch_size,
                        args.batch_pause_ms,
                        args.retry_jitter_ms,
                        args.cache_dir,
                        args.source_timestamp_shift_hours,
                    )
                    if ticks.empty:
                        logger.warning("%s %s: no ticks in window", symbol, window_tag)
                        if args.strict_validate and (win_end - win_start) >= timedelta(days=7):
                            raise AggregationError(
                                f"strict validation failed: empty tick window for {symbol} {window_tag}"
                            )

                    bars_1m = build_1m_bars(ticks, win_start, win_end)
                    to_write: dict[str, pd.DataFrame] = {"1m": bars_1m}
                    if "5m" in intervals:
                        to_write["5m"] = build_multi_interval_bars(bars_1m, 5, win_start, win_end)
                    if "15m" in intervals:
                        to_write["15m"] = build_multi_interval_bars(bars_1m, 15, win_start, win_end)
                    if "1d" in intervals:
                        to_write["1d"] = build_daily_bars_ny_close(bars_1m)

                    consistency_errors: list[str] = []
                    if args.validate:
                        for interval in intervals:
                            if interval not in to_write:
                                continue
                            try:
                                issues = check_ohlc_consistency(interval, ticks, to_write[interval], base_1m=bars_1m)
                            except (AggregationError, RuntimeError, ValueError, TypeError) as exc:
                                issues = [f"{interval}: validate error: {exc}"]
                            for issue in issues:
                                logger.warning(issue)
                                if issue.endswith("ohlcv ok") or issue.endswith("raw empty, skip"):
                                    continue
                                consistency_errors.append(issue)
                        if "1d" in intervals:
                            try:
                                alignment_df = to_write.get("1d", pd.DataFrame(columns=EXPECTED_COLUMNS))
                                alignment = check_ny_close_alignment(alignment_df)
                                logger.info(alignment)
                                if alignment != "1d alignment: ok":
                                    consistency_errors.append(alignment)
                            except (AggregationError, RuntimeError, ValueError, TypeError) as exc:
                                issue = f"1d alignment: validate error: {exc}"
                                logger.error(issue)
                                consistency_errors.append(issue)

                    strict_issues: list[str] = []
                    if args.strict_validate:
                        for interval in intervals:
                            if interval not in to_write:
                                continue
                            strict_issues.extend(check_interval_integrity(interval, to_write[interval], win_start, win_end))
                        strict_issues.extend(consistency_errors)
                        if strict_issues:
                            unique_issues = list(dict.fromkeys(strict_issues))
                            preview = "; ".join(unique_issues[:8])
                            if len(unique_issues) > 8:
                                preview += f"; ... (+{len(unique_issues) - 8} more)"
                            raise AggregationError(f"strict validation failed: {preview}")

                    for interval, df in to_write.items():
                        if interval in outputs:
                            # pandas 将 tuple 视作单列键；这里需要显式按多列切片。
                            write_csv(outputs[interval], df.loc[:, list(EXPECTED_COLUMNS)])

                    completed = True
                    break
                except (FetchError, AggregationError, RuntimeError, ValueError, OSError, TimeoutError) as exc:
                    last_error = exc
                    if attempt >= window_attempts:
                        break
                    base_delay = min(60.0, args.retry_delay * (2 ** (attempt - 1)))
                    jitter = args.retry_jitter_ms / 1000.0
                    delay = base_delay + random.uniform(0.0, jitter)
                    logger.warning(
                        "window retry %d/%d for %s %s after error: %s; sleep %.1fs",
                        attempt,
                        window_attempts - 1,
                        symbol,
                        window_tag,
                        exc,
                        delay,
                    )
                    time.sleep(delay)

            if not completed:
                has_failure = True
                reason = str(last_error) if last_error is not None else "unknown window failure"
                append_failure(fail_csv, FailedWindow(symbol=symbol, start=win_start, end=win_end, reason=reason))
                logger.error("window failed: %s %s: %s", symbol, window_tag, reason)
                continue

    return 1 if has_failure else 0


if __name__ == "__main__":
    raise SystemExit(run())
