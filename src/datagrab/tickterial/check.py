#!/usr/bin/env python
"""Check integrity of tickterial output artifacts for configured symbols and windows."""

from __future__ import annotations

import argparse
import csv
import json
import logging
import sys
from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from pathlib import Path
from subprocess import list2cmdline
from zoneinfo import ZoneInfo
from typing import Any

from .common import (
    COUNT_CHECK_INTERVALS,
    EXPECTED_COLUMNS,
    VALID_INTERVALS,
    build_expected_index as common_build_expected_index,
    iter_year_windows as common_iter_year_windows,
    parse_dt as common_parse_dt,
    parse_intervals as common_parse_intervals,
    parse_symbols as common_parse_symbols,
    to_minute_floor as common_to_minute_floor,
    to_naive_utc as common_to_naive_utc,
)

import numpy as np
import pandas as pd


UTC = timezone.utc
NY_TZ = ZoneInfo("America/New_York")
logger = logging.getLogger(__name__)


@dataclass(frozen=True, order=True)
class Window:
    """Immutable time window defined by a start and end datetime."""

    start: datetime
    end: datetime

    def tag(self) -> str:
        return f"{self.start:%Y%m%d}_{self.end:%Y%m%d}"


@dataclass
class FileResult:
    """Result of a single file integrity check."""

    symbol: str
    interval: str
    window: str
    file: str
    status: str
    fails: list[str] = field(default_factory=list)
    warns: list[str] = field(default_factory=list)
    metrics: dict[str, Any] = field(default_factory=dict)
    command: str | None = None


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the check command.

    Args:
        argv: Command-line argument list, or ``None`` for ``sys.argv``.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description="Validate tickterial CSV outputs")
    parser.add_argument("--symbols", default="XAUUSD,XAGUSD", help="comma separated symbols")
    parser.add_argument(
        "--start",
        required=True,
        help="start datetime, e.g. 2016-01-01T00:00:00",
    )
    parser.add_argument(
        "--end",
        required=True,
        help="end datetime, e.g. 2026-01-01T00:00:00",
    )
    parser.add_argument(
        "--output",
        default="tickterial_csv",
        help="tickterial output directory",
    )
    parser.add_argument(
        "--intervals",
        default="1m,5m,15m,1d",
        help="intervals to check, default 1m,5m,15m,1d",
    )
    parser.add_argument(
        "--report-json",
        default="",
        help="write machine-readable json report to this file",
    )
    parser.add_argument(
        "--report-csv",
        default="",
        help="write summary csv report to this file",
    )
    parser.add_argument(
        "--emit-repair-commands",
        action="store_true",
        help="print one repair command per failed window",
    )
    parser.add_argument(
        "--repair-command-file",
        default="",
        help="optional path to save repair commands",
    )
    parser.add_argument(
        "--repair-cache-dir",
        default=".tick-data",
        help="cache dir used by generated repair commands",
    )
    parser.add_argument(
        "--python",
        default=sys.executable,
        help="python executable for generated repair commands",
    )
    parser.add_argument(
        "--mvp-script",
        default=str(Path(__file__).resolve().parents[3] / "scripts" / "tickterial_mvp.py"),
        help="tickterial_mvp.py path used in repair commands",
    )
    parser.add_argument(
        "--max-retries",
        type=int,
        default=6,
        help="repair command: retries per hourly request",
    )
    parser.add_argument(
        "--retry-delay",
        type=float,
        default=2.0,
        help="repair command: seconds",
    )
    parser.add_argument(
        "--download-workers",
        type=int,
        default=4,
        help="repair command: concurrency",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=8,
        help="repair command: hourly task batch size",
    )
    parser.add_argument(
        "--batch-pause-ms",
        type=int,
        default=1000,
        help="repair command: pause between batches",
    )
    parser.add_argument(
        "--retry-jitter-ms",
        type=int,
        default=300,
        help="repair command: retry jitter",
    )
    parser.add_argument(
        "--log-level",
        default="WARNING",
        choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"],
        help="repair command: python log level",
    )
    parser.add_argument(
        "--suppress-tickloader-info",
        action="store_true",
        help="repair command: suppress tickterial.tickloader INFO logs",
    )
    parser.add_argument(
        "--repair-validate",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="add --validate when generating repair commands",
    )
    parser.add_argument(
        "--repair-force",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="add --force when generating repair commands",
    )
    parser.add_argument(
        "--repair-1d-alignment-warn",
        action=argparse.BooleanOptionalAction,
        default=True,
        help="when emitting repair commands, include 1d_alignment_invalid warnings as repair targets",
    )
    parser.add_argument(
        "--nan-warning-threshold",
        type=float,
        default=0.8,
        help="warn if any ohlc column NaN ratio > threshold",
    )
    return parser.parse_args(argv)


def parse_dt(raw: str) -> datetime:
    """Parse an ISO-format datetime string into a UTC-aware datetime.

    Args:
        raw: ISO-format datetime string.

    Returns:
        A timezone-aware datetime in UTC.
    """
    return common_parse_dt(raw)


def to_naive_utc(dt: datetime) -> datetime:
    """Convert a datetime to a naive (tzinfo-free) UTC datetime.

    Args:
        dt: A datetime, optionally timezone-aware.

    Returns:
        A naive datetime representing the equivalent UTC time.
    """
    return common_to_naive_utc(dt)


def parse_intervals(raw: str) -> list[str]:
    """Parse and normalize a comma-separated interval string.

    Args:
        raw: Comma-separated interval string.

    Returns:
        List of normalized interval strings.
    """
    return common_parse_intervals(raw, VALID_INTERVALS)


def floor_to_minute(dt: datetime, minutes: int) -> datetime:
    """Floor a datetime to the nearest lower minute boundary.

    Args:
        dt: The datetime to floor.
        minutes: The minute-granularity for flooring.

    Returns:
        A naive UTC datetime floored to the given minute boundary.
    """
    return common_to_minute_floor(dt, minutes)


def iter_year_windows(start: datetime, end: datetime) -> list[Window]:
    """Split a date range into yearly Window objects.

    Args:
        start: Start of the range.
        end: End of the range.

    Returns:
        List of ``Window`` objects, one per calendar year.
    """
    return [
        Window(start=start, end=end) for start, end in common_iter_year_windows(start, end)
    ]


def build_expected_index(window: Window, interval: str) -> pd.DatetimeIndex:
    """Build the expected minute-level DatetimeIndex for a window and interval.

    Args:
        window: Time window to generate the index for.
        interval: Interval string (e.g. ``"1m"``, ``"5m"``, ``"15m"``).

    Returns:
        A ``pd.DatetimeIndex`` with the expected timestamps.
    """
    return common_build_expected_index(window.start, window.end, interval)


def check_file(
    symbol: str,
    interval: str,
    window: Window,
    output_dir: Path,
    nan_warning_threshold: float,
) -> FileResult:
    """Run integrity checks on a single tickterial CSV output file.

    Validates file existence, column schema, datetime parsing, ordering,
    duplicates, row counts, OHLC relations, and NaN ratios.

    Args:
        symbol: Trading symbol (e.g. ``"XAUUSD"``).
        interval: Data interval (e.g. ``"1m"``, ``"5m"``, ``"15m"``, ``"1d"``).
        window: Time window the file should cover.
        output_dir: Directory containing the CSV files.
        nan_warning_threshold: Warn if any OHLC column NaN ratio exceeds
            this value (0--1).

    Returns:
        A ``FileResult`` with status, failure details, warnings, and metrics.
    """
    path = output_dir / f"{symbol}_{interval}_{window.tag()}.csv"
    result = FileResult(
        symbol=symbol,
        interval=interval,
        window=window.tag(),
        file=str(path),
        status="pass",
    )
    result.metrics.update(
        {
            "rows": 0,
            "expected_rows": None,
            "duplicate_rows": 0,
            "missing_rows": 0,
            "datetime_parse_fail": 0,
            "out_of_range_rows": 0,
            "nan_open_ratio": None,
            "nan_high_ratio": None,
            "nan_low_ratio": None,
            "nan_close_ratio": None,
            "nan_volume_ratio": None,
            "ohlc_invalid_rows": 0,
            "alignment_invalid_rows": 0,
        }
    )

    if not path.exists():
        result.status = "fail"
        result.fails.append("missing_file")
        return result
    if path.stat().st_size == 0:
        result.status = "fail"
        result.fails.append("empty_file")
        return result

    try:
        df = pd.read_csv(path, encoding="utf-8")
    except (OSError, pd.errors.ParserError, ValueError) as exc:
        result.status = "fail"
        result.fails.append(f"read_csv_failed:{exc}")
        return result

    result.metrics["rows"] = int(len(df))
    if df.empty:
        result.status = "fail"
        result.fails.append("no_data_rows")
        return result

    missing_columns = [c for c in EXPECTED_COLUMNS if c not in df.columns]
    if missing_columns:
        result.status = "fail"
        result.fails.append(f"missing_columns:{','.join(missing_columns)}")
        return result

    dt = pd.to_datetime(df["datetime"], errors="coerce", utc=True)
    parse_fail = int(dt.isna().sum())
    result.metrics["datetime_parse_fail"] = parse_fail
    if parse_fail == len(df):
        result.status = "fail"
        result.fails.append("all_datetime_parse_failed")
        return result
    if parse_fail > 0:
        result.warns.append(f"datetime_parse_fail:{parse_fail}")

    dt_naive = dt.dt.tz_localize(None)
    window_start = window.start
    window_end = window.end

    if interval != "1d":
        out_of_range_mask = (dt_naive < window_start) | (dt_naive >= window_end)
        out_of_range = int(out_of_range_mask.fillna(True).sum())
        result.metrics["out_of_range_rows"] = out_of_range
        if out_of_range > 0:
            result.status = "fail"
            result.fails.append(f"out_of_range_rows:{out_of_range}")

    dt_series = dt_naive.dropna()
    if dt_series.empty:
        result.status = "fail"
        result.fails.append("no_parseable_datetime")
        return result

    if not dt_series.is_monotonic_increasing:
        result.status = "fail"
        result.fails.append("datetime_not_monotonic_increasing")

    duplicate_count = int(dt_series.duplicated().sum())
    result.metrics["duplicate_rows"] = duplicate_count
    if duplicate_count > 0:
        result.status = "fail"
        result.fails.append(f"duplicate_timestamps:{duplicate_count}")

    if interval in COUNT_CHECK_INTERVALS:
        expected_idx = build_expected_index(window, interval)
        expected_count = len(expected_idx)
        result.metrics["expected_rows"] = expected_count
        if expected_count != len(df):
            result.status = "fail"
            result.fails.append(f"row_count_mismatch:{len(df)}!={expected_count}")

        actual = dt_series.dt.floor(f"{interval[:-1]}min")
        actual_index = pd.DatetimeIndex(actual.sort_values().unique())
        expected_index_set = pd.DatetimeIndex(expected_idx)
        missing = expected_index_set.difference(actual_index)
        extra = actual_index.difference(expected_index_set)
        missing_count = int(len(missing))
        result.metrics["missing_rows"] = missing_count
        if missing_count > 0:
            result.status = "fail"
            result.fails.append(f"missing_timestamps:{missing_count}")
        if len(extra) > 0:
            result.status = "fail"
            result.fails.append(f"extra_timestamps:{len(extra)}")

    numeric_values: dict[str, pd.Series] = {}
    for col in ("open", "high", "low", "close", "volume"):
        series = pd.to_numeric(df[col], errors="coerce")
        numeric_values[col] = series
        total = len(series)
        if total > 0:
            nan_count = int(series.isna().sum())
            ratio = nan_count / total
            result.metrics[f"nan_{col}_ratio"] = round(ratio, 6)
            if ratio > nan_warning_threshold:
                result.warns.append(f"nan_ratio_{col}:{ratio:.4f}")
        if np.isinf(series).any():
            result.status = "fail"
            result.fails.append(f"non_finite_{col}")

    open_series = numeric_values["open"]
    high_series = numeric_values["high"]
    low_series = numeric_values["low"]
    close_series = numeric_values["close"]
    valid_ohlc = (
        open_series.notna()
        & high_series.notna()
        & low_series.notna()
        & close_series.notna()
    )
    if valid_ohlc.any():
        invalid = (
            (high_series[valid_ohlc] < open_series[valid_ohlc])
            | (high_series[valid_ohlc] < close_series[valid_ohlc])
            | (low_series[valid_ohlc] > open_series[valid_ohlc])
            | (low_series[valid_ohlc] > close_series[valid_ohlc])
            | (high_series[valid_ohlc] < low_series[valid_ohlc])
        )
        invalid_count = int(invalid.sum())
        result.metrics["ohlc_invalid_rows"] = invalid_count
        if invalid_count > 0:
            result.status = "fail"
            result.fails.append(f"ohlc_relation_invalid:{invalid_count}")

    if interval == "1d":
        if dt_series.notna().any():
            ny = dt_series.dt.tz_localize("UTC").dt.tz_convert(NY_TZ)
            invalid_align = (
                (ny.dt.hour != 17)
                | (ny.dt.minute != 0)
                | (ny.dt.second != 0)
            )
            invalid_align_count = int(invalid_align.sum())
            result.metrics["alignment_invalid_rows"] = invalid_align_count
            if invalid_align_count > 0:
                result.warns.append(f"1d_alignment_invalid:{invalid_align_count}")

    if result.status == "pass" and not result.warns:
        return result
    if result.status == "pass" and result.warns:
        result.status = "warn"
    return result


def build_repair_command(
    window: Window,
    symbol: str,
    intervals: list[str],
    args: argparse.Namespace,
    output: Path,
) -> str:
    """Build a shell command string for repairing a failed window.

    Args:
        window: The time window to repair.
        symbol: Trading symbol.
        intervals: List of intervals to include in the repair.
        args: Parsed CLI namespace with repair parameters.
        output: Output directory path.

    Returns:
        A shell-safe command string.
    """
    cmd = [
        args.python,
        args.mvp_script,
        "--start",
        window.start.strftime("%Y-%m-%dT%H:%M:%S"),
        "--end",
        window.end.strftime("%Y-%m-%dT%H:%M:%S"),
        "--symbols",
        symbol,
        "--output",
        str(output),
        "--cache-dir",
        args.repair_cache_dir,
        "--intervals",
        ",".join(intervals),
        "--max-retries",
        str(args.max_retries),
        "--retry-delay",
        str(args.retry_delay),
        "--download-workers",
        str(args.download_workers),
        "--batch-size",
        str(args.batch_size),
        "--batch-pause-ms",
        str(args.batch_pause_ms),
        "--retry-jitter-ms",
        str(args.retry_jitter_ms),
        "--log-level",
        args.log_level,
    ]
    if args.suppress_tickloader_info:
        cmd.append("--suppress-tickloader-info")
    if args.repair_validate:
        cmd.append("--validate")
    if args.repair_force:
        cmd.append("--force")
    return list2cmdline(cmd)


def write_json_report(path: Path, payload: dict[str, Any]) -> None:
    """Write the check results to a JSON file.

    Args:
        path: Output file path.
        payload: Dict payload to serialize as JSON.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)


def write_csv_report(path: Path, records: list[FileResult]) -> None:
    """Write the check results to a CSV file.

    Args:
        path: Output file path.
        records: List of ``FileResult`` objects to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=[
                "symbol",
                "interval",
                "window",
                "status",
                "file",
                "rows",
                "expected_rows",
                "duplicate_rows",
                "missing_rows",
                "out_of_range_rows",
                "datetime_parse_fail",
                "ohlc_invalid_rows",
                "alignment_invalid_rows",
                "nan_open_ratio",
                "nan_high_ratio",
                "nan_low_ratio",
                "nan_close_ratio",
                "nan_volume_ratio",
                "fails",
                "warns",
            ],
        )
        writer.writeheader()
        for r in records:
            writer.writerow(
                {
                    "symbol": r.symbol,
                    "interval": r.interval,
                    "window": r.window,
                    "status": r.status,
                    "file": r.file,
                    "rows": r.metrics.get("rows"),
                    "expected_rows": r.metrics.get("expected_rows"),
                    "duplicate_rows": r.metrics.get("duplicate_rows"),
                    "missing_rows": r.metrics.get("missing_rows"),
                    "out_of_range_rows": r.metrics.get("out_of_range_rows"),
                    "datetime_parse_fail": r.metrics.get("datetime_parse_fail"),
                    "ohlc_invalid_rows": r.metrics.get("ohlc_invalid_rows"),
                    "alignment_invalid_rows": r.metrics.get("alignment_invalid_rows"),
                    "nan_open_ratio": r.metrics.get("nan_open_ratio"),
                    "nan_high_ratio": r.metrics.get("nan_high_ratio"),
                    "nan_low_ratio": r.metrics.get("nan_low_ratio"),
                    "nan_close_ratio": r.metrics.get("nan_close_ratio"),
                    "nan_volume_ratio": r.metrics.get("nan_volume_ratio"),
                    "fails": "|".join(r.fails),
                    "warns": "|".join(r.warns),
                }
            )


def parse_symbols(raw: str) -> list[str]:
    """Parse a comma-separated string of symbols into uppercase tokens.

    Args:
        raw: Comma-separated symbol string.

    Returns:
        List of trimmed, uppercase symbol strings.
    """
    return common_parse_symbols(raw)


def should_emit_repair_for_result(r: FileResult, repair_1d_alignment_warn: bool) -> bool:
    """Determine whether a check result should trigger a repair command.

    Args:
        r: The file check result.
        repair_1d_alignment_warn: If ``True``, treat 1d alignment warnings
            as repair-worthy.

    Returns:
        ``True`` if a repair command should be emitted for this result.
    """
    if r.status == "fail":
        return True
    if not repair_1d_alignment_warn:
        return False
    if r.status != "warn" or r.interval != "1d":
        return False
    return any(w.startswith("1d_alignment_invalid:") for w in r.warns)


def main(args: argparse.Namespace | None = None) -> int:
    """Run the full check pipeline and optionally emit repair commands.

    Args:
        args: Pre-parsed CLI arguments, or ``None`` to parse from ``sys.argv``.

    Returns:
        Exit code: 0 on success, 1 if any check failed, 2 on invalid input.
    """
    args = parse_args() if args is None else args
    try:
        intervals = parse_intervals(args.intervals)
    except ValueError as exc:
        logger.error(str(exc))
        return 2

    symbols = parse_symbols(args.symbols)
    if not symbols:
        logger.error("symbols must not be empty")
        return 2

    if args.nan_warning_threshold < 0 or args.nan_warning_threshold > 1:
        logger.error("--nan-warning-threshold must be in [0,1]")
        return 2

    start = parse_dt(args.start)
    end = parse_dt(args.end)
    if start >= end:
        logger.error("start must be earlier than end")
        return 2

    output_dir = Path(args.output).resolve()
    mvp_script = Path(args.mvp_script).resolve()
    if not mvp_script.exists():
        logger.error("mvp script not found: %s", mvp_script)
        return 2

    windows = iter_year_windows(start, end)
    if not windows:
        logger.warning("no windows to check")
        return 0

    results: list[FileResult] = []
    for symbol in symbols:
        for window in windows:
            for interval in intervals:
                results.append(
                    check_file(
                        symbol=symbol,
                        interval=interval,
                        window=window,
                        output_dir=output_dir,
                        nan_warning_threshold=args.nan_warning_threshold,
                    )
                )

    pass_count = sum(1 for r in results if r.status == "pass")
    warn_count = sum(1 for r in results if r.status == "warn")
    fail_count = sum(1 for r in results if r.status == "fail")
    total = len(results)
    fail_windows = sorted({(r.symbol, r.window) for r in results if r.status == "fail"})
    warn_only_windows = sorted({(r.symbol, r.window) for r in results if r.status == "warn"})

    logger.info("checked windows=%s symbols=%s intervals=%s", len(windows), len(symbols), len(intervals))
    logger.info(
        "status summary: total=%d pass=%d warn=%d fail=%d",
        total,
        pass_count,
        warn_count,
        fail_count,
    )
    if warn_only_windows:
        logger.info("warning windows:")
        for symbol, window in warn_only_windows:
            logger.info("  [warn] %s %s", symbol, window)
    if fail_windows:
        logger.warning("failed windows:")
        for symbol, window in fail_windows:
            logger.warning("  [fail] %s %s", symbol, window)

    repair_commands: list[dict[str, Any]] = []
    if args.emit_repair_commands:
        failures_by_window: dict[tuple[str, str], set[str]] = {}
        for r in results:
            if should_emit_repair_for_result(r, repair_1d_alignment_warn=args.repair_1d_alignment_warn):
                failures_by_window.setdefault((r.symbol, r.window), set()).add(r.interval)
        if failures_by_window:
            result_by_key = {(r.symbol, r.window, r.interval): r for r in results}
            window_by_tag = {w.tag(): w for w in windows}
            logger.info("repair command list:")
            for (symbol, window_tag), interval_set in sorted(
                failures_by_window.items(), key=lambda x: (x[0][0], x[0][1])
            ):
                window = window_by_tag.get(window_tag)
                if window is None:
                    continue
                sorted_intervals = sorted(interval_set)
                cmd = build_repair_command(window, symbol, sorted_intervals, args, output_dir)
                logger.info("%s", cmd)
                entry = {
                    "symbol": symbol,
                    "window": window_tag,
                    "intervals": ",".join(sorted_intervals),
                    "command": cmd,
                }
                for interval in sorted_intervals:
                    r = result_by_key.get((symbol, window_tag, interval))
                    if r is None:
                        continue
                    reason = "|".join(r.fails if r.fails else r.warns)
                    entry.setdefault("reasons", []).append(f"{r.interval}:{reason}")
                repair_commands.append(entry)
            if args.repair_command_file:
                command_file = Path(args.repair_command_file).resolve()
                command_file.parent.mkdir(parents=True, exist_ok=True)
                if command_file.suffix.lower() == ".json":
                    with command_file.open("w", encoding="utf-8") as f:
                        json.dump(repair_commands, f, ensure_ascii=False, indent=2)
                else:
                    with command_file.open("w", encoding="utf-8", newline="") as f:
                        writer = csv.DictWriter(
                            f,
                            fieldnames=["symbol", "window", "intervals", "command"],
                        )
                        writer.writeheader()
                        for entry in repair_commands:
                            row = {
                                "symbol": entry["symbol"],
                                "window": entry["window"],
                                "intervals": entry["intervals"],
                                "command": entry["command"],
                            }
                            writer.writerow(row)

    payload = {
        "meta": {
            "symbols": symbols,
            "start": start.isoformat(),
            "end": end.isoformat(),
            "intervals": intervals,
            "output": str(output_dir),
            "generated_at": datetime.now(UTC).isoformat(),
        },
        "summary": {
            "total": total,
            "pass": pass_count,
            "warn": warn_count,
            "fail": fail_count,
            "windows": len(windows),
            "failure_rate": float(fail_count / total) if total else 0.0,
        },
        "results": [
            {
                "symbol": r.symbol,
                "interval": r.interval,
                "window": r.window,
                "file": r.file,
                "status": r.status,
                "fails": r.fails,
                "warns": r.warns,
                "metrics": r.metrics,
                "command": r.command,
            }
            for r in results
        ],
        "repair_commands": repair_commands,
    }

    if args.report_json:
        write_json_report(Path(args.report_json).resolve(), payload)
    if args.report_csv:
        write_csv_report(Path(args.report_csv).resolve(), results)

    if fail_count > 0:
        logger.error("check failed with %d failed file(s).", fail_count)
        return 1
    if warn_count > 0:
        logger.warning("check passed with warnings.")
        return 0
    logger.info("check passed with no warnings.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

