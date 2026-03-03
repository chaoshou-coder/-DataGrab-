"""Repair missing/broken tickterial CSV outputs by rerunning needed windows."""

from __future__ import annotations

import argparse
import csv
import logging
import os
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pandas as pd

from .common import (
    COUNT_CHECK_INTERVALS,
    EXPECTED_COLUMNS,
    VALID_INTERVALS,
    build_daily_bars_ny_close as common_build_daily_bars_ny_close,
    iter_year_windows as common_iter_year_windows,
    parse_dt as common_parse_dt,
    parse_intervals as common_parse_intervals,
    to_naive_utc as common_to_naive_utc,
)

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

    def key(self) -> str:
        return f"{self.start.isoformat()}:{self.end.isoformat()}"

    def start_iso(self) -> str:
        return self.start.strftime("%Y-%m-%dT%H:%M:%S")

    def end_iso(self) -> str:
        return self.end.strftime("%Y-%m-%dT%H:%M:%S")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the repair command."""
    parser = argparse.ArgumentParser(
        description="Repair yearly windows that are missing/broken in tickterial CSV outputs."
    )
    parser.add_argument("--symbol", default="XAUUSD")
    parser.add_argument("--start", required=True)
    parser.add_argument("--end", required=True)
    parser.add_argument("--output", default="tickterial_csv")
    parser.add_argument("--cache-dir", default=".tick-data")
    parser.add_argument("--intervals", default="1m,5m,15m,1d")
    parser.add_argument("--failures-csv", default="", help="defaults to <output>/failures_mvp.csv")
    parser.add_argument("--max-retries", type=int, default=6)
    parser.add_argument("--retry-delay", type=float, default=2.0)
    parser.add_argument("--download-workers", type=int, default=4)
    parser.add_argument("--batch-size", type=int, default=8)
    parser.add_argument("--batch-pause-ms", type=int, default=1000)
    parser.add_argument("--retry-jitter-ms", type=int, default=300)
    parser.add_argument("--source-timestamp-shift-hours", type=float, default=8.0)
    parser.add_argument("--log-level", default="WARNING", choices=["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"])
    parser.add_argument("--suppress-tickloader-info", action="store_true")
    parser.add_argument("--validate", action=argparse.BooleanOptionalAction, default=True, help="run OHLCV consistency checks")
    parser.add_argument("--strict-validate", action=argparse.BooleanOptionalAction, default=True, help="strict validation flag")
    parser.add_argument("--force", action=argparse.BooleanOptionalAction, default=False, help="force overwrite yearly files")
    parser.add_argument("--check-row-count", action=argparse.BooleanOptionalAction, default=True, help="check 1m/5m/15m row counts")
    parser.add_argument("--check-1d-alignment", action=argparse.BooleanOptionalAction, default=True, help="check 1d close alignment at 17:00 NY")
    parser.add_argument("--prefer-local-1m-for-1d", action=argparse.BooleanOptionalAction, default=True, help="rebuild 1d from local 1m first")
    parser.add_argument("--dry-run", action="store_true", help="scan only, do not execute repair")
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


def iter_year_windows(start: datetime, end: datetime) -> list[Window]:
    """Split a date range into yearly Window objects.

    Args:
        start: Start of the range.
        end: End of the range.

    Returns:
        List of ``Window`` objects, one per calendar year.
    """
    return [Window(start=s, end=e) for s, e in common_iter_year_windows(start, end)]


def expected_rows(window: Window, interval: str) -> int | None:
    """Calculate the expected number of CSV rows for a window and interval.

    Args:
        window: The time window.
        interval: Interval string (e.g. ``"1m"``, ``"5m"``, ``"15m"``).

    Returns:
        Expected row count, or ``None`` if the interval does not require
        row-count validation.
    """
    if interval not in COUNT_CHECK_INTERVALS:
        return None
    minutes = int((window.end - window.start).total_seconds() // 60)
    step = int(interval[:-1])
    return minutes // step


def count_csv_rows(path: Path) -> int:
    """Count the number of data rows in a CSV file (excluding the header).

    Args:
        path: Path to the CSV file.

    Returns:
        Number of data rows (non-negative).
    """
    total_lines = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        for _ in f:
            total_lines += 1
    return max(0, total_lines - 1)


def count_1d_alignment_issues(path: Path) -> int:
    """Count daily-bar rows whose timestamps are not aligned to 17:00 New York.

    Args:
        path: Path to a 1d CSV file.

    Returns:
        Number of rows with invalid alignment.
    """
    invalid = 0
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            raw = (row.get("datetime") or "").strip()
            if not raw:
                invalid += 1
                continue
            try:
                dt = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            except ValueError:
                invalid += 1
                continue
            if dt.tzinfo is None:
                dt_utc = dt.replace(tzinfo=UTC)
            else:
                dt_utc = dt.astimezone(UTC)
            ny = dt_utc.astimezone(NY_TZ)
            if ny.hour != 17 or ny.minute != 0 or ny.second != 0:
                invalid += 1
    return invalid


def write_csv_atomic(path: Path, df: pd.DataFrame) -> None:
    """Write a DataFrame to CSV atomically via a temporary file.

    Args:
        path: Destination file path.
        df: DataFrame to write.
    """
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + f".tmp.{os.getpid()}")
    try:
        df.to_csv(tmp, index=False, encoding="utf-8", date_format="%Y-%m-%d %H:%M:%S")
        tmp.replace(path)
    finally:
        if tmp.exists():
            tmp.unlink()


def build_daily_bars_ny_close(base_1m: pd.DataFrame) -> pd.DataFrame:
    """Aggregate 1-minute bars into daily bars using the 17:00 New York close.

    Args:
        base_1m: DataFrame with 1-minute OHLCV data.

    Returns:
        DataFrame with daily OHLCV bars.
    """
    return common_build_daily_bars_ny_close(base_1m)


def rebuild_1d_from_local_1m(
    symbol: str,
    window: Window,
    output_dir: Path,
    check_row_count: bool,
) -> tuple[bool, str]:
    """Attempt to rebuild 1d bars from a local 1m CSV without network."""
    src_1m = output_dir / f"{symbol}_1m_{window.tag()}.csv"
    dst_1d = output_dir / f"{symbol}_1d_{window.tag()}.csv"

    if not src_1m.exists():
        return False, "local_1m_missing"
    if src_1m.stat().st_size == 0:
        return False, "local_1m_empty"

    try:
        m1 = pd.read_csv(src_1m, encoding="utf-8")
    except (OSError, pd.errors.ParserError, ValueError) as exc:
        return False, f"read_local_1m_failed:{exc}"

    missing_cols = [c for c in EXPECTED_COLUMNS if c not in m1.columns]
    if missing_cols:
        return False, f"local_1m_missing_columns:{','.join(missing_cols)}"

    if check_row_count:
        expected = expected_rows(window, "1m")
        actual = int(len(m1))
        if expected is not None and actual != expected:
            return False, f"local_1m_rows {actual}!={expected}"

    d1 = build_daily_bars_ny_close(m1)
    if d1.empty:
        return False, "local_1m_rebuild_empty"

    try:
        write_csv_atomic(dst_1d, d1)
    except (OSError, ValueError) as exc:
        return False, f"write_1d_failed:{exc}"

    invalid = count_1d_alignment_issues(dst_1d)
    if invalid > 0:
        return False, f"rebuilt_alignment_invalid {invalid}"
    return True, f"rebuilt_from_local_1m rows={len(d1)}"


def assess_window_outputs(
    symbol: str,
    intervals: list[str],
    window: Window,
    output_dir: Path,
    check_row_count: bool,
    check_1d_alignment: bool,
) -> list[str]:
    """Scan a window's output files for issues."""
    issues: list[str] = []
    for interval in intervals:
        path = output_dir / f"{symbol}_{interval}_{window.tag()}.csv"
        if not path.exists():
            issues.append(f"{interval}:missing")
            continue
        if path.stat().st_size == 0:
            issues.append(f"{interval}:empty")
            continue
        if check_row_count and interval in COUNT_CHECK_INTERVALS:
            expected = expected_rows(window, interval)
            if expected is None:
                continue
            actual = count_csv_rows(path)
            if actual != expected:
                issues.append(f"{interval}:rows {actual}!={expected}")
        if check_1d_alignment and interval == "1d":
            invalid_align = count_1d_alignment_issues(path)
            if invalid_align > 0:
                issues.append(f"{interval}:alignment_invalid {invalid_align}")
    return issues


def extract_issue_intervals(issues: list[str], fallback: list[str]) -> list[str]:
    """Extract interval identifiers from issue description strings.

    Args:
        issues: List of issue strings in the format ``"interval:description"``.
        fallback: Intervals to return if no intervals can be extracted.

    Returns:
        Sorted list of unique interval strings found in the issues.
    """
    targeted: set[str] = set()
    for issue in issues:
        token, sep, _rest = issue.partition(":")
        token = token.strip().lower()
        if sep and token in VALID_INTERVALS:
            targeted.add(token)
    if not targeted:
        return list(fallback)
    return sorted(targeted)


def read_failed_windows(path: Path, symbol: str) -> set[Window]:
    """Read previously failed windows from a failures CSV file.

    Args:
        path: Path to the failures CSV file.
        symbol: Symbol to filter rows by.

    Returns:
        Set of ``Window`` objects representing previously failed windows.
    """
    if not path.exists():
        return set()
    out: set[Window] = set()
    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if not row:
                continue
            row_symbol = (row.get("symbol") or "").strip().upper()
            if row_symbol != symbol.upper():
                continue
            if (row.get("interval") or "").strip() != "mvp":
                continue
            start_raw = (row.get("start") or "").strip()
            end_raw = (row.get("end") or "").strip()
            if not start_raw or not end_raw:
                continue
            try:
                start = to_naive_utc(datetime.fromisoformat(start_raw))
                end = to_naive_utc(datetime.fromisoformat(end_raw))
            except ValueError:
                continue
            if start >= end:
                continue
            out.add(Window(start=start, end=end))
    return out


def _repair_window_via_download(
    args: argparse.Namespace,
    symbol: str,
    intervals: list[str],
    window: Window,
    force: bool,
) -> int:
    """Invoke the download runner as a Python API call (no subprocess)."""
    from .runner import run as download_run

    download_args = argparse.Namespace(
        symbols=symbol,
        start=window.start_iso(),
        end=window.end_iso(),
        output=args.output,
        cache_dir=args.cache_dir,
        intervals=",".join(intervals),
        max_retries=args.max_retries,
        retry_delay=args.retry_delay,
        download_workers=args.download_workers,
        batch_size=args.batch_size,
        batch_pause_ms=args.batch_pause_ms,
        retry_jitter_ms=args.retry_jitter_ms,
        source_timestamp_shift_hours=getattr(args, "source_timestamp_shift_hours", 8.0),
        resume_failures="",
        validate=args.validate,
        strict_validate=args.strict_validate,
        window_retries=0,
        log_level=args.log_level,
        suppress_tickloader_info=args.suppress_tickloader_info,
        force=force,
    )
    return download_run(download_args)


def main(args: argparse.Namespace | None = None) -> int:
    """Main repair entry point.

    Args:
        args: Pre-parsed CLI arguments, or ``None`` to parse from ``sys.argv``.

    Returns:
        Exit code: 0 on success, 1 if any repair failed, 2 on invalid input.
    """
    args = parse_args() if args is None else args

    try:
        intervals = parse_intervals(args.intervals)
    except ValueError as exc:
        logger.error(str(exc))
        return 2

    start = parse_dt(args.start)
    end = parse_dt(args.end)
    if start >= end:
        logger.error("start must be earlier than end")
        return 2

    output_dir = Path(args.output).resolve()
    output_dir.mkdir(parents=True, exist_ok=True)
    failures_csv = Path(args.failures_csv).resolve() if args.failures_csv else output_dir / "failures_mvp.csv"

    windows = iter_year_windows(start, end)
    if not windows:
        logger.warning("no windows to scan")
        return 0

    issues_by_window: dict[Window, list[str]] = {}
    for window in windows:
        issues = assess_window_outputs(
            symbol=args.symbol.upper(),
            intervals=intervals,
            window=window,
            output_dir=output_dir,
            check_row_count=args.check_row_count,
            check_1d_alignment=args.check_1d_alignment,
        )
        if issues:
            issues_by_window[window] = issues

    failed_windows = read_failed_windows(failures_csv, symbol=args.symbol.upper())
    failed_windows = {w for w in failed_windows if start <= w.start.replace(tzinfo=UTC) < end}

    targets = sorted(set(issues_by_window.keys()) | failed_windows)
    logger.info(
        "scan complete: windows=%d missing_or_broken=%d failures_csv=%d targets=%d",
        len(windows),
        len(issues_by_window),
        len(failed_windows),
        len(targets),
    )
    if targets:
        for idx, window in enumerate(targets, 1):
            reasons = []
            if window in issues_by_window:
                reasons.extend(issues_by_window[window])
            if window in failed_windows:
                reasons.append("listed_in_failures_csv")
            reason_text = ", ".join(reasons) if reasons else "unknown"
            logger.info("%02d. %s -> %s", idx, window.tag(), reason_text)

    if args.dry_run:
        logger.info("dry-run complete")
        return 0

    if not targets:
        logger.info("no repair needed")
        return 0

    failed_repairs: list[tuple[Window, int, str]] = []
    for idx, window in enumerate(targets, 1):
        logger.info("repair [%d/%d] %s start", idx, len(targets), window.tag())
        window_issues = issues_by_window.get(window, [])
        if window_issues:
            target_intervals = extract_issue_intervals(window_issues, intervals)
        else:
            target_intervals = list(intervals)
        target_force = args.force or bool(window_issues)
        logger.info(
            "repair [%d/%d] %s intervals=%s force=%s",
            idx,
            len(targets),
            window.tag(),
            ",".join(target_intervals),
            "on" if target_force else "off",
        )

        if args.prefer_local_1m_for_1d and set(target_intervals) == {"1d"}:
            ok, detail = rebuild_1d_from_local_1m(
                symbol=args.symbol.upper(),
                window=window,
                output_dir=output_dir,
                check_row_count=args.check_row_count,
            )
            if ok:
                logger.info("repair [%d/%d] %s local-rebuild ok: %s", idx, len(targets), window.tag(), detail)
                post_issues = assess_window_outputs(
                    symbol=args.symbol.upper(),
                    intervals=intervals,
                    window=window,
                    output_dir=output_dir,
                    check_row_count=args.check_row_count,
                    check_1d_alignment=args.check_1d_alignment,
                )
                if post_issues:
                    failed_repairs.append((window, 1, f"post_check_failed: {', '.join(post_issues)}"))
                    logger.warning("repair [%d/%d] %s post-check failed: %s", idx, len(targets), window.tag(), ", ".join(post_issues))
                else:
                    logger.info("repair [%d/%d] %s ok", idx, len(targets), window.tag())
                continue
            logger.info(
                "repair [%d/%d] %s local-rebuild skipped: %s; fallback to download",
                idx, len(targets), window.tag(), detail,
            )

        returncode = _repair_window_via_download(
            args,
            args.symbol.upper(),
            target_intervals,
            window,
            force=target_force,
        )
        if returncode != 0:
            failed_repairs.append((window, returncode, "download_failed"))
            logger.error("repair [%d/%d] %s failed with code %d", idx, len(targets), window.tag(), returncode)
            continue

        post_issues = assess_window_outputs(
            symbol=args.symbol.upper(),
            intervals=intervals,
            window=window,
            output_dir=output_dir,
            check_row_count=args.check_row_count,
            check_1d_alignment=args.check_1d_alignment,
        )
        if post_issues:
            failed_repairs.append((window, 1, f"post_check_failed: {', '.join(post_issues)}"))
            logger.warning("repair [%d/%d] %s post-check failed: %s", idx, len(targets), window.tag(), ", ".join(post_issues))
            continue

        logger.info("repair [%d/%d] %s ok", idx, len(targets), window.tag())

    if failed_repairs:
        logger.error("repair finished with failures:")
        for window, code, reason in failed_repairs:
            logger.error("- %s code=%s reason=%s", window.tag(), code, reason)
        return 1

    logger.info("repair finished successfully")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
