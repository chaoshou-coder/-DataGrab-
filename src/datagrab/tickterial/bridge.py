#!/usr/bin/env python
"""Convert tickterial MVP CSV files into datagrab parquet format."""

from __future__ import annotations

import argparse
import logging
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl

from datagrab.pipeline.writer import ParquetWriter
from datagrab.tickterial.exceptions import BridgeError

logger = logging.getLogger(__name__)


NAME_RE = re.compile(r"(?P<symbol>[A-Za-z0-9.+#-]+)_(?P<interval>1m|5m|15m|1d)_(?P<start>\d{8})_(?P<end>\d{8})\.csv$")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    """Parse CLI arguments for the CSV-to-parquet bridge.

    Args:
        argv: Command-line argument list, or ``None`` for ``sys.argv``.

    Returns:
        Parsed argument namespace.
    """
    parser = argparse.ArgumentParser(description="convert tickterial_mvp csv files to parquet")
    parser.add_argument("--input-dir", required=True, help="directory of CSV files")
    parser.add_argument("--output-root", default="data", help="data_root for parquet writer")
    parser.add_argument("--asset-type", default="commodity", help="asset type")
    parser.add_argument(
        "--merge-on-incremental",
        action="store_true",
        help="merge into existing parquet if same output path exists",
    )
    parser.add_argument("--symbol", default="", help="optional filter symbol")
    parser.add_argument("--interval", default="", help="optional filter interval")
    return parser.parse_args(argv)


def parse_window(file: Path) -> tuple[str, str, datetime, datetime] | None:
    """Extract symbol, interval, and date range from a CSV filename.

    The filename must match the pattern
    ``<SYMBOL>_<INTERVAL>_<START>_<END>.csv``.

    Args:
        file: Path to the CSV file.

    Returns:
        A tuple of ``(symbol, interval, start, end)`` or ``None`` if the
        filename does not match the expected pattern.
    """
    m = NAME_RE.match(file.name)
    if not m:
        return None
    symbol = m.group("symbol")
    interval = m.group("interval")
    start = datetime.strptime(m.group("start"), "%Y%m%d").replace(tzinfo=timezone.utc)
    end = datetime.strptime(m.group("end"), "%Y%m%d").replace(tzinfo=timezone.utc)
    return symbol, interval, start, end


def csv_to_parquet(
    csv_path: Path,
    writer: ParquetWriter,
    asset_type: str,
    merge_on_incremental: bool,
) -> None:
    """Convert a single tickterial CSV file to datagrab parquet format.

    Args:
        csv_path: Path to the source CSV file.
        writer: ``ParquetWriter`` instance for output.
        asset_type: Asset type label (e.g. ``"commodity"``).
        merge_on_incremental: If ``True``, merge into existing parquet if one
            exists at the same output path.

    Raises:
        BridgeError: If the filename format is invalid or the datetime column
            is missing.
    """
    parsed = parse_window(csv_path)
    if not parsed:
        raise BridgeError(f"invalid csv name: {csv_path.name}")
    symbol, interval, start, end = parsed

    pdf = pd.read_csv(csv_path, encoding="utf-8")
    if pdf.empty:
        return
    if "datetime" not in pdf.columns:
        raise BridgeError(f"missing datetime column: {csv_path}")
    pdf["datetime"] = pd.to_datetime(pdf["datetime"])
    pdf = pdf.sort_values("datetime")

    df = pl.from_pandas(pdf[["datetime", "open", "high", "low", "close", "volume"]])
    out_path = writer.build_path(asset_type, symbol, interval, start, end)
    if merge_on_incremental:
        existing = writer.find_existing(asset_type, symbol, interval)
        existing_path = existing.path if existing else None
    else:
        existing_path = None
    writer.merge_and_write(
        existing_path,
        df,
        out_path,
        adjustment=None,
        extra_metadata={"source": "tickterial_mvp"},
    )


def run(args: argparse.Namespace | None = None) -> int:
    """Execute the bridge, converting all matching CSVs to parquet.

    Args:
        args: Pre-parsed CLI arguments, or ``None`` to parse from ``sys.argv``.

    Returns:
        Exit code: 0 on success, 2 if the input directory is not found.
    """
    args = parse_args() if args is None else args
    input_dir = Path(args.input_dir)
    if not input_dir.exists():
        logger.error("input-dir not found")
        return 2

    writer = ParquetWriter(Path(args.output_root).resolve(), merge_on_incremental=args.merge_on_incremental)
    files = [p for p in input_dir.glob("*.csv") if p.is_file()]
    if not files:
        logger.warning("no csv files found")
        return 0

    total = 0
    ok = 0
    skipped = 0
    for csv_file in sorted(files):
        total += 1
        parsed = parse_window(csv_file)
        if not parsed:
            skipped += 1
            continue
        symbol, interval, _, _ = parsed
        if args.symbol and symbol != args.symbol.upper():
            continue
        if args.interval and interval != args.interval:
            skipped += 1
            continue
        try:
            csv_to_parquet(csv_file, writer, args.asset_type, args.merge_on_incremental)
            logger.info("converted: %s", csv_file.name)
            ok += 1
        except (BridgeError, OSError, pd.errors.ParserError, ValueError, pl.exceptions.PolarsError) as exc:
            logger.warning("skip %s: %s", csv_file.name, exc)
            skipped += 1
            continue
    logger.info("total=%d ok=%d skipped=%d", total, ok, skipped)
    return 0


if __name__ == "__main__":
    raise SystemExit(run())

