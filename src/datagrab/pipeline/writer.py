from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from ..fsutils import ensure_dir
from ..logging import get_logger
from ..storage.schema import ADJUSTED_COLUMN, BASE_COLUMNS
from ..timeutils import BEIJING_TZ, format_date_for_path, to_beijing


@dataclass(frozen=True)
class ExistingRange:
    path: Path
    start: datetime
    end: datetime


class ParquetWriter:
    def __init__(self, data_root: Path):
        self.data_root = data_root
        self.logger = get_logger("datagrab.writer")

    def symbol_dir(self, asset_type: str, symbol: str) -> Path:
        return self.data_root / asset_type / symbol

    def build_path(self, asset_type: str, symbol: str, interval: str, start: datetime, end: datetime) -> Path:
        start_str = format_date_for_path(start)
        end_str = format_date_for_path(end)
        return self.symbol_dir(asset_type, symbol) / f"{interval}_{start_str}_{end_str}.parquet"

    def find_existing(self, asset_type: str, symbol: str, interval: str) -> ExistingRange | None:
        sym_dir = self.symbol_dir(asset_type, symbol)
        if not sym_dir.exists():
            return None
        candidates = []
        for path in sym_dir.glob(f"{interval}_*.parquet"):
            parsed = self._parse_range(path, interval)
            if parsed:
                candidates.append(parsed)
        if not candidates:
            return None
        candidates.sort(key=lambda item: item.end)
        return candidates[-1]

    def _parse_range(self, path: Path, interval: str) -> ExistingRange | None:
        stem = path.name.rsplit(".", 1)[0]
        parts = stem.split("_")
        if len(parts) != 3 or parts[0] != interval:
            return None
        try:
            start = datetime.strptime(parts[1], "%Y%m%d").replace(tzinfo=BEIJING_TZ)
            end = datetime.strptime(parts[2], "%Y%m%d").replace(tzinfo=BEIJING_TZ)
        except ValueError:
            return None
        return ExistingRange(path=path, start=start, end=end)

    def read_range_max(self, path: Path) -> datetime | None:
        if not path.exists():
            return None
        try:
            lazy = pl.scan_parquet(path)
            result = lazy.select(pl.col("datetime").max()).collect()
            if result.is_empty():
                return None
            value = result.item()
            if isinstance(value, datetime):
                if value.tzinfo is None:
                    return value.replace(tzinfo=BEIJING_TZ)
                return to_beijing(value)
            return None
        except Exception as exc:
            self.logger.warning("max datetime read failed: %s", exc)
            return None

    def next_start(self, last_dt: datetime, interval: str) -> datetime:
        delta = self._interval_delta(interval)
        return to_beijing(last_dt) + delta

    def merge_and_write(
        self,
        existing_path: Path | None,
        new_df: pl.DataFrame,
        output_path: Path,
        adjustment: str | None,
    ) -> None:
        ensure_dir(output_path.parent)
        if existing_path and existing_path.exists():
            existing_df = pl.read_parquet(existing_path)
            df = pl.concat([existing_df, new_df], how="diagonal_relaxed")
        else:
            df = new_df
        df = df.unique(subset=["datetime"], keep="last").sort("datetime")
        columns = [c for c in BASE_COLUMNS if c in df.columns]
        if ADJUSTED_COLUMN in df.columns:
            columns.append(ADJUSTED_COLUMN)
        df = df.select(columns)
        table = df.to_arrow()
        if adjustment:
            metadata = dict(table.schema.metadata or {})
            metadata[b"datagrab.adjustment"] = adjustment.encode("utf-8")
            table = table.replace_schema_metadata(metadata)
        tmp = output_path.with_suffix(output_path.suffix + ".tmp")
        pq.write_table(table, tmp)
        if existing_path and existing_path.exists() and existing_path != output_path:
            existing_path.unlink(missing_ok=True)
        tmp.replace(output_path)

    def _interval_delta(self, interval: str) -> timedelta:
        interval = interval.strip().lower()
        if interval.endswith("wk"):
            count = int(interval[:-2] or "1")
            return timedelta(days=7 * count)
        if interval.endswith("mo"):
            count = int(interval[:-2] or "1")
            return timedelta(days=30 * count)
        if interval.endswith("y"):
            count = int(interval[:-1] or "1")
            return timedelta(days=365 * count)
        unit = interval[-1]
        count = int(interval[:-1] or "1")
        if unit == "d":
            return timedelta(days=count)
        if unit == "h":
            return timedelta(hours=count)
        if unit == "m":
            return timedelta(minutes=count)
        return timedelta(days=1)
