from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path

import polars as pl
import pyarrow.parquet as pq

from ..fsutils import ensure_dir
from ..logging import get_logger
from ..storage.schema import ADJUSTED_COLUMN, BASE_COLUMNS
from ..timeutils import BEIJING_TZ, format_date_for_path, to_beijing

_SYMBOL_SAFE_RE = re.compile(r"^[A-Za-z0-9._+=#@$%&^-]+$")
_INVALID_SYMBOL_SUBSTRINGS = ("\\", "/", "..", ":", "|", "?", "*", '"', "<", ">", ";")


@dataclass(frozen=True)
class ExistingRange:
    path: Path
    start: datetime
    end: datetime


class ParquetWriter:
    def __init__(self, data_root: Path, *, merge_on_incremental: bool = True):
        self.data_root = Path(data_root)
        self.logger = get_logger("datagrab.writer")
        self.merge_on_incremental = merge_on_incremental

    def set_data_root(self, data_root: Path) -> None:
        self.data_root = Path(data_root)

    @staticmethod
    def _validate_symbol(symbol: str) -> str:
        token = (symbol or "").strip()
        if not token:
            raise ValueError("symbol is empty")
        if len(token) > 128:
            raise ValueError(f"invalid symbol length: {token}")
        if any(item in token for item in _INVALID_SYMBOL_SUBSTRINGS):
            raise ValueError(f"unsafe symbol: {token}")
        if not _SYMBOL_SAFE_RE.fullmatch(token):
            raise ValueError(f"unsafe symbol: {token}")
        return token

    def _ensure_within_data_root(self, path: Path) -> Path:
        resolved_root = self.data_root.resolve()
        resolved_path = path.resolve()
        try:
            resolved_path.relative_to(resolved_root)
        except ValueError as exc:
            raise ValueError(f"output path escapes data_root: {path}") from exc
        return resolved_path

    def symbol_dir(self, asset_type: str, symbol: str) -> Path:
        symbol = self._validate_symbol(symbol)
        return self.data_root / asset_type / symbol

    def build_path(self, asset_type: str, symbol: str, interval: str, start: datetime, end: datetime) -> Path:
        self._validate_symbol(symbol)
        start_str = format_date_for_path(start)
        end_str = format_date_for_path(end)
        output_path = self.symbol_dir(asset_type, symbol) / f"{interval}_{start_str}_{end_str}.parquet"
        return self._ensure_within_data_root(output_path)

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
        output_path = self._ensure_within_data_root(output_path)
        ensure_dir(output_path.parent)
        df = new_df
        if self.merge_on_incremental and existing_path:
            existing_path = self._ensure_within_data_root(existing_path)
            if existing_path.exists():
                existing_df = pl.read_parquet(existing_path)
                # 历史版本/异常中断可能写出缺失 datetime 的坏文件；遇到则忽略旧文件，避免后续 unique/sort 报错
                if "datetime" not in existing_df.columns:
                    self.logger.warning(
                        "existing parquet missing datetime, ignoring: %s (cols=%s)",
                        existing_path,
                        list(existing_df.columns),
                    )
                    existing_df = pl.DataFrame()
                    existing_path = None
                else:
                    df = pl.concat([existing_df, new_df], how="diagonal_relaxed")
        elif existing_path:
            existing_path = self._ensure_within_data_root(existing_path)
            if existing_path.exists() and existing_path != output_path:
                self.logger.info("merge_on_incremental disabled, existing file will be replaced: %s", existing_path)
        if "datetime" not in df.columns:
            raise RuntimeError(f"datetime missing before write; cols={list(df.columns)}")
        if "close" not in df.columns:
            # close 缺失会导致后续回测/导出不可用，按“硬失败”处理
            raise RuntimeError(f"close missing before write; cols={list(df.columns)}")

        # 非关键列缺失：补齐为 null，保证 parquet schema 更稳定（并记录 warning）
        optional_missing = [c for c in ("open", "high", "low", "volume") if c not in df.columns]
        if optional_missing:
            self.logger.warning(
                "parquet missing optional columns, will fill nulls: %s (%s)",
                ",".join(optional_missing),
                output_path,
            )
            fill_exprs = []
            for c in optional_missing:
                dtype = pl.Float64
                if c == "volume":
                    dtype = pl.Float64
                fill_exprs.append(pl.lit(None).cast(dtype).alias(c))
            df = df.with_columns(fill_exprs)

        df = df.unique(subset=["datetime"], keep="last").sort("datetime")
        columns = list(BASE_COLUMNS)
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
        tmp.replace(output_path)

    def _interval_delta(self, interval: str) -> timedelta:
        interval = interval.strip().lower()
        if interval.endswith("wk"):
            count = int(interval[:-2] or "1")
            return timedelta(days=7 * count)
        if interval == "w":
            return timedelta(days=7)
        if interval.endswith("mo"):
            count = int(interval[:-2] or "1")
            return timedelta(days=30 * count)
        if interval.endswith("y"):
            count = int(interval[:-1] or "1")
            return timedelta(days=365 * count)
        unit = interval[-1]
        if unit not in {"d", "h", "m", "s"}:
            raise ValueError(f"unsupported interval for writer: {interval}")
        try:
            count = int(interval[:-1] or "1")
        except ValueError as exc:
            raise ValueError(f"unsupported interval for writer: {interval}") from exc
        if unit == "d":
            return timedelta(days=count)
        if unit == "h":
            return timedelta(hours=count)
        if unit == "m":
            return timedelta(minutes=count)
        if unit == "s":
            return timedelta(seconds=count)
        raise ValueError(f"unsupported interval for writer: {interval}")
