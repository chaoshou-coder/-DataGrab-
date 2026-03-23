from __future__ import annotations

from collections import defaultdict
from pathlib import Path

import pandas as pd
import numpy as np
import polars as pl

from datagrab.tickterial.bridge import parse_window

REQUIRED_OHLCV_COLUMNS = ("datetime", "open", "high", "low", "close", "volume")
MT4_INTERVAL_MAP = {"1m": "M1", "5m": "M5", "15m": "M15", "30m": "M30", "1h": "H1", "1d": "D1"}


def _validate_ohlcv_columns(df: pl.DataFrame) -> None:
    missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"缺少必需列: {', '.join(missing)}")


def _to_mt4_interval(interval: str) -> str:
    mapped = MT4_INTERVAL_MAP.get(interval)
    if mapped is None:
        raise ValueError(f"unsupported mt4 interval: {interval}, supported: {', '.join(sorted(MT4_INTERVAL_MAP))}")
    return mapped


def _normalize_frame(df: pl.DataFrame | pd.DataFrame) -> pl.DataFrame:
    if isinstance(df, pd.DataFrame):
        frame = pl.from_pandas(df)
    elif isinstance(df, pl.DataFrame):
        frame = df
    else:
        raise TypeError(f"expected pandas.DataFrame or polars.DataFrame, got {type(df)}")
    missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in frame.columns]
    if missing:
        raise ValueError(f"missing required ohlcv columns: {', '.join(missing)}")
    return frame.select(REQUIRED_OHLCV_COLUMNS)


def export_vectorbt_npz(input_path: Path, output_path: Path) -> None:
    df = pl.read_parquet(input_path)
    _validate_ohlcv_columns(df)
    out = {
        "datetime": df["datetime"].to_numpy(),
        "open": df["open"].to_numpy(),
        "high": df["high"].to_numpy(),
        "low": df["low"].to_numpy(),
        "close": df["close"].to_numpy(),
        "volume": df["volume"].to_numpy(),
    }
    if "adjusted_close" in df.columns:
        out["adjusted_close"] = df["adjusted_close"].to_numpy()
    output_path.parent.mkdir(parents=True, exist_ok=True)
    np.savez_compressed(output_path, **out)


def export_mt4_csv(df: pl.DataFrame | pd.DataFrame, output_path: Path) -> None:
    """Export an OHLCV dataframe to MT4 history format."""
    frame = _normalize_frame(df)
    _validate_ohlcv_columns(frame)
    pdf = frame.to_pandas().copy()
    ts = pd.to_datetime(pdf["datetime"], errors="coerce")
    if ts.isna().any():
        raise ValueError("export_mt4_csv: invalid datetime value in source data")
    pdf["datetime"] = ts
    if ts.dt.tz is not None:
        pdf["datetime"] = ts.dt.tz_convert(None)
    pdf = pdf.sort_values("datetime")
    out = pd.DataFrame(
        {
            "date": pd.to_datetime(pdf["datetime"]).dt.strftime("%Y.%m.%d"),
            "time": pd.to_datetime(pdf["datetime"]).dt.strftime("%H:%M"),
            "open": pd.to_numeric(pdf["open"], errors="raise").astype("float64"),
            "high": pd.to_numeric(pdf["high"], errors="raise").astype("float64"),
            "low": pd.to_numeric(pdf["low"], errors="raise").astype("float64"),
            "close": pd.to_numeric(pdf["close"], errors="raise").astype("float64"),
            "volume": pd.to_numeric(pdf["volume"], errors="coerce")
            .fillna(0.0)
            .round(0)
            .astype("int64"),
        }
    )
    out = out[["date", "time", "open", "high", "low", "close", "volume"]]
    output_path.parent.mkdir(parents=True, exist_ok=True)
    out.to_csv(output_path, index=False, header=False)


def export_mt4_batch(
    input_dir: Path,
    output_dir: Path,
    symbol_filter: str | None = None,
    interval_filter: str | None = None,
) -> list[Path]:
    """Convert all matching tickterial CSV files in a directory into MT4 CSV files."""
    if not input_dir.exists() or not input_dir.is_dir():
        raise ValueError(f"input_dir must be an existing directory: {input_dir}")
    output_dir.mkdir(parents=True, exist_ok=True)

    symbol_filter = symbol_filter.strip().upper() if symbol_filter else None
    interval_filter = interval_filter.strip() if interval_filter else None

    grouped: dict[tuple[str, str], list[Path]] = defaultdict(list)
    for csv_path in sorted(input_dir.rglob("*.csv")):
        if not csv_path.is_file():
            continue
        parsed = parse_window(csv_path)
        if not parsed:
            continue
        symbol, interval, _, _ = parsed
        if symbol_filter and symbol != symbol_filter:
            continue
        if interval_filter and interval != interval_filter:
            continue
        grouped[(symbol, interval)].append(csv_path)

    outputs: list[Path] = []
    if not grouped:
        return outputs

    for (symbol, interval), files in sorted(grouped.items(), key=lambda item: item[0]):
        frames: list[pl.DataFrame] = []
        for csv_path in files:
            pdf = pd.read_csv(csv_path, encoding="utf-8")
            frame = _normalize_frame(pdf)
            frames.append(frame)
        merged = pl.concat(frames, how="vertical_relaxed").sort("datetime")
        merged = merged.unique(subset=["datetime"], keep="last").sort("datetime")

        output_name = f"{symbol}_{_to_mt4_interval(interval)}.csv"
        output_path = output_dir / output_name
        export_mt4_csv(merged, output_path)
        outputs.append(output_path)

    return outputs
