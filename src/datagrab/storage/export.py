from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl


REQUIRED_OHLCV_COLUMNS = ("datetime", "open", "high", "low", "close", "volume")


def _validate_ohlcv_columns(df: pl.DataFrame) -> None:
    missing = [col for col in REQUIRED_OHLCV_COLUMNS if col not in df.columns]
    if missing:
        raise ValueError(f"parquet 缺少必需列: {', '.join(missing)}")


def export_backtrader_csv(input_path: Path, output_path: Path) -> None:
    df = pl.read_parquet(input_path)
    _validate_ohlcv_columns(df)
    df = df.rename(
        {
            "datetime": "datetime",
            "open": "open",
            "high": "high",
            "low": "low",
            "close": "close",
            "volume": "volume",
        }
    )
    if "openinterest" not in df.columns:
        df = df.with_columns(pl.lit(0).alias("openinterest"))
    output_path.parent.mkdir(parents=True, exist_ok=True)
    df.write_csv(output_path)


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
