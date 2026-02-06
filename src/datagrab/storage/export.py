from __future__ import annotations

from pathlib import Path

import numpy as np
import polars as pl


def export_backtrader_csv(input_path: Path, output_path: Path) -> None:
    df = pl.read_parquet(input_path)
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
    df.write_csv(output_path)


def export_vectorbt_npz(input_path: Path, output_path: Path) -> None:
    df = pl.read_parquet(input_path)
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
    np.savez_compressed(output_path, **out)
