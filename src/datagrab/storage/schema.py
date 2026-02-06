from __future__ import annotations

from dataclasses import dataclass

import polars as pl


BASE_COLUMNS = ["datetime", "open", "high", "low", "close", "volume"]
ADJUSTED_COLUMN = "adjusted_close"


@dataclass(frozen=True)
class SchemaInfo:
    columns: list[str]
    has_adjusted: bool


def normalize_ohlcv_columns(df: pl.DataFrame) -> pl.DataFrame:
    rename_map = {}
    for col in df.columns:
        lowered = col.strip().lower().replace(" ", "_")
        rename_map[col] = lowered
    df = df.rename(rename_map)
    if "date" in df.columns and "datetime" not in df.columns:
        df = df.rename({"date": "datetime"})
    if "adj_close" in df.columns and ADJUSTED_COLUMN not in df.columns:
        df = df.rename({"adj_close": ADJUSTED_COLUMN})
    return df


def schema_info(df: pl.DataFrame) -> SchemaInfo:
    has_adjusted = ADJUSTED_COLUMN in df.columns
    cols = list(BASE_COLUMNS)
    if has_adjusted:
        cols.append(ADJUSTED_COLUMN)
    return SchemaInfo(columns=cols, has_adjusted=has_adjusted)
