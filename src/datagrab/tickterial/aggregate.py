"""OHLCV bar aggregation and integrity validation."""

from __future__ import annotations

import logging
from datetime import datetime, timedelta

import numpy as np
import pandas as pd

from .common import (
    EXPECTED_COLUMNS,
    NY_TZ,
    build_daily_bars_ny_close as common_build_daily_bars_ny_close,
    to_minute_floor as floor_to_minute,
)

logger = logging.getLogger(__name__)


def _to_epoch_ns(value: pd.Index | pd.Series | list[pd.Timestamp] | pd.DatetimeIndex) -> pd.Series:
    """Convert datetime-like values to int64 epoch nanoseconds for merge alignment."""
    dt = pd.to_datetime(value, errors="coerce", utc=True)
    if isinstance(dt, pd.Series):
        return dt.reset_index(drop=True).astype("int64")
    if isinstance(dt, pd.Index):
        return pd.Series(dt, index=range(len(dt))).astype("int64")
    return pd.Series(pd.DatetimeIndex(dt)).astype("int64")


def build_1m_bars(ticks: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
    """Resample raw ticks into 1-minute OHLCV bars aligned to [start, end).

    Args:
        ticks: DataFrame with ``datetime``, ``price``, ``volume`` columns.
        start: Inclusive start (UTC-naive).
        end: Exclusive end (UTC-naive).

    Returns:
        DataFrame with OHLCV columns indexed by minute-aligned datetime.
    """
    start_floor = floor_to_minute(start, 1)
    end_floor = floor_to_minute(end, 1)
    index = pd.date_range(start=start_floor, end=end_floor, freq="1min", inclusive="left")
    if ticks.empty:
        return pd.DataFrame(index=index, columns=EXPECTED_COLUMNS[1:]).reset_index().rename(columns={"index": "datetime"})

    series = ticks.set_index("datetime")["price"]
    ohlc = series.resample("1min", label="left", closed="left").ohlc()
    vol = (
        ticks.set_index("datetime")["volume"]
        .resample("1min", label="left", closed="left")
        .sum(min_count=1)
        .rename("volume")
    )
    bars = ohlc.join(vol, how="outer").reindex(index)
    return bars.reset_index().rename(columns={"index": "datetime"})


def build_multi_interval_bars(base_1m: pd.DataFrame, minutes: int, start: datetime, end: datetime) -> pd.DataFrame:
    """Aggregate 1m bars into *minutes*-minute bars (5m, 15m, etc.).

    Args:
        base_1m: 1-minute bars produced by :func:`build_1m_bars`.
        minutes: Target interval in minutes.
        start: Window start (UTC-naive).
        end: Window end (UTC-naive).

    Returns:
        DataFrame with OHLCV columns at the requested resolution.
    """
    if minutes == 1:
        return base_1m
    start_floor = floor_to_minute(start, minutes)
    end_floor = floor_to_minute(end, 1)
    index = pd.date_range(start=start_floor, end=end_floor, freq=f"{minutes}min", inclusive="left")
    if base_1m.empty:
        return pd.DataFrame(index=index, columns=EXPECTED_COLUMNS[1:]).reset_index().rename(columns={"index": "datetime"})

    df = base_1m.set_index("datetime")
    bars = df.resample(f"{minutes}min", label="left", closed="left").agg(
        {
            "open": "first",
            "high": "max",
            "low": "min",
            "close": "last",
            "volume": lambda s: s.sum(min_count=1),
        }
    )
    return bars.reindex(index).reset_index().rename(columns={"index": "datetime"})


def build_daily_bars_ny_close(base_1m: pd.DataFrame) -> pd.DataFrame:
    """Build daily OHLCV bars with NY 17:00 session close alignment.

    Args:
        base_1m: 1-minute bars with a ``datetime`` column in UTC.

    Returns:
        DataFrame with one row per trading day, datetime at NY 17:00 converted to UTC.
    """
    return common_build_daily_bars_ny_close(base_1m)


def check_ny_close_alignment(d1_bars: pd.DataFrame) -> str:
    """Check that all 1d bar timestamps align to NY 17:00.

    Args:
        d1_bars: Daily bars DataFrame.

    Returns:
        Human-readable status string (``"1d alignment: ok"`` on success).
    """
    if d1_bars.empty:
        return "1d alignment: empty"
    if "datetime" in d1_bars.columns:
        dt_series = pd.to_datetime(d1_bars["datetime"], errors="coerce")
    elif "shifted" in d1_bars.columns:
        dt_series = pd.to_datetime(d1_bars["shifted"], errors="coerce")
    else:
        return "1d alignment: missing datetime column"

    if dt_series.isna().all():
        return "1d alignment: all datetime invalid"

    if dt_series.dt.tz is None:
        dt_series = dt_series.dt.tz_localize("UTC")
    else:
        dt_series = dt_series.dt.tz_convert("UTC")

    ny = dt_series.dt.tz_convert(NY_TZ)
    invalid = (ny.dt.hour != 17) | (ny.dt.minute != 0) | (ny.dt.second != 0)
    if invalid.any():
        return f"1d alignment: {int(invalid.sum())} rows not aligned to 17:00 NY"
    return "1d alignment: ok"


def check_interval_integrity(interval: str, bars: pd.DataFrame, start: datetime, end: datetime) -> list[str]:
    """Run structural integrity checks on a set of OHLCV bars.

    Args:
        interval: Bar interval string (``"1m"``, ``"5m"``, ``"15m"``, ``"1d"``).
        bars: The OHLCV DataFrame to validate.
        start: Expected window start (UTC-naive).
        end: Expected window end (UTC-naive).

    Returns:
        List of issue description strings (empty means all checks passed).
    """
    issues: list[str] = []
    missing_columns = [col for col in EXPECTED_COLUMNS if col not in bars.columns]
    if missing_columns:
        issues.append(f"{interval}: missing_columns {','.join(missing_columns)}")
        return issues

    dt_utc = pd.to_datetime(bars["datetime"], errors="coerce", utc=True)
    parse_fail = int(dt_utc.isna().sum())
    if parse_fail > 0:
        issues.append(f"{interval}: datetime_parse_fail {parse_fail}")
    dt_series = dt_utc.dropna()
    if dt_series.empty:
        issues.append(f"{interval}: no_parseable_datetime")
        return issues

    dt_naive = dt_series.dt.tz_localize(None)
    if not dt_naive.is_monotonic_increasing:
        issues.append(f"{interval}: datetime_not_monotonic")
    duplicate_count = int(dt_naive.duplicated().sum())
    if duplicate_count > 0:
        issues.append(f"{interval}: duplicate_timestamps {duplicate_count}")

    if interval in {"1m", "5m", "15m"}:
        step = int(interval[:-1])
        start_floor = floor_to_minute(start, step)
        end_floor = floor_to_minute(end, 1)
        expected_index = pd.date_range(start=start_floor, end=end_floor, freq=f"{step}min", inclusive="left")
        expected_count = len(expected_index)
        if len(bars) != expected_count:
            issues.append(f"{interval}: row_count_mismatch {len(bars)}!={expected_count}")
        if expected_count > 0:
            if dt_naive.iloc[0] != expected_index[0].to_pydatetime():
                issues.append(f"{interval}: first_timestamp_unexpected {dt_naive.iloc[0].isoformat()}")
            if dt_naive.iloc[-1] != expected_index[-1].to_pydatetime():
                issues.append(f"{interval}: last_timestamp_unexpected {dt_naive.iloc[-1].isoformat()}")
    elif interval == "1d":
        alignment = check_ny_close_alignment(bars)
        if alignment != "1d alignment: ok":
            issues.append(alignment)

    for col in ("open", "high", "low", "close", "volume"):
        series = pd.to_numeric(bars[col], errors="coerce")
        if np.isinf(series).any():
            issues.append(f"{interval}: non_finite_{col}")

    open_series = pd.to_numeric(bars["open"], errors="coerce")
    high_series = pd.to_numeric(bars["high"], errors="coerce")
    low_series = pd.to_numeric(bars["low"], errors="coerce")
    close_series = pd.to_numeric(bars["close"], errors="coerce")
    valid_ohlc = open_series.notna() & high_series.notna() & low_series.notna() & close_series.notna()
    if valid_ohlc.any():
        invalid_ohlc = (
            (high_series[valid_ohlc] < open_series[valid_ohlc])
            | (high_series[valid_ohlc] < close_series[valid_ohlc])
            | (low_series[valid_ohlc] > open_series[valid_ohlc])
            | (low_series[valid_ohlc] > close_series[valid_ohlc])
            | (high_series[valid_ohlc] < low_series[valid_ohlc])
        )
        invalid_count = int(invalid_ohlc.sum())
        if invalid_count > 0:
            issues.append(f"{interval}: ohlc_relation_invalid {invalid_count}")

    return issues


def check_ohlc_consistency(
    interval: str, raw_ticks: pd.DataFrame, bars: pd.DataFrame, base_1m: pd.DataFrame | None = None
) -> list[str]:
    """Compare actual bars against expected bars recomputed from raw ticks.

    Args:
        interval: Bar interval string.
        raw_ticks: Raw tick DataFrame (``datetime``, ``price``, ``volume``).
        bars: The bars to validate.
        base_1m: Pre-computed 1m bars (optional optimisation).

    Returns:
        List of mismatch descriptions, or ``["<interval>: ohlcv ok"]`` on success.
    """
    issues: list[str] = []
    if raw_ticks.empty:
        return [f"{interval}: raw empty, skip"]

    tick_df = raw_ticks.set_index("datetime")["price"].sort_index()
    if interval == "1m":
        expected = tick_df.resample("1min", label="left", closed="left").ohlc()
        raw_volume = raw_ticks.set_index("datetime")["volume"].resample("1min", label="left", closed="left").sum(min_count=1)
        expected = expected.join(raw_volume, how="left")
    elif interval in {"5m", "15m"}:
        base = (
            base_1m
            if base_1m is not None
            else build_1m_bars(raw_ticks, raw_ticks["datetime"].min(), raw_ticks["datetime"].max() + timedelta(minutes=1))
        )
        start_bound = base["datetime"].min()
        end_bound = base["datetime"].max() + timedelta(minutes=1)
        expected = build_multi_interval_bars(base, int(interval[:-1]), start_bound, end_bound).set_index("datetime")
    elif interval == "1d":
        base = (
            base_1m
            if base_1m is not None
            else build_1m_bars(raw_ticks, raw_ticks["datetime"].min(), raw_ticks["datetime"].max() + timedelta(minutes=1))
        )
        expected = build_daily_bars_ny_close(base).set_index("datetime")
    else:
        return [f"{interval}: unsupported interval"]

    expected_df = expected.reset_index()
    if "datetime" not in expected_df.columns and "index" in expected_df.columns:
        expected_df = expected_df.rename(columns={"index": "datetime"})
    if "datetime" not in expected_df.columns:
        return [f"{interval}: expected missing datetime"]

    actual_df = bars.copy()
    if "datetime" not in actual_df.columns and "index" in actual_df.columns:
        actual_df = actual_df.rename(columns={"index": "datetime"})
    if "datetime" not in actual_df.columns:
        return [f"{interval}: actual missing datetime"]

    expected_df = expected_df.reset_index(drop=True)
    actual_df = actual_df.reset_index(drop=True)
    expected_df["__epoch"] = _to_epoch_ns(expected_df["datetime"]).to_numpy()
    actual_df["__epoch"] = _to_epoch_ns(actual_df["datetime"]).to_numpy()
    merged = expected_df.merge(actual_df, on="__epoch", suffixes=("_exp", "_act"), how="inner")
    if merged.empty:
        return [f"{interval}: no overlap to check"]

    exp_cols = merged[[col for col in merged.columns if col.endswith("_exp")] + ["__epoch"]]
    act_cols = merged[[col for col in merged.columns if col.endswith("_act")] + ["__epoch"]]

    exp_cols.columns = [c[:-4] if c.endswith("_exp") else c for c in exp_cols.columns]
    act_cols.columns = [c[:-4] if c.endswith("_act") else c for c in act_cols.columns]
    for col in ("open", "high", "low", "close", "volume"):
        if col not in exp_cols.columns:
            continue
        exp = exp_cols[col]
        act = act_cols[col]
        equal = np.isclose(
            exp.astype(float),
            act.astype(float),
            rtol=0,
            atol=1e-9,
            equal_nan=True,
        )
        bad = (~equal).sum()
        if int(bad) > 0:
            issues.append(f"{interval}: {col} mismatch {int(bad)}")
    if not issues:
        issues.append(f"{interval}: ohlcv ok")
    return issues
