"""Async tick-vault adapter for faster Dukascopy tick download."""

from __future__ import annotations

import asyncio
import inspect
import logging
from datetime import datetime, timezone
from typing import Any

import pandas as pd

from .exceptions import FetchError

logger = logging.getLogger(__name__)

try:
    from tick_vault import download_range, read_tick_data  # type: ignore
except Exception:  # pragma: no cover - depends on optional dependency
    download_range = None  # type: ignore
    read_tick_data = None  # type: ignore

TICKVAULT_AVAILABLE = download_range is not None and read_tick_data is not None


def _to_naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _first_present(mapping: pd.Series, names: tuple[str, ...]) -> str | None:
    for name in names:
        if name in mapping:
            return name
    return None


def _resolve_param_name(func: Any, candidates: tuple[str, ...]) -> str | None:
    params = inspect.signature(func).parameters
    if any(param.kind == inspect.Parameter.VAR_KEYWORD for param in params.values()):
        return candidates[0]
    for name in candidates:
        if name in params:
            return name
    return None


def _build_kwargs(
    func: Any,
    *,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    base_dir: str,
    workers: int,
) -> dict[str, Any]:
    symbol_key = _resolve_param_name(
        func,
        ("symbol", "instrument", "pair", "ticker", "asset"),
    )
    if symbol_key is None:
        raise FetchError(f"tick-vault download_range missing symbol argument: {func!r}")

    kwargs: dict[str, Any] = {symbol_key: symbol}

    start_key = _resolve_param_name(
        func,
        ("start", "start_time", "from_", "from_time", "start_datetime", "start_ts"),
    )
    if start_key is not None:
        kwargs[start_key] = window_start

    end_key = _resolve_param_name(
        func,
        ("end", "end_time", "to", "to_time", "end_datetime", "end_ts"),
    )
    if end_key is not None:
        kwargs[end_key] = window_end

    worker_key = _resolve_param_name(
        func,
        ("workers", "concurrency", "max_workers", "worker", "worker_count"),
    )
    if worker_key is not None:
        kwargs[worker_key] = workers

    base_dir_key = _resolve_param_name(
        func,
        ("cache_dir", "base_dir", "data_dir", "work_dir", "root", "storage_dir"),
    )
    if base_dir_key is not None:
        kwargs[base_dir_key] = base_dir
    return kwargs


def _timestamp_to_datetime(values: pd.Series) -> pd.Series:
    if pd.api.types.is_numeric_dtype(values):
        numeric = pd.to_numeric(values, errors="coerce")
        if numeric.empty:
            return pd.Series([pd.NaT] * len(numeric), dtype="datetime64[ns, UTC]")
        positive = numeric.dropna()
        if positive.empty:
            return pd.Series(pd.to_datetime(numeric, errors="coerce", utc=True))
        sample = float(positive.abs().max())
        # 秒级时间戳通常为 1e9，毫秒级约为 1e12
        unit = "ms" if sample > 1_000_000_000 else "s"
        return pd.to_datetime(numeric, unit=unit, utc=True, errors="coerce")

    return pd.to_datetime(values, errors="coerce", utc=True)


def _normalize_ticks_dataframe(raw: pd.DataFrame, window_start: datetime, window_end: datetime) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    frame = raw.copy()
    normalized = frame.columns.str.lower()
    frame.columns = normalized

    time_key = _first_present(frame.columns, ("time", "datetime", "ts", "timestamp", "time_utc"))
    if time_key is None:
        raise FetchError("tick-vault read_tick_data returned data without time column")

    price_key = _first_present(frame.columns, ("price", "last", "mid", "close"))
    ask_key = _first_present(frame.columns, ("ask", "ask_price"))
    bid_key = _first_present(frame.columns, ("bid", "bid_price"))
    vol_key = _first_present(frame.columns, ("volume", "ask_volume", "bid_volume", "ask-volume", "bid-volume"))
    ask_volume_key = _first_present(frame.columns, ("ask_volume", "ask-vol", "askvol", "ask_volume_", "ask_qty"))
    bid_volume_key = _first_present(frame.columns, ("bid_volume", "bid-vol", "bidvol", "bid_qty"))

    dt = _timestamp_to_datetime(frame[time_key]).dt.tz_convert("UTC").dt.tz_localize(None)
    if price_key is not None:
        price = pd.to_numeric(frame[price_key], errors="coerce")
    else:
        ask = pd.to_numeric(frame[ask_key], errors="coerce") if ask_key is not None else None
        bid = pd.to_numeric(frame[bid_key], errors="coerce") if bid_key is not None else None
        if ask is not None and bid is not None:
            price = (ask + bid) / 2
        elif ask is not None:
            price = ask
        elif bid is not None:
            price = bid
        else:
            raise FetchError("tick-vault tick data has no usable price columns")

    if vol_key is not None:
        volume = pd.to_numeric(frame[vol_key], errors="coerce")
    else:
        ask_volume = pd.to_numeric(frame[ask_volume_key], errors="coerce") if ask_volume_key is not None else None
        bid_volume = pd.to_numeric(frame[bid_volume_key], errors="coerce") if bid_volume_key is not None else None
        if ask_volume is not None and bid_volume is not None:
            volume = ask_volume.fillna(0.0) + bid_volume.fillna(0.0)
        elif ask_volume is not None:
            volume = ask_volume
        elif bid_volume is not None:
            volume = bid_volume
        else:
            volume = None

    output = pd.DataFrame(
        {
            "datetime": dt,
            "price": pd.Series(price, dtype="float"),
            "volume": pd.Series(volume, dtype="float") if volume is not None else pd.Series([None] * len(frame)),
        }
    )
    output = output.dropna(subset=["datetime", "price"])
    output = output[
        (output["datetime"] >= _to_naive_utc(window_start))
        & (output["datetime"] < _to_naive_utc(window_end))
    ]
    output = output.drop_duplicates(subset=["datetime"]).sort_values("datetime").reset_index(drop=True)
    return output.loc[:, ["datetime", "price", "volume"]]


async def _call_download_range(
    *,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    base_dir: str,
    workers: int,
) -> None:
    if not TICKVAULT_AVAILABLE:
        raise FetchError("tick-vault is not installed: pip install tick-vault")
    fn = download_range
    if fn is None:
        raise FetchError("tick-vault download_range unavailable")
    call_kwargs = _build_kwargs(
        fn,
        symbol=symbol,
        window_start=window_start,
        window_end=window_end,
        base_dir=base_dir,
        workers=workers,
    )
    result = fn(**call_kwargs)
    if inspect.isawaitable(result):
        await result


async def _read_tick_data(
    *,
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    base_dir: str,
) -> pd.DataFrame:
    if not TICKVAULT_AVAILABLE:
        raise FetchError("tick-vault is not installed: pip install tick-vault")
    fn = read_tick_data
    if fn is None:
        raise FetchError("tick-vault read_tick_data unavailable")

    candidates = (
        {"symbol": symbol, "start": window_start, "end": window_end, "cache_dir": base_dir},
        {"symbol": symbol, "start": window_start, "end": window_end, "base_dir": base_dir},
        {"instrument": symbol, "start": window_start, "end": window_end, "cache_dir": base_dir},
        {"instrument": symbol, "from_": window_start, "to": window_end, "base_dir": base_dir},
    )

    last_error: Exception | None = None
    for raw_kwargs in candidates:
        try:
            candidate_kwargs = {name: value for name, value in raw_kwargs.items() if _resolve_param_name(fn, (name,))}
            if not candidate_kwargs:
                continue
            maybe_data = fn(**candidate_kwargs)
            if inspect.isawaitable(maybe_data):
                maybe_data = await maybe_data
            if isinstance(maybe_data, pd.DataFrame):
                return _normalize_ticks_dataframe(maybe_data, window_start, window_end)
            if hasattr(maybe_data, "to_pandas"):
                return _normalize_ticks_dataframe(pd.DataFrame(maybe_data), window_start, window_end)
            if maybe_data is None:
                return pd.DataFrame(columns=["datetime", "price", "volume"])
            return _normalize_ticks_dataframe(pd.DataFrame(maybe_data), window_start, window_end)
        except (TypeError, ValueError, OverflowError, FetchError) as exc:
            last_error = exc
            continue
    if last_error is None:
        return pd.DataFrame(columns=["datetime", "price", "volume"])
    raise FetchError(f"tick-vault read_tick_data 调用失败: {last_error}") from last_error


async def fetch_ticks_async(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    *,
    base_dir: str,
    workers: int,
) -> pd.DataFrame:
    """Fetch ticks from tick-vault and return a datagab-compatible DataFrame."""
    if not TICKVAULT_AVAILABLE:
        raise FetchError("tick-vault is not installed: pip install tick-vault")
    if window_start >= window_end:
        return pd.DataFrame(columns=["datetime", "price", "volume"])

    start_naive = _to_naive_utc(window_start)
    end_naive = _to_naive_utc(window_end)
    logger.info("tick-vault downloading symbol=%s %s -> %s", symbol, start_naive.isoformat(), end_naive.isoformat())
    await _call_download_range(
        symbol=symbol,
        window_start=window_start,
        window_end=window_end,
        base_dir=base_dir,
        workers=workers,
    )
    return await _read_tick_data(
        symbol=symbol,
        window_start=window_start,
        window_end=window_end,
        base_dir=base_dir,
    )


def fetch_ticks(
    symbol: str,
    window_start: datetime,
    window_end: datetime,
    *,
    base_dir: str,
    workers: int,
) -> pd.DataFrame:
    """Synchronous wrapper of :func:`fetch_ticks_async` for compatibility."""
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(
            fetch_ticks_async(
                symbol,
                window_start,
                window_end,
                base_dir=base_dir,
                workers=workers,
            )
        )
    raise FetchError("fetch_ticks called within running asyncio loop; call fetch_ticks_async directly")
