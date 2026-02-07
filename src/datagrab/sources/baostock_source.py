from __future__ import annotations

import contextlib
import io
import time
from datetime import datetime
from threading import Lock

import pandas as pd
import polars as pl

from ..config import AppConfig, FilterConfig
from ..logging import get_logger
from ..pipeline.catalog import CatalogService
from ..rate_limiter import RateLimiter
from ..storage.schema import normalize_ohlcv_columns
from ..timeutils import BEIJING_TZ, to_beijing
from .base import DataSource, OhlcvResult, SymbolInfo


class BaostockDataSource(DataSource):
    def __init__(self, config: AppConfig, rate_limiter: RateLimiter, catalog: CatalogService):
        self.config = config
        self.rate_limiter = rate_limiter
        self.catalog = catalog
        self.logger = get_logger("datagrab.baostock")
        self._login_lock = Lock()
        self._query_lock = Lock()
        self._logged_in = False
        self._bs = None

    def list_symbols(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
    ) -> list[SymbolInfo]:
        result = self.catalog.get_catalog(
            asset_type=asset_type,
            refresh=refresh,
            limit=limit,
            filters_override=filters_override,
        )
        self.logger.info("catalog source=%s size=%d", result.source, len(result.items))
        return result.items

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        adjust: str,
    ) -> OhlcvResult:
        freq = self._map_interval(interval)
        adjust_flag = self._map_adjust(adjust)
        start_str = to_beijing(start).strftime("%Y-%m-%d")
        end_str = to_beijing(end).strftime("%Y-%m-%d")
        fields = "date,code,open,high,low,close,volume"
        if freq in {"5", "15", "30", "60"}:
            fields = "date,time,code,open,high,low,close,volume"

        for attempt in range(self.config.download.max_retries + 1):
            self._ensure_login()
            try:
                with self._query_lock:
                    self.rate_limiter.wait()
                    df = self._query_history(symbol, fields, start_str, end_str, freq, adjust_flag)
                if df is None or df.empty:
                    return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                df = self._normalize_datetime(df)
                pl_df = pl.from_pandas(df)
                pl_df = normalize_ohlcv_columns(pl_df)
                return OhlcvResult(data=pl_df, adjustment=adjust)
            except Exception as exc:
                if attempt >= self.config.download.max_retries:
                    raise
                delay = self.rate_limiter.backoff(attempt + 1)
                self.logger.warning("fetch failed for %s (retry in %.1fs): %s", symbol, delay, exc)
                time.sleep(delay)
                self._logged_in = False
        return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)

    def _ensure_login(self) -> None:
        with self._login_lock:
            if self._logged_in:
                return
            import baostock as bs

            self._bs = bs
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                resp = bs.login()
            if resp.error_code != "0":
                raise RuntimeError(f"baostock login failed: {resp.error_msg}")
            self._logged_in = True

    def _query_history(
        self,
        symbol: str,
        fields: str,
        start: str,
        end: str,
        freq: str,
        adjust_flag: str,
    ) -> pd.DataFrame | None:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rs = self._bs.query_history_k_data_plus(
                symbol,
                fields,
                start_date=start,
                end_date=end,
                frequency=freq,
                adjustflag=adjust_flag,
            )
        if rs.error_code != "0":
            raise RuntimeError(f"baostock error {rs.error_code}: {rs.error_msg}")
        return rs.get_data()

    def _normalize_datetime(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        if "time" in df.columns:
            dt = pd.to_datetime(df["time"], format="%Y%m%d%H%M%S", errors="coerce")
        else:
            dt = pd.to_datetime(df["date"], format="%Y-%m-%d", errors="coerce")
        dt = dt.dt.tz_localize(BEIJING_TZ, nonexistent="shift_forward", ambiguous="NaT").dt.tz_localize(None)
        df["datetime"] = dt
        df = df.drop(columns=[c for c in ["date", "time"] if c in df.columns])
        return df

    def _map_interval(self, interval: str) -> str:
        interval = interval.strip().lower()
        mapping = {
            "1d": "d",
            "1w": "w",
            "1wk": "w",
            "1mo": "m",
            "1min": "1",
            "1m": "1",
            "5m": "5",
            "15m": "15",
            "30m": "30",
            "60m": "60",
            "1h": "60",
        }
        if interval not in mapping:
            raise ValueError(f"unsupported interval for baostock: {interval}")
        return mapping[interval]

    def _map_adjust(self, adjust: str) -> str:
        key = (adjust or "").strip().lower()
        default = self.config.baostock.adjust_default
        if key == "auto":
            key = default
        mapping = {
            "front": "2",
            "forward": "2",
            "back": "1",
            "backward": "1",
            "none": "3",
            "raw": "3",
        }
        if key not in mapping:
            key = default if default in mapping else "back"
        return mapping[key]
