from __future__ import annotations

import contextlib
import io
import time
from datetime import datetime

import pandas as pd
import polars as pl
import yfinance as yf

from ..config import AppConfig
from ..logging import get_logger
from ..pipeline.catalog import CatalogService
from ..rate_limiter import RateLimiter
from ..storage.schema import normalize_ohlcv_columns
from ..timeutils import to_beijing
from .base import DataSource, OhlcvResult, SymbolInfo


class YFinanceDataSource(DataSource):
    def __init__(self, config: AppConfig, rate_limiter: RateLimiter, catalog: CatalogService):
        self.config = config
        self.rate_limiter = rate_limiter
        self.catalog = catalog
        self.logger = get_logger("datagrab.yfinance")

    def list_symbols(self, asset_type: str, refresh: bool = False, limit: int | None = None) -> list[SymbolInfo]:
        result = self.catalog.get_catalog(asset_type=asset_type, refresh=refresh, limit=limit)
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
        auto_adjust = adjust.lower() in {"auto", "back", "forward"}
        start_str = to_beijing(start).strftime("%Y-%m-%d")
        end_str = to_beijing(end).strftime("%Y-%m-%d")
        for attempt in range(self.config.download.max_retries + 1):
            try:
                self.rate_limiter.wait()
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = yf.download(
                        symbol,
                        start=start_str,
                        end=end_str,
                        interval=interval,
                        auto_adjust=auto_adjust,
                        progress=False,
                        threads=False,
                        proxy=self.config.yfinance.proxy,
                    )
                if df is None or df.empty:
                    return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                df = df.reset_index()
                df = df.rename(columns={"Date": "datetime", "Datetime": "datetime"})
                if isinstance(df["datetime"].iloc[0], pd.Timestamp):
                    dt = df["datetime"]
                    if dt.dt.tz is None:
                        dt = dt.dt.tz_localize("UTC")
                    dt = dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
                    df["datetime"] = dt
                pl_df = pl.from_pandas(df)
                pl_df = normalize_ohlcv_columns(pl_df)
                return OhlcvResult(data=pl_df, adjustment=adjust)
            except Exception as exc:
                msg = str(exc)
                if "429" in msg or "Too Many Requests" in msg:
                    delay = self.rate_limiter.backoff(attempt + 1)
                    self.logger.warning("rate limited for %s, sleep %.1fs", symbol, delay)
                    time.sleep(delay)
                    continue
                if attempt >= self.config.download.max_retries:
                    raise
                delay = self.rate_limiter.backoff(attempt + 1)
                self.logger.warning("fetch failed for %s (retry in %.1fs): %s", symbol, delay, exc)
                time.sleep(delay)
        return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
