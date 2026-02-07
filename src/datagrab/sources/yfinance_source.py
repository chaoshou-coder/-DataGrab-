from __future__ import annotations

import contextlib
import io
import inspect
import time
from datetime import datetime

import pandas as pd
import polars as pl
import yfinance as yf

from ..config import AppConfig, FilterConfig
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
        auto_adjust = adjust.lower() in {"auto", "back", "forward"}
        start_str = to_beijing(start).strftime("%Y-%m-%d")
        end_str = to_beijing(end).strftime("%Y-%m-%d")
        for attempt in range(self.config.download.max_retries + 1):
            try:
                self.rate_limiter.wait()
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    df = self._download_df(
                        symbol=symbol,
                        start=start_str,
                        end=end_str,
                        interval=interval,
                        auto_adjust=auto_adjust,
                    )
                if df is None or df.empty:
                    return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                df = df.reset_index()
                # yfinance 有时返回 MultiIndex 列（第二层为 ticker），先拍扁
                if isinstance(df.columns, pd.MultiIndex):
                    df.columns = [str(c[0]) if isinstance(c, tuple) else str(c) for c in df.columns]
                else:
                    df.columns = [str(c) for c in df.columns]
                # yfinance 在部分场景下 reset_index() 的列名可能是 index/Date/Datetime
                rename_candidates = {
                    "Date": "datetime",
                    "Datetime": "datetime",
                    "date": "datetime",
                    "index": "datetime",
                }
                df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})
                if "datetime" not in df.columns:
                    # 尝试寻找一个“像时间”的列作为兜底
                    for col in df.columns:
                        if str(col).strip().lower() in {"open", "high", "low", "close", "adj close", "volume"}:
                            continue
                        series = df[col]
                        if pd.api.types.is_datetime64_any_dtype(series) or (
                            len(series) > 0 and isinstance(series.iloc[0], (pd.Timestamp, datetime))
                        ):
                            df = df.rename(columns={col: "datetime"})
                            break
                if "datetime" not in df.columns:
                    raise RuntimeError(f"datetime not found in yfinance result columns={list(df.columns)}")
                df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                try:
                    df = df.dropna(subset=["datetime"])
                except KeyError as exc:
                    raise RuntimeError(
                        f"datetime not found after normalization; columns={list(df.columns)}"
                    ) from exc
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

    def _download_df(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
    ):
        """兼容不同 yfinance 版本的 download() 调用。

        一些 yfinance 版本不支持 download(proxy=...)，会报：
          download() got an unexpected keyword argument 'proxy'
        这里会自动回退到 session.proxies（若支持）或不使用代理重试。
        """
        kwargs = dict(
            start=start,
            end=end,
            interval=interval,
            auto_adjust=auto_adjust,
            progress=False,
            threads=False,
        )
        proxy = self.config.yfinance.proxy
        if not proxy:
            return yf.download(symbol, **kwargs)

        # 1) 尝试 proxy=
        try:
            sig = inspect.signature(yf.download)
        except Exception:
            sig = None

        if sig and "proxy" in sig.parameters:
            try:
                return yf.download(symbol, **kwargs, proxy=proxy)
            except TypeError:
                pass

        # 2) 回退到 session=
        if sig and "session" in sig.parameters:
            try:
                import requests

                session = requests.Session()
                session.proxies.update({"http": proxy, "https": proxy})
                return yf.download(symbol, **kwargs, session=session)
            except Exception as exc:
                self.logger.warning("yfinance session proxy failed for %s: %s", symbol, exc)

        # 3) 最后：不带代理参数
        self.logger.warning(
            "yfinance.download proxy kwarg unsupported; retry without proxy for %s", symbol
        )
        return yf.download(symbol, **kwargs)
