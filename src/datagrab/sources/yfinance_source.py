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
        adjust = adjust.lower()
        if adjust not in {"auto", "none"}:
            raise ValueError(
                "yfinance strict adjust policy only supports auto|none for non-ashare assets. "
                "If you need back/forward/repaired adjustments, use --asset-type ashare."
            )
        auto_adjust = adjust == "auto"
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
                if not isinstance(df, pd.DataFrame):
                    df = self._download_df_history(
                        symbol=symbol,
                        start=start_str,
                        end=end_str,
                        interval=interval,
                        auto_adjust=auto_adjust,
                    )
                # yfinance 在少量特殊标的（如含 '=') 可能返回空 DataFrame 并记录内部错误，但仍可通过 history() 补齐
                if (df is None or df.empty) and "=" in symbol:
                    try:
                        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                            fallback_df = self._download_df_history(
                                symbol=symbol,
                                start=start_str,
                                end=end_str,
                                interval=interval,
                                auto_adjust=auto_adjust,
                            )
                    except Exception as fallback_exc:
                        if self._is_empty_chart_error(fallback_exc):
                            fallback_df = None
                        else:
                            raise
                    if fallback_df is not None and not fallback_df.empty:
                        df = fallback_df
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
                # 某些市场/时间粒度下会出现重复列名，Polars 不允许，先做去重
                seen: dict[str, int] = {}
                unique_columns: list[str] = []
                for raw in df.columns:
                    base = str(raw).strip()
                    if not base:
                        base = "col"
                    idx = seen.get(base, 0)
                    seen[base] = idx + 1
                    unique_columns.append(base if idx == 0 else f"{base}_{idx}")
                df.columns = unique_columns

                dt = df["datetime"]
                if dt.dt.tz is None:
                    dt = dt.dt.tz_localize("UTC")
                dt = dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
                df["datetime"] = dt
                pl_df = pl.from_pandas(df)
                pl_df = normalize_ohlcv_columns(pl_df)
                return OhlcvResult(data=pl_df, adjustment=adjust)
            except Exception as exc:
                if self._is_empty_chart_error(exc):
                    self.logger.warning(
                        "yfinance chart data empty for %s (attempt %s/%s), fallback history()",
                        symbol,
                        attempt + 1,
                        self.config.download.max_retries + 1,
                    )
                    try:
                        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                            fallback_df = self._download_df_history(
                                symbol=symbol,
                                start=start_str,
                                end=end_str,
                                interval=interval,
                                auto_adjust=auto_adjust,
                            )
                    except Exception as fallback_exc:
                        if self._is_empty_chart_error(fallback_exc):
                            self.logger.warning(
                                "yfinance history() also returned empty for %s, treat as no data: %s",
                                symbol,
                                fallback_exc,
                            )
                            return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                        if attempt < self.config.download.max_retries:
                            delay = self.rate_limiter.backoff(attempt + 1)
                            self.logger.warning(
                                "history() failed for %s (retry in %.1fs): %s", symbol, delay, fallback_exc
                            )
                            time.sleep(delay)
                            continue
                        raise
                    if fallback_df is None or fallback_df.empty:
                        return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                    df = fallback_df
                    df = df.reset_index()
                    # yfinance 一些历史路径下列名不同，复用统一处理
                    if isinstance(df.columns, pd.MultiIndex):
                        df.columns = [str(c[0]) if isinstance(c, tuple) else str(c) for c in df.columns]
                    else:
                        df.columns = [str(c) for c in df.columns]
                    rename_candidates = {
                        "Date": "datetime",
                        "Datetime": "datetime",
                        "date": "datetime",
                        "index": "datetime",
                    }
                    df = df.rename(columns={k: v for k, v in rename_candidates.items() if k in df.columns})
                    if "datetime" not in df.columns:
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
                        self.logger.warning("yfinance history() result lacks datetime for %s, skip this task", symbol)
                        return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
                    df["datetime"] = pd.to_datetime(df["datetime"], errors="coerce")
                    df = df.dropna(subset=["datetime"])
                    seen: dict[str, int] = {}
                    unique_columns: list[str] = []
                    for raw in df.columns:
                        base = str(raw).strip() or "col"
                        idx = seen.get(base, 0)
                        seen[base] = idx + 1
                        unique_columns.append(base if idx == 0 else f"{base}_{idx}")
                    df.columns = unique_columns
                    dt = df["datetime"]
                    if dt.dt.tz is None:
                        dt = dt.dt.tz_localize("UTC")
                    dt = dt.dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)
                    df["datetime"] = dt
                    pl_df = pl.from_pandas(df)
                    pl_df = normalize_ohlcv_columns(pl_df)
                    return OhlcvResult(data=pl_df, adjustment=adjust)

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
        try:
            sig = inspect.signature(yf.download)
            if "raise_errors" in sig.parameters:
                kwargs["raise_errors"] = False
        except Exception:
            pass
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

    def _download_df_history(
        self,
        symbol: str,
        start: str,
        end: str,
        interval: str,
        auto_adjust: bool,
    ):
        kwargs = dict(
            start=start,
            end=end,
            interval=interval,
            auto_adjust=auto_adjust,
            progress=False,
            actions=False,
            prepost=False,
            repair=False,
            threads=False,
        )
        return yf.Ticker(symbol).history(**kwargs)

    @staticmethod
    def _is_empty_chart_error(exc: Exception) -> bool:
        msg = str(exc)
        return (
            "'NoneType' object is not subscriptable" in msg
            or "chart']['result'] is None" in msg
            or "No data found, symbol may be delisted" in msg
            or "No data found" in msg
        )
