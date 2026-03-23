"""Async HTTP data source using httpx with session reuse.

Provides faster US stock/forex/crypto downloads by:
- Reusing HTTP connections via persistent session
- Async request handling for concurrent downloads
- Integration with quantdb SQLite cache layer
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Any

import pandas as pd
import polars as pl

from ..config import AppConfig, FilterConfig
from ..logging import get_logger
from ..pipeline.catalog import CatalogService
from ..rate_limiter import RateLimiter
from ..storage.schema import normalize_ohlcv_columns
from ..timeutils import to_beijing
from .base import DataSource, OhlcvResult, SymbolInfo


class HttpxDataSource(DataSource):
    """Async HTTP data source using httpx with persistent session."""

    def __init__(self, config: AppConfig, rate_limiter: RateLimiter, catalog: CatalogService):
        self.config = config
        self.rate_limiter = rate_limiter
        self.catalog = catalog
        self.logger = get_logger("datagrab.httpx")
        self._session: Any = None
        self._client: Any = None

    async def _get_client(self):
        """Lazily initialize httpx async client with connection pooling."""
        if self._client is None:
            import httpx

            limits = httpx.Limits(max_keepalive_connections=20, max_connections=100)
            self._client = httpx.AsyncClient(
                limits=limits,
                timeout=httpx.Timeout(30.0, connect=10.0),
                follow_redirects=True,
            )
        return self._client

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
        return asyncio.run(self._fetch_ohlcv_async(symbol, interval, start, end, adjust))

    async def _fetch_ohlcv_async(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        adjust: str,
    ) -> OhlcvResult:
        """Fetch OHLCV data using async httpx.

        This method is designed to work with the QuantDB cache layer.
        For full async pipeline, use quantdb_source which wraps this source.
        """
        start_str = to_beijing(start).strftime("%Y-%m-%d")
        end_str = to_beijing(end).strftime("%Y-%m-%d")

        for attempt in range(self.config.download.max_retries + 1):
            try:
                client = await self._get_client()
                await self.rate_limiter.async_wait()

                df = await self._fetch_via_api(client, symbol, interval, start_str, end_str, adjust)
                if df is not None and not df.empty:
                    pl_df = pl.from_pandas(df)
                    pl_df = normalize_ohlcv_columns(pl_df)
                    return OhlcvResult(data=pl_df, adjustment=adjust)

                return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)
            except Exception as exc:
                if attempt >= self.config.download.max_retries:
                    self.logger.error("fetch failed for %s after %d attempts: %s", symbol, attempt + 1, exc)
                    raise
                delay = self.rate_limiter.backoff(attempt + 1)
                self.logger.warning("fetch failed for %s (retry in %.1fs): %s", symbol, delay, exc)
                await asyncio.sleep(delay)

        return OhlcvResult(data=pl.DataFrame(), adjustment=adjust)

    async def _fetch_via_api(
        self,
        client: Any,
        symbol: str,
        interval: str,
        start: str,
        end: str,
        adjust: str,
    ) -> pd.DataFrame | None:
        """Fetch data from Yahoo Finance API via httpx.

        Uses the unofficial Yahoo Finance API endpoint which is faster
        than yfinance's download() method.
        """
        import httpx

        interval_map = {
            "1m": "1m",
            "2m": "2m",
            "5m": "5m",
            "15m": "15m",
            "30m": "30m",
            "60m": "60m",
            "90m": "90m",
            "1h": "60m",
            "1d": "1d",
            "5d": "5d",
            "1wk": "1wk",
            "1mo": "1mo",
        }
        interval_str = interval_map.get(interval, interval)

        url = f"https://query1.finance.yahoo.com/v8/finance/chart/{symbol}"
        params = {
            "period1": int(datetime.strptime(start, "%Y-%m-%d").timestamp()),
            "period2": int(datetime.strptime(end, "%Y-%m-%d").timestamp()),
            "interval": interval_str,
            "events": "div,split",
        }
        if adjust and adjust.lower() != "none":
            params["adjParam"] = "1"

        response = await client.get(url, params=params)
        if response.status_code == 404:
            self.logger.warning("symbol not found: %s", symbol)
            return None
        if response.status_code == 429:
            raise RuntimeError(f"rate limited: 429 for {symbol}")

        response.raise_for_status()
        data = response.json()

        result = data.get("chart", {}).get("result")
        if not result:
            return None

        result_data = result[0]
        timestamps = result_data["timestamp"]
        indicators = result_data.get("indicators", {})
        quote = indicators.get("quote", [{}])[0]
        adj_close = indicators.get("adjclose", [{}])[0]

        df = pd.DataFrame({"datetime": timestamps})
        for col in ["open", "high", "low", "close", "volume"]:
            df[col] = quote.get(col, [])

        if adj_close and "adjclose" in adj_close:
            df["adjusted_close"] = adj_close["adjclose"]

        df["datetime"] = pd.to_datetime(df["datetime"], unit="s", utc=True)
        df["datetime"] = df["datetime"].dt.tz_convert("Asia/Shanghai").dt.tz_localize(None)

        df = df[df["close"].notna()]
        return df

    async def close(self) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    def __del__(self) -> None:
        """Ensure client is closed on deletion."""
        if self._client is not None:
            try:
                asyncio.run(self.close())
            except Exception:
                pass
