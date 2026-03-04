from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pandas as pd
import polars as pl
from ..tickterial.common import EXPECTED_COLUMNS, VALID_INTERVALS, build_daily_bars_ny_close

from ..config import AppConfig, FilterConfig
from ..logging import get_logger
from ..pipeline.catalog import CatalogService
from ..rate_limiter import RateLimiter
from ..storage.schema import normalize_ohlcv_columns
from .base import DataSource, OhlcvResult, SymbolInfo
from ..tickterial import download as tickterial_download

UTC = timezone.utc
SOURCE_NAME = "tickterial"
_DEFAULT_TICKTERIAL_DOWNLOAD_WORKERS = 10
_DEFAULT_TICKTERIAL_BATCH_SIZE = 8
_DEFAULT_TICKTERIAL_BATCH_PAUSE_MS = 1000
_DEFAULT_TICKTERIAL_RETRY_DELAY = 1.5
_DEFAULT_TICKTERIAL_RETRY_JITTER_MS = 300


class TickterialDataSource(DataSource):
    def __init__(self, config: AppConfig, rate_limiter: RateLimiter, catalog: CatalogService):
        self.config = config
        self.rate_limiter = rate_limiter
        self.catalog = catalog
        self.logger = get_logger("datagrab.tickterial")
        self._assert_library_available()

    def list_symbols(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
    ) -> list[SymbolInfo]:
        symbols: list[str] = []
        for symbol in self.config.tickterial.symbols:
            normalized = str(symbol or "").strip().upper()
            if normalized:
                symbols.append(normalized)
        return [
            SymbolInfo(
                symbol=item,
                name=f"{item} (tickterial)",
                exchange=None,
                asset_type=asset_type,
            )
            for item in symbols
        ]

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        adjust: str,
    ) -> OhlcvResult:
        interval = interval.strip().lower()
        if interval not in VALID_INTERVALS:
            raise ValueError(f"unsupported interval for tickterial: {interval}")
        norm_symbol = str(symbol or "").strip().upper()
        if not norm_symbol:
            raise ValueError("symbol is required")
        start_dt = self._ensure_utc(start)
        end_dt = self._ensure_utc(end)
        if start_dt >= end_dt:
            raise ValueError("start must be earlier than end")
        ticks = self._fetch_ticks(norm_symbol, start_dt, end_dt)
        if ticks.empty:
            return OhlcvResult(data=pl.DataFrame(), adjustment=adjust or "none", metadata=self._metadata(norm_symbol))
        if interval != "1m":
            bars_1m = self._build_1m_bars(ticks, start_dt, end_dt)
            bars = self._resample_bars(bars_1m, interval)
        else:
            bars = self._build_1m_bars(ticks, start_dt, end_dt)
        pl_df = pl.from_pandas(bars[list(EXPECTED_COLUMNS)])
        pl_df = normalize_ohlcv_columns(pl_df)
        return OhlcvResult(data=pl_df, adjustment=adjust or "none", metadata=self._metadata(norm_symbol))

    def _metadata(self, symbol: str) -> dict[str, str]:
        return {
            "source": SOURCE_NAME,
            "source.symbol": symbol.upper(),
            "source.price_basis": self.config.tickterial.price_basis,
            "source.interval_tz": "UTC",
            "source.ny_close_hour": str(self.config.tickterial.ny_close_hour),
            "source.utcoffset": str(self.config.tickterial.utcoffset),
        }

    def _ensure_utc(self, dt: datetime) -> datetime:
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt.astimezone(UTC).replace(tzinfo=None)

    def _assert_library_available(self) -> None:
        if tickterial_download.Tickloader is None:
            raise RuntimeError("tickterial not installed: pip install tickterial")

    def _fetch_ticks(self, symbol: str, start: datetime, end: datetime) -> pd.DataFrame:
        try:
            return tickterial_download.fetch_ticks(
                symbol=symbol.upper(),
                window_start=self._ensure_utc(start),
                window_end=self._ensure_utc(end),
                max_retries=self.config.tickterial.max_retries,
                retry_delay=_DEFAULT_TICKTERIAL_RETRY_DELAY,
                download_workers=max(1, self.config.download.concurrency or _DEFAULT_TICKTERIAL_DOWNLOAD_WORKERS),
                batch_size=_DEFAULT_TICKTERIAL_BATCH_SIZE,
                batch_pause_ms=_DEFAULT_TICKTERIAL_BATCH_PAUSE_MS,
                retry_jitter_ms=_DEFAULT_TICKTERIAL_RETRY_JITTER_MS,
                cache_dir=self.config.tickterial.cache_dir,
                source_timestamp_shift_hours=self.config.tickterial.source_timestamp_shift_hours,
            )
        except Exception as exc:
            raise RuntimeError(f"tickterial fetch failed for {symbol}: {exc}") from exc

    def _build_1m_bars(self, ticks: pd.DataFrame, start: datetime, end: datetime) -> pd.DataFrame:
        start_dt = self._dt_to_datetime(start)
        end_dt = self._dt_to_datetime(end)
        if start_dt.tzinfo is not None:
            start_dt = start_dt.astimezone(UTC).replace(tzinfo=None)
        if end_dt.tzinfo is not None:
            end_dt = end_dt.astimezone(UTC).replace(tzinfo=None)
        start_floor = self._to_minute_floor(start_dt, 1)
        index = pd.date_range(start=start_floor, end=end_dt, freq="1min", inclusive="left")
        if ticks.empty:
            return pd.DataFrame(index=index, columns=["open", "high", "low", "close", "volume"]).reset_index().rename(
                columns={"index": "datetime"}
            )
        tick_index = pd.to_datetime(ticks["datetime"])
        if tick_index.dt.tz is not None:
            tick_index = tick_index.dt.tz_convert("UTC").dt.tz_localize(None)
        ticks = ticks.copy()
        ticks["datetime"] = tick_index
        price = ticks.set_index("datetime")["price"]
        ohlc = price.resample("1min", label="left", closed="left").ohlc()
        volume = (
            ticks.set_index("datetime")["volume"]
            .resample("1min", label="left", closed="left")
            .sum(min_count=1)
            .rename("volume")
        )
        bars = ohlc.join(volume, how="outer").reindex(index)
        bars = bars.rename_axis(index="datetime")
        return bars.reset_index()[list(EXPECTED_COLUMNS)]

    def _resample_bars(self, bars_1m: pd.DataFrame, interval: str) -> pd.DataFrame:
        if interval == "1m":
            return bars_1m
        if interval == "1d":
            return self._build_daily_bars_ny_close(bars_1m)
        minutes = int(interval[:-1])
        start = self._to_minute_floor(self._dt_to_datetime(bars_1m["datetime"].min()), minutes)
        end = self._to_minute_floor(self._dt_to_datetime(bars_1m["datetime"].max()), 1) + timedelta(minutes=1)
        index = pd.date_range(start=start, end=end, freq=f"{minutes}min", inclusive="left")
        if bars_1m.empty:
            return pd.DataFrame(index=index, columns=["open", "high", "low", "close", "volume"]).reset_index().rename(
                columns={"index": "datetime"}
            )
        df = bars_1m.set_index("datetime")
        aggregated = df.resample(f"{minutes}min", label="left", closed="left").agg(
            {
                "open": "first",
                "high": "max",
                "low": "min",
                "close": "last",
                "volume": lambda x: x.sum(min_count=1),
            }
        )
        return aggregated.reindex(index).reset_index().rename(columns={"index": "datetime"})

    @staticmethod
    def _to_minute_floor(dt: datetime | pd.Timestamp, minute: int) -> datetime:
        dt_obj = pd.Timestamp(dt)
        dt_obj = dt_obj.to_pydatetime()
        return dt_obj.replace(
            minute=dt_obj.minute - (dt_obj.minute % minute),
            second=0,
            microsecond=0,
        )

    @staticmethod
    def _dt_to_datetime(value: datetime | pd.Timestamp | str) -> datetime:
        if isinstance(value, pd.Timestamp):
            return value.to_pydatetime()
        if isinstance(value, datetime):
            return value
        return pd.Timestamp(value).to_pydatetime()

    def _build_daily_bars_ny_close(self, bars_1m: pd.DataFrame) -> pd.DataFrame:
        return build_daily_bars_ny_close(bars_1m)
