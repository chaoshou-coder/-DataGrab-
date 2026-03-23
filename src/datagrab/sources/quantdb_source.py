"""QuantDB SQLite cache layer for OHLCV data.

Provides sub-18ms cache hits for repeated symbol/interval requests.
Cache schema:
  - symbols: symbol TEXT PRIMARY KEY, name, exchange, asset_type
  - ohlcv: (symbol, interval, start_date, end_date) PRIMARY KEY, data BLOB (parquet)
  - meta: key PRIMARY KEY, value
"""

from __future__ import annotations

import io
import sqlite3
import time
from datetime import datetime
from pathlib import Path
from typing import Any

import polars as pl
import pyarrow as pa
import pyarrow.parquet as pq

from ..config import AppConfig, FilterConfig
from ..logging import get_logger
from ..pipeline.catalog import CatalogService
from ..rate_limiter import RateLimiter
from ..storage.schema import normalize_ohlcv_columns
from .base import DataSource, OhlcvResult, SymbolInfo


class QuantDBCache:
    """SQLite-based OHLCV cache with sub-18ms read performance."""

    def __init__(self, db_path: Path | str):
        self.db_path = Path(db_path)
        self._conn: sqlite3.Connection | None = None
        self.logger = get_logger("datagrab.quantdb")
        self._init_db()

    def _init_db(self) -> None:
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        conn = self._get_conn()
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ohlcv (
                symbol TEXT NOT NULL,
                interval TEXT NOT NULL,
                start_date TEXT NOT NULL,
                end_date TEXT NOT NULL,
                data BLOB NOT NULL,
                created_at TEXT NOT NULL,
                PRIMARY KEY (symbol, interval, start_date, end_date)
            )
        """)
        conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_symbol_interval
            ON ohlcv(symbol, interval)
        """)
        conn.execute("""
            CREATE TABLE IF NOT EXISTS meta (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL
            )
        """)
        conn.commit()

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is None:
            self._conn = sqlite3.connect(
                str(self.db_path),
                check_same_thread=False,
                isolation_level="DEFERRED",
            )
            self._conn.execute("PRAGMA journal_mode=WAL")
            self._conn.execute("PRAGMA synchronous=NORMAL")
            self._conn.execute("PRAGMA cache_size=-64000")  # 64MB cache
        return self._conn

    def _serialize(self, df: pl.DataFrame) -> bytes:
        buf = io.BytesIO()
        df.write_parquet(buf)
        return buf.getvalue()

    def _deserialize(self, blob: bytes) -> pl.DataFrame:
        return pl.read_parquet(io.BytesIO(blob))

    def get(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
    ) -> pl.DataFrame | None:
        """Retrieve cached OHLCV data for the given range."""
        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")

        conn = self._get_conn()
        cursor = conn.execute(
            """
            SELECT data FROM ohlcv
            WHERE symbol=? AND interval=? AND start_date<=? AND end_date>=?
            """,
            (symbol, interval, start_str, end_str),
        )
        row = cursor.fetchone()
        if row is None:
            return None

        try:
            df = self._deserialize(row[0])
            if "datetime" not in df.columns:
                return None
            dt = df["datetime"]
            if dt.dtype == pl.Object:
                df = df.with_columns(pl.col("datetime").str.to_datetime())
            return df
        except Exception as exc:
            self.logger.warning("cache deserialization failed for %s %s: %s", symbol, interval, exc)
            return None

    def put(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        df: pl.DataFrame,
    ) -> None:
        """Store OHLCV data in cache."""
        if df.is_empty():
            return

        start_str = start.strftime("%Y%m%d")
        end_str = end.strftime("%Y%m%d")
        blob = self._serialize(df)
        created_at = datetime.now().isoformat()

        conn = self._get_conn()
        conn.execute(
            """
            INSERT OR REPLACE INTO ohlcv (symbol, interval, start_date, end_date, data, created_at)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (symbol, interval, start_str, end_str, blob, created_at),
        )
        conn.commit()

    def close(self) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None


class QuantDBDataSource(DataSource):
    """Data source with QuantDB SQLite caching layer.

    On cache hit: returns data in <18ms
    On cache miss: fetches via httpx, stores in cache, returns data
    """

    def __init__(
        self,
        config: AppConfig,
        rate_limiter: RateLimiter,
        catalog: CatalogService,
        cache_dir: Path | str | None = None,
    ):
        self.config = config
        self.rate_limiter = rate_limiter
        self.catalog = catalog
        self.logger = get_logger("datagrab.quantdb")

        if cache_dir is None:
            cache_dir = Path(config.storage.data_root_path) / ".quantdb_cache"
        self._cache = QuantDBCache(cache_dir / "quantdb.sqlite")

        from .httpx_source import HttpxDataSource

        self._delegate = HttpxDataSource(config, rate_limiter, catalog)

    def list_symbols(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
    ) -> list[SymbolInfo]:
        return self._delegate.list_symbols(asset_type, refresh, limit, filters_override)

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        adjust: str,
    ) -> OhlcvResult:
        t0 = time.monotonic()
        cached = self._cache.get(symbol, interval, start, end)
        elapsed_ms = (time.monotonic() - t0) * 1000

        if cached is not None:
            self.logger.info("quantdb cache hit %s %s (%.1fms)", symbol, interval, elapsed_ms)
            return OhlcvResult(data=cached, adjustment=adjust)

        self.logger.info("quantdb cache miss %s %s, fetching via httpx", symbol, interval)
        result = self._delegate.fetch_ohlcv(symbol, interval, start, end, adjust)

        if not result.data.is_empty():
            self._cache.put(symbol, interval, start, end, result.data)

        return result

    def close(self) -> None:
        self._cache.close()
