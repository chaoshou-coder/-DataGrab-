"""Tests for QuantDB yfinance fallback when httpx source fails."""
from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock, patch

import polars as pl
import pytest

from datagrab.sources.quantdb_source import QuantDBDataSource
from datagrab.sources.base import OhlcvResult
from datagrab.config import AppConfig
from datagrab.rate_limiter import RateLimiter, RateLimitConfig
from datagrab.pipeline.catalog import CatalogService


class TestQuantDBYFinanceFallback:
    """Test that QuantDB falls back to yfinance when httpx fails."""

    def test_does_not_fallback_when_httpx_succeeds(self, tmp_path: pytest.TempPathFactory) -> None:
        """Verify httpx success path does not trigger fallback."""
        config = AppConfig()
        rate_limiter = RateLimiter(RateLimitConfig(requests_per_second=0))
        catalog = MagicMock(spec=CatalogService)
        cache_dir = tmp_path / ".quantdb_cache"
        ds = QuantDBDataSource(config, rate_limiter, catalog, cache_dir=cache_dir)

        df = pl.DataFrame({"datetime": [], "open": [], "high": [], "low": [], "close": [], "volume": []})

        with patch.object(
            ds._delegate, "fetch_ohlcv", return_value=OhlcvResult(data=df, adjustment="none")
        ) as mock_httpx:
            result = ds.fetch_ohlcv(
                "AAPL", "1d", datetime(2024, 1, 1), datetime(2024, 1, 2), "none"
            )
            mock_httpx.assert_called_once()
            assert result.data.shape == (0, 6)
