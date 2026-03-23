"""Performance benchmarks for DataGrab optimization verification.

These benchmarks establish baseline metrics for the performance optimization work.
Run with: pytest tests/benchmark_performance.py --benchmark-only
Install: pip install pytest-benchmark
"""
from __future__ import annotations

import time
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import polars as pl
import pytest

from datagrab.rate_limiter import RateLimiter, RateLimitConfig
from datagrab.tickterial.aggregate import build_1m_bars, build_multi_interval_bars


class TestRateLimiterBenchmark:
    """Benchmark Token Bucket + Sliding Window rate limiter."""

    def test_async_wait_single_request(self, benchmark):
        config = RateLimitConfig(
            requests_per_second=10.0,
            burst_capacity=10.0,
            window_seconds=1.0,
            jitter_min=0.0,
            jitter_max=0.0,
        )
        limiter = RateLimiter(config)

        async def _run():
            for _ in range(10):
                await limiter.async_wait()

        import asyncio
        result = asyncio.run(_run())
        benchmark(result)

    def test_sync_wait_single_request(self, benchmark):
        config = RateLimitConfig(
            requests_per_second=10.0,
            burst_capacity=10.0,
            window_seconds=1.0,
            jitter_min=0.0,
            jitter_max=0.0,
        )
        limiter = RateLimiter(config)

        for _ in range(10):
            limiter.wait()


class TestAggregationBenchmark:
    """Benchmark OHLCV aggregation functions."""

    @pytest.fixture
    def sample_ticks(self) -> pd.DataFrame:
        np = pytest.importorskip("numpy")
        n = 10000
        timestamps = pd.date_range("2024-01-01", periods=n, freq="1min")
        return pd.DataFrame({
            "datetime": timestamps,
            "price": 100.0 + np.random.randn(n).cumsum(),
            "volume": (np.random.rand(n) * 1000).astype(int),
        })

    def test_build_1m_bars(self, benchmark, sample_ticks):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        result = benchmark(build_1m_bars, sample_ticks, start, end)
        assert not result.empty

    def test_build_multi_interval_bars(self, benchmark, sample_ticks):
        start = datetime(2024, 1, 1, tzinfo=timezone.utc)
        end = datetime(2024, 1, 8, tzinfo=timezone.utc)
        import pandas as pd
        one_minute_bars = build_1m_bars(sample_ticks, start, end)
        result = benchmark(build_multi_interval_bars, one_minute_bars, 5, start, end)
        assert not result.empty


class TestSlidingWindowCounterBenchmark:
    """Benchmark SlidingWindowCounter check_and_wait atomic method."""

    def test_check_and_wait_allowed(self, benchmark):
        from datagrab.rate_limiter import SlidingWindowCounter
        swc = SlidingWindowCounter(max_requests=100.0, window_seconds=1.0)
        for _ in range(50):
            swc.record_request()

        def _run():
            for _ in range(1000):
                allowed, wait = swc.check_and_wait()
                if not allowed:
                    swc.record_request()

        benchmark(_run)

    def test_can_request_vs_check_and_wait(self, benchmark):
        """Compare old can_request + wait_time vs new check_and_wait."""
        from datagrab.rate_limiter import SlidingWindowCounter
        swc = SlidingWindowCounter(max_requests=100.0, window_seconds=1.0)
        for _ in range(50):
            swc.record_request()

        def _run_old():
            for _ in range(1000):
                while not swc.can_request():
                    wt = swc.wait_time()
                swc.record_request()

        def _run_new():
            for _ in range(1000):
                allowed, wait = swc.check_and_wait()
                if not allowed:
                    swc.record_request()

        benchmark(_run_new)
