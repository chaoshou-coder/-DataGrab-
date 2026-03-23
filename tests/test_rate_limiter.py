"""Tests for rate_limiter fixes discovered during eng review."""
from __future__ import annotations

import asyncio
import time

import pytest

from datagrab.rate_limiter import (
    RateLimiter,
    RateLimitConfig,
    SlidingWindowCounter,
    TokenBucket,
)


class TestSlidingWindowCheckAndWait:
    """Test the new check_and_wait atomic method."""

    def test_check_and_wait_allowed_when_under_limit(self) -> None:
        swc = SlidingWindowCounter(max_requests=5, window_seconds=1.0)
        allowed, wait = swc.check_and_wait()
        assert allowed is True
        assert wait == 0.0

    def test_check_and_wait_denied_when_at_limit(self) -> None:
        swc = SlidingWindowCounter(max_requests=2, window_seconds=2.0)
        swc.record_request()
        swc.record_request()
        allowed, wait = swc.check_and_wait()
        assert allowed is False
        assert wait > 0
        assert wait <= 2.0

    def test_record_after_wait_reclaims_slot(self) -> None:
        swc = SlidingWindowCounter(max_requests=1, window_seconds=0.1)
        swc.record_request()
        allowed, wait = swc.check_and_wait()
        assert allowed is False
        time.sleep(0.15)
        allowed, wait = swc.check_and_wait()
        assert allowed is True
        assert wait == 0.0


class TestRateLimiterWaitMaxLogic:
    """Test that wait() uses max(bucket_wait, window_wait) correctly."""

    def test_wait_uses_max_of_bucket_and_window(self) -> None:
        """When bucket allows immediately but window is full, should sleep window_wait not bucket_wait."""
        config = RateLimitConfig(
            requests_per_second=10.0,
            burst_capacity=10.0,
            window_seconds=1.0,
            jitter_min=0.0,
            jitter_max=0.0,
        )
        limiter = RateLimiter(config)

        bucket_wait_1 = limiter._bucket.consume(1.0)
        assert bucket_wait_1 == 0.0

        allowed, window_wait = limiter._window.check_and_wait()
        assert allowed is True

        limiter._window.record_request()

        for _ in range(9):
            limiter._window.record_request()

        allowed, window_wait = limiter._window.check_and_wait()
        assert allowed is False
        assert window_wait > 0

        t0 = time.monotonic()
        limiter.wait()
        elapsed = time.monotonic() - t0

        assert window_wait <= elapsed < window_wait + 0.05


class TestRateLimiterAsyncWaitWithCheckAndWait:
    """Test async_wait uses check_and_wait correctly."""

    @pytest.mark.asyncio
    async def test_async_wait_uses_check_and_wait(self) -> None:
        config = RateLimitConfig(
            requests_per_second=5.0,
            burst_capacity=5.0,
            window_seconds=0.5,
            jitter_min=0.0,
            jitter_max=0.0,
        )
        limiter = RateLimiter(config)

        for _ in range(5):
            limiter._window.record_request()

        t0 = time.monotonic()
        await limiter.async_wait()
        elapsed = time.monotonic() - t0

        allowed, window_wait = limiter._window.check_and_wait()
        assert allowed is True
        assert window_wait == 0.0
