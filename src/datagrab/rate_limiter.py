"""Token Bucket + Sliding Window rate limiter for DataGrab."""

from __future__ import annotations

import asyncio
import math
import random
import time
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class RateLimitConfig:
    requests_per_second: float = 2.0
    burst_capacity: float = 1.0
    jitter_min: float = 0.2
    jitter_max: float = 0.6
    backoff_base: float = 1.5
    backoff_max: float = 30.0
    window_seconds: float = 1.0


class TokenBucket:
    """Token Bucket algorithm supporting burst capacity."""

    def __init__(self, rate: float, burst: float):
        self.rate = rate
        self.burst = burst
        self._tokens = burst
        self._last_refill = time.monotonic()
        self._lock = Lock()

    def _refill(self) -> None:
        now = time.monotonic()
        elapsed = now - self._last_refill
        self._tokens = min(self.burst, self._tokens + elapsed * self.rate)
        self._last_refill = now

    def consume(self, tokens: float = 1.0) -> float:
        """Attempt to consume tokens. Returns seconds to wait (0 if allowed)."""
        with self._lock:
            self._refill()
            if self._tokens >= tokens:
                self._tokens -= tokens
                return 0.0
            needed = tokens - self._tokens
            wait = needed / self.rate
            return wait


class SlidingWindowCounter:
    """Sliding Window algorithm for precise request counting."""

    def __init__(self, max_requests: float, window_seconds: float):
        self.max_requests = max_requests
        self.window_seconds = window_seconds
        self._window_start = time.monotonic()
        self._request_count = 0.0
        self._lock = Lock()

    def _expire(self, now: float) -> None:
        age = now - self._window_start
        if age >= self.window_seconds:
            self._window_start = now
            self._request_count = 0.0

    def can_request(self) -> bool:
        with self._lock:
            now = time.monotonic()
            self._expire(now)
            return self._request_count < self.max_requests

    def record_request(self) -> None:
        with self._lock:
            now = time.monotonic()
            self._expire(now)
            self._request_count += 1

    def wait_time(self) -> float:
        """Return seconds until a request would be allowed."""
        with self._lock:
            now = time.monotonic()
            self._expire(now)
            if self._request_count < self.max_requests:
                return 0.0
            # How far into the current window are we?
            elapsed = now - self._window_start
            return self.window_seconds - elapsed


class RateLimiter:
    """Combined Token Bucket + Sliding Window rate limiter.

    Token Bucket handles burst traffic while Sliding Window provides
    precise per-second accounting.
    """

    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._bucket = TokenBucket(config.requests_per_second, config.burst_capacity)
        self._window = SlidingWindowCounter(
            config.requests_per_second * config.window_seconds,
            config.window_seconds,
        )

    def wait(self) -> None:
        if self.config.requests_per_second <= 0:
            return

        # Determine max wait time from both algorithms
        bucket_wait = self._bucket.consume(1.0)

        # Also check sliding window
        while not self._window.can_request():
            window_wait = self._window.wait_time()
            if window_wait > 0:
                time.sleep(window_wait)

        self._window.record_request()

        total_wait = bucket_wait
        if self.config.jitter_max > 0:
            total_wait += random.uniform(self.config.jitter_min, self.config.jitter_max)

        if total_wait > 0:
            time.sleep(total_wait)

    def backoff(self, attempt: int) -> float:
        delay = self.config.backoff_base ** max(1, attempt)
        return min(delay, self.config.backoff_max)

    async def async_wait(self) -> None:
        """Async-compatible wait using asyncio.sleep instead of blocking time.sleep."""
        if self.config.requests_per_second <= 0:
            return

        bucket_wait = self._bucket.consume(1.0)

        while not self._window.can_request():
            window_wait = self._window.wait_time()
            if window_wait > 0:
                await asyncio.sleep(window_wait)

        self._window.record_request()

        total_wait = bucket_wait
        if self.config.jitter_max > 0:
            total_wait += random.uniform(self.config.jitter_min, self.config.jitter_max)

        if total_wait > 0:
            await asyncio.sleep(total_wait)
