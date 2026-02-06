from __future__ import annotations

import random
import time
from dataclasses import dataclass
from threading import Lock


@dataclass
class RateLimitConfig:
    requests_per_second: float = 2.0
    jitter_min: float = 0.2
    jitter_max: float = 0.6
    backoff_base: float = 1.5
    backoff_max: float = 30.0


class RateLimiter:
    def __init__(self, config: RateLimitConfig):
        self.config = config
        self._lock = Lock()
        self._last_ts = 0.0

    def wait(self) -> None:
        min_interval = 0.0
        if self.config.requests_per_second > 0:
            min_interval = 1.0 / self.config.requests_per_second
        with self._lock:
            now = time.monotonic()
            next_allowed = self._last_ts + min_interval
            sleep_for = max(0.0, next_allowed - now)
            if self.config.jitter_max > 0:
                sleep_for += random.uniform(self.config.jitter_min, self.config.jitter_max)
            self._last_ts = now + sleep_for
        if sleep_for > 0:
            time.sleep(sleep_for)

    def backoff(self, attempt: int) -> float:
        delay = self.config.backoff_base ** max(1, attempt)
        return min(delay, self.config.backoff_max)
