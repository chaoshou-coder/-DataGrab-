"""Domain-specific exceptions for the tickterial pipeline."""

from __future__ import annotations


class TickterialError(Exception):
    """Base exception for all tickterial pipeline errors."""


class FetchError(TickterialError):
    """Tick data fetch failed (network / timeout / rate-limit)."""


class AggregationError(TickterialError):
    """OHLCV aggregation or validation failed."""


class RepairError(TickterialError):
    """Repair workflow failed."""


class BridgeError(TickterialError):
    """CSV-to-Parquet conversion failed."""
