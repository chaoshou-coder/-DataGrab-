"""Tests for asyncio.run() conflict fix — calling sync wrappers from within a running loop."""
from __future__ import annotations

import asyncio
from datetime import datetime

import pytest

from datagrab.tickterial.fetch_dukas import fetch_ticks
from datagrab.tickterial.exceptions import FetchError


class TestFetchTicksFromRunningLoop:
    """Test that fetch_ticks works when called from within an existing event loop."""

    def test_fetch_ticks_from_async_context_raises_fetch_error(self) -> None:
        """Verify fetch_ticks raises FetchError in running loop (not RuntimeError)."""
        async def _caller() -> None:
            with pytest.raises(FetchError, match="running asyncio loop"):
                fetch_ticks("XAUUSD", datetime(2024, 1, 1), datetime(2024, 1, 2))

        asyncio.run(_caller())

    def test_fetch_ticks_from_sync_context_raises_when_library_unavailable(self) -> None:
        """Verify fetch_ticks raises FetchError in sync context when dukascopy unavailable."""
        with pytest.raises(FetchError, match="not installed"):
            fetch_ticks("INVALID_SYMBOL", datetime(2024, 1, 1), datetime(2024, 1, 2))
