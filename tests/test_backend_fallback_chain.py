"""Tests for dukas->tickvault->tickterial backend fallback chain."""
from __future__ import annotations

from argparse import Namespace
from datetime import datetime

import pandas as pd
import pytest

from datagrab.tickterial import runner
from datagrab.tickterial import fetch_dukas
from datagrab.tickterial import fetch_tickvault
from datagrab.tickterial.exceptions import FetchError


def test_backend_dukas_falls_back_to_tickvault(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify auto mode falls back from dukas to tickvault when dukas fails."""
    args = Namespace(
        backend="auto",
        cache_dir=".",
        tickvault_workers=5,
        tickvault_base_dir="",
        download_workers=3,
    )
    called = {"dukas": 0, "tickvault": 0}

    def fake_dukas_fetch_ticks(symbol, win_start, win_end):
        called["dukas"] += 1
        raise FetchError("dukascopy error")

    def fake_tickvault_fetch_ticks(*args, **kwargs):
        called["tickvault"] += 1
        return pd.DataFrame(
            {
                "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 1)],
                "price": [1.0, 1.1],
                "volume": [10.0, 20.0],
            }
        )

    monkeypatch.setattr(fetch_dukas, "DUKAS_AVAILABLE", True)
    monkeypatch.setattr(fetch_dukas, "fetch_ticks", fake_dukas_fetch_ticks)
    monkeypatch.setattr(fetch_tickvault, "TICKVAULT_AVAILABLE", True)
    monkeypatch.setattr(fetch_tickvault, "fetch_ticks", fake_tickvault_fetch_ticks)

    df = runner._load_ticks_for_window(
        symbol="XAUUSD",
        win_start=datetime(2024, 1, 1),
        win_end=datetime(2024, 1, 1, 0, 2),
        args=args,
        max_retries=1,
        retry_delay=0.1,
        batch_size=1,
        batch_pause_ms=0,
        retry_jitter_ms=0,
        cache_dir=".",
        source_timestamp_shift_hours=0.0,
    )
    assert called["dukas"] == 1
    assert called["tickvault"] == 1
    assert list(df.columns) == ["datetime", "price", "volume"]


def test_backend_auto_full_chain_dukas_tickvault_legacy(monkeypatch: pytest.MonkeyPatch) -> None:
    """Verify auto mode falls back dukas -> tickvault -> legacy tickterial when both higher priority fail."""
    args = Namespace(
        backend="auto",
        cache_dir=".",
        tickvault_workers=5,
        tickvault_base_dir="",
        download_workers=3,
    )
    called = {"dukas": 0, "tickvault": 0, "legacy": 0}

    def fake_dukas_fetch_ticks(symbol, win_start, win_end):
        called["dukas"] += 1
        raise FetchError("dukascopy error")

    def fake_tickvault_fetch_ticks(*args, **kwargs):
        called["tickvault"] += 1
        raise FetchError("tickvault error")

    def fake_legacy_fetch_ticks(*args, **kwargs):
        called["legacy"] += 1
        return pd.DataFrame(
            {
                "datetime": [datetime(2024, 1, 1, 0, 0)],
                "price": [1.0],
                "volume": [10.0],
            }
        )

    monkeypatch.setattr(fetch_dukas, "DUKAS_AVAILABLE", True)
    monkeypatch.setattr(fetch_dukas, "fetch_ticks", fake_dukas_fetch_ticks)
    monkeypatch.setattr(fetch_tickvault, "TICKVAULT_AVAILABLE", True)
    monkeypatch.setattr(fetch_tickvault, "fetch_ticks", fake_tickvault_fetch_ticks)
    monkeypatch.setattr(runner, "fetch_ticks", fake_legacy_fetch_ticks)

    df = runner._load_ticks_for_window(
        symbol="XAUUSD",
        win_start=datetime(2024, 1, 1),
        win_end=datetime(2024, 1, 1, 0, 1),
        args=args,
        max_retries=1,
        retry_delay=0.1,
        batch_size=1,
        batch_pause_ms=0,
        retry_jitter_ms=0,
        cache_dir=".",
        source_timestamp_shift_hours=0.0,
    )
    assert called["dukas"] == 1
    assert called["tickvault"] == 1
    assert called["legacy"] == 1
