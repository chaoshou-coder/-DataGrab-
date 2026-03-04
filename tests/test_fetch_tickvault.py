from datetime import datetime, timedelta

from datagrab.tickterial import fetch_tickvault
import pandas as pd
import pytest
from datagrab.tickterial.exceptions import FetchError


def test_fetch_tickvault_converts_raw_ask_bid_to_price_volume(monkeypatch: pytest.MonkeyPatch) -> None:
    start = datetime(2024, 1, 1)
    end = datetime(2024, 1, 1, 0, 2)

    async def fake_download_range(*_args, **_kwargs):
        return None

    def fake_read_tick_data(*_args, **_kwargs) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "time": [start, start + timedelta(minutes=1)],
                "ask": [1.2, 1.4],
                "bid": [1.0, 1.0],
                "ask_volume": [4, 6],
                "bid_volume": [1, 2],
            }
        )

    monkeypatch.setattr(fetch_tickvault, "download_range", fake_download_range)
    monkeypatch.setattr(fetch_tickvault, "read_tick_data", fake_read_tick_data)
    monkeypatch.setattr(fetch_tickvault, "TICKVAULT_AVAILABLE", True)

    out = fetch_tickvault.fetch_ticks("XAUUSD", start, end, base_dir=".", workers=4)
    assert list(out.columns) == ["datetime", "price", "volume"]
    assert len(out) == 2
    assert out["price"].tolist() == [1.1, 1.2]
    assert out["volume"].tolist() == [5.0, 8.0]


def test_fetch_tickvault_rejects_missing_dependency(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setattr(fetch_tickvault, "TICKVAULT_AVAILABLE", False)
    with pytest.raises(FetchError):
        fetch_tickvault.fetch_ticks(
            "XAUUSD",
            datetime(2024, 1, 1),
            datetime(2024, 1, 1, 0, 2),
            base_dir=".",
            workers=4,
        )
