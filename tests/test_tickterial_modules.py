from __future__ import annotations

from argparse import Namespace
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd
import pytest

from datagrab.sources.tickterial_source import TickterialDataSource
from datagrab.config import AppConfig
from datagrab.tickterial import check as checker
from datagrab.tickterial.common import build_daily_bars_ny_close, parse_intervals


def test_parse_intervals_normalizes_and_validates() -> None:
    assert parse_intervals("1,5m") == ["1d", "5m"]
    assert parse_intervals(" 1M , 15MIN ") == ["1m", "15m"]
    assert parse_intervals("") == ["1m", "5m", "15m", "1d"]
    with pytest.raises(ValueError, match="invalid intervals"):
        parse_intervals("2h")


def test_source_build_1m_bars_aligns_expected_grid() -> None:
    source = TickterialDataSource.__new__(TickterialDataSource)
    source.config = AppConfig()

    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 0, 4, tzinfo=timezone.utc)
    ticks = pd.DataFrame(
        {
            "datetime": [
                datetime(2024, 1, 1, 0, 0, 5),
                datetime(2024, 1, 1, 0, 0, 30),
                datetime(2024, 1, 1, 0, 1, 0),
            ],
            "price": [1.0, 1.5, 2.0],
            "volume": [10.0, 20.0, 30.0],
        }
    )
    bars = source._build_1m_bars(ticks, start, end)
    assert list(bars.columns) == ["datetime", "open", "high", "low", "close", "volume"]
    assert len(bars) == 4
    assert bars.loc[0, "open"] == 1.0
    assert bars.loc[0, "high"] == 1.5
    assert bars.loc[0, "low"] == 1.0
    assert bars.loc[0, "close"] == 1.5
    assert bars.loc[0, "volume"] == 30.0
    assert pd.isna(bars.loc[3, "open"])  # NaN row preserved


def test_build_daily_bars_ny_close_splits_sessions_by_17_clock() -> None:
    ny_before_close = pd.Timestamp("2024-03-08 16:59", tz="America/New_York").tz_convert("UTC").to_pydatetime()
    ny_after_close = pd.Timestamp("2024-03-08 17:01", tz="America/New_York").tz_convert("UTC").to_pydatetime()

    base_1m = pd.DataFrame(
        {
            "datetime": [ny_before_close, ny_after_close],
            "open": [1.0, 1.5],
            "high": [1.1, 1.6],
            "low": [0.9, 1.4],
            "close": [1.0, 1.5],
            "volume": [10.0, 20.0],
        }
    )

    daily = build_daily_bars_ny_close(base_1m)
    assert len(daily) == 2
    assert daily["datetime"].dtype == "datetime64[ns]"
    assert daily["datetime"].iloc[0] < daily["datetime"].iloc[1]


def test_check_file_integrity_pass_and_parse_path(tmp_path: Path) -> None:
    window = checker.Window(start=datetime(2024, 1, 1, 0, 0), end=datetime(2024, 1, 1, 0, 5))
    output = tmp_path / "XAUUSD_1m_20240101_20240101.csv"
    dt = pd.date_range("2024-01-01T00:00:00", periods=5, freq="1min")
    pd.DataFrame(
        {
            "datetime": dt,
            "open": [1, 1, 1, 1, 1],
            "high": [1, 1, 1, 1, 1],
            "low": [1, 1, 1, 1, 1],
            "close": [1, 1, 1, 1, 1],
            "volume": [1, 1, 1, 1, 1],
        }
    ).to_csv(output, index=False)

    result = checker.check_file("XAUUSD", "1m", window, tmp_path, 0.8)
    assert result.status == "pass"
    assert result.metrics["rows"] == 5


def test_check_file_integrity_detects_ohlc_relation_issue(tmp_path: Path) -> None:
    window = checker.Window(start=datetime(2024, 1, 1, 0, 0), end=datetime(2024, 1, 1, 0, 5))
    output = tmp_path / "XAUUSD_1m_20240101_20240101.csv"
    dt = pd.date_range("2024-01-01T00:00:00", periods=5, freq="1min")
    pd.DataFrame(
        {
            "datetime": dt,
            "open": [1, 1, 1, 1, 1],
            "high": [1, 1, 1, 1, 1],
            "low": [2, 1, 1, 1, 1],
            "close": [1, 1, 1, 1, 1],
            "volume": [1, 1, 1, 1, 1],
        }
    ).to_csv(output, index=False)

    result = checker.check_file("XAUUSD", "1m", window, tmp_path, 0.8)
    assert result.status == "fail"
    assert any(r.startswith("ohlc_relation_invalid") for r in result.fails)


def test_check_file_integrity_catches_missing_columns(tmp_path: Path) -> None:
    window = checker.Window(start=datetime(2024, 1, 1, 0, 0), end=datetime(2024, 1, 1, 0, 5))
    output = tmp_path / "XAUUSD_1m_20240101_20240101.csv"
    pd.DataFrame(
        {
            "datetime": ["2024-01-01T00:00:00"],
            "open": [1],
            "high": [1],
            "low": [1],
            "close": [1],
        }
    ).to_csv(output, index=False)

    result = checker.check_file("XAUUSD", "1m", window, tmp_path, 0.8)
    assert result.status == "fail"
    assert result.fails and any(item.startswith("missing_columns:volume") for item in result.fails)


def test_fetch_to_float_edge_cases() -> None:
    from datagrab.tickterial.fetch import to_float

    assert to_float(None) is None
    assert to_float("abc") is None
    assert to_float(float("inf")) is None
    assert to_float(float("nan")) is None
    assert to_float("1.5") == 1.5
    assert to_float(0) == 0.0


def test_fetch_extract_price_prefers_last() -> None:
    from datagrab.tickterial.fetch import extract_price

    assert extract_price({"last": 1.0, "ask": 2.0, "bid": 3.0}) == 1.0
    assert extract_price({"ask": 2.0, "bid": 4.0}) == 3.0
    assert extract_price({}) is None


def test_fetch_extract_volume() -> None:
    from datagrab.tickterial.fetch import extract_volume

    assert extract_volume({"ask-vol": 10.0, "bid-vol": 20.0}) == 30.0
    assert extract_volume({}) is None


def test_aggregate_build_1m_bars_empty_ticks() -> None:
    from datagrab.tickterial.aggregate import build_1m_bars

    ticks = pd.DataFrame(columns=["datetime", "price", "volume"])
    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 0, 5)
    bars = build_1m_bars(ticks, start, end)
    assert list(bars.columns) == ["datetime", "open", "high", "low", "close", "volume"]
    assert len(bars) == 5


def test_aggregate_build_multi_interval_bars_5m() -> None:
    from datagrab.tickterial.aggregate import build_multi_interval_bars

    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 0, 10)
    bars_1m = pd.DataFrame(
        {
            "datetime": pd.date_range(start, periods=10, freq="1min"),
            "open": [1.0] * 10,
            "high": [1.1] * 10,
            "low": [0.9] * 10,
            "close": [1.0] * 10,
            "volume": [100.0] * 10,
        }
    )
    result = build_multi_interval_bars(bars_1m, 5, start, end)
    assert len(result) == 2


def test_aggregate_check_interval_integrity_passes_valid_data() -> None:
    from datagrab.tickterial.aggregate import check_interval_integrity

    start = datetime(2024, 1, 1, 0, 0)
    end = datetime(2024, 1, 1, 0, 5)
    df = pd.DataFrame(
        {
            "datetime": pd.date_range(start, periods=5, freq="1min"),
            "open": [1.0] * 5,
            "high": [1.1] * 5,
            "low": [0.9] * 5,
            "close": [1.0] * 5,
            "volume": [100.0] * 5,
        }
    )
    result = check_interval_integrity("1m", df, start, end)
    assert result == []


def test_runner_failed_window_dataclass() -> None:
    from datagrab.tickterial.runner import FailedWindow

    fw = FailedWindow(
        symbol="XAUUSD",
        start=datetime(2024, 1, 1),
        end=datetime(2025, 1, 1),
        reason="test error",
    )
    assert fw.symbol == "XAUUSD"
    assert fw.start == datetime(2024, 1, 1)
    assert fw.end == datetime(2025, 1, 1)
    assert fw.reason == "test error"


def test_runner_run_writes_expected_columns_without_keyerror(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from datagrab.tickterial import runner

    monkeypatch.setattr(runner, "Tickloader", object())
    monkeypatch.setattr(runner, "configure_logging", lambda *_args, **_kwargs: None)

    ticks = pd.DataFrame(
        {
            "datetime": [datetime(2024, 1, 1, 0, 0), datetime(2024, 1, 1, 0, 1)],
            "price": [1.0, 1.1],
            "volume": [10.0, 20.0],
        }
    )
    bars_1m = pd.DataFrame(
        {
            "datetime": pd.date_range("2024-01-01T00:00:00", periods=2, freq="1min"),
            "open": [1.0, 1.1],
            "high": [1.1, 1.2],
            "low": [0.9, 1.0],
            "close": [1.05, 1.15],
            "volume": [10.0, 20.0],
            "extra": [1, 2],
        }
    )
    monkeypatch.setattr(runner, "fetch_ticks", lambda *_args, **_kwargs: ticks)
    monkeypatch.setattr(runner, "build_1m_bars", lambda *_args, **_kwargs: bars_1m)

    args = Namespace(
        symbols="XAGUSD",
        start="2024-01-01T00:00:00",
        end="2024-01-01T00:02:00",
        output=str(tmp_path),
        cache_dir=str(tmp_path / ".tick-data"),
        intervals="1m",
        max_retries=0,
        retry_delay=0.1,
        download_workers=1,
        batch_size=1,
        batch_pause_ms=0,
        retry_jitter_ms=0,
        source_timestamp_shift_hours=0.0,
        resume_failures="",
        validate=False,
        strict_validate=False,
        window_retries=0,
        log_level="ERROR",
        suppress_tickloader_info=True,
        force=True,
    )
    assert runner.run(args) == 0
    output_files = list(tmp_path.glob("XAGUSD_1m_*.csv"))
    assert len(output_files) == 1
    written = pd.read_csv(output_files[0])
    assert list(written.columns) == ["datetime", "open", "high", "low", "close", "volume"]


def test_bridge_parse_window_valid(tmp_path: Path) -> None:
    from datagrab.tickterial.bridge import parse_window

    f = tmp_path / "XAUUSD_1m_20240101_20240102.csv"
    f.touch()
    result = parse_window(f)
    assert result is not None
    symbol, interval, start, end = result
    assert symbol == "XAUUSD"
    assert interval == "1m"


def test_bridge_parse_window_invalid(tmp_path: Path) -> None:
    from datagrab.tickterial.bridge import parse_window

    f = tmp_path / "badname.csv"
    f.touch()
    assert parse_window(f) is None


def test_repair_expected_rows() -> None:
    from datagrab.tickterial.repair import Window, expected_rows

    window = Window(start=datetime(2024, 1, 1), end=datetime(2025, 1, 1))
    assert expected_rows(window, "1m") == 527040
    assert expected_rows(window, "1d") is None


def test_exceptions_hierarchy() -> None:
    from datagrab.tickterial.exceptions import FetchError, TickterialError

    assert issubclass(FetchError, TickterialError)
    assert issubclass(FetchError, Exception)