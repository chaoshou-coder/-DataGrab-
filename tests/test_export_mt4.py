from __future__ import annotations

import csv

import polars as pl

from datagrab.storage.export import export_mt4_batch, export_mt4_csv


def test_export_mt4_csv_format(tmp_path):
    df = pl.DataFrame(
        {
            "datetime": ["2024-01-01 00:00:00", "2024-01-01 00:01:00"],
            "open": [100.12, 100.22],
            "high": [100.5, 100.6],
            "low": [99.9, 100.1],
            "close": [100.3, 100.4],
            "volume": [12.0, 12.9],
        }
    )

    output = tmp_path / "XAGUSD_M1.csv"
    export_mt4_csv(df, output)

    rows = list(csv.reader(output.read_text(encoding="utf-8").splitlines()))
    assert len(rows) == 2
    assert rows[0] == ["2024.01.01", "00:00", "100.12", "100.5", "99.9", "100.3", "12"]
    assert rows[1] == ["2024.01.01", "00:01", "100.22", "100.6", "100.1", "100.4", "13"]


def test_export_mt4_batch_merges_windows(tmp_path):
    input_dir = tmp_path / "in"
    output_dir = tmp_path / "out"
    input_dir.mkdir()

    (input_dir / "XAGUSD_1m_20240101_20240102.csv").write_text(
        "datetime,open,high,low,close,volume\n"
        "2024-01-01 00:00:00,100,101,99,100,10\n"
        "2024-01-01 00:01:00,100,102,99.5,101,11\n",
        encoding="utf-8",
    )
    (input_dir / "XAGUSD_1m_20240102_20240103.csv").write_text(
        "datetime,open,high,low,close,volume\n"
        "2024-01-01 00:01:00,200,202,199.8,201,12\n"
        "2024-01-01 00:02:00,200.5,203,200.1,202,13\n",
        encoding="utf-8",
    )
    (input_dir / "XAGUSD_5m_20240101_20240102.csv").write_text(
        "datetime,open,high,low,close,volume\n"
        "2024-01-01 00:00:00,300,301,299,300,7\n",
        encoding="utf-8",
    )
    (input_dir / "INVALID_1m_20240101_20240102.txt").write_text("bad", encoding="utf-8")

    outputs = export_mt4_batch(input_dir, output_dir, symbol_filter="XAGUSD", interval_filter="1m")
    assert len(outputs) == 1
    out_path = outputs[0]
    assert out_path.name == "XAGUSD_M1.csv"
    assert out_path.exists()

    rows = list(csv.reader(out_path.read_text(encoding="utf-8").splitlines()))
    assert rows == [
        ["2024.01.01", "00:00", "100.0", "101.0", "99.0", "100.0", "10"],
        ["2024.01.01", "00:01", "200.0", "202.0", "199.8", "201.0", "12"],
        ["2024.01.01", "00:02", "200.5", "203.0", "200.1", "202.0", "13"],
    ]


def test_export_mt4_batch_scans_nested_directories(tmp_path):
    input_dir = tmp_path / "in"
    output_dir = tmp_path / "out"
    nested_symbol_dir = input_dir / "XAUUSD"
    nested_symbol_dir.mkdir(parents=True)

    (nested_symbol_dir / "XAUUSD_5m_20240101_20240102.csv").write_text(
        "datetime,open,high,low,close,volume\n"
        "2024-01-01 00:00:00,101,102,100,101,10\n",
        encoding="utf-8",
    )

    outputs = export_mt4_batch(input_dir, output_dir, symbol_filter="XAUUSD")
    assert len(outputs) == 1
    out_path = outputs[0]
    assert out_path.name == "XAUUSD_M5.csv"
    assert out_path.exists()

    rows = list(csv.reader(out_path.read_text(encoding="utf-8").splitlines()))
    assert rows == [["2024.01.01", "00:00", "101.0", "102.0", "100.0", "101.0", "10"]]
