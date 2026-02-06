from datetime import datetime
from pathlib import Path

import pytest

pytest.importorskip("polars")
pytest.importorskip("pyarrow")

from datagrab.pipeline.writer import ParquetWriter


def test_parse_range_timezone(tmp_path):
    writer = ParquetWriter(tmp_path)
    path = Path("1d_20240101_20240131.parquet")
    result = writer._parse_range(path, "1d")
    assert result is not None
    assert result.start.tzinfo.key == "Asia/Shanghai"
    assert result.end.tzinfo.key == "Asia/Shanghai"


def test_find_existing_latest(tmp_path):
    writer = ParquetWriter(tmp_path)
    sym_dir = tmp_path / "stock" / "AAPL"
    sym_dir.mkdir(parents=True, exist_ok=True)
    (sym_dir / "1d_20240101_20240131.parquet").touch()
    (sym_dir / "1d_20240101_20240228.parquet").touch()
    (sym_dir / "1h_20240101_20240131.parquet").touch()

    existing = writer.find_existing("stock", "AAPL", "1d")
    assert existing is not None
    assert existing.end.date() == datetime(2024, 2, 28).date()
