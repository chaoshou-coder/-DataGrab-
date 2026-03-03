from datetime import datetime
from pathlib import Path

import pytest
from datagrab.pipeline.writer import ParquetWriter


def test_symbol_path_safety_rejects_traversal(tmp_path: Path):
    writer = ParquetWriter(tmp_path)
    now = datetime(2026, 1, 1)
    with pytest.raises(ValueError, match="unsafe symbol"):
        writer.build_path("stock", "../bad", "1d", now, now)


def test_interval_delta_rejects_unsupported(tmp_path: Path):
    writer = ParquetWriter(tmp_path)
    with pytest.raises(ValueError, match="unsupported interval"):
        writer._interval_delta("bad")


def test_ensure_within_data_root_blocks_escape(tmp_path: Path):
    writer = ParquetWriter(tmp_path)
    with pytest.raises(ValueError, match="escapes data_root"):
        writer._ensure_within_data_root(tmp_path / ".." / "secret")
