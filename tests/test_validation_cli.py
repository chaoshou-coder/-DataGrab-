from argparse import Namespace

import pytest

from datagrab.validation import CliValidationError, validate_cli_args


def _ns(**overrides):
    base = Namespace(
        command="download",
        log_level="INFO",
        asset_type="stock",
        source="auto",
        format="jsonl",
        symbols=None,
        symbol=None,
        intervals="1d",
        start=None,
        end=None,
        adjust="auto",
        limit=None,
        workers=None,
        verbose=False,
        download_log_file=None,
        strict_failures_csv=False,
        failures_file=None,
        only_failures=False,
        config=None,
        data_root=None,
    )
    for key, value in overrides.items():
        setattr(base, key, value)
    return base


def test_symbol_path_rejects_parent_escape():
    with pytest.raises(CliValidationError):
        validate_cli_args(_ns(symbols="../bad"), asset_types=["stock", "ashare"])


def test_interval_invalid_rejected():
    with pytest.raises(CliValidationError):
        validate_cli_args(_ns(intervals="1x"), asset_types=["stock", "ashare"])


def test_start_after_end_rejected():
    with pytest.raises(CliValidationError):
        validate_cli_args(_ns(start="2026-01-10", end="2026-01-01"), asset_types=["stock", "ashare"])


def test_non_ashare_adjust_only_auto_none():
    with pytest.raises(CliValidationError):
        validate_cli_args(_ns(asset_type="stock", adjust="back"), asset_types=["stock", "ashare"])


def test_cli_args_exposes_source_field():
    model = validate_cli_args(_ns(source="tickterial"), asset_types=["stock", "ashare"])
    assert model.source == "tickterial"
