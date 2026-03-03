from datetime import datetime

import pytest

from datagrab.validation.failures import ValidationFailureRecordError, validate_failure_rows


def test_failure_rows_invalid_date_warns_when_not_strict():
    rows = [
        {
            "symbol": "AAPL",
            "interval": "1d",
            "start": "not-a-date",
            "end": "2026-01-02",
            "asset_type": "stock",
            "adjust": "auto",
        }
    ]
    tasks, warnings = validate_failure_rows(rows, strict=False)
    assert len(tasks) == 1
    assert len(warnings) == 1
    assert "invalid start" in warnings[0]
    assert isinstance(tasks[0].start, datetime)


def test_failure_rows_invalid_date_raises_when_strict():
    rows = [
        {
            "symbol": "AAPL",
            "interval": "1d",
            "start": "not-a-date",
            "end": "2026-01-02",
            "asset_type": "stock",
            "adjust": "auto",
        }
    ]
    with pytest.raises(ValidationFailureRecordError):
        validate_failure_rows(rows, strict=True)
