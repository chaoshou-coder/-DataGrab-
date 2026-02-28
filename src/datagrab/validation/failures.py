from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Iterable

from pydantic import BaseModel, ConfigDict, ValidationError, field_validator

from ..timeutils import beijing_now, parse_date


class ValidationFailureRecordError(ValueError):
    """解析 failures.csv 行记录时的错误。"""


class FailureRecordModel(BaseModel):
    """failures.csv 的标准化行模型。"""

    version: str = "1"
    symbol: str
    interval: str
    start: str | None = None
    end: str | None = None
    asset_type: str = "stock"
    adjust: str = "auto"
    reason: str | None = None
    created_at: str | None = None

    model_config = ConfigDict(extra="ignore")

    @field_validator("symbol", "interval")
    @classmethod
    def _not_empty(cls, value: str) -> str:
        value = (value or "").strip()
        if not value:
            raise ValueError("required")
        return value

    @field_validator("start", "end", mode="before")
    @classmethod
    def _normalize_datetime(cls, value: str | None) -> str | None:
        if value is None or value == "":
            return None
        return value.strip()

    @field_validator("adjust")
    @classmethod
    def _normalize_adjust(cls, value: str) -> str:
        normalized = (value or "auto").strip().lower()
        if normalized not in {"auto", "back", "forward", "none", "front", "backward"}:
            raise ValueError("invalid adjust")
        return normalized


@dataclass(frozen=True)
class ValidatedFailureTask:
    symbol: str
    interval: str
    start: datetime
    end: datetime
    asset_type: str
    adjust: str


def _failure_default_interval(now: datetime) -> tuple[datetime, datetime]:
    return now - timedelta(days=365), now


def validate_failure_rows(rows: Iterable[dict[str, str]], strict: bool) -> tuple[list[ValidatedFailureTask], list[str]]:
    failures: list[ValidatedFailureTask] = []
    warnings: list[str] = []

    now = beijing_now()
    default_start, default_end = _failure_default_interval(now)

    for index, row in enumerate(rows, start=2):
        try:
            record = FailureRecordModel.model_validate(row)
        except ValidationError as exc:
            message = f"failures row {index} invalid: {exc}"
            if strict:
                raise ValidationFailureRecordError(message) from exc
            warnings.append(message)
            continue

        task_start = default_start
        task_end = default_end
        if record.start:
            task_start = _parse_failure_datetime(
                record.start,
                index,
                "start",
                default_start,
                warnings,
                strict,
            )
        if record.end:
            task_end = _parse_failure_datetime(
                record.end,
                index,
                "end",
                default_end,
                warnings,
                strict,
            )

        if task_start > task_end:
            warning = f"failures row {index}: start > end, will normalize to default range"
            if strict:
                raise ValidationFailureRecordError(warning)
            warnings.append(warning)
            task_start = default_start
            task_end = default_end

        failures.append(
            ValidatedFailureTask(
                symbol=record.symbol,
                interval=record.interval,
                start=task_start,
                end=task_end,
                asset_type=record.asset_type or "stock",
                adjust=record.adjust or "auto",
            )
        )
    return failures, warnings


def _parse_failure_datetime(
    raw: str,
    row_no: int,
    field: str,
    fallback: datetime,
    warnings: list[str],
    strict: bool,
) -> datetime:
    try:
        return parse_date(raw)
    except ValueError as exc:
        message = f"failures row {row_no}: invalid {field}={raw!r}, fallback={fallback.isoformat()}"
        if strict:
            raise ValidationFailureRecordError(message) from exc
        warnings.append(message)
        return fallback


def write_failures_rows(path: Path, rows: list[dict[str, Any]]) -> None:
    from csv import DictWriter

    fieldnames = ["version", "symbol", "interval", "start", "end", "asset_type", "adjust", "reason", "created_at"]
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        now = beijing_now().isoformat()
        for row in rows:
            out = dict(row)
            out.setdefault("version", "1")
            out.setdefault("created_at", now)
            writer.writerow(out)
