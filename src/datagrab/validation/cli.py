from __future__ import annotations

from argparse import Namespace
import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ValidationError, field_validator, ValidationInfo

from ..timeutils import parse_date


LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
ADJUST_MODES = {"auto", "back", "forward", "none", "backward", "front"}


class CliValidationError(ValueError):
    """包装 CLI 验证错误。"""


def _normalize_interval(value: str) -> str:
    value = value.strip().lower()
    if not value:
        raise ValueError("interval is empty")
    # 支持常见时间粒度格式，放宽校验以兼容现有输入
    if re.fullmatch(r"\d+(d|wk|w|mo|m|h|s)?", value):
        return value
    raise ValueError(f"invalid interval format: {value}")


def _safe_datetime(value: str | None) -> datetime | None:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        return value
    return parse_date(value)


class CliArgsModel(BaseModel):
    """命令行参数的统一解析模型（语义级校验）。"""

    command: str
    log_level: str = "INFO"
    config: str | None = None
    asset_type: str | None = None
    limit: int | None = Field(default=None, ge=1)
    workers: int | None = Field(default=None, ge=1, le=256)
    adjust: str | None = None
    intervals: list[str] = Field(default_factory=list)
    start: datetime | None = None
    end: datetime | None = None
    strict_failures_csv: bool = False
    failures_file: str | None = None
    only_failures: bool = False

    model_config = {
        "extra": "ignore",
    }

    @field_validator("log_level")
    @classmethod
    def _log_level(cls, value: str) -> str:
        level = (value or "").strip().upper()
        if level not in LOG_LEVELS:
            raise ValueError(f"unsupported log_level: {value}")
        return level

    @field_validator("asset_type")
    @classmethod
    def _asset_type(cls, value: str | None, info: ValidationInfo) -> str | None:
        if value is None:
            return None
        asset_types = (info.context or {}).get("asset_types")
        if asset_types and value not in asset_types:
            raise ValueError(f"unsupported asset_type: {value}")
        return value

    @field_validator("adjust")
    @classmethod
    def _adjust(cls, value: str | None) -> str | None:
        if value is None:
            return None
        normalized = value.strip().lower()
        if normalized not in ADJUST_MODES:
            raise ValueError(f"unsupported adjust: {value}")
        return normalized

    @field_validator("start", "end", mode="before")
    @classmethod
    def _parse_datetime(cls, value: Any) -> datetime | None:
        return _safe_datetime(value)

    @field_validator("intervals", mode="before")
    @classmethod
    def _split_intervals(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            items = value
        else:
            items = [item.strip() for item in str(value).split(",") if str(item).strip()]
        return [_normalize_interval(item) for item in items]


def validate_cli_args(args: Namespace, *, asset_types: list[str]) -> CliArgsModel:
    """Validate argparse args and return typed model."""

    try:
        return CliArgsModel.model_validate(vars(args), context={"asset_types": asset_types})
    except ValidationError as exc:
        raise CliValidationError(str(exc)) from exc


def render_cli_error(error: CliValidationError) -> str:
    return str(error)
