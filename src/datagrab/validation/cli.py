from __future__ import annotations

from argparse import Namespace
import re
from datetime import datetime
from typing import Any

from pydantic import BaseModel, Field, ValidationError, ValidationInfo, field_validator, model_validator

from ..timeutils import parse_date


LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}
ADJUST_MODES = {"auto", "back", "forward", "none", "backward", "front"}
INTERVAL_RE = re.compile(r"(?i)^(?P<value>\d+)(?P<unit>d|wk|w|mo|m|h|y|s)$")
SYMBOL_SAFE_RE = re.compile(r"^[A-Za-z0-9._+=#@$%&^-]+$")
INVALID_SYMBOL_SUBSTRINGS = ("\\", "/", "..", ":", "|", "?", "*", '"', "<", ">", ";")


class CliValidationError(ValueError):
    """包装 CLI 验证错误。"""


def _normalize_interval(value: str) -> str:
    value = value.strip().lower()
    if not value:
        raise ValueError("interval is empty")
    match = INTERVAL_RE.fullmatch(value)
    if not match:
        raise ValueError(f"invalid interval format: {value}")
    factor = int(match.group("value"))
    if factor <= 0:
        raise ValueError(f"interval must be > 0: {value}")
    return value


def _validate_symbol(symbol: str, *, field: str = "symbol") -> str:
    token = (symbol or "").strip()
    if not token:
        raise ValueError(f"{field} is empty")
    if len(token) > 128:
        raise ValueError(f"{field} too long: {token}")
    for bad in INVALID_SYMBOL_SUBSTRINGS:
        if bad in token:
            raise ValueError(f"unsafe {field}: {token}")
    if not SYMBOL_SAFE_RE.fullmatch(token):
        raise ValueError(f"unsafe {field}: {token}")
    return token


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
    data_root: str | None = None
    asset_type: str | None = None
    limit: int | None = Field(default=None, ge=1)
    workers: int | None = Field(default=None, ge=1, le=256)
    verbose: bool = False
    adjust: str | None = None
    download_log_file: str | None = None
    intervals: list[str] = Field(default_factory=list)
    symbols: list[str] = Field(default_factory=list)
    symbol: list[str] = Field(default_factory=list)
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

    @field_validator("symbols", mode="before")
    @classmethod
    def _symbols(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            raw_values = value
        else:
            raw_values = [value]
        if not raw_values:
            return []
        return [_validate_symbol(item, field="symbols") for item in raw_values]

    @field_validator("symbol", mode="before")
    @classmethod
    def _symbol(cls, value: Any) -> list[str]:
        if value is None:
            return []
        if isinstance(value, list):
            raw_values = value
        else:
            if isinstance(value, str):
                raw_values = [item for item in value.split(",") if item.strip()]
            else:
                raw_values = [value]
        return [_validate_symbol(item, field="symbol") for item in raw_values]

    @field_validator("data_root", "download_log_file")
    @classmethod
    def _non_empty_path(cls, value: str | None) -> str | None:
        if value is None:
            return None
        trimmed = str(value).strip()
        if not trimmed:
            return None
        return trimmed

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

    @model_validator(mode="after")
    @classmethod
    def _cross_checks(cls, model: "CliArgsModel", info: ValidationInfo) -> "CliArgsModel":
        if model.start and model.end and model.start > model.end:
            raise ValueError("start must be <= end")
        context = info.context or {}
        command = context.get("command")
        asset_type = context.get("asset_type", model.asset_type)
        if command in {"download", "wizard"} and asset_type and asset_type != "ashare":
            adjust = model.adjust or "auto"
            if adjust not in {"auto", "none"}:
                raise ValueError(
                    "for non-ashare asset_type, adjust supports only auto or none; "
                    "use --asset-type ashare for back/forward"
                )
        return model


def validate_cli_args(args: Namespace, *, asset_types: list[str]) -> CliArgsModel:
    """Validate argparse args and return typed model."""

    try:
        return CliArgsModel.model_validate(
            vars(args),
            context={
                "asset_types": asset_types,
                "command": getattr(args, "command", None),
                "asset_type": getattr(args, "asset_type", None),
            },
        )
    except ValidationError as exc:
        raise CliValidationError(str(exc)) from exc


def render_cli_error(error: CliValidationError) -> str:
    return str(error)
