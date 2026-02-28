from __future__ import annotations

from zoneinfo import ZoneInfo

from pydantic import BaseModel, ConfigDict, Field, ValidationError, field_validator, model_validator

from ..config import (
    AppConfig,
    RateLimitConfig,
    CatalogConfig,
    DownloadConfig,
    FilterConfig,
    StorageConfig,
    YFinanceConfig,
    BaostockConfig,
)


class ValidationConfigError(ValueError):
    """包装配置校验错误。"""


class RateLimitConfigModel(BaseModel):
    requests_per_second: float = 2.0
    jitter_min: float = 0.2
    jitter_max: float = 0.6
    backoff_base: float = 1.5
    backoff_max: float = 30.0

    model_config = ConfigDict(extra="ignore")

    @field_validator("requests_per_second")
    @classmethod
    def _requests_per_second(cls, value: float) -> float:
        if value < 0:
            raise ValueError("requests_per_second must be >= 0")
        return float(value)

    @field_validator("jitter_min", "jitter_max", "backoff_base", "backoff_max")
    @classmethod
    def _non_negative(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be >= 0")
        return float(value)

    @model_validator(mode="after")
    def _jitter_range(self) -> "RateLimitConfigModel":
        if self.jitter_min > self.jitter_max:
            raise ValueError("jitter_min cannot be greater than jitter_max")
        return self


class CatalogConfigModel(BaseModel):
    retries: int = 3
    sleep_sec: float = 0.6
    retry_backoff: float = 1.5
    limit: int = 500

    model_config = ConfigDict(extra="ignore")

    @field_validator("retries", "limit")
    @classmethod
    def _positive_int(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return int(value)

    @field_validator("sleep_sec", "retry_backoff")
    @classmethod
    def _non_negative_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be >= 0")
        return float(value)


class DownloadConfigModel(BaseModel):
    concurrency: int = 4
    batch_days: int = 60
    max_retries: int = 2
    startup_jitter_max: float = 0.6

    model_config = ConfigDict(extra="ignore")

    @field_validator("concurrency", "batch_days", "max_retries")
    @classmethod
    def _positive_int(cls, value: int) -> int:
        if value < 1:
            raise ValueError("must be >= 1")
        return int(value)

    @field_validator("startup_jitter_max")
    @classmethod
    def _non_negative_float(cls, value: float) -> float:
        if value < 0:
            raise ValueError("must be >= 0")
        return float(value)


class StorageConfigModel(BaseModel):
    data_root: str = "./data"
    merge_on_incremental: bool = True

    model_config = ConfigDict(extra="ignore")

    @field_validator("data_root")
    @classmethod
    def _data_root(cls, value: str) -> str:
        if not value:
            raise ValueError("storage.data_root is empty")
        return str(value)


class YFinanceConfigModel(BaseModel):
    proxy: str | None = None
    auto_adjust_default: str = "auto"

    model_config = ConfigDict(extra="ignore")

    @field_validator("auto_adjust_default")
    @classmethod
    def _adjust_default(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"auto", "back", "forward", "none", "front", "backward"}:
            raise ValueError("yfinance.auto_adjust_default invalid")
        return normalized


class BaostockConfigModel(BaseModel):
    adjust_default: str = "back"

    model_config = ConfigDict(extra="ignore")

    @field_validator("adjust_default")
    @classmethod
    def _adjust_default(cls, value: str) -> str:
        normalized = value.strip().lower()
        if normalized not in {"auto", "back", "forward", "none", "front", "backward"}:
            raise ValueError("baostock.adjust_default invalid")
        return normalized


class FilterConfigModel(BaseModel):
    include_regex: list[str] = Field(default_factory=list)
    exclude_regex: list[str] = Field(default_factory=list)
    include_prefixes: list[str] = Field(default_factory=list)
    exclude_prefixes: list[str] = Field(default_factory=list)
    include_symbols: list[str] = Field(default_factory=list)
    exclude_symbols: list[str] = Field(default_factory=list)
    include_name_regex: list[str] = Field(default_factory=list)
    exclude_name_regex: list[str] = Field(default_factory=list)
    include_exchanges: list[str] = Field(default_factory=list)
    exclude_exchanges: list[str] = Field(default_factory=list)
    include_market_categories: list[str] = Field(default_factory=list)
    exclude_market_categories: list[str] = Field(default_factory=list)
    only_etf: bool | None = None
    only_fund: bool | None = None
    include_fund_categories: list[str] = Field(default_factory=list)
    exclude_fund_categories: list[str] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")


class AppConfigPayload(BaseModel):
    rate_limit: RateLimitConfigModel = RateLimitConfigModel()
    catalog: CatalogConfigModel = CatalogConfigModel()
    filters: FilterConfigModel = Field(default_factory=FilterConfigModel)
    download: DownloadConfigModel = DownloadConfigModel()
    storage: StorageConfigModel = StorageConfigModel()
    yfinance: YFinanceConfigModel = YFinanceConfigModel()
    baostock: BaostockConfigModel = BaostockConfigModel()
    timezone: str = "Asia/Shanghai"
    intervals_default: list[str] = Field(default_factory=lambda: ["1d"])
    asset_types: list[str] = Field(
        default_factory=lambda: ["stock", "ashare", "forex", "crypto", "commodity"]
    )

    model_config = ConfigDict(extra="ignore")

    @field_validator("asset_types")
    @classmethod
    def _asset_types(cls, value: list[str]) -> list[str]:
        if not value:
            raise ValueError("asset_types 不能为空")
        normalized = []
        for item in value:
            token = (item or "").strip()
            if not token:
                raise ValueError("asset_types 包含空值")
            normalized.append(token)
        # 保持输入顺序并去重
        uniq = []
        seen = set()
        for item in normalized:
            if item in seen:
                continue
            uniq.append(item)
            seen.add(item)
        return uniq

    @field_validator("intervals_default")
    @classmethod
    def _intervals_default(cls, value: list[str]) -> list[str]:
        if not value:
            return ["1d"]
        return [item.strip() for item in value if item and item.strip()]

    @field_validator("timezone")
    @classmethod
    def _timezone(cls, value: str) -> str:
        zone = (value or "").strip() or "Asia/Shanghai"
        try:
            ZoneInfo(zone)
        except Exception as exc:
            raise ValueError(f"invalid timezone: {value}") from exc
        return zone


def validate_config_payload(data: dict) -> AppConfigPayload:
    try:
        return AppConfigPayload.model_validate(data)
    except ValidationError as exc:
        raise ValidationConfigError(str(exc)) from exc


def build_config_model(validated: AppConfigPayload) -> AppConfig:
    """将 Pydantic 配置模型转换为内部 dataclass 结构。"""

    return AppConfig(
        rate_limit=RateLimitConfig(
            **validated.rate_limit.model_dump(),
        ),
        catalog=CatalogConfig(
            **validated.catalog.model_dump(),
        ),
        filters=FilterConfig(**validated.filters.model_dump()),
        download=DownloadConfig(
            **validated.download.model_dump(),
        ),
        storage=StorageConfig(
            **validated.storage.model_dump(),
        ),
        yfinance=YFinanceConfig(**validated.yfinance.model_dump()),
        baostock=BaostockConfig(**validated.baostock.model_dump()),
        timezone=validated.timezone,
        intervals_default=validated.intervals_default,
        asset_types=validated.asset_types,
    )
