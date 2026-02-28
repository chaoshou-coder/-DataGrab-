from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from .rate_limiter import RateLimitConfig


@dataclass
class FilterConfig:
    include_regex: list[str] = field(default_factory=list)
    exclude_regex: list[str] = field(default_factory=list)
    include_prefixes: list[str] = field(default_factory=list)
    exclude_prefixes: list[str] = field(default_factory=list)
    include_symbols: list[str] = field(default_factory=list)
    exclude_symbols: list[str] = field(default_factory=list)
    include_name_regex: list[str] = field(default_factory=list)
    exclude_name_regex: list[str] = field(default_factory=list)
    include_exchanges: list[str] = field(default_factory=list)
    exclude_exchanges: list[str] = field(default_factory=list)
    include_market_categories: list[str] = field(default_factory=list)
    exclude_market_categories: list[str] = field(default_factory=list)
    only_etf: bool | None = None
    only_fund: bool | None = None
    include_fund_categories: list[str] = field(default_factory=list)
    exclude_fund_categories: list[str] = field(default_factory=list)


@dataclass
class CatalogConfig:
    retries: int = 3
    sleep_sec: float = 0.6
    retry_backoff: float = 1.5
    limit: int = 500


@dataclass
class DownloadConfig:
    concurrency: int = 4
    batch_days: int = 60
    max_retries: int = 2
    startup_jitter_max: float = 0.6


@dataclass
class StorageConfig:
    data_root: str = "./data"
    merge_on_incremental: bool = True


@dataclass
class YFinanceConfig:
    proxy: str | None = None
    auto_adjust_default: str = "auto"


@dataclass
class BaostockConfig:
    adjust_default: str = "back"


@dataclass
class AppConfig:
    rate_limit: RateLimitConfig = field(default_factory=RateLimitConfig)
    catalog: CatalogConfig = field(default_factory=CatalogConfig)
    filters: FilterConfig = field(default_factory=FilterConfig)
    download: DownloadConfig = field(default_factory=DownloadConfig)
    storage: StorageConfig = field(default_factory=StorageConfig)
    yfinance: YFinanceConfig = field(default_factory=YFinanceConfig)
    baostock: BaostockConfig = field(default_factory=BaostockConfig)
    timezone: str = "Asia/Shanghai"
    intervals_default: list[str] = field(default_factory=lambda: ["1d"])
    asset_types: list[str] = field(default_factory=lambda: ["stock", "ashare", "forex", "crypto", "commodity"])

    @property
    def data_root_path(self) -> Path:
        return Path(self.storage.data_root).resolve()


def _load_yaml(path: Path) -> dict[str, Any]:
    try:
        import yaml  # type: ignore
    except Exception as exc:
        raise RuntimeError("pyyaml is required for YAML config") from exc
    content = path.read_text(encoding="utf-8")
    return yaml.safe_load(content) or {}


def _load_toml(path: Path) -> dict[str, Any]:
    import tomllib

    return tomllib.loads(path.read_text(encoding="utf-8"))


def _deep_merge(base: dict[str, Any], override: dict[str, Any]) -> dict[str, Any]:
    result = dict(base)
    for key, value in override.items():
        if isinstance(value, dict) and isinstance(result.get(key), dict):
            result[key] = _deep_merge(result[key], value)
        else:
            result[key] = value
    return result


def merge_filters(base: FilterConfig, extra: FilterConfig | None) -> FilterConfig:
    if extra is None:
        return base
    return FilterConfig(
        include_regex=base.include_regex + extra.include_regex,
        exclude_regex=base.exclude_regex + extra.exclude_regex,
        include_prefixes=base.include_prefixes + extra.include_prefixes,
        exclude_prefixes=base.exclude_prefixes + extra.exclude_prefixes,
        include_symbols=base.include_symbols + extra.include_symbols,
        exclude_symbols=base.exclude_symbols + extra.exclude_symbols,
        include_name_regex=base.include_name_regex + extra.include_name_regex,
        exclude_name_regex=base.exclude_name_regex + extra.exclude_name_regex,
        include_exchanges=base.include_exchanges + extra.include_exchanges,
        exclude_exchanges=base.exclude_exchanges + extra.exclude_exchanges,
        include_market_categories=base.include_market_categories + extra.include_market_categories,
        exclude_market_categories=base.exclude_market_categories + extra.exclude_market_categories,
        only_etf=extra.only_etf if extra.only_etf is not None else base.only_etf,
        only_fund=extra.only_fund if extra.only_fund is not None else base.only_fund,
        include_fund_categories=base.include_fund_categories + extra.include_fund_categories,
        exclude_fund_categories=base.exclude_fund_categories + extra.exclude_fund_categories,
    )


def load_config(path: str | None = None) -> AppConfig:
    data: dict[str, Any] = {}
    config_path = path or os.getenv("DATAGRAB_CONFIG")
    if config_path:
        p = Path(config_path)
        if not p.exists():
            raise FileNotFoundError(f"config not found: {p}")
        if p.suffix.lower() in (".yaml", ".yml"):
            data = _load_yaml(p)
        elif p.suffix.lower() == ".toml":
            data = _load_toml(p)
        else:
            raise ValueError("config must be YAML or TOML")
    from .validation.config import ValidationConfigError, build_config_model, validate_config_payload

    try:
        validated = validate_config_payload(data)
    except ValidationConfigError as exc:
        raise RuntimeError(f"invalid config: {exc}") from exc
    config = build_config_model(validated)
    data_root_override = os.getenv("DATAGRAB_DATA_ROOT")
    if data_root_override:
        config.storage.data_root = data_root_override
    return config
