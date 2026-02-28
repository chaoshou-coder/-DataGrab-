from __future__ import annotations

from typing import TYPE_CHECKING, Mapping

from .base import DataSource, OhlcvResult, SymbolInfo

if TYPE_CHECKING:
    from ..config import FilterConfig


class SourceRouter(DataSource):
    def __init__(
        self,
        default_source: DataSource,
        source_by_asset: Mapping[str, DataSource],
        *,
        allowed_asset_types: list[str] | None = None,
    ):
        self.default_source = default_source
        self.source_by_asset = dict(source_by_asset)
        self.allowed_asset_types = set(allowed_asset_types or list(source_by_asset.keys()) + ["stock"])
        self.current_asset_type: str | None = None

    def set_asset_type(self, asset_type: str) -> None:
        if asset_type not in self.allowed_asset_types:
            raise ValueError(f"unsupported asset_type: {asset_type}")
        self.current_asset_type = asset_type

    def list_symbols(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
    ) -> list[SymbolInfo]:
        return self._select(asset_type).list_symbols(
            asset_type, refresh=refresh, limit=limit, filters_override=filters_override
        )

    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start,
        end,
        adjust: str,
    ) -> OhlcvResult:
        return self._select(self.current_asset_type).fetch_ohlcv(symbol, interval, start, end, adjust)

    def _select(self, asset_type: str | None) -> DataSource:
        if asset_type is not None and asset_type not in self.allowed_asset_types:
            raise ValueError(f"unsupported asset_type: {asset_type}")
        if asset_type and asset_type in self.source_by_asset:
            return self.source_by_asset[asset_type]
        if not self.default_source:
            raise ValueError(f"unsupported asset_type: {asset_type}")
        return self.default_source
