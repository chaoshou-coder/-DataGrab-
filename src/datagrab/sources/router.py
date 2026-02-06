from __future__ import annotations

from typing import Mapping

from .base import DataSource, OhlcvResult, SymbolInfo


class SourceRouter(DataSource):
    def __init__(self, default_source: DataSource, source_by_asset: Mapping[str, DataSource]):
        self.default_source = default_source
        self.source_by_asset = dict(source_by_asset)
        self.current_asset_type: str | None = None

    def set_asset_type(self, asset_type: str) -> None:
        self.current_asset_type = asset_type

    def list_symbols(self, asset_type: str, refresh: bool = False, limit: int | None = None) -> list[SymbolInfo]:
        return self._select(asset_type).list_symbols(asset_type, refresh=refresh, limit=limit)

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
        if asset_type and asset_type in self.source_by_asset:
            return self.source_by_asset[asset_type]
        if self.default_source is None:
            raise ValueError(f"no source for asset_type={asset_type}")
        return self.default_source
