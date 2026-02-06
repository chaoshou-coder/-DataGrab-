from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass
from datetime import datetime

import polars as pl


@dataclass(frozen=True)
class SymbolInfo:
    symbol: str
    name: str | None
    exchange: str | None
    asset_type: str
    market_category: str | None = None
    is_etf: bool | None = None
    is_fund: bool | None = None
    fund_category: str | None = None


@dataclass(frozen=True)
class OhlcvResult:
    data: pl.DataFrame
    adjustment: str | None = None


class DataSource(ABC):
    @abstractmethod
    def list_symbols(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
    ) -> list[SymbolInfo]:
        raise NotImplementedError

    @abstractmethod
    def fetch_ohlcv(
        self,
        symbol: str,
        interval: str,
        start: datetime,
        end: datetime,
        adjust: str,
    ) -> OhlcvResult:
        raise NotImplementedError
