from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from textual.app import App

from ..config import AppConfig
from ..pipeline.writer import ParquetWriter
from ..sources.base import DataSource
from ..timeutils import default_date_range
from .screens import AssetTypeScreen


@dataclass
class UiState:
    asset_type: str = "stock"
    symbols: List[str] = field(default_factory=list)
    intervals: List[str] = field(default_factory=lambda: ["1d"])
    start: datetime = field(default_factory=lambda: default_date_range().start)
    end: datetime = field(default_factory=lambda: default_date_range().end)
    adjust: str = "auto"
    concurrency: int = 4
    requests_per_second: float = 2.0


class DatagrabApp(App):
    CSS = """
    Screen {
        align: center middle;
    }
    .panel {
        width: 80%;
        height: 90%;
    }
    """

    def __init__(self, config: AppConfig, source: DataSource, writer: ParquetWriter) -> None:
        super().__init__()
        self.config = config
        self.source = source
        self.writer = writer
        self.state = UiState(
            intervals=config.intervals_default,
            adjust=config.yfinance.auto_adjust_default,
            concurrency=config.download.concurrency,
            requests_per_second=config.rate_limit.requests_per_second,
        )
        self.set_asset_type(self.state.asset_type)

    def set_asset_type(self, asset_type: str) -> None:
        self.state.asset_type = asset_type
        if hasattr(self.source, "set_asset_type"):
            self.source.set_asset_type(asset_type)
        if asset_type == "ashare":
            self.state.adjust = self.config.baostock.adjust_default
        else:
            self.state.adjust = self.config.yfinance.auto_adjust_default

    def on_mount(self) -> None:
        self.push_screen(AssetTypeScreen())
