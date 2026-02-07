from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from typing import List

from textual.app import App

from pathlib import Path

from ..config import AppConfig
from ..pipeline.catalog import CatalogService
from ..pipeline.downloader import Downloader, DownloadStats
from ..pipeline.writer import ParquetWriter
from ..sources.base import DataSource
from ..timeutils import default_date_range
from .screens import SetupScreen


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


@dataclass
class DownloadJob:
    """一个正在运行或已完成的下载任务（按资产类型分组）。"""
    asset_type: str
    label: str                    # 中文展示名
    downloader: Downloader
    stats: DownloadStats = field(default_factory=lambda: DownloadStats(total=0))
    status: str = "running"       # running | paused | cancelled | done | failed
    message: str = ""             # 完成/失败后的信息


class DatagrabApp(App):
    CSS = """
    Screen {
        align: center middle;
        background: #2a2a2a;
        color: #f0f0f0;
    }
    .panel {
        width: 80%;
        height: 90%;
    }
    .hidden {
        display: none;
    }
    .sel-actions {
        height: auto;
    }
    .action-bar {
        height: auto;
        margin-top: 1;
    }
    .busy-bar {
        height: auto;
        margin: 1 0;
    }
    Input, Select, Button, Label, Static, SelectionList, ProgressBar, RichLog {
        color: #f0f0f0;
    }
    #include_exchange_list, #include_market_list {
        height: 5;
        max-height: 8;
    }
    #catalog_selection {
        height: 10;
        max-height: 16;
    }
    #catalog_progress {
        min-height: 1;
        padding: 0 0 1 0;
        color: #f2f2f2;
    }
    #fetch_log {
        min-height: 3;
        padding: 1 2;
        color: #f2f2f2;
    }
    .job-ctrl {
        height: auto;
        margin-bottom: 1;
    }
    #jobs_display {
        min-height: 3;
        padding: 1 0;
    }
    """

    def __init__(
        self,
        config: AppConfig,
        source: DataSource,
        writer: ParquetWriter,
        catalog_service: CatalogService,
    ) -> None:
        super().__init__()
        self.config = config
        self.source = source
        self.writer = writer
        self.catalog_service = catalog_service
        self.state = UiState(
            intervals=config.intervals_default,
            adjust=config.yfinance.auto_adjust_default,
            concurrency=config.download.concurrency,
            requests_per_second=config.rate_limit.requests_per_second,
        )
        self.set_asset_type(self.state.asset_type)
        # ── 多任务下载管理（按 asset_type 键控） ─────────────
        self.jobs: dict[str, DownloadJob] = {}

    def set_data_root(self, path: Path) -> None:
        path = Path(path).resolve()
        self.writer.set_data_root(path)
        self.catalog_service.set_data_root(path)

    def apply_config(self, new_config: AppConfig) -> None:
        self.config = new_config
        self.set_data_root(self.config.data_root_path)

    def set_asset_type(self, asset_type: str) -> None:
        self.state.asset_type = asset_type
        if hasattr(self.source, "set_asset_type"):
            self.source.set_asset_type(asset_type)
        # 复权仅适用于股票/ETF；商品/外汇/加密货币不适用
        if asset_type in ("crypto", "forex", "commodity"):
            self.state.adjust = "none"
            return
        if asset_type == "ashare":
            self.state.adjust = self.config.baostock.adjust_default
        else:
            self.state.adjust = self.config.yfinance.auto_adjust_default

    # ── 多任务下载 ─────────────────────────────────────────

    def submit_job(
        self,
        asset_type: str,
        label: str,
        symbols: list[str],
        intervals: list[str],
        start: datetime,
        end: datetime,
        adjust: str,
        concurrency: int,
    ) -> DownloadJob:
        """创建一个下载 job 并在后台线程启动。"""
        downloader = Downloader(
            source=self.source,
            writer=self.writer,
            concurrency=concurrency,
            batch_days=self.config.download.batch_days,
            startup_jitter_max=self.config.download.startup_jitter_max,
        )
        tasks = downloader.build_tasks(
            symbols=symbols,
            intervals=intervals,
            start=start,
            end=end,
            asset_type=asset_type,
            adjust=adjust,
        )
        job = DownloadJob(
            asset_type=asset_type,
            label=label,
            downloader=downloader,
            stats=DownloadStats(total=len(tasks)),
            status="running",
        )
        self.jobs[asset_type] = job

        import threading

        def _run() -> None:
            failures_path = self.config.data_root_path / f"failures_{asset_type}.csv"

            def on_progress(stats: DownloadStats) -> None:
                job.stats = stats

            failures = downloader.run(tasks, failures_path=failures_path, progress_cb=on_progress)
            if downloader.is_cancelled:
                job.status = "cancelled"
                job.message = "已手动终止"
            elif failures:
                job.status = "done"
                job.message = f"完成，有 {len(failures)} 条失败（{failures_path.name}）"
            else:
                job.status = "done"
                job.message = "完成，无失败任务"

        t = threading.Thread(target=_run, daemon=True, name=f"job-{asset_type}")
        t.start()
        return job

    def on_mount(self) -> None:
        self.push_screen(SetupScreen())
