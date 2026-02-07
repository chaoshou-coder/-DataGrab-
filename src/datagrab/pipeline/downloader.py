from __future__ import annotations

import csv
import io
import random
import threading
import time
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable

import polars as pl

from ..fsutils import atomic_write_text
from ..logging import get_logger
from ..timeutils import beijing_now, parse_date, to_beijing
from .writer import ParquetWriter
from ..sources.base import DataSource


@dataclass(frozen=True)
class DownloadTask:
    symbol: str
    interval: str
    start: datetime
    end: datetime
    asset_type: str
    adjust: str


@dataclass
class FailureRecord:
    task: DownloadTask
    reason: str


@dataclass
class DownloadStats:
    total: int
    completed: int = 0
    active: int = 0
    failed: int = 0
    skipped: int = 0
    recent_failures: list[FailureRecord] = field(default_factory=list)


ProgressCallback = Callable[[DownloadStats], None]


def _format_failure_reason(exc: Exception) -> str:
    """格式化失败原因，保证即使异常消息为空也有可读信息。"""
    name = type(exc).__name__
    msg = str(exc).strip()
    if not msg:
        return name
    # 避免重复 “ValueError: ValueError: ...”
    if msg.startswith(f"{name}:"):
        return msg
    return f"{name}: {msg}"


class Downloader:
    def __init__(
        self,
        source: DataSource,
        writer: ParquetWriter,
        concurrency: int,
        batch_days: int,
        startup_jitter_max: float,
    ):
        self.source = source
        self.writer = writer
        self.concurrency = max(1, concurrency)
        self.batch_days = batch_days
        self.startup_jitter_max = startup_jitter_max
        self.logger = get_logger("datagrab.downloader")
        # ── 运行控制 ──────────────────────────────────────────
        self._cancel = threading.Event()   # set() → 取消
        self._pause = threading.Event()    # clear() → 暂停, set() → 运行
        self._pause.set()                  # 初始：运行

    def pause(self) -> None:
        """暂停下载（已在跑的任务完成当前文件后暂停）。"""
        self._pause.clear()

    def resume(self) -> None:
        """恢复下载。"""
        self._pause.set()

    def cancel(self) -> None:
        """取消下载（不可恢复）。"""
        self._cancel.set()
        self._pause.set()  # 唤醒可能正在等 pause 的线程，让它们检测到 cancel

    @property
    def is_paused(self) -> bool:
        return not self._pause.is_set()

    @property
    def is_cancelled(self) -> bool:
        return self._cancel.is_set()

    def build_tasks(
        self,
        symbols: list[str],
        intervals: list[str],
        start: datetime,
        end: datetime,
        asset_type: str,
        adjust: str,
    ) -> list[DownloadTask]:
        tasks = []
        for symbol in symbols:
            for interval in intervals:
                tasks.append(
                    DownloadTask(
                        symbol=symbol,
                        interval=interval,
                        start=start,
                        end=end,
                        asset_type=asset_type,
                        adjust=adjust,
                    )
                )
        return tasks

    def run(
        self,
        tasks: list[DownloadTask],
        failures_path: Path,
        only_failures: bool = False,
        progress_cb: ProgressCallback | None = None,
    ) -> list[FailureRecord]:
        if only_failures:
            tasks = self._load_failures(failures_path)
        random.shuffle(tasks)
        stats = DownloadStats(total=len(tasks))
        failures: list[FailureRecord] = []
        lock = threading.Lock()

        def update(cb_stats: DownloadStats) -> None:
            if progress_cb:
                progress_cb(cb_stats)

        def worker(task: DownloadTask) -> None:
            nonlocal stats
            # ── cancel check before start ──
            if self._cancel.is_set():
                return
            # ── pause gate ──
            self._pause.wait()
            if self._cancel.is_set():
                return
            if self.startup_jitter_max > 0:
                time.sleep(random.uniform(0, self.startup_jitter_max))
            with lock:
                stats.active += 1
                update(stats)
            try:
                status = self._run_task(task)
                with lock:
                    if status == "skipped":
                        stats.skipped += 1
            except Exception as exc:
                if self._cancel.is_set():
                    return
                # 记录详细异常（含堆栈），避免 TUI/CLI 只能看到“失败=N”
                self.logger.error(
                    "download failed: asset_type=%s symbol=%s interval=%s start=%s end=%s adjust=%s err=%s",
                    task.asset_type,
                    task.symbol,
                    task.interval,
                    task.start.date(),
                    task.end.date(),
                    task.adjust,
                    exc,
                    exc_info=True,
                )
                with lock:
                    stats.failed += 1
                    record = FailureRecord(task=task, reason=_format_failure_reason(exc))
                    failures.append(record)
                    stats.recent_failures.append(record)
                    # 保留最近 N 条，供 TUI 直观查看
                    if len(stats.recent_failures) > 20:
                        stats.recent_failures = stats.recent_failures[-20:]
                    # 失败发生时也立即推送一次（不必等到 finally）
                    update(stats)
            finally:
                with lock:
                    stats.active -= 1
                    stats.completed += 1
                    update(stats)

        if not tasks:
            return failures

        from concurrent.futures import ThreadPoolExecutor, as_completed

        with ThreadPoolExecutor(max_workers=self.concurrency) as executor:
            futures = [executor.submit(worker, task) for task in tasks]
            # 等待全部完成或被取消
            for f in as_completed(futures):
                if self._cancel.is_set():
                    break

        if failures:
            self._write_failures(failures_path, failures)
        return failures

    def _run_task(self, task: DownloadTask) -> str:
        if self._cancel.is_set():
            return "cancelled"
        self._pause.wait()
        if self._cancel.is_set():
            return "cancelled"
        existing = self.writer.find_existing(task.asset_type, task.symbol, task.interval)
        existing_path = None
        existing_start = task.start
        task_start = task.start
        if existing:
            existing_path = existing.path
            existing_start = min(existing.start, task.start)
            existing_max = self.writer.read_range_max(existing_path)
            if existing.start <= task.start and existing_max and existing_max >= task.end:
                return "skipped"
            if existing.start <= task.start and existing_max:
                task_start = self.writer.next_start(existing_max, task.interval)
        new_data = self._fetch_range(task, task_start, task.end)
        if new_data.is_empty():
            return "empty"
        output_path = self.writer.build_path(
            task.asset_type,
            task.symbol,
            task.interval,
            existing_start,
            task.end,
        )
        self.writer.merge_and_write(existing_path, new_data, output_path, task.adjust)
        return "ok"

    def _fetch_range(self, task: DownloadTask, start: datetime, end: datetime) -> pl.DataFrame:
        chunks = list(self._split_range(start, end))
        frames = []
        for chunk_start, chunk_end in chunks:
            if self._cancel.is_set():
                break
            self._pause.wait()
            if self._cancel.is_set():
                break
            result = self.source.fetch_ohlcv(
                task.symbol, task.interval, chunk_start, chunk_end, task.adjust
            )
            if not result.data.is_empty():
                frames.append(result.data)
        if not frames:
            return pl.DataFrame()
        df = pl.concat(frames, how="diagonal_relaxed")
        return df.unique(subset=["datetime"], keep="last").sort("datetime")

    def _split_range(self, start: datetime, end: datetime):
        current = to_beijing(start)
        final = to_beijing(end)
        while current < final:
            chunk_end = min(final, current + timedelta(days=self.batch_days))
            yield current, chunk_end
            current = chunk_end

    def _write_failures(self, path: Path, failures: list[FailureRecord]) -> None:
        # 使用标准 CSV writer，避免 reason 含逗号/换行导致文件不可读
        buf = io.StringIO()
        writer = csv.DictWriter(
            buf,
            fieldnames=["symbol", "interval", "start", "end", "asset_type", "adjust", "reason"],
            lineterminator="\n",
        )
        writer.writeheader()
        for failure in failures:
            t = failure.task
            writer.writerow(
                {
                    "symbol": t.symbol,
                    "interval": t.interval,
                    "start": str(t.start.date()),
                    "end": str(t.end.date()),
                    "asset_type": t.asset_type,
                    "adjust": t.adjust,
                    "reason": failure.reason,
                }
            )
        atomic_write_text(path, buf.getvalue())

    def _load_failures(self, path: Path) -> list[DownloadTask]:
        if not path.exists():
            return []
        tasks: list[DownloadTask] = []
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                start_str = row.get("start", "").strip()
                end_str = row.get("end", "").strip()
                start = parse_date(start_str) if start_str else (beijing_now() - timedelta(days=365))
                end = parse_date(end_str) if end_str else beijing_now()
                tasks.append(
                    DownloadTask(
                        symbol=row["symbol"],
                        interval=row["interval"],
                        start=start,
                        end=end,
                        asset_type=row.get("asset_type", "stock"),
                        adjust=row.get("adjust", "auto"),
                    )
                )
        return tasks
