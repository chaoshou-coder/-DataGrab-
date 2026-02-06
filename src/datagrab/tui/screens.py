from __future__ import annotations

import re

from textual import work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical
from textual.screen import Screen
from textual.widgets import Button, Input, Label, ProgressBar, Select, Static, TextLog

from ..pipeline.downloader import Downloader
from ..timeutils import default_date_range, parse_date


class AssetTypeScreen(Screen):
    def compose(self) -> ComposeResult:
        with Vertical(classes="panel"):
            yield Static("选择资产类型", id="title")
            yield Select(
                options=[(t, t) for t in self.app.config.asset_types],
                value=self.app.state.asset_type,
                id="asset_type",
            )
            with Horizontal():
                yield Button("下一步", id="next")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "next":
            asset_type = self.query_one("#asset_type", Select).value
            self.app.set_asset_type(asset_type)
            self.app.push_screen(CatalogScreen())


class CatalogScreen(Screen):
    def __init__(self) -> None:
        super().__init__()
        self.items = []

    def compose(self) -> ComposeResult:
        with Vertical(classes="panel"):
            yield Static("目录选择（加载后可用过滤）", id="title")
            yield Input(placeholder="包含过滤（symbol/name；re: 正则）", id="include_filter")
            yield Input(placeholder="排除过滤（symbol/name；re: 正则）", id="exclude_filter")
            yield Input(placeholder="交易所包含（逗号分隔）", id="include_exchange")
            yield Input(placeholder="交易所排除（逗号分隔）", id="exclude_exchange")
            yield Input(placeholder="板块包含（market category）", id="include_market")
            yield Input(placeholder="板块排除（market category）", id="exclude_market")
            yield Input(placeholder="数量上限（空=默认）", id="limit")
            with Horizontal():
                yield Button("加载目录", id="load")
                yield Button("刷新目录", id="refresh")
                yield Button("使用目录前N", id="use-top")
                yield Button("下一步", id="next")
                yield Button("返回", id="back")
            yield TextLog(id="catalog", highlight=False)
            yield Label("手工输入 symbol（逗号分隔），留空则使用目录结果")
            yield Input(placeholder="AAPL,MSFT", id="symbols")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "load":
            self._do_load_catalog(refresh=False)
        if event.button.id == "refresh":
            self._do_load_catalog(refresh=True)
        if event.button.id == "use-top":
            symbols = [item.symbol for item in self.items][: self._limit()]
            self.query_one("#symbols", Input).value = ",".join(symbols)
        if event.button.id == "next":
            symbols_value = self.query_one("#symbols", Input).value.strip()
            if symbols_value:
                symbols = [s.strip() for s in symbols_value.split(",") if s.strip()]
            else:
                symbols = [item.symbol for item in self.items][: self._limit()]
            self.app.state.symbols = symbols
            self.app.push_screen(ConfigScreen())
        if event.button.id == "back":
            self.app.pop_screen()

    def _do_load_catalog(self, refresh: bool) -> None:
        include_filt = self.query_one("#include_filter", Input).value.strip()
        exclude_filt = self.query_one("#exclude_filter", Input).value.strip()
        include_exchange = self.query_one("#include_exchange", Input).value.strip()
        exclude_exchange = self.query_one("#exclude_exchange", Input).value.strip()
        include_market = self.query_one("#include_market", Input).value.strip()
        exclude_market = self.query_one("#exclude_market", Input).value.strip()
        limit = self._limit()
        self.load_catalog(
            include_filt,
            exclude_filt,
            include_exchange,
            exclude_exchange,
            include_market,
            exclude_market,
            limit,
            refresh=refresh,
        )

    def _limit(self) -> int:
        limit_value = self.query_one("#limit", Input).value.strip()
        if not limit_value:
            return self.app.config.catalog.limit
        try:
            return max(1, int(limit_value))
        except ValueError:
            return self.app.config.catalog.limit

    @work(thread=True)
    def load_catalog(
        self,
        include_filt: str,
        exclude_filt: str,
        include_exchange: str,
        exclude_exchange: str,
        include_market: str,
        exclude_market: str,
        limit: int,
        refresh: bool = False,
    ) -> None:
        asset_type = self.app.state.asset_type
        items = self.app.source.list_symbols(asset_type=asset_type, refresh=refresh, limit=limit)
        if include_filt or exclude_filt:
            items = self._apply_text_filters(items, include_filt, exclude_filt)
        if include_exchange or exclude_exchange or include_market or exclude_market:
            items = self._apply_exchange_filters(
                items,
                include_exchange,
                exclude_exchange,
                include_market,
                exclude_market,
            )
        self.items = items
        self.call_from_thread(self._render_catalog)

    def _render_catalog(self) -> None:
        log = self.query_one("#catalog", TextLog)
        log.clear()
        for item in self.items:
            extra = []
            if item.exchange:
                extra.append(item.exchange)
            if item.market_category:
                extra.append(item.market_category)
            if getattr(item, "market_category", None):
                try:
                    from ..pipeline.catalog import exchange_alias, fund_category_alias, market_alias

                    exch_alias = exchange_alias(item.exchange)
                    if exch_alias:
                        extra.append(exch_alias)
                    alias = market_alias(item.market_category)
                    if alias:
                        extra.append(alias)
                    fund_alias = fund_category_alias(getattr(item, "fund_category", None))
                    if fund_alias:
                        extra.append(fund_alias)
                except Exception:
                    pass
            suffix = f" ({' / '.join(extra)})" if extra else ""
            log.write(f"{item.symbol}  {item.name or ''}{suffix}")

    def _apply_text_filters(self, items, include_filt: str, exclude_filt: str):
        def match_text(text: str, pattern: str) -> bool:
            if not pattern:
                return True
            if pattern.startswith("re:"):
                expr = pattern[3:].strip()
                if not expr:
                    return True
                try:
                    return re.search(expr, text, re.IGNORECASE) is not None
                except re.error:
                    return expr.lower() in text.lower()
            return pattern.lower() in text.lower()

        def include_match(item) -> bool:
            if not include_filt:
                return True
            texts = [item.symbol, item.name or ""]
            return any(match_text(t, include_filt) for t in texts if t)

        def exclude_match(item) -> bool:
            if not exclude_filt:
                return False
            texts = [item.symbol, item.name or ""]
            return any(match_text(t, exclude_filt) for t in texts if t)

        filtered = []
        for item in items:
            if not include_match(item):
                continue
            if exclude_match(item):
                continue
            filtered.append(item)
        return filtered

    def _apply_exchange_filters(
        self,
        items,
        include_exchange: str,
        exclude_exchange: str,
        include_market: str,
        exclude_market: str,
    ):
        def split_values(value: str) -> list[str]:
            return [v.strip().upper() for v in value.split(",") if v.strip()]

        include_ex = set(split_values(include_exchange))
        exclude_ex = set(split_values(exclude_exchange))
        include_mk = set(split_values(include_market))
        exclude_mk = set(split_values(exclude_market))

        filtered = []
        for item in items:
            exchange = (item.exchange or "").upper()
            market = (item.market_category or "").upper()
            if include_ex and exchange not in include_ex:
                continue
            if exclude_ex and exchange in exclude_ex:
                continue
            if include_mk and market not in include_mk:
                continue
            if exclude_mk and market in exclude_mk:
                continue
            filtered.append(item)
        return filtered


class ConfigScreen(Screen):
    def compose(self) -> ComposeResult:
        default_range = default_date_range()
        with Vertical(classes="panel"):
            yield Static("下载配置", id="title")
            yield Input(
                value=",".join(self.app.state.intervals),
                placeholder="intervals: 1d,1h",
                id="intervals",
            )
            yield Input(
                value=default_range.start.strftime("%Y-%m-%d"),
                placeholder="start (YYYY-MM-DD)",
                id="start",
            )
            yield Input(
                value=default_range.end.strftime("%Y-%m-%d"),
                placeholder="end (YYYY-MM-DD)",
                id="end",
            )
            yield Select(
                options=[("none", "none"), ("auto", "auto"), ("back", "back"), ("forward", "forward")],
                value=self.app.state.adjust,
                id="adjust",
            )
            yield Input(
                value=str(self.app.state.concurrency),
                placeholder="并发数",
                id="concurrency",
            )
            yield Input(
                value=str(self.app.state.requests_per_second),
                placeholder="请求/秒上限",
                id="rps",
            )
            with Horizontal():
                yield Button("开始下载", id="run")
                yield Button("返回", id="back")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.app.pop_screen()
            return
        if event.button.id != "run":
            return
        intervals_value = self.query_one("#intervals", Input).value.strip()
        intervals = [s.strip() for s in intervals_value.split(",") if s.strip()] or ["1d"]
        start_value = self.query_one("#start", Input).value.strip()
        end_value = self.query_one("#end", Input).value.strip()
        default_range = default_date_range()
        try:
            start = parse_date(start_value) if start_value else default_range.start
        except Exception:
            start = default_range.start
        try:
            end = parse_date(end_value) if end_value else default_range.end
        except Exception:
            end = default_range.end
        adjust = self.query_one("#adjust", Select).value
        concurrency_value = self.query_one("#concurrency", Input).value.strip()
        rps_value = self.query_one("#rps", Input).value.strip()
        concurrency = int(concurrency_value) if concurrency_value.isdigit() else self.app.state.concurrency
        try:
            rps = float(rps_value)
        except ValueError:
            rps = self.app.state.requests_per_second

        self.app.state.intervals = intervals
        self.app.state.start = start
        self.app.state.end = end
        self.app.state.adjust = adjust
        self.app.state.concurrency = concurrency
        self.app.state.requests_per_second = rps
        if hasattr(self.app.source, "rate_limiter"):
            self.app.source.rate_limiter.config.requests_per_second = rps
        self.app.push_screen(RunScreen())


class RunScreen(Screen):
    def compose(self) -> ComposeResult:
        with Vertical(classes="panel"):
            yield Static("执行下载", id="title")
            yield ProgressBar(total=1, id="progress")
            yield Label("等待开始", id="stats")
            yield TextLog(id="log", highlight=False)
            yield Button("退出", id="exit")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "exit":
            self.app.exit()

    def on_mount(self) -> None:
        self.run_download()

    @work(thread=True)
    def run_download(self) -> None:
        state = self.app.state
        downloader = Downloader(
            source=self.app.source,
            writer=self.app.writer,
            concurrency=state.concurrency,
            batch_days=self.app.config.download.batch_days,
            startup_jitter_max=self.app.config.download.startup_jitter_max,
        )
        tasks = downloader.build_tasks(
            symbols=state.symbols,
            intervals=state.intervals,
            start=state.start,
            end=state.end,
            asset_type=state.asset_type,
            adjust=state.adjust,
        )
        failures_path = self.app.config.data_root_path / "failures.csv"

        def on_progress(stats):
            self.app.call_from_thread(self._update_progress, stats)

        failures = downloader.run(tasks, failures_path=failures_path, progress_cb=on_progress)
        self.app.call_from_thread(self._finalize, failures, failures_path)

    def _update_progress(self, stats) -> None:
        progress = self.query_one("#progress", ProgressBar)
        progress.total = max(1, stats.total)
        progress.update(stats.completed)
        label = self.query_one("#stats", Label)
        label.update(
            f"total={stats.total} done={stats.completed} active={stats.active} "
            f"failed={stats.failed} skipped={stats.skipped}"
        )

    def _finalize(self, failures, failures_path) -> None:
        log = self.query_one("#log", TextLog)
        if failures:
            log.write(f"完成，有失败记录：{failures_path}")
        else:
            log.write("完成，无失败任务")
