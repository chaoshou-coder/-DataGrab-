from __future__ import annotations

import re
from concurrent.futures import ThreadPoolExecutor, TimeoutError as FuturesTimeoutError
from datetime import datetime
from pathlib import Path

from textual import on, work
from textual.app import ComposeResult
from textual.containers import Horizontal, Vertical, VerticalScroll
from textual.screen import Screen
from textual.widgets import (
    Button,
    Checkbox,
    Input,
    Label,
    LoadingIndicator,
    RichLog,
    Select,
    SelectionList,
    Static,
)

from ..config import FilterConfig
from ..timeutils import default_date_range, parse_date
from ..storage.quality import QualityIssue, Severity, write_issues_csv, write_issues_jsonl
from ..storage.validate import BatchProgress, FileSummary, iter_parquet_files, validate_batch

# 交易所/板块/基金子类选项改为从目录数据动态生成（见 load_catalog 后 result.exchange_options 等），不再硬编码

# 记住上次使用的数据根目录（写入用户目录下文件）
_LAST_DATA_ROOT_PATH = Path.home() / ".datagrab" / "last_data_root.txt"


def _load_last_data_root() -> str | None:
    try:
        if _LAST_DATA_ROOT_PATH.exists():
            val = _LAST_DATA_ROOT_PATH.read_text(encoding="utf-8").strip()
            return val or None
    except Exception:
        return None
    return None


def _save_last_data_root(value: str) -> None:
    v = (value or "").strip()
    if not v:
        return
    try:
        _LAST_DATA_ROOT_PATH.parent.mkdir(parents=True, exist_ok=True)
        _LAST_DATA_ROOT_PATH.write_text(v, encoding="utf-8")
    except Exception:
        # 忽略持久化失败，不影响主流程
        pass


def _timestamp_for_filename() -> str:
    return datetime.now().strftime("%Y%m%d_%H%M%S")


# 联网获取 symbol 列表时的总超时（秒），超时后终止并提示
CATALOG_FETCH_TIMEOUT_SECONDS = 300

# 目录拉取分步进度展示（step_id -> 中文描述）
# 美股多步 / A股两步 / 其它资产单步，按顺序匹配
CATALOG_PROGRESS_STEPS = (
    ("cache", "已从缓存加载"),
    # 美股
    ("reachability", "1. 检测连接"),
    ("download_nasdaq", "2. 下载 nasdaqlisted"),
    ("download_other", "3. 下载 otherlisted"),
    ("write_cache", "4. 写入缓存"),
    # A股 (akshare + 代码前缀 ETF 识别)
    ("fetch_ashare_stock", "1. 获取 A 股列表（股票）"),
    ("fetch_ashare_etf", "2. 识别 ETF"),
    # 其它 (crypto/forex/commodity)
    ("fetch", "1. 拉取列表"),
)


def _asset_kind_options(asset_type: str) -> list[tuple[str, str]]:
    """标的类型选项随资产种类变动：A 股与美股均为全部/仅股票/仅ETF（akshare 分类中基金与 ETF 合一）。"""
    if asset_type == "ashare":
        return [
            ("全部", "all"),
            ("仅股票", "stock_only"),
            ("仅ETF", "etf_only"),
        ]
    if asset_type == "stock":
        return [
            ("全部", "all"),
            ("仅股票", "stock_only"),
            ("仅ETF", "etf_only"),
        ]
    return [("全部", "all")]


# 资产类型展示名（A股作为股票子集与美股并列）
ASSET_TYPE_LABELS: dict[str, str] = {
    "stock": "股票 · 美股",
    "ashare": "股票 · A股",
    "forex": "外汇",
    "crypto": "加密货币",
    "commodity": "商品",
}


class SetupScreen(Screen):
    """TUI 首屏：变量配置。数据根目录决定 symbol 列表与行情数据保存位置。"""

    def compose(self) -> ComposeResult:
        data_root = getattr(
            self.app.config.storage,
            "data_root",
            "",
        )
        dr = (data_root if isinstance(data_root, str) else str(data_root or "")).strip()
        last = _load_last_data_root()
        # 若配置仍是默认值（或为空），尝试回填上次使用的目录
        if (not dr) or dr in ("./data", ".\\data"):
            if last:
                dr = last
        if dr and dr not in ("./data", ".\\data"):
            data_root_val = dr
            placeholder = f"默认使用上次目录：{dr}" if last else "例如 .\\data 或 /path/to/data"
        else:
            data_root_val = ""
            placeholder = f"默认使用上次目录：{last}" if last else "必填：填写路径（例如 .\\data 或 /path/to/data）"
        with VerticalScroll(classes="panel"):
            yield Static("变量配置（保存位置由数据根目录决定）", id="title")
            label_text = "数据根目录（symbol 列表与行情数据保存于此，必填）"
            if last:
                label_text = f"数据根目录（默认使用上次目录：{last}，可修改）"
            yield Label(label_text)
            yield Input(
                value=data_root_val,
                placeholder=placeholder,
                id="data_root",
            )
            yield Static("", id="config_message")
            with Horizontal():
                yield Button("更新配置", id="update_config")
                yield Button("数据检查", id="validate")
                yield Button("下一步", id="config_next")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        msg = self.query_one("#config_message", Static)
        if event.button.id == "update_config":
            self._do_update_config(msg)
            return
        if event.button.id == "validate":
            self.app.push_screen(ValidateScreen())
            return
        if event.button.id == "config_next":
            self._do_next(msg)

    def _do_update_config(self, msg: Static) -> None:
        data_root = self.query_one("#data_root", Input).value.strip()
        if not data_root:
            msg.update("更新配置失败：请填写数据根目录")
            return
        self.app.config.storage.data_root = data_root
        self.app.set_data_root(Path(data_root).resolve())
        _save_last_data_root(data_root)
        msg.update("已更新数据根目录")

    def _do_next(self, msg: Static) -> None:
        data_root = (self.query_one("#data_root", Input).value or "").strip()
        if not data_root:
            msg.update("请先填写数据根目录并「更新配置」后再下一步")
            return
        self.app.config.storage.data_root = data_root
        self.app.set_data_root(Path(data_root).resolve())
        _save_last_data_root(data_root)
        self.app.push_screen(AssetTypeScreen())


class AssetTypeScreen(Screen):
    def compose(self) -> ComposeResult:
        with VerticalScroll(classes="panel"):
            yield Static("选择资产类型", id="title")
            yield Select(
                options=[
                    (ASSET_TYPE_LABELS.get(t, t), t) for t in self.app.config.asset_types
                ],
                value=self.app.state.asset_type,
                id="asset_type",
            )
            with Horizontal():
                yield Button("返回", id="back")
                yield Button("下一步", id="next")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "back":
            self.app.pop_screen()
        elif event.button.id == "next":
            asset_type = self.query_one("#asset_type", Select).value
            self.app.set_asset_type(asset_type)
            self.app.push_screen(ConfirmFetchScreen())


class ConfirmFetchScreen(Screen):
    """选择资产类型后，先确认是否联网拉取最新 symbol 列表并保存到本地；联网更新时展示进度，成功后再进入下一步。"""

    def compose(self) -> ComposeResult:
        asset = self.app.state.asset_type
        label = ASSET_TYPE_LABELS.get(asset, asset)
        if asset in ("stock", "ashare"):
            source_hint = "数据源：美股=NASDAQ，A股=baostock"
        else:
            source_hint = "数据源：Yahoo Finance screener（加密货币/外汇/商品期货）"
        with VerticalScroll(classes="panel"):
            yield Static(
                f"是否联网获取【{label}】的最新 symbol 列表并保存到本地？\n"
                f"{source_hint}\n"
                "更新后，目录选择将优先使用本地列表。",
                id="title",
            )
            with Horizontal(id="fetch_busy", classes="busy-bar hidden"):
                yield LoadingIndicator(id="fetch_busy_spinner")
                yield Static("", id="fetch_busy_text")
            with Horizontal(id="fetch_choice"):
                yield Button("返回", id="fetch_go_back")
                yield Button("是，联网更新", id="fetch")
                yield Button("否，使用本地", id="skip")
            with Vertical(id="fetch_progress_container", classes="hidden"):
                yield Static("", id="fetch_log")
                with Horizontal(id="fetch_fail_actions", classes="hidden"):
                    yield Button("返回", id="fetch_back")

    def on_mount(self) -> None:
        self.query_one("#fetch_progress_container", Vertical).add_class("hidden")
        self.query_one("#fetch_fail_actions", Horizontal).add_class("hidden")
        self._set_fetch_busy(None, False)

    def on_button_pressed(self, event: Button.Pressed) -> None:
        if event.button.id == "fetch_go_back":
            self.app.pop_screen()
        elif event.button.id == "fetch":
            self._start_fetch()
        elif event.button.id == "skip":
            self.app.push_screen(CatalogScreen())
        elif event.button.id == "fetch_back":
            self.app.pop_screen()

    def _start_fetch(self) -> None:
        self.query_one("#fetch_choice", Horizontal).add_class("hidden")
        self.query_one("#fetch_progress_container", Vertical).remove_class("hidden")
        self.query_one("#fetch_log", Static).update("正在准备…")
        self.query_one("#fetch_fail_actions", Horizontal).add_class("hidden")
        self._progress_steps: dict[str, tuple[str, str | None]] = {}
        self._set_fetch_busy("正在联网获取目录…", True)
        self._do_fetch_then_catalog()

    def _on_fetch_ok(self) -> None:
        self._update_fetch_log(suffix="[green]获取成功，正在进入目录选择…[/green]")
        self._set_fetch_busy(None, False)
        self.app.push_screen(CatalogScreen())

    def _on_fetch_fail(self, message: str) -> None:
        self._update_fetch_log(suffix=f"[red]获取失败：{message}[/red]")
        self.query_one("#fetch_fail_actions", Horizontal).remove_class("hidden")
        self._set_fetch_busy(None, False)

    def _on_fetch_timeout(self, timeout_seconds: int) -> None:
        """联网获取超时：在日志区追加超时提示，保留已展示的步骤。"""
        mins = timeout_seconds // 60
        self._update_fetch_log(
            suffix=f"[red]获取超时（已等待 {mins} 分钟），已终止。请检查网络/代理后重试。[/red]"
        )
        self.query_one("#fetch_fail_actions", Horizontal).remove_class("hidden")
        self._set_fetch_busy(None, False)

    def _on_fetch_progress(self, step_id: str, status: str, detail: str | None) -> None:
        self._progress_steps[step_id] = (status, detail)
        self._update_fetch_log()
        label_map = dict(CATALOG_PROGRESS_STEPS)
        label = label_map.get(step_id)
        if label:
            if status == "start":
                msg = f"{label} …"
            elif status == "progress" and detail:
                msg = f"{label} ({detail})"
            elif status == "done":
                msg = f"{label} 完成"
            else:
                msg = f"{label} {status}"
            self._set_fetch_busy(msg, True)

    def _update_fetch_log(self, suffix: str | None = None) -> None:
        """按终端日志样式渲染分步进度，已完成步骤保留展示。"""
        try:
            log_widget = self.query_one("#fetch_log", Static)
        except Exception:
            return
        steps = getattr(self, "_progress_steps", {})
        if not steps and not suffix:
            log_widget.update("正在准备…")
            return
        lines: list[str] = []
        for step_id, label in CATALOG_PROGRESS_STEPS:
            if step_id not in steps:
                continue
            status, detail = steps[step_id]
            if status == "start":
                lines.append(f"  [yellow]●[/yellow] {label} …")
            elif status == "done":
                detail_str = f" ({detail} 条)" if detail else ""
                lines.append(f"  [green]✓[/green] {label} 完成{detail_str}")
            elif status == "progress" and detail:
                lines.append(f"  [yellow]●[/yellow] {label} 进行中 ({detail})")
            else:
                lines.append(f"  {label} {status}")
        if suffix:
            lines.append(suffix)
        log_widget.update("\n".join(lines) if lines else "正在准备…")

    def _set_fetch_busy(self, message: str | None, active: bool) -> None:
        try:
            bar = self.query_one("#fetch_busy", Horizontal)
            text = self.query_one("#fetch_busy_text", Static)
        except Exception:
            return
        if active:
            bar.remove_class("hidden")
            text.update(message or "处理中…")
        else:
            bar.add_class("hidden")
            text.update("")

    @work(thread=True)
    def _do_fetch_then_catalog(self) -> None:
        asset_type = self.app.state.asset_type

        def progress_cb(step_id: str, status: str, detail: str | None) -> None:
            self.app.call_from_thread(
                lambda s=step_id, t=status, d=detail: self._on_fetch_progress(s, t, d)
            )

        def do_fetch():
            return self.app.catalog_service.get_catalog(
                asset_type=asset_type,
                refresh=True,
                limit=None,
                filters_override=None,
                progress_callback=progress_cb,
            )

        ex = ThreadPoolExecutor(max_workers=1)
        future = ex.submit(do_fetch)
        try:
            future.result(timeout=CATALOG_FETCH_TIMEOUT_SECONDS)
            self.app.call_from_thread(self._on_fetch_ok)
        except FuturesTimeoutError:
            future.cancel()
            ex.shutdown(wait=False)
            self.app.call_from_thread(
                lambda: self._on_fetch_timeout(CATALOG_FETCH_TIMEOUT_SECONDS)
            )
            return
        except Exception as e:
            self.app.call_from_thread(self._on_fetch_fail, str(e))
        ex.shutdown(wait=False)


class CatalogScreen(Screen):
    def __init__(self) -> None:
        super().__init__()
        self.items: list = []
        self.total_count = 0  # 筛选后总条数（含被 limit 截断部分），用于提示
        self._last_filters_tui: FilterConfig | None = None

    def compose(self) -> ComposeResult:
        _asset = self.app.state.asset_type
        _has_filters = _asset in ("stock", "ashare")
        with VerticalScroll(classes="panel"):
            yield Static("目录选择（加载后可用过滤）", id="title")
            with Horizontal(id="catalog_busy", classes="busy-bar hidden"):
                yield LoadingIndicator(id="catalog_busy_spinner")
                yield Static("", id="catalog_busy_text")
            if _has_filters:
                yield Label("交易所（可多选，空=不筛；加载/刷新目录后从数据更新）")
                with Horizontal(classes="sel-actions"):
                    yield Button("全选", id="exchange_all")
                    yield Button("反选", id="exchange_invert")
                yield SelectionList[str](id="include_exchange_list")
                yield Label("板块（可多选，空=不筛；加载/刷新目录后从数据更新）")
                with Horizontal(classes="sel-actions"):
                    yield Button("全选", id="market_all")
                    yield Button("反选", id="market_invert")
                yield SelectionList[str](id="include_market_list")
                yield Label("标的类型")
                _kind_opts = _asset_kind_options(_asset)
                yield Select(
                    options=_kind_opts,
                    value="all",
                    id="asset_kind",
                )
            else:
                _label = ASSET_TYPE_LABELS.get(_asset, _asset)
                yield Label(f"资产类型：{_label}（从数据源获取列表；可在下方手工添加/删减 symbol）")
            with Horizontal(classes="action-bar"):
                yield Button("加载目录", id="load")
                yield Button("刷新目录", id="refresh")
                yield Button(f"使用前 {self.app.config.catalog.limit} 条", id="use-top")
            yield Static("", id="catalog_progress")
            yield Label("目录共 0 条，已选 0 个标的（勾选下方列表；空格键勾选）", id="catalog_count")
            with Horizontal(classes="sel-actions"):
                yield Button("全选", id="catalog_sel_all")
                yield Button("反选", id="catalog_sel_invert")
            yield SelectionList[str](id="catalog_selection")
            yield Label("手工输入 symbol（逗号分隔）；有内容时优先使用此处，忽略上方勾选")
            yield Input(placeholder="例如 AAPL,MSFT,GOOGL 或 BTC-USD,EURUSD=X,GC=F", id="symbols")
            with Horizontal(classes="action-bar"):
                yield Button("返回", id="back")
                yield Button("下一步", id="next")

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id
        if bid in ("exchange_all", "exchange_invert", "market_all", "market_invert"):
            list_id = {"exchange_all": "include_exchange_list", "exchange_invert": "include_exchange_list",
                       "market_all": "include_market_list", "market_invert": "include_market_list"}[bid]
            try:
                sel = self.query_one(f"#{list_id}", SelectionList)
                if bid.endswith("_all"):
                    sel.select_all()
                else:
                    sel.toggle_all()
            except Exception:
                pass
            return
        if bid == "catalog_sel_all":
            self.query_one("#catalog_selection", SelectionList).select_all()
            return
        if bid == "catalog_sel_invert":
            self.query_one("#catalog_selection", SelectionList).toggle_all()
            return
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
                try:
                    sel = self.query_one("#catalog_selection", SelectionList)
                    symbols = list(sel.selected)
                except Exception:
                    symbols = []
                if not symbols:
                    # 没有手工输入也没有勾选：默认下载“全部筛选结果”，而不是仅前 N 条
                    if self._last_filters_tui is not None and self.total_count > len(self.items):
                        self._fetch_all_symbols_then_next()
                        return
                    symbols = [item.symbol for item in self.items]
            self.app.state.symbols = symbols
            self.app.push_screen(DownloadConfigScreen())
        if event.button.id == "back":
            self.app.pop_screen()

    @on(SelectionList.SelectedChanged)
    def _on_selection_changed(self, event: SelectionList.SelectedChanged) -> None:
        if getattr(event.control, "id", None) == "catalog_selection":
            self._on_catalog_selection_changed()

    def _do_load_catalog(self, refresh: bool) -> None:
        try:
            include_exchange_list = self.query_one("#include_exchange_list", SelectionList)
            include_exchanges = list(include_exchange_list.selected)
        except Exception:
            include_exchanges = []
        try:
            include_market_list = self.query_one("#include_market_list", SelectionList)
            include_markets = list(include_market_list.selected)
        except Exception:
            include_markets = []
        try:
            asset_kind = self.query_one("#asset_kind", Select).value
        except Exception:
            asset_kind = "all"
        only_etf, only_fund = self._asset_kind_to_flags(asset_kind)
        limit = self.app.config.catalog.limit
        busy_label = "正在刷新目录…" if refresh else "正在加载目录…"
        self._set_catalog_busy(busy_label, True)
        self.load_catalog(
            include_exchanges,
            include_markets,
            [],
            only_etf,
            only_fund,
            limit,
            refresh=refresh,
        )

    def _asset_kind_to_flags(self, kind: str) -> tuple[bool | None, bool | None]:
        if kind == "stock_only":
            return False, False
        if kind == "etf_only":
            return True, None
        if kind == "fund_only":
            return None, True
        return None, None

    def _limit(self) -> int:
        return self.app.config.catalog.limit

    @work(thread=True)
    def load_catalog(
        self,
        include_exchanges: list[str],
        include_markets: list[str],
        include_fund_cats: list[str],
        only_etf: bool | None,
        only_fund: bool | None,
        limit: int,
        refresh: bool = False,
    ) -> None:
        asset_type = self.app.state.asset_type
        base = self.app.config.filters
        filters_tui = FilterConfig(
            include_regex=base.include_regex,
            exclude_regex=base.exclude_regex,
            include_prefixes=base.include_prefixes,
            exclude_prefixes=base.exclude_prefixes,
            include_symbols=base.include_symbols,
            exclude_symbols=base.exclude_symbols,
            include_name_regex=base.include_name_regex,
            exclude_name_regex=base.exclude_name_regex,
            include_exchanges=include_exchanges or base.include_exchanges,
            exclude_exchanges=base.exclude_exchanges,
            include_market_categories=include_markets or base.include_market_categories,
            exclude_market_categories=base.exclude_market_categories,
            only_etf=only_etf,
            only_fund=only_fund,
            include_fund_categories=include_fund_cats or base.include_fund_categories,
            exclude_fund_categories=base.exclude_fund_categories,
        )
        # 记住本次筛选条件，供“下一步”在需要时拉取完整列表（limit=None）
        self._last_filters_tui = filters_tui
        self._progress_steps: dict[str, tuple[str, str | None]] = {}
        self.app.call_from_thread(self._update_catalog_progress_display)

        def progress_cb(step_id: str, status: str, detail: str | None) -> None:
            # 用 lambda 捕获当前参数，避免回调执行时被后续步骤覆盖
            self.app.call_from_thread(
                lambda s=step_id, t=status, d=detail: self._on_catalog_progress(s, t, d)
            )

        def do_get_catalog():
            return self.app.catalog_service.get_catalog(
                asset_type=asset_type,
                refresh=refresh,
                limit=limit,
                filters_override=filters_tui,
                progress_callback=progress_cb,
            )

        if refresh:
            # 联网拉取：带超时，超时后终止并保留已展示的步骤
            ex = ThreadPoolExecutor(max_workers=1)
            future = ex.submit(do_get_catalog)
            try:
                result = future.result(timeout=CATALOG_FETCH_TIMEOUT_SECONDS)
            except FuturesTimeoutError:
                future.cancel()
                ex.shutdown(wait=False)
                self.app.call_from_thread(
                    lambda: self._on_catalog_timeout(CATALOG_FETCH_TIMEOUT_SECONDS)
                )
                return
            ex.shutdown(wait=False)
        else:
            result = do_get_catalog()

        self.items = result.items
        self.total_count = result.total_count
        self._exchange_options = getattr(result, "exchange_options", [])
        self._market_options = getattr(result, "market_options", [])
        self._fund_options = getattr(result, "fund_options", [])
        self.app.call_from_thread(self._render_catalog)

    @work(thread=True)
    def _fetch_all_symbols_then_next(self) -> None:
        """当筛选结果超过展示上限(limit)时，下一步自动拉取完整列表用于下载。"""
        asset_type = self.app.state.asset_type
        filters = self._last_filters_tui
        try:
            self.app.call_from_thread(
                lambda: self.query_one("#catalog_progress", Static).update("正在加载全部筛选结果…")
            )
        except Exception:
            pass
        self.app.call_from_thread(lambda: self._set_catalog_busy("正在加载全部筛选结果…", True))
        try:
            result = self.app.catalog_service.get_catalog(
                asset_type=asset_type,
                refresh=False,
                limit=None,
                filters_override=filters,
                progress_callback=None,
            )
            symbols = [i.symbol for i in result.items]
            self.app.call_from_thread(self._set_symbols_and_next, symbols)
        except Exception as exc:
            # 失败则回退用当前展示列表，至少能继续
            exc_msg = str(exc)
            symbols = [item.symbol for item in self.items]
            self.app.call_from_thread(self._set_symbols_and_next, symbols)
            try:
                self.app.call_from_thread(
                    lambda: self.query_one("#catalog_progress", Static).update(
                        f"加载全部失败，已回退：{exc_msg}"
                    )
                )
            except Exception:
                pass

    def _set_symbols_and_next(self, symbols: list[str]) -> None:
        self._set_catalog_busy(None, False)
        self.app.state.symbols = symbols
        self.app.push_screen(DownloadConfigScreen())

    def _on_catalog_progress(self, step_id: str, status: str, detail: str | None) -> None:
        self._progress_steps[step_id] = (status, detail)
        self._update_catalog_progress_display()
        label_map = dict(CATALOG_PROGRESS_STEPS)
        label = label_map.get(step_id)
        if label:
            if status == "start":
                msg = f"{label} …"
            elif status == "progress" and detail:
                msg = f"{label} ({detail})"
            elif status == "done":
                msg = f"{label} 完成"
            else:
                msg = f"{label} {status}"
            self._set_catalog_busy(msg, True)

    def _on_catalog_timeout(self, timeout_seconds: int) -> None:
        """联网获取超时：在进度区追加超时提示，保留已展示的步骤，不更新目录列表。"""
        self._progress_steps["_timeout"] = ("done", f"{timeout_seconds // 60} 分钟")
        try:
            prog = self.query_one("#catalog_progress", Static)
            steps = getattr(self, "_progress_steps", {})
            lines: list[str] = []
            for step_id, label in CATALOG_PROGRESS_STEPS:
                if step_id not in steps:
                    continue
                status, detail = steps[step_id]
                if status == "start":
                    lines.append(f"{label} …")
                elif status == "done":
                    lines.append(f"{label} 完成" + (f" ({detail} 条)" if detail else ""))
                elif status == "progress" and detail:
                    lines.append(f"{label} 进行中 ({detail})")
                else:
                    lines.append(f"{label} {status}")
            if "_timeout" in steps:
                _, mins = steps["_timeout"]
                lines.append(f"[red]获取超时（{mins}），已终止[/red]")
            prog.update("\n".join(lines) if lines else "获取超时，已终止")
        except Exception:
            pass
        self._set_catalog_busy(None, False)

    def _update_catalog_progress_display(self) -> None:
        try:
            prog = self.query_one("#catalog_progress", Static)
        except Exception:
            return
        steps = getattr(self, "_progress_steps", {})
        if not steps:
            prog.update("正在准备…")
            return
        lines: list[str] = []
        for step_id, label in CATALOG_PROGRESS_STEPS:
            if step_id not in steps:
                continue
            status, detail = steps[step_id]
            if status == "start":
                lines.append(f"{label} …")
            elif status == "done":
                lines.append(f"{label} 完成" + (f" ({detail} 条)" if detail else ""))
            elif status == "progress" and detail:
                lines.append(f"{label} 进行中 ({detail})")
            else:
                lines.append(f"{label} {status}")
        prog.update("\n".join(lines) if lines else "正在准备…")

    def _render_catalog(self) -> None:
        sel = self.query_one("#catalog_selection", SelectionList)
        count_label = self.query_one("#catalog_count", Label)
        sel.clear_options()
        self._set_catalog_busy(None, False)
        if self.items:
            sel.add_options(
                [(f"{item.symbol}  {item.name or ''}", item.symbol) for item in self.items]
            )
        n = len(self.items)
        total = getattr(self, "total_count", n)
        m = len(sel.selected)
        if total > n:
            count_label.update(
                f"显示前 {n} 条 / 筛选共 {total} 条，已选 {m} 个标的（勾选列表；空格键勾选）"
            )
        else:
            count_label.update(f"目录共 {n} 条，已选 {m} 个标的（勾选列表；空格键勾选）")
        # 用本次目录数据更新交易所/板块选项（动态，非硬编码）
        for list_id, opts in (
            ("include_exchange_list", getattr(self, "_exchange_options", [])),
            ("include_market_list", getattr(self, "_market_options", [])),
        ):
            try:
                lst = self.query_one(f"#{list_id}", SelectionList)
                lst.clear_options()
                if opts:
                    lst.add_options([(label, code) for label, code in opts])
            except Exception:
                pass

    def _set_catalog_busy(self, message: str | None, active: bool) -> None:
        try:
            bar = self.query_one("#catalog_busy", Horizontal)
            text = self.query_one("#catalog_busy_text", Static)
        except Exception:
            return
        if active:
            bar.remove_class("hidden")
            text.update(message or "处理中…")
        else:
            bar.add_class("hidden")
            text.update("")

    def _on_catalog_selection_changed(self) -> None:
        try:
            sel = self.query_one("#catalog_selection", SelectionList)
            count_label = self.query_one("#catalog_count", Label)
            n = len(self.items)
            total = getattr(self, "total_count", n)
            m = len(sel.selected)
            if total > n:
                count_label.update(
                    f"显示前 {n} 条 / 筛选共 {total} 条，已选 {m} 个标的（勾选列表；空格键勾选）"
                )
            else:
                count_label.update(f"目录共 {n} 条，已选 {m} 个标的（勾选列表；空格键勾选）")
        except Exception:
            pass

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


# 下载配置：复权方式下拉选项（显示名, 传给后端的值），不含自动复权
ADJUST_OPTIONS: list[tuple[str, str]] = [
    ("不复权", "none"),
    ("后复权", "back"),
    ("前复权", "forward"),
]
# 请求/秒上限推荐值（社区建议保守 0.5～2，避免 yfinance 被雅虎 429/封禁）
RPS_PRESET_OPTIONS: list[tuple[str, str]] = [
    ("保守 0.5/秒（推荐，不易被封）", "0.5"),
    ("1/秒", "1"),
    ("1.5/秒", "1.5"),
    ("2/秒（默认）", "2"),
    ("2.5/秒", "2.5"),
    ("3/秒（风险稍高）", "3"),
    ("自定义（下方填写）", ""),
]
# K线粒度常用预设（显示名, 值如 1d 或 1d,1h）
INTERVAL_PRESET_OPTIONS: list[tuple[str, str]] = [
    ("日线（1d）", "1d"),
    ("日线 + 1小时", "1d,1h"),
    ("日线 + 周线", "1d,1wk"),
    ("仅1小时线", "1h"),
    ("仅周线（1wk）", "1wk"),
    ("日/周/月线", "1d,1wk,1mo"),
    ("自定义（在下方填写）", ""),
]


class DownloadConfigScreen(Screen):
    def compose(self) -> ComposeResult:
        default_range = default_date_range()
        asset_type = getattr(self.app.state, "asset_type", "stock")
        has_adjust = asset_type in ("stock", "ashare")
        with VerticalScroll(classes="panel"):
            yield Static("下载配置", id="title")
            yield Label("K线粒度（可多选预设或下方自定义，如 1d,1h）", id="lbl_intervals")
            _joined = ",".join(self.app.state.intervals)
            _preset_value = _joined if any(o[1] == _joined for o in INTERVAL_PRESET_OPTIONS) else ""
            yield Select(
                options=INTERVAL_PRESET_OPTIONS,
                value=_preset_value,
                id="interval_preset",
            )
            yield Input(
                value=",".join(self.app.state.intervals),
                placeholder="自定义时填写，如 1d 或 1d,1h（1d=日 1h=时 1wk=周 1mo=月）",
                id="intervals",
            )
            yield Label("开始日期（YYYY-MM-DD）", id="lbl_start")
            yield Input(
                value=default_range.start.strftime("%Y-%m-%d"),
                placeholder="例如 2020-01-01",
                id="start",
            )
            yield Label("结束日期（YYYY-MM-DD）", id="lbl_end")
            yield Input(
                value=default_range.end.strftime("%Y-%m-%d"),
                placeholder="例如 2024-12-31",
                id="end",
            )
            if has_adjust:
                yield Label("复权方式（股票/ETF 价格按除权除息调整）", id="lbl_adjust")
                _adjust = self.app.state.adjust
                if _adjust not in {o[1] for o in ADJUST_OPTIONS}:
                    _adjust = "back"
                yield Select(
                    options=ADJUST_OPTIONS,
                    value=_adjust,
                    id="adjust",
                )
            else:
                yield Label("复权方式：不适用（外汇/加密货币/商品无需复权）", id="lbl_adjust")
            yield Label("并发数（同时下载的任务数，建议 2～8）", id="lbl_concurrency")
            yield Input(
                value=str(self.app.state.concurrency),
                placeholder="例如 4",
                id="concurrency",
            )
            yield Label("请求/秒上限（限速，避免被雅虎封禁；推荐 0.5～2）", id="lbl_rps")
            _rps_str = str(self.app.state.requests_per_second)
            _rps_preset = _rps_str if any(o[1] == _rps_str for o in RPS_PRESET_OPTIONS) else ""
            yield Select(
                options=RPS_PRESET_OPTIONS,
                value=_rps_preset,
                id="rps_preset",
            )
            yield Input(
                value=_rps_str if _rps_preset == "" else "",
                placeholder="自定义时填写，如 1.5",
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
        preset = self.query_one("#interval_preset", Select).value
        if preset:
            intervals = [s.strip() for s in preset.split(",") if s.strip()]
        else:
            intervals_value = self.query_one("#intervals", Input).value.strip()
            intervals = [s.strip() for s in intervals_value.split(",") if s.strip()]
        if not intervals:
            intervals = ["1d"]
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
        # 复权仅用于股票/ETF；其它资产强制 none
        asset_type = self.app.state.asset_type
        if asset_type in ("stock", "ashare"):
            adjust = self.query_one("#adjust", Select).value
        else:
            adjust = "none"
        concurrency_value = self.query_one("#concurrency", Input).value.strip()
        rps_preset = self.query_one("#rps_preset", Select).value
        rps_value = (rps_preset if rps_preset else self.query_one("#rps", Input).value.strip()).strip()
        concurrency = int(concurrency_value) if concurrency_value.isdigit() else self.app.state.concurrency
        try:
            rps = float(rps_value) if rps_value else self.app.state.requests_per_second
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

        # ── 提交任务并跳转到统一监控屏 ────────────────────────
        state = self.app.state
        asset_type = state.asset_type
        label = ASSET_TYPE_LABELS.get(asset_type, asset_type)

        # 不允许重复启动同一资产类型
        existing = self.app.jobs.get(asset_type)
        if existing and existing.status in ("running", "paused"):
            return

        # 开始下载时也更新「上次使用的数据根目录」，下次打开 TUI 可默认回填
        _save_last_data_root(getattr(self.app.config.storage, "data_root", "") or "")

        self.app.submit_job(
            asset_type=asset_type,
            label=label,
            symbols=list(state.symbols),
            intervals=list(state.intervals),
            start=state.start,
            end=state.end,
            adjust=state.adjust,
            concurrency=state.concurrency,
        )
        self.app.switch_screen(RunScreen())


# ── 状态符号 ──────────────────────────────────────────────
_STATUS_ICONS: dict[str, str] = {
    "running":   "[green]▶[/green]",
    "paused":    "[yellow]⏸[/yellow]",
    "cancelled": "[red]■[/red]",
    "done":      "[green]✓[/green]",
    "failed":    "[red]✗[/red]",
}


class RunScreen(Screen):
    """统一下载任务监控屏：展示所有资产类型的下载任务，支持暂停/继续/终止。"""

    _refresh_timer = None

    def compose(self) -> ComposeResult:
        with VerticalScroll(classes="panel"):
            yield Static("下载任务管理", id="title")
            with Horizontal(id="jobs_busy", classes="busy-bar hidden"):
                yield LoadingIndicator(id="jobs_busy_spinner")
                yield Static("", id="jobs_busy_text")
            yield Static("", id="jobs_display")
            with Horizontal(classes="action-bar"):
                yield Button("添加新任务", id="add_task")
                yield Button("数据检查", id="validate")
                yield Button("退出", id="exit")

    def on_mount(self) -> None:
        self.refresh_jobs()
        # 定时刷新（每 1 秒），用于拾取后台线程推送的进度
        self._refresh_timer = self.set_interval(1.0, self.refresh_jobs)

    def on_unmount(self) -> None:
        if self._refresh_timer:
            self._refresh_timer.stop()
            self._refresh_timer = None

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        if bid == "add_task":
            self._go_add_task()
        elif bid == "validate":
            self.app.push_screen(ValidateScreen())
        elif bid == "exit":
            self.app.exit()
        elif bid.startswith("pause_"):
            self._toggle_pause(bid.removeprefix("pause_"))
        elif bid.startswith("cancel_"):
            self._cancel_job(bid.removeprefix("cancel_"))

    def _go_add_task(self) -> None:
        """返回资产类型选择，让用户配置新的下载任务。"""
        self.app.push_screen(AssetTypeScreen())

    def _toggle_pause(self, asset_type: str) -> None:
        job = self.app.jobs.get(asset_type)
        if not job:
            return
        if job.status == "running":
            job.downloader.pause()
            job.status = "paused"
        elif job.status == "paused":
            job.downloader.resume()
            job.status = "running"
        self.refresh_jobs()

    def _cancel_job(self, asset_type: str) -> None:
        job = self.app.jobs.get(asset_type)
        if not job or job.status in ("done", "cancelled"):
            return
        job.downloader.cancel()
        job.status = "cancelled"
        job.message = "已手动终止"
        self.refresh_jobs()

    def refresh_jobs(self) -> None:
        """重新渲染所有任务的状态。"""
        try:
            display = self.query_one("#jobs_display", Static)
        except Exception:
            return
        jobs = self.app.jobs
        if not jobs:
            display.update("暂无下载任务。点击「添加新任务」开始。")
            self._set_jobs_busy(None, False)
            return
        lines: list[str] = []
        for asset_type, job in jobs.items():
            icon = _STATUS_ICONS.get(job.status, "?")
            s = job.stats
            pct = f"{s.completed * 100 // max(1, s.total)}%" if s.total else "0%"
            status_cn = {
                "running": "下载中",
                "paused": "已暂停",
                "cancelled": "已终止",
                "done": "已完成",
                "failed": "失败",
            }.get(job.status, job.status)

            lines.append(f"{icon} [bold]{job.label}[/bold]  {status_cn}  {pct}")
            lines.append(
                f"    总计={s.total}  完成={s.completed}  活跃={s.active}  "
                f"失败={s.failed}  跳过={s.skipped}"
            )
            if job.message:
                lines.append(f"    {job.message}")

            # 显示最近失败原因（最多 5 条，避免刷屏）
            recent = getattr(s, "recent_failures", None) or []
            if recent:
                lines.append("    [red]最近失败：[/red]")
                for fr in recent[-5:]:
                    t = fr.task
                    reason = fr.reason.replace("\n", " ").strip()
                    if len(reason) > 100:
                        reason = reason[:100] + "..."
                    lines.append(f"      - {t.symbol}/{t.interval}: {reason}")

            # 操作按钮行（通过按钮 id 传递 asset_type）
            # 活跃任务才显示控制
            if job.status in ("running", "paused"):
                # 按钮在 compose 中不好动态增减，改为在 _ensure_job_buttons 中处理
                pass

            lines.append("")

        display.update("\n".join(lines))
        self._update_jobs_busy(jobs)
        self._ensure_job_buttons()

    def _update_jobs_busy(self, jobs: dict) -> None:
        active = [j for j in jobs.values() if j.status in ("running", "paused")]
        if not active:
            self._set_jobs_busy(None, False)
            return
        total = sum(j.stats.total for j in active)
        completed = sum(j.stats.completed for j in active)
        pct = int(completed * 100 // max(1, total))
        status = "下载中" if any(j.status == "running" for j in active) else "已暂停"
        self._set_jobs_busy(f"{status} 总进度 {pct}% ({completed}/{total})", True)

    def _set_jobs_busy(self, message: str | None, active: bool) -> None:
        try:
            bar = self.query_one("#jobs_busy", Horizontal)
            text = self.query_one("#jobs_busy_text", Static)
        except Exception:
            return
        if active:
            bar.remove_class("hidden")
            text.update(message or "处理中…")
        else:
            bar.add_class("hidden")
            text.update("")

    def _ensure_job_buttons(self) -> None:
        """确保每个活跃任务有暂停/终止按钮。使用动态挂载。"""
        # 先移除旧的动态按钮
        for widget in self.query(".job-ctrl"):
            widget.remove()

        jobs = self.app.jobs
        container = None
        try:
            container = self.query_one("#jobs_display", Static).parent
        except Exception:
            return
        if container is None:
            return

        # 在 jobs_display 之后、action-bar 之前插入按钮
        insert_before = None
        try:
            bars = list(self.query(".action-bar"))
            if bars:
                insert_before = bars[-1]
        except Exception:
            pass

        for asset_type, job in jobs.items():
            if job.status in ("running", "paused"):
                pause_label = "继续" if job.status == "paused" else "暂停"
                h = Horizontal(classes="job-ctrl action-bar")
                pause_btn = Button(f"{pause_label} [{job.label}]", id=f"pause_{asset_type}")
                cancel_btn = Button(f"终止 [{job.label}]", id=f"cancel_{asset_type}")
                if insert_before:
                    container.mount(h, before=insert_before)
                else:
                    container.mount(h)
                h.mount(pause_btn)
                h.mount(cancel_btn)


class ValidateScreen(Screen):
    """数据检查/验数：扫描 parquet 并展示摘要与质量问题，可导出 jsonl/csv。"""

    CSS = """
    #validate_summary {
        min-height: 2;
        padding: 1 0;
    }
    #validate_log {
        height: 18;
        min-height: 10;
        border: round #666666;
        padding: 1 1;
    }
    """

    def __init__(self) -> None:
        super().__init__()
        self._last_issues: list[QualityIssue] = []
        self._last_file_summaries: list[FileSummary] = []
        self._last_data_root: Path | None = None
        self._validating: bool = False
        self._active: bool = False
        self._ready: bool = False  # on_mount 完成后才允许响应按钮

    def compose(self) -> ComposeResult:
        asset_options = [(ASSET_TYPE_LABELS.get(t, t), t) for t in self.app.config.asset_types]
        with VerticalScroll(classes="panel"):
            yield Static("数据检查 / 验数", id="title")
            with Horizontal(id="validate_busy", classes="busy-bar hidden"):
                yield LoadingIndicator(id="validate_busy_spinner")
                yield Static("", id="validate_busy_text")
            yield Label("数据根目录（可选；留空=使用配置；可临时覆盖）")
            yield Input(value="", placeholder="例如 .\\data 或 /path/to/data", id="validate_data_root")
            yield Static("", id="validate_data_root_effective")
            yield Static("", id="validate_hint")
            yield Label("资产类型")
            yield Select(
                options=asset_options,
                value=self.app.state.asset_type,
                id="validate_asset_type",
            )
            yield Label("Symbol（可空；为空=使用当前已选 symbols）")
            yield Input(value="", placeholder="例如 AAPL（留空则使用当前选择）", id="validate_symbol")
            yield Checkbox("symbol 留空时扫描该资产类型全部 symbols", value=False, id="validate_scan_all_symbols")
            yield Label("Interval（可空；为空=使用当前已选 intervals）")
            yield Input(value="", placeholder="例如 1d（留空则使用当前选择）", id="validate_interval")
            yield Checkbox("interval 留空时扫描全部 intervals", value=False, id="validate_scan_all_intervals")
            with Horizontal(classes="action-bar"):
                yield Button("开始检查", id="validate_start")
                yield Button("导出JSONL", id="validate_export_jsonl")
                yield Button("导出CSV", id="validate_export_csv")
                yield Button("返回", id="validate_back")
            yield Static("", id="validate_summary")
            yield RichLog(id="validate_log", markup=True, highlight=True)

    def on_mount(self) -> None:
        self._active = True
        self._render_defaults()
        # 延迟 0.3 秒后才允许响应按钮——防止屏幕切换时"幻影点击"
        self.set_timer(0.3, self._mark_ready)

    def _mark_ready(self) -> None:
        self._ready = True

    def on_unmount(self) -> None:
        self._active = False

    def _render_defaults(self) -> None:
        # 用 placeholder 呈现“配置里的默认值”，但不强制写入 Input.value（保持可选）
        data_root_str = ""
        try:
            data_root_str = str(getattr(self.app.config, "data_root_path", "") or "").strip()
        except Exception:
            data_root_str = ""
        if not data_root_str:
            try:
                data_root_str = str(getattr(self.app.config.storage, "data_root", "") or "").strip()
            except Exception:
                data_root_str = ""
        try:
            self.query_one("#validate_data_root", Input).placeholder = (
                data_root_str or "例如 .\\data 或 /path/to/data"
            )
        except Exception:
            pass
        self.query_one("#validate_data_root_effective", Static).update(
            f"当前使用：{data_root_str or '(未设置)'}（可在上方临时覆盖）"
        )

        s = self.app.state
        sym_hint = f"{len(getattr(s, 'symbols', []) or [])} 个" if getattr(s, "symbols", None) is not None else "0 个"
        int_hint = f"{len(getattr(s, 'intervals', []) or [])} 个" if getattr(s, "intervals", None) is not None else "0 个"
        self.query_one("#validate_hint", Static).update(
            f"默认检查范围：asset_type={s.asset_type}；symbols={sym_hint}；intervals={int_hint}\n"
            "提示：symbol/interval 留空默认使用当前选择；勾选开关后可扩大为扫描全部（注意耗时）。"
        )
        self.query_one("#validate_summary", Static).update("就绪。点击「开始检查」。")
        try:
            self.query_one("#validate_log", RichLog).clear()
        except Exception:
            pass

    def on_button_pressed(self, event: Button.Pressed) -> None:
        bid = event.button.id or ""
        # screen not ready yet -> only allow back (prevents phantom click during push)
        if not self._ready and bid != "validate_back":
            return
        if bid == "validate_back":
            self.app.pop_screen()
            return
        if bid == "validate_start":
            if self._validating:
                self.notify("检查正在运行中，请等待完成。", severity="warning")
                return
            self.notify("开始数据检查…")
            self._start_validation()
            return
        if bid == "validate_export_jsonl":
            self._export(fmt="jsonl")
            return
        if bid == "validate_export_csv":
            self._export(fmt="csv")
            return

    def _set_controls_running(self, running: bool) -> None:
        """运行期间禁用按钮，避免重复启动；完成后恢复。"""
        try:
            start_btn = self.query_one("#validate_start", Button)
            start_btn.disabled = running
            start_btn.label = "检查中…" if running else "开始检查"
        except Exception:
            pass
        try:
            title = self.query_one("#title", Static)
            title.update("数据检查 / 验数（检查中…）" if running else "数据检查 / 验数")
        except Exception:
            pass
        try:
            bar = self.query_one("#validate_busy", Horizontal)
            text = self.query_one("#validate_busy_text", Static)
            if running:
                bar.remove_class("hidden")
                text.update("正在检查数据…")
            else:
                bar.add_class("hidden")
                text.update("")
        except Exception:
            pass
        # 导出按钮在运行中也先禁用，避免导出到一半的结果
        try:
            self.query_one("#validate_export_jsonl", Button).disabled = running
        except Exception:
            pass
        try:
            self.query_one("#validate_export_csv", Button).disabled = running
        except Exception:
            pass

    def _start_validation(self) -> None:
        try:
            self._last_issues = []
            self._last_file_summaries = []
            self._last_data_root = None
            self._validating = True
            self._set_controls_running(True)

            try:
                log = self.query_one("#validate_log", RichLog)
                log.clear()
            except Exception:
                log = None

            self.query_one("#validate_summary", Static).update(
                "检查中… 已处理 0 文件（后台执行，不会阻塞 UI；可随时返回）"
            )
            if log:
                log.write("[cyan]正在扫描 parquet 文件…[/cyan]")

            # 注意：Windows 上某些网络/无效路径的 resolve/exists 可能卡住事件循环，
            # 因此把所有文件系统操作与扫描放到后台线程。
            asset_type = str(self.query_one("#validate_asset_type", Select).value or "").strip()
            data_root_override = (self.query_one("#validate_data_root", Input).value or "").strip()
            symbol_override = (self.query_one("#validate_symbol", Input).value or "").strip()
            interval_override = (self.query_one("#validate_interval", Input).value or "").strip()
            scan_all_symbols = bool(self.query_one("#validate_scan_all_symbols", Checkbox).value)
            scan_all_intervals = bool(self.query_one("#validate_scan_all_intervals", Checkbox).value)
            self.prepare_and_run_validation(
                asset_type=asset_type,
                data_root_override=data_root_override,
                symbol_override=symbol_override,
                interval_override=interval_override,
                scan_all_symbols=scan_all_symbols,
                scan_all_intervals=scan_all_intervals,
            )
        except Exception as exc:
            # 任何 UI 查询/更新异常，都直接回写到 summary，避免“无反应”
            try:
                self.query_one("#validate_summary", Static).update(f"[red]点击开始检查失败：{exc}[/red]")
            except Exception:
                pass
            self._validating = False
            self._set_controls_running(False)

    @work(thread=True)
    def prepare_and_run_validation(
        self,
        asset_type: str,
        data_root_override: str,
        symbol_override: str,
        interval_override: str,
        scan_all_symbols: bool,
        scan_all_intervals: bool,
    ) -> None:
        t0 = datetime.now()
        total_files = 0
        issue_files = 0
        issues: list[QualityIssue] = []
        summaries: list[FileSummary] = []
        seen: set[str] = set()
        last_ui = datetime.now()

        def ui(callable_):
            if not self._active:
                return
            self.app.call_from_thread(callable_)

        def log_line(text: str) -> None:
            def _do() -> None:
                try:
                    self.query_one("#validate_log", RichLog).write(text)
                except Exception:
                    pass

            ui(_do)

        def fail_fast(message: str) -> None:
            def _do() -> None:
                try:
                    self.query_one("#validate_summary", Static).update(message)
                except Exception:
                    pass
                self._validating = False
                self._set_controls_running(False)

            ui(_do)

        def update_summary(processed: int, current: str | None = None) -> None:
            def _do() -> None:
                if not self._active:
                    return
                try:
                    extra = f"  当前：{current}" if current else ""
                    self.query_one("#validate_summary", Static).update(
                        f"检查中… 已处理 {processed} 文件{extra}"
                    )
                except Exception:
                    pass

            ui(_do)

        try:
            # 解析 data_root（后台线程），避免卡住 UI
            data_root: Path | None = None
            if data_root_override:
                try:
                    data_root = Path(data_root_override).expanduser().resolve()
                except Exception:
                    data_root = None
            if data_root is None:
                data_root = getattr(self.app.config, "data_root_path", None)
            if not data_root:
                try:
                    dr = getattr(self.app.config.storage, "data_root", "") or ""
                    data_root = Path(str(dr)).expanduser().resolve() if str(dr).strip() else None
                except Exception:
                    data_root = None

            if not data_root:
                fail_fast("[red]未设置 data_root：请在本屏上方填写目录，或先在首页设置数据根目录。[/red]")
                return
            if not Path(data_root).exists():
                fail_fast(f"[red]data_root 不存在：{data_root}[/red]")
                return

            def _set_effective_root() -> None:
                self._last_data_root = Path(data_root)
                try:
                    self.query_one("#validate_data_root_effective", Static).update(f"当前使用：{data_root}")
                except Exception:
                    pass

            ui(_set_effective_root)

            state_symbols = list(getattr(self.app.state, "symbols", []) or [])
            state_intervals = list(getattr(self.app.state, "intervals", []) or [])

            if symbol_override:
                symbol_specs: list[str | None] = [symbol_override]
                symbol_scope = f"symbol={symbol_override}"
            elif scan_all_symbols:
                symbol_specs = [None]
                symbol_scope = "symbol=ALL"
            else:
                symbol_specs = state_symbols
                symbol_scope = f"symbols={len(symbol_specs)}(current)"

            if interval_override:
                interval_specs: list[str | None] = [interval_override]
                interval_scope = f"interval={interval_override}"
            elif scan_all_intervals:
                interval_specs = [None]
                interval_scope = "interval=ALL"
            else:
                interval_specs = state_intervals
                interval_scope = f"intervals={len(interval_specs)}(current)"

            if not symbol_specs:
                fail_fast(
                    "[yellow]当前没有已选 symbols。请先在目录选择里选择 symbols，或在此处输入一个 symbol；"
                    "也可勾选「symbol 留空时扫描该资产类型全部 symbols」。[/yellow]"
                )
                return
            if not interval_specs:
                fail_fast(
                    "[yellow]当前没有已选 intervals。请先在配置里选择 intervals，或在此处输入一个 interval；"
                    "也可勾选「interval 留空时扫描全部 intervals」。[/yellow]"
                )
                return

            scan_specs: list[tuple[str | None, str | None]] = []
            for sym in symbol_specs:
                for itv in interval_specs:
                    scan_specs.append((sym, itv))

            log_line(
                f"[bold]开始检查[/bold] data_root={data_root}  asset_type={asset_type}  "
                f"{symbol_scope}  {interval_scope}"
            )
            # ── Phase 1: 收集文件列表（去重）──
            log_line("[cyan]扫描文件列表…[/cyan]")
            update_summary(0)
            all_files: list[Path] = []
            for sym, itv in scan_specs:
                sym_s = sym or "*"
                itv_s = itv or "*"
                log_line(f"[dim]扫描范围：symbol={sym_s}  interval={itv_s}[/dim]")
                for p in iter_parquet_files(Path(data_root), asset_type=asset_type, symbol=sym, interval=itv):
                    ps = str(p.resolve())
                    if ps in seen:
                        continue
                    seen.add(ps)
                    all_files.append(p)

            total_files = len(all_files)
            if total_files == 0:
                log_line("[yellow]未找到匹配的 parquet 文件。[/yellow]")
                update_summary(0)
            else:
                import os
                import threading

                workers = min(os.cpu_count() or 4, total_files, 32)
                log_line(f"[cyan]开始并行验证：{total_files} 文件 × {workers} 线程[/cyan]")

                # ── Phase 2: 并行验证 ──
                # 回调仅收集数据，不做任何 UI 调用（避免 call_from_thread 死锁）
                per_file_results: list[tuple[FileSummary, list]] = []
                _result_lock = threading.Lock()
                _completed_count = 0

                def _on_result(
                    file_summary: FileSummary,
                    file_issues: list,
                    progress: BatchProgress,
                ) -> None:
                    nonlocal _completed_count
                    with _result_lock:
                        per_file_results.append((file_summary, file_issues))
                        _completed_count = progress.completed

                validate_batch(all_files, max_workers=workers, on_result=_on_result)

                # ── Phase 3: 批量刷新 UI（安全地在 worker 线程逐条调用 call_from_thread）──
                for file_summary, file_issues in per_file_results:
                    summaries.append(file_summary)
                    if file_issues:
                        issue_files += 1
                        issues.extend(file_issues)

                    min_dt = file_summary.min_dt.isoformat() if file_summary.min_dt else "?"
                    max_dt = file_summary.max_dt.isoformat() if file_summary.max_dt else "?"
                    gap = str(file_summary.max_gap) if file_summary.max_gap else "?"
                    fname = file_summary.path.name if hasattr(file_summary.path, "name") else str(file_summary.path)
                    log_line(
                        f"\n[bold]{fname}[/bold]\n"
                        f"  rows={file_summary.row_count}  dt=[{min_dt} ~ {max_dt}]  "
                        f"dup_dt={file_summary.duplicate_datetime_count}  max_gap={gap}\n"
                    )
                    for iss in file_issues:
                        sev = iss.severity.value if isinstance(iss.severity, Severity) else str(iss.severity)
                        details = f" ({iss.details})" if iss.details else ""
                        color = "red" if sev == "ERROR" else "yellow"
                        log_line(f"  [{color}]{sev}[/{color}] {iss.rule_id}: {iss.message}{details}")

            def _finish_ok() -> None:
                self._last_issues = issues
                self._last_file_summaries = summaries
                err = sum(1 for i in issues if i.severity == Severity.ERROR)
                warn = sum(1 for i in issues if i.severity == Severity.WARN)
                dt = (datetime.now() - t0).total_seconds()
                if total_files == 0:
                    try:
                        self.query_one("#validate_log", RichLog).write(
                            "[yellow]未找到任何匹配的 parquet 文件。[/yellow]\n"
                            "请检查：data_root 是否正确；asset_type/symbol/interval 是否匹配目录与文件名；"
                            "或勾选“扫描全部”。"
                        )
                    except Exception:
                        pass
                try:
                    self.query_one("#validate_summary", Static).update(
                        f"完成：files={total_files}  issue_files={issue_files}  ERROR={err}  WARN={warn}  耗时={dt:.1f}s"
                    )
                except Exception:
                    pass
                self._validating = False
                self._set_controls_running(False)

            ui(_finish_ok)
        except Exception as exc:
            def _finish_fail() -> None:
                try:
                    self.query_one("#validate_summary", Static).update(f"[red]检查失败：{exc}[/red]")
                except Exception:
                    pass
                self._validating = False
                self._set_controls_running(False)

            ui(_finish_fail)

    def _export(self, fmt: str) -> None:
        issues = list(getattr(self, "_last_issues", []) or [])
        if not issues:
            self.query_one("#validate_summary", Static).update("[yellow]暂无 issues 可导出（先运行检查）。[/yellow]")
            return
        root = getattr(self, "_last_data_root", None) or getattr(self.app.config, "data_root_path", None)
        if not root:
            try:
                dr = getattr(self.app.config.storage, "data_root", "") or ""
                root = Path(str(dr)).resolve() if str(dr).strip() else None
            except Exception:
                root = None
        if not root:
            self.query_one("#validate_summary", Static).update("[red]未设置 data_root，无法导出。[/red]")
            return
        ts = _timestamp_for_filename()
        out = Path(root) / f"quality_issues_{ts}.{fmt}"
        try:
            if fmt == "jsonl":
                write_issues_jsonl(out, issues)
            else:
                write_issues_csv(out, issues)
            self.query_one("#validate_summary", Static).update(f"已导出：{out}")
        except Exception as exc:
            self.query_one("#validate_summary", Static).update(f"[red]导出失败：{exc}[/red]")
