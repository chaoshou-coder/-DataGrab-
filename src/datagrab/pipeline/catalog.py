from __future__ import annotations

import contextlib
import csv
import io
import re
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path
from typing import Callable

from ..config import CatalogConfig, FilterConfig, YFinanceConfig
from ..fsutils import atomic_write_text, ensure_dir
from ..logging import get_logger
from ..sources.base import SymbolInfo

# 分步进度回调：(step_id, status, detail)。status: "start"|"done"|"progress"，detail 可选如 "500/8000"
ProgressCallback = Callable[[str, str, str | None], None]


# 美股列表：官方已从 ftp.nasdaqtrader.com 迁移到 www.nasdaqtrader.com（ftp 子域多地区不可达）
NASDAQ_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://www.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"

# Yahoo Finance screener API — 用于动态获取加密货币、外汇、商品期货目录
_YAHOO_SCREENER_URL = "https://query2.finance.yahoo.com/v1/finance/screener/predefined/saved"
_YAHOO_SCREENER_IDS: dict[str, str] = {
    "crypto": "all_cryptocurrencies_us",
    "forex": "most_actives_currencies",
    "commodity": "most_actives_futures",
}


@dataclass(frozen=True)
class CatalogResult:
    items: list[SymbolInfo]
    source: str
    """筛选后总条数（截断 limit 前）；与 len(items) 一致当 limit 为 None 时"""
    total_count: int = 0
    """从本次目录数据中提取的交易所/板块/基金子类选项（非硬编码）"""
    exchange_options: list[tuple[str, str]] = ()
    market_options: list[tuple[str, str]] = ()
    fund_options: list[tuple[str, str]] = ()


EXCHANGE_ALIAS_TO_CODE = {
    "上交所": "SSE",
    "上海证券交易所": "SSE",
    "深交所": "SZSE",
    "深圳证券交易所": "SZSE",
    "北交所": "BSE",
    "北京证券交易所": "BSE",
    "纳斯达克": "NASDAQ",
    "纽交所": "NYSE",
    # 美股 otherlisted 单字母代码（展示用英文，此处供筛选归一化）
    "NYSE": "N",
    "AMEX": "A",
    "NYSE American": "A",
    "NYSE Arca": "P",
    "NYSE ARCA": "P",
    "BATS": "Z",
    "IEX": "V",
    "NYSE MKT": "M",
}

# A 股市场层级 + 美股 GICS 行业（美股板块用行业，非 NASDAQ 市场层级 Q/G/S）
MARKET_ALIAS_TO_CODE = {
    "主板": "MAIN",
    "科创板": "STAR",
    "创业板": "CHINEXT",
    "北交所": "BSE",
    "b股": "B",
    "b股市场": "B",
    "纳斯达克全球精选": "Q",
    "全球精选": "Q",
    "纳斯达克全球市场": "G",
    "全球市场": "G",
    "纳斯达克资本市场": "S",
    "资本市场": "S",
    # GICS 11 行业（美股板块），含 Yahoo 常见变体
    "energy": "Energy",
    "materials": "Materials",
    "industrials": "Industrials",
    "consumer discretionary": "Consumer Discretionary",
    "consumer cyclical": "Consumer Discretionary",
    "consumer staples": "Consumer Staples",
    "consumer defensive": "Consumer Staples",
    "health care": "Health Care",
    "healthcare": "Health Care",
    "financials": "Financials",
    "financial services": "Financials",
    "information technology": "Information Technology",
    "technology": "Information Technology",
    "communication services": "Communication Services",
    "utilities": "Utilities",
    "real estate": "Real Estate",
}

MARKET_CODE_TO_ALIAS = {
    "MAIN": "主板",
    "STAR": "科创板",
    "CHINEXT": "创业板",
    "BSE": "北交所",
    "B": "B股",
    "Q": "纳斯达克全球精选",
    "G": "纳斯达克全球市场",
    "S": "纳斯达克资本市场",
    # GICS 行业（美股板块展示）
    "Energy": "Energy",
    "Materials": "Materials",
    "Industrials": "Industrials",
    "Consumer Discretionary": "Consumer Discretionary",
    "Consumer Staples": "Consumer Staples",
    "Health Care": "Health Care",
    "Financials": "Financials",
    "Information Technology": "Information Technology",
    "Communication Services": "Communication Services",
    "Utilities": "Utilities",
    "Real Estate": "Real Estate",
}

# A 股用中文展示；美股用英文（N/A/P/Z/V/M 为 otherlisted.txt 单字母代码）
EXCHANGE_CODE_TO_ALIAS = {
    "SSE": "上交所",
    "SZSE": "深交所",
    "BSE": "北交所",
    "NASDAQ": "NASDAQ",
    "NYSE": "NYSE",
    "N": "NYSE",
    "A": "AMEX",
    "P": "NYSE Arca",
    "Z": "BATS",
    "V": "IEX",
    "M": "NYSE MKT",
}

FUND_CATEGORY_ALIAS_TO_CODE = {
    "ETF": "ETF",
    "etf": "ETF",
    "LOF": "LOF",
    "lof": "LOF",
    "REIT": "REIT",
    "REITS": "REIT",
    "reits": "REIT",
    "QDII": "QDII",
    "qdii": "QDII",
    "货币": "MONEY",
    "货币基金": "MONEY",
    "债券": "BOND",
    "债券基金": "BOND",
    "联接": "ETF_LINK",
    "联结": "ETF_LINK",
    "ETF联接": "ETF_LINK",
    "ETF联结": "ETF_LINK",
    "分级": "GRADED",
    "基金": "FUND",
}

FUND_CATEGORY_CODE_TO_ALIAS = {
    "ETF": "ETF",
    "LOF": "LOF",
    "REIT": "REITs",
    "QDII": "QDII",
    "MONEY": "货币基金",
    "BOND": "债券基金",
    "ETF_LINK": "ETF联接",
    "GRADED": "分级基金",
    "FUND": "基金",
}


def _numeric_to_baostock_code(code_num: str) -> str | None:
    """将纯数字代码 (如 '600000') 转为 baostock 格式 (如 'sh.600000')。

    映射规则：
      6xxxxx / 9xxxxx / 5xxxxx → sh  (上交所：主板/B股/ETF&基金)
      688xxx                   → sh  (科创板)
      0xxxxx / 3xxxxx / 2xxxxx / 1xxxxx → sz  (深交所：主板/创业板/B股/ETF&基金)
      4xxxxx / 8xxxxx          → bj  (北交所)
    """
    c = code_num.strip()
    if not c or not c.isdigit():
        return None
    if c.startswith(("6", "9", "5")):
        return f"sh.{c}"
    if c.startswith(("0", "3", "2", "1")):
        return f"sz.{c}"
    if c.startswith(("4", "8")):
        return f"bj.{c}"
    return None


def classify_ashare_code(code: str) -> tuple[str | None, str | None]:
    code = code.strip().lower()
    if "." in code:
        prefix, num = code.split(".", 1)
    else:
        prefix, num = "", code
    exchange = None
    market = None
    if prefix == "sh":
        exchange = "SSE"
        if num.startswith("688"):
            market = "STAR"
        elif num.startswith("900"):
            market = "B"
        else:
            market = "MAIN"
    elif prefix == "sz":
        exchange = "SZSE"
        if num.startswith("300"):
            market = "CHINEXT"
        elif num.startswith("200"):
            market = "B"
        else:
            market = "MAIN"
    elif prefix == "bj":
        exchange = "BSE"
        market = "BSE"
    return exchange, market


def classify_ashare_security(
    code: str,
    name: str | None,
    market_category: str | None = None,
) -> tuple[bool | None, bool | None, str | None]:
    """基于代码/名称对 A 股标的做粗分类（ETF/基金子类）。

    返回：(is_etf, is_fund, fund_category)

    说明：
    - 该分类用于筛选与展示，采用启发式规则；不依赖联网查询。
    - fund_category 取值见 FUND_CATEGORY_CODE_TO_ALIAS 的 key（如 ETF/LOF/REIT/ETF_LINK...）。
    """
    code = (code or "").strip().lower()
    name_raw = (name or "").strip()
    name_upper = name_raw.upper()

    # 1) 优先识别“ETF 联接”
    if "联接" in name_raw or "ETF_LINK" in name_upper:
        return False, True, "ETF_LINK"

    # 2) 子类关键词
    if "REIT" in name_upper:
        return False, True, "REIT"
    if "LOF" in name_upper:
        return False, True, "LOF"
    if "QDII" in name_upper:
        return False, True, "QDII"

    # 3) ETF：名称包含 ETF 或代码前缀命中（兜底）
    is_etf: bool | None = None
    if "ETF" in name_upper:
        is_etf = True
    else:
        # 代码启发：sh.51xxxx、sz.15xxxx、sz.159xxx 常见为 ETF/ETF 类
        num = code.split(".", 1)[1] if "." in code else code
        if num.startswith(("51", "15", "159")):
            is_etf = True

    # 4) fund：只要像基金就算（ETF 也是基金的一种）
    is_fund: bool | None = None
    if is_etf is True:
        is_fund = True
        return True, True, "ETF"

    # 5) 其他基金兜底：名称含“基金”/“货币”/“债券”等
    if any(k in name_raw for k in ("基金", "货币", "债券", "债基")):
        is_fund = True
        if "货币" in name_raw:
            return False, True, "MONEY"
        if "债" in name_raw:
            return False, True, "BOND"
        return False, True, "FUND"

    # market_category 暂不参与判断（保留未来扩展）
    _ = market_category
    return is_etf, is_fund, None


def normalize_exchange_value(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if raw in EXCHANGE_ALIAS_TO_CODE:
        return EXCHANGE_ALIAS_TO_CODE[raw]
    return raw.upper()


def normalize_market_value(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    key = raw.lower()
    if key in MARKET_ALIAS_TO_CODE:
        return MARKET_ALIAS_TO_CODE[key]
    return raw.upper()


def normalize_fund_category(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    if raw in FUND_CATEGORY_ALIAS_TO_CODE:
        return FUND_CATEGORY_ALIAS_TO_CODE[raw]
    key = raw.upper()
    return FUND_CATEGORY_ALIAS_TO_CODE.get(key, key)


def market_alias(value: str | None) -> str | None:
    if not value:
        return None
    return MARKET_CODE_TO_ALIAS.get(value.upper())


def exchange_alias(value: str | None) -> str | None:
    if not value:
        return None
    return EXCHANGE_CODE_TO_ALIAS.get(value.upper())


def fund_category_alias(value: str | None) -> str | None:
    if not value:
        return None
    return FUND_CATEGORY_CODE_TO_ALIAS.get(value.upper())


def filter_options_from_items(
    items: list[SymbolInfo],
) -> tuple[
    list[tuple[str, str]],
    list[tuple[str, str]],
    list[tuple[str, str]],
]:
    """从目录条目中提取交易所/板块/基金子类选项，避免硬编码。"""
    exchanges = sorted(set(item.exchange for item in items if item.exchange))
    markets = sorted(set(item.market_category for item in items if item.market_category))
    funds = sorted(set(item.fund_category for item in items if item.fund_category))
    ex_opts = [(EXCHANGE_CODE_TO_ALIAS.get(e, e), e) for e in exchanges]
    mkt_opts = [(MARKET_CODE_TO_ALIAS.get(m, m), m) for m in markets]
    fund_opts = [(FUND_CATEGORY_CODE_TO_ALIAS.get(f, f), f) for f in funds]
    ex_opts.sort(key=lambda x: x[0])
    mkt_opts.sort(key=lambda x: x[0])
    fund_opts.sort(key=lambda x: x[0])
    return (ex_opts, mkt_opts, fund_opts)


class CatalogService:
    def __init__(
        self,
        data_root: Path,
        config: CatalogConfig,
        filters: FilterConfig,
        *,
        yfinance_config: YFinanceConfig | None = None,
    ):
        self.data_root = Path(data_root)
        self.config = config
        self.filters = filters
        self._yfinance_proxy = (yfinance_config.proxy or "").strip() if yfinance_config else None
        self.logger = get_logger("datagrab.catalog")

    def set_data_root(self, data_root: Path) -> None:
        self.data_root = Path(data_root)

    def _result_with_options(
        self,
        items_full: list[SymbolInfo],
        total: int,
        source: str,
        limit: int | None,
    ) -> CatalogResult:
        ex_opts, mkt_opts, fund_opts = filter_options_from_items(items_full)
        items_return = items_full[:limit] if limit is not None else items_full
        return CatalogResult(
            items_return,
            source,
            total_count=total,
            exchange_options=ex_opts,
            market_options=mkt_opts,
            fund_options=fund_opts,
        )

    def get_catalog(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
        progress_callback: ProgressCallback | None = None,
    ) -> CatalogResult:
        def _progress(step: str, status: str, detail: str | None = None) -> None:
            if progress_callback:
                progress_callback(step, status, detail)

        filters = filters_override or self.filters
        cache_path = self._cache_path(asset_type)
        if not refresh:
            cached = self._load_cache(cache_path)
            if cached:
                items_full = self._apply_filters(cached, None, filters)
                total = len(items_full)
                _progress("cache", "done", str(len(items_full)))
                return self._result_with_options(items_full, total, "cache", limit)
        self.logger.info("catalog: 正在远程拉取 %s ...", asset_type)
        fetched, last_error = self._fetch_with_retry(asset_type, progress_callback=_progress)
        if fetched:
            _progress("write_cache", "start", str(len(fetched)))
            self.logger.info("catalog: 拉取成功，正在写入缓存 %s (%d 条) ...", asset_type, len(fetched))
            self._write_cache(cache_path, fetched)
            _progress("write_cache", "done", None)
            items_full = self._apply_filters(fetched, None, filters)
            total = len(items_full)
            return self._result_with_options(items_full, total, "remote", limit)
        cached = self._load_cache(cache_path)
        if cached:
            items_full = self._apply_filters(cached, None, filters)
            total = len(items_full)
            return self._result_with_options(items_full, total, "cache-fallback", limit)
        if asset_type == "stock":
            fallback = self._static_stock_catalog()
            if fallback:
                self._write_cache(cache_path, fallback)
                self.logger.info(
                    "stock catalog: using built-in list (%d symbols). "
                    "Run with --refresh when network can access NASDAQ for full list.",
                    len(fallback),
                )
                items_full = self._apply_filters(fallback, None, filters)
                total = len(items_full)
                return self._result_with_options(items_full, total, "static-fallback", limit)
        msg = f"no catalog available for {asset_type}"
        if last_error:
            msg += f". 拉取失败原因: {last_error}"
        if asset_type == "stock":
            msg += " (美股列表需访问 NASDAQ 列表源，若在国内可配置代理后再尝试 --refresh)"
        raise RuntimeError(msg)

    def _fetch_with_retry(
        self, asset_type: str, progress_callback: ProgressCallback | None = None
    ) -> tuple[list[SymbolInfo] | None, str | None]:
        """Returns (fetched_list or None, last_error_message or None)."""
        def _progress(step: str, status: str, detail: str | None = None) -> None:
            if progress_callback:
                progress_callback(step, status, detail)

        last_error: str | None = None
        delay = self.config.sleep_sec
        for attempt in range(self.config.retries + 1):
            try:
                if asset_type == "stock":
                    return (self._fetch_stock_catalog(_progress), None)
                if asset_type == "ashare":
                    _progress("fetch_ashare_stock", "start", None)
                    out = self._fetch_ashare_catalog()
                    n_etf = sum(1 for i in out if i.is_etf)
                    n_stock = len(out) - n_etf
                    _progress("fetch_ashare_stock", "done", str(n_stock))
                    if n_etf > 0:
                        _progress("fetch_ashare_etf", "done", str(n_etf))
                    return (out, None)
                # 加密/外汇/商品：先尝试从 Yahoo Finance screener 动态拉取
                if asset_type in _YAHOO_SCREENER_IDS:
                    _progress("fetch", "start", None)
                    items = self._fetch_yahoo_screener(asset_type)
                    if items:
                        _progress("fetch", "done", str(len(items)))
                        return (items, None)
                    out = self._static_catalog(asset_type)
                    _progress("fetch", "done", str(len(out)))
                    return (out, None)
                # 动态拉取失败则使用内置最小列表
                _progress("fetch", "start", None)
                out = self._static_catalog(asset_type)
                _progress("fetch", "done", str(len(out)))
                return (out, None)
            except Exception as exc:
                last_error = str(exc)
                self.logger.warning("catalog fetch failed: %s", exc)
                time.sleep(delay)
                delay *= self.config.retry_backoff
        return (None, last_error)

    def _fetch_yahoo_screener(self, asset_type: str) -> list[SymbolInfo]:
        """从 Yahoo Finance screener API 动态获取加密货币/外汇/商品期货目录。"""
        import httpx

        scr_id = _YAHOO_SCREENER_IDS.get(asset_type)
        if not scr_id:
            return []
        headers = {
            "User-Agent": "Mozilla/5.0",
        }
        items: list[SymbolInfo] = []
        try:
            client_kwargs = {"timeout": 15.0, "headers": headers}
            if self._yfinance_proxy:
                client_kwargs["proxies"] = {
                    "http://": self._yfinance_proxy,
                    "https://": self._yfinance_proxy,
                }
            with httpx.Client(**client_kwargs) as client:
                resp = client.get(
                    _YAHOO_SCREENER_URL,
                    params={"scrIds": scr_id, "count": 250},
                )
                resp.raise_for_status()
                data = resp.json()
        except Exception as exc:
            self.logger.warning(
                "yahoo screener fetch failed for %s (scrId=%s): %s",
                asset_type,
                scr_id,
                exc,
            )
            return []

        results = (data.get("finance") or {}).get("result") or []
        if not results:
            self.logger.warning("yahoo screener returned empty for %s", asset_type)
            return []
        quotes = results[0].get("quotes") or []
        for q in quotes:
            symbol = (q.get("symbol") or "").strip()
            if not symbol:
                continue
            name = (
                q.get("shortName")
                or q.get("longName")
                or q.get("displayName")
                or ""
            ).strip() or None
            exchange = (q.get("exchange") or "").strip() or None
            items.append(
                SymbolInfo(
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    asset_type=asset_type,
                )
            )
        if items:
            self.logger.info(
                "yahoo screener fetched %d symbols for %s", len(items), asset_type
            )
        return items

    def _check_stock_catalog_reachable(self) -> None:
        """拉取前先检测能否访问 NASDAQ 列表地址，便于用户排查网络/代理。"""
        import httpx

        host = "www.nasdaqtrader.com"
        url = NASDAQ_LISTED_URL
        self.logger.info("stock catalog: 正在检测 %s 连通性...", host)
        # VPN/跨境时 SSL 握手较慢，给足超时（25s）
        try:
            client_kwargs = {"timeout": 25.0}
            if self._yfinance_proxy:
                client_kwargs["proxies"] = {
                    "http://": self._yfinance_proxy,
                    "https://": self._yfinance_proxy,
                }
            with httpx.Client(**client_kwargs) as client:
                resp = client.get(url)
                resp.raise_for_status()
        except Exception as exc:
            self.logger.warning("stock catalog unreachable: %s", exc)
            detail = f"{type(exc).__name__}: {exc}"
            hint = ""
            if "handshake" in str(exc).lower() or "ConnectTimeout" in type(exc).__name__:
                hint = "SSL 握手超时多为 VPN/跨境延迟导致，程序已使用较长超时；可稍后重试或换节点。"
            else:
                hint = "能 ping 通但下载失败通常表示仅放行 ICMP 未放行 HTTPS，或程序未走代理；请检查代理/防火墙。"
            raise RuntimeError(
                f"无法连接 {host}（美股列表地址）。{hint} 详细错误: {detail}"
            ) from exc
        self.logger.info("stock catalog: 连通性正常，开始下载列表")

    def _fetch_stock_catalog(self, progress_callback: ProgressCallback | None = None) -> list[SymbolInfo]:
        def _progress(step: str, status: str, detail: str | None = None) -> None:
            if progress_callback:
                progress_callback(step, status, detail)

        self.logger.info("stock catalog: 开始拉取美股列表（检测连通性 -> 下载列表 -> 写入缓存）")
        _progress("reachability", "start", None)
        self._check_stock_catalog_reachable()
        _progress("reachability", "done", None)
        items = []
        _progress("download_nasdaq", "start", None)
        self.logger.info("stock catalog: 正在下载 nasdaqlisted.txt ...")
        text1 = self._download_text(NASDAQ_LISTED_URL)
        items.extend(self._parse_pipe_catalog(text1, "Symbol"))
        _progress("download_nasdaq", "done", str(len(items)))
        _progress("download_other", "start", None)
        self.logger.info("stock catalog: 正在下载 otherlisted.txt ...")
        text2 = self._download_text(OTHER_LISTED_URL)
        items.extend(self._parse_pipe_catalog(text2, "ACT Symbol"))
        deduped = {}
        for item in items:
            if item.symbol not in deduped:
                deduped[item.symbol] = item
        result = list(deduped.values())
        _progress("download_other", "done", str(len(result)))
        # 逐只 sector enrichment 已移除：对 8000+ 美股逐只 HTTP 查询行业信息耗时非常高。
        # 板块筛选改用 NASDAQ 原始 Market Category (Q/G/S) 及 exchange 字段。
        self.logger.info("stock catalog: 美股目录拉取完成，共 %d 只", len(result))
        return result

    def _download_text(self, url: str) -> str:
        import httpx

        # VPN/跨境时连接与下载均较慢，使用较长超时
        client_kwargs = {"timeout": 30.0}
        if self._yfinance_proxy:
            client_kwargs["proxies"] = {
                "http://": self._yfinance_proxy,
                "https://": self._yfinance_proxy,
            }
        with httpx.Client(**client_kwargs) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.text

    # ── A 股 ETF 代码前缀（确定性规则，不靠名称猜测）──────────
    _ETF_CODE_PREFIXES = (
        "sh.510", "sh.511", "sh.512", "sh.513", "sh.515", "sh.516",
        "sh.517", "sh.518", "sh.560", "sh.561", "sh.562", "sh.563",
        "sh.588",
        "sz.159",
    )

    def _fetch_ashare_catalog(self) -> list[SymbolInfo]:
        """获取 A 股 symbol 列表。

        策略：
        1. akshare stock_info_a_code_name() → 纯股票（高质量名称）
        2. baostock query_all_stock()       → 补充 ETF（akshare 股票列表不含 ETF）
        3. ETF 识别：代码前缀规则（sh.510*/sh.588*/sz.159* 等），确定性映射
        4. 若 akshare 整体失败 → baostock 全量兜底
        symbol 格式统一为 baostock 风格 (sh.600000) 以兼容行情下载。
        """
        # ── 尝试 akshare 获取股票 ─────────────────────────────
        stock_items = self._fetch_ashare_via_akshare()
        if stock_items:
            # akshare 股票成功，从 baostock 补充 ETF
            seen = {item.symbol for item in stock_items}
            etf_items = self._fetch_ashare_etf_via_baostock(seen)
            self.logger.info(
                "A-share catalog: %d stocks (akshare) + %d ETFs (baostock) = %d total",
                len(stock_items), len(etf_items), len(stock_items) + len(etf_items),
            )
            return stock_items + etf_items

        # ── akshare 失败，baostock 全量兜底 ───────────────────
        self.logger.warning("akshare 获取 A 股列表失败，回退至 baostock 全量")
        return self._fetch_ashare_via_baostock()

    def _fetch_ashare_via_akshare(self) -> list[SymbolInfo]:
        """通过 akshare 获取 A 股列表（含重试）。成功返回列表，失败返回空列表。"""
        try:
            import akshare as ak
        except ImportError:
            self.logger.warning("akshare not installed, skipping")
            return []

        max_attempts = 3
        for attempt in range(max_attempts):
            try:
                stock_df = ak.stock_info_a_code_name()
                if stock_df is None or stock_df.empty:
                    self.logger.warning("akshare stock_info_a_code_name returned empty (attempt %d)", attempt + 1)
                    time.sleep(2 * (attempt + 1))
                    continue

                items: list[SymbolInfo] = []
                seen: set[str] = set()
                for _, row in stock_df.iterrows():
                    code_num = str(row.get("code", "")).strip()
                    name = str(row.get("name", "")).strip() or None
                    if not code_num:
                        continue
                    bs_code = _numeric_to_baostock_code(code_num)
                    if not bs_code or bs_code in seen:
                        continue
                    seen.add(bs_code)
                    exchange, market = classify_ashare_code(bs_code)
                    is_etf = bs_code.startswith(self._ETF_CODE_PREFIXES)
                    items.append(
                        SymbolInfo(
                            symbol=bs_code,
                            name=name,
                            exchange=exchange,
                            asset_type="ashare",
                            market_category=market,
                            is_etf=is_etf,
                            is_fund=is_etf,
                            fund_category="ETF" if is_etf else None,
                        )
                    )

                if items:
                    n_etf = sum(1 for i in items if i.is_etf)
                    self.logger.info(
                        "akshare A-share catalog: %d total (%d stocks, %d ETFs)",
                        len(items), len(items) - n_etf, n_etf,
                    )
                    return items

            except Exception as exc:
                self.logger.warning("akshare attempt %d failed: %s", attempt + 1, exc)
                if attempt < max_attempts - 1:
                    time.sleep(2 * (attempt + 1))

        return []

    def _fetch_ashare_etf_via_baostock(self, seen: set[str]) -> list[SymbolInfo]:
        """从 baostock query_all_stock 中提取 ETF（代码前缀过滤），按天数回退最多 7 天，跳过 seen 中已有的。"""
        try:
            import baostock as bs
        except ImportError:
            self.logger.warning("baostock not installed, skipping ETF supplement")
            return []

        today = datetime.now().date()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            login = bs.login()
        if login.error_code != "0":
            self.logger.warning("baostock login failed for ETF supplement: %s", login.error_msg)
            return []

        etf_items: list[SymbolInfo] = []
        try:
            for i in range(7):
                day = today - timedelta(days=i)
                day_str = day.strftime("%Y-%m-%d")
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    rs = bs.query_all_stock(day=day_str)
                if rs.error_code != "0":
                    continue
                df = rs.get_data()
                if df is None or df.empty:
                    continue
                for _, row in df.iterrows():
                    code = str(row.get("code", "")).strip()
                    if not code or code in seen:
                        continue
                    if not code.startswith(self._ETF_CODE_PREFIXES):
                        continue
                    name = str(row.get("code_name", "")).strip() or None
                    exchange, market = classify_ashare_code(code)
                    etf_items.append(
                        SymbolInfo(
                            symbol=code,
                            name=name,
                            exchange=exchange,
                            asset_type="ashare",
                            market_category=market,
                            is_etf=True,
                            is_fund=True,
                            fund_category="ETF",
                        )
                    )
                self.logger.info("baostock ETF supplement: %d ETFs", len(etf_items))
                return etf_items
        except Exception as exc:
            self.logger.warning("baostock ETF supplement failed: %s", exc)
        finally:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                bs.logout()
        return etf_items

    def _fetch_ashare_via_baostock(self) -> list[SymbolInfo]:
        """通过 baostock 获取 A 股全量列表（兜底方案）。"""
        try:
            import baostock as bs
            import pandas as pd  # noqa: F401
        except ImportError as exc:
            raise RuntimeError("baostock is required for ashare catalog fallback") from exc

        today = datetime.now().date()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"baostock login failed: {login.error_msg}")

        try:
            for i in range(7):
                day = today - timedelta(days=i)
                day_str = day.strftime("%Y-%m-%d")
                with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                    rs = bs.query_all_stock(day=day_str)
                if rs.error_code != "0":
                    continue
                df = rs.get_data()
                if df is None or df.empty:
                    continue
                items: list[SymbolInfo] = []
                for _, row in df.iterrows():
                    code = str(row.get("code", "")).strip()
                    if not code:
                        continue
                    name = str(row.get("code_name", "")).strip() or None
                    exchange, market = classify_ashare_code(code)
                    is_etf = code.startswith(self._ETF_CODE_PREFIXES)
                    items.append(
                        SymbolInfo(
                            symbol=code,
                            name=name,
                            exchange=exchange,
                            asset_type="ashare",
                            market_category=market,
                            is_etf=is_etf,
                            is_fund=is_etf,
                            fund_category="ETF" if is_etf else None,
                        )
                    )
                self.logger.info("baostock A-share catalog (fallback): %d items", len(items))
                return items
        finally:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                bs.logout()
        return []

    def _parse_pipe_catalog(self, text: str, symbol_key: str) -> list[SymbolInfo]:
        reader = csv.DictReader(io.StringIO(text), delimiter="|")
        items: list[SymbolInfo] = []
        for row in reader:
            symbol = (row.get(symbol_key) or "").strip()
            if not symbol or symbol.upper().startswith("FILE CREATION"):
                continue
            name = (row.get("Security Name") or row.get("SecurityName") or "").strip() or None
            market_category = (row.get("Market Category") or "").strip() or None
            exchange = (row.get("Exchange") or "").strip() or None
            if exchange is None and market_category is not None:
                exchange = "NASDAQ"
            etf_flag = (row.get("ETF") or "").strip().upper()
            is_etf = True if etf_flag == "Y" else False if etf_flag else None
            items.append(
                SymbolInfo(
                    symbol=symbol,
                    name=name,
                    exchange=exchange,
                    asset_type="stock",
                    market_category=market_category,
                    is_etf=is_etf,
                )
            )
        return items

    def _static_stock_catalog(self) -> list[SymbolInfo]:
        """仅当美股远程与缓存均不可用时使用的兜底列表（最小集），写入缓存后可供下载。谨慎扩展。"""
        presets: list[tuple[str, str]] = [
            ("AAPL", "Apple"),
            ("MSFT", "Microsoft"),
            ("GOOGL", "Alphabet"),
            ("AMZN", "Amazon"),
            ("NVDA", "NVIDIA"),
            ("META", "Meta"),
            ("TSLA", "Tesla"),
            ("BRK-B", "Berkshire Hathaway"),
            ("JPM", "JPMorgan Chase"),
            ("V", "Visa"),
            ("JNJ", "Johnson & Johnson"),
            ("WMT", "Walmart"),
            ("PG", "Procter & Gamble"),
            ("MA", "Mastercard"),
            ("HD", "Home Depot"),
            ("DIS", "Walt Disney"),
            ("PYPL", "PayPal"),
            ("BAC", "Bank of America"),
            ("XOM", "Exxon Mobil"),
            ("UNH", "UnitedHealth"),
            ("SPY", "SPDR S&P 500 ETF"),
            ("QQQ", "Invesco QQQ Trust"),
        ]
        return [
            SymbolInfo(symbol=sym, name=name, exchange=None, asset_type="stock")
            for sym, name in presets
        ]

    def _static_catalog(self, asset_type: str) -> list[SymbolInfo]:
        """仅当 Yahoo screener 与缓存均不可用时使用的兜底列表（最小集）。谨慎扩展。"""
        presets: dict[str, list[tuple[str, str]]] = {
            "crypto": [
                ("BTC-USD", "Bitcoin"),
                ("ETH-USD", "Ethereum"),
                ("SOL-USD", "Solana"),
                ("BNB-USD", "BNB"),
                ("XRP-USD", "XRP"),
                ("ADA-USD", "Cardano"),
                ("DOGE-USD", "Dogecoin"),
                ("AVAX-USD", "Avalanche"),
                ("DOT-USD", "Polkadot"),
                ("MATIC-USD", "Polygon"),
                ("LINK-USD", "Chainlink"),
                ("UNI7083-USD", "Uniswap"),
                ("LTC-USD", "Litecoin"),
                ("ATOM-USD", "Cosmos"),
            ],
            "forex": [
                ("EURUSD=X", "EUR/USD"),
                ("USDJPY=X", "USD/JPY"),
                ("GBPUSD=X", "GBP/USD"),
                ("AUDUSD=X", "AUD/USD"),
                ("USDCAD=X", "USD/CAD"),
                ("USDCHF=X", "USD/CHF"),
                ("NZDUSD=X", "NZD/USD"),
                ("EURGBP=X", "EUR/GBP"),
                ("EURJPY=X", "EUR/JPY"),
                ("GBPJPY=X", "GBP/JPY"),
                ("USDCNY=X", "USD/CNY"),
                ("USDHKD=X", "USD/HKD"),
            ],
            "commodity": [
                ("GC=F", "Gold"),
                ("CL=F", "Crude Oil WTI"),
                ("SI=F", "Silver"),
                ("HG=F", "Copper"),
                ("PL=F", "Platinum"),
                ("NG=F", "Natural Gas"),
                ("ZC=F", "Corn"),
                ("ZW=F", "Wheat"),
                ("ZS=F", "Soybeans"),
                ("KC=F", "Coffee"),
            ],
        }
        items = []
        for symbol, name in presets.get(asset_type, []):
            items.append(SymbolInfo(symbol=symbol, name=name, exchange=None, asset_type=asset_type))
        return items

    def _apply_filters(
        self, items: list[SymbolInfo], limit: int | None, filters: FilterConfig
    ) -> list[SymbolInfo]:
        include_regex = self._compile_patterns(filters.include_regex)
        exclude_regex = self._compile_patterns(filters.exclude_regex)
        include_name_regex = self._compile_patterns(filters.include_name_regex)
        exclude_name_regex = self._compile_patterns(filters.exclude_name_regex)
        include_prefixes = [p.upper() for p in filters.include_prefixes if p]
        exclude_prefixes = [p.upper() for p in filters.exclude_prefixes if p]
        include_symbols = {s.upper() for s in filters.include_symbols if s}
        exclude_symbols = {s.upper() for s in filters.exclude_symbols if s}
        include_exchanges = {normalize_exchange_value(s) for s in filters.include_exchanges if s}
        exclude_exchanges = {normalize_exchange_value(s) for s in filters.exclude_exchanges if s}
        include_markets = {normalize_market_value(s) for s in filters.include_market_categories if s}
        exclude_markets = {normalize_market_value(s) for s in filters.exclude_market_categories if s}
        include_fund_categories = {
            normalize_fund_category(s) for s in filters.include_fund_categories if s
        }
        exclude_fund_categories = {
            normalize_fund_category(s) for s in filters.exclude_fund_categories if s
        }

        def match(item: SymbolInfo) -> bool:
            symbol = item.symbol
            symbol_upper = symbol.upper()
            name = item.name or ""
            exchange_upper = normalize_exchange_value(item.exchange or "")
            market_upper = normalize_market_value(item.market_category or "")
            exchange_alias_value = exchange_alias(item.exchange)
            market_alias_value = market_alias(item.market_category)
            fund_category = normalize_fund_category(item.fund_category or "")
            fund_alias_value = fund_category_alias(item.fund_category)
            if include_symbols and symbol_upper not in include_symbols:
                return False
            if exclude_symbols and symbol_upper in exclude_symbols:
                return False
            if include_exchanges:
                if exchange_upper not in include_exchanges and (
                    exchange_alias_value is None or exchange_alias_value not in include_exchanges
                ):
                    return False
            if exclude_exchanges:
                if exchange_upper in exclude_exchanges or (
                    exchange_alias_value is not None and exchange_alias_value in exclude_exchanges
                ):
                    return False
            if include_markets:
                if market_upper not in include_markets and (
                    market_alias_value is None or market_alias_value not in include_markets
                ):
                    return False
            if exclude_markets:
                if market_upper in exclude_markets or (
                    market_alias_value is not None and market_alias_value in exclude_markets
                ):
                    return False
            if include_fund_categories:
                if fund_category not in include_fund_categories and (
                    fund_alias_value is None or fund_alias_value not in include_fund_categories
                ):
                    return False
            if exclude_fund_categories:
                if fund_category in exclude_fund_categories or (
                    fund_alias_value is not None and fund_alias_value in exclude_fund_categories
                ):
                    return False
            if include_prefixes and not any(symbol_upper.startswith(p) for p in include_prefixes):
                return False
            if exclude_prefixes and any(symbol_upper.startswith(p) for p in exclude_prefixes):
                return False
            if include_regex and not any(r.search(symbol) for r in include_regex):
                return False
            if exclude_regex and any(r.search(symbol) for r in exclude_regex):
                return False
            if include_name_regex and not any(r.search(name) for r in include_name_regex):
                return False
            if exclude_name_regex and any(r.search(name) for r in exclude_name_regex):
                return False
            if filters.only_etf is True and filters.only_fund is True:
                if not (item.is_etf or item.is_fund):
                    return False
            else:
                if filters.only_etf is True and not item.is_etf:
                    return False
                if filters.only_fund is True and not item.is_fund:
                    return False
            if filters.only_etf is False and item.is_etf:
                return False
            if filters.only_fund is False and item.is_fund:
                return False
            return True

        filtered = [item for item in items if match(item)]
        if limit is None:
            return filtered
        return filtered[: max(0, limit)]

    def _compile_patterns(self, patterns: list[str]) -> list[re.Pattern]:
        compiled: list[re.Pattern] = []
        for pat in patterns:
            try:
                compiled.append(re.compile(pat, re.IGNORECASE))
            except re.error:
                self.logger.warning("invalid regex ignored: %s", pat)
        return compiled

    def _cache_path(self, asset_type: str) -> Path:
        return self.data_root / "catalog" / f"{asset_type}_symbols.csv"

    def _load_cache(self, path: Path) -> list[SymbolInfo] | None:
        if not path.exists():
            return None
        items: list[SymbolInfo] = []
        with path.open("r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                symbol = (row.get("symbol") or "").strip()
                if not symbol:
                    continue
                etf_value = (row.get("is_etf") or "").strip().upper()
                if etf_value == "Y":
                    is_etf = True
                elif etf_value == "N":
                    is_etf = False
                else:
                    is_etf = None
                fund_value = (row.get("is_fund") or "").strip().upper()
                if fund_value == "Y":
                    is_fund = True
                elif fund_value == "N":
                    is_fund = False
                else:
                    is_fund = None
                items.append(
                    SymbolInfo(
                        symbol=symbol,
                        name=row.get("name") or None,
                        exchange=row.get("exchange") or None,
                        asset_type=row.get("asset_type") or "stock",
                        market_category=row.get("market_category") or None,
                        is_etf=is_etf,
                        is_fund=is_fund,
                        fund_category=row.get("fund_category") or None,
                    )
                )
        return items

    def _write_cache(self, path: Path, items: list[SymbolInfo]) -> None:
        ensure_dir(path.parent)
        out = io.StringIO()
        writer = csv.DictWriter(
            out,
            fieldnames=[
                "symbol",
                "name",
                "exchange",
                "asset_type",
                "market_category",
                "is_etf",
                "is_fund",
                "fund_category",
            ],
        )
        writer.writeheader()
        for item in items:
            writer.writerow(
                {
                    "symbol": item.symbol,
                    "name": item.name or "",
                    "exchange": item.exchange or "",
                    "asset_type": item.asset_type,
                    "market_category": item.market_category or "",
                    "is_etf": "" if item.is_etf is None else ("Y" if item.is_etf else "N"),
                    "is_fund": "" if item.is_fund is None else ("Y" if item.is_fund else "N"),
                    "fund_category": item.fund_category or "",
                }
            )
        atomic_write_text(path, out.getvalue())
