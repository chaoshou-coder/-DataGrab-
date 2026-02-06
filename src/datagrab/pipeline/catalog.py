from __future__ import annotations

import contextlib
import csv
import io
import random
import re
import time
from datetime import datetime, timedelta
from dataclasses import dataclass
from pathlib import Path

from ..config import CatalogConfig, FilterConfig
from ..fsutils import atomic_write_text, ensure_dir
from ..logging import get_logger
from ..sources.base import SymbolInfo


NASDAQ_LISTED_URL = "https://ftp.nasdaqtrader.com/dynamic/SymDir/nasdaqlisted.txt"
OTHER_LISTED_URL = "https://ftp.nasdaqtrader.com/dynamic/SymDir/otherlisted.txt"


@dataclass(frozen=True)
class CatalogResult:
    items: list[SymbolInfo]
    source: str


EXCHANGE_ALIAS_TO_CODE = {
    "上交所": "SSE",
    "上海证券交易所": "SSE",
    "深交所": "SZSE",
    "深圳证券交易所": "SZSE",
    "北交所": "BSE",
    "北京证券交易所": "BSE",
    "纳斯达克": "NASDAQ",
    "纽交所": "NYSE",
}

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
}

EXCHANGE_CODE_TO_ALIAS = {
    "SSE": "上交所",
    "SZSE": "深交所",
    "BSE": "北交所",
    "NASDAQ": "纳斯达克",
    "NYSE": "纽交所",
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
    code: str, name: str | None, type_value: str | None
) -> tuple[bool | None, bool | None, str | None]:
    is_etf: bool | None = None
    is_fund: bool | None = None
    fund_category: str | None = None
    type_str = (type_value or "").strip().lower()
    if type_str:
        if type_str in {"etf", "6"}:
            fund_category = "ETF"
        elif type_str in {"fund", "基金", "5"}:
            fund_category = "FUND"
        elif type_str in {"stock", "1"}:
            is_fund = False
            is_etf = False
        elif type_str in {"index", "2", "industry", "3"}:
            is_fund = False
            is_etf = False

    name_value = name or ""
    name_upper = name_value.upper()
    if "REIT" in name_upper:
        fund_category = "REIT"
    if "LOF" in name_upper:
        fund_category = "LOF"
    if "ETF" in name_upper and "联接" in name_value:
        fund_category = "ETF_LINK"
    if "ETF" in name_upper and fund_category is None:
        fund_category = "ETF"
    if "QDII" in name_upper and fund_category is None:
        fund_category = "QDII"
    if "货币" in name_value and fund_category is None:
        fund_category = "MONEY"
    if ("债券" in name_value or "债" in name_value) and fund_category is None:
        fund_category = "BOND"
    if any(key in name_value for key in ["基金", "LOF", "联接", "分级", "REIT", "REITs"]):
        if fund_category is None:
            fund_category = "FUND"

    code_lower = code.strip().lower()
    if code_lower.startswith(("sh.508", "sz.180")):
        fund_category = fund_category or "REIT"
    if code_lower.startswith(("sz.16", "sh.501", "sh.502", "sh.506")):
        fund_category = fund_category or "LOF"
    if code_lower.startswith(
        ("sh.50", "sh.51", "sh.56", "sh.58", "sh.510", "sh.588", "sz.15", "sz.159", "sz.18")
    ):
        fund_category = fund_category or "ETF"

    if fund_category:
        is_fund = True
        is_etf = fund_category == "ETF"
    return is_etf, is_fund, fund_category


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


class CatalogService:
    def __init__(self, data_root: Path, config: CatalogConfig, filters: FilterConfig):
        self.data_root = data_root
        self.config = config
        self.filters = filters
        self.logger = get_logger("datagrab.catalog")

    def get_catalog(
        self,
        asset_type: str,
        refresh: bool = False,
        limit: int | None = None,
        filters_override: FilterConfig | None = None,
    ) -> CatalogResult:
        filters = filters_override or self.filters
        cache_path = self._cache_path(asset_type)
        if not refresh:
            cached = self._load_cache(cache_path)
            if cached:
                return CatalogResult(self._apply_filters(cached, limit, filters), "cache")
        fetched = self._fetch_with_retry(asset_type)
        if fetched:
            self._write_cache(cache_path, fetched)
            return CatalogResult(self._apply_filters(fetched, limit, filters), "remote")
        cached = self._load_cache(cache_path)
        if cached:
            return CatalogResult(self._apply_filters(cached, limit, filters), "cache-fallback")
        raise RuntimeError(f"no catalog available for {asset_type}")

    def _fetch_with_retry(self, asset_type: str) -> list[SymbolInfo] | None:
        delay = self.config.sleep_sec
        for attempt in range(self.config.retries + 1):
            try:
                if asset_type == "stock":
                    return self._fetch_stock_catalog()
                if asset_type == "ashare":
                    return self._fetch_ashare_catalog()
                return self._static_catalog(asset_type)
            except Exception as exc:
                self.logger.warning("catalog fetch failed: %s", exc)
                time.sleep(delay)
                delay *= self.config.retry_backoff
        return None

    def _fetch_stock_catalog(self) -> list[SymbolInfo]:
        items = []
        for url, symbol_key in [
            (NASDAQ_LISTED_URL, "Symbol"),
            (OTHER_LISTED_URL, "ACT Symbol"),
        ]:
            text = self._download_text(url)
            items.extend(self._parse_pipe_catalog(text, symbol_key))
        deduped = {}
        for item in items:
            if item.symbol not in deduped:
                deduped[item.symbol] = item
        result = list(deduped.values())
        random.shuffle(result)
        return result

    def _download_text(self, url: str) -> str:
        import httpx

        with httpx.Client(timeout=20.0) as client:
            resp = client.get(url)
            resp.raise_for_status()
            return resp.text

    def _fetch_ashare_catalog(self) -> list[SymbolInfo]:
        try:
            import baostock as bs
            import pandas as pd  # noqa: F401
        except Exception as exc:
            raise RuntimeError("baostock is required for ashare catalog") from exc

        today = datetime.now().date()
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            login = bs.login()
        if login.error_code != "0":
            raise RuntimeError(f"baostock login failed: {login.error_msg}")

        try:
            basic_map = self._fetch_ashare_basic_types(bs)
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
                return self._parse_ashare_df(df, basic_map)
        finally:
            with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                bs.logout()
        return []

    def _fetch_ashare_basic_types(self, bs) -> dict[str, str]:
        with contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
            rs = bs.query_stock_basic()
        if rs.error_code != "0":
            return {}
        df = rs.get_data()
        if df is None or df.empty:
            return {}
        mapping: dict[str, str] = {}
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            mapping[code] = str(row.get("type", "")).strip()
        return mapping

    def _parse_ashare_df(self, df, basic_map: dict[str, str]) -> list[SymbolInfo]:
        items: list[SymbolInfo] = []
        for _, row in df.iterrows():
            code = str(row.get("code", "")).strip()
            if not code:
                continue
            name = str(row.get("code_name", "")).strip() or None
            exchange, market = classify_ashare_code(code)
            type_value = basic_map.get(code)
            is_etf, is_fund, fund_category = classify_ashare_security(code, name, type_value)
            items.append(
                SymbolInfo(
                    symbol=code,
                    name=name,
                    exchange=exchange,
                    asset_type="ashare",
                    market_category=market,
                    is_etf=is_etf,
                    is_fund=is_fund,
                    fund_category=fund_category,
                )
            )
        return items

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

    def _static_catalog(self, asset_type: str) -> list[SymbolInfo]:
        presets: dict[str, list[tuple[str, str]]] = {
            "crypto": [
                ("BTC-USD", "Bitcoin"),
                ("ETH-USD", "Ethereum"),
                ("SOL-USD", "Solana"),
                ("BNB-USD", "BNB"),
            ],
            "forex": [
                ("EURUSD=X", "EUR/USD"),
                ("USDJPY=X", "USD/JPY"),
                ("GBPUSD=X", "GBP/USD"),
                ("AUDUSD=X", "AUD/USD"),
            ],
            "commodity": [
                ("GC=F", "Gold"),
                ("CL=F", "Crude Oil"),
                ("SI=F", "Silver"),
                ("HG=F", "Copper"),
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
