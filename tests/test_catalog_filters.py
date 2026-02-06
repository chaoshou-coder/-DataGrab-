from datagrab.config import CatalogConfig, FilterConfig
from datagrab.pipeline.catalog import CatalogService, classify_ashare_code, classify_ashare_security
from datagrab.sources.base import SymbolInfo


def _service(tmp_path, filters: FilterConfig) -> CatalogService:
    return CatalogService(tmp_path, CatalogConfig(), filters)


def test_filters_include_exclude_symbols(tmp_path):
    items = [
        SymbolInfo(symbol="AAPL", name="Apple", exchange="NASDAQ", asset_type="stock"),
        SymbolInfo(symbol="MSFT", name="Microsoft", exchange="NASDAQ", asset_type="stock"),
    ]
    filters = FilterConfig(include_symbols=["AAPL", "MSFT"], exclude_symbols=["MSFT"])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["AAPL"]


def test_filters_name_and_exchange(tmp_path):
    items = [
        SymbolInfo(symbol="AAA", name="Test Holdings", exchange="NASDAQ", asset_type="stock"),
        SymbolInfo(symbol="BBB", name="Alpha Inc", exchange="NYSE", asset_type="stock"),
    ]
    filters = FilterConfig(include_name_regex=["alpha"], include_exchanges=["NYSE"])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["BBB"]


def test_invalid_regex_is_ignored(tmp_path):
    items = [
        SymbolInfo(symbol="AAA", name="Alpha", exchange=None, asset_type="stock"),
        SymbolInfo(symbol="BBB", name="Beta", exchange=None, asset_type="stock"),
    ]
    filters = FilterConfig(include_regex=["["])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["AAA", "BBB"]


def test_market_category_and_etf(tmp_path):
    items = [
        SymbolInfo(
            symbol="QQQ",
            name="Invesco QQQ",
            exchange="NASDAQ",
            asset_type="stock",
            market_category="Q",
            is_etf=True,
        ),
        SymbolInfo(
            symbol="FOO",
            name="Foo Corp",
            exchange="NASDAQ",
            asset_type="stock",
            market_category="G",
            is_etf=False,
        ),
    ]
    filters = FilterConfig(include_market_categories=["Q"], only_etf=True)
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["QQQ"]


def test_classify_ashare_code():
    exchange, market = classify_ashare_code("sh.688001")
    assert exchange == "SSE"
    assert market == "STAR"

    exchange, market = classify_ashare_code("sz.300123")
    assert exchange == "SZSE"
    assert market == "CHINEXT"

    exchange, market = classify_ashare_code("sz.000001")
    assert exchange == "SZSE"
    assert market == "MAIN"


def test_market_alias_filter(tmp_path):
    items = [
        SymbolInfo(
            symbol="sh.688001",
            name="科创板样例",
            exchange="SSE",
            asset_type="ashare",
            market_category="STAR",
        )
    ]
    filters = FilterConfig(include_market_categories=["科创板"])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert len(result) == 1


def test_exchange_alias_filter(tmp_path):
    items = [
        SymbolInfo(
            symbol="sh.600000",
            name="浦发银行",
            exchange="SSE",
            asset_type="ashare",
            market_category="MAIN",
        )
    ]
    filters = FilterConfig(include_exchanges=["上交所"])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert len(result) == 1


def test_fund_detection():
    is_etf, is_fund, fund_category = classify_ashare_security("sh.510300", "沪深300ETF", None)
    assert is_etf is True
    assert is_fund is True
    assert fund_category == "ETF"


def test_fund_subtypes():
    is_etf, is_fund, fund_category = classify_ashare_security("sh.508001", "REIT样例", None)
    assert is_fund is True
    assert fund_category == "REIT"

    is_etf, is_fund, fund_category = classify_ashare_security("sz.160001", "LOF样例", None)
    assert is_fund is True
    assert fund_category == "LOF"

    is_etf, is_fund, fund_category = classify_ashare_security("sh.510300", "ETF联接A", None)
    assert is_etf is False
    assert is_fund is True
    assert fund_category == "ETF_LINK"


def test_only_fund_filter(tmp_path):
    items = [
        SymbolInfo(
            symbol="sh.510300",
            name="沪深300ETF",
            exchange="SSE",
            asset_type="ashare",
            market_category="MAIN",
            is_etf=True,
            is_fund=True,
            fund_category="ETF",
        ),
        SymbolInfo(
            symbol="sh.600000",
            name="浦发银行",
            exchange="SSE",
            asset_type="ashare",
            market_category="MAIN",
            is_etf=False,
            is_fund=False,
        ),
    ]
    filters = FilterConfig(only_fund=True)
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["sh.510300"]


def test_fund_category_filter(tmp_path):
    items = [
        SymbolInfo(
            symbol="sh.508001",
            name="REIT样例",
            exchange="SSE",
            asset_type="ashare",
            market_category="MAIN",
            is_etf=False,
            is_fund=True,
            fund_category="REIT",
        ),
        SymbolInfo(
            symbol="sh.510300",
            name="ETF样例",
            exchange="SSE",
            asset_type="ashare",
            market_category="MAIN",
            is_etf=True,
            is_fund=True,
            fund_category="ETF",
        ),
    ]
    filters = FilterConfig(include_fund_categories=["REIT"])
    service = _service(tmp_path, filters)
    result = service._apply_filters(items, None, filters)
    assert [item.symbol for item in result] == ["sh.508001"]
