from __future__ import annotations

import argparse
import sys
from pathlib import Path

from .config import FilterConfig, load_config, merge_filters
from .deps import check_deps
from .logging import configure_logging, get_logger
from .pipeline.catalog import CatalogService
from .pipeline.downloader import Downloader
from .pipeline.writer import ParquetWriter
from .rate_limiter import RateLimiter
from .sources.baostock_source import BaostockDataSource
from .sources.router import SourceRouter
from .sources.yfinance_source import YFinanceDataSource
from .storage.export import export_backtrader_csv, export_vectorbt_npz
from .timeutils import DateRange, default_date_range, parse_date


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagrab", description="Yahoo Finance batch downloader")
    parser.add_argument("--config", "-c", type=str, help="config path (YAML/TOML)")
    parser.add_argument("--log-level", type=str, default="INFO")
    subparsers = parser.add_subparsers(dest="command")

    tui_parser = subparsers.add_parser("tui", help="launch Textual TUI")
    tui_parser.add_argument("--asset-type", type=str, default="stock")

    catalog_parser = subparsers.add_parser("catalog", help="fetch and cache catalog")
    catalog_parser.add_argument("--asset-type", type=str, default="stock")
    catalog_parser.add_argument("--refresh", action="store_true")
    catalog_parser.add_argument("--limit", type=int)
    _add_filter_args(catalog_parser)

    dl_parser = subparsers.add_parser("download", help="download historical data")
    dl_parser.add_argument("--asset-type", type=str, default="stock")
    dl_parser.add_argument("--symbols", type=str, help="comma separated symbols")
    dl_parser.add_argument("--symbol", action="append", help="single symbol (repeatable)")
    dl_parser.add_argument("--intervals", type=str, help="comma separated intervals")
    dl_parser.add_argument("--start", type=str)
    dl_parser.add_argument("--end", type=str)
    dl_parser.add_argument("--adjust", type=str, help="none/auto/back/forward")
    dl_parser.add_argument("--limit", type=int)
    dl_parser.add_argument("--only-failures", action="store_true")
    dl_parser.add_argument("--failures-file", type=str)
    _add_filter_args(dl_parser)

    deps_parser = subparsers.add_parser("check-deps", help="check dependencies")
    deps_parser.add_argument("--auto-install", action="store_true")

    export_parser = subparsers.add_parser("export", help="export data for engines")
    export_parser.add_argument("--engine", choices=["vectorbt", "backtrader"], required=True)
    export_parser.add_argument("--input", required=True)
    export_parser.add_argument("--output", required=True)

    return parser


def _parse_symbols(args: argparse.Namespace) -> list[str]:
    symbols: list[str] = []
    if args.symbols:
        symbols.extend([s.strip() for s in args.symbols.split(",") if s.strip()])
    if args.symbol:
        symbols.extend([s.strip() for s in args.symbol if s.strip()])
    return symbols


def _split_values(values: list[str] | None) -> list[str]:
    result: list[str] = []
    for value in values or []:
        result.extend([item.strip() for item in value.split(",") if item.strip()])
    return result


def _filters_from_args(args: argparse.Namespace) -> FilterConfig:
    only_etf = None
    if getattr(args, "only_etf", False):
        only_etf = True
    elif getattr(args, "exclude_etf", False):
        only_etf = False
    only_fund = None
    if getattr(args, "only_fund", False):
        only_fund = True
    elif getattr(args, "exclude_fund", False):
        only_fund = False
    return FilterConfig(
        include_regex=_split_values(getattr(args, "include", None)),
        exclude_regex=_split_values(getattr(args, "exclude", None)),
        include_prefixes=_split_values(getattr(args, "include_prefix", None)),
        exclude_prefixes=_split_values(getattr(args, "exclude_prefix", None)),
        include_symbols=_split_values(getattr(args, "include_symbols", None)),
        exclude_symbols=_split_values(getattr(args, "exclude_symbols", None)),
        include_name_regex=_split_values(getattr(args, "include_name", None)),
        exclude_name_regex=_split_values(getattr(args, "exclude_name", None)),
        include_exchanges=_split_values(getattr(args, "include_exchange", None)),
        exclude_exchanges=_split_values(getattr(args, "exclude_exchange", None)),
        include_market_categories=_split_values(getattr(args, "include_market", None)),
        exclude_market_categories=_split_values(getattr(args, "exclude_market", None)),
        only_etf=only_etf,
        only_fund=only_fund,
        include_fund_categories=_split_values(getattr(args, "include_fund_category", None)),
        exclude_fund_categories=_split_values(getattr(args, "exclude_fund_category", None)),
    )


def _add_filter_args(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--include", action="append", help="symbol include regex")
    parser.add_argument("--exclude", action="append", help="symbol exclude regex")
    parser.add_argument("--include-prefix", action="append", help="symbol include prefix")
    parser.add_argument("--exclude-prefix", action="append", help="symbol exclude prefix")
    parser.add_argument("--include-symbols", action="append", help="symbol whitelist")
    parser.add_argument("--exclude-symbols", action="append", help="symbol blacklist")
    parser.add_argument("--include-name", action="append", help="name include regex")
    parser.add_argument("--exclude-name", action="append", help="name exclude regex")
    parser.add_argument("--include-exchange", action="append", help="exchange whitelist")
    parser.add_argument("--exclude-exchange", action="append", help="exchange blacklist")
    parser.add_argument("--include-market", action="append", help="market category whitelist")
    parser.add_argument("--exclude-market", action="append", help="market category blacklist")
    parser.add_argument("--include-fund-category", action="append", help="fund category whitelist")
    parser.add_argument("--exclude-fund-category", action="append", help="fund category blacklist")
    etf_group = parser.add_mutually_exclusive_group()
    etf_group.add_argument("--only-etf", action="store_true", help="only ETF symbols")
    etf_group.add_argument("--exclude-etf", action="store_true", help="exclude ETF symbols")
    fund_group = parser.add_mutually_exclusive_group()
    fund_group.add_argument("--only-fund", action="store_true", help="only fund symbols")
    fund_group.add_argument("--exclude-fund", action="store_true", help="exclude fund symbols")


def main(argv: list[str] | None = None) -> None:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.command is None:
        args.command = "tui"

    configure_logging(args.log_level)
    logger = get_logger("datagrab.cli")
    config = load_config(args.config)

    if args.command == "check-deps":
        missing = check_deps(auto_install=args.auto_install)
        if missing:
            logger.warning("missing dependencies: %s", ", ".join(missing))
            sys.exit(1)
        logger.info("all dependencies satisfied")
        return

    if args.command == "export":
        input_path = Path(args.input)
        output_path = Path(args.output)
        if args.engine == "vectorbt":
            export_vectorbt_npz(input_path, output_path)
        else:
            export_backtrader_csv(input_path, output_path)
        logger.info("exported %s to %s", args.engine, output_path)
        return

    catalog_service = CatalogService(config.data_root_path, config.catalog, config.filters)
    rate_limiter = RateLimiter(config.rate_limit)
    yfinance_source = YFinanceDataSource(config, rate_limiter, catalog_service)
    baostock_source = BaostockDataSource(config, rate_limiter, catalog_service)
    source = SourceRouter(yfinance_source, {"ashare": baostock_source})
    writer = ParquetWriter(config.data_root_path)

    if args.command == "catalog":
        source.set_asset_type(args.asset_type)
        filters_override = merge_filters(config.filters, _filters_from_args(args))
        result = catalog_service.get_catalog(
            asset_type=args.asset_type,
            refresh=args.refresh,
            limit=args.limit or config.catalog.limit,
            filters_override=filters_override,
        )
        logger.info("catalog loaded: %s (%d)", result.source, len(result.items))
        return

    if args.command == "tui":
        from .tui.app import DatagrabApp

        app = DatagrabApp(
            config=config,
            source=source,
            writer=writer,
        )
        app.run()
        return

    if args.command == "download":
        source.set_asset_type(args.asset_type)
        symbols = _parse_symbols(args)
        if not symbols:
            filters_override = merge_filters(config.filters, _filters_from_args(args))
            result = catalog_service.get_catalog(
                asset_type=args.asset_type,
                refresh=False,
                limit=args.limit or config.catalog.limit,
                filters_override=filters_override,
            )
            symbols = [item.symbol for item in result.items]
        intervals = (
            [s.strip() for s in args.intervals.split(",") if s.strip()]
            if args.intervals
            else config.intervals_default
        )
        date_range = default_date_range()
        if args.start:
            date_range = DateRange(parse_date(args.start), date_range.end)
        if args.end:
            date_range = DateRange(date_range.start, parse_date(args.end))
        if args.asset_type == "ashare":
            adjust_default = config.baostock.adjust_default
        else:
            adjust_default = config.yfinance.auto_adjust_default
        adjust = args.adjust or adjust_default
        downloader = Downloader(
            source=source,
            writer=writer,
            concurrency=config.download.concurrency,
            batch_days=config.download.batch_days,
            startup_jitter_max=config.download.startup_jitter_max,
        )
        tasks = downloader.build_tasks(
            symbols=symbols,
            intervals=intervals,
            start=date_range.start,
            end=date_range.end,
            asset_type=args.asset_type,
            adjust=adjust,
        )
        failures_path = (
            Path(args.failures_file)
            if args.failures_file
            else config.data_root_path / "failures.csv"
        )

        def on_progress(stats):
            logger.info(
                "progress total=%d done=%d active=%d failed=%d skipped=%d",
                stats.total,
                stats.completed,
                stats.active,
                stats.failed,
                stats.skipped,
            )

        failures = downloader.run(
            tasks,
            failures_path=failures_path,
            only_failures=args.only_failures,
            progress_cb=on_progress,
        )
        if failures:
            logger.warning("failures recorded at %s", failures_path)
            sys.exit(1)
        return

    parser.print_help()
