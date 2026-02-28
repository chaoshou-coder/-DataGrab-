from __future__ import annotations

import argparse
import csv
import json
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
from .storage.quality import Severity
from .storage.validate import BatchProgress, iter_parquet_files, validate_batch
from .timeutils import DateRange, default_date_range
from .validation import (
    CliValidationError,
    ValidationFailureRecordError,
    validate_cli_args,
    render_cli_error,
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagrab", description="Yahoo Finance batch downloader")
    parser.add_argument("--config", "-c", type=str, help="config path (YAML/TOML)")
    parser.add_argument("--log-level", type=str, default="INFO")
    subparsers = parser.add_subparsers(dest="command")

    catalog_parser = subparsers.add_parser(
        "catalog",
        help="下载并缓存对应资产的 symbol 列表到本地 (data/catalog/<asset_type>_symbols.csv)；--refresh 表示联网拉取最新列表",
    )
    catalog_parser.add_argument(
        "--asset-type",
        type=str,
        default="stock",
        help="资产类型: stock(美股), ashare(A股), forex, crypto, commodity",
    )
    catalog_parser.add_argument(
        "--refresh",
        action="store_true",
        help="联网拉取最新 symbol 列表并写入本地，不传则优先读本地缓存",
    )
    catalog_parser.add_argument(
        "--refresh-all",
        action="store_true",
        help="与 --refresh 同时使用时，依次更新所有支持联网的资产类型 (stock, ashare)",
    )
    catalog_parser.add_argument("--limit", type=int)
    _add_filter_args(catalog_parser)

    dl_parser = subparsers.add_parser("download", help="download historical data")
    dl_parser.add_argument("--asset-type", type=str, default="stock")
    dl_parser.add_argument("--symbols", type=str, help="comma separated symbols")
    dl_parser.add_argument("--symbol", action="append", help="single symbol (repeatable)")
    dl_parser.add_argument("--intervals", type=str, help="comma separated intervals")
    dl_parser.add_argument("--start", type=str)
    dl_parser.add_argument("--end", type=str)
    dl_parser.add_argument("--adjust", type=str, help="none/auto/back/forward（front/backward 兼容）")
    dl_parser.add_argument("--limit", type=int)
    dl_parser.add_argument("--only-failures", action="store_true")
    dl_parser.add_argument("--failures-file", type=str)
    dl_parser.add_argument(
        "--strict-failures-csv",
        action="store_true",
        help="failures 文件中存在任何错误行时立即中断",
    )
    _add_filter_args(dl_parser)

    deps_parser = subparsers.add_parser("check-deps", help="check dependencies")
    deps_parser.add_argument("--auto-install", action="store_true")

    export_parser = subparsers.add_parser("export", help="export data for engines")
    export_parser.add_argument("--engine", choices=["vectorbt", "backtrader"], required=True)
    export_parser.add_argument("--input", required=True)
    export_parser.add_argument("--output", required=True)

    validate_parser = subparsers.add_parser("validate", help="validate parquet data quality under data_root")
    validate_parser.add_argument("--root", type=str, help="data root path (default: config.data_root_path)")
    validate_parser.add_argument("--asset-type", type=str, help="asset type filter (e.g. stock/ashare)")
    validate_parser.add_argument("--symbol", type=str, help="symbol filter (single symbol)")
    validate_parser.add_argument("--interval", type=str, help="interval filter (e.g. 1d/5m)")
    validate_parser.add_argument("--out", type=str, help="output issues file path (jsonl/csv)")
    validate_parser.add_argument("--format", choices=["jsonl", "csv"], default="jsonl", help="issues output format")
    validate_parser.add_argument("--summary", action="store_true", help="only print totals")
    validate_parser.add_argument(
        "--workers",
        type=int,
        default=None,
        help="parallel worker threads (default: auto = cpu_count)",
    )

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
        parser.print_help()
        return

    try:
        config = load_config(args.config)
    except Exception as exc:
        print(f"配置加载失败: {exc}", file=sys.stderr)
        sys.exit(1)

    try:
        cli_args = validate_cli_args(args, asset_types=config.asset_types)
    except CliValidationError as exc:
        print(f"参数校验失败: {render_cli_error(exc)}", file=sys.stderr)
        sys.exit(2)

    try:
        configure_logging(cli_args.log_level)
    except Exception as exc:
        print(f"日志级别配置失败: {exc}", file=sys.stderr)
        configure_logging("INFO")
    logger = get_logger("datagrab.cli")

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

    if args.command == "validate":
        root = Path(args.root).resolve() if getattr(args, "root", None) else config.data_root_path
        asset_type = cli_args.asset_type
        symbol = args.symbol
        interval = args.interval
        out_path = Path(args.out).resolve() if getattr(args, "out", None) else None
        out_format = args.format
        only_summary = bool(args.summary)
        workers = cli_args.workers

        files = list(iter_parquet_files(root, asset_type=asset_type, symbol=symbol, interval=interval))
        if not files:
            logger.warning("no parquet files found under %s", root)
            return

        import os

        effective_workers = workers or min(os.cpu_count() or 4, len(files), 32)
        logger.info("validating %d files with %d workers", len(files), effective_workers)

        error_files = 0
        warn_files = 0
        issue_count = 0
        error_count = 0

        def _on_issue(issue) -> None:
            nonlocal issue_count, error_count
            issue_count += 1
            if issue.severity == Severity.ERROR:
                error_count += 1

        def _on_result(summary, file_issues, progress: BatchProgress) -> None:
            nonlocal error_files, warn_files
            if file_issues:
                has_error = any(i.severity == Severity.ERROR for i in file_issues)
                if has_error:
                    error_files += 1
                else:
                    warn_files += 1
            if not only_summary:
                missing = ",".join(summary.missing_columns) if summary.missing_columns else "-"
                logger.info(
                    "[%d/%d] file=%s rows=%d range=%s..%s dup=%d missing=%s",
                    progress.completed,
                    progress.total,
                    summary.path,
                    summary.row_count,
                    summary.min_dt,
                    summary.max_dt,
                    summary.duplicate_datetime_count,
                    missing,
                )

        all_issues: list = []
        if out_path:
            out_path.parent.mkdir(parents=True, exist_ok=True)
            if out_format == "csv":
                with out_path.open("w", encoding="utf-8", newline="") as f:
                    fieldnames = [
                        "created_at",
                        "severity",
                        "rule_id",
                        "asset_type",
                        "symbol",
                        "interval",
                        "path",
                        "message",
                        "details",
                    ]
                    writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
                    writer.writeheader()

                    def _on_issue_csv(issue) -> None:
                        _on_issue(issue)
                        writer.writerow(issue.to_dict())

                    _, all_issues = validate_batch(
                        files, max_workers=effective_workers, on_result=_on_result, issue_writer=_on_issue_csv
                    )
            else:
                with out_path.open("w", encoding="utf-8") as f:
                    def _on_issue_jsonl(issue) -> None:
                        _on_issue(issue)
                        f.write(json.dumps(issue.to_dict(), ensure_ascii=False))
                        f.write("\n")

                    _, all_issues = validate_batch(
                        files, max_workers=effective_workers, on_result=_on_result, issue_writer=_on_issue_jsonl
                    )
            logger.info("issues written to %s", out_path)
            issue_files = error_files + warn_files
            logger.info(
                "validated files=%d issue_files=%d (error=%d warn=%d) issues=%d",
                len(files),
                issue_files,
                error_count,
                issue_files - error_count,
                issue_count,
            )
        else:
            summaries, all_issues = validate_batch(
                files, max_workers=effective_workers, on_result=_on_result, issue_writer=None
            )
            issue_count = len(all_issues)
            error_count = sum(1 for issue in all_issues if issue.severity == Severity.ERROR)
            issue_files = error_files + warn_files
            logger.info(
                "validated files=%d issue_files=%d (error=%d warn=%d) issues=%d",
                len(files),
                issue_files,
                error_count,
                issue_files - error_count,
                issue_count,
            )

        if error_count > 0:
            sys.exit(1)
        return

    catalog_service = CatalogService(config.data_root_path, config.catalog, config.filters, yfinance_config=config.yfinance)
    rate_limiter = RateLimiter(config.rate_limit)
    yfinance_source = YFinanceDataSource(config, rate_limiter, catalog_service)
    baostock_source = BaostockDataSource(config, rate_limiter, catalog_service)
    source = SourceRouter(yfinance_source, {"ashare": baostock_source}, allowed_asset_types=config.asset_types)
    writer = ParquetWriter(config.data_root_path)

    if args.command == "catalog":
        data_root = config.data_root_path
        cache_dir = data_root / "catalog"

        if getattr(args, "refresh_all", False) and args.refresh:
            # 依次联网更新所有支持远程拉取的资产类型，并写入本地
            for asset_type in ("stock", "ashare"):
                source.set_asset_type(asset_type)
                filters_override = merge_filters(config.filters, _filters_from_args(args))
                try:
                    result = catalog_service.get_catalog(
                        asset_type=asset_type,
                        refresh=True,
                        limit=args.limit or config.catalog.limit,
                        filters_override=filters_override,
                    )
                    path = cache_dir / f"{asset_type}_symbols.csv"
                    logger.info(
                        "catalog %s: %s (%d) -> %s",
                        asset_type,
                        result.source,
                        len(result.items),
                        path,
                    )
                except Exception as e:
                    logger.warning("catalog %s failed: %s", asset_type, e)
            return

        source.set_asset_type(cli_args.asset_type or "stock")
        filters_override = merge_filters(config.filters, _filters_from_args(args))
        result = catalog_service.get_catalog(
            asset_type=cli_args.asset_type or "stock",
            refresh=args.refresh,
            limit=args.limit or config.catalog.limit,
            filters_override=filters_override,
        )
        path = cache_dir / f"{cli_args.asset_type or 'stock'}_symbols.csv"
        logger.info(
            "catalog loaded: %s (%d) -> %s",
            result.source,
            len(result.items),
            path,
        )
        return

    if args.command == "download":
        source.set_asset_type(cli_args.asset_type or "stock")
        symbols = _parse_symbols(args)
        if not symbols:
            filters_override = merge_filters(config.filters, _filters_from_args(args))
            result = catalog_service.get_catalog(
                asset_type=cli_args.asset_type or "stock",
                refresh=False,
                limit=args.limit or config.catalog.limit,
                filters_override=filters_override,
            )
            symbols = [item.symbol for item in result.items]
        intervals = cli_args.intervals if cli_args.intervals else config.intervals_default
        date_range = default_date_range()
        if cli_args.start:
            date_range = DateRange(cli_args.start, date_range.end)
        if cli_args.end:
            date_range = DateRange(date_range.start, cli_args.end)
        if (cli_args.asset_type or "stock") == "ashare":
            adjust_default = config.baostock.adjust_default
        else:
            adjust_default = config.yfinance.auto_adjust_default
        adjust = cli_args.adjust or adjust_default
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
            asset_type=cli_args.asset_type or "stock",
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

        try:
            failures = downloader.run(
                tasks,
                failures_path=failures_path,
                only_failures=args.only_failures,
                strict_failures_csv=cli_args.strict_failures_csv,
                progress_cb=on_progress,
            )
        except ValidationFailureRecordError as exc:
            print(f"failures 文件解析失败: {exc}", file=sys.stderr)
            sys.exit(2)
        if failures:
            logger.warning("failures recorded at %s", failures_path)
            sys.exit(1)
        return

    parser.print_help()
