from __future__ import annotations

import argparse
import csv
import json
import logging
import shlex
from datetime import datetime
import sys
import tempfile
from pathlib import Path

from .config import FilterConfig, load_config, merge_filters
from .deps import check_deps
from .logging import DEFAULT_FORMAT, configure_logging, get_logger
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
from .timeutils import DateRange, default_date_range, parse_date
from .validation import (
    CliValidationError,
    ValidationFailureRecordError,
    validate_cli_args,
    render_cli_error,
)


_WIZARD_BACK = "<wizard_back>"
_WIZARD_ASSET_TYPES = ("stock", "ashare", "forex", "crypto", "commodity")
_WIZARD_INTERVALS = ("1m", "2m", "5m", "15m", "30m", "60m", "90m", "1h", "1d", "5d", "1wk", "1mo", "3mo")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="datagrab", description="Yahoo Finance batch downloader")
    parser.add_argument("--config", "-c", type=str, help="config path (YAML/TOML)")
    parser.add_argument("--log-level", type=str, default="INFO")
    parser.add_argument("--verbose", action="store_true", help="download 命令开启详细日志（默认仅 timeline+报错）")
    parser.add_argument("--data-root", type=str, help="临时覆盖 storage.data_root")
    subparsers = parser.add_subparsers(dest="command")

    catalog_parser = subparsers.add_parser(
        "catalog",
        help="下载并缓存对应资产的 symbol 列表到本地 (data/catalog/<asset_type>_symbols.csv)；--refresh 表示联网拉取最新列表",
    )
    catalog_parser.add_argument(
        "--asset-type",
        type=str,
        default="stock",
        help="资产类型: stock（美股）, ashare（A股）, forex, crypto, commodity",
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

    update_symbols_parser = subparsers.add_parser(
        "update-symbols",
        help="一次性刷新美股(stock)与A股(ashare)的 symbol 缓存（network-only）",
    )
    update_symbols_parser.add_argument("--limit", type=int)
    update_symbols_parser.add_argument(
        "--asset-types",
        type=str,
        default="stock,ashare",
        help="更新范围：stock（美股）,ashare（A股）或 stock（仅美股）",
    )
    _add_filter_args(update_symbols_parser)

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
        "--download-log-file",
        type=str,
        help="download 命令日志保存路径；不传则使用 <data_root>/logs/download_YYYYMMDD_HHMMSS.log",
    )
    dl_parser.add_argument(
        "--strict-failures-csv",
        action="store_true",
        help="failures 文件中存在任何错误行时立即中断",
    )
    _add_filter_args(dl_parser)

    deps_parser = subparsers.add_parser("check-deps", help="check dependencies")
    deps_parser.add_argument("--auto-install", action="store_true")

    doctor_parser = subparsers.add_parser("doctor", help="run environment and scope diagnostics")
    doctor_parser.add_argument("--json", action="store_true", help="output JSON report")
    doctor_parser.add_argument("--strict", action="store_true", help="strict mode: treat warnings as failures")
    doctor_parser.add_argument("--check-scope", action="store_true", help="validate asset/symbol/interval scope")
    doctor_parser.add_argument("--asset-type", type=str, default="stock", help="for scope check")
    doctor_parser.add_argument("--symbols", type=str, help="for scope check, comma separated")
    doctor_parser.add_argument("--symbol", action="append", help="for scope check, single symbol (repeatable)")
    doctor_parser.add_argument("--interval", type=str, help="for scope check, interval token")

    export_parser = subparsers.add_parser("export", help="export data for engines")
    export_parser.add_argument("--engine", choices=["vectorbt", "backtrader"], required=True)
    export_parser.add_argument("--input", required=True)
    export_parser.add_argument("--output", required=True)

    validate_parser = subparsers.add_parser("validate", help="validate parquet data quality under data_root")
    validate_parser.add_argument("path", nargs="?", help="可选：直接指定待校验目录（例如 E:\\data\\DateGrab\\commodity）")
    validate_parser.add_argument("--root", type=str, help="data root path (default: config.data_root_path)")
    validate_parser.add_argument("--asset-type", type=str, help="asset type filter (e.g. stock(美股)/ashare(A股))")
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

    subparsers.add_parser("wizard", help="交互式预览并确认后执行更新/下载/验数")

    return parser


def _parse_symbols(args: argparse.Namespace) -> list[str]:
    symbols: list[str] = []
    if args.symbols:
        symbols.extend([s.strip() for s in args.symbols.split(",") if s.strip()])
    if args.symbol:
        symbols.extend([s.strip() for s in args.symbol if s.strip()])
    return symbols


def _normalize_asset_types(value: object | None) -> tuple[str, ...]:
    raw_values: list[str]
    if value is None:
        raw_values = ["stock", "ashare"]
    elif isinstance(value, (list, tuple)):
        raw_values = [str(v).strip() for v in value]
    else:
        raw_values = [item.strip() for item in str(value).split(",") if item.strip()]
    normalized: list[str] = []
    for item in raw_values:
        item = item.strip().lower()
        if item in {"stock", "ashare"} and item not in normalized:
            normalized.append(item)
    return tuple(normalized) if normalized else ("stock", "ashare")


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


def _apply_data_root_override(config, cli_args) -> None:
    if cli_args.data_root:
        config.storage.data_root = cli_args.data_root


def _default_download_log_path(data_root: Path) -> Path:
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    return data_root / "logs" / f"download_{timestamp}.log"


def _configure_download_logging(
    cli_args,
    data_root: Path,
    logger,
) -> Path | None:
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.DEBUG)

    console_level = (
        getattr(logging, cli_args.log_level.upper(), logging.INFO)
        if cli_args.verbose
        else logging.WARNING
    )
    had_stream_handler = False
    for handler in list(root_logger.handlers):
        if isinstance(handler, logging.StreamHandler) and not isinstance(handler, logging.FileHandler):
            handler.setLevel(console_level)
            had_stream_handler = True
    if not had_stream_handler:
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(console_level)
        stream_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        root_logger.addHandler(stream_handler)

    log_path = Path(cli_args.download_log_file) if cli_args.download_log_file else _default_download_log_path(data_root)
    try:
        log_path.parent.mkdir(parents=True, exist_ok=True)
        file_handler = logging.FileHandler(log_path, encoding="utf-8")
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(logging.Formatter(DEFAULT_FORMAT))
        duplicate = False
        target = str(log_path.resolve())
        for handler in root_logger.handlers:
            if isinstance(handler, logging.FileHandler) and str(Path(handler.baseFilename).resolve()) == target:
                duplicate = True
                break
        if not duplicate:
            root_logger.addHandler(file_handler)
        logger.debug("download detailed log: %s", log_path)
        return log_path
    except Exception as exc:
        logger.warning("下载日志文件创建失败，将仅保留终端日志: %s", exc)
        return None


def _is_wizard_back(value: str) -> bool:
    return value.strip().lower() in {"b", "back", "上一步", "返回"}


def _prompt_non_empty(prompt: str) -> str:
    while True:
        value = input(f"{prompt}: ").strip()
        if value:
            return value
        print("输入不能为空，请重试。")


def _prompt_text(prompt: str, default: str = "", allow_back: bool = False) -> str:
    suffix = "（输入 b 返回上一步）" if allow_back else ""
    value = input(f"{prompt}{f' [{default}]' if default else ''}{suffix}: ").strip()
    if allow_back and _is_wizard_back(value):
        return _WIZARD_BACK
    return value or default


def _prompt_choice(prompt: str, options: list[str], default: str, allow_back: bool = False) -> str:
    while True:
        value = _prompt_text(prompt, default, allow_back=allow_back)
        if value == _WIZARD_BACK:
            return value
        normalized = value.strip().lower()
        if normalized in options:
            return normalized
        print(f"无效输入: {value}，支持值: {', '.join(options)}。输入 b 可返回上一步")


def _prompt_int(prompt: str, default: int | None = None, allow_back: bool = False) -> int | None | str:
    while True:
        raw = _prompt_text(prompt, str(default) if default is not None else "", allow_back=allow_back)
        if raw == _WIZARD_BACK:
            return raw
        if not raw:
            return default
        try:
            parsed = int(raw)
            if parsed >= 1:
                return parsed
        except ValueError:
            pass
        print("请输入正整数（或直接回车使用默认，输入 b 返回上一步）。")


def _prompt_bool(prompt: str, default: bool, allow_back: bool = False) -> bool | str:
    while True:
        value = _prompt_text(prompt, "Y" if default else "N", allow_back=allow_back).lower().strip()
        if value == _WIZARD_BACK:
            return value
        if value in {"y", "yes"}:
            return True
        if value in {"n", "no"}:
            return False
        if value == "":
            return default
        print("请输入 Y/N，或输入 b 返回上一步。")


def _validate_intervals(value: str) -> str:
    raw = value.strip().lower()
    if not raw:
        return ""
    items = [item.strip() for item in raw.split(",") if item.strip()]
    invalid = [item for item in items if item not in _WIZARD_INTERVALS]
    if invalid:
        raise ValueError(f"不支持的周期：{','.join(invalid)}，可用值: {', '.join(_WIZARD_INTERVALS)}")
    return ",".join(items)


def _validate_symbols(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    symbols = [item.strip() for item in raw.split(",") if item.strip()]
    if not symbols:
        raise ValueError("symbols 不能为空字符串片段")
    return ",".join(symbols)


def _validate_date_value(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    parse_date(raw)
    return raw


def _validate_file_path(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    path = Path(raw).expanduser()
    if path.exists() and path.is_dir():
        raise ValueError("路径是目录，请输入文件路径")
    return str(path)


def _validate_root_path(value: str) -> str:
    raw = value.strip()
    if not raw:
        return ""
    path = Path(raw).expanduser()
    if path.exists() and not path.is_dir():
        raise ValueError("路径不是目录")
    return str(path)


def _build_wizard_command_preview(parts: list[str]) -> str:
    return " ".join(shlex.quote(part) for part in parts)


def _run_doctor(args: argparse.Namespace, config, logger) -> int:
    checks: dict[str, dict[str, object]] = {}
    any_fail = False
    any_warn = False
    any_warn_blocks_strict = False
    run_strict = bool(getattr(args, "strict", False))
    check_scope = bool(getattr(args, "check_scope", False))

    def _set_check(
        name: str,
        status: str,
        message: str,
        detail: str | None = None,
        *,
        blocks_strict: bool = True,
    ) -> None:
        nonlocal any_fail, any_warn, any_warn_blocks_strict
        checks[name] = {
            "status": status,
            "message": message,
            "detail": detail,
        }
        if status == "fail":
            any_fail = True
        elif status == "warn":
            any_warn = True
            if blocks_strict:
                any_warn_blocks_strict = True

    # 1) 配置状态（已加载即表示基础结构正确）
    config_summary = {
        "timezone": config.timezone,
        "asset_types": list(config.asset_types),
        "storage": {
            "data_root": str(config.storage.data_root),
            "merge_on_incremental": bool(config.storage.merge_on_incremental),
        },
        "rate_limit": {
            "requests_per_second": config.rate_limit.requests_per_second,
            "jitter_min": config.rate_limit.jitter_min,
            "jitter_max": config.rate_limit.jitter_max,
        },
    }
    _set_check("config", "ok", "config loaded and validated", json.dumps(config_summary, ensure_ascii=False))

    # 2) 文件系统检查
    data_root = config.data_root_path
    try:
        if not data_root.exists():
            _set_check(
                "filesystem",
                "warn",
                f"data_root 不存在，将尝试按需创建: {data_root}",
            )
            try:
                data_root.mkdir(parents=True, exist_ok=True)
            except Exception as exc:
                _set_check(
                    "filesystem",
                    "fail",
                    f"data_root 创建失败: {data_root}",
                    str(exc),
                )
        elif not data_root.is_dir():
            _set_check(
                "filesystem",
                "fail",
                f"data_root 不是目录: {data_root}",
            )
        else:
            _set_check(
                "filesystem",
                "ok",
                f"data_root 存在: {data_root}",
            )
        if data_root.exists() and data_root.is_dir():
            try:
                with tempfile.NamedTemporaryFile(dir=data_root, delete=True) as fh:
                    fh.write(b"")
                _set_check(
                    "filesystem",
                    "ok",
                    f"data_root 可写: {data_root}",
                )
            except Exception as exc:
                _set_check(
                    "filesystem",
                    "fail",
                    f"data_root 写入失败: {data_root}",
                    str(exc),
                )
    except Exception as exc:
        _set_check("filesystem", "fail", "filesystem check 异常", str(exc))

    # 3) 依赖检查
    try:
        missing = check_deps(auto_install=False)
        if missing:
            status = "warn" if not run_strict else "fail"
            _set_check(
                "dependencies",
                status,
                "依赖不完整",
                "missing=" + ",".join(missing),
            )
        else:
            _set_check("dependencies", "ok", "required dependencies 已满足")
    except Exception as exc:
        _set_check("dependencies", "fail", "依赖检查异常", str(exc))

    # 4) 网络连通性检查
    network_ok = True
    network_soft_issue = False
    network_details: list[str] = []
    try:
        from .pipeline.catalog import (
            NASDAQ_LISTED_URL,
            OTHER_LISTED_URL,
            _YAHOO_SCREENER_IDS,
            _YAHOO_SCREENER_URL,
        )

        import httpx  # type: ignore
    except Exception as exc:
        network_ok = False
        network_details.append(f"network check module import failed: {exc}")
        _set_check("network", "warn", "network 模块不可用", str(exc))
    else:
        proxy = config.yfinance.proxy
        client_kwargs = {"timeout": 8.0}
        if proxy:
            client_kwargs["proxies"] = {
                "http://": proxy,
                "https://": proxy,
            }

        def _probe(
            name: str,
            url: str,
            *,
            critical: bool = True,
        ) -> tuple[str, str, bool]:
            try:
                with httpx.Client(**client_kwargs, headers={"User-Agent": "datagrab/doctor"}) as client:
                    resp = client.get(url, follow_redirects=True)
                resp.raise_for_status()
                return "ok", f"200 {resp.status_code}", True
            except Exception as exc:
                if critical:
                    return "fail", str(exc), False
                # 非关键端点失败不应直接视为致命故障：保留可见告警并在 strict 下给出提示
                return "warn", str(exc), False

        targets = [
            ("nasdaq-listed", NASDAQ_LISTED_URL, True),
            ("nasdaq-other", OTHER_LISTED_URL, True),
        ]
        for asset_type, scr_id in _YAHOO_SCREENER_IDS.items():
            try:
                url = f"{_YAHOO_SCREENER_URL}?scrIds={scr_id}&count=1"
            except Exception as exc:
                network_details.append(f"{asset_type} screener url 无法构造: {exc}")
                continue
            targets.append((f"yahoo-{asset_type}-screener", url, False))

        for name, url, critical in targets:
            status, detail, can_influence = _probe(name, url, critical=critical)
            network_details.append(f"{name}={status}:{detail}")
            if can_influence and status != "ok":
                network_ok = False
            if status == "warn":
                network_soft_issue = True

        if network_ok and network_soft_issue:
            _set_check(
                "network",
                "warn",
                "存在可选网络端点异常（通常不会影响核心流程）",
                "; ".join(network_details),
                blocks_strict=False,
            )
        elif network_ok:
            _set_check("network", "ok", "network checks passed")
        else:
            status = "warn" if not run_strict else "fail"
            _set_check(
                "network",
                status,
                "部分网络检查失败",
                "; ".join(network_details),
            )

    # 5) scope 检查
    if check_scope:
        try:
            from .validation.cli import _normalize_interval, _validate_symbol
            symbols_input = []
            symbols_input.extend([s for s in (getattr(args, "symbols", "") or "").split(",") if s.strip()])
            symbols_input.extend([s for s in (getattr(args, "symbol", []) or []) if s])
            symbols = [s.strip() for s in symbols_input if str(s).strip()]
            interval = (getattr(args, "interval", "") or "").strip()
            if not symbols:
                _set_check("scope_symbols", "warn", "未提供 --symbols/--symbol，跳过符号检查")
            else:
                symbol_status = "ok"
                bad_symbols: list[str] = []
                for symbol in symbols:
                    try:
                        _validate_symbol(symbol, field="symbol")
                    except Exception as exc:
                        symbol_status = "fail" if run_strict else "warn"
                        bad_symbols.append(f"{symbol}: {exc}")
                _set_check(
                    "scope_symbols",
                    symbol_status,
                    "symbol 检查完成",
                    ", ".join(bad_symbols) if bad_symbols else None,
                )
            if interval:
                try:
                    _normalize_interval(interval)
                    interval_status = "ok"
                    interval_msg = "interval 合法"
                except Exception as exc:
                    interval_status = "warn" if not run_strict else "fail"
                    interval_msg = str(exc)
                _set_check("scope_interval", interval_status, "interval check", interval_msg)
            else:
                _set_check("scope_interval", "warn", "未提供 --interval，跳过周期检查")
            from .validation import validate_cli_args

            fake_ns = argparse.Namespace(
                command="download",
                log_level="INFO",
                config=None,
                data_root=None,
                asset_type=(getattr(args, "asset_type", "stock") or "stock").strip().lower(),
                limit=None,
                workers=None,
                verbose=False,
                adjust="auto",
                download_log_file=None,
                intervals=[interval] if interval else [],
                symbols=symbols,
                symbol=symbols[:1],
                start=None,
                end=None,
                strict_failures_csv=False,
                failures_file=None,
                only_failures=False,
            )
            validate_cli_args(fake_ns, asset_types=config.asset_types)
            _set_check("scope_cli", "ok", "scope cli 校验通过")
        except Exception as exc:
            status = "warn" if not run_strict else "fail"
            _set_check("scope", status, "scope 校验失败", str(exc))

    report = {
        "timestamp": datetime.now().isoformat(),
        "status": "fail" if any_fail else ("warn" if any_warn else "ok"),
        "strict_mode": run_strict,
        "checks": checks,
    }

    if getattr(args, "json", False):
        print(json.dumps(report, ensure_ascii=False, indent=2))
    else:
        for name, item in checks.items():
            logger.info("[%s] %s | %s", name, item["status"], item["message"])
            detail = item.get("detail")
            if detail:
                logger.info("  %s", detail)
    if run_strict and (any_fail or any_warn_blocks_strict):
        return 2
    return 0


def _run_validate(args: argparse.Namespace, cli_args, config, logger) -> int:
    root_arg = getattr(args, "path", None) or getattr(args, "root", None)
    root = Path(root_arg).resolve() if root_arg else config.data_root_path
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
        return 0

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

                validate_batch(
                    files, max_workers=effective_workers, on_result=_on_result, issue_writer=_on_issue_csv
                )
        else:
            with out_path.open("w", encoding="utf-8") as f:
                def _on_issue_jsonl(issue) -> None:
                    _on_issue(issue)
                    f.write(json.dumps(issue.to_dict(), ensure_ascii=False))
                    f.write("\n")

                validate_batch(
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
        _, all_issues = validate_batch(files, max_workers=effective_workers, on_result=_on_result, issue_writer=None)
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

    return 1 if error_count > 0 else 0


def _extract_global_args(argv: list[str] | None) -> tuple[argparse.Namespace, list[str]]:
    global_parser = argparse.ArgumentParser(add_help=False)
    global_parser.add_argument("--config", "-c", type=str, default=None)
    global_parser.add_argument("--log-level", type=str, default=None)
    global_parser.add_argument("--verbose", action="store_true")
    global_parser.add_argument("--data-root", type=str, default=None)
    global_ns, remaining = global_parser.parse_known_args(argv or [])
    return global_ns, list(remaining)


def _run_update_symbols(
    args: argparse.Namespace,
    config,
    catalog_service: CatalogService,
    logger,
    *,
    asset_types: tuple[str, ...] | None = None,
) -> None:
    filters_override = merge_filters(config.filters, _filters_from_args(args))
    targets = asset_types or _normalize_asset_types(getattr(args, "asset_types", None))
    for asset_type in targets:
        result = catalog_service.get_catalog(
            asset_type=asset_type,
            refresh=True,
            limit=args.limit or config.catalog.limit,
            filters_override=filters_override,
        )
        cache_path = config.data_root_path / "catalog" / f"{asset_type}_symbols.csv"
        logger.info(
            "%s: %d items refreshed -> %s",
            asset_type,
            len(result.items),
            cache_path,
        )


def _run_catalog(args: argparse.Namespace, cli_args, config, catalog_service: CatalogService, source, logger) -> None:
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


def _run_download(args: argparse.Namespace, cli_args, config, catalog_service, source, writer, logger) -> int:
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

    log_path = _configure_download_logging(cli_args, config.data_root_path, logger)
    if log_path:
        logger.info("下载详细日志: %s", log_path)

    def on_progress(stats):
        print(
            f"timeline total={stats.total} done={stats.completed} active={stats.active} "
            f"failed={stats.failed} skipped={stats.skipped}",
            flush=True,
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
        return 2
    if failures:
        logger.warning("failures recorded at %s", failures_path)
        return 1
    return 0


def _run_wizard(config, catalog_service, source, writer, logger) -> int:
    while True:
        print("wizard: 请选择要执行的流程")
        print("1) 更新 symbol")
        print("2) 下载数据")
        print("3) 数据检查(validate)")
        mode = _prompt_choice("请输入序号", ["1", "2", "3"], "1")
        while True:
            data_root_raw = _prompt_text("data-root（可空，留空=用配置/环境变量）", "")
            try:
                data_root = _validate_root_path(data_root_raw)
                break
            except ValueError as exc:
                print(f"data-root 输入有误: {exc}")
                continue
        data_root_path = Path(data_root).expanduser() if data_root else config.data_root_path

        if mode == "1":
            step = 0
            limit: int | None = None
            scope = "1"
            while True:
                if step == 0:
                    current = _prompt_int("更新条数（可空，留空=全部）", None, allow_back=True)
                    if current == _WIZARD_BACK:
                        break
                    limit = current
                    step = 1
                    continue

                if step == 1:
                    current = _prompt_choice(
                        "更新范围 [1=全部 stock+ashare, 2=仅 stock(美股)]",
                        ["1", "2"],
                        "1",
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 0
                        continue
                    scope = current
                    step = 2
                    continue

                preview_parts = ["datagrab"]
                if data_root:
                    preview_parts.extend(["--data-root", data_root])
                preview_parts.append("update-symbols")
                if limit:
                    preview_parts.extend(["--limit", str(limit)])
                if scope == "2":
                    preview_parts.extend(["--asset-types", "stock"])
                preview = _build_wizard_command_preview(preview_parts)
                print("\n执行预览：")
                print(preview)
                confirmed = _prompt_bool("确认执行？[Y/N]", False, allow_back=True)
                if confirmed == _WIZARD_BACK:
                    step = 1
                    continue
                if not confirmed:
                    print("已取消，不执行。")
                    return 0

                fake_ns = argparse.Namespace(command="update-symbols")
                fake_ns.limit = limit
                fake_ns.config = None
                fake_ns.data_root = data_root or None
                fake_ns.asset_types = ("stock",) if scope == "2" else ("stock", "ashare")
                fake_ns.log_level = "INFO"
                fake_ns.verbose = False
                fake_ns.download_log_file = None
                fake_ns.include = []
                fake_ns.exclude = []
                fake_ns.include_prefix = []
                fake_ns.exclude_prefix = []
                fake_ns.include_symbols = []
                fake_ns.exclude_symbols = []
                fake_ns.include_name = []
                fake_ns.exclude_name = []
                fake_ns.include_exchange = []
                fake_ns.exclude_exchange = []
                fake_ns.include_market = []
                fake_ns.exclude_market = []
                fake_ns.include_fund_category = []
                fake_ns.exclude_fund_category = []
                fake_ns.only_etf = False
                fake_ns.exclude_etf = False
                fake_ns.only_fund = False
                fake_ns.exclude_fund = False
                try:
                    cli_ns = validate_cli_args(fake_ns, asset_types=config.asset_types)
                except CliValidationError as exc:
                    print(f"wizard 参数校验失败: {render_cli_error(exc)}", file=sys.stderr)
                    return 2
                _apply_data_root_override(config, cli_ns)
                catalog_service.set_data_root(config.data_root_path)
                writer.set_data_root(config.data_root_path)
                _run_update_symbols(fake_ns, config, catalog_service, logger, asset_types=fake_ns.asset_types)
                return 0

        if mode == "2":
            interval_default = ",".join(config.intervals_default)
            default_failures_file = str((data_root_path / "failures.csv").resolve())
            default_download_log_file = str(_default_download_log_path(Path(data_root_path)).resolve())
            step = 0
            asset_type = "stock"
            symbols = ""
            intervals = interval_default
            start = ""
            end = ""
            adjust = ""
            only_failures = False
            strict_failures_csv = False
            failures_file = default_failures_file
            download_log_file = default_download_log_file
            while True:
                if step == 0:
                    current = _prompt_choice(
                        "asset-type（stock=美股, ashare=A股, forex, crypto, commodity；回车=stock）",
                        list(_WIZARD_ASSET_TYPES),
                        "stock",
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        break
                    asset_type = current
                    adjust = (
                        config.baostock.adjust_default
                        if asset_type == "ashare"
                        else config.yfinance.auto_adjust_default
                    )
                    step = 1
                    continue

                if step == 1:
                    current = _prompt_text("symbols（可空，逗号分隔）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 0
                        continue
                    try:
                        symbols = _validate_symbols(current)
                    except ValueError as exc:
                        print(f"symbols 输入有误: {exc}")
                        continue
                    step = 2
                    continue

                if step == 2:
                    current = _prompt_text(
                        "intervals（可多选，逗号隔开；可选："
                        + ", ".join(_WIZARD_INTERVALS)
                        + f"，默认 {interval_default}）",
                        interval_default,
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 1
                        continue
                    try:
                        intervals = _validate_intervals(current)
                    except ValueError as exc:
                        print(f"intervals 输入有误: {exc}")
                        continue
                    step = 3
                    continue

                if step == 3:
                    current = _prompt_text("start（YYYY-MM-DD，可空）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 2
                        continue
                    try:
                        start = _validate_date_value(current)
                    except ValueError as exc:
                        print(f"start 输入有误: {exc}")
                        continue
                    step = 4
                    continue

                if step == 4:
                    current = _prompt_text("end（YYYY-MM-DD，可空）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 3
                        continue
                    try:
                        end = _validate_date_value(current)
                        if start and end:
                            if parse_date(start) > parse_date(end):
                                raise ValueError("start 不能晚于 end")
                    except ValueError as exc:
                        print(f"end 输入有误: {exc}")
                        continue
                    step = 5
                    continue

                if step == 5:
                    current = _prompt_choice(
                        "adjust（auto/none/back/front/fallback/backward）",
                        ["none", "auto", "back", "front", "forward", "backward"],
                        adjust,
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 4
                        continue
                    adjust = current
                    step = 6
                    continue

                if step == 6:
                    current = _prompt_bool("仅重跑失败任务？[Y/N]", False, allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 5
                        continue
                    only_failures = bool(current)
                    step = 7
                    continue

                if step == 7:
                    current = _prompt_bool("strict 失败文件？[Y/N]", False, allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 6
                        continue
                    strict_failures_csv = bool(current)
                    step = 8
                    continue

                if step == 8:
                    current = _prompt_text(
                        f"failures-file（可空，默认 {default_failures_file}，输入 none 清空）",
                        default_failures_file,
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 7
                        continue
                    if current.strip().lower() == "none":
                        failures_file = ""
                    else:
                        try:
                            failures_file = _validate_file_path(current)
                        except ValueError as exc:
                            print(f"failures-file 输入有误: {exc}")
                            continue
                    step = 9
                    continue

                if step == 9:
                    current = _prompt_text(
                        f"下载日志文件（可空，默认 {default_download_log_file}，输入 none 清空）",
                        default_download_log_file,
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 8
                        continue
                    if current.strip().lower() == "none":
                        download_log_file = ""
                    else:
                        try:
                            download_log_file = _validate_file_path(current)
                        except ValueError as exc:
                            print(f"下载日志文件输入有误: {exc}")
                            continue
                    step = 10
                    continue

                preview_parts = ["datagrab"]
                if data_root:
                    preview_parts.extend(["--data-root", data_root])
                preview_parts.extend(["download", "--asset-type", asset_type, "--intervals", intervals, "--adjust", adjust])
                if symbols:
                    preview_parts.extend(["--symbols", symbols])
                if start:
                    preview_parts.extend(["--start", start])
                if end:
                    preview_parts.extend(["--end", end])
                if only_failures:
                    preview_parts.append("--only-failures")
                if strict_failures_csv:
                    preview_parts.append("--strict-failures-csv")
                if failures_file:
                    preview_parts.extend(["--failures-file", failures_file])
                if download_log_file:
                    preview_parts.extend(["--download-log-file", download_log_file])
                print("\n执行预览：")
                print(_build_wizard_command_preview(preview_parts))
                confirmed = _prompt_bool("确认执行？[Y/N]", False, allow_back=True)
                if confirmed == _WIZARD_BACK:
                    step = 9
                    continue
                if not confirmed:
                    print("已取消，不执行。")
                    return 0

                fake_ns = argparse.Namespace(command="download")
                fake_ns.asset_type = asset_type
                fake_ns.symbols = symbols or None
                fake_ns.symbol = None
                fake_ns.intervals = intervals
                fake_ns.start = start or None
                fake_ns.end = end or None
                fake_ns.adjust = adjust
                fake_ns.limit = None
                fake_ns.only_failures = only_failures
                fake_ns.failures_file = failures_file or None
                fake_ns.strict_failures_csv = strict_failures_csv
                fake_ns.verbose = False
                fake_ns.download_log_file = download_log_file or None
                fake_ns.config = None
                fake_ns.data_root = data_root or None
                fake_ns.log_level = "INFO"
                fake_ns.workers = None
                fake_ns.include = []
                fake_ns.exclude = []
                fake_ns.include_prefix = []
                fake_ns.exclude_prefix = []
                fake_ns.include_symbols = []
                fake_ns.exclude_symbols = []
                fake_ns.include_name = []
                fake_ns.exclude_name = []
                fake_ns.include_exchange = []
                fake_ns.exclude_exchange = []
                fake_ns.include_market = []
                fake_ns.exclude_market = []
                fake_ns.include_fund_category = []
                fake_ns.exclude_fund_category = []
                fake_ns.only_etf = False
                fake_ns.exclude_etf = False
                fake_ns.only_fund = False
                fake_ns.exclude_fund = False
                try:
                    cli_ns = validate_cli_args(fake_ns, asset_types=config.asset_types)
                except CliValidationError as exc:
                    print(f"wizard 参数校验失败: {render_cli_error(exc)}", file=sys.stderr)
                    return 2
                _apply_data_root_override(config, cli_ns)
                catalog_service.set_data_root(config.data_root_path)
                writer.set_data_root(config.data_root_path)
                return _run_download(fake_ns, cli_ns, config, catalog_service, source, writer, logger)

        if mode == "3":
            step = 0
            root = ""
            asset_type = ""
            symbol = ""
            interval = ""
            out = ""
            format_ = "jsonl"
            summary = False
            workers: int | None = None
            while True:
                if step == 0:
                    current = _prompt_text("root（可空，留空=当前 data-root）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        break
                    try:
                        root = _validate_root_path(current)
                    except ValueError as exc:
                        print(f"root 输入有误: {exc}")
                        continue
                    step = 1
                    continue

                if step == 1:
                    raw = _prompt_text(
                        "asset-type（stock=美股, ashare=A股, forex, crypto, commodity；可留空）",
                        "",
                        allow_back=True,
                    )
                    if raw == _WIZARD_BACK:
                        step = 0
                        continue
                    raw = raw.strip().lower()
                    if raw:
                        if raw not in _WIZARD_ASSET_TYPES:
                            print(f"asset-type 输入有误: {raw}，支持值: {', '.join(_WIZARD_ASSET_TYPES)}")
                            continue
                        current = raw
                    else:
                        current = ""
                    if current:
                        asset_type = current
                    else:
                        asset_type = ""
                    step = 2
                    continue

                if step == 2:
                    current = _prompt_text("symbol（可空，单标的）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 1
                        continue
                    if current:
                        if "," in current:
                            print("symbol 目前仅支持单个值，多个请分次校验")
                            continue
                        symbol = current
                    else:
                        symbol = ""
                    step = 3
                    continue

                if step == 3:
                    current = _prompt_text(
                        f"interval（可空，可选值：{', '.join(_WIZARD_INTERVALS)}）",
                        "",
                        allow_back=True,
                    )
                    if current == _WIZARD_BACK:
                        step = 2
                        continue
                    if current:
                        try:
                            normalized = _validate_intervals(current)
                        except ValueError as exc:
                            print(f"interval 输入有误: {exc}")
                            continue
                        if "," in normalized:
                            print("interval 目前仅支持单个值")
                            continue
                        interval = normalized
                    else:
                        interval = ""
                    step = 4
                    continue

                if step == 4:
                    current = _prompt_text("out（可空）", "", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 3
                        continue
                    if current:
                        try:
                            out = _validate_file_path(current)
                        except ValueError as exc:
                            print(f"out 输入有误: {exc}")
                            continue
                    else:
                        out = ""
                    step = 5
                    continue

                if step == 5:
                    current = _prompt_choice("format（jsonl/csv）", ["jsonl", "csv"], "jsonl", allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 4
                        continue
                    format_ = current
                    step = 6
                    continue

                if step == 6:
                    current = _prompt_bool("仅汇总？[Y/N]", False, allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 5
                        continue
                    summary = bool(current)
                    step = 7
                    continue

                if step == 7:
                    current = _prompt_int("workers（可空）", None, allow_back=True)
                    if current == _WIZARD_BACK:
                        step = 6
                        continue
                    workers = current
                    preview = ["datagrab validate"]
                    if root:
                        preview.extend(["--root", root])
                    if asset_type:
                        preview.extend(["--asset-type", asset_type])
                    if symbol:
                        preview.extend(["--symbol", symbol])
                    if interval:
                        preview.extend(["--interval", interval])
                    if out:
                        preview.extend(["--out", out])
                    preview.extend(["--format", format_])
                    if summary:
                        preview.append("--summary")
                    if workers:
                        preview.extend(["--workers", str(workers)])
                    print("\n执行预览：")
                    print(_build_wizard_command_preview(preview))
                    confirmed = _prompt_bool("确认执行？[Y/N]", False, allow_back=True)
                    if confirmed == _WIZARD_BACK:
                        step = 6
                        continue
                    if not confirmed:
                        print("已取消，不执行。")
                        return 0

                    fake_ns = argparse.Namespace(command="validate")
                    fake_ns.root = root or None
                    fake_ns.asset_type = asset_type or None
                    fake_ns.symbol = symbol or None
                    fake_ns.interval = interval or None
                    fake_ns.out = out or None
                    fake_ns.format = format_
                    fake_ns.summary = summary
                    fake_ns.workers = workers
                    fake_ns.config = None
                    fake_ns.log_level = "INFO"
                    fake_ns.verbose = False
                    fake_ns.data_root = data_root or None
                    fake_ns.download_log_file = None
                    fake_ns.only_etf = False
                    fake_ns.exclude_etf = False
                    fake_ns.only_fund = False
                    fake_ns.exclude_fund = False
                    fake_ns.include = []
                    fake_ns.exclude = []
                    fake_ns.include_prefix = []
                    fake_ns.exclude_prefix = []
                    fake_ns.include_symbols = []
                    fake_ns.exclude_symbols = []
                    fake_ns.include_name = []
                    fake_ns.exclude_name = []
                    fake_ns.include_exchange = []
                    fake_ns.exclude_exchange = []
                    fake_ns.include_market = []
                    fake_ns.exclude_market = []
                    fake_ns.include_fund_category = []
                    fake_ns.exclude_fund_category = []
                    try:
                        cli_ns = validate_cli_args(fake_ns, asset_types=config.asset_types)
                    except CliValidationError as exc:
                        print(f"wizard 参数校验失败: {render_cli_error(exc)}", file=sys.stderr)
                        return 2
                    _apply_data_root_override(config, cli_ns)
                    catalog_service.set_data_root(config.data_root_path)
                    writer.set_data_root(config.data_root_path)
                    return _run_validate(fake_ns, cli_ns, config, logger)

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
    raw_argv = sys.argv[1:] if argv is None else argv
    global_args, argv = _extract_global_args(raw_argv)
    args = parser.parse_args(argv)
    if global_args.config is not None:
        args.config = global_args.config
    if global_args.log_level is not None:
        args.log_level = global_args.log_level
    if global_args.verbose:
        args.verbose = True
    if global_args.data_root is not None:
        args.data_root = global_args.data_root
    if args.command is None:
        parser.print_help()
        return

    try:
        config = load_config(args.config)
    except Exception as exc:
        print(f"配置加载失败: {exc}", file=sys.stderr)
        sys.exit(1)

    if args.command == "doctor":
        cli_args = args
    else:
        try:
            cli_args = validate_cli_args(args, asset_types=config.asset_types)
        except CliValidationError as exc:
            print(f"参数校验失败: {render_cli_error(exc)}", file=sys.stderr)
            sys.exit(2)

    _apply_data_root_override(config, cli_args)

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

    if args.command == "doctor":
        exit_code = _run_doctor(args, config, logger)
        if exit_code:
            sys.exit(exit_code)
        return

    if args.command == "validate":
        exit_code = _run_validate(args, cli_args, config, logger)
        if exit_code:
            sys.exit(exit_code)
        return

    catalog_service = CatalogService(config.data_root_path, config.catalog, config.filters, yfinance_config=config.yfinance)
    rate_limiter = RateLimiter(config.rate_limit)
    yfinance_source = YFinanceDataSource(config, rate_limiter, catalog_service)
    baostock_source = BaostockDataSource(config, rate_limiter, catalog_service)
    source = SourceRouter(yfinance_source, {"ashare": baostock_source}, allowed_asset_types=config.asset_types)
    writer = ParquetWriter(config.data_root_path, merge_on_incremental=config.storage.merge_on_incremental)

    if args.command == "catalog":
        _run_catalog(args, cli_args, config, catalog_service, source, logger)
        return

    if args.command == "update-symbols":
        _run_update_symbols(args, config, catalog_service, logger)
        return

    if args.command == "download":
        exit_code = _run_download(args, cli_args, config, catalog_service, source, writer, logger)
        if exit_code:
            sys.exit(exit_code)
        return

    if args.command == "wizard":
        exit_code = _run_wizard(config, catalog_service, source, writer, logger)
        if exit_code:
            sys.exit(exit_code)
        return

    parser.print_help()
