from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Callable, Iterable

import polars as pl

from .quality import QualityIssue, Severity
from .schema import ADJUSTED_COLUMN, BASE_COLUMNS


@dataclass(frozen=True)
class FileSummary:
    path: Path
    asset_type: str | None
    symbol: str | None
    interval: str | None
    row_count: int
    min_dt: datetime | None
    max_dt: datetime | None
    duplicate_datetime_count: int
    missing_columns: list[str]
    null_close_count: int | None
    invalid_ohlc_count: int | None
    negative_value_count: int | None
    max_gap: timedelta | None


@dataclass(frozen=True)
class BatchProgress:
    """validate_batch 的进度信息。"""

    total: int
    completed: int
    current_file: str | None


def iter_parquet_files(
    root: Path,
    asset_type: str | None = None,
    symbol: str | None = None,
    interval: str | None = None,
) -> Iterable[Path]:
    """遍历 data_root 下的 parquet 文件（默认 root/<asset_type>/<symbol>/*.parquet）。"""
    root = Path(root)
    if asset_type and symbol:
        base = root / asset_type / symbol
        patterns = [f"{interval}_*.parquet" if interval else "*.parquet"]
    elif asset_type:
        base = root / asset_type
        patterns = [f"**/{interval}_*.parquet" if interval else "**/*.parquet"]
    else:
        base = root
        patterns = [f"**/{interval}_*.parquet" if interval else "**/*.parquet"]
    for pat in patterns:
        yield from base.glob(pat)


# ---------------------------------------------------------------------------
#  单文件验证（融合单次 collect）
# ---------------------------------------------------------------------------


def validate_parquet_file(path: Path) -> tuple[FileSummary, list[QualityIssue]]:
    """对单个 parquet 进行快速质量检查。

    优化：将所有聚合、行级布尔统计融合到 **一次** ``lf.select(...).collect()``，
    避免反复读取文件；对于无法融合的场景自动降级为逐项检查。
    """
    path = Path(path)
    asset_type, symbol, interval = _infer_context_from_path(path)
    issues: list[QualityIssue] = []
    ctx = dict(path=str(path), asset_type=asset_type, symbol=symbol, interval=interval)

    # ---------- 读取 schema ----------
    try:
        lf = pl.scan_parquet(path)
        schema = lf.collect_schema()
        cols = list(schema.names())
    except Exception as exc:
        issues.append(
            QualityIssue(
                rule_id="parquet.read_failed",
                severity=Severity.ERROR,
                message="Parquet 读取失败",
                details=str(exc),
                **ctx,
            )
        )
        return _empty_summary(path, asset_type, symbol, interval), issues

    # ---------- schema 完整性 ----------
    missing = [c for c in BASE_COLUMNS if c not in cols]
    for key in ("datetime", "close"):
        if key not in cols:
            issues.append(
                QualityIssue(
                    rule_id=f"schema.missing_{key}",
                    severity=Severity.ERROR,
                    message=f"缺少关键列：{key}",
                    details=f"columns={cols}",
                    **ctx,
                )
            )
    for c in ("open", "high", "low", "volume"):
        if c not in cols:
            issues.append(
                QualityIssue(
                    rule_id=f"schema.missing_{c}",
                    severity=Severity.WARN,
                    message=f"缺少列：{c}",
                    details=f"columns={cols}",
                    **ctx,
                )
            )

    # ---------- 构建融合表达式 ----------
    exprs: list[pl.Expr] = [pl.len().alias("n_total")]

    has_dt = "datetime" in cols
    has_close = "close" in cols
    has_ohlc = all(c in cols for c in ("high", "low", "close"))
    price_cols = [c for c in ("open", "high", "low", "close") if c in cols]
    vol_cols = ["volume"] if "volume" in cols else []

    if has_dt:
        exprs.extend(
            [
                pl.col("datetime").min().alias("min_dt"),
                pl.col("datetime").max().alias("max_dt"),
                pl.col("datetime").n_unique().alias("dt_nunique"),
                pl.col("datetime").sort().diff().max().alias("max_gap"),
            ]
        )

    if has_close:
        exprs.append(pl.col("close").null_count().alias("null_close"))

    if has_ohlc:
        exprs.append(
            (
                (pl.col("high") < pl.col("low"))
                | (pl.col("close") < pl.col("low"))
                | (pl.col("close") > pl.col("high"))
            )
            .sum()
            .alias("invalid_ohlc")
        )

    neg_cond: pl.Expr | None = None
    for c in price_cols + vol_cols:
        cc = pl.col(c) < 0
        neg_cond = cc if neg_cond is None else (neg_cond | cc)
    if neg_cond is not None:
        exprs.append(neg_cond.sum().alias("neg_count"))

    # ---------- 单次 collect ----------
    try:
        result = lf.select(exprs).collect()
    except Exception:
        # 融合 collect 失败 → 降级为逐项检查
        return _validate_fallback(lf, cols, path, asset_type, symbol, interval, missing, issues)

    row_count = _safe_int(result, "n_total")

    # ---------- 解析结果 ----------
    min_dt: datetime | None = None
    max_dt: datetime | None = None
    duplicate_count = 0
    max_gap_val: timedelta | None = None
    null_close_count: int | None = None
    invalid_ohlc_count: int | None = None
    negative_value_count: int | None = None

    if has_dt:
        try:
            min_dt = result["min_dt"][0]
            max_dt = result["max_dt"][0]
            n_unique = _safe_int(result, "dt_nunique")
            duplicate_count = max(0, row_count - n_unique)
            if duplicate_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="datetime.duplicated",
                        severity=Severity.WARN,
                        message=f"datetime 有重复：{duplicate_count} 行",
                        **ctx,
                    )
                )
            raw_gap = result["max_gap"][0]
            if isinstance(raw_gap, timedelta):
                max_gap_val = raw_gap
                threshold = _gap_threshold(interval)
                if threshold and max_gap_val > threshold:
                    issues.append(
                        QualityIssue(
                            rule_id="datetime.gap_too_large",
                            severity=Severity.WARN,
                            message=f"时间间隔异常大：max_gap={max_gap_val}",
                            details=f"threshold={threshold}",
                            **ctx,
                        )
                    )
        except Exception:
            pass

    if has_close:
        try:
            null_close_count = _safe_int(result, "null_close")
            if null_close_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="close.has_nulls",
                        severity=Severity.WARN,
                        message=f"close 存在空值：{null_close_count} 行",
                        **ctx,
                    )
                )
        except Exception:
            pass

    if has_ohlc:
        try:
            invalid_ohlc_count = _safe_int(result, "invalid_ohlc")
            if invalid_ohlc_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="ohlc.invalid_range",
                        severity=Severity.WARN,
                        message=f"OHLC 逻辑异常（high/low/close）：{invalid_ohlc_count} 行",
                        **ctx,
                    )
                )
        except Exception:
            pass

    if neg_cond is not None:
        try:
            negative_value_count = _safe_int(result, "neg_count")
            if negative_value_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="values.negative",
                        severity=Severity.WARN,
                        message=f"存在负值（价格/成交量）：{negative_value_count} 行",
                        **ctx,
                    )
                )
        except Exception:
            pass

    _ = ADJUSTED_COLUMN  # 保留给未来扩展

    summary = FileSummary(
        path=path,
        asset_type=asset_type,
        symbol=symbol,
        interval=interval,
        row_count=row_count,
        min_dt=min_dt,
        max_dt=max_dt,
        duplicate_datetime_count=duplicate_count,
        missing_columns=missing,
        null_close_count=null_close_count,
        invalid_ohlc_count=invalid_ohlc_count,
        negative_value_count=negative_value_count,
        max_gap=max_gap_val,
    )
    return summary, issues


# ---------------------------------------------------------------------------
#  批量并行验证
# ---------------------------------------------------------------------------


def validate_batch(
    files: list[Path],
    max_workers: int | None = None,
    on_result: Callable[[FileSummary, list[QualityIssue], BatchProgress], None] | None = None,
    issue_writer: Callable[[QualityIssue], None] | None = None,
) -> tuple[list[FileSummary], list[QualityIssue]]:
    """并行批量验证 parquet 文件，充分利用多核 CPU。

    Polars 在 ``.collect()`` 期间会释放 GIL，因此多个 Python 线程可以
    真正并行执行 Polars 原生运算。ThreadPoolExecutor 没有 IPC 序列化开销，
    是此场景下的最优选择。

    Parameters
    ----------
    files : list[Path]
        待验证的 parquet 文件路径列表。
    max_workers : int | None
        线程数。``None`` 时自动取 ``min(cpu_count, len(files), 32)``。
    on_result : callback(summary, issues, progress)
        每完成一个文件后调用（从工作线程回调，调用方自行处理线程安全）。
    issue_writer : callback(issue) 或 None
        逐条 issue 写出回调。若提供该参数，不再返回完整 issue 列表。
    """
    if max_workers is None:
        cpu = os.cpu_count() or 4
        max_workers = min(cpu, len(files), 32) if files else 1
    max_workers = max(max_workers, 1)

    summaries: list[FileSummary] = []
    all_issues: list[QualityIssue] = []
    total = len(files)

    if total == 0:
        return summaries, all_issues

    # 文件数很少时直接串行，避免线程池开销
    if total <= 2 or max_workers == 1:
        for idx, p in enumerate(files):
            summary, issues = validate_parquet_file(p)
            summaries.append(summary)
            if issue_writer is None:
                all_issues.extend(issues)
            else:
                for issue in issues:
                    issue_writer(issue)
            if on_result:
                prog = BatchProgress(total=total, completed=idx + 1, current_file=p.name)
                on_result(summary, issues, prog)
        return summaries, all_issues

    completed = 0
    with ThreadPoolExecutor(max_workers=max_workers) as pool:
        future_to_path = {pool.submit(validate_parquet_file, p): p for p in files}
        for future in as_completed(future_to_path):
            p = future_to_path[future]
            completed += 1
            try:
                summary, issues = future.result()
            except Exception as exc:
                at, sym, itv = _infer_context_from_path(p)
                issues = [
                    QualityIssue(
                        rule_id="validate.unexpected_error",
                        severity=Severity.ERROR,
                        message=f"验证异常：{exc}",
                        path=str(p),
                        asset_type=at,
                        symbol=sym,
                        interval=itv,
                    )
                ]
                summary = _empty_summary(p, at, sym, itv)
            summaries.append(summary)
            if issue_writer is None:
                all_issues.extend(issues)
            else:
                for issue in issues:
                    issue_writer(issue)
            if on_result:
                prog = BatchProgress(total=total, completed=completed, current_file=p.name)
                on_result(summary, issues, prog)
    return summaries, all_issues


# ---------------------------------------------------------------------------
#  内部辅助
# ---------------------------------------------------------------------------


def _safe_int(df: pl.DataFrame, col_name: str) -> int:
    """从单行 DataFrame 安全取整数。"""
    try:
        v = df[col_name][0]
        return int(v) if v is not None else 0
    except Exception:
        return 0


def _empty_summary(
    path: Path,
    asset_type: str | None,
    symbol: str | None,
    interval: str | None,
) -> FileSummary:
    return FileSummary(
        path=path,
        asset_type=asset_type,
        symbol=symbol,
        interval=interval,
        row_count=0,
        min_dt=None,
        max_dt=None,
        duplicate_datetime_count=0,
        missing_columns=list(BASE_COLUMNS),
        null_close_count=None,
        invalid_ohlc_count=None,
        negative_value_count=None,
        max_gap=None,
    )


def _validate_fallback(
    lf: pl.LazyFrame,
    cols: list[str],
    path: Path,
    asset_type: str | None,
    symbol: str | None,
    interval: str | None,
    missing: list[str],
    issues: list[QualityIssue],
) -> tuple[FileSummary, list[QualityIssue]]:
    """融合 collect 失败时的降级路径：逐项 try/except。"""
    ctx = dict(path=str(path), asset_type=asset_type, symbol=symbol, interval=interval)
    row_count = 0
    min_dt = max_dt = None
    duplicate_count = 0
    max_gap_val: timedelta | None = None
    null_close_count: int | None = None
    invalid_ohlc_count: int | None = None
    negative_value_count: int | None = None

    try:
        row_count = _collect_one_int(lf.select(pl.len()))
    except Exception:
        pass

    if "datetime" in cols:
        try:
            dt_stats = lf.select(
                pl.col("datetime").min().alias("min_dt"),
                pl.col("datetime").max().alias("max_dt"),
                pl.col("datetime").n_unique().alias("n_unique"),
                pl.len().alias("n_total"),
            ).collect()
            if dt_stats.height:
                min_dt = dt_stats["min_dt"][0]
                max_dt = dt_stats["max_dt"][0]
                n_unique = int(dt_stats["n_unique"][0] or 0)
                n_total = int(dt_stats["n_total"][0] or 0)
                duplicate_count = max(0, n_total - n_unique)
                if duplicate_count > 0:
                    issues.append(
                        QualityIssue(
                            rule_id="datetime.duplicated",
                            severity=Severity.WARN,
                            message=f"datetime 有重复：{duplicate_count} 行",
                            **ctx,
                        )
                    )
        except Exception as exc:
            issues.append(
                QualityIssue(
                    rule_id="datetime.stats_failed",
                    severity=Severity.WARN,
                    message="datetime 统计失败",
                    details=str(exc),
                    **ctx,
                )
            )

        try:
            gap_df = lf.select(pl.col("datetime").sort().diff().max().alias("max_gap")).collect()
            if gap_df.height:
                raw = gap_df["max_gap"][0]
                if isinstance(raw, timedelta):
                    max_gap_val = raw
                    threshold = _gap_threshold(interval)
                    if threshold and max_gap_val > threshold:
                        issues.append(
                            QualityIssue(
                                rule_id="datetime.gap_too_large",
                                severity=Severity.WARN,
                                message=f"时间间隔异常大：max_gap={max_gap_val}",
                                details=f"threshold={threshold}",
                                **ctx,
                            )
                        )
        except Exception:
            pass

    if "close" in cols:
        try:
            null_close_count = _collect_one_int(lf.select(pl.col("close").null_count()))
            if null_close_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="close.has_nulls",
                        severity=Severity.WARN,
                        message=f"close 存在空值：{null_close_count} 行",
                        **ctx,
                    )
                )
        except Exception:
            pass

    if all(c in cols for c in ("high", "low", "close")):
        try:
            invalid_ohlc_count = _collect_one_int(
                lf.filter(
                    (pl.col("high") < pl.col("low"))
                    | (pl.col("close") < pl.col("low"))
                    | (pl.col("close") > pl.col("high"))
                ).select(pl.len())
            )
            if invalid_ohlc_count > 0:
                issues.append(
                    QualityIssue(
                        rule_id="ohlc.invalid_range",
                        severity=Severity.WARN,
                        message=f"OHLC 逻辑异常（high/low/close）：{invalid_ohlc_count} 行",
                        **ctx,
                    )
                )
        except Exception:
            pass

    price_cols = [c for c in ("open", "high", "low", "close") if c in cols]
    vol_cols = ["volume"] if "volume" in cols else []
    if price_cols or vol_cols:
        try:
            cond = None
            for c in price_cols + vol_cols:
                cc = pl.col(c) < 0
                cond = cc if cond is None else (cond | cc)
            if cond is not None:
                negative_value_count = _collect_one_int(lf.filter(cond).select(pl.len()))
                if negative_value_count > 0:
                    issues.append(
                        QualityIssue(
                            rule_id="values.negative",
                            severity=Severity.WARN,
                            message=f"存在负值（价格/成交量）：{negative_value_count} 行",
                            **ctx,
                        )
                    )
        except Exception:
            pass

    summary = FileSummary(
        path=path,
        asset_type=asset_type,
        symbol=symbol,
        interval=interval,
        row_count=row_count,
        min_dt=min_dt,
        max_dt=max_dt,
        duplicate_datetime_count=duplicate_count,
        missing_columns=missing,
        null_close_count=null_close_count,
        invalid_ohlc_count=invalid_ohlc_count,
        negative_value_count=negative_value_count,
        max_gap=max_gap_val,
    )
    return summary, issues


def _collect_one_int(lf: pl.LazyFrame) -> int:
    df = lf.collect()
    if df.is_empty():
        return 0
    value = df.item()
    try:
        return int(value or 0)
    except Exception:
        return 0


def _infer_context_from_path(path: Path) -> tuple[str | None, str | None, str | None]:
    """从 data_root/<asset_type>/<symbol>/<interval>_*.parquet 推断上下文。"""
    asset_type = None
    symbol = None
    interval = None
    try:
        symbol = path.parent.name
        asset_type = path.parent.parent.name
        name = path.name
        if "_" in name:
            interval = name.split("_", 1)[0]
    except Exception:
        pass
    return asset_type, symbol, interval


def _gap_threshold(interval: str | None) -> timedelta | None:
    """对 gap 做一个粗略阈值判断（不引入交易日历）。"""
    if not interval:
        return None
    key = interval.strip().lower()
    if key.endswith("d"):
        return timedelta(days=10)
    if key.endswith("m") or key.endswith("h"):
        return timedelta(hours=6)
    if key.endswith("wk") or key.endswith("w"):
        return timedelta(days=60)
    if key.endswith("mo"):
        return timedelta(days=120)
    return None
