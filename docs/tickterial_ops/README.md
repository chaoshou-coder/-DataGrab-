# Tickterial 下载/检验/修复命令总览

本文档说明 `datagrab` 中 Dukascopy 互补品种链路的内嵌使用方式。主入口全部在 `datagrab` CLI；仓库内 `scripts/tickterial_*.py` 仅保留兼容 wrapper，不再承载独立 subprocess 流程。

目标链路为：

**下载 CSV（tickterial） -> 校验 -> 必要时修复 -> 桥接为 Parquet**

## 1) 命令职责

- 下载（CSV 主产物）：`datagrab download --source tickterial`
- 校验（CSV）：`datagrab validate --format csv`
- 修复：`datagrab repair`
- 桥接：`datagrab bridge`

兼容入口（保留原有调用习惯）：

- `scripts/tickterial_mvp.py`
- `scripts/check_tickterial_outputs.py`
- `scripts/repair_tickterial_xau.py`
- `scripts/tickterial_csv_bridge.py`

核心能力沿用 tickterial 路线：

- 并发小时拉取、重试与抖动；
- 源时间戳校正（`--tickterial-source-timestamp-shift-hours`）；
- 窗口级校验失败重试；
- 原子写入，减少中断文件损坏风险；
- 失败列表与可重跑机制。

## 2) 关键产物与失败日志

- 年度 CSV 命名：
  - `{symbol}_1m_YYYYMMDD_YYYYMMDD.csv`
  - `{symbol}_5m_YYYYMMDD_YYYYMMDD.csv`
  - `{symbol}_15m_YYYYMMDD_YYYYMMDD.csv`
  - `{symbol}_1d_YYYYMMDD_YYYYMMDD.csv`
- 失败日志：`<tickterial_output>/failures_mvp.csv`

报告导出（可选）：

- `--report-json`
- `--report-csv`
- `--repair-command-file`

## 3) 推荐执行流程

1. **下载**

```bash
datagrab download --source tickterial --symbols XAUUSD --start 2016-01-01 --end 2016-01-02
```

2. **校验**

```bash
datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD
```

3. **修复**（不再走外部脚本，而是直接调用 datagrab tickterial pipeline）

```bash
datagrab repair --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --intervals 1m,5m,15m
```

4. **1d 对齐问题时可重建**

```bash
datagrab repair --symbol XAUUSD --intervals 1d --prefer-local-1m-for-1d
```

5. **复检并桥接（可选）**

```bash
datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --interval 1m
datagrab bridge --input-dir ./data/tickterial_csv --output-root ./data --asset-type commodity --symbol XAUUSD --interval 1m
```

## 4) 常用命令模板

### A. tickterial 下载（示例）

```bash
datagrab download --source tickterial --symbols XAUUSD --tickterial-output ./data/tickterial_csv --start 2016-01-01 --end 2016-01-02 --intervals 1m,5m,15m,1d --tickterial-workers 4
```

### B. tickterial CSV 校验

```bash
datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --interval 1m
```

### C. tickterial 修复 + 重检

```bash
datagrab repair --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --intervals 1d,5m,15m --output ./data/tickterial_csv

datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --interval 1m
```

### D. CSV 转 Parquet

```bash
datagrab bridge --input-dir ./data/tickterial_csv --output-root ./data --asset-type commodity --symbol XAUUSD --interval 1m
```

### E. 更新 tickterial 品种表

```bash
datagrab update-symbols --source tickterial
```

此命令会从 Dukascopy 官方接口刷新互补品种表（约 422 个互补类别）。

## 5) 网络重下场景

- 下载失败且窗口无法恢复；
- `1m/5m/15m` 修复后仍检测到窗口缺口；
- `1d` 修复前本地 `1m` 不足且未开启离线复用；
- 强制覆盖时通过 `--tickterial-force` 触发重写。

## 6) 注意事项

- 请勿混用过期 `DateGrab` 路径写法，建议用 `./data`、`--data-root` 统一管理。
- `--mvp-script` / `--project-root` / `--python` / `--mvp-timeout-seconds` 已不再属于主链路参数范围。
