# 财采 (DataGrab) — 使用说明

## CLI 使用说明

## 推荐阅读顺序

1. 先执行 `datagrab --help` 确认当前仓库支持的子命令；
2. 查看“命令行参考”确认参数默认值；
3. 按“doctor -> 更新目录 -> 下载 -> 数据检查 -> 导出”跑一遍最小链路；
4. 对照“常见问题”处理执行异常。

> 建议新同事先跑一遍：
> `datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d`
> `datagrab catalog --asset-type stock --refresh`
> `datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2024-01-01 --end 2024-01-31 --data-root ./data`
> `datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d`
> `datagrab download --source tickterial --symbols XAUUSD --intervals 1m,5m,15m,1d --start 2016-01-01 --end 2016-01-02`
> `datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --interval 1m --start 2016-01-01 --end 2016-01-02`

### 0. 入场前先运行 doctor

#### Windows（PowerShell）

```powershell
$env:PYTHONPATH = "src"
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
```

#### Linux / macOS（bash）

```bash
export PYTHONPATH=src
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
```

`doctor` 通过 `--check-scope` 先验证 CLI 关键参数，避免把下载问题误判为数据源问题。

### 0.1 doctor 输出解读与执行建议

- `config`：配置加载和 schema 是否成功；
- `filesystem`：`data_root` 是否存在且可写；
- `dependencies`：关键依赖可否 import；
- `network`：核心端点（核心失败会升高阻塞）与可选端点（多数以 warn 呈现）；
- `scope_symbols` / `scope_interval` / `scope_cli`：参数与交叉校验一致性。

- 当出现 `warn` 而任务可继续时，可先按 [doctor-runbook](doctor-runbook.md) 的优先级修复；
- `--strict` 适合 CI 场景，配合严格失败规则作为门禁。

### 0.2 `--strict` 使用建议

CI 或批处理建议启用 `--strict`，把阻塞告警升级为失败码；本地排障可先不加 `--strict`。

### 1. 更新目录

先拉取/更新目录，再下载，避免“symbol 不存在”或无效过滤。

- 美股/ETF/基金：`datagrab catalog --asset-type stock`
- A 股目录：`datagrab catalog --asset-type ashare`
- 筛选示例：`datagrab catalog --asset-type stock --include-prefix A --exclude TEST`
- 一次更新多个源：`datagrab catalog --asset-type stock --refresh --limit 2000`

### 2. 下载

#### 2.1 标准下载

```bash
datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31
```

#### 2.2 tickterial 下载（CSV 主输出）

```bash
datagrab download --source tickterial --symbols XAUUSD --intervals 1m,5m,15m,1d --start 2016-01-01 --end 2016-01-02
```

- 该路径默认产出到 `<data-root>/tickterial_csv`（可用 `--tickterial-output` 覆盖）；
- 常用参数：`--tickterial-workers`、`--tickterial-batch-size`、`--tickterial-batch-pause-ms`、`--tickterial-retry-jitter-ms`；
- 建议先配合 `--tickterial-validate` 进行下游一致性校验。

### 2.3 失败与重跑

- `--only-failures`：按 `failures.csv` 仅重跑失败窗口；
- `--failures-file`：自定义 failures 文件路径；
- `--strict-failures-csv`：坏行直接失败（适合 CI）；
- 失败日志默认在 `<data_root>/failures.csv`。

### 3. 更新 symbol 列表

```bash
datagrab update-symbols --limit 2000
```

#### Tickterial 特化更新

```bash
datagrab update-symbols --source tickterial
# 可配合 --asset-types 不传时，默认返回 stock/ashare；当使用 --source tickterial 时按 Dukascopy 互补品种更新
```

### 4. Wizard（预览确认）

`datagrab wizard` 会生成命令预览，支持回退/修正；可用于 update-symbols、download、validate 的交互式执行。

### 5. 数据检查（验数）

```bash
datagrab validate --root ./data --asset-type stock --symbol AAPL --interval 1d
datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d
# 或指定目录（更推荐）：
datagrab validate ./data/commodity --asset-type commodity --symbol GC=F --interval 1m --workers 4
```

#### 可选验数流程（推荐）

```bash
# 1) 下载一批示例数据（含 symbol 列表）
datagrab catalog --asset-type commodity --refresh

# 2) 下载 1m 并生成日志
datagrab download --asset-type commodity --symbols GC=F,SI=F --intervals 1m --start 2024-02-25 --end 2024-02-27 --data-root ./data --verbose

# 3) 直接复核并导出 issue

datagrab validate ./data/commodity --asset-type commodity --workers 4 --out quality_issues.jsonl --format jsonl

# 4) 重跑失败窗口
datagrab download --only-failures --failures-file ./data/failures.csv

# 5) 先检查 doctor（可选）
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

#### 5.1 Tickterial CSV 验数 / 修复 / 转换

```bash
datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --interval 1m --start 2016-01-01 --end 2016-01-02
datagrab repair --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --intervals 1d,5m,15m --output ./data/tickterial_csv
datagrab bridge --input-dir ./data/tickterial_csv --output-root ./data --asset-type commodity --symbol XAUUSD --interval 1m
```

---

## 命令行参考

| 命令 | 说明 |
|---|---|
| `datagrab update-symbols [--asset-types stock,ashare] [--limit N]` | 刷新美股与A股目录（network-only） |
| `datagrab update-symbols --source tickterial` | 刷新 Dukascopy 互补品种 symbol 清单 |
| `datagrab wizard` | 交互式预览并确认更新/下载/验数 |
| `datagrab catalog --asset-type <类型> [--refresh] [--limit N]` | 拉取并缓存 symbol；不加 `--refresh` 先读缓存 |
| `datagrab catalog --refresh --refresh-all` | 同时刷新美股 + A 股 |
| `datagrab download --asset-type <类型> --symbols A,B [--intervals 1d/5m/15m] [--start <YYYY-MM-DD>] [--end <YYYY-MM-DD>] [--adjust auto\|back\|front\|backward\|none] [--only-failures] [--failures-file <path>] [--strict-failures-csv] [--verbose] [--download-log-file <path>]` | 按参数批量抓取 |
| `datagrab download --source tickterial --symbols <symbol> [--tickterial-output <path>] [--tickterial-workers N] [--tickterial-batch-size N] [--tickterial-batch-pause-ms N] [--tickterial-retry-jitter-ms N] [--tickterial-strict-validate]` | Dukascopy 走并发抓取，默认产出 CSV |
| `datagrab validate --format csv [--tickterial-output <path>] [--symbol <sym>] [--interval <itv>] [--start <YYYY-MM-DD>] [--end <YYYY-MM-DD>]` | 扫描 tickterial CSV 并输出质量报告 |
| `datagrab repair --symbol <sym> --start <YYYY-MM-DD> --end <YYYY-MM-DD> [--intervals <itv,...>]` | 重建 tickterial CSV（支持 dry-run/strict） |
| `datagrab bridge --input-dir <path> [--output-root <path>] [--asset-type <类型>] [--symbol <sym>] [--interval <itv>]` | 批量将 tickterial CSV 转为 Parquet |
| `datagrab doctor [--json] [--strict] [--check-scope] [--asset-type <类型>] [--symbol AAPL] [--interval 1d]` | 健康检查 |
| `datagrab check-deps [--auto-install]` | 依赖检查 |
| `datagrab export --engine vectorbt\|backtrader --input <parquet> --output <path>` | 导出为 vectorbt/backtrader 输入 |
| `datagrab validate [path] [--root <path>] [--asset-type <类型>] [--symbol <sym>] [--interval <itv>] [--out <path>] [--format jsonl\|csv] [--summary] [--workers N]` | 扫描 parquet 质量；`path` 与 `--root` 语义见下文 |

### 全局与目录筛选参数（catalog / download / validate 通用）

- `--config` / `-c`：配置路径（YAML/TOML）
- `--data-root`：覆盖 `storage.data_root`
- `--verbose`：开启更详细日志
- `--include` / `--exclude`：按 symbol 正则
- `--include-prefix` / `--exclude-prefix`：按 symbol 前缀
- `--include-symbols` / `--exclude-symbols`：白名单/黑名单
- `--include-name` / `--exclude-name`：名称正则
- `--include-exchange` / `--exclude-exchange`：交易所（支持中文）
- `--include-market` / `--exclude-market`：板块（如「科创板」）
- `--include-fund-category` / `--exclude-fund-category`：基金子类
- `--only-etf` / `--exclude-etf`：仅 ETF / 排除 ETF
- `--only-fund` / `--exclude-fund`：仅基金 / 排除基金

---

## 统一参数与 failures 契约

### 统一参数失败行为

- `--asset-type`、`--log-level`、`--workers`、`--intervals`、日期参数、`--adjust` 由 Pydantic 校验；
- 校验失败返回非零退出码并附带明确错误。

### failures.csv v1

- 字段：`version`、`symbol`、`interval`、`start`、`end`、`asset_type`、`adjust`、`reason`、`created_at`。
- `--only-failures` 会兼容旧文件中缺失列。
- `--strict-failures-csv` 下坏行会直接失败。

---

## 资产类型与数据源

| asset_type | 数据源 | 说明 |
|---|---|---|
| stock | yfinance | 美股等，目录来自 NASDAQ 列表 |
| ashare | baostock | A 股，目录与复权由 baostock 提供 |
| forex / crypto / commodity | yfinance + 预设列表 | 下载走 yfinance |
| tickterial | tickterial | Dukascopy 互补品种，下载产物为 CSV 主输出 |

---

## 常见问题

**Q：时间与时区？** 统一使用北京时区（Asia/Shanghai）。

**Q：yfinance 429 或封禁？** 降低 `rate_limit.requests_per_second`、提高抖动、配置代理。

**Q：commodity/期货目录如何验数？** 直接传目录扫描更稳妥：`datagrab validate ./data/commodity --asset-type commodity --symbol GC=F --interval 1m`。

**Q：下载日志如何配置？** 默认落盘 `<data_root>/logs/download_YYYYMMDD_HHMMSS.log`，或通过 `--download-log-file` 指定。

**Q：1m 数据是否支持？** yfinance 通常支持；baostock 若不支持则用更高粒度。

**Q：check-deps --auto-install 仍报 missing？** 安装后会自动重试 import，仍失败时根据提示安装缺失依赖。

**Q：`datagrab update-symbols --source tickterial` 的作用是什么？** 用于更新 Dukascopy 互补品种清单，可用于 tickterial 下载任务前的可观测预热。
