# 财采 (DataGrab) — 使用说明

## CLI 使用说明

## 推荐阅读顺序

1. 先看 `datagrab --help` 确认当前仓库支持的子命令；
2. 再看“命令行参考”确认参数含义和默认值；
3. 按“doctor -> 更新目录 -> 下载 -> 数据检查 -> 导出”跑一遍最小链路；
4. 最后阅读“常见问题”中的可复现场景与修复建议。

> 建议新同事先用一遍最小命令：
> `datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d`
> `datagrab catalog --asset-type stock --refresh`  
> `datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-31 --data-root ./data`  
> `datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d --out quality_issues.jsonl`

### 0. 入场前先运行 doctor

#### 命令

##### Windows（PowerShell）

```powershell
$env:PYTHONPATH = "src"
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
export PYTHONPATH=src
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
```

#### 输出解读

- `config`：配置加载、时区、允许资产类型、速率限制等是否正常；
- `filesystem`：`data_root` 是否存在且可写；
- `dependencies`：关键依赖 import 是否成功；
- `network`：核心源和可选源连通性；
  - 核心源不可用会拉高风险；
  - 可选源告警会保留在详情，不直接阻塞下载；
- `scope_symbols` / `scope_interval`：scope 参数是否可通过统一校验；
- `scope_cli`：组合参数在统一模型下是否仍通过。

> 发生 404、依赖缺失、`data_root` 不可写或 scope 校验失败时，先按 [doctor-runbook](doctor-runbook.md) 的优先级修复脚本排查。

#### `--strict` 何时用

CI 或批处理建议启用 `--strict`，把阻塞性告警视作失败码；本地排障时可先不带 `--strict`，优先定位可恢复问题。

### 1. 更新目录

先使用 `catalog` 拉取/更新目录，结合命令行筛选参数控制标的范围：

- 美股/ETF/基金目录：`datagrab catalog --asset-type stock`
- A 股目录：`datagrab catalog --asset-type ashare`
- 指定配置筛选：`datagrab catalog --asset-type stock --include-prefix A --exclude TEST`（更多筛选参数见下）

### 2. 下载

- `datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31`
- `datagrab download --asset-type ashare --symbols sh.600000 --intervals 1d --adjust back`
- 支持 `--only-failures` 与 `--failures-file` 对失败任务重跑。
- 默认模式：坏行只警告并跳过；使用 `--strict-failures-csv` 时，读取 `failures.csv` 遇到坏行会立即退出。
- 失败任务默认写 `data/failures.csv`，可用 `--failures-file` 覆盖输出路径。
- `--verbose`：开启下载命令的完整日志；默认只输出 timeline 与错误/告警。
- `--download-log-file`：下载过程完整日志落盘到指定文件，未指定则默认 `<data_root>/logs/download_YYYYMMDD_HHMMSS.log`。

### 3. 一键更新 symbol

- `datagrab update-symbols --limit 2000`

### 4. Wizard（预览确认）

- `datagrab wizard`：进入交互式流程，先预览再确认执行，支持更新 symbol、下载、数据检查(validate)。

#### Wizard 关键行为

- 模式 1：`更新 symbol`  
  - 生成 `datagrab update-symbols` 预览命令；
  - 支持返回上一步修正参数（输入 `b`）；
  - 确认后直接执行并输出结果。
- 模式 2：`下载数据`  
  - 覆盖 `asset-type/symbols/intervals/start/end/adjust/only-failures/strict` 等参数；
  - 可直接从当前 `data-root` 推导 `failures.csv` 与下载日志路径；
  - `symbols` 与 `intervals` 支持空值和逗号分隔列表。
- 模式 3：`数据检查(validate)`  
  - 可指定 `root`（或留空使用当前 data-root）；
  - 可继续指定资产类型、symbol、interval、输出格式与并发；
  - 预览命令可直接复制到脚本中复用，适合批处理固化。

### 5. 数据检查（验数）

- `datagrab validate --root data --asset-type stock --symbol AAPL --interval 1d`  
- `datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d`  
- `datagrab validate ./data/DateGrab/commodity`（直接粘入目录，自动递归扫描该目录下所有 parquet）
- 导出建议：`--out quality_issues.jsonl --format jsonl` 或 `--out quality_issues.csv --format csv`。
- `--summary` 模式用于只看总体汇总。

说明：`path` 为可选位置参数，扫描优先级为 `path` > `--root`。如果你要检查目录下某个类型（如 commodity），直接传目录路径会更稳妥：`datagrab validate ./data/DateGrab/commodity --asset-type commodity --workers 4`。

---

## 命令行参考

| 命令 | 说明 |
|---|---|
| `datagrab update-symbols [--limit N]` | 一次性刷新 `stock`（美股）+ `ashare`（A 股）symbol 缓存（network-only） |
| `datagrab wizard` | 交互式预览并确认执行更新/下载/验数 |
| `datagrab catalog --asset-type <类型> [--refresh] [--limit N]` | 拉取并缓存该资产的 symbol 列表；不加 `--refresh` 则优先读本地 |
| `datagrab catalog --refresh --refresh-all` | 联网更新美股 + A 股列表并写入 `data/catalog/` |
| `datagrab download --asset-type <类型> --symbols A,B [--intervals 1d/5m/15m] [--start <YYYY-MM-DD>] [--end <YYYY-MM-DD>] [--adjust auto\|back\|front\|backward\|none] [--only-failures] [--failures-file <path>] [--strict-failures-csv] [--verbose] [--download-log-file <path>]` | 按参数批量抓取行情；`--only-failures` 与 `--failures-file` 支持从 `failures.csv` 重跑 |
| `datagrab doctor [--json] [--strict] [--check-scope] [--asset-type <类型>] [--symbol AAPL] [--interval 1d]` | 运行健康检查：配置、文件系统、依赖、网络和 scope。支持严格模式和 JSON 输出。 |
| `datagrab check-deps [--auto-install]` | 检查/安装依赖 |
| `datagrab export --engine vectorbt\|backtrader --input <parquet> --output <path>` | 导出为 vectorbt/backtrader 输入格式 |
| `datagrab validate [path] [--root <path>] [--asset-type <类型>] [--symbol <sym>] [--interval <itv>] [--out <path>] [--format jsonl\|csv] [--summary] [--workers N]` | 扫描 Parquet 数据质量并输出问题列表；`path` 与 `--root` 语义见“说明”部分 |


### 全局与目录筛选参数（catalog / download 均可使用）

- `--config` / `-c`：配置文件路径（YAML/TOML）
- `--data-root`：临时覆盖 `storage.data_root`（优先级高于配置文件）
- `--verbose`：开启更详细控制台日志（download/其他命令）
- `--include` / `--exclude`：按 symbol 正则包含/排除
- `--include-prefix` / `--exclude-prefix`：symbol 前缀
- `--include-symbols` / `--exclude-symbols`：符号白名单/黑名单
- `--include-name` / `--exclude-name`：名称正则
- `--include-exchange` / `--exclude-exchange`：交易所（支持中文如「上交所」）
- `--include-market` / `--exclude-market`：板块（支持中文如「科创板」「主板」）
- `--include-fund-category` / `--exclude-fund-category`：基金子类（如 ETF、LOF、REIT、ETF联接）
- `--only-etf` / `--exclude-etf`：仅 ETF / 排除 ETF
- `--only-fund` / `--exclude-fund`：仅基金 / 排除基金

---

## 跨平台示例（Windows 与 Linux）

以下示例使用统一参数，复制相应代码块即可在不同系统执行。

### 最小链路（推荐）

##### Windows（PowerShell）

```powershell
$env:PYTHONPATH = "src"
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab catalog --asset-type stock --refresh
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-02 --data-root .\data
datagrab validate .\data\stock --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
export PYTHONPATH=src
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab catalog --asset-type stock --refresh
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-02 --data-root ./data
datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d
```

### 典型故障复现与修复（doctor 引导）

##### Windows（PowerShell）

```powershell
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab check-deps
```

##### Linux / macOS（bash）

```bash
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab check-deps
```

---

## 统一参数与 failures 契约

### CLI 参数失败时的行为

- `--asset-type`（`stock`=美股，`ashare`=A 股）、`--log-level`、`--workers`、`--intervals`、日期区间、`--adjust` 均由 Pydantic 校验。
- 校验失败时返回非零退出码，并输出明确错误信息，例如：`参数校验失败: ...`。

### failures.csv v1 字段

- `version`：固定写入 `"1"`。
- `symbol`/`interval`：必填。
- `start`/`end`：可选日期字符串。
- `asset_type`/`adjust`：可选，默认分别为 `stock` 与 `auto`。
- `reason`：失败原因（可为空）。
- `created_at`：写入时间（ISO 格式）。
- 重跑兼容：`--only-failures` 会兼容旧文件缺失列；在严格模式下坏行会直接失败并返回错误。

## 资产类型与数据源


| asset_type                 | 数据源             | 说明                                          |
| -------------------------- | --------------- | ------------------------------------------- |
| stock                      | yfinance        | 美股等，目录来自 NASDAQ 列表                          |
| ashare                     | baostock        | A 股，目录与复权由 baostock 提供；基金子类口径也以 baostock 为准 |
| forex / crypto / commodity | yfinance + 预设列表 | 目录为内置预设，下载走 yfinance                        |


A 股交易所与板块（筛选时可写中文）：上交所(SSE)、深交所(SZSE)、北交所(BSE)；主板(MAIN)、科创板(STAR)、创业板(CHINEXT)、B 股(B) 等。

---

## 基金子类（A 股）


| 子类代码     | 中文名    |
| -------- | ------ |
| ETF      | ETF    |
| LOF      | LOF    |
| REIT     | REITs  |
| QDII     | QDII   |
| MONEY    | 货币基金   |
| BOND     | 债券基金   |
| ETF_LINK | ETF 联接 |
| GRADED   | 分级基金   |
| FUND     | 基金     |


说明：A 股基金子类（`ETF/LOF/REIT/...`）依据 baostock 标的信息生成；工具内置的中文别名仅用于本地展示和筛选兼容，不会新增或改写 baostock 原始分类。  
如需核对口径，请以 baostock 官方接口返回值为准。  
筛选时 `--include-fund-category` / `--exclude-fund-category` 支持上述代码或中文别名（如「ETF联接」「货币基金」）。

---

## 常见问题

**Q：时间与时区？**  
统一使用北京时区（Asia/Shanghai），「今日」与增量 end 边界按北京时间。

**Q：yfinance 报 429 或封禁？**  
请求/秒建议 **0.5～2**。调低 `rate_limit.requests_per_second`、增大抖动，或配置代理 `yfinance.proxy`。

**Q：A 股目录拉取失败？**  
程序会多日重试并回退到上次缓存的目录文件；可 `--refresh` 强制重新拉取。

**Q：1m 分钟线？**  
yfinance 支持 1m；baostock 若服务端不支持 1 分钟会报错，可改用 5m/15m/30m/60m。

**Q：下载输出太多怎么办？**  
默认 `download` 只展示 `timeline` 和错误/警告，配合 `--verbose` 可恢复完整日志。

**Q：怎么检验 `datagrab` 的 commodity/期货目录（如 `GC=F`、`SI=F`）？**  
推荐用目录方式触发扫描：`datagrab validate ./data/DateGrab/commodity`。  
该命令会递归扫描该目录下全部 `*.parquet`，并按 symbol/asset/inerval 过滤规则逐个文件校验。

**Q：下载日志文件怎么配置？**  
默认会落盘到 `<data_root>/logs/download_YYYYMMDD_HHMMSS.log`，也可用 `--download-log-file` 指定路径。  
即使文件创建失败，命令依然可继续执行，终端仅保留告警/错误。

**Q：仅重跑失败任务？**  
`datagrab download --only-failures --failures-file data/failures.csv`（或自定义路径）。失败列表中的日期按北京时区解析。

**Q：`wizard` 里第 3 步“数据检查(validate)”点了执行仍不生效？**  
请检查是否在 wizard 中输入了 `root`，若 root 指向资产目录（如 `.../commodity`）则会在该目录递归扫描 parquet；  
也可直接在终端执行同等命令 `datagrab validate <root> --asset-type commodity`。

**Q：`doctor --strict` 为什么有时仍返回 `warn` 而不是失败？**  
是设计行为。`doctor` 会将可选网络端点（如部分 yfinance screener）视为告警，不会直接阻断核心闭环；  
但会把核心端点（如 Nasdaq listed/otherlisted）不可达提升为阻塞问题。  
如果你在 CI 场景需要阻断所有非预期状态，可配合环境预热并在网络策略稳定后再使用 `--strict`。

**Q：`doctor` 的 `--check-scope` 在做什么？**  
它会复用统一 CLI 校验逻辑，检查 `symbol`/`interval` 是否满足项目当前规则；  
典型收益是：提前发现参数语义不一致导致的 `download` 运行时失败（例如非法字符、interval 语法错误）。

**Q：下载报 `TypeError("'NoneType' object is not subscriptable")`？**  
这是 yfinance 某些窗口返回异常数据（`chart['result'] is None`）时常见行为。程序已加入历史接口回退路径；
仍建议缩小时间窗重试，或先单独下载该标的再次确认。

**Q：怎么检查 Parquet 数据有没有问题？**  

- **CLI**：`datagrab validate [--root <data_root>] [--asset-type stock(美股)] [--symbol AAPL] [--interval 1d] [--out <path>] [--format jsonl|csv]`。  
若存在 `severity=ERROR` 的问题，命令返回非 0 退出码，便于批处理/CI。

**Q：check-deps --auto-install 仍报 missing？**  
安装完成后会再次尝试 import，仅仍失败才报缺失；可手动 `pip install <包名>` 后重试。