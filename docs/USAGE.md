# 财采 (DataGrab) — 使用说明

## CLI 使用说明

### 1. 更新目录

先使用 `catalog` 拉取/更新目录，结合命令行筛选参数控制标的范围：

- 股票/ETF/基金目录：`datagrab catalog --asset-type stock`
- A 股目录：`datagrab catalog --asset-type ashare`
- 指定配置筛选：`datagrab catalog --asset-type stock --include-prefix A --exclude TEST`（更多筛选参数见下）

### 2. 下载

- `datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31`
- `datagrab download --asset-type ashare --symbols sh.600000 --intervals 1d --adjust back`
- 支持 `--only-failures` 与 `--failures-file` 对失败任务重跑。
- 默认模式：坏行只警告并跳过；使用 `--strict-failures-csv` 时，读取 `failures.csv` 遇到坏行会立即退出。
- 失败任务默认写 `data/failures.csv`，可用 `--failures-file` 覆盖输出路径。

### 3. 数据检查（验数）

- `datagrab validate --root data --asset-type stock --symbol AAPL --interval 1d`  
- 导出建议：`--out quality_issues.jsonl --format jsonl` 或 `--format csv`。
- `--summary` 模式用于只看总体汇总。

---

## 命令行参考

| 命令 | 说明 |
|------|------|
| `datagrab catalog --asset-type <类型> [--refresh] [--limit N]` | 拉取并缓存该资产的 symbol 列表；不加 `--refresh` 则优先读本地 |
| `datagrab catalog --refresh --refresh-all` | 联网更新美股 + A 股列表并写入 `data/catalog/` |
| `datagrab download --asset-type <类型> --symbols A,B [--intervals 1d] [--start/--end] [--adjust auto\|back\|forward\|front\|none]` | 按标的与区间下载 |
| `datagrab download --only-failures [--failures-file <path>] [--strict-failures-csv]` | 仅重跑失败列表中的任务 |
| `datagrab check-deps [--auto-install]` | 检查/安装依赖 |
| `datagrab export --engine vectorbt\|backtrader --input <parquet> --output <path>` | 导出为指定引擎格式 |
| `datagrab validate [--root <path>] [--asset-type <类型>] [--symbol <sym>] [--interval <itv>] [--out <path>] [--format jsonl\|csv] [--summary] [--workers <N>]` | 扫描 Parquet 数据质量并输出问题列表；`--summary` 仅打印汇总 |

### 全局与目录筛选参数（catalog / download 均可使用）

- `--config` / `-c`：配置文件路径（YAML/TOML）
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

## 统一参数与 failures 契约

### CLI 参数失败时的行为

- `--asset-type`、`--log-level`、`--workers`、`--intervals`、日期区间、`--adjust` 均由 Pydantic 校验。
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

| asset_type | 数据源 | 说明 |
|------------|--------|------|
| stock | yfinance | 美股等，目录来自 NASDAQ 列表 |
| ashare | baostock | A 股，目录与复权由 baostock 提供 |
| forex / crypto / commodity | yfinance + 预设列表 | 目录为内置预设，下载走 yfinance |

A 股交易所与板块（筛选时可写中文）：上交所(SSE)、深交所(SZSE)、北交所(BSE)；主板(MAIN)、科创板(STAR)、创业板(CHINEXT)、B 股(B) 等。

---

## 基金子类（A 股）

| 子类代码 | 中文名 |
|----------|--------|
| ETF | ETF |
| LOF | LOF |
| REIT | REITs |
| QDII | QDII |
| MONEY | 货币基金 |
| BOND | 债券基金 |
| ETF_LINK | ETF 联接 |
| GRADED | 分级基金 |
| FUND | 基金 |

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

**Q：仅重跑失败任务？**  
`datagrab download --only-failures --failures-file data/failures.csv`（或自定义路径）。失败列表中的日期按北京时区解析。

**Q：怎么检查 Parquet 数据有没有问题？**  
- **CLI**：`datagrab validate [--root <data_root>] [--asset-type stock] [--symbol AAPL] [--interval 1d] [--out <path>] [--format jsonl|csv]`。  
若存在 `severity=ERROR` 的问题，命令返回非 0 退出码，便于批处理/CI。

**Q：check-deps --auto-install 仍报 missing？**  
安装完成后会再次尝试 import，仅仍失败才报缺失；可手动 `pip install <包名>` 后重试。
