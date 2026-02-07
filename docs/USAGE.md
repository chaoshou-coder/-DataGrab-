# 财采 (DataGrab) — 使用说明

## TUI 使用说明

### 1. 选择资产类型

下拉显示「股票 · 美股」「股票 · A股」等（A 股为股票子集），以及外汇、加密货币、商品。

### 2. 目录界面

- **过滤（多选，无需记代码）**  
  名称/代码包含·排除预设（如「包含：名称含科技」「排除：名称含ETF」）、交易所、板块、基金子类、标的类型（全部/仅股票/仅ETF/仅基金）。
- **加载/刷新**  
  点击「加载目录」或「刷新目录」拉取列表；TUI 下默认显示全部类型（股票+ETF+基金），不受配置文件 only_fund 等限制。
- **选择标的**  
  加载后显示「目录共 N 条，已选 M 个标的」；在列表中**空格键勾选**要下载的标的，或「使用目录前 N」填入上方。  
  **手工输入 symbol**（逗号分隔）若有内容，点「下一步」时**优先使用此处**，忽略勾选。

### 3. 下载配置

- **K线粒度**：下拉预设（日线、日线+1小时、仅周线等）或下方自定义（如 `1d,1h`）。
- **开始/结束日期**、**复权方式**（不复权/后复权/前复权）、**并发数**、**请求/秒上限**（推荐 0.5～2，TUI 提供 0.5/秒～3/秒 预设及自定义）。
- 点「开始下载」执行。

### 4. 执行与结果

进度条与日志实时刷新；失败任务写入 `data/failures_<asset_type>.csv`，可用 CLI `--only-failures` 重跑。下载任务界面会显示最近失败原因摘要，便于排查。

### 5. 数据检查（验数）

- **入口**：首屏（变量配置）或「下载任务管理」屏的 **「数据检查」** 按钮。
- **作用**：对当前 data_root 下指定资产类型 / symbol / interval 的 Parquet 做质量扫描（行数、日期范围、重复、缺列、空值、OHLC 逻辑、时间 gap 等），并列出 `QualityIssue`（ERROR/WARN）。
- **可选**：数据根目录可留空（使用配置）或临时填写；symbol/interval 留空默认使用当前已选，可勾选「扫描全部 symbols / 全部 intervals」扩大范围。
- **反馈**：点击「开始检查」后会有旋转指示器与「检查中… 已处理 N 文件」等进度；完成后可「导出 JSONL」或「导出 CSV」到 `data_root/quality_issues_<时间戳>.(jsonl|csv)`。

### 6. 进度可见性

TUI 中所有可能较长时间的操作都会显示**进度指示**（旋转图标 + 当前步骤文案），包括：联网获取目录、加载/刷新目录、加载全部筛选结果、数据检查、下载任务总进度。无需担心“点了没反应”，只要任务在跑就会有可见状态。

---

## 命令行参考

| 命令 | 说明 |
|------|------|
| `datagrab tui` | 启动 Textual TUI |
| `datagrab catalog --asset-type <类型> [--refresh] [--limit N]` | 拉取并缓存该资产的 symbol 列表；不加 `--refresh` 则优先读本地 |
| `datagrab catalog --refresh --refresh-all` | 联网更新美股 + A 股列表并写入 `data/catalog/` |
| `datagrab download --asset-type <类型> --symbols A,B [--intervals 1d] [--start/--end]` | 按标的与区间下载 |
| `datagrab download --only-failures [--failures-file <path>]` | 仅重跑失败列表中的任务 |
| `datagrab check-deps [--auto-install]` | 检查/安装依赖 |
| `datagrab export --engine vectorbt\|backtrader --input <parquet> --output <path>` | 导出为指定引擎格式 |
| `datagrab validate [--root <path>] [--asset-type <类型>] [--symbol <sym>] [--interval <itv>] [--out <path>] [--format jsonl\|csv] [--summary]` | 扫描 Parquet 数据质量并输出问题列表；`--summary` 仅打印汇总 |

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
请求/秒建议 **0.5～2**。调低 `rate_limit.requests_per_second`、增大抖动，或配置代理 `yfinance.proxy`。TUI 下载配置提供 0.5/秒（推荐）～3/秒 预设。

**Q：A 股目录拉取失败？**  
程序会多日重试并回退到上次缓存的目录文件；可 `--refresh` 强制重新拉取。

**Q：1m 分钟线？**  
yfinance 支持 1m；baostock 若服务端不支持 1 分钟会报错，可改用 5m/15m/30m/60m。

**Q：仅重跑失败任务？**  
`datagrab download --only-failures --failures-file data/failures_stock.csv`（或 `data/failures_<asset_type>.csv`）。失败列表中的日期按北京时区解析。

**Q：怎么直观检查 Parquet 数据有没有问题？**  
- **TUI**：在首屏或下载任务屏点击「数据检查」，选择范围后「开始检查」，可查看摘要与问题列表并导出 jsonl/csv。  
- **CLI**：`datagrab validate [--root <data_root>] [--asset-type stock] [--symbol AAPL] [--interval 1d] [--out <path>] [--format jsonl|csv]`。  
若存在 `severity=ERROR` 的问题，命令返回非 0 退出码，便于批处理/CI。

**Q：check-deps --auto-install 仍报 missing？**  
安装完成后会再次尝试 import，仅仍失败才报缺失；可手动 `pip install <包名>` 后重试。
