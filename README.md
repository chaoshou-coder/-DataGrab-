# 财采 (DataGrab)

**财采**（财经数据采集）是一款高并发、可断点续传与增量更新的多市场历史行情下载器。支持美股（雅虎财经）、A 股（baostock），提供 Textual TUI 与完整 CLI，数据以 Parquet 按标的存储，便于回测与量化使用。

- **项目昵称**：财采（CaiCai）
- **英文名**：DataGrab
- **定位**：数据下载与整理，不绑定单一回测引擎；时区统一北京，支持复权与多粒度。

---

## 功能概览

| 能力 | 说明 |
|------|------|
| **多市场** | 美股/ETF（yfinance）、A 股（baostock）、外汇/加密货币/商品（预设列表） |
| **粒度** | 日线 1d、周月 1w/1mo、分钟 1m/5m/15m/30m/60m（依数据源支持） |
| **断点续传** | 按「标的+粒度+日期范围」判断已有 Parquet，跳过或仅拉增量 |
| **增量更新** | 已有数据到 D 则只拉 D 至今，合并为单文件，减少小文件 |
| **限速与重试** | 可配置请求/秒、随机休眠、429 指数退避、失败重试与 failures 列表 |
| **目录与筛选** | 先拉产品目录，支持交易所/板块/基金子类白名单与黑名单，支持中文别名 |
| **基金子类** | A 股识别 ETF/LOF/REIT/QDII/货币/债券/ETF 联接/分级等，可按子类筛选 |
| **导出** | 可选导出为 VectorBT（NumPy）、Backtrader（CSV）等格式 |

---

## 环境要求

- Python 3.11+
- 依赖：textual, yfinance, polars, pyarrow, httpx, pyyaml, rich, numpy, pandas, baostock, tzdata

---

## 安装

```bash
git clone https://github.com/chaoshou-coder/-DataGrab-.git
cd -DataGrab-
python -m pip install -e .
```

可选：依赖自检并自动安装缺失包

```bash
datagrab check-deps --auto-install
```

---

## 快速开始

**TUI 交互（推荐首次使用）**

```bash
datagrab tui
```

按提示选择资产类型 → 加载目录（可筛选）→ 选择标的与粒度、日期范围 → 配置并发与限速 → 执行下载。

**CLI 单次下载**

```bash
# 美股日线
datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31

# A 股日线（后复权）
datagrab download --asset-type ashare --symbols sh.600000,sz.000001 --intervals 1d --start 2020-01-01 --end 2024-12-31
```

---

## 命令行参考

| 命令 | 说明 |
|------|------|
| `datagrab tui` | 启动 Textual TUI |
| `datagrab catalog --asset-type <类型> [--refresh] [--limit N]` | 拉取并缓存目录，可选刷新与数量上限 |
| `datagrab download --asset-type <类型> --symbols A,B [--intervals 1d] [--start/--end]` | 按标的与区间下载 |
| `datagrab download --only-failures --failures-file <path>` | 仅重跑失败列表中的任务 |
| `datagrab check-deps [--auto-install]` | 检查/安装依赖 |
| `datagrab export --engine vectorbt\|backtrader --input <parquet> --output <path>` | 导出为指定引擎格式 |

**全局与目录筛选参数（catalog / download 均可使用）**

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

## TUI 使用说明

1. **选择资产类型**：stock（美股）、ashare（A 股）、forex、crypto、commodity。
2. **目录界面**：点击「加载目录」拉取列表；可填「包含过滤」「排除过滤」（支持 `re:正则`）；可填「交易所包含/排除」「板块包含/排除」；「数量上限」留空则用配置默认。
3. **选择标的**：在「手工输入 symbol」中填逗号分隔代码，或点「使用目录前 N」再点「下一步」。
4. **下载配置**：设置 intervals、起止日期、复权方式、并发数、请求/秒上限，然后「开始下载」。
5. **执行与结果**：进度条与日志实时刷新；失败任务会写入 `data/failures.csv`，可用 CLI `--only-failures` 重跑。

---

## 配置说明

默认配置内置于代码，可通过 `--config` 指定 YAML/TOML 覆盖。项目根目录提供 `datagrab.example.yaml` 示例。

**主要配置项**

| 区块 | 说明 |
|------|------|
| `rate_limit` | 请求/秒、抖动范围、退避基数/上限 |
| `catalog` | 重试次数、休眠与退避、目录条数上限 |
| `filters` | 各类 include/exclude 与 only_etf/only_fund、基金子类 |
| `download` | 并发数、batch_days、重试、启动抖动 |
| `storage` | 数据根目录、是否增量合并 |
| `yfinance` | 代理、复权默认值（auto/back/forward/none） |
| `baostock` | A 股复权默认值（back/front/none） |
| `intervals_default` | 默认粒度列表 |
| `asset_types` | 资产类型列表 |

环境变量：`DATAGRAB_CONFIG` 指定配置文件路径，`DATAGRAB_DATA_ROOT` 覆盖数据根目录。

---

## 数据目录结构

```
<data_root>/
  catalog/
    stock_symbols.csv      # 美股目录缓存
    ashare_symbols.csv     # A 股目录缓存
  stock/
    AAPL/
      1d_20200101_20241231.parquet
  ashare/
    sh.600000/
      1d_20200101_20241231.parquet
  failures.csv             # 本次运行失败任务（可选）
```

Parquet 列：`datetime`（北京时区）、`open`、`high`、`low`、`close`、`volume`，可选 `adjusted_close`。文件名格式：`{interval}_{start_yyyymmdd}_{end_yyyymmdd}.parquet`。

---

## 资产类型与数据源

| asset_type | 数据源 | 说明 |
|------------|--------|------|
| stock | yfinance | 美股等，目录来自 NASDAQ 列表 |
| ashare | baostock | A 股，目录与复权由 baostock 提供 |
| forex / crypto / commodity | yfinance + 预设列表 | 目录为内置预设，下载走 yfinance |

A 股交易所与板块代码（筛选时可写中文）：上交所(SSE)、深交所(SZSE)、北交所(BSE)；主板(MAIN)、科创板(STAR)、创业板(CHINEXT)、B 股(B) 等。

---

## 基金子类（A 股）

财采对 A 股标的做基金子类识别，用于筛选与展示：

| 子类代码 | 中文名 | 说明 |
|----------|--------|------|
| ETF | ETF | 交易型开放式指数基金 |
| LOF | LOF | 上市型开放式基金 |
| REIT | REITs | 不动产投资信托 |
| QDII | QDII | 合格境内机构投资者基金 |
| MONEY | 货币基金 | 货币型 |
| BOND | 债券基金 | 债券型 |
| ETF_LINK | ETF 联接 | 联接基金 |
| GRADED | 分级基金 | 分级 |
| FUND | 基金 | 其他基金 |

识别依据：baostock 证券类型 + 名称关键词 + 代码前缀（如 sh.50*/sz.15* 等）。筛选时 `--include-fund-category` / `--exclude-fund-category` 支持上述代码或中文别名（如「ETF联接」「货币基金」）。

---

## 导出到回测引擎

- **VectorBT**：`datagrab export --engine vectorbt --input <path.parquet> --output <path.npz>`，得到 NumPy 数组与 datetime。
- **Backtrader**：`datagrab export --engine backtrader --input <path.parquet> --output <path.csv>`，得到兼容 GenericCSVData 的 CSV。

其他引擎可自行读取 Parquet（Polars/PyArrow/pandas）后按需转换。

---

## 常见问题

**Q：时间与时区？**  
统一使用北京时区（Asia/Shanghai），「今日」与增量 end 边界按北京时间。

**Q：yfinance 报 429 或封禁？**  
调低 `rate_limit.requests_per_second`、增大 `jitter_min/jitter_max`，或配置代理 `yfinance.proxy`。

**Q：A 股目录拉取失败？**  
程序会多日重试并回退到上次缓存的目录文件；可 `--refresh` 强制重新拉取。

**Q：1m 分钟线？**  
yfinance 支持 1m；baostock 若服务端不支持 1 分钟会报错，可改用 5m/15m/30m/60m。

**Q：仅重跑失败任务？**  
`datagrab download --only-failures --failures-file data/failures.csv`（需先有一次下载生成 failures 文件）。

---

## 开发与测试

```bash
pip install -e ".[dev]"   # 若有 dev 依赖
ruff check .
pytest
```

---

## 许可证

本项目采用 MIT 许可证，详见 [LICENSE](LICENSE)。

---

## 贡献

欢迎提交 Issue 与 Pull Request。请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)。

---

**财采 (DataGrab)** — 财经数据采集，简单可依赖。

---

## 发布到 GitHub

1. 在 GitHub 已建仓库：<https://github.com/chaoshou-coder/-DataGrab->  
2. 本地在项目根目录执行：
   ```bash
   git remote add origin https://github.com/chaoshou-coder/-DataGrab-.git
   git branch -M main
   git add .
   git commit -m "chore: 财采 DataGrab 初始版本与文档"
   git push -u origin main
   ```
   若 `git add .` 曾报错，请确认已用本项目提供的 `.gitignore` 与 `.gitattributes`（见下）。
