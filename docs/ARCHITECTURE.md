# 财采 (DataGrab) — 架构说明

## 定位

财采是**数据下载与整理**程序，不绑定单一回测引擎；时区统一北京，支持复权与多粒度。Parquet 为默认主存储格式，tickterial 采用 CSV 主产物并可桥接为 Parquet。

---

## 整体数据流

```text
数据源层 (yfinance / baostock / tickterial)
        ↓
目录服务 (拉取/缓存/过滤) → CLI 指定下载任务
        ↓
下载调度 (任务队列、断点判断、增量区间、并发+限速)
        ↓
解析与校验 (Polars/日志/失败记录)
        ↓
存储 (data_root/asset_type/symbol/*.parquet)
        ↓
健康检查（doctor）→ 配置/目录/依赖/网络/scope 自检
        ↓
可选导出 (VectorBT/Backtrader)
```

- **数据源层**：`DataSource` 抽象（列出标的、拉取 OHLCV）首实现 `YFinanceDataSource`、`BaostockDataSource`、`TickterialDataSource`；tickterial 通道复用 `datagrab.tickterial` 的并发+限流实现。
- **统一校验层**：命令行参数、配置与 `failures.csv` 统一由 `pydantic` 校验。
- **目录服务**：按资产类型拉取或读缓存，过滤参数（交易所、板块、基金子类、名称）由 `catalog` 和下载参数传入。
- **下载调度**：按「标的 + 粒度 + 日期范围」组任务，判断已有 Parquet 覆盖区间；若无缺口则增量续传。
- **存储**：`{data_root}/{asset_type}/{symbol}/{interval}_{start}_{end}.parquet`。
- **健康检查（doctor）**：运行前检查配置、目录、依赖、网络和参数作用域，减少下游排障成本。

### 典型执行链路（面向交接）

1. `doctor`：环境与参数预检；
2. `catalog`：更新 symbol 缓存；
3. `download`：按 symbol/interval/start/end 下发任务；
4. `writer`：合并区间并写入单文件；
5. `validate`：扫描 parquet 质量告警；
6. `export`：转换为回测引擎输入。

---

## 模块划分

| 模块/包 | 职责 |
|---|---|
| `datagrab.cli` | CLI 入口，统一解析命令与参数、加载配置、路由子命令 |
| `datagrab.validation` | `pydantic` 校验（CLI 参数、配置、failures）及错误映射 |
| `datagrab.config` | 配置加载（YAML/TOML）、默认值、环境变量覆盖 |
| `datagrab.sources.base` | `DataSource` 抽象、`SymbolInfo` / `OhlcvResult` |
| `datagrab.sources.yfinance_source` | 美股等 yfinance 实现，含限速与回退 |
| `datagrab.sources.baostock_source` | A 股 baostock 实现 |
| `datagrab.sources.tickterial_source` | tickterial（Dukascopy）数据源适配 |
| `datagrab.sources.router` | 按资产类型/来源路由到具体实现 |
| `datagrab.tickterial.download` | 向后兼容转发层：保留旧导入路径，真实逻辑位于 `fetch` / `aggregate` / `runner` |
| `datagrab.tickterial.fetch` | 并发小时级 tick 拉取、重试与限流 |
| `datagrab.tickterial.aggregate` | tick 聚合到 1m/5m/15m/1d，完整性校验 |
| `datagrab.tickterial.runner` | tickterial CLI 编排、CSV I/O、失败窗口与日志 |
| `datagrab.tickterial.check` | tickterial CSV 校验、告警报告 |
| `datagrab.tickterial.repair` | tickterial 重建与补缺（含 1d 从 1m 重建） |
| `datagrab.tickterial.bridge` | tickterial CSV 到 Parquet 的批量转换 |
| `datagrab.tickterial.symbols` | Dukascopy 品种表与在线刷新 |
| `datagrab.tickterial.exceptions` | tickterial 领域异常体系（FetchError / AggregationError / ...） |
| `datagrab.tickterial.common` | 共享常量与工具函数 |
| `datagrab.pipeline.catalog` | symbol 拉取、缓存、过滤 |
| `datagrab.pipeline.downloader` | 任务构建、断点/增量、并发执行 |
| `datagrab.pipeline.writer` | Parquet 路径规则、区间合并、原子替换写入 |
| `datagrab.storage.schema` | OHLCV 列定义与类型 |
| `datagrab.storage.quality` | `QualityIssue` 与异常等级模型 |
| `datagrab.storage.validate` | Parquet 扫描与单文件质量校验 |
| `datagrab.storage.export` | VectorBT / Backtrader 等导出 |
| `datagrab.rate_limiter` | 请求/秒 + 随机抖动 + 429 退避 |
| `datagrab.timeutils` | 北京时区与时间解析、窗口起止处理 |

---

## 数据目录结构

```text
<data_root>/
  catalog/
    stock_symbols.csv
    ashare_symbols.csv
  stock/
    AAPL/
      1d_20200101_20241231.parquet
  ashare/
    sh.600000/
      1d_20200101_20241231.parquet
  failures.csv                # 本次运行失败任务列表（v1）
  quality_issues_<时间戳>.jsonl # 可选质量问题导出
  quality_issues_<时间戳>.csv
  tickterial_csv/             # --source tickterial 的 CSV 主产物
```

- `datagrab validate <path>` 一律将 `<path>` 作为扫描起点递归查找 parquet；
- `--root` 与 `<path>` 语义一致，优先级为 `path` > `--root`；
- `failures.csv` 供 `--only-failures` 与 `--strict-failures-csv` 使用。

---

## 资产类型与数据源

| asset_type | 数据源 | 说明 |
|---|---|---|
| stock | yfinance | 美股等，目录来自 NASDAQ/otherlisted |
| ashare | baostock | A 股，目录与复权由 baostock 提供 |
| forex / crypto / commodity | yfinance + 预设列表 | 下载走 yfinance |
| tickterial（可选） | tickterial | `--source tickterial` 拉取 Dukascopy 原始 tick，产出 CSV 后可 bridge 到 Parquet |
