# 财采 (DataGrab) — 架构说明

## 定位

财采是**数据下载与整理**程序，不绑定单一回测引擎；时区统一北京，支持复权与多粒度，数据以 Parquet 按标的存储，便于多种回测引擎使用。

---

## 整体数据流

```
数据源层 (yfinance / baostock / 预留扩展)
        ↓
目录服务 (拉取/缓存/过滤) → CLI 指定下载任务
        ↓
下载调度 (任务队列、断点判断、增量区间、并发+限速)
        ↓
解析与校验 (Polars) → 写入 Parquet (按标的+粒度+范围)
        ↓
存储 (data_root/asset_type/symbol/*.parquet) → 可选导出 (VectorBT/Backtrader 等)
        ↓
健康检查（doctor）→ 配置/目录/依赖/网络/scope 快速自检
```

- **数据源层**：`DataSource` 抽象（列出标的、拉取 OHLCV），首实现 `YFinanceDataSource`、`BaostockDataSource`；限速与随机休眠在数据源侧统一实现。
- **统一校验层**：命令行参数、配置与 `failures.csv` 都在入口/服务边界通过 `pydantic` 校验，非法输入在执行前失败返回。
- **目录服务**：按资产类型拉取或读缓存，支持多日重试与回退到上次缓存；过滤（交易所、板块、基金子类、名称包含·排除）通过 `datagrab catalog` 命令行参数完成。
- **下载调度**：按「标的 + 粒度 + 日期范围」组任务，判断已有 Parquet 是否覆盖区间，未覆盖则仅拉增量；合并后写单文件，减少小文件。
- **存储**：`{data_root}/{asset_type}/{symbol}/{interval}_{start}_{end}.parquet`；可选按回测引擎导出。
- **健康检查（doctor）**：在执行下载/验数前提供快速一致性检查，覆盖配置、目录权限、依赖、网络与 scope，降低下游故障定位时间。

### 典型执行链路（面向交接）

1. `doctor` 先做环境与参数预检；
2. `catalog` 更新 symbol 缓存；
3. `download` 按 symbol/interval/start/end 下发任务；
4. `writer` 判断已有区间，决定 skip、增量或覆盖写；
5. `validate` 扫描 parquet 输出质量告警；
6. 必要时 `export` 转换目标引擎输入。

---

## 模块划分

| 模块/包 | 职责 |
|--------|------|
| `datagrab.cli` | 命令行入口，解析 catalog / download / check-deps / export / validate，加载配置并注入依赖 |
| `datagrab.validation` | Pydantic 验证模型（CLI 参数、配置、failures）及错误映射 |
| `datagrab.config` | 配置加载（YAML/TOML）、默认值、环境变量覆盖、FilterConfig 等 |
| `datagrab.sources.base` | `DataSource` 抽象、`SymbolInfo` / `OhlcvResult` |
| `datagrab.sources.yfinance_source` | 美股等 yfinance 实现，限速、429 退避、复权参数 |
| `datagrab.sources.baostock_source` | A 股 baostock 实现 |
| `datagrab.sources.router` | 按 `asset_type` 路由到对应数据源 |
| `datagrab.pipeline.catalog` | 目录拉取、缓存、过滤（含交易所/板块/基金子类别名与规则） |
| `datagrab.pipeline.downloader` | 任务构建、断点/增量逻辑、并发执行、失败列表写入与仅重跑 |
| `datagrab.pipeline.writer` | Parquet 路径规则、已有区间读取、合并写入、原子写临时后 replace |
| `datagrab.storage.schema` | OHLCV 列名与类型、复权字段 |
| `datagrab.storage.quality` | 数据质量模型（QualityIssue、Severity）、问题列表写出 jsonl/csv |
| `datagrab.storage.validate` | Parquet 扫描（iter_parquet_files）、单文件校验（validate_parquet_file）、指标与 QualityIssue 生成 |
| `datagrab.storage.export` | 导出 VectorBT（NumPy）、Backtrader（CSV）等 |
| `datagrab.rate_limiter` | 请求/秒 + 随机抖动、429 指数退避 |
| `datagrab.timeutils` | 北京时区、日期解析与路径格式化 |
| `datagrab.cli::_run_doctor` | 健康检查入口：配置、文件系统、依赖、网络、scope 预检 |

---

## 数据目录结构

```
<data_root>/
  catalog/
    stock_symbols.csv       # 美股目录缓存
    ashare_symbols.csv     # A 股目录缓存
  stock/
    AAPL/
      1d_20200101_20241231.parquet
  ashare/
    sh.600000/
      1d_20200101_20241231.parquet
  failures.csv               # 本次运行失败任务列表
  quality_issues_<时间戳>.jsonl  # 数据检查导出的质量问题（可选）
  quality_issues_<时间戳>.csv
failures.csv（v1，包含 version / created_at）

### validate 扫描语义（近期修订）

- `datagrab validate <path>`：`<path>` 一律作为扫描起点递归查找 `.parquet`；
- 递归扫描后再应用 `--asset-type / --symbol / --interval` 过滤，避免手工拼接路径带来的漏扫；
- `--root` 与 `<path>` 等价，优先级为 `path` > `--root`；
- 当你的目录是 `.../commodity` 时，直接执行 `datagrab validate .../commodity` 最容易避开资产层级误拼接问题。

### 容错与兼容

- `download` 遇到 yfinance 的偶发异常时，会将异常信息写入 `failures.csv`；在 CLI 可见 `timeline` 与错误原因；
- `validate` 在单文件扫描异常时采用回退路径，优先保证问题可见且不中断全局统计。
```

文件名格式：`{interval}_{start_yyyymmdd}_{end_yyyymmdd}.parquet`。增量后同标的同粒度会合并为单文件。

---

## 资产类型与数据源

| asset_type | 数据源 | 说明 |
|------------|--------|------|
| stock | yfinance | 美股等，目录来自 NASDAQ/其他交易所列表 |
| ashare | baostock | A 股，目录与复权由 baostock 提供；基金子类同样遵循 baostock 口径 |
| forex / crypto / commodity | yfinance + 预设列表 | 目录为内置预设，下载走 yfinance |

