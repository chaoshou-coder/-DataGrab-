# 财采 (DataGrab) — 技术说明

## 技术选型

- **语言与运行时**：Python 3.11+。
- **数据处理策略**：主链路基于 `Polars` 与 `PyArrow` 处理 Parquet（ZSTD 压缩）；`tickterial` 链路内部使用 `pandas` 进行 tick 聚合与校验前处理。
- **并发与限速**：`RateLimiter`（Token Bucket + Sliding Window，支持突发容量、精确计数、jitter 和指数退避）；tickterial 源支持 `dukascopy-python` → `tickvault` → `tickterial` 三级自动回退。
- **CLI 与配置**：`argparse` + `pydantic`，通过统一模型完成参数与配置校验。
- **数据质量**：下载与验数分别形成 `QualityIssue`，支持 ERROR/WARN 分级与导出。
- **配置来源**：YAML/TOML 文件、环境变量覆盖。
- **分类口径补充**：A 股基金子类口径以 baostock 为准，CLI 仅做中文别名兼容映射。

---

## 配置项

默认配置内置于代码，可通过 `--config` 指定覆盖。项目根目录提供 `datagrab.example.yaml`。

| 区块 | 说明 |
|---|---|
| `rate_limit` | 请求频率、抖动、退避参数 |
| `catalog` | symbol 拉取重试次数与回退策略 |
| `filters` | 各类 include/exclude 筛选条件 |
| `download` | 并发数、批次长度、重试、启动抖动 |
| `storage` | 存储根目录、增量合并策略 |
| `yfinance` | 代理、复权默认值 |
| `baostock` | A 股复权默认值 |
| `intervals_default` | 默认下载粒度 |
| `asset_types` | 支持的资产类型 |
| `tickterial` | Dukascopy 源参数（见下） |
| `validation` | 校验与 failures 相关开关（通过 Pydantic 模型统一约束） |

### Tickterial 配置（`tickterial`）

| 字段 | 默认值 | 说明 |
|---|---:|---|
| `backend` | `auto` | `auto`/`tickterial`/`tickvault` |
| `cache_dir` | `.tick-data` | tickterial 原始缓存目录 |
| `tickvault_base_dir` | `.tick-data/tick_vault_data` | tick-vault 元数据与缓存目录 |
| `tickvault_workers` | `10` | tick-vault 下载并发 |
| `max_retries` | `6` | 单小时窗口最大重试次数 |
| `retry_delay` | `2.0` | 重试基准秒数 |
| `download_workers` | `10` | `tickterial` 后端并发 worker 数 |
| `batch_size` | `8` | 每批小时窗口数量 |
| `batch_pause_ms` | `1000` | 批次间隔（毫秒） |
| `retry_jitter_ms` | `300` | 重试/批次休眠抖动（毫秒） |
| `source_timestamp_shift_hours` | `8.0` | 时间戳移位（UTC 对齐） |
| `symbols` | `["XAUUSD", "XAGUSD"]` | 默认允许的 Dukascopy 标的 |
| `price_basis` | `last_or_price_or_mid` | 价格选取口径 |
| `ny_close_hour` | `17` | 日线 NY 收盘齐平小时 |
| `utcoffset` | `0` | 时区补偿参数 |
| `force_utc_timezone` | `true` | 强制 UTC 时区转换 |

---

## 数据源实战兼容

### YFinance（QuantDB 缓存层）

- **数据范围**：约 1970 年起，个股受限于上市日期；分钟级数据（1m-60m）**仅最近 730 天**
- **复权**：`none`（不复权）或 `auto`（自动复权），**不支持前复权/后复权**
- **回退机制**：httpx 请求失败（429 限速/网络错误）时自动降级到 YFinance 原生接口
- **缓存**：首次下载通过 QuantDB 缓存（SQLite），读取速度 < 18ms

### baostock（A 股）

- **数据范围**：日线 **1990-12-19** 起；分钟线（5/15/30/60 分钟）**1999-07-26** 起
- **复权**：`back`（后复权）/ `forward`（前复权）/ `none`（不复权）；回测推荐前复权
- **异常处理**：统一异常边界，失败记录 `failures.csv`

### akshare（A 股 fallback）

- **数据范围**：约 **2005 年**起；粒度以日线为主
- **复权**：部分接口不支持复权，需手动处理
- **回退时机**：baostock 请求异常时自动降级

### Dukascopy（tickterial）

- **数据范围**：主要货币对从 **2003 年 5 月**起；指数从 2011 年起
- **复权**：原始报价，不做复权处理
- **三级回退**：`dukascopy-python` → `tickvault` → `tickterial`（自动选择）

---

## 验数路径策略

- `validate` 支持直接扫描目录：`datagrab validate <path>`，<path> 一律作为递归扫描起点；
- 扫描后再按 `asset_type / symbol / interval / adjust` 过滤，减少路径拼接导致的漏扫；
- 当目录为子类型目录（如 `./data/commodity`）时可直接传目录执行最稳健验数。

---

## doctor 与可观测性

`doctor` 在执行其他命令前做系统级前置检查，覆盖配置、文件系统、依赖、网络与 scope 校验。

- `config`：配置加载与 schema 校验；
- `filesystem`：`data_root` 可访问性；
- `dependencies`：关键依赖是否 import 成功；
- `network`：核心与可选端点连通性；
- `scope_*`：参数一致性复用。

---

## doctor JSON 报告字段（SRE/CI）

`datagrab doctor --json` 的输出可直接入日志平台：

| 字段 | 含义 |
|---|---|
| `timestamp` | 检查完成时间（ISO） |
| `status` | `ok` \| `warn` \| `fail` |
| `strict_mode` | 是否启用 `--strict` |
| `checks` | 按检查项聚合的字典 |

`checks.*.status` 与 `checks.*.message`、`checks.*.detail` 可用于告警分级与告警脚本。

### failures 契约（v1）

- 字段：`version`、`symbol`、`interval`、`start`、`end`、`asset_type`、`adjust`、`reason`、`created_at`。
- `--only-failures` 会按该契约读取重试；
- `--strict-failures-csv` 打开后遇到坏行立即退出。

---

## 日志约定与环境变量

- 下载默认写入：`<data_root>/logs/download_YYYYMMDD_HHMMSS.log`，也可通过 `--download-log-file` 覆盖。
- `--verbose` 开启更细粒度终端日志；
- 日志约定与排障字段定义见 `docs/doctor-runbook.md`。

环境变量：

- `DATAGRAB_CONFIG`：覆盖配置文件路径（YAML/TOML）；
- `DATAGRAB_DATA_ROOT`：覆盖 `storage.data_root`。

---

## 数据格式与日志

- **Parquet 列**：`datetime`（Asia/Shanghai）、`open/high/low/close/volume`，可选 `adjusted_close`；
- **文件名**：`{interval}_{start_yyyymmdd}_{end_yyyymmdd}.parquet`；
- **时区**：统一北京时区处理；tickterial 通过 `force_utc_timezone` 与 `source_timestamp_shift_hours` 对齐。

---

## 退出码

- `0`：成功且未发现严重错误；
- `1`：任务处理完成但有 `ERROR` 风险（`validate` 错误、下载失败等）；
- `2`：参数/配置错误或关键运行异常。

---

## 导出与验证

- `datagrab export --engine vectorbt`：导出 NumPy 数组（含 datetime），比 backtrader 快 167x；
- 其他引擎可直接消费 Parquet。

---

## 依赖与开发

`pyproject.toml` 依赖至少包括：

- `yfinance`
- `polars`
- `pyarrow`
- `httpx`（异步 HTTP 客户端）
- `pyyaml`
- `numpy`
- `pandas`
- `baostock`
- `akshare`（A 股 fallback）
- `pydantic`
- `ruff`
- `mypy`
- `tickterial>=1.1.2`
- `dukascopy-node`（Dukascopy tick 下载）
- `tick-vault`（可选，`pip install .[tickvault]` 安装）

开发校验建议：

```bash
ruff check src/ tests/
pytest
ruff check src/ tests/ && pytest
```
