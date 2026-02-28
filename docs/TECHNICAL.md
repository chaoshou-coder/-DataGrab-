# 财采 (DataGrab) — 技术说明

## 技术选型

- **语言与运行时**：Python 3.11+（asyncio、类型提示）
- **数据处理**：Polars 清洗与去重，PyArrow 写 Parquet；全链路避免 pandas（除 yfinance/baostock 返回后即时转 Polars）
- **并发与限速**：线程池并发下载；限速器：请求/秒 + 随机抖动，429 指数退避
- **CLI**：命令行参数驱动的数据拉取与下载流程；目录更新、任务执行、质量检查都通过子命令完成
- **统一校验层**：`pydantic` 统一校验 CLI 参数、配置文件与 `failures.csv` 记录（可控的 schema + 结构化错误）。
- **数据质量检查**：`storage.validate` 扫描 Parquet（缺列、重复、OHLC 逻辑、时间 gap 等），产出 `QualityIssue`（ERROR/WARN），并可通过 CLI `validate` 导出 jsonl/csv
- **配置**：YAML 或 TOML，路径可由 `DATAGRAB_CONFIG`、数据根目录由 `DATAGRAB_DATA_ROOT` 覆盖

---

## 配置项

默认配置内置于代码，可通过 `--config` 指定文件覆盖。项目根目录提供 `datagrab.example.yaml` 示例。

| 区块 | 说明 |
|------|------|
| `rate_limit` | 请求/秒、抖动范围（jitter_min/jitter_max）、退避基数/上限 |
| `catalog` | 重试次数、休眠与退避、目录条数上限 |
| `filters` | 各类 include/exclude、only_etf/only_fund、基金子类；CLI 与配置文件使用 |
| `download` | 并发数、batch_days、重试、启动抖动 |
| `storage` | 数据根目录、是否增量合并 |
| `yfinance` | 代理、复权默认值（back/forward/none） |
| `baostock` | A 股复权默认值（back/front/none） |
| `intervals_default` | 默认粒度列表 |
| `asset_types` | 资产类型列表 |
| `validation` | 当前由 `pydantic` 统一约束（CLI、配置、failures） |

**限速建议**（yfinance）：社区建议请求/秒 **0.5～2**，避免被雅虎 429 或封 IP；CLI 参数可根据网络环境调低 `rate_limit.requests_per_second` 与 jitter。

---

## 数据格式与存储

- **Parquet 列**：`datetime`（北京时区）、`open`、`high`、`low`、`close`、`volume`，可选 `adjusted_close`。
- **复权**：schema 可区分复权类型；与 yfinance `auto_adjust` 对齐，CLI 支持不复权/后复权/前复权。
- **时区**：统一北京（Asia/Shanghai），「今日」与增量 end 边界按北京时间；failures 列表中的日期按北京时区解析。

### failures 契约（v1）

- 字段：`version`、`symbol`、`interval`、`start`、`end`、`asset_type`、`adjust`、`reason`、`created_at`。
- `--only-failures` 会按该契约读取重跑任务。
- `--strict-failures-csv` 打开时遇到坏行立即失败；默认关闭时坏行记录到日志并跳过。

### 验证失败与命令行为

- `--log-level` 与 `--asset-type` 非法输入将停止执行并返回错误。
- `validate` 支持 `--workers` 并可通过 `--out` 导出 `csv/jsonl`。

---

## 导出到回测引擎

- **VectorBT**：`datagrab export --engine vectorbt --input <path.parquet> --output <path.npz>`，得到 NumPy 数组与 datetime。
- **Backtrader**：`datagrab export --engine backtrader --input <path.parquet> --output <path.csv>`，兼容 GenericCSVData。
- 其他引擎可自行读取 Parquet（Polars/PyArrow/pandas）后按需转换。

---

## 开发与测试

- **环境**：Python 3.11+，依赖见 `pyproject.toml`（yfinance, polars, pyarrow, httpx, pyyaml, rich, numpy, pandas, baostock, akshare, pydantic, tzdata 等）。
- **自检**：`datagrab check-deps [--auto-install]`；自动安装后会再次 import，仅仍失败才报缺失。
- **代码质量与测试**：
  ```bash
  ruff check .
  pytest   # 需本地保留 tests 目录时运行
  ```
