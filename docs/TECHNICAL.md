# 财采 (DataGrab) — 技术说明

## 技术选型

- **语言与运行时**：Python 3.11+（asyncio、类型提示）
- **数据处理**：Polars 清洗与去重，PyArrow 写 Parquet；全链路避免 pandas（除 yfinance/baostock 返回后即时转 Polars）
- **并发与限速**：线程池并发下载；限速器：请求/秒 + 随机抖动，429 指数退避
- **CLI**：命令行参数驱动的数据拉取与下载流程；目录更新、任务执行、质量检查都通过子命令完成
- **统一校验层**：`pydantic` 统一校验 CLI 参数、配置文件与 `failures.csv` 记录（可控的 schema + 结构化错误）。
- **验证与降级层**：`validate` 与下载异常路径都有容错分支；下载侧对空数据/异常图形路径进行回退与重试。
- **数据质量检查**：`storage.validate` 扫描 Parquet（缺列、重复、OHLC 逻辑、时间 gap 等），产出 `QualityIssue`（ERROR/WARN），并可通过 CLI `validate` 导出 jsonl/csv
- **配置**：YAML 或 TOML，路径可由 `DATAGRAB_CONFIG`、数据根目录由 `DATAGRAB_DATA_ROOT` 覆盖
- **分类口径补充**：A 股筛选中的基金子类（`include_fund_category` / `exclude_fund_category`）以 baostock 标的元数据口径为准，CLI 仅提供中文别名映射。

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

### yfinance 实战兼容说明

- `download()` 在部分场景会返回 `None`、`dict` 异常结构或抛 `TypeError("'NoneType' object is not subscriptable")`；
- 当前实现采取两阶段策略：
  1. 主路径：`yf.download(...)`；
  2. 失败且符号命中可疑场景：切换到 `Ticker(ticker).history(...)` 重试；
- 若仍为空结果，则视作“本窗口无数据”写入 `empty/skip`，避免将偶发接口异常放大为全量失败。
- 重试层面仍保留 429 退避和整体尝试次数，下载失败最终可回写 `failures.csv` 供手工重试。

### 验数路径策略（新修订）

- validate 扫描起点可直接用 `datagrab validate <path>`，`<path>` 可以是 `data_root/<asset_type>`；
- 内部扫描会先做目录递归，再根据 `asset_type/symbol/interval` 过滤文件，减少路径拼接偏差；
- 典型例子：`datagrab validate E:\stock_data\DateGrab\commodity` 会正确落到 `/commodity/GC=F`、`/commodity/SI=F` 下的文件集合。

### `doctor` 可观测性（健康检查）

`doctor` 在执行其他命令前做系统级前置检查，帮助你快速判断是否应立即停下来修配置/网络，避免误以为是下载逻辑故障。

- `config`：加载 `datagrab` 配置、校验 schema（`rate_limit`、`storage`、`catalog`、`download`、`filters`）；
- `filesystem`：`data_root` 路径存在性、可创建性、可写性；
- `dependencies`：关键依赖 import 可用性；
- `network`：上游目录源连通性（区分核心端点与可选端点）；
- `scope_*`：复用 `validation.cli` 做参数一致性检查（symbol/interval/asset_type）。

网络检查采用分层策略：

- NASDAQ listed 与 otherlisted（核心）不可达会上浮为高优先级告警；
- Yahoo screener（crypto/forex/commodity）作为辅源，可达性问题会记为可见告警，但不会直接打断核心闭环；
- 告警明细会完整落在 `detail`，用于排障脚本和日志归档。

当你在 CI 使用 `--strict` 时，建议将 doctor 检查纳入发布门禁：  
`datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d`

### doctor JSON 报告字段（SRE/CI）

`datagrab doctor --json` 的输出可直接入日志平台，字段说明如下：

| 字段 | 类型 | 含义 | CI/SRE 关注项 |
|------|------|------|------|
| `timestamp` | ISO 时间串 | 检查完成时间 | 用于告警时序归档 |
| `status` | `ok` \| `warn` \| `fail` | 全局状态 | 非 `ok` 时触发人工/告警流程 |
| `strict_mode` | bool | 是否启用 `--strict` | 决定可见告警是否阻断 |
| `checks` | object | 检查项字典 | 按需聚合告警 |
| `checks.config` | object | 配置校验结果 | 关注 schema 与关键字段 |
| `checks.filesystem` | object | data_root 检查结果 | 关注可写/可访问 |
| `checks.dependencies` | object | 依赖 import 检查 | 关注 Python 环境一致性 |
| `checks.network` | object | 网络连通性检查 | 区分核心与可选端点 |
| `checks.scope_symbols` | object | symbol 语义校验 | 与 `download` 入参一致性 |
| `checks.scope_interval` | object | interval 校验 | 粒度策略与可下载范围 |
| `checks.scope_cli` | object | CLI 参数联合校验 | 运行前参数一致性 |

每个 check 的对象均包含 `status`、`message`、`detail`；请优先对 `detail` 做结构化落库，便于自动化分析。

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
- 质量问题分级：
  - `ERROR`：缺少关键列、Parquet 读取失败等，会使 `validate` 最终退出码 > 0；
  - `WARN`：重复时间戳、OHLC 逻辑问题、gap 超阈值等，可用于告警和逐步修复；
- 结果统计默认聚合到 `issue_files`、`error/warn`、`issues` 并在命令行打印。

### 质量规则（摘录）

- 缺列：`datetime`、`close` 为 ERROR，`open/high/low/volume` 仅 WARN；
- `close` 空值、负值、OHLC 逆序为 WARN；
- `datetime` 重复与跨度间隔异常为 WARN；gap 阈值依据 interval 粗略设置（`d/w/mo/m` 等）。

### 日志

- 下载模块默认仅保留 `timeline` 和异常输出；
- `--verbose` 会打开更细粒度终端日志；
- 下载日志文件始终包含详细请求/解析链路，便于排查 429、cookie、chart 空返回等问题。

### CLI 退出码

- `0`：成功完成，未出现严格错误。
- `1`：任务处理完成但存在严重问题（`validate` 有 ERROR、下载失败列表非空等）。
- `2`：参数解析/配置错误或关键运行异常。

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
