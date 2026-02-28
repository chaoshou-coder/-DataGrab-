# 财采 (DataGrab)

**财采**（财经数据采集）是一款高并发、可断点续传与增量更新的多市场历史行情下载器。支持美股（Yahoo Finance）、A 股（baostock），仅提供 CLI，数据以 Parquet 按标的存储，覆盖从数据下载、校验到导出的全链路日常化运维。

- **项目昵称**：财采（CaiCai）
- **英文名**：DataGrab
- **定位**：数据下载与整理，不绑定单一回测引擎；时区统一北京，支持复权与多粒度，强调“可复现”与“可运维”。

---

## 功能概览


| 能力               | 说明                                                                            |
| ---------------- | ----------------------------------------------------------------------------- |
| **多市场**          | 美股/ETF（yfinance）、A 股（baostock）、外汇/加密货币/商品（预设列表）                               |
| **粒度**           | 日线 1d、周月 1wk/1mo、分钟 1m/5m/15m/30m/60m（依数据源支持）                                 |
| **断点续传**         | 按「标的+粒度+日期范围」判断已有 Parquet，跳过或仅拉增量                                             |
| **限速与重试**        | 可配置请求/秒、随机休眠、429 退避、失败列表与仅重跑                                                  |
| **目录与筛选**        | 通过 `catalog` + 命令行参数（include/exclude 前缀、交易所、基金子类、名称关键词等）进行筛选                  |
| **数据检查/验数**      | CLI `datagrab validate`：扫描 Parquet、输出质量问题（缺列/重复/OHLC 异常/gap），可导出 jsonl/csv    |
| **进度可见**         | CLI 下载任务会持续输出进度与日志，支持长任务场景下追踪执行状态                                             |
| **导出**           | VectorBT（NumPy）、Backtrader（CSV）等                                              |
| **失败任务清单**       | 默认写入 `data/failures.csv`（`version`+`created_at` 字段），可用 `--failures-file` 指定文件 |
| **参数与配置校验**      | CLI 参数与配置文件统一由 Pydantic 校验；非法参数会在入口快速失败                                       |
| **验数（validate）** | 支持递归目录或限定范围扫描，适配 `.../commodity` 直接验数到 `GC=F`、`SI=F` 等 symbol 子目录             |


详细说明见：[架构](docs/ARCHITECTURE.md) | [技术](docs/TECHNICAL.md) | [使用说明](docs/USAGE.md)

---

## 快速认知（3 分钟能跑通）

### 先建立三层心智模型

把 datagrab 当成「目录服务 -> 下载 -> 质量检查 -> 可选导出」四步：

- `catalog`：构建/刷新可下载标的列表（含过滤）；
- `download`：按 symbol + interval + 区间抓取并落盘；
- `validate`：检查 Parquet 文件质量，防止脏数据进入策略链路；
- `export`：按引擎格式输出 CSV/NPZ。

在一次实操会话里，建议始终先做「先查再跑」：

```bash
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab catalog --asset-type stock --refresh
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2024-12-31
datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d
```

### 你进入仓库后要先确认三件事

1. 数据保存在哪（`storage.data_root` / `--data-root` / `DATAGRAB_DATA_ROOT`）
2. 任务入口是谁（通常是 `datagrab` 命令 + `wizard`）
3. 失败任务与验数策略（`failures.csv`、`validate`）

### 常见工作流（推荐）

1. 先确认数据目录是否可写、配置是否可加载；
2. 先 `catalog` 拉取/更新 symbol 清单；
3. 再 `download` 下载；
4. 最后 `validate` 复核 parquet 质量；
5. 必要时 `export` 给回测引擎使用。

如果你直接接手现有数据，直接执行 `validate` 从目录下手工复核最省心。

## 环境与安装

- **要求**：Python 3.11+
- **安装**（推荐使用项目内虚拟环境）：
  ```bash
  git clone https://github.com/chaoshou-coder/-DataGrab-.git
  cd -DataGrab-   # 或 cd datagrab，视仓库目录名而定
  python -m venv .venv
  .venv\Scripts\activate   # Windows PowerShell
  # source .venv/bin/activate   # Linux/macOS
  pip install -e .
  ```
- **依赖自检**（可选）：`datagrab check-deps [--auto-install]`

---

## 快速开始

**CLI 快速开始**

```bash
datagrab doctor --json --check-scope --symbol AAPL --interval 1d  # 先做环境与参数健康检查
datagrab catalog --asset-type stock --refresh  # stock = 美股
datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31 --data-root .\data
```

## 最小成功案例 Checklist

按照此列表执行，若每一步判断结果与预期一致，说明流程可以进入下载闭环：

1. 先跑环境预检（非阻塞）：`datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d`，预期 `status` 为 `ok` 或 `warn`，`checks.config.status = ok`，`checks.filesystem.status = ok` 或仅 `warn: data_root 不存在（会自动创建）`，`checks.dependencies.status = ok`，`checks.network.status != fail`。
2. 拉取/刷新股票目录：`datagrab catalog --asset-type stock --refresh`，预期命令退出码 `0`，出现 catalog 输出且 `data/catalog/stock_symbols.csv` 可更新或保留历史缓存且无致命异常。
3. 下发一条小样本下载（确认闭环）：`datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-02 --data-root .\data`，预期退出码 `0`，生成 `data/stock/AAPL/1d_*.parquet`，并且结束日志里无 `download failed` 或 `ERROR` 告警。
4. 验数复核：`datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d`，预期退出码 `0`，`error` 计数为 `0`，若有 `warn` 则记录文件并进行人工确认。
5. （可选）导出为回测输入：`datagrab export --engine vectorbt --input data/stock/AAPL/1d_*.parquet --output data/AAPL.npz`，预期退出码 `0`，生成可直接被模型读取的 `npz` 文件。

先更新目录缓存，再执行下载，更多参数见 [使用说明](docs/USAGE.md)（查看“命令行参考”部分）。

**一键更新 symbol / wizard**

```bash
datagrab update-symbols --limit 2000
datagrab wizard   # 交互式预览并确认更新/下载/验数
```

默认只输出 `timeline` 与错误告警，详细日志可用 `--verbose`。

## 验数快速示例

```bash
# 验数给一个资产目录（含多个 symbol 子目录）
datagrab validate E:\stock_data\DateGrab\commodity

# 只验单个 symbol/粒度
datagrab validate E:\stock_data\DateGrab\commodity --symbol GC=F --interval 1m

# 使用根目录参数（等价）
datagrab validate --root E:\stock_data\DateGrab\commodity --asset-type commodity
```

## 日志与输出约定

- 下载日志默认落盘到 `<data_root>/logs/download_YYYYMMDD_HHMMSS.log`。
- `--download-log-file` 可覆盖日志文件路径；若目录不存在自动创建。
- 下载失败详情会在终端输出 `timeline` 与失败记录，同时详细栈写入日志文件。

### 关键环境变量

- `DATAGRAB_CONFIG`：配置文件路径（YAML/TOML）
- `DATAGRAB_DATA_ROOT`：`storage.data_root` 的环境变量覆盖值

### CLI 优先级（推荐记忆）

- 数据目录：`--data-root` > `storage.data_root` > `DATAGRAB_DATA_ROOT` > 示例配置中的 `storage.data_root`
- 验数扫描起点：`datagrab validate <path>` > `--root` > `--data-root` > `storage.data_root` > `DATAGRAB_DATA_ROOT`

## 可选验证流程

```bash
# 1) 生成 symbol 列表
datagrab catalog --asset-type stock --refresh

# 2) 下载 1m 并记录完整日志
datagrab download --asset-type commodity --symbols GC=F,SI=F --intervals 1m --start 2026-02-25 --end 2026-02-27 --data-root E:\stock_data\DateGrab --verbose

# 3) 直接复核 commodity 目录
datagrab validate E:\stock_data\DateGrab\commodity --workers 4 --out quality_issues.jsonl --format jsonl

# 4) 若有失败，按失败清单重跑
datagrab download --only-failures --failures-file E:\stock_data\DateGrab\failures.csv

# 5) 先跑诊断器，再进入下一阶段（可选）
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

## `doctor` 深入说明

`doctor` 的目标是让下载前的失败尽量在入口层被拦截，不再把时间浪费在下载重试。

### 检查项

- `config`：配置是否能加载、主机时区与关键字段是否齐备；
- `filesystem`：`data_root` 是否存在、是否可写、是否可创建缺失目录；
- `dependencies`：关键依赖是否可 import；
- `network`：上游目录源连通性；
  - 核心必达：NASDAQ listed 与 otherlisted；
  - 可选非必达：Yahoo screener（crypto/forex/commodity）；
- `scope_symbols`：输入 symbols 合规性（长度、字符白名单、路径安全）；
- `scope_interval`：时间粒度可解析；
- `scope_cli`：参数模型整体复用校验（与 `download` 一致）。

### 为什么有「可选端点告警」而不直接失败

某些环境下，Yahoo 的部分 screener 接口会返回 404 或短时不可用，但这不一定影响 `stock` 与核心流程。  
当前策略是：

- 核心端点不可达：提高告警等级；
- 可选端点不可达：输出 `warn`，但不阻断 `--strict` 的硬失败路径；
- 所有检查输出仍保留在 `detail` 中，便于你判断是否需要网络策略修复。

### 推荐命令

日常建议：

```bash
datagrab doctor --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

第一次接入建议执行三段：

1. `datagrab doctor`（确认入口）；
2. `datagrab catalog`（确认目录）；
3. `datagrab download`（确认链路）。

如果第三步失败，优先回到第一步的网络和作用域项再重试。

> `validate` 对目录不敏感于“symbol 名称中的符号”（如 `GC=F` / `SI=F`），可放心用于期货商品目录。

> 也可以先做 `datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d`，再决定是否进入下载闭环。

```bash
datagrab download --asset-type stock --symbols AAPL --intervals 1m --start 2026-02-26 --end 2026-02-28  # stock = 美股
datagrab --verbose download --asset-type stock --symbols AAPL --intervals 1m --start 2026-02-26 --end 2026-02-28  # stock = 美股
```

**更新目录缓存**

```bash
datagrab catalog --asset-type stock --refresh  # stock = 美股
datagrab catalog --refresh --refresh-all   # 美股 + A 股
```

自定义数据目录优先级：`--data-root > storage.data_root > DATAGRAB_DATA_ROOT`。
下载日志将自动落盘到 `<data_root>/logs/download_YYYYMMDD_HHMMSS.log`。
可通过 `--download-log-file` 指定自定义路径。

完整命令与筛选参数见 [使用说明](docs/USAGE.md)（查看“命令行参考”部分）。

## 统一校验与 failures 契约

- CLI 与配置均由 `pydantic` 进行统一校验：`--asset-type`（`stock`=美股，`ashare`=A股）、`--log-level`、日期、`--intervals`、并发、重试与限速参数都会在执行前被检查。
- `failures.csv` 契约字段为：`version`、`symbol`、`interval`、`start`、`end`、`asset_type`、`adjust`、`reason`、`created_at`。
- 重跑方式：
  - `--only-failures`：按 `failures.csv` 重跑；
  - `--strict-failures-csv`：文件中有任一脏行时立即退出并返回错误，便于 CI/批处理失败快速识别。

### 退出码说明

- `0`：成功且未发现错误级问题
- `1`：执行成功但存在 `ERROR` 风险（如 validate 有 ERROR、下载有失败任务）
- `2`：参数解析或严重运行时错误

---

## 文档索引


| 文档                                               | 内容                        |
| ------------------------------------------------ | ------------------------- |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)     | 架构、数据流、模块划分、目录结构、资产类型与数据源 |
| [docs/TECHNICAL.md](docs/TECHNICAL.md)           | 技术选型、配置项、数据格式、导出、开发与测试    |
| [docs/USAGE.md](docs/USAGE.md)                   | CLI 参考、筛选参数、基金子类、常见问题     |
| [docs/doctor-runbook.md](docs/doctor-runbook.md) | doctor 按问题现象的优先级修复脚本与实战排障 |


## doctor JSON 报告字段（SRE/CI 快速解析）

`datagrab doctor --json` 输出固定结构（便于日志聚合）：


| 字段                      | 类型                     | 含义                  |
| ----------------------- | ---------------------- | ------------------- |
| `timestamp`             | ISO 时间串                | 检查完成时间              |
| `status`                | `ok` | `warn` | `fail` | 全局结果；`warn` 表示有可见告警 |
| `strict_mode`           | bool                   | 是否加了 `--strict`     |
| `checks`                | object                 | 按检查项聚合的结果字典         |
| `checks.<name>.status`  | `ok` | `warn` | `fail` | 某项检查状态              |
| `checks.<name>.message` | string                 | 简要摘要（人可读）           |
| `checks.<name>.detail`  | string | null          | 详细诊断内容，建议纳入日志归档     |


常见 `checks` 子项：`config`、`filesystem`、`dependencies`、`network`、`scope_symbols`、`scope_interval`、`scope_cli`。

---

## 许可证与贡献

- **许可证**：MIT，详见 [LICENSE](LICENSE)
- **贡献**：欢迎 Issue 与 Pull Request，请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)

**财采 (DataGrab)** — 财经数据采集，简单可依赖。