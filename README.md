# 财采 (DataGrab)

**财采**（财经数据采集）是一款高并发、可断点续传与增量更新的多市场历史行情下载器。支持美股（雅虎财经）、A 股（baostock），仅提供 CLI，数据以 Parquet 按标的存储，便于回测与量化使用。

- **项目昵称**：财采（CaiCai）
- **英文名**：DataGrab
- **定位**：数据下载与整理，不绑定单一回测引擎；时区统一北京，支持复权与多粒度。

---

## 功能概览

| 能力 | 说明 |
|------|------|
| **多市场** | 美股/ETF（yfinance）、A 股（baostock）、外汇/加密货币/商品（预设列表） |
| **粒度** | 日线 1d、周月 1wk/1mo、分钟 1m/5m/15m/30m/60m（依数据源支持） |
| **断点续传** | 按「标的+粒度+日期范围」判断已有 Parquet，跳过或仅拉增量 |
| **限速与重试** | 可配置请求/秒、随机休眠、429 退避、失败列表与仅重跑 |
| **目录与筛选** | 通过 `catalog` + 命令行参数（include/exclude 前缀、交易所、基金子类、名称关键词等）进行筛选 |
| **数据检查/验数** | CLI `datagrab validate`：扫描 Parquet、输出质量问题（缺列/重复/OHLC 异常/gap），可导出 jsonl/csv |
| **进度可见** | CLI 下载任务会持续输出进度与日志，支持长任务场景下追踪执行状态 |
| **导出** | VectorBT（NumPy）、Backtrader（CSV）等 |
| **失败任务清单** | 默认写入 `data/failures.csv`（`version`+`created_at` 字段），可用 `--failures-file` 指定文件 |
| **参数与配置校验** | CLI 参数与配置文件统一由 Pydantic 校验；非法参数会在入口快速失败 |

详细说明见：[架构](docs/ARCHITECTURE.md) | [技术](docs/TECHNICAL.md) | [使用说明](docs/USAGE.md)

---

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
datagrab catalog --asset-type stock --refresh
datagrab download --asset-type stock --symbols AAPL,MSFT --intervals 1d --start 2020-01-01 --end 2024-12-31
```

先更新目录缓存，再执行下载，更多参数见 [使用说明](docs/USAGE.md)（查看“命令行参考”部分）。

**更新目录缓存**

```bash
datagrab catalog --asset-type stock --refresh
datagrab catalog --refresh --refresh-all   # 美股 + A 股
```

完整命令与筛选参数见 [使用说明](docs/USAGE.md)（查看“命令行参考”部分）。

## 统一校验与 failures 契约

- CLI 与配置均由 `pydantic` 进行统一校验：`--asset-type`、`--log-level`、日期、`--intervals`、并发、重试与限速参数都会在执行前被检查。
- `failures.csv` 契约字段为：`version`、`symbol`、`interval`、`start`、`end`、`asset_type`、`adjust`、`reason`、`created_at`。
- 重跑方式：
  - `--only-failures`：按 `failures.csv` 重跑；
  - `--strict-failures-csv`：文件中有任一脏行时立即退出并返回错误，便于 CI/批处理失败快速识别。

---

## 文档索引

| 文档 | 内容 |
|------|------|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 架构、数据流、模块划分、目录结构、资产类型与数据源 |
| [docs/TECHNICAL.md](docs/TECHNICAL.md) | 技术选型、配置项、数据格式、导出、开发与测试 |
| [docs/USAGE.md](docs/USAGE.md) | CLI 参考、筛选参数、基金子类、常见问题 |

---

## 许可证与贡献

- **许可证**：MIT，详见 [LICENSE](LICENSE)
- **贡献**：欢迎 Issue 与 Pull Request，请先阅读 [CONTRIBUTING.md](CONTRIBUTING.md)

**财采 (DataGrab)** — 财经数据采集，简单可依赖。
