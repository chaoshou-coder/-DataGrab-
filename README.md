# 财采 (DataGrab)

**财采（DataGrab）**是一款高并发、可断点续传与增量更新的历史行情下载器，覆盖美股（Yahoo Finance）、A 股（baostock）以及 Dukascopy 补充源（tickterial）。

项目面向三类使用场景：可复用的 CLI 运维链路、可验证的质量约束、以及可直接用于回测引擎的标准化存储。

## 功能概览

| 能力 | 说明 |
|---|---|
| 多市场支持 | 美股/ETF（`stock`）、A 股（`ashare`）、forex/crypto/commodity（预设名单） |
| tickterial 原始源 | `--source tickterial` 拉取 Dukascopy 互补品种，默认产出 CSV |
| 下载与断点续传 | 按标的+粒度+时间窗判断是否覆盖，支持仅重跑失败 |
| 断网/限流韧性 | 重试、随机抖动、并发、失败窗口记录 |
| 校验与失败追溯 | `validate` 与 `failures.csv` 验收闭环 |
| 质量导出 | `vectorbt/backtrader` 导出，或直接消费 Parquet |
| tickterial 辅助命令 | `validate`/`repair`/`bridge` 子命令 |
| 兼容入口 | `datagrab` 为主入口，旧脚本保留兼容 wrapper |
| 目录与筛选 | `catalog` + 命令参数（前缀、交易所、基金子类等） |
| 配置能力 | YAML/TOML + 环境变量覆盖 + `pydantic` 统一校验 |

详细机制说明见：[架构](docs/ARCHITECTURE.md) | [技术说明](docs/TECHNICAL.md) | [使用说明](docs/USAGE.md) | [tickterial 运维](docs/tickterial_ops/README.md)

## 快速上手（3 分钟）

### 一次最小闭环

```bash
datagrab check-deps --auto-install
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab catalog --asset-type stock --refresh
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2020-01-01 --end 2020-12-31 --data-root ./data
datagrab validate ./data/stock --asset-type stock --symbol AAPL --interval 1d
datagrab export --engine vectorbt --input ./data/stock/AAPL/1d_20200101_20201231.parquet --output ./data/AAPL.npz
```

### tickterial 链路（CSV 主输出）

```bash
datagrab download --source tickterial --symbols XAUUSD --intervals 1m,5m,15m,1d --start 2016-01-01 --end 2016-01-02 --data-root ./data
datagrab validate --format csv --tickterial-output ./data/tickterial_csv --symbol XAUUSD --interval 1m --start 2016-01-01 --end 2016-01-02
datagrab repair --symbol XAUUSD --start 2016-01-01 --end 2016-01-02 --intervals 1d,5m,15m
datagrab bridge --input-dir ./data/tickterial_csv --output-root ./data --asset-type commodity --symbol XAUUSD --interval 1m
```

## 运行建议

- 常规流程：`doctor` → `catalog` → `download` → `validate`。
- 先补齐 symbol 后再跑下载：`datagrab update-symbols`；Dukascopy 品种需用 `--source tickterial`。
- 只处理异常时执行 `--only-failures` 重跑，避免全量重拉。

```bash
datagrab update-symbols --source tickterial
# datagrab update-symbols --source tickterial --limit 422
```

## 环境与安装

- 要求：Python 3.11+。
- 安装（示例）：

```bash
git clone https://github.com/chaoshou-coder/-DataGrab-.git
git checkout main
cd -DataGrab-
python -m venv .venv
.\.venv\Scripts\activate  # Windows PowerShell
pip install -e .
```

- 启动检查：`datagrab check-deps [--auto-install]`

## 文档索引

| 文档 | 说明 |
|---|---|
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | 数据流、目录结构、模块边界 |
| [docs/TECHNICAL.md](docs/TECHNICAL.md) | 技术选型、配置项、数据格式、日志与退出码 |
| [docs/USAGE.md](docs/USAGE.md) | CLI 参考、参数示例、常见问题 |
| [docs/doctor-runbook.md](docs/doctor-runbook.md) | doctor 失败排障流程 |
| [docs/tickterial_ops/README.md](docs/tickterial_ops/README.md) | Tickterial 下载/检验/修复/桥接运维 |
| [CONTRIBUTING.md](CONTRIBUTING.md) | 贡献与提交流程 |

## 许可与贡献

- 许可证：MIT，[LICENSE](LICENSE)
- 贡献说明：[CONTRIBUTING.md](CONTRIBUTING.md)
- 财采（DataGrab）—财经数据采集，强调可复现、可运维、可复用。
