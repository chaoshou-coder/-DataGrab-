# 财采 (DataGrab) — 技术说明

## 技术选型

- **语言与运行时**：Python 3.11+（asyncio、类型提示）
- **数据处理**：Polars 清洗与去重，PyArrow 写 Parquet；全链路避免 pandas（除 yfinance/baostock 返回后即时转 Polars）
- **并发与限速**：线程池并发下载；限速器：请求/秒 + 随机抖动，429 指数退避
- **TUI**：Textual；多选/下拉均带中文名称，无需记代码；长时间操作（联网拉目录、加载目录、数据检查、下载）均有旋转指示器与进度文案
- **数据质量检查**：`storage.validate` 扫描 Parquet（缺列、重复、OHLC 逻辑、时间 gap 等），产出 `QualityIssue`（ERROR/WARN），可写 jsonl/csv；TUI「数据检查」与 CLI `validate` 共用该逻辑
- **配置**：YAML 或 TOML，路径可由 `DATAGRAB_CONFIG`、数据根目录由 `DATAGRAB_DATA_ROOT` 覆盖

---

## 配置项

默认配置内置于代码，可通过 `--config` 指定文件覆盖。项目根目录提供 `datagrab.example.yaml` 示例。

| 区块 | 说明 |
|------|------|
| `rate_limit` | 请求/秒、抖动范围（jitter_min/jitter_max）、退避基数/上限 |
| `catalog` | 重试次数、休眠与退避、目录条数上限 |
| `filters` | 各类 include/exclude、only_etf/only_fund、基金子类；CLI 与配置文件使用，TUI 另有预设多选 |
| `download` | 并发数、batch_days、重试、启动抖动 |
| `storage` | 数据根目录、是否增量合并 |
| `yfinance` | 代理、复权默认值（back/forward/none；TUI 仅提供此三项） |
| `baostock` | A 股复权默认值（back/front/none） |
| `intervals_default` | 默认粒度列表 |
| `asset_types` | 资产类型列表 |

**限速建议**（yfinance）：社区建议请求/秒 **0.5～2**，避免被雅虎 429 或封 IP；TUI 下载配置提供 0.5/秒～3/秒 预设及自定义。

---

## 数据格式与存储

- **Parquet 列**：`datetime`（北京时区）、`open`、`high`、`low`、`close`、`volume`，可选 `adjusted_close`。
- **复权**：schema 可区分复权类型；与 yfinance `auto_adjust` 对齐，TUI 仅提供不复权/后复权/前复权。
- **时区**：统一北京（Asia/Shanghai），「今日」与增量 end 边界按北京时间；failures 列表中的日期按北京时区解析。

---

## 导出到回测引擎

- **VectorBT**：`datagrab export --engine vectorbt --input <path.parquet> --output <path.npz>`，得到 NumPy 数组与 datetime。
- **Backtrader**：`datagrab export --engine backtrader --input <path.parquet> --output <path.csv>`，兼容 GenericCSVData。
- 其他引擎可自行读取 Parquet（Polars/PyArrow/pandas）后按需转换。

---

## 开发与测试

- **环境**：Python 3.11+，依赖见 `pyproject.toml`（textual, yfinance, polars, pyarrow, httpx, pyyaml, rich, numpy, pandas, baostock, akshare, tzdata 等）。
- **自检**：`datagrab check-deps [--auto-install]`；自动安装后会再次 import，仅仍失败才报缺失。
- **代码质量与测试**：
  ```bash
  ruff check .
  pytest   # 需本地保留 tests 目录时运行
  ```
