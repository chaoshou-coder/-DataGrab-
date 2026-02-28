# `doctor` Runbook（问题现象优先级修复脚本）

本页用于 `datagrab doctor` 异常后的快速闭环处理，按“先后级”给出可复用脚本。

## 0) 通用预检查（必做）

先执行（两套环境任选其一）：

##### Windows（PowerShell）

```powershell
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

CI/批处理场景建议加：

##### Windows（PowerShell）

```powershell
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

## 1) 现象：404 或网络连通异常

### 1.1 判定

1) 看 `checks.network.status` 是否是 `fail` 或 `warn`。  
2) 检查 `checks.network.detail` 是否出现 `404`、`status=404`、`NameResolutionError`。

### 1.2 修复步骤

1) 先判断端点类型：

1) `nasdaq-listed`、`otherlisted` 为核心，先修复再放行。  
2) `yahoo-<type>-screener` 为可选端点，属于可见告警可先记录。

2) 验证 DNS 与基础连通：

##### Windows（PowerShell）

```powershell
python -c "import socket; print('dns ok:', socket.gethostbyname('query1.finance.yahoo.com'))"
```

##### Linux / macOS（bash）

```bash
python - << 'PY'
import socket
print("dns ok:", socket.gethostbyname("query1.finance.yahoo.com"))
PY
```

3) 检查代理是否生效后重跑 doctor：

##### Windows（PowerShell）

```powershell
$env:HTTP_PROXY = "http://127.0.0.1:7890"
$env:HTTPS_PROXY = "http://127.0.0.1:7890"
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
export HTTP_PROXY=http://127.0.0.1:7890
export HTTPS_PROXY=http://127.0.0.1:7890
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

4) 收敛判断：

1) 若核心端点失败：修复网络/DNS/代理后重试 doctor。  
2) 若仅 screener 失败：记日志后继续执行 `catalog`/`download` 核心链路。

## 2) 现象：依赖缺失

### 2.1 判定

1) 看 `checks.dependencies.status`。  
2) `warn`/`fail` 下 detail 一般带有 `missing=<pkg1>,<pkg2>`。

### 2.2 修复步骤

1) 先快速查看缺失项：

##### Windows（PowerShell）

```powershell
datagrab check-deps
```

##### Linux / macOS（bash）

```bash
datagrab check-deps
```

2) 手工补齐缺失依赖：

##### Windows（PowerShell）

```powershell
pip install -U <缺失包名>
```

##### Linux / macOS（bash）

```bash
pip install -U <缺失包名>
```

3) 重测 doctor：

##### Windows（PowerShell）

```powershell
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

4) 若仍异常，重建环境后重试：

##### Windows（PowerShell）

```powershell
python -m pip -V
pip install -e .
```

##### Linux / macOS（bash）

```bash
python -m pip -V
pip install -e .
```

## 3) 现象：data_root 不可写 / 无法创建

### 3.1 判定

1) 看 `checks.filesystem.status`。  
2) 常见 `warn`/`fail` 原因：

1) `data_root 写入失败`  
2) `data_root 不是目录`

### 3.2 修复步骤

1) 对齐生效路径（环境变量优先级）：

##### Windows（PowerShell）

```powershell
Write-Output $env:DATAGRAB_DATA_ROOT
python -c "import os;print(os.environ.get('DATAGRAB_DATA_ROOT',''))"
```

##### Linux / macOS（bash）

```bash
echo "$DATAGRAB_DATA_ROOT"
python -c "import os;print(os.environ.get('DATAGRAB_DATA_ROOT',''))"
```

2) 检查目录状态：

##### Windows（PowerShell）

```powershell
python -c "from pathlib import Path; import os; p=Path('你判断的 data_root 路径'); print('exists', p.exists(), 'is_dir', p.is_dir()); print('can_write', os.access(p, os.W_OK))"
```

##### Linux / macOS（bash）

```bash
python - << 'PY'
import os
from pathlib import Path
p = Path("你判断的 data_root 路径")
print("exists", p.exists(), "is_dir", p.is_dir())
print("can_write", os.access(p, os.W_OK))
PY
```

3) 逐项修复：

1) 目录不存在：先创建。  
2) 已被占用：释放占用后重试。  
3) 无权限：改用有写权限路径或调整权限。

4) 修复后复测：

##### Windows（PowerShell）

```powershell
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

##### Linux / macOS（bash）

```bash
datagrab doctor --json --check-scope --asset-type stock --symbol AAPL --interval 1d
```

## 4) 现象：scope 校验失败

### 4.1 判定

1) 看 `scope_symbols`、`scope_interval`、`scope_cli` 三项。  
2) 常见错误关键词包括 symbol 非法、interval 非法、asset_type 不在白名单。

### 4.2 修复步骤

1) 用 strict 重现并固定错误：

##### Windows（PowerShell）

```powershell
datagrab doctor --strict --json --check-scope --asset-type stock --symbol "AAPL" --interval 1d
```

##### Linux / macOS（bash）

```bash
datagrab doctor --strict --json --check-scope --asset-type stock --symbol "AAPL" --interval 1d
```

2) 按错误逐项处理：

1) symbol 清洗：移除 `/`、`\`、`..` 等路径字符。  
2) interval 调整为支持值：`1m`、`5m`、`15m`、`1d`、`1wk`、`1mo`。  
3) asset_type 使用 `stock`、`ashare`、`forex`、`crypto`、`commodity`。

3) 修复后复测与下载验证：

##### Windows（PowerShell）

```powershell
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-02
```

##### Linux / macOS（bash）

```bash
datagrab doctor --strict --json --check-scope --asset-type stock --symbol AAPL --interval 1d
datagrab download --asset-type stock --symbols AAPL --intervals 1d --start 2026-01-01 --end 2026-01-02
```

## 5) 一条可复制的排障总顺序

1) `doctor` 定位报错现象（按本页对应项）。  
2) 依赖问题先处理。  
3) data_root 问题再处理。  
4) 网络问题按核心/可选分层处理。  
5) scope 校验问题最终修正并回放。  
6) 重新跑 `doctor`，仅当 `status` 达到你环境约定阈值后，再进行下载动作。
