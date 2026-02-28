# 贡献指南

感谢你对 **财采 (DataGrab)** 的关注。欢迎通过 Issue 和 Pull Request 参与改进。

## 开发环境

- Python 3.11+
- 安装依赖：`pip install -e .`
- 代码风格：`ruff check .` / `ruff format .`
- 测试：`pytest`

## 提交流程

1. Fork 本仓库，在本地创建分支（如 `feature/xxx` 或 `fix/xxx`）。
2. 修改代码并确保通过 `ruff check .` 与 `pytest`。
3. 提交信息请简洁说明改动（中英文均可）。
4. 向上游仓库发起 Pull Request，描述变更动机与影响范围。

### 测试文件与提交约定（tests 忽略策略）

项目当前采用 `tests/` 忽略策略（`tests/` 为本地临时目录），PR 不再包含仓库内 `tests/` 的提交。

如你的改动涉及行为变更，请按以下方式提交：

1. 在 PR 描述中附上本次变更的验证命令与结果（例如：
   - `ruff check .`
   - `pytest`
2. 如需复现问题，提供最小重现步骤与环境信息（Python 版本、OS、命令参数）。
3. 若你有新增或更新的测试脚本，请将其放在评审说明中给出可执行路径与内容摘要，便于仓库维护者在评审中复现验证。
4. 若评审方需要临时测试代码，请在评审会话中单独交换测试文件，而不是直接提交到 `tests/` 路径中。

## 代码与文档

- 新增功能请尽量补充测试与 README/文档说明。
- 配置项、CLI 参数变更请在 `README.md`、`docs/USAGE.md` 与 `datagrab.example.yaml` 中体现，必要时同步 `docs/ARCHITECTURE.md`、`docs/TECHNICAL.md`。
- 保持对 Python 3.11 的兼容性，避免使用过新语法。

## Issue

- Bug 报告请尽量包含：环境（Python 版本、操作系统）、复现步骤、报错信息。
- 功能建议可简述使用场景与期望行为。

再次感谢你的贡献。
