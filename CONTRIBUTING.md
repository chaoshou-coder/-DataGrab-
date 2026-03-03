# 贡献指南

感谢你对 **财采 (DataGrab)** 的关注。欢迎通过 Issue 与 Pull Request 参与改进。

## 开发环境

- Python 3.11+
- 安装依赖：`pip install -e .`
- 建议工具：`ruff`、`mypy`、`pytest`

## 提交流程

1. Fork 仓库，在本地创建分支（如 `feature/xxx` 或 `fix/xxx`）。
2. 提交前请确保通过以下命令：
   - `ruff check src/ tests/`
   - `ruff format src/ tests/`
   - `pytest`
3. 在 PR 描述中附上变更摘要、验证命令与结果。
4. 如改动涉及行为变更，请提供最小复现步骤、命令和环境信息。
5. PR 发起前请确认是否同步更新了 `docs` 与 `datagrab.example.yaml`。

## 代码与文档规范

- 新增功能请同步补充/更新 README、`docs/USAGE.md`、`docs/ARCHITECTURE.md`、`docs/TECHNICAL.md`，并保留接口兼容性说明。
- 配置项、CLI 参数变更必须更新 `datagrab.example.yaml`。
- 遵循 `.cursor/rules/datagrab.mdc` 约定（日志、异常、可测性、类型注解）。

## Issue

- Bug 报告请尽量包含：环境（Python 版本、操作系统）、复现步骤、报错信息。
- 功能建议可简述使用场景与期望行为。

再次感谢你的贡献。
