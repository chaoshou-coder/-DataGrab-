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

## 代码与文档

- 新增功能请尽量补充测试与 README/文档说明。
- 配置项、CLI 参数变更请在 `README.md` 与 `datagrab.example.yaml` 中体现。
- 保持对 Python 3.11 的兼容性，避免使用过新语法。

## Issue

- Bug 报告请尽量包含：环境（Python 版本、操作系统）、复现步骤、报错信息。
- 功能建议可简述使用场景与期望行为。

再次感谢你的贡献。
