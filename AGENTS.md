# Kolyan Agent 协作指南

本文件服务于 Codex、Claude Code 以及其他协作 Agent。

## 工作原则

1. 先阅读 `PROJECT.md`、相关模块的 `README.md` 和对应 ADR，再修改代码。
2. 不跨越模块边界引入隐式依赖；需要改变边界时先更新架构文档。
3. 核心执行逻辑保持小而明确，不在 `kolyan-core` 中加入模型供应商、数据库、UI 或具体工具逻辑。
4. 优先提交小批次、可验证的变化；每次修改说明影响范围和验证方式。
5. 不把实验性实现直接当成稳定协议；跨语言对象必须先更新 `protocols/` 或 `schemas/`。
6. 不引入秘密、真实凭据或用户数据。

## 工具链

项目 Rust 版本由根目录的 `rust-toolchain.toml` 固定。修改 Rust 代码前使用项目目录中的 `cargo`、`rustc`、`cargo fmt` 和 `cargo clippy`，不要自行切换到其他版本。

Git hooks 位于 `.githooks/`，首次初始化或 clone 后运行：

```sh
./scripts/setup-git-hooks.sh
```

统一检查入口是 `./scripts/check.sh`。不要绕过检查提交明显未格式化、无法通过 clippy 或无法测试的代码。

项目级 Agent 入口文件 `CLAUDE.md` 和 `.github/copilot-instructions.md` 复用本文件，不要分别维护重复规则。

## 修改路由

| 需求 | 首选位置 |
| --- | --- |
| Agent Loop、Turn、Step | `crates/kolyan-core` |
| 基础消息和事件类型 | `crates/kolyan-types`、`protocols/` |
| 模型接入 | `crates/kolyan-model` |
| Tool 注册和执行 | `crates/kolyan-tools` |
| 权限、审批、治理 | `crates/kolyan-policy` |
| 执行轨迹、观测 | `crates/kolyan-trace` |
| Session、Ledger、持久化 | `crates/kolyan-storage` |
| 个人 Agent 行为 | `apps/kolyan-agent` |
| CLI / TUI / 服务接口 | 对应 `apps/` 或 `crates/kolyan-cli` |

## 完成标准

每次修改至少应提供：

- 修改了什么，以及为什么；
- 影响了哪些模块和协议；
- 执行了哪些检查；
- 尚未解决的限制。
