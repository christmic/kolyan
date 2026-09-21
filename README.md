# Kolyan

Kolyan 是一个以 Rust 为核心、面向个人 Agent 的多语言 Agent Runtime workspace。

项目目标不是一开始实现一个庞大的 Agent 平台，而是从一个清晰、可测试的 Agent Kernel 演进出：

```text
用户输入 → Turn → Step → LLM / Tool → 结束状态
```

## 当前阶段

当前仓库处于空间初始化阶段：已经确定模块边界、跨语言协议边界和 AI 协作约定，尚未实现具体的 Agent Loop。

Rust 工具链由 `rust-toolchain.toml` 固定为 `1.98.1`，以保证不同协作 Agent 和开发机器使用一致的编译版本。

## 目录入口

- `crates/`：Rust 核心库和运行时模块
- `apps/`：个人 Agent、CLI、TUI 等应用
- `protocols/`：跨语言事件和消息协议
- `schemas/`：JSON Schema、OpenAPI 等机器可读契约
- `examples/`：最小可运行示例
- `evals/`：Agent 行为回归测试
- `docs/architecture/`：架构说明
- `docs/decisions/`：架构决策记录

## 架构入口

- [项目说明](PROJECT.md)
- [模块地图](docs/architecture/module-map.md)
- [Agent 执行模型](docs/architecture/agent-execution-model.md)
- [多语言边界](docs/architecture/multi-language-boundary.md)
- [AI 协作指南](AGENTS.md)

CI 配置位于 `.github/workflows/ci.yml`，并自动使用根目录 `rust-toolchain.toml` 中声明的 Rust 版本。

## 本地开发检查

```sh
./scripts/setup-git-hooks.sh
./scripts/check.sh
```

项目 hooks 会在提交前检查格式、clippy 和 staged whitespace，并在 push 前运行完整 workspace 检查。

## License

Kolyan is dual-licensed under either the [MIT License](LICENSE-MIT) or
the [Apache License 2.0](LICENSE-APACHE), at your option.
