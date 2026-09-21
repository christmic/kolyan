# ADR 0001：采用多模块、多语言 workspace

## 状态

Accepted

## 决策

Kolyan 从初始化阶段采用 Rust workspace，并预留 `apps/`、`bindings/`、`protocols/`、`schemas/`、`evals/` 和 `docs/`。

## 原因

- Rust 适合作为稳定的 Agent Kernel 和 Runtime；
- 个人 Agent 产品与通用内核需要隔离；
- 未来需要 Python、TypeScript 或外部 Agent 接入；
- AI 协作需要清晰的模块所有权和机器可读边界。

## 影响

初始化阶段目录会比单 crate 更完整，但只建立边界，不提前实现所有模块。
