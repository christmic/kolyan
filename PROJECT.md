# Kolyan

## 项目定位

Kolyan 是一个以 Rust 为核心、面向个人 Agent 的多语言 Agent Runtime workspace。

仓库从初始化阶段就按多 Rust 模块、多语言协议和 AI 协作场景规划，但第一阶段仍只聚焦于一次输入到结束状态的模型调用与工具调用闭环。

## 核心架构决策

Agent 执行采用三层抽象：

```text
Session
  └── Turn
        └── Step
```

- `Session`：长期会话边界，未来承载历史消息、恢复、分叉和持久化。
- `Turn`：一次用户输入触发的一次完整 Agent 执行，从输入开始，到最终回答、失败、取消或达到上限结束。
- `Step`：Turn 内的一次 LLM 调用和响应处理；模型产生的 Tool Call 由 Turn 调度，Tool Result 由 Turn 回填。

第一阶段不实现 Session 持久化；Session 作为架构边界保留。当前实现优先完成 `Turn + Step` 的内存闭环。

## 仓库分区

- `crates/`：Rust Kernel、Runtime 和适配器
- `apps/`：个人 Agent、CLI、TUI 等应用
- `protocols/`、`schemas/`：跨语言和跨进程契约
- `bindings/`：Python、TypeScript 等语言接入
- `examples/`、`evals/`：可运行示例和行为回归
- `docs/`：架构、ADR 和协作说明

详细模块职责见 [模块地图](docs/architecture/module-map.md)，多语言边界见 [多语言边界](docs/architecture/multi-language-boundary.md)。

## 设计文档

- [Agent 执行模型](docs/architecture/agent-execution-model.md)
- [Turn 边界控制与恢复收尾](docs/requirements/0016-turn-boundary-and-resume.md)
- [Runtime 执行边界](docs/requirements/0017-runtime-execution-boundary.md)
