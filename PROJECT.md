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

- `Session`：长期会话边界，承载历史消息、恢复、分叉和持久化；由 Server
  在 Turn 执行前后协调，Runtime/Turn 不直接依赖 SessionStore。
- `Turn`：一次用户输入触发的一次完整 Agent 执行，从输入开始，到最终回答、失败、取消或达到上限结束。
- `Step`：Turn 内的一次 LLM 调用和响应处理；模型产生的 Tool Call 由 Turn 调度，Tool Result 由 Turn 回填。

当前已完成 Session 的最小持久化执行闭环；后续再扩展分叉、并发策略和更丰富的会话治理。

## 仓库分区

- `crates/`：Rust Kernel、Runtime 和适配器
- `apps/`：通过 Server 通信的个人 Agent、TUI 和未来桌面客户端
- `services/`：常驻 Server 进程；一次性 CLI 本进程执行，无需启动服务
- `protocols/`、`schemas/`：跨语言和跨进程契约
- `bindings/`：Python、TypeScript 等语言接入
- `examples/`、`evals/`：可运行示例和行为回归
- `docs/`：架构、ADR 和协作说明

详细模块职责见 [模块地图](docs/architecture/module-map.md)，多语言边界见 [多语言边界](docs/architecture/multi-language-boundary.md)。

## 设计文档

- [架构整改需求与分阶段验证记录（实施中）](docs/requirements/0025-architecture-hardening.md)
- [Provider 官方 SDK 对照整改与真实证据](docs/requirements/0026-provider-sdk-conformance.md)
- [多厂商模型参数表与调用前请求规划](docs/requirements/0027-model-request-planning.md)
- [源码组织、测试分离与注释规范](docs/architecture/code-conventions.md)
- [Agent 执行模型](docs/architecture/agent-execution-model.md)
- [Turn 边界控制与恢复收尾](docs/requirements/0016-turn-boundary-and-resume.md)
- [Runtime 执行边界](docs/requirements/0017-runtime-execution-boundary.md)
- [Durable Turn Driver](docs/requirements/0018-durable-turn-driver.md)
- [SQLite Ledger 与 Execution Lease](docs/requirements/0019-sqlite-ledger-and-execution-lease.md)
- [Server 与 Execution Coordinator 边界](docs/requirements/0020-server-coordinator-boundary.md)
- [Server Execution Service](docs/requirements/0021-server-execution-service.md)
- [Minimal Session Boundary](docs/requirements/0022-session-boundary.md)
- [Server JSON-RPC Entrypoint](docs/requirements/0023-server-rpc-entrypoint.md)
- [Session Execution Integration](docs/requirements/0024-session-execution-integration.md)
