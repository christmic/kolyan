# Kolyan Architecture

## 定位

Kolyan 是个人 Agent 应用与 Agent Runtime 的组合项目：Rust 负责稳定内核，多语言通过协议、CLI、网络接口或绑定接入。

## 分层

```text
Applications
    ↓
Server / Session Coordination
    ↓
Runtime
    ↓
Agent Kernel
    ↓
Protocols / Adapters
```

### Agent Kernel

负责最小执行闭环：

```text
输入 → LLM → Tool Call（可选）→ Tool Result → LLM → 结束状态
```

核心抽象为：

```text
Session
  └── Turn
        └── Step
```

当前已实现 Step/Turn、Runtime 和最小持久化 Session 执行链。Session 是上层会话边界，不属于最小 Kernel；个人 Agent 产品入口仍未完成。

### Runtime

负责执行生命周期、取消接入、限制、执行事实与副作用收据，通过内核接口接入治理和观测。Runtime 不直接管理 SessionStore，也不让最小 Kernel 依赖数据库。

### Server and Session Coordination

Server 协调 Session 输入、执行身份、审批恢复与会话提交。执行所有权协调属于 Server 的协调器，不属于 Runtime/Turn。未来分布式租约也留在这个边界，不作为单进程内核的前提。

### Adapters and Protocols

模型、工具和其他语言通过 trait、CLI、JSON/JSONL、HTTP 或绑定接入。Rust 内部实现不是跨语言契约，`protocols/` 和 `schemas/` 才是跨语言的稳定边界。

### Applications

个人 Agent 和 TUI 放在 `apps/`，通过 Server 通信。一次性 CLI 位于 `crates/kolyan-cli`，本进程复用执行能力，无需启动服务。常驻进程装配位于 `services/kolyan-server`。

## 依赖规则

```text
clients -> Server services -> Runtime driver -> Core Turn/Step
                |                  |                  |
          SessionStore       Ledger / Trace     ModelProvider / ToolExecutor
                                                Policy / Boundary ports
```

核心库不得依赖具体模型供应商、数据库、UI 或个人 Agent 配置。

## Extension Status

现有模型、工具执行、边界控制、记录器和存储接口支持适配器替换；上下文压缩、Prepared Call、完整策略替换与副作用核验仍需补专用契约。Turn 当前绑定具体 PolicyEngine，不能仅因存在 PolicyResolver 就宣称策略完全可插拔。

评估、源码证据、理论依据和后续增强顺序统一维护在 [内核演进评估](docs/architecture/kernel-evolution.md)。模块职责以 [模块地图](docs/architecture/module-map.md) 为准，不重复维护能力清单。
