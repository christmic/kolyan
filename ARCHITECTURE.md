# Kolyan Architecture

## 定位

Kolyan 是个人 Agent 应用与 Agent Runtime 的组合项目：Rust 负责稳定内核，多语言通过协议、CLI、网络接口或绑定接入。

## 分层

```text
Applications
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

第一阶段只实现 `Turn + Step` 的内存闭环，Session 只保留架构边界。

### Runtime

负责执行环境能力：取消、重试、限制、策略、轨迹、Session 和持久化。Runtime 不能把这些能力反向耦合进最小 Kernel。

### Adapters and Protocols

模型、工具和其他语言通过 trait、CLI、JSON/JSONL、HTTP 或绑定接入。Rust 内部实现不是跨语言契约，`protocols/` 和 `schemas/` 才是跨语言的稳定边界。

### Applications

个人 Agent 放在 `apps/kolyan-agent`，不污染通用 Runtime。CLI、TUI、服务端是不同的使用入口。

## 依赖规则

```text
kolyan-types → kolyan-core → kolyan-runtime → applications
                    ↑
       model / tools / policy / trace / storage
```

核心库不得依赖具体模型供应商、数据库、UI 或个人 Agent 配置。
