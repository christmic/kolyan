# ADR 0003：协议 SDK 与 Provider Adapter 分离

## 状态

Accepted

## 决策

OpenAI 和 Anthropic 分别使用独立的 protocol crate 和 provider adapter crate。`kolyan-model` 只保留供应商无关的模型调用抽象。

## 原因

- 官方 SDK 的协议类型变化频繁，需要与 Agent 内核隔离；
- OpenAI Responses 和 Anthropic Messages 的消息、Tool 和流事件模型不同；
- 未来可以增加其他 Provider，而不修改核心类型语义；
- 低层协议测试可以独立于 Agent Loop 运行。

## 影响

第一阶段 crate 数量会增加，但每层拥有清晰职责。Provider-specific 能力不强行压缩成最低公分母，而通过显式扩展保留。
