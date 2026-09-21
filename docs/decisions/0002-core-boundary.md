# ADR 0002：Turn 和 Step 构成第一阶段内核

## 状态

Accepted

## 决策

第一阶段实现 `Turn + Step` 的内存闭环。`Session`、Ledger、Memory、事件总线和多 Agent 编排暂不进入最小内核。

## 原因

最小 Agent 的核心问题是：固定输入后，如何调用模型、执行工具、追加结果并到达结束状态。先稳定这个闭环，后续能力通过 Runtime 和 Storage 扩展。

## 影响

核心 API 保持小而明确；未来实现 Session 或 Ledger 时，应通过接口和事件接入，而不是重写 Agent Loop。
