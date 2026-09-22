# Requirement 0006：Turn 核心语义闭合

## 目标

补齐 Turn 作为短生命周期执行单元的核心语义，先明确同一次模型响应产生的工具调用批次，再逐步完善终止、上下文一致性和取消语义。

本需求不引入 Session 持久化、长期预算、审批或跨 Turn 恢复。

## ToolCallBatch

一个 Step 返回的全部 ToolCall 构成一个批次。批次是 Turn 的一次调度边界，即使只有一个 ToolCall，也必须视为一个单元素批次。

```text
Step response
    ↓
ToolCallBatch
    ├── validate call ids
    ├── choose Serial / Parallel
    ├── execute each call
    ├── collect results
    └── append results together
        ↓
next Step
```

批次必须满足：

1. 不能为空；
2. 每个 ToolCall 的 `call_id` 在批次内唯一；
3. 每个调用最多执行一次；
4. 结果必须与原始调用关联；
5. 批次完成前不能发起下一次模型请求；
6. 结果回填顺序与原始调用顺序一致。

`ToolCallBatch` 只表达同一轮调度边界，不决定串行或并行。调度模式仍由 `ToolDispatchPolicy` 决定。

## 当前实现范围

V1.3 第一阶段实现：

- 增加 `ToolCallBatch` 公共抽象；
- 在 Turn 中从 Step 响应构造并校验批次；
- 保留现有 Serial / Parallel 行为；
- 增加空批次和重复 `call_id` 单元测试；
- 保持既有工具和真实测试逻辑不变。

后续阶段再实现：

- ToolCall 依赖关系和混合调度；
- Turn 级取消、超时和预算；
- 检查点恢复。

`TurnEndReason` 和状态迁移校验已在 V1.3 第二阶段完成，详见
`0007-turn-terminal-semantics.md`。

批次结果校验和上下文一致性已在 V1.3 第三阶段完成，详见
`0008-turn-context-consistency.md`。
