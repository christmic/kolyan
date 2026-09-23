# 0014 Turn 终态事件与实时事件流

## 目标

补齐内存版 Turn 的最后一组运行时契约：无论 Turn 以正常结果、最大步数、失败、取消还是超时结束，观察者都能看到完整的生命周期；调用方可以在 Turn 执行期间消费事件，而不是等待执行结束后再读取事件列表。

本需求只属于 Turn Core，不引入 Session、持久化 Ledger、Provider 适配或 UI 事件总线。

## 终态契约

Turn 的终态分为两类：

1. 业务终态：`FinalAnswer`、`Refused`、`Incomplete`、`MaxSteps`，通过 `TurnExecution` 返回，并产生 `Completed` 事件。
2. 执行终态：`Failed`、`Cancelled`、`TimedOut`，通过 `TurnError` 返回，并在错误返回前产生对应的终态事件。

`MaxSteps` 不再作为异常返回。它是一个可观察、可判断的正常 Turn 结果，且不能继续调用模型。

## 事件契约

事件顺序必须满足：

```text
Started
  → StepStarted
  → StepCompleted
  → ToolCallRequested / ToolExecutionStarted / ToolResult
  → ...
  → Completed | Failed | Cancelled | TimedOut
```

错误终态事件是最后一个事件。事件流可以在终态事件之后关闭；事件流本身不替代 `TurnResult` 或 `TurnError`。

## 实时流

`execute_event_stream` 必须在事件产生后立即向调用方发送事件。它不能先等待整个 Turn 完成再把内存数组转换成流。

流消费方提前关闭流时，Turn 执行任务应被取消；Turn 本身不负责把取消强制传播给已经运行的外部工具，但不得继续无界运行。

## 审批边界

普通内存审批和可恢复审批都必须暴露 `ApprovalRequested`。批准、拒绝、过期属于 Turn 的结果语义：批准后继续执行；拒绝和过期产生终态结果，不伪装成普通最终回答。

## 非目标

- 不实现跨进程恢复或持久化事件账本；
- 不实现自动重试、长期预算或 Session；
- 不把 Provider token delta 复制到 Turn 事件，细粒度模型流仍由 Step 提供。

## 验收标准

- `max_steps` 返回 `TurnOutcome::MaxSteps` 和 `Completed` 事件；
- 模型、工具、取消、超时错误均产生对应终态事件；
- 实时事件流能在模型执行尚未结束时收到 `Started`/`StepStarted`；
- 既有单步、多步、批次、审批和轨迹测试保持通过。
