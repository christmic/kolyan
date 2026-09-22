# Requirement 0007：Turn 终止语义与状态机

## 目标

让 Turn 的成功和失败都能被明确分类，并用有限状态迁移约束执行顺序，避免把“模型完成”“工具失败”“取消”和“达到上限”混成同一种结果。

## 状态机

```text
Pending
  ↓
Running
  ↓
WaitingModel
  ├── Completed
  ├── Failed
  ├── Cancelled
  ├── TimedOut
  └── WaitingTool → ExecutingTools → Running
```

约束：

- 模型请求必须发生在 `WaitingModel`；
- 工具批次必须经过 `WaitingTool` 和 `ExecutingTools`；
- 工具执行完成后回到 `Running`，再进入下一次模型请求；
- 终态不能继续迁移；
- 不允许跳过模型等待或工具执行阶段。

## 结束原因

成功的 `TurnResult` 必须携带 `TurnEndReason`：

- `FinalAnswer`
- `Refused`
- `Incomplete`

错误通过 `TurnError::end_reason()` 映射为：

- `Failed`
- `Cancelled`
- `TimedOut`
- `MaxSteps`

错误仍然通过 `Result` 返回，不伪装成成功结果；结束原因只是提供稳定的分类接口。

## 当前实现范围

本阶段实现：

- 增加 Turn 状态迁移校验；
- 在 Turn 执行循环中经过模型等待、工具等待和工具执行状态；
- `TurnResult` 增加成功结束原因；
- `TurnError` 增加统一结束原因映射；
- 新增状态迁移和结束原因单元测试；
- 新增全模型、双协议真实最终结果验证。

暂不实现：

- Turn 状态持久化；
- 取消信号向底层工具的强制中断；
- 自动重试和检查点恢复；
- 跨 Turn 的长期状态管理。
