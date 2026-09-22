# Step 执行设计

## 边界

Step 是一次模型调用，不是对话会话。它不拥有消息历史，也不决定下一次调用。

```text
StepRequest
    ↓
StepExecutor::start
    ↓ ModelProvider::stream
StepExecution
    ├── StepControl
    └── ModelEventStream
    ↓ map to StepEvent
StepEventStream
    ├── execute_stream → StepEventStream
    └── execute → aggregate_step_stream → StepResult / StepError
```

## 状态

概念状态为：

```text
Pending → Running → Completed
                    ↘ Failed
```

当前实现只在内存中执行，不对外暴露可变状态机。`StepExecutor::start` 返回数据面 `StepEventStream` 和控制面 `StepControl`；`execute_stream` 是只返回流的兼容便捷接口；`execute` 消费同一条流并聚合为 `StepResult`，不维护第二套调用路径。成功返回代表 `Completed`，主动停止和超时分别代表 `Cancelled`、`TimedOut`，Provider/协议问题返回 `StepError`。

## 职责

`kolyan-core::step` 负责：

- 接收 Step ID 和完整模型请求；
- 调用一次 Provider；
- 将模型事件映射为带 `step_id` 的 Step 事件；
- 按需消费并聚合 Step 事件；
- 将 Provider 的结束原因映射为统一的 `StepOutcome`；
- 在事件边界检查取消和 deadline；
- 校验事件顺序和唯一终止状态；
- 返回统一事件流、结果或错误。

明确不负责：

- 执行 Tool Call；
- 修改消息历史；
- 创建下一个 Step；
- Session、Turn、Ledger、权限和调度。
- Tool 执行、重试、消息历史和轨迹持久化。

更完整的流、控制和终止状态契约见 [Step 流与执行控制](../requirements/0003-step-stream-control.md)。
