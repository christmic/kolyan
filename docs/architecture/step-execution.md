# Step 执行设计

## 边界

Step 是一次模型调用，不是对话会话。它不拥有消息历史，也不决定下一次调用。

```text
StepRequest
    ↓
StepExecutor
    ↓ ModelProvider::stream
ModelEventStream
    ↓ aggregate_stream
StepResult / StepError
```

## 状态

概念状态为：

```text
Pending → Running → Completed
                    ↘ Failed
```

当前实现只在内存中执行，不对外暴露可变状态机；`StepExecutor::execute` 的成功返回代表 `Completed`，错误返回代表 `Failed`。未来需要取消、重试或持久化时，再把生命周期事件接入 Runtime、Trace 和 Storage。

## 职责

`kolyan-core::step` 负责：

- 接收 Step ID 和完整模型请求；
- 调用一次 Provider；
- 消费并聚合模型事件；
- 返回统一结果或错误。

明确不负责：

- 执行 Tool Call；
- 修改消息历史；
- 创建下一个 Step；
- Session、Turn、Ledger、权限和调度。
