# Requirement 0002：最小 Step 执行

## 目标

在不引入 Turn、Session、Tool 执行、重试或持久化的前提下，完成一次模型调用的最小内存闭环。

## 范围

Step 接收一份完整的 `ModelRequest`，调用一次 `ModelProvider`，消费模型事件直到 `Completed`，最后返回 `ModelResponse` 或统一的 `StepError`。

支持：

- Step 标识；
- `Pending → Running → Completed / Failed` 生命周期；
- OpenAI / Anthropic Provider；
- 文本、Reasoning、Tool Call、Structured Output、Usage 和 Stop Reason；
- Provider 错误和协议错误向上转发；
- 内存执行，不保存历史。

不支持：

- Turn 和 Session；
- Tool 注册、审批和执行；
- 自动重试、超时、取消和并发调度；
- Ledger、Trace、Memory 和持久化；
- 模型路由和能力协商。

## API

```rust
pub struct StepRequest {
    pub step_id: String,
    pub model_request: ModelRequest,
    pub options: StepExecutionOptions,
}

pub struct StepResult {
    pub step_id: String,
    pub response: ModelResponse,
    pub outcome: StepOutcome,
}

pub struct StepExecutor<P> {
    provider: P,
}

impl<P: ModelProvider> StepExecutor<P> {
    pub async fn start(
        &self,
        request: StepRequest,
    ) -> Result<StepExecution, StepError>;

    pub async fn execute_stream(
        &self,
        request: StepRequest,
    ) -> Result<StepEventStream, StepError>;

    pub async fn execute(&self, request: StepRequest) -> Result<StepResult, StepError>;
}
```

`execute_stream` 返回 Step 层事件，而不是直接暴露 Provider 的
`ModelEventStream`。`execute` 内部消费同一条 Step 流并聚合
`Completed(StepResult)`，因此结果接口和流式接口不会产生两套模型调用逻辑。
需要显式取消时使用 `start` 返回的 `StepControl`；`execute_stream` 适合不需要外部控制的兼容场景。

## 验收标准

- Provider 成功返回 `Completed` 时，Step 返回完整 `StepResult`；
- Step 流包含 `Started`、文本/推理增量、Tool Call、Usage 和 `Completed` 等事件；
- `execute` 与 `execute_stream` 使用相同的 Provider 调用和事件映射路径；
- Provider 打开或消费失败时，Step 返回 `StepError`；
- 没有 `Completed` 的流返回协议错误；
- Step 结果包含统一的 `StepOutcome`；
- 取消和超时通过流事件暴露，并由 `execute` 转换为 `StepError`；
- 终止事件之后继续产生事件时返回协议错误；
- Tool Call 只作为 `ModelResponse` 返回，不在 Step 内执行；
- Structured Output、Usage 和 Stop Reason 原样保留；
- 测试不需要 API Key，可以使用内存 Mock Provider。
