# Requirement 0003：Step 流与执行控制

## 目标

在保持 Step 只代表一次模型调用的前提下，定义稳定的事件流、结果分类、取消和超时契约，使上层 Turn 能够可靠地消费、停止和判断一次 Step。

本需求不引入 Turn、Session、Tool 执行、权限审批、重试、轨迹持久化或账本。

## 核心边界

```text
StepExecution
  ├── StepEventStream  数据面：模型调用过程
  └── StepControl      控制面：取消和生命周期控制
```

Step 只调用一次 `ModelProvider`，并把 Provider 事件映射成 Provider-neutral 的 `StepEvent`。Tool Call 只作为模型产物返回，不在 Step 内执行。

## 流契约

流类型保持为：

```rust
pub type StepEventStream =
    Pin<Box<dyn Stream<Item = Result<StepEvent, StepError>> + Send>>;
```

事件约束：

1. 第一个事件是 `Started`。
2. `TextDelta`、`ReasoningDelta`、Tool Call 参数增量保持模型原始顺序和内容。
3. 一个 Tool Call 按 `Started → ArgumentsDelta* → Completed` 顺序出现。
4. 正常结束只能有一个终止事件：`Completed`、`Cancelled` 或 `TimedOut`。
5. 终止事件之后不能再产生事件，流随后结束。
6. Provider 错误、协议错误和验证错误通过 `Err(StepError)` 返回，并结束流。
7. 流结束前没有终止事件时，返回协议错误。
8. `StepEvent` 中的 `step_id` 在整个流中保持一致。

`Completed` 必须是最后一个业务事件。聚合器必须消费到流结束并验证该约束，不能在看到 `Completed` 后立即返回。

## 结果分类

Provider 的 `StopReason` 不能直接作为上层 Agent 的控制语义。Step 聚合出统一的 `StepOutcome`：

```rust
pub enum StepOutcome {
    FinalAnswer,
    ToolCalls,
    Refused,
    Incomplete,
}
```

映射规则：

```text
EndTurn         → FinalAnswer
ToolUse         → ToolCalls
Refusal         → Refused
MaxOutputTokens → Incomplete
Other           → Incomplete
```

## 执行控制

流本身只负责输出，取消通过独立的 `StepControl` 完成：

```rust
pub struct StepExecution {
    pub stream: StepEventStream,
    pub control: StepControl,
}
```

`StepControl::cancel()` 是协作式取消。Step 在产生下一个事件前检查取消状态；取消后输出 `Cancelled` 并结束流。丢弃执行句柄或流也必须释放底层 Provider 流，避免继续消耗网络和模型资源。

Step 级 timeout 由 `StepExecutionOptions::deadline` 表达。Provider 自己的 HTTP timeout 只限制网络请求，不等同于整个 Step 的 deadline。到达 deadline 后输出 `TimedOut` 并结束流。

第一版不要求 `kolyan-core` 绑定 Tokio。当前使用通用的 `AtomicWaker` 在取消时唤醒等待中的流；Provider 建连阶段仍由 Provider 自己的 HTTP timeout 负责，Runtime 后续可为建连阶段增加统一 deadline 包装。

## 错误语义

```rust
pub enum StepError {
    Provider(ProviderError),
    Protocol { message: String },
    InvalidRequest { message: String },
    Validation(StepValidationError),
    Cancelled,
    TimedOut,
}
```

正常流事件和执行失败分开表达：

```text
Completed / Cancelled / TimedOut → StepEvent
Provider / Protocol / Validation → StepError
```

`execute()` 消费流并将 `Cancelled`、`TimedOut` 转换成对应的 `StepError`；`execute_stream()` 保留生命周期事件，方便 UI、轨迹和测试观察过程。

## 执行选项

```rust
pub struct StepExecutionOptions {
    pub deadline: Option<Instant>,
    pub context_version: Option<String>,
    pub trace_id: Option<String>,
}
```

`context_version` 和 `trace_id` 只作为调用元数据，不改变模型请求内容。Session 的预算、Turn 的重试次数和权限决策不属于这些选项。

## 非目标

- Step 不执行 Tool Call；
- Step 不追加消息历史；
- Step 不创建下一个 Step；
- Step 不负责重试、压缩、权限、治理、Ledger 或持久化；
- Step 不把测试快照、临时文件或轨迹写入生产执行路径。

## 验收标准

- 流事件顺序和终止状态可被单元测试验证；
- `StepResult` 携带统一的 `StepOutcome`；
- 显式取消产生 `Cancelled`，超时产生 `TimedOut`；
- Provider 流错误、无终止事件、终止后事件都能被识别为错误；
- `execute` 与 `execute_stream` 仍然共享同一条 Provider 调用路径；
- 不增加 Tool 执行、Turn 循环或 Session 持久化职责。
