# Requirement 0004：Turn 执行模型

## 目标

定义一次用户输入触发的完整 Agent 执行边界。Turn 负责驱动一个或多个 Step，根据 StepOutcome 决定结束、执行 Tool 或继续下一次 Step。

Turn 是内存中的短生命周期执行单元，不负责 Session 持久化、Ledger、长期 Memory、模型 Provider 适配或 UI 事件总线。

## 三层边界

```text
Session
  └── Turn
        ├── Context / Trajectory
        ├── Step 1
        ├── Tool Execution（可选）
        ├── Tool Result
        ├── Step 2
        └── Final Outcome
```

职责边界：

| 层 | 负责 | 不负责 |
|---|---|---|
| Step | 一次模型调用、事件流、结果分类、调用级取消/校验 | 工具执行、消息历史、重试策略 |
| Turn | 本轮消息轨迹、Step 循环、最大步数、工具结果追加、结束判断 | Session 持久化、长期预算、跨 Turn 恢复 |
| Session | 长期历史、恢复、分叉、持久化 | 单次模型调用和工具调度细节 |

## 核心抽象

### TurnRequest

```rust
pub struct TurnRequest {
    pub turn_id: String,
    pub model_request: ModelRequest,
    pub config: TurnConfig,
}
```

`model_request` 是本轮第一个 Step 的完整模型请求。后续 Step 使用 Turn 内部维护的消息轨迹重新生成请求；Step 不修改原始请求或历史。

### TurnConfig

```rust
pub struct TurnConfig {
    pub max_steps: usize,
    pub max_tool_calls: Option<usize>,
}
```

第一版只使用 `max_steps`。`max_tool_calls` 作为后续工具执行阶段的安全上限保留。

Turn 的 deadline、取消和 trace 元数据应通过 TurnControl/TurnExecutionOptions 统一向下传递给 Step，而不是复制到每个业务接口中。

### TurnState

```rust
pub enum TurnState {
    Pending,
    Running,
    WaitingTool,
    Completed,
    Failed,
    Cancelled,
    MaxSteps,
}
```

状态转换：

```text
Pending
  ↓
Running
  ├── Step → FinalAnswer ───────→ Completed
  ├── Step → Refused ───────────→ Completed
  ├── Step → Incomplete ────────→ Completed / MaxSteps
  ├── Step → ToolCalls ─────────→ WaitingTool
  │                                  ↓
  │                            Tool Results
  │                                  ↓
  └─────────────────────────────── Running

Running / WaitingTool → Cancelled
Running / WaitingTool → Failed
Running → MaxSteps
```

状态只表达 Turn 生命周期，不暴露为可任意修改的公共状态机。状态变化由 TurnExecutor 驱动。

### TurnResult

```rust
pub struct TurnResult {
    pub turn_id: String,
    pub outcome: TurnOutcome,
    pub steps: Vec<StepResult>,
}

pub enum TurnOutcome {
    FinalAnswer { response: ModelResponse },
    Refused { response: ModelResponse },
    Incomplete { response: ModelResponse },
    MaxSteps,
}
```

取消和失败通过 `TurnError` 返回，不伪装成成功结果。

```rust
pub enum TurnError {
    InvalidRequest,
    Step(StepError),
    Tool(ToolError),
    Cancelled,
    TimedOut,
    MaxSteps,
}
```

## Turn 事件流

Turn 不直接暴露 Provider 流，而是把 Step 事件包装到 Turn 语义中：

```rust
pub enum TurnEvent {
    Started { turn_id: String },
    StepStarted { turn_id: String, step_id: String },
    Step { turn_id: String, event: StepEvent },
    ToolCallRequested { turn_id: String, call: ToolCall },
    ToolResult { turn_id: String, result: ToolResult },
    Completed { turn_id: String, outcome: TurnOutcome },
    Failed { turn_id: String, error: TurnError },
    Cancelled { turn_id: String },
}
```

V1.1 已提供 `TurnEvent`、`TurnExecution` 和 `TurnEventStream`。当前事件覆盖
Turn/Step/Tool 的生命周期，以及最终完成结果；模型文本、思考和参数增量仍由
Step 的 `StepEventStream` 提供，避免在 Turn 层复制 Provider 细节。

事件流是观察面，不是 Turn 的状态存储。轨迹持久化由 `kolyan-trace` 或
`kolyan-storage` 后续订阅事件完成。V1.1 不引入重试、预算、并发工具或持久化恢复。

## Tool 抽象

Turn 只依赖工具执行接口，不依赖具体工具实现：

```rust
pub trait ToolExecutor {
    async fn execute(&self, call: ToolCall) -> Result<ToolResult, ToolError>;
}
```

Tool 的注册、权限、审批、沙箱、重试和并发策略不属于 Turn 核心。Turn 只负责：

1. 接收 Step 产生的 Tool Call；
2. 按配置交给 ToolExecutor；
3. 将 Tool Result 转成消息并追加到本轮轨迹；
4. 发起下一次 Step。

V0 使用 `kolyan-tools::RestrictedShellTool` 作为临时工具实现。它只接受结构化的 `shell.query` 调用，支持 `pwd`、`list`、`count_lines`、`count_entries`，不执行任意 Shell 字符串。未注册的工具名称返回明确错误。

## Context / Trajectory

Turn 内部维护本轮的消息轨迹：

```text
Initial messages
  → User message
  → Assistant response
  → Tool result
  → Assistant response
  → ...
```

每次 Step 前生成一个不可变的 Context Snapshot：

```rust
pub struct TurnContext {
    pub messages: Vec<Message>,
    pub step_index: usize,
}
```

Step 只读取快照。Turn 在 Step 完成后追加 Assistant 消息，在 Tool 完成后追加 Tool Result。不能由 Step 直接修改 Turn 消息列表。

## 主流程

```text
TurnRequest
    ↓
校验 turn_id / max_steps
    ↓
创建内存 Context
    ↓
检查取消和步数
    ↓
构造 StepRequest
    ↓
执行 Step
    ↓
追加 Assistant Response
    ↓
读取 StepOutcome
    ├── FinalAnswer → TurnCompleted
    ├── Refused → TurnCompleted
    ├── Incomplete → TurnCompleted 或 MaxSteps
    └── ToolCalls
          ↓
      校验/执行 Tool
          ↓
      追加 Tool Result
          ↓
      step_index += 1
          ↓
      回到检查取消和步数
```

## V0 实现范围

V0 先实现一个可验证的最小 Turn，不一次性接入完整 Tool Runtime：

- 一个 Turn 可以执行多个 Step；
- 使用 `RestrictedShellTool` 完成一次真实的 `ToolCall → ToolResult → 下一次 Step`；
- 将 Assistant 响应和 Tool Result 追加到内存消息轨迹；
- 使用 `max_steps` 防止工具循环；
- 复用现有 `StepExecutor`；
- `FinalAnswer`、`Refused`、`Incomplete` 可以结束 Turn；
- 未注册或不支持的 Tool Call 返回明确错误，不静默丢弃；
- 支持 Step 开始前的 Turn 取消检查；当前执行中的 Step 仍由 StepControl 独立控制；
- 不做重试、Session、Ledger、权限和持久化。

V1 再加入：

- `ToolExecutor`；
- Tool Call 校验和 Tool Result 追加；
- `WaitingTool` 状态；
- TurnEventStream；
- 工具调用上限和并发策略。

## 验收标准

- Turn 能从一个 `TurnRequest` 执行至少一个 Step；
- Step 的 `FinalAnswer` 能转换成 `TurnOutcome::FinalAnswer`；
- Step 的 `Refused`、`Incomplete` 不会被误判为普通成功回答；
- Step 的 Provider、Protocol、Validation、Cancelled、TimedOut 错误能向上转换；
- 达到 `max_steps` 时不会继续调用模型；
- V0 能完成至少一次 Tool Call、Tool Result 和第二次 Step；
- 不支持的 Tool Call 返回明确错误，不会静默丢弃；
- Turn 不修改 Session，不执行持久化，不实现具体工具；
- 所有 Turn 单测使用 Mock Step/Provider，不依赖 API Key。
