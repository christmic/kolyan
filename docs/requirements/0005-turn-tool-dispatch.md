# Requirement 0005：Turn V1.2 工具调用策略

## 目标

定义 Turn 收到一个或多个 \`ToolCall\` 后的调度、执行、失败和结果回填语义。

V1.2 只解决“工具如何被调度”，不解决工具本身的权限、审批、沙箱、长期账本或成本预算。

## 当前 V0 的边界

V0 已支持：

- 从 Step 响应中提取 \`ToolCall\`；
- 调用 \`ToolExecutor\`；
- 将 \`ToolResult\` 追加回 Turn 消息；
- 进入下一次 Step。

V0 尚未明确：

- 一个 Step 返回多个 ToolCall 时的调度模式；
- 工具调用的串行/并行语义；
- 单个工具失败是否影响同批次其他工具；
- 工具级取消和超时；
- 工具结果与调用的稳定关联；
- 调度过程中的事件和错误分类。

## 职责边界

\`\`\`text
Step
  └── ToolCall[]
        ↓
Turn Tool Dispatch
  ├── 校验调用
  ├── 选择调度模式
  ├── 执行工具
  ├── 汇总结果
  └── 回填 Turn Context
        ↓
下一次 Step
\`\`\`

| 层 | 负责 | 不负责 |
|---|---|---|
| Step | 产生 ToolCall，报告 ToolUse | 执行工具和决定并发 |
| Turn Dispatch | 调度、关联、汇总、错误传播 | 工具权限和具体实现 |
| ToolExecutor | 执行单个 ToolCall | 批量调度和 Turn 状态 |
| Policy/Governance | 审批、权限、资源限制 | 具体模型消息拼接 |

## 核心抽象

### 调度模式

V1.2 定义两种模式：

\`\`\`rust
pub enum ToolDispatchMode {
    Serial,
    Parallel,
}
\`\`\`

- \`Serial\`：按照模型返回顺序逐个执行。默认模式，结果顺序稳定，适合有副作用或存在依赖的工具。
- \`Parallel\`：同一个 Step 返回的独立 ToolCall 并行执行；结果仍按原始 ToolCall 顺序回填，避免模型上下文出现非确定顺序。

V1.2 默认使用 \`Serial\`；调用方显式选择 \`Parallel\` 时启用并行执行，不通过隐式行为改变默认语义。

### 工具调用策略

\`\`\`rust
pub struct ToolDispatchPolicy {
    pub mode: ToolDispatchMode,
    pub on_error: ToolErrorPolicy,
}

pub enum ToolErrorPolicy {
    FailTurn,
    ContinueBatch,
}
\`\`\`

- \`FailTurn\`：任意工具失败，当前 Turn 立即失败。
- \`ContinueBatch\`：当前批次继续执行其他 ToolCall，并为失败调用生成 \`ToolResult { is_error: true }\`，然后由模型决定下一步。

V1.2 默认使用 \`Serial + FailTurn\`，保持 V0 的安全行为；\`Parallel\` 和 \`ContinueBatch\` 都必须通过显式策略启用。

### 执行结果

\`\`\`rust
pub struct ToolDispatchResult {
    pub call_id: String,
    pub result: Result<ToolResult, ToolError>,
}
\`\`\`

\`call_id\` 是唯一关联键。工具名不能替代 \`call_id\`，因为同一个 Step 可能多次调用同名工具。

## 调度规则

1. 按 Step 响应中 \`ToolCall\` 的出现顺序建立调用批次。
2. 每个 ToolCall 只能执行一次。
3. 未注册工具在调度阶段立即产生 \`ToolError::Unavailable\`。
4. \`Serial\` 模式按原始顺序执行。
5. \`Parallel\` 模式允许并行开始，但结果必须按原始顺序回填。
6. 每个结果必须带回原始 \`call_id\`。
7. 工具结果回填后，Turn 才能发起下一次 Step。
8. 没有成功完成整个批次前，不能伪造下一轮模型上下文。

## 失败语义

\`\`\`text
ToolCall
  ↓
ToolExecutor
  ├── success → ToolResult → append context → next Step
  └── error
        ├── FailTurn → TurnError::Tool
        └── ContinueBatch → error ToolResult → next Step
\`\`\`

V1.2 不做自动重试。重试属于后续错误恢复能力，避免把“失败后如何处理”和“失败后是否重试”混在同一个策略中。

工具错误必须保留：

- \`call_id\`
- 工具名称
- 错误类别
- 是否已经开始执行
- 是否产生了可回填结果

## 取消和超时

- Turn 取消时，不再启动新的 ToolCall。
- 串行模式下，取消发生在当前工具执行期间，由 ToolExecutor 决定是否能中断底层操作。
- 并行模式下，取消需要向尚未完成的调用广播取消信号。
- V1.2 只定义传播边界，不实现全局时间预算；全局预算属于 V1.3。
- 工具级超时由 \`ToolExecutor\` 或其包装器实现，Turn 负责将超时归类为 ToolError。

## Turn 事件

V1.2 在已有 \`TurnEvent\` 基础上补充调度语义：

\`\`\`rust
ToolCallRequested { turn_id, call }
ToolExecutionStarted { turn_id, call_id, name }
ToolResult { turn_id, result }
ToolExecutionFailed { turn_id, call_id, error }
\`\`\`

事件要求：

- \`ToolExecutionStarted\` 不早于 \`ToolCallRequested\`；
- 成功执行必须有一个 \`ToolResult\`；
- 失败执行必须有一个 \`ToolExecutionFailed\`，或按策略转换为错误 \`ToolResult\`；
- 同一个 \`call_id\` 不能产生两个终态结果；
- 并行模式下事件顺序不代表完成顺序，结果回填顺序必须单独保证。

## 与 V1.3 的关系

\`\`\`text
V1.2 Tool Dispatch
  提供调用数量、开始/完成、成功/失败事件
              ↓
V1.3 Budget
  统计 ToolCall 数、执行时长、Token 和成本
\`\`\`

V1.2 不添加 \`max_tool_calls\`、总耗时、Token 或成本限制；这些限制应建立在稳定的调度事件之上。

## 测试要求

### 单元测试

- 单个 ToolCall 串行执行；
- 多个 ToolCall 按顺序执行；
- 同名工具通过不同 \`call_id\` 正确关联；
- 未注册工具；
- \`FailTurn\` 失败传播；
- \`ContinueBatch\` 失败结果回填；
- 取消时不启动后续 ToolCall；
- 并行结果按原始调用顺序回填；
- 重复终态和缺失结果被拒绝。

### 集成测试

- 使用真实模型产生多个 ToolCall；
- OpenAI/Anthropic 两种协议分别覆盖；
- 真实工具结果按 \`call_id\` 回填；
- Turn JSONL 记录调度事件；
- 至少验证串行策略；并行策略单独使用明确 fixture。

## 验收标准

1. 默认串行行为与 V0 兼容。
2. 多 ToolCall 不丢失、不覆盖、不乱序。
3. 工具失败行为由策略决定，而不是由实现细节决定。
4. 每个 ToolCall 都能通过 \`call_id\` 找到唯一终态。
5. 取消、失败和正常完成在事件流中可区分。
6. V1.3 可以只订阅调度事件完成预算统计。

## 不属于本需求

- 工具注册中心；
- 权限与审批；
- 沙箱实现；
- 自动重试；
- 全局预算；
- Session 持久化；
- Ledger 和长期审计存储。
