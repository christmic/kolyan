# Requirement 0009：Turn 正常与异常测试矩阵

## 目标

完整验证 Turn 的正常闭环、工具批次语义、上下文回填和异常终止，不依赖模型随机制造错误。

## 正常场景

真实模型矩阵必须覆盖：

1. 单步最终回答；
2. 多轮、每轮一个 ToolCall；
3. 单轮多个 ToolCall；
4. 多轮、每轮多个 ToolCall；
5. 串行批次；
6. 并行批次；
7. 工具结果完整回填到下一次模型请求；
8. OpenAI-compatible 和 Anthropic-compatible 两种协议；
9. 所有配置模型。

真实轨迹使用 fixture + JSONL 契约，不在测试代码中写死模型调用顺序。

## 异常场景

使用确定性的测试 Provider 和 ToolExecutor，覆盖：

- 空 ToolCall 批次；
- 重复 ToolCall ID；
- 未知 ToolResult ID；
- ToolResult 内部 ID 不匹配；
- FailTurn；
- ContinueBatch；
- ContinueBatch 的部分成功、部分失败；
- FailTurn 不发起下一次模型请求；
- 最大 Step 限制；
- Provider/Step 错误；
- 取消发生在模型请求前。
- 工具执行中的取消；
- 单工具超时。

异常测试必须验证：

- 错误类型；
- 结束原因；
- 已发起的模型请求次数；
- 是否产生半截上下文；
- 事件流是否出现错误终态。

## 已实现的工具异常控制

TurnExecutor 通过 TurnControl 和可选的工具超时配置控制单个工具：

- TurnControl 取消会中断正在等待的工具 Future；
- with_tool_timeout 会将超时转换为 ToolError::TimedOut；
- 串行和并行调度都使用相同的取消/超时边界；
- 取消和超时不会伪装成成功 ToolResult。

工具级重试、恢复和跨 Turn 检查点仍属于后续能力。
