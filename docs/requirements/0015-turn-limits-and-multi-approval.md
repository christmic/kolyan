# 0015 Turn 多审批、运行限制与取消传播

## 目标

补齐 Turn 内核在真实编排中的三个控制能力：同一工具批次的多审批、Turn 总体运行限制、以及取消对当前 Step/Provider 的传播。

本需求仍只属于内存 Turn，不引入 Session、持久化账本、Provider 协议逻辑或工具实现。

## 多审批批次

一个 Step 返回的 ToolCallBatch 可能同时包含多个需要审批的调用。Turn 必须：

1. 为每个需要审批的调用生成独立 approval id；
2. 任意审批未完成前不执行该批次的副作用，也不重复调用模型；
3. 每次恢复只确认一个 approval，并返回下一个待审批边界；
4. 全部审批完成后，按原批次策略执行所有调用，再进入下一次 Step；
5. 任一审批拒绝或过期，整个批次终止，未批准调用不得执行。

`TurnContinuation` 保存批次全部调用、已批准调用和原始模型上下文。恢复不依赖原进程内存。

## Turn 运行限制

`TurnConfig` 增加：

- `max_tool_calls`：本 Turn 可执行的工具调用总数上限；超过后以明确的 `ToolBudgetExceeded` 失败终态结束；
- `deadline`：本 Turn 的总运行时长上限，覆盖等待模型、等待审批和工具执行。

限制由 Turn 统一计算和检查。Step/Tool 只接收剩余 deadline，不自行维护 Turn 预算。

## 取消传播

TurnControl 取消后必须：

- 在下一次模型调用前停止 Turn；
- 在模型 Step 执行期间取消 StepControl，使 Provider 流收到取消并结束；
- 在工具执行期间中断 Turn 等待；
- 产生 `Cancelled` 终态事件并返回 `TurnError::Cancelled`。

底层 Provider/Tool 是否能中断其外部副作用不由 Turn 保证，但 Turn 不得继续等待或发起下一步。

## 验收标准

- 两个审批调用需要两个 approval checkpoint，只有第二个批准后才执行批次；
- 工具预算在第二个调用前 fail-closed，模型不再被调用；
- 慢 Provider 在 TurnControl 取消后收到 StepControl 取消；
- Turn deadline 能终止慢模型、审批等待和慢工具；
- 单测、确定性集成测试和已有真实模型矩阵保持通过。
