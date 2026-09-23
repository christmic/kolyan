# Turn V1.4 持久化状态机

本文描述持久 Runtime 的目标边界。当前 Core 已统一普通/恢复循环，详见
[0016](../requirements/0016-turn-boundary-and-resume.md)；下述 execution ledger
提交、结果收据和崩溃恢复尚未实现，不能把文件审批快照视为完整持久执行。

StepResult
  → ToolCallBatch + PolicyPlan
  → ApprovalStore
  → approve / reject / expire / claim
  → execution ledger
  → next Turn Step

TurnContinuation 是数据，不是执行器。它不包含 provider client、Future、
mutex 或 task。ApprovalRequest 只描述当前需要外部决定的调用；批准后
重新计算 PolicyPlan，并将批准的调用加入已批准集合。

批次恢复采用“两阶段”：

1. 所有审批边界完成前，只更新 checkpoint，不执行批次副作用。
2. 批次可执行后生成 grant，按资源冲突 plan stage 执行。未来 Runtime 在此
   接入每个 call 的 execution ledger；当前 Core 只提供执行边界和观察事件。

Ledger 解决恢复幂等：同一 turn_id + step_id + call_id + args_fingerprint
只能有一个执行记录。已完成记录直接复用 ToolResult，不能再次调用底层工具。

文件 Store 只作为 V1.4 参考实现；生产环境可替换为数据库或事务型 Store，
但必须保持 claim/resolve 的原子语义。
