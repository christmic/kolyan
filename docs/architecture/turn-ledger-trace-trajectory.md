# Turn Ledger、Trajectory 与 Trace

## 边界

Turn Core 产生 TurnEvent；Runtime / TurnDriver 负责编排、提交 Ledger 和生成
Trajectory；Ledger 保存权威事实；Trace 保存实时观察和调试细节。

Trajectory 是一次 Agent 执行的语义路径，由已提交的 TurnEvent 组合而成，
不是另一套存储真相。Trace 可以丢失或压缩，Ledger 不能静默丢失。

## 持久化对象

- LedgerEvent：Turn/Step/Tool/Approval 的语义事实。
- Ledger cursor：单调递增的提交位置。
- idempotency key：避免同一事实或工具副作用重复提交。
- Continuation：审批或崩溃恢复所需的下一步上下文。
- Projection：从 Ledger replay 得到的 Session/Turn 查询视图。

Provider token delta、stdout、debug 信息只进入 Trace；工具是否完成必须由
Ledger 的 ToolExecutionCompleted 事实确认。
