# 0013 Ledger、Trajectory 与 Trace

## 术语

- Ledger：持久化权威事实，负责 cursor、顺序、幂等和恢复依据。
- Trajectory：一次 Agent/Turn 的语义执行路径，由 LedgerEvent 组合得到。
- Trace：实时观察和调试数据，可以丢失、压缩或采样。
- Checkpoint：从某个 Turn 状态恢复所需的数据快照。
- Projection：从 Ledger replay 计算出的查询视图，不是事实来源。

## 规则

1. Turn Core 只产生语义 TurnEvent，不依赖 Ledger 或 Trace 实现。
2. Runtime 负责把 TurnEvent 提交到 Ledger，并生成 Trajectory。
3. Ledger 事件必须有稳定 event_id、幂等 key 和单调 cursor。
4. ToolExecutionCompleted 才能证明工具副作用完成；Trace 不具备这个语义。
5. Provider token、stdout、debug 和 UI 增量只进入 Trace。
6. Checkpoint 与 Ledger 事实一起决定恢复位置，不能只依赖内存。
7. Projection 可以删除并从 Ledger 重建。

## 当前实现

V1 已提供内存 Ledger、TraceSink、Trajectory 和 TurnDriver，使用手工构造
provider/tool 输入的数据驱动测试验证事件顺序。文件/SQLite Ledger、durable
TurnDriver、工具执行账本和多审批事务将在后续阶段接入。
