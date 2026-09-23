# 0019 SQLite Ledger and Execution Lease

Status: implemented as the Runtime v1 persistence slice.

## Goal

将 Runtime 的参考 FileLedger 升级为可查询、可事务提交的 SQLite Ledger，并
提供与 Ledger 独立的 Execution lease/fencing 边界。它仍然不理解 Agent 或
具体 Tool，只负责持久事实和执行所有权。

## SQLite Ledger

- `events` 以 cursor 单调排序，`event_id` 和 `idempotency_key` 有唯一约束；
- append 在一个事务中分配 cursor 和写入事件；
- claim 使用唯一约束原子占用；
- 关闭连接后重开可 replay 全部事实；
- 事件 payload 仍是版本化 JSON，未来可迁移为 blob 引用。

## Execution Lease

```text
acquire(execution_id, owner_id, now, ttl)
  → granted(revision) / fenced
renew(execution_id, owner_id, revision)
release(execution_id, owner_id, revision)
```

Lease revision 是 fencing token。旧 owner 即使仍在运行，也不能用旧 revision
更新或提交新的 Runtime 事实。过期 lease 允许新 owner 接管并递增 revision。

## 边界

- Ledger 是权威事实；Lease 是当前执行所有权，不替代 Ledger。
- Runtime Driver 后续通过 lease owner 包装 boundary admission；Core 不知道 lease。
- Lease 不保证外部副作用 exactly-once；EffectReceipt 和 Uncertain reconciliation
  仍是另一条边界。

## 验收

- SQLite Ledger 真实文件关闭重开后 cursor、事件和 claim 保持一致；
- 重复 append/claim 不产生第二份事实；
- 过期 lease 可被新 owner 接管；旧 owner 的 renew/release 被拒绝；
- 测试不依赖模型供应商或 Agent 实现。

实现结果：`SqliteLedger` 已提供事务 append、唯一 idempotency claim、事件 replay
和 execution lease/fencing；真实集成测试覆盖关闭重开、过期接管和旧 owner 拒绝。
Driver 尚未强制要求 lease，下一切片将把 lease revision 接入每次 boundary admission。
