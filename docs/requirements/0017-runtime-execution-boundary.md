# 0017 Runtime Execution Boundary

Status: implemented as the Runtime v0 boundary. SQLite, cross-process leases and
full Core-driven durable Turn orchestration remain follow-up slices.

本文定义 Runtime 的最小持久执行边界。它不定义 Agent、Tool 或业务策略，
只负责一次 Execution 的身份、准入、外部副作用、收据、取消和恢复。

## 边界

```text
Session
  └── Runtime Execution
        ├── Turn Core
        ├── Admission
        ├── Ledger
        ├── Lease
        └── Effect Receipt
```

- Session 只提供长期上下文和 Ledger 命名空间；本需求不实现完整 Session。
- Turn Core 决定下一次模型调用或工具处理；不直接写 Runtime Ledger。
- Runtime 只理解通用 Effect，不理解 Agent 意图、具体工具名或产品策略。
- Policy、Tool 和 Executor 通过端口接入；Runtime 校验它们的身份、摘要和收据绑定。

## Execution 生命周期

```text
Created → Running → Suspended → Running → Completed
                    ├──────────→ Cancelled
                    └──────────→ Failed / Uncertain
```

每个外部 Effect 都遵循：

```text
Prepared → Authorized → Started → Completed / Failed / Uncertain
             └────────→ AwaitingDecision
```

Runtime 必须在 `Started` Ledger 事实提交后才调用外部 Executor；没有可信
`EffectReceipt` 的 Started Effect 不得自动重放，必须进入 Uncertain。

## 最小通用契约

- `ExecutionId`、`EffectId`、`AuthorizationId` 是 Runtime 身份，不由模型提供。
- `EffectRequest` 携带 operation kind、input digest、requirements 和 policy revision。
- `EffectGrant` 绑定 effect、input digest、constraints digest 和 authorization revision。
- `EffectReceipt` 绑定 effect、grant、executor revision 和 result digest。
- Admission 的实现可以是内存、文件或数据库；Core 不感知实现方式。
- 相同 idempotency key 的重试返回已记录事实，冲突输入 fail closed。

## 取消、审批与恢复

外部命令只接受 `execution_id` 或 `effect_id`，不依赖进程、线程或内存对象。
审批和取消必须写入 Ledger，并由恢复时的事实重放决定下一步。Runtime 负责
lease/fencing 的扩展位置；本最小版本先提供可持久 Ledger 和恢复检查，完整多进程
lease 在后续切片实现。

## 实现顺序

1. 通用 Effect、Grant、Receipt 和 Admission/Executor 端口；
2. 前置 Ledger 事实和 fail-closed 状态机；
3. 文件 Ledger 关闭后重开，恢复 Suspended/Completed/Uncertain；
4. 取消和重复提交测试；
5. 后续再加入 SQLite、lease、沙箱执行器和 Session context provider。

当前实现完成了 1–4：通用 Effect/Grant/Receipt、文件 Ledger 前置事实、
取消阻断、收据重放和关闭后重开恢复。文件 Ledger 是可替换的参考持久化适配器，
不宣称已经解决多进程并发 lease 或外部副作用的 exactly-once。

## 验收

- 单元测试覆盖状态迁移、digest/identity mismatch、拒绝、重复和 Uncertain；
- 集成测试使用真实文件 Ledger，关闭并重开 Runtime 后仍可恢复；
- 真实测试只使用 Runtime 的通用 Effect，不生成 Agent 专用实现；
- 不把 Trace 当作权威状态，不在 Runtime 内推断 Agent 业务结论。
