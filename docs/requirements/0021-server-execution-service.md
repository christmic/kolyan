# 0021 Server Execution Service

Status: implementation in progress as the first Server orchestration slice.

## Goal

让 Server 成为 Runtime 的唯一编排入口。调用方不再手动组合
`ExecutionCoordinator`、`DurableTurnDriver` 和 release 收尾逻辑。

```text
Server::ExecutionService
  ├── Coordinator admission
  ├── Runtime start / resume
  ├── Suspended release
  ├── Completed / Failed release
  └── Cancel / State facade
          ↓
      DurableTurnDriver
          ↓
      Turn → Step → Model / Tool
```

## Responsibilities

- 根据 `session_id + turn_id + execution_id` 创建一次 Execution 引用；
- 调用 Coordinator 执行 start/resume admission；
- 创建 Runtime Driver 并调用 start/resume；
- 无论 Runtime 返回 Completed、AwaitingApproval 还是失败，都释放本地执行槽；
- 将 cancel 和 state 查询暴露给 Server transport；
- 保持 Ledger 为生命周期事实源；
- 不实现 HTTP/RPC，不解释 Agent 策略，不管理租约。

## API

```rust
ExecutionService::start(executor, request, session_id, execution_id)
ExecutionService::resume(executor, session_id, execution_id, approval_id)
ExecutionService::cancel(execution_ref)
ExecutionService::state(execution_id)
```

`start` 和 `resume` 返回 Runtime 的 `DurableTurnResult`。审批暂停会返回
checkpoint，但 Server 会释放本地执行槽；用户确认后由 Server 再次调用
`resume`。Runtime 的错误不会被吞掉，Server 负责释放后原样返回。

## Failure and cancellation

- Coordinator admission 失败时，不创建 Runtime；
- Runtime 启动/恢复失败时，Server 释放执行槽并返回错误；
- cancel 先写入 Ledger，再由 Runtime 在下一个 boundary 感知；
- 已完成、已取消或失败的 Execution 不允许重新 start；
- Server 不保证正在进行的外部副作用可以被强制终止。

## Verification

- 单元测试验证服务的 admission、release 和错误传播；
- 跨模块测试验证 Server→Runtime 的多 Step、审批暂停/恢复和完成；
- 真实 Provider 矩阵验证相同链路在 OpenAI-compatible 与 Anthropic-compatible
  协议下成立；
- 测试轨迹由测试代码输出临时 JSONL 并与契约文件比较。

## Non-goals

- 本需求不实现 Session；
- 本需求不实现 HTTP/RPC transport；
- 本需求不引入 Lease/Worker assignment；这些属于未来 Server Coordinator
  的部署扩展。
