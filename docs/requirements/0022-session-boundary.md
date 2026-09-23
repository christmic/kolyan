# 0022 Minimal Session Boundary

Status: implementation in progress as the first Session persistence slice.

## Goal

提供跨独立 Turn 的长期会话边界，但不让 Session 进入 Turn/Step 的执行状态机。

```text
Session
  ├── ordered Turn records
  ├── conversation messages
  └── version
        ↓
Server / ExecutionService
        ↓
Turn → Step
```

## Responsibilities

- 持有稳定的 `session_id`；
- 持久化 Turn 的顺序、`turn_id`、`execution_id` 和终态；
- 持久化可供下一 Turn 构造 ModelRequest 的消息；
- 关闭并重开后恢复同一 Session；
- 通过版本号拒绝隐式覆盖旧快照；
- 为 Server 提供会话查询和追加边界。

## Non-responsibilities

- 不执行模型或工具；
- 不决定 Turn 内下一步；
- 不保存 Runtime 的临时控制对象；
- 不负责审批租约或 Worker 调度；
- 不把 Trace 当作会话事实。

## V0 data model

```rust
SessionRecord {
    session_id,
    version,
    turns: [SessionTurn],
    messages: [Message],
}
```

File persistence is an adapter only. The model is storage-neutral so SQLite or
another process boundary can be added later without changing Core.

## Verification

- 两个独立 Turn 按顺序写入同一 Session；
- 第一 Turn 的消息成为第二 Turn 的上下文；
- 关闭并重开 SessionStore 后，版本、Turn 索引和消息完全恢复；
- 重复 Turn 不静默覆盖；
- 测试使用真实文件快照，不需要 Provider API Key。
