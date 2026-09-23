# 0024 Session Execution Integration

Status: implemented.

## Goal

把 Session 从独立的持久化边界接入 Server ExecutionService，使多个独立
Turn 真正共享同一份可恢复上下文。

```text
SessionExecutionService
  ├── load Session messages
  ├── prepend history to current Turn request
  ├── register Running / Suspended / Completed / Failed / Cancelled
  ├── append completed user + assistant messages
  └── delegate execution to ExecutionService
```

## Rules

- Session 只由 Server 层协调，Turn/Step 不直接访问 SessionStore；
- 当前 Turn 的输入消息在执行开始时读取，但只在完成后提交到 Session；
- 审批暂停保留 Turn 为 `Suspended`，不重复追加用户消息；
- 恢复继续使用 Ledger 中的 Approval checkpoint，不重新请求已完成的模型 Step；
- Completed、Refused、Incomplete 追加助手响应；Rejected、Expired、MaxSteps
  只更新 Turn 状态，不伪造助手消息；
- Runtime 错误更新 Session 为 Failed 后原样返回；
- Session 更新失败不能被静默吞掉；
- 同一 `turn_id` 或 `execution_id` 不能重复提交。

## Verification

- 两个独立 Turn 使用同一 Session，第二 Turn 请求包含第一 Turn 的消息；
- 第一 Turn 完成后关闭并重建 Server、Runtime 和 SessionStore；
- 第二 Turn 完成后 Session 的 Turn 顺序、版本和消息顺序正确；
- 审批暂停/恢复、取消和失败分别更新对应 Session 状态；
- 测试使用真实文件 SessionStore、Ledger 和临时轨迹，不需要模型 API Key。

## Implementation

- `SessionStore::begin_turn` 只登记 Running Turn，不提前写入用户消息；
- `SessionStore::update_turn` 原子更新状态并追加最终消息；
- `SessionExecutionService` 位于 Server 层，负责历史消息拼接和结果提交；
- Runtime 的 Approval checkpoint 仍由 Ledger 持有，Session 只记录 Suspended 状态，
  恢复时由 Server 根据 execution/approval id 重新装配 Runtime。
- `session_execution_live.rs` 对 21 个配置模型/协议组合执行两轮真实 Session Turn，
  并在第二轮前重建 Server、Runtime 和 SessionStore。
