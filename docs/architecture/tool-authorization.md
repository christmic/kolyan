# 工具权限技术规范

## 1. 分层

```text
ToolDefinition       模型可见描述
ToolManifest         工具可信的能力上限
InvocationClaim      当前参数解析出的资源与副作用
PolicyDecision       当前上下文的动态判断
ExecutionGrant       本次调用的短期授权
ToolExecutor         执行期强制检查与副作用边界
AuditEvent           实际发生的事实
```

其中 `ToolManifest` 不是当前授权，而是 capability ceiling；`ExecutionGrant` 才是当前调用可以携带到执行边界的授权。Grant 由 `TurnExecutor` 传给 `ToolExecutor::execute_with_grant`，不能只停留在策略结果中。

## 2. 权限计算

```text
effective =
  manifest ceiling
  ∩ runtime deny/allow policy
  ∩ workspace scope
  ∩ invocation resource claim
```

任何动态策略只能缩小静态上限，不能把只读工具升级为写入工具。没有 Manifest 的工具默认拒绝。

## 3. 决策接口

```rust
trait PolicyResolver {
    fn decide(&self, call: &ToolCall) -> PolicyDecision;
}
```

批次决策使用同一个 `PolicyEngine`，但额外接收 `PolicyContext`：

```rust
let plan = engine.resolve_batch(&context, &tool_calls);
```

`PolicyContext` 目前包含 Agent/User/Task/Turn 标识、workspace、剩余 ToolCall 预算和此前副作用；其中预算已参与 v1.1 的 fail-closed 判断。

决策结果：

- `Allow`：可以生成 `ExecutionGrant`。
- `AllowWithConstraints`：可以生成 Grant，但必须携带约束。
- `RequireApproval`：暂停在执行边界之前，等待外部批准。
- `Deny`：生成 `ToolError::PolicyDenied`，不得调用底层工具。

## 4. 当前实现位置

- `kolyan-policy`：Manifest、Claim、PolicyEngine、Decision 和 Grant。
- `kolyan-tools`：内置工具的 Manifest，以及 `PolicyEnforcingTool` 执行边界。
- `kolyan-core`：新增 `ToolError::PolicyDenied`；`TurnExecutor::with_policy_engine` 按 `BatchExecutionPlan` 执行批次阶段。
- `kolyan-trace`：未来记录 Decision、Grant 和 AuditEvent；当前不改变已有 Turn 轨迹格式。

## 5. 调用流程

```text
模型产生 ToolCall
       ↓
InvocationClaim::from_call
       ↓
PolicyEngine::decide
       ├─ Deny ───────────────→ ToolError::PolicyDenied
       ├─ RequireApproval ────→ TurnControl 等待批准
       └─ Allow/Constrained
                    ↓
             ExecutionGrant
                    ↓
              ToolExecutor
```

批次流程：

```text
ToolCallBatch
      ↓
PolicyContext + 逐调用 PolicyDecision
      ↓
提取 ResourceClaim / Effect
      ↓
资源冲突图
      ├─ 无冲突 → 同一 Parallel Stage
      ├─ 读写冲突 → 有序 Serial Stages
      └─ Deny/Approval → 不进入执行 Stage
```

## 6. 后续扩展顺序

1. 将审批记录持久化，支持单调用/本 Turn/持久会话三种审批范围。
2. 将约束真正接入超时、输出大小、网络和沙箱执行器。
3. 为 Manifest 增加来源、信任级别和签名校验。
4. 将 Decision、Grant、Approval 和实际结果写入统一审计事件。
