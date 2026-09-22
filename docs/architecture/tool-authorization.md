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

其中 `ToolManifest` 不是当前授权，而是 capability ceiling；`ExecutionGrant` 才是当前调用可以携带到执行边界的授权。

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

决策结果：

- `Allow`：可以生成 `ExecutionGrant`。
- `AllowWithConstraints`：可以生成 Grant，但必须携带约束。
- `RequireApproval`：暂停在执行边界之前，等待外部批准。
- `Deny`：生成 `ToolError::PolicyDenied`，不得调用底层工具。

## 4. 当前实现位置

- `kolyan-policy`：Manifest、Claim、PolicyEngine、Decision 和 Grant。
- `kolyan-tools`：内置工具的 Manifest，以及 `PolicyEnforcingTool` 执行边界。
- `kolyan-core`：新增 `ToolError::PolicyDenied`，Turn 将其作为工具失败处理。
- `kolyan-trace`：未来记录 Decision、Grant 和 AuditEvent；当前不改变已有 Turn 轨迹格式。

## 5. 调用流程

```text
模型产生 ToolCall
       ↓
InvocationClaim::from_call
       ↓
PolicyEngine::decide
       ├─ Deny ───────────────→ ToolError::PolicyDenied
       ├─ RequireApproval ────→ 等待审批（v2）
       └─ Allow/Constrained
                    ↓
             ExecutionGrant
                    ↓
              ToolExecutor
```

## 6. 后续扩展顺序

1. 将 `PolicyInput` 增加 agent、user、task、turn、预算和环境属性。
2. 增加审批记录与单调用/本 Turn/持久会话三种审批范围。
3. 在 ToolCallBatch 上做资源冲突图，决定并行、串行、拆批或拒绝。
4. 将约束真正接入超时、输出大小、网络和沙箱执行器。
5. 为 Manifest 增加来源、信任级别和签名校验。
6. 将 Decision、Grant、Approval 和实际结果写入统一审计事件。
