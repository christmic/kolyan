# 0010 工具权限与动态决策

## 状态

已实现 v1 内核；批次级上下文和冲突规划进入 v1.1；审批持久化、签名 Manifest 留到后续版本。

## 背景

`ToolDefinition` 只服务于模型：它描述工具名称、用途和参数 Schema，不能作为安全授权。工具执行需要同时具备：工具能力上限、当前调用的实际资源声明、运行时策略和执行期强制校验。

## 需求

1. 工具必须有可信的 `ToolManifest`，未注册工具默认拒绝。
2. Manifest 使用能力集合、副作用集合和资源范围表达能力上限，不使用单一布尔权限。
3. 每次 `ToolCall` 必须解析为 `InvocationClaim`，至少包含能力、副作用和资源路径。
4. 动态决策必须取静态能力上限与运行时策略的交集，不能扩大 Manifest 权限。
5. 决策必须区分允许、带约束允许、需要审批和拒绝。
6. 只有允许结果能生成本次调用的 `ExecutionGrant`。
7. ToolExecutor 必须在实际执行边界接收策略结果；拒绝和需要审批都不能转化为成功工具结果。
8. 无法识别的工具、能力或资源必须 fail closed。
9. 决策应携带策略版本、原因和执行约束，便于后续轨迹与审计。

## v1 范围

- 已支持文件读、文件写、受限查询工具的 Manifest。
- 已支持文件路径前缀范围、运行时 workspace 范围和工具 deny-list。
- 已支持 `PolicyEnforcingTool`，可直接包裹现有 ToolExecutor。
- 已支持输出大小、超时约束的决策字段；实际预算执行由后续执行器版本接管。
- 已增加真实模型权限矩阵：模型实际生成 scoped allow/deny ToolCall，使用 OpenAI/Anthropic 双协议和配置模型矩阵验证副作用及 JSONL 轨迹契约。
- v1.1 正在实现 `PolicyContext`、批次资源冲突分析和 `BatchExecutionPlan`；策略只允许对授权调用生成执行阶段，不允许越权调用进入计划。

## 非目标

- v1 不实现人工审批 UI、审批持久化和租户策略文件。
- v1 不把外部 MCP 注解直接当成授权；外部注解需要经过信任层转换。
- v1 不在模型提示词中注入真实授权信息来替代执行期校验。

## 验收标准

- 未注册工具调用得到 Deny。
- 被运行时 deny-list 命中的调用得到 Deny。
- Manifest 路径和 workspace 路径任一不匹配时得到 Deny。
- Always approval 只得到 RequireApproval，不能生成 Grant。
- 通过 `PolicyEnforcingTool` 的拒绝调用返回 `ToolError::PolicyDenied`，不会到达底层工具。

## v1.1 批次治理验收标准

- 独立资源的允许调用进入同一个并行阶段。
- 同一资源的读写或写写调用被拆分到有序阶段。
- Tool-call 预算耗尽时，批次调用全部得到 Deny。
- 批次中被拒绝或需审批的调用不进入执行阶段。
- 真实模型矩阵验证允许副作用、拒绝副作用和完整 Turn 轨迹。
