# Server 执行协议 v1（R4）

JSON-RPC 2.0 / JSONL；每个请求独立执行，等待模型不阻塞其他请求的状态与取消。
Server 从本地配置装配 Provider 和工具，客户端不传凭据或任意服务端文件路径。

- `server.info`：返回 `protocol_version: 1`。
- `session.create` / `session.load`：参数 `session_id`。
- `execution.start`：参数 `session_id`, `turn_id`, `execution_id`, `input`（文本）。
  使用服务端配置的模型、工具和执行预算，返回最终完成或审批挂起状态。
- `execution.approve`：相同执行身份及 `approval_id`；持久检查点恢复，不重跑已完成 Step。
- `execution.cancel`：相同执行身份；持久取消请求，后续执行边界拒绝准入。
- `execution.status`：相同执行身份；返回账本状态。
- `execution.events`：相同执行身份及 `after_cursor`；返回后续事件和 `next_cursor`。
  客户端可轮询续读，断线不删除事件；这是拉取订阅，不宣称 WebSocket 推送已实现。
- `session.reconcile`：`session_id`, `execution_id`；补齐中断的 Session 提交，不调用模型。

响应只出现在 stdout，诊断使用 stderr。错误保留 JSON-RPC `id`；非法消息 -32600，
未知方法 -32601，非法参数 -32602，执行/存储错误 -32000。无凭据时启动失败，不能退化为模拟执行。

持久状态由 Ledger / Session 持有；进程内 Coordinator 只持有正在运行的任务。
进程关闭或崩溃后释放内存所有权；未落收据的副作用禁止自动重复执行。
本版本不提供网络认证；stdio 仅用于本机受信任的客户端。

### 状态、幂等与事件边界

请求字段见 [请求 schema](../schemas/server-rpc.schema.json)。响应为
`{jsonrpc:"2.0", id, result}` 或 `{jsonrpc:"2.0", id, error:{code,message}}`。
同一 Session 同时只接受一个活动 Turn；版本冲突或重复 start 显式拒绝，
不把相同 JSON-RPC id 当作自动重放授权。审批 attempt 只消费一次，重复提交
不会重复执行工具，也不能将原活动 Turn 改为失败。

`execution.status` 区分 cancellation_requested 与 execution_stopped；请求取消
不等于当前模型调用已经中断。挂起状态可以立即落停止事实，运行中在执行边界停止。
对账只接受明确终态事实，不把取消意图当作已停止。

每条事件带全局递增 cursor、execution_id、turn_id、kind 和实际 payload。
每页最多 1000 条；下一页使用 next_cursor，重复查询同一 cursor 不产生执行副作用。
当前订阅同时包含耐久 Turn/Step/Tool 事实、实际 ModelRequest，以及 Provider 中立化后的
模型增量。模型增量 kind 为 `model_stream_event`，payload.type 为 `text_delta`、
`reasoning_delta`、`tool_call_started`、`tool_call_arguments_delta`、
`tool_call_completed` 或 `usage`；均包含 step_id，工具事件还包含 call id。
`StepCompleted` 仍是唯一 Step 终态；Provider 原始 metadata 不通过该接口公开。
增量与其他事实共享全局 cursor，断线后用 after_cursor 续读，不维护第二套易失流状态。
模型流只用于观察和客户端呈现，不作为恢复或效果重放输入。
签名思考等上下文按模型返回保留，不生成或补造模型未提供的内容。

EOF 等待在途任务结束；强制终止后不会自动重跑已 claimed 的 attempt。
已挂起审批可用持久检查点恢复；Started 无工具收据必须走显式效果对账。
本版本不承诺任意指令位置自动恢复，也不宣称外部副作用 exactly-once。
