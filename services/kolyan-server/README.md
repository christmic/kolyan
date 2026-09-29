# Kolyan Server 服务进程

本机 JSONL / JSON-RPC 执行服务，装配 Provider、受限文件工具、Policy、Runtime、
SQLite Ledger 和文件 SessionStore。模型请求运行在独立任务中，状态与取消请求
不等待模型结束。协议见 [Server v1](../../protocols/server-execution.md)。

```sh
cargo build -p kolyan-server-service
KOLYAN_SERVER_CONFIG=/absolute/path/server.json target/debug/kolyan-server
```

配置样例：[server.example.json](../../examples/server.example.json)。配置只保存凭据的
环境变量名，真实值由进程环境提供；不要把个人配置或密钥提交到仓库。
先创建工作区及允许访问的子目录；文件工具不会替用户任意创建目录。
常驻 Server 使用完整执行上下文投影，保留前一 Turn 的工具调用、结果及签名思考内容。

`execution.start` 返回本次执行完成或审批挂起的结果；可同时发送其他 RPC。
挂起检查点已持久化，可以关闭进程，重启后用原执行身份和 approval_id 确认。
`execution.events` 使用账本游标增量拉取；日志保留实际内容，不是只有事件名称。
文本、思考、工具参数和 usage 的中立模型增量以 `model_stream_event` 返回，与执行事实
共享 cursor，因此客户端重连后可续读。它们是观察数据，不参与恢复或重复执行判定；
Provider 原始 metadata 和鉴权信息不通过该接口暴露。
这不是 HTTP/WebSocket 服务，当前不提供网络认证或分布式协调。

正常 EOF 等待在途任务完成；强制退出后，已启动且无收据的工具副作用必须人工对账，
不能自动重跑。`session.reconcile` 只补 Session 提交，不执行模型和工具。
取消请求和已停止分开报告：`Cancelling` / `execution_stopped: false` 表示仍等待当前边界。

## 测试

子进程测试代码、场景和预期放在 `crates/kolyan-integration-tests/tests/`；测试 target
登记在本服务包，从而使用 Cargo 提供的本次构建二进制，而非可能过时的 target 文件。

```sh
cargo test -p kolyan-server-service --test server_process -- --nocapture
cargo test -p kolyan-server-service --test server_process server_process_live_matrix -- --ignored --nocapture
```

真实矩阵按配置模型 × 审批重启双 Turn / 无进展治理 / 审批取消列出每一行；
缺凭据和普通 Provider 错误均算失败，独立场景继续执行。配置、RPC 请求响应、完整
账本 JSONL、stderr 和报告仅写入测试临时目录。精确收据数量及禁止事件也参与比较。
