# Kolyan Server 服务进程

本机 JSONL / JSON-RPC 执行服务，装配 Provider、默认 macOS 隔离四工具、Policy、Runtime、
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

## 默认隔离装配

生产环境只装配 `IsolatedToolSet` 的 `file.read`、`file.write`、`file.edit`、
`shell`，不提供 RestrictedFileTool、旧 shell.query 或无沙箱 fallback。
基础 inventory 保持四工具；Service 的 template.tools 只在 allow_shell=true
且物理 scope 等于整个 workspace 时广告 Shell，否则只广告三个文件工具。
广告过滤不替代实际执行 deny；未来 Agent 更细 permissions 也须同时过滤广告
并在执行时检查独立宿主权限，不能以模型未见 schema 当作安全边界。
不支持 macOS Seatbelt、缺失 backend/worker 或不安全布局时启动失败。
`cargo build -p kolyan-server-service` 同时构建 `kolyan-server` 和
`kolyan-server-tool-worker`；后者直接引用真实 tool-worker 生产入口，不复制实现。

必填宿主配置如下（路径均由可信宿主选择，不由模型提供）：

```json
{
  "workspace": "/host/agent/workspace",
  "tool_scope": "safe",
  "ledger_path": "/host/agent/state/ledger.sqlite",
  "session_root": "/host/agent/state/sessions",
  "worker_path": "/host/bin/kolyan-server-tool-worker",
  "staging_root": "/host/agent/staging",
  "allow_shell": false
}
```

这是配置片段；Provider、请求、预算与 progress 字段仍须提供。
工作区、scope 子目录、state 目录及 staging parent 必须预先存在。
worker_path 必填，没有 PATH、当前可执行文件旁目录或环境变量发现 fallback。
worker 必须在 workspace、staging 和持久 state 之外。
staging 不存在时以 0700 创建；已存在时必须是 0700 目录，与 workspace
同设备。state 从 ledger_path 的物理 parent 推导，Session 必须位于其中。
workspace、state、staging 两两不可包含。保护整棵 state，因此新生成的
SQLite WAL/SHM/journal 和 Session 文件也受保护，不依赖 sidecar 预先存在。
宿主将实际加载的配置路径传入 `App::new(config, config_path)`，只保护该
配置文件自身，不以其 parent 扩大 deny。配置文件必须存在，不能位于 staging。

tool_scope 必须是非空 workspace-relative 现存目录；绝对路径、父路径遍历、
控制路径以及解析到 workspace 外的 symlink 均拒绝。它转换为物理绝对路径，
与工具准备出的绝对资源使用同一 policy namespace；模型文件路径仍相对 workspace。
Read 声明 Read；Write 同时声明 Create/Update；Edit 声明 Read/Update 及读写
capabilities。Read 无需审批，Write/Edit 始终需审批。目录外资源不会因审批而放行。

allow_shell 可省略，默认 false；增加四工具库存不会扩张旧 safe 配置的执行权限。
开启后 Shell 仍需审批，并保守声明整个 workspace 的读/创建/更新/删除/执行能力。
因此仅授权 safe 子目录时，Shell 即便 cwd=safe 也拒绝；只有显式授权整个 workspace
（tool_scope="."）才可能执行。不能通过解析 command 或缩小 cwd 假称 Shell 只影响子目录。
所有工具实际执行均受沙箱网络、控制路径、输入/输出上限和取消清理约束；
路径 pinning、原子 staging 和 sandbox 是不同防线，不宣称 profile 单独解决 TOCTOU。

生产装配的固定宿主限额：Read/Write 内容各 1,048,576 bytes，完整 ToolResult
envelope 至多 1,048,576 bytes（不只是 pipe 内容），Shell command 至多
65,536 bytes；每次工具执行 timeout 为 30 秒。Shell stdin ceiling 为 0，
只接受空输入。文件 worker 协议输入有独立 67,108,864-byte 宿主 ceiling，
它不提升或消费 output grant。上述值是宿主代码选择的上限，不是模型可提升的参数；
实际执行还取 policy/grant 与 adapter requirements 的更窄约束。Provider 的
timeout_secs 是另一条模型请求 timeout，不放宽工具 timeout。

新增装配校验仅执行准备与 policy 判断，不以伪 worker 响应冒充真实 effects。
实际 worker/HTTP/stdio effects 继续由进程 fixture 验证。本装配不等于 AgentRunner 完成。

```sh
cargo test -p kolyan-server-service --bin kolyan-server assembly::tests
```

`execution.start` 返回本次执行完成或审批挂起的结果；可同时发送其他 RPC。
挂起检查点已持久化，可以关闭进程，重启后用原执行身份和 approval_id 确认。
`execution.events` 使用账本游标增量拉取；日志保留实际内容，不是只有事件名称。
文本、思考、工具参数和 usage 的中立模型增量以 `model_stream_event` 返回，与执行事实
共享 cursor，因此客户端重连后可续读。它们是观察数据，不参与恢复或重复执行判定；
Provider 原始 metadata 和鉴权信息不通过该接口暴露。
上述为 stdio 模式；不配置 HTTP 时保持原行为。不提供分布式协调。

## 最小 HTTP 模式

在同一份项目本地配置中增加以下字段，即可切换到 HTTP（不同时监听 stdio）：

```json
"http": {
  "listen": "127.0.0.1:3000",
  "api_token_env": "KOLYAN_HTTP_API_TOKEN",
  "max_active_turns": 4
}
```

API token 的真实值由环境提供，不能放进配置；它与模型 API Key 分离。
客户端携带 `Authorization: Bearer <token>`，并使用监听地址作为 Host。
只接受 loopback，拒绝 Origin，不启用 CORS。启动方式仍是上面的服务命令。

契约见 [HTTP v1](../../protocols/server-http.md) 和
[OpenAPI](../../schemas/server-http.openapi.json)。本期六个接口返回完整结果，
不提供事件流。提交输入和批准审批会等到执行完成或再次挂起；
挂起不占用连接等待用户。拒绝与取消是不同终态。
客户端指定 Session/Turn ID，断线后查询，不盲目重发；重复提交返回冲突。

HTTP 测试独立于已有 stdio 测试，普通运行不含网络真实矩阵：

```sh
cargo test -p kolyan-server-service --test server_http_process -- --nocapture
cargo test -p kolyan-server-service --test server_http_process http_process_live_matrix -- --ignored --nocapture
```

案例数据位于 `tests/fixtures/server_http*.json`，生产代码不写测试轨迹。
验收状态与未完成的测试项以 [需求 0028](../../docs/requirements/0028-http-server-boundary.md)
为准，接口存在不等于所有验收场景已通过。

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
