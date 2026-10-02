# 可治理 Hooks：受控执行与真实消费者

## 状态与范围

状态：Main 已审查 A/B 冻结交付并核对全部 21 项摘要；生产基础及新增测试
按小批次集成。私有门禁不是主干或真实 Agent 消费者的验收。
C/D 的 Runtime、Server、Runner 共享接线待 Main 集成，当前未授权修改。
基线：`6db275c8fe90ede0fca2953a5b309aafea9f47ea`。
工作树：`/tmp/kolyan-evolution-worktrees.3FHbOB/governed-hooks`，
分支 `codex/governed-hooks`。上述路径记录隔离交付来源；主干集成由 Main
所有，不修改其他 worker 文件。

落实 0031 E3 的 concrete hooks，而不是注册空 trait 后宣布完成。
复用 Agent → Server → Runtime → Turn → Step，不另造模型或工具 loop。
自我迭代是实验反馈，不是此能力的前置条件；旧实验保持冻结。
首版只支持宿主精确绑定的同步 native script：BeforeModel、BeforeTool、AfterTool。
不支持模型注册 hook、HTTP/MCP hook、后台 hook、参数改写、提示注入、
自动审批、命令字符串展开或 hook 再触发 hook。扩展必须新版本契约。

## 已核实的执行边界

下列路径相对上述基线；实现与拟新增接口严格分开。

| 现有实现 | 真实作用与限制 |
| --- | --- |
| `crates/kolyan-agent/src/runner/preparation.rs` | Root 输入组装与来源发布，不是每 Step 的模型开启 hook |
| `crates/kolyan-agent/src/runner/routing/advertisement.rs:22` | 每次 stream 验证广告与 Skills 当前绑定，再发送原请求 |
| `crates/kolyan-agent/src/provider.rs:83` | ContextPreparingProvider 每次开启验证并记录；拒绝改写请求，不替代 Provider retry |
| `crates/kolyan-agent/src/runner/routing.rs:41` | 真实 prepare 路由环境工具、Skills、agent.invoke |
| `crates/kolyan-runtime/src/driver/tools.rs:52` | 校验独立 scope/grant，读取已有 receipt/wait，拒绝不确定效果重放 |
| `crates/kolyan-runtime/src/driver/tools.rs:204` | 授权事实后记录 EffectStarted，再调用 inner executor；适配器内阻断已经太迟 |
| `crates/kolyan-runtime/src/driver.rs:158`、`driver/resume.rs:362` | start/resume 均装配 DurableTools，两个消费者必须同样接线 |
| `crates/kolyan-server/src/lib.rs:564` | TurnPreparationHook 仅新 Turn 的历史选择，不能拿来任意插入 system 或权限 |
| `crates/kolyan-sandbox/src/lib.rs:113` | SandboxExecutor 有界 stdin/output、cancel/drop；MacOsSandbox fail closed，不签发授权 |
| `crates/kolyan-sandbox/src/exact_file.rs:14` | 精确 read_files/write_files，workspace 只是 cwd/目录元数据锚点 |

禁止将 BeforeTool 包装在 SnapshotTools.execute_invocation 内宣称“效果前阻断”。
禁止 Provider wrapper 偷改 ModelRequest，造成 Core 请求轨迹与出站内容不一致。
模型边界使用真实 execution 与本次 opening 序号，不从文本推测 step_id；
第一版 Provider port 没有 Step identity，因此 step_id 必须明确 null。
工具边界使用 Core 提供的完整 ToolExecutionScope，不能由 grant 反推 scope。

## 设计原则与参照证据

参考仓库只读，未更新；公开文档查阅日期 2026-10-03，可能晚于本地 HEAD。

| 仓库与 HEAD | 真实消费者/机制 | 本需求采用与不采用 |
| --- | --- | --- |
| Codex `5c5308fc9a9ee789049d646ef11e5400384b9c6f` | `codex-rs/core/src/tools/registry.rs:602,718` 消费 pre/post；`core/src/hook_runtime.rs:186,289` 组装输入并处理结果；`hooks/src/engine/command_runner.rs:41` 有界运行管理 | 采用执行路径接入和有界观察；不复制 input rewrite、上下文插入或所有事件名 |
| Grok `4247f661689354b831191f11eeeac8424993fe3d` | `crates/codegen/xai-grok-shell/src/session/acp_session_impl/tool_calls.rs:1072,1286` 消费 post/pre；`hook_dispatch.rs:300` 实际结果投递；`xai-grok-sandbox/src/hook_write_deny.rs:78` 身份捕获与重验 | 采用注册内容保护、真实结果和身份重验；不照搬完整 shell/ACP 生态 |
| Garive `3d67ac6ad6cf990f7e1a9d463978a6bfb6433bfa` | `runtime/replica/src/execution_work_binding.rs:35` 从固定事实前缀重建并验证绑定 | 本次 `runtime/replica/src` 检索无通用 script hook 消费者；只借鉴 durable scope/来源验证，不声称已有 hooks |
| Claude Code `7974a70773fa229e4cc65aa1b356cc21f5c216c4` | `examples/hooks/bash_command_validator_example.py` 读取 stdin JSON，Bash 匹配，exit 2 阻断 | 这是官方可执行示例，不是私有核心源码；采用明确输入/结果，Kolyan 不把 exit code 或输出文字当 grant |

官方 [Codex 插件契约](https://developers.openai.com/plugins/build/plugins)
明确安装不等于信任 hooks；[Codex hooks 文档](https://learn.chatgpt.com/docs/hooks)
说明部分工具路径不经过 hooks，因此 hooks 不是完整权限隔离边界。
[Claude hooks 文档](https://code.claude.com/docs/en/hooks) 区分阻断、普通权限处理与
输入改写；Kolyan 首版只采用收紧，不引入修改输入的权限风险。
[NIST SP 800-207](https://doi.org/10.6028/NIST.SP.800-207) 的决策/执行分离
支持把静态声明、当前授权、真实资源执行分开。这里是设计依据，不是安全认证。

## 静态定义、版本、绑定与动态授权

新增 Agent hooks 独立模块，不把 hook 加入四工具或 Agent delegation ceiling。
Hook 是宿主能力，不因模型拥有 shell 而自动启用，也不因模型没有 shell 而
禁止已授权的宿主 hook。Agent tool grant 与 hook 自身执行授权分别验证。

`HookKey { id, revision }` 精确版本，不支持 latest。`HookManifest` 包含：
事件集合、匹配的精确 tool names（只适用于工具事件）、脚本 ArtifactRef/摘要、
固定 interpreter 路径/身份、静态能力与资源声明、输入字段投影、时间/字节上限。
首版静态能力限定 ProcessExecute + FilesystemRead，效果 Execute + Read，
不声明 idempotent；拒绝写、网络、凭据、delegate、未知能力以及不受限目录。
不会直接利用 ToolManifest 空 path_scopes 的宽泛语义作为 hook ACL。

script 从真实 ArtifactStore 取出，安装于 workspace 外私有 readonly 文件；
精确文件 identity/摘要固定，拒绝 symlink/hardlink 重定向。
同 key 同 manifest/body 幂等，异内容冲突；新内容必须新 revision。
解释器固定 `/bin/sh` 的宿主可信身份，argv 为固定脚本路径，不接受 `-c`、
模型参数拼接、变量替换、PATH 搜索或登录环境。系统 loader 基础读取沿用现有
Sandbox，不能声称它只能读 script 一份文件。

`HookAccessPolicy` 是宿主配置，默认 deny，按 AgentKey、logical Session、
Task/invocation 与事件授权；子 Agent 不隐式继承父 hook。
`HookScope` 绑定这些身份、private Session、execution、snapshot digest；
工具事件另外含原始 scope/prepared digest，模型事件含原请求摘要/opening 序号。
模型 request 的 system/text/tool arguments 不能选择 manifest、policy 或 scope。

动态 `HookAuthorizer::authorize(binding, event, current_revision)` 返回 opaque
`HookAdmission` 或明确拒绝；检查当次 ACL、撤销、静态 ceiling、脚本身份与限额。
它不是可 Deserialize 的 bearer grant。普通脚本返回 continue 不授予任何能力。
首版需人工批准 hook 安装时由宿主预先完成，事件期间不另开审批 loop。

## 拟新增 port/API（均不是已实现接口）

Agent 模块拟公开 `HookCatalog::{register,revoke,bind,restore_binding}`，
复用 FactJournal 与 Required ArtifactStore，事实 namespace 与 Skills 分开。
binding 采用私有字段 opaque `VerifiedHookBinding`，不得只信输入 DTO。
拟新增 `AgentRunner::with_hooks(Arc<HookRuntime>)`，未配置保持现有默认行为。

`HookRuntime::dispatch(binding, event, control, remaining)` 返回 `HookDispatchResult`；
内部完成动态授权、持久化 intent、调用一次 SandboxExecutor、解析并记录完整结果。
`HookEventV1` 是严格 tagged DTO：BeforeModel、BeforeTool、AfterTool；
含版本、真实身份与摘要，以及 manifest 明确允许的有界输入投影。
未知/重复字段、超界或序列化失败必须拒绝，不截断为另一份判定输入。
模型全文/工具结果可能含敏感信息：默认只传来源摘要与工具结构；正文必须
显式 ACL 授权且属于 manifest 声明投影，不传 credentials/全局环境/任意 transcript。

Agent 新 `HookPreparingProvider<P>` 实现 ModelProvider，严格原样 delegate：
广告/Skills/Context 验证成功 → BeforeModel → acknowledgement → inner.stream。
只在 Provider opening 外侧运行一次；SDK 内部 HTTP retry 不再次触发 hook。
返回原 stream；不修改 delta、模型响应、重试机制、请求 DTO 或 timeout。

Runtime 拟新增中立 `EffectHookPort`，只负责现有工具效果边界：
`before_effect(&ToolInvocation) -> Future<Result<(), ToolError>>`；
`after_receipt(&ToolInvocation, receipt: CommittedEffectReceipt) -> Future<Result<(), HookObservationError>>`。
`CommittedEffectReceipt` 必须来自 Runtime 已验证/提交的事件引用、cursor、payload；
不能由 Agent 伪造工具结果替代。Runtime 不依赖 Agent hook DTO/注册实现。
`DurableTurnDriver::with_effect_hooks(...)` 将 port 装到 start 与 resume 的 DurableTools；
Agent bridge 将精确 invocation 转为 HookEvent，声明跨层接线由 Main 所有。
已核实 `crates/kolyan-server/src/lib.rs:255` 的私有 driver() 每次构造 Runtime，
因此 Main 必须新增 `SessionExecutionService::with_effect_hooks(...)` 及 driver()
透传，不能只在 Agent 内配置就声称 Runtime 消费者已接入。宿主将同一 HookRuntime
绑定给 Runner 和 SessionExecutionService 的 bridge；bridge 按 invocation 精确
ownership 查已验证 binding，未知 binding 拒绝，不能拿全局最近一次 Runner 状态。
预算上下文由 Runtime 传入 port 的执行窗口，不能由 ToolInvocation 猜 deadline；
具体签名应同时含 `HookExecutionWindow { remaining, control }`，上述调用列出业务载荷。

BeforeTool 调用位置：PreparedEvidence 已验证且没有 saved receipt/wait/Started，
**在写 EffectAuthorized/EffectStarted 之前**。然后再次检查 control、当前授权与
版本，才进入现有 append_unless_cancelled 与原 inner executor。
hook通过与真正 effect entry 不是原子事务；不声明撤销可回收已经开始的操作。
policy deny 的调用不运行 hook；审批 pending 时不执行；批准恢复后、实际 entry 前
才运行。因此不把 hook 凭据写进模型批准对象，也不重发目标工具。

AfterTool 只针对已提交 Completed receipt（包括已记录工具错误结果），在原结果
回到 Core 之前有界同步观察。AwaitingExternal 不是 Completed，不伪造 receipt。
receipt replay 不重新执行 script；历史 AfterTool intent/result 由事实重建。
AfterTool 失败保存独立诊断，不改成功/失败 ToolResult，不删除 receipt、不重复工具、
不冒充 rollback。证据存储失败使当前宿主执行显式失败且保留原 receipt；恢复须
走现有 receipt reconciliation，不因 host failure 授权重复实际效果。

## 输出协议、失败、取消和重建

stdout 必须恰好一份 UTF-8 JSON：`{schema_version:1,decision:"continue"|"deny",reason:string}`。
BeforeModel/BeforeTool 可 deny；AfterTool 只允许 continue，deny 是 observer 协议错误。
reason 有界且仅诊断，不注入后续模型/system；不支持 allow/grant/updated_input。
非零 exit、空 stdout、重复/未知字段、额外 prose、坏 UTF-8、输出溢出均显式失败。
拒绝与执行失败分开记录；BeforeTool deny → PolicyDenied，故障 → InvalidBatch
（宿主失败，不能被 ContinueBatch 当成模型参数错误反复尝试）；BeforeModel 失败
→ Provider InvalidRequest/Validate，不进入 HTTP retry。

首版每事件最多 8 hooks，按精确 key 排序串行；集合失败关闭，不 run-until-allow。
每脚本 timeout 配置正数且不超过 5s，整个 dispatch 不超过 10s，并取当前执行
remaining 的更小值；输入至多 256 KiB、stdout+stderr 合计至多 64 KiB。
这些是新 hook ceiling，不扩大现有 30s 工具或模型 timeout。
无 remaining 或无 Tokio timer/runtime 等执行前提必须明确报错，不 fallback。
AfterTool 预算耗尽不清除既有 receipt，明确记未完成观察，保持效果事实。

TurnControl.cancelled 桥接 SandboxCancellation；drop 通知已有独立 reaper。
cancel/timeout 返回必须区分 admission 未开始与 spawned 后清理；drop 不承诺
调用方已等待回收，不伪造 terminated/reaped。跨进程测试读取真实 observer。
进程组 cleanup、reaped、双 captureComplete 的事实与 output 独立保留；
掉队列/断开不能成为权威 proof，明确记录损失且不能宣布清理已证实。

注册、撤销、绑定、dispatch intent、完成/失败使用 schema1 critical facts；
真实 stdout/stderr/event/input 投影另存 Required Artifact，记录完整摘要和引用。
hook run identity 使用 execution+event+opening 序号或 step-scoped effect identity，
不能只用模型 call_id（不同 Steps 可重用），绑定策略 revision 和 manifest digest。
完成事实必须由已验证 intent 因果关联，缺引用/未知 schema/外来 scope 拒绝。
恢复重新校验 binding、当前 ACL、撤销、安装文件身份，不扫描目录重新选择。
Before gate 的旧 continue 不是新生命周期授权；明确新 run，仍受原 deadline。
未完成 Started 一律 Interrupted，不自动重跑 script，也不凭内存说已经成功。
AfterTool 无完成证明不得补写虚构完成，保留 observation incomplete；工具 receipt
不受影响。Task关闭沿用现有宿主终态链，不把 hook失败换成 TaskCompleted。

## 分批拟写集与所有权

每批最多 15 文件或 400 net lines；以下是将来审查通过后的目录计划，不是本轮写集。
实现若超预算再拆，不能为了表格数量把多份测试塞进生产源码。

| 批次 | 计划写集 | 验收/所有者 |
| --- | --- | --- |
| A | Agent 新 `hooks.rs`、`hooks/{definition,catalog,binding}.rs` 与对应独立 tests/json；`lib.rs` 声明 | durable 注册/撤销/重建，独立 owner；不能宣布完整能力 |
| B | Agent 新 `hooks/{runtime,native,protocol}.rs` 与独立 tests/json；integration 新 `tests/agent/hooks/native.rs`、fixture/expected/script、Cargo test登记 | 同批必须实际 MacOsSandbox script，不以 mock结案 |
| C | Runtime 新 `effect_hooks.rs` 与独立测试；`lib.rs`、`driver.rs`、`driver/resume.rs`、`driver/tools.rs`；Server `lib.rs` 服务 port/driver 透传；Agent 新 `runner/hooks.rs`、`hooks/provider.rs` | Main 所有共享边界；保持默认 disabled/旧断言 |
| D | Main `runner.rs` 及 root/resume/delegation/pump/continuation Provider 与 Driver 装配点；integration `tests/agent/hooks/consumer.rs`+data/expected | 全路径真实消费者、恢复、无绕过；超过上限分批 |

Core、协议 SDK、Provider wire DTO、四环境工具实现、Server HTTP API 不在预期写集。
Server 只透传 Runtime 的中立 port，不解释 script 协议或反向依赖 Agent。
公共 API 由 Main 审查再实施，不以测试手组 executor 冒充 AgentRunner 消费者。

## 独立数据测试与实证要求

跨 crate/进程测试统一 `crates/kolyan-integration-tests`；拟入口
`tests/agent/hooks.rs`，子模块 native/consumer/recovery，fixture
`tests/fixtures/agent/hooks/*.json`、readonly scripts，expected
`tests/expected/agent/hooks/*.json`。单元 tests 在独立文件，不动旧 case/assertion。
case 数据包含事件、精确 scope、manifest、policy、脚本选择、故障阶段与预期语义；
脚本为受控真实 native 执行，不生成假 OS receipt，不靠模糊 sleep 排列触发次序。
故障等待使用实际 Spawn/host admission barrier，有限 host 等待，不扩生产 deadline。

| 组 | 必须独立 case |
| --- | --- |
| 定义/权限 | 同版本幂等/冲突、foreign scope、无 ACL、撤销、未知 capability、修改 manifest/body/interpreter、子不继承 |
| native协议 | continue/deny、exit失败、prose/坏JSON/重复key、超input/output、禁止写、禁止网络、control/credential读取拒绝 |
| liveness | timeout、cancel beforeSpawn/afterSpawn、drop、fork后代清理、output pressure，真实 PID/group/reaped/capture/loss计数 |
| BeforeModel | pass请求深等于Core原请求、deny零Provider开启、后续Step每次开启、SDK内部retry不重复hook、录制失败阻断 |
| BeforeTool | pass精确scope/grant、deny零EffectStarted/receipt/物理效果、approval挂起不执行、恢复执行一次、savedreceipt不重做 |
| AfterTool | 已有receipt后observer失败仍保留原结果、waiting不伪完成、receipt replay不重做、恢复发现incomplete不回滚/重放 |
| rebuild | 重新构造Host/Runner、exact注册facts+artifact恢复、撤销/换脚本拒绝、Started无完成不执行、跨Step重用callID不混淆 |

每 case 将完整实际 hook stdin/stdout/stderr、ModelRequest、Provider/Step事件、
scope/grant/prepared、工具结果、Ledger/Journal与 native lifecycle 输出至 proof
目录 `actual.jsonl`，模型 reasoning/text 不只保存标签；敏感字段只按声明投影。
先持久化全计划，再逐行观察/导出后比较；早失败继续收集其他 case，attempt=1，
物理读回 actual 对 expected，比对忽略 PID/时间随机值但不忽略事件内容/错误类型。
只有 drop 触发且实际 observer 有回收证据才能声称 OS 已停止。

第一可验收纵向实现必须联合 B/C/D 提供 scripted Provider、真实受控 native script
及实际 Agent消费者；A 或 B 单独只能报告基础切片，不报告 hooks 已完成。
mock动态授权只作为单元负例，不能替代 production bridge 验收；没有vendor请求
的证据明确称离线真实 OS，不称真实模型。后续实际模型只由 Main单独授权，
限定 MiniMax 实际协议/模型配置并保存真实请求，不由本文自动授予网络实验权限。

## 门禁、缓存与交付

本轮仅文档检查：唯一新增路径、基线/分支、diff空白、行数，不跑 Rust或模型。
批准后每批按序 focused → crate/integration → strict → fmt/layout，完整门由 Main。
私有固定 target 拟 `/private/tmp/kolyan-governed-hooks-target`，当前未创建；不复制
Main缓存，不另建v1/v2 target。5GiB预警、10GiB停止新编译并报精确清理清单，
只在无活动cargo/rustc时由授权宿主清理build产物；不删 source/proof/log/receipts。
proof 使用独立 tempfile目录，绝不放target内。提交/推送均由Main，当前禁止。
完成报告区分设计、已实现、离线OS、真实模型四种状态，附真实退出码/日志/摘要。

## A/B 冻结实现与实际证据

实现仅新增 Agent `hooks.rs`/hooks 子树与 central integration hooks 新路径。
注册/撤销复用真实 FactJournal/Required ArtifactStore；bind/restore 读取真实
AgentInvocationBindingStore 的 ownership。动态 host ACL、当前撤销、native fingerprint
每次 dispatch 重验；BeforeModel 必须显式提供 HookExecutionWindow(control, remaining)，
没有 ModelProvider deadline 推测或隐式 context。有效脚本 deny 返回独立 Denied，
Journal/Artifact/协议/安装故障返回 HookError，不能当普通模型参数反馈。

A/B 暂只提供 digest-only event 投影；native fingerprint 固定安装路径/cwd、
解释器 identity/hash 和 Sandbox policy revision，脚本是 Required artifact 精确内容。
直接使用固定 /bin/sh argv + exact-file MacOsSandbox，无写/网络/凭据授权。
阻塞安装/存储/文件校验在 blocking worker；总等待有界，但已开始的 blocking
publication 可以在 async waiter drop 后完成，不能因此声称事实或安装不存在。
脚本内容身份前后重验不是原子 TOCTOU 防护，宿主私有目录仍是信任前提。

operation_id 绑定原 binding，重复 dispatch 拒绝；Started 无 Completed 为 Interrupted，
不自动重放。没有 Runner/Runtime/Server 消费者，AfterTool 测试中的目标 receipt
坐标为明确 synthetic event 输入，不冒充实际四工具效果。真实 native 生命周期
来自现有 Sandbox observer；C/D 尚未实现，完整 hooks 与真实模型均未验收。

2026-10-03 私有固定 target `/private/tmp/kolyan-governed-hooks-target`：

- 全 Agent 110/110，exit 0，`/private/tmp/kolyan-governed-hooks-agent-final.log`。
- hook 基础数据 36 行、来源/存储故障 16 行、严格协议 10 行；3 个独立单测。
- central `agent_hooks` 1 个数据框架，16/16 真实 native 行，exit 0，
  `/private/tmp/kolyan-governed-hooks-native-final.log`；完整实际轨迹：
  `/private/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-native-hooks-lERdjx/actual.jsonl`。
- Agent all-targets strict 与 central target strict 均 exit 0，日志分别为
  `/private/tmp/kolyan-governed-hooks-agent-strict-final.log`、
  `/private/tmp/kolyan-governed-hooks-native-strict-final.log`；fmt exit 0。

临时验证登记已撤回，Main 最终登记需：Agent lib `pub mod hooks;`、Agent
直接依赖 kolyan-sandbox（Tokio 显式 time feature）、integration Cargo 的 agent_hooks
test target 指向 tests/agent/hooks.rs。原 lib/Cargo/lock 与既有测试均保持基线字节。
所有 proof 在 target 外；缓存约 1.3 GiB，所有本批 gate 终态，无网络/commit/push。
