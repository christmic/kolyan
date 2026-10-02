# Goal Verification and Usable Agent Host

## Status and outcome

Implementation in progress under [the evolution program](0031-agent-evolution-program.md).
Deterministic goal evidence and the file-write checker have integrated receipts
below; Root admission/finalization is undergoing Main acceptance. Production
AgentRunner assembly and bounded correction remain unfinished. The historical
baseline `3043095` had physical completion only, not these goal capabilities.

The required outcome is explicit, evidence-backed postconditions and a usable
host that reports execution, goal satisfaction and remaining work separately.
A final answer or final refusal alone cannot satisfy a tool-effect objective.
Unmet postconditions can lead to bounded new work, never blind replay.

## Confirmed implementation seams

- Agent `runner/admission.rs::admit` registers only ExecutionCompleted.
- Agent `runner.rs::finish` invokes finalization when invocations stop;
  `runner/finalization.rs::finalize_saved_task` verifies physical results and
  result consumption before calling TaskExecutionService::complete.
- Server `task_driver.rs::record_stopped` derives completion evidence from actual
  Runtime facts. ArtifactDigest currently verifies serialized ModelResponse
  bytes, not a workspace file or an arbitrary business artifact.
- Server `task_driver.rs::complete` revalidates physical evidence before success;
  `tasks/reducer/completion.rs::complete` requires previously observed evidence
  for every criterion. Preserve this proof boundary.
- Server `tasks/reducer/topology.rs::admit` allows Continuation only after a
  completed predecessor and with an explicit dependency. A correction should
  use this new-work relationship, not retry the predecessor.
- Server `task_driver/retry.rs::authorize_retry` refuses a whole-attempt retry
  when any effect receipt exists. Goal correction must not evade this rule.
- `services/kolyan-server/src/assembly.rs` assembles real isolated tools and
  providers but not AgentRunner. `apps/kolyan-agent` is a placeholder;
  `crates/kolyan-cli` is not yet a shipped one-shot Agent entry.

## Required contracts

### Goal definition

Goals are trusted host inputs, separate from model-selected tool arguments.
Persist the exact versioned postcondition definitions with Task admission,
including stable criterion IDs, scope, bounded predicate content and digest.
Recovery cannot consult a replacement verifier or reinterpret old evidence.
Reject unknown predicate kinds, invalid schemas, excessive bounds and duplicate
IDs before any model call or tool effect.

The first useful deterministic predicates must operate on evidence Kolyan
actually possesses: admitted model-result content and physically verified tool
results/receipts. They must name the observed property and temporal scope.
For example, a receipt-backed successful write proves an admitted operation
completed; it does not prove that its file remains unchanged afterward.
Workspace-at-verification assertions require an independently scoped physical
read/measurement, not a model statement or a hash supplied by the model.

Physical ExecutionCompleted remains meaningful for lower-level execution tasks,
but a production business goal must declare nontrivial postconditions explicitly.
Do not retrofit hidden semantic interpretation into that existing criterion.

### Assessment and evidence

Assess only independently loaded, scope-checked evidence. A serializable evidence
object passed by a caller is not sufficient proof. Bind the decision to Task,
criterion definition/digest, verifier revision, actual invocation/attempt and
immutable fact coordinates. Verification must not issue grants or run a model.

Distinguish Satisfied, Unsatisfied, Indeterminate and CheckerFailed. These are
goal outcomes, not alternative physical Turn outcomes. Preserve bounded reasons
and evidence references. Missing input, an uncertain effect or unsupported
verification cannot become Satisfied. A final refusal remains visible.

Persist assessments as validated authoritative facts. A successful Task must
consume all required satisfied evidence through the existing Server completion
boundary. Replay and finalization revalidate ownership, definition, source and
the admitted verifier contract. Trace/UI projections cannot authorize success.

The first implementation will be deterministic; model-based semantic review is
a later optional verifier with explicit uncertainty, cost and failure policy.
No fail-open model verdict or text-matching heuristic proves a filesystem goal.

### Bounded correction

#### Root 目标输入和收尾接线

第一宿主接线在 `RootRunRequest` 新增显式 `goals: Vec<GoalCriterion>`。所有
调用方同步迁移，现有 execution-only 场景明确传空数组，不引入旧入口或隐式
兼容 fallback。实际业务目标宿主必须提供非空目标，不能在任务结束后从模型
写入内容反推 predicate。Root 注册保留独立 `root-final-answer` 执行 criterion，
并追加全部 Goal；总 criteria 沿用 Server 1..128 上限，因此最多 127 个 Goal。
目标 ID 不得与执行 criterion 重复，invocation_id 必须等于本次实际 root。
Server 的精确 registry/schema/digest 校验仍是最终准入门禁；缺失 checker、
未知 revision、错误 predicate 或错误 owner 在任何模型与工具调用前失败。

TaskDefinition 的不可变 criteria 是目标 SSOT，不把另一份 goals 写入模型输入
或重复维护在 Binding 中。Goal 不是权限，不能改变模型、工具库存、grant 或
Agent snapshot。信任集合来自宿主实际已装配的适配器，不能来自模型或 predicate。

现有 `finalize_task` 继续先验证全部 invocation、历史终态和必要 child result
consumption。物理失败与取消沿用现有明确结果，不为了取得 assessment 重入
模型、工具或恢复执行。全部物理执行成功后，对尚未 assessment 的已声明 Goal
调用实际 `TaskExecutionService::assess_goal`，再调用其 `complete`；已有结果
由 snapshot/replay 重新验证，不能改写或以新事实替换失败结论。assessment
fact ID 必须与 Task/criterion 稳定绑定；断点重建及重复收尾不重复评估写入。
Satisfied 才能进入业务 Completed；其他结论返回 durable Waiting，操作性
checker/source/storage 错误保留原错误，不持久化为可纠正的假 Unsatisfied。

新增独立 Runner 数据场景覆盖真实文件写入目标、最终拒绝、错误内容、合法
物理终态但无效果、缺 checker/版本错误、伪造目标归属、审批前零效果、审批
重建后目标验证、assessment 与 complete 之间断点以及重复收尾。离线运行
实际 Runner/Runtime/Tools；真实 MiniMax 两协议由模型产生调用，并比较完整
实际 JSONL、收据和 Goal facts。原输入、fixture、断言保留；结构体字段迁移
只增加显式空 goals。该接线不启动自动纠错，后续 correction 必须单独绑定
原目标与新 Continuation 的证据范围，不能放宽当前 root-only checker 门禁。

An unsatisfied goal does not make the completed physical attempt unsuccessful
retroactively. Keep the Task nonterminal while an admitted correction remains
possible; distinguish goal waiting from active execution in host results.

Correction creates a new Continuation and attempt, using verified predecessor
context and result dependencies. Bound correction count, shared invocations,
Steps, tokens and elapsed work with a persisted policy. Reconstruct the remaining
budget after restart. Never reset limits through a new Runner instance.

Each continuation uses fresh identity and independent preparation, policy and
grants. A previous receipt is evidence, not permission to repeat an effect.
Uncertain effects require existing reconciliation first. Cancellation, exhausted
limits and checker failure stop automatic correction explicitly.

Define exactly which continuation evidence may satisfy which original goal;
an unrelated child, Task or later invocation cannot supply it. Completion must
still account for admitted child outcomes and required result consumption.

### Host assembly

#### 首批生产宿主的实现契约

新增可复用 crates/kolyan-agent-host，由服务与一次性 CLI 共同依赖；Server
领域 crate 不反向依赖 Agent。AgentHost::open 装配真实 AgentRunner、精确
catalog、SQLite Ledger/FactJournal、FileSessionStore、ArtifactStore、
BindingStore 和 InstanceRegistry；重建复用相同路径和稳定 host ID，不
重新选择已保存 definition。ProviderFactory 使用真实两协议适配器与参数表，
EnvironmentToolFactory 使用真实 IsolatedToolSet 和 snapshot 权限。
不导入 integration-tests 或测试 factory，也不新增 Agent loop。

配置显式提供 workspace、worker、staging、受保护控制目录、工具 scope、
Provider deployment、参数表、环境凭据字段、精确 Agent catalog、当前 host
permissions、context policy 和预算。首批支持一个明确匹配的 deployment，
不匹配 definition 的模型必须在准入前拒绝；设计保留 factory 扩展而不猜测
供应商。输出上限须在进入 Root 前明确解析；未知计数仅在显式 Inspect 策略
下运行，不偷换为 Strict 通过。控制存储、配置与工作区沿用真实物理隔离检查。

公共操作为 start(session_id, request)、query(session_id, task_id)、
cancel(session_id, task_id)、decide_approval(session_id, task_id,
invocation_id, approval_id, decision)。所有调用都核验逻辑 Session 与实际
保存 ownership，不接受客户端提供的 snapshot、grant、checkpoint 或可信
FactRef。selector 复用严格 AgentSelector，具名必须精确 revision；inline
权限不能超过当前 host ceiling，不能从具名配置或 display_name 借权限。

首批业务 Goal 输入是相对文件 path 和期望 UTF-8 content。Host 在准入前
解析物理目录身份、取得实际 adapter revision、计算 bytes/SHA-256，构造
FileWriteCommittedV1 criterion；客户端和模型不选择 checker/revision。
Task view 分别呈现物理执行、Goal verdict、待审批、外部等待、恢复需求和
错误，不把最终文字视为文件目标成功，也不自动启动尚未实现的纠错。

审批 Accept 沿用 root resume_approval 或 child resume_agent_child_approval，
子恢复后继续真实 pump_agent_children，不能返回等待而无人推进。Deny 必须
增加 Agent/Task-aware 的验证与收尾入口，复用相同 ownership/attempt 检查
和已有底层 deny；不得绕过 Runner，也不得替换为整 Task cancel。取消使用
真实 Task 服务及 Ledger 投递，不承诺跨库原子或所有进行中效果立即停止。

宿主批次先实现可复用 crate 与 Agent 审批拒绝缺口；涉及服务和 CLI 的共享
装配迁移、HTTP API、跨语言 schema 由主控另冻下一批，再接产品入口。已
存在的 Session/Turn API 保留其合法低层语义，不伪装成 Agent API。新增四类
Task HTTP 操作的精确路径与 DTO 必须先登记到 HTTP/OpenAPI 契约；首批不
添加事件流、分布式租约或传输层命令账本。

新增数据门覆盖具名/inline、成功/未满足、扩权拒绝、根/子审批 Accept/Deny
跨进程重建、取消及重复恢复零重入。真实 MiniMax 两协议必须由生产 Host
执行，不能以现有测试 factory 矩阵代替。共享装配避免复制 Provider/工具
治理实现；新源码与测试分文件，写集交叉和 Cargo.lock 由主控协调。

Compose the existing AgentRunner, exact catalog, ownership stores, real providers,
context preparation, isolated tools and Task/Session services. Do not copy the
loop or use test factories. Secrets are environment references in project-local
configuration, not persisted Agent definitions or logs.

The host must expose start, query, cancellation and approval resume with stable
Task/execution/approval identities. Approval suspension releases the running
loop; a rebuilt host can query and resume the exact durable continuation.
Clearly report physical disposition, goal assessment and remaining work.

Server remains the shared endpoint for interactive clients. A one-shot CLI
reuses host assembly locally without requiring a server process. It returns a
non-success outcome for unmet/indeterminate goals and provides pending approval
coordinates rather than inventing approval. A desktop/TUI Agent app remains
a client, not a second runtime.

## Implementation increments and design gates

### First tool goal and effect proof inspection

The first concrete tool predicate is FileWriteCommittedV1: a specified physical
target received the specified UTF-8 content through a governed file.write.
Bind workspace/parent physical directory identities and leaf, expected SHA-256
and byte length. Do not bind an old target inode: atomic replacement changes it.
This proves the operation at completion, not the file's later current state.
Goal checker code interprets the tool-specific binding; Server does not acquire
a dependency on concrete tool adapters just to implement a universal loop.

Satisfied requires an independently verified actual preparation, authorization,
start and Completed receipt, a non-error ToolResult, matching physical target,
and matching prepared content/result digest and byte length. Display paths and
model assertions cannot substitute for physical resource binding. Unknown
checker revision, missing proof or incomplete inspection fails closed.

#### 文件 worker 收据的独立校验

现有 isolated file adapter 对零退出码的 JSON 结果只比较显示 path；目标证明
不能把这种结果直接视为经过内容校验。本批增加 production decoder：严格按
FileOperationResult 解码并校验 path、规范小写 SHA-256、操作上限和内容形状。
Read 必须有完整 UTF-8 content，其字节数与摘要必须等于返回值；Write 的返回
content 必须为空，bytes/hash 必须等于实际 prepared write content。Edit 返回
content 为空且满足写入上限、摘要格式；不能凭 new_text 推算整个修改后文件，
不宣称已经独立验证其完整内容。校验不重新打开文件，也不比较旧 inode。

零退出码后 JSON 损坏或收据不一致时，Read 返回普通验证错误；Write/Edit
可能已在 rename 提交后产生该结果，因此返回新的 IsolatedFileError::Uncertain，
Turn adapter 必须映射为已有 ToolError::Uncertain，而非普通可反馈后纠正的
Failed。不得重放效果或把未验证结果保存成 Completed。保留正常工具上限与
权限检查、现有超时/取消契约；这不是对所有进程终止后效果状态的完整判定。

新增独立数据驱动解码与边界分类测试，覆盖空/Unicode 内容、不同 path、缺失
read 内容、非空 replacement 内容、bytes/hash 不一致、未知/重复 JSON 字段、
负数/溢出 bytes、上限和 edit 的不可独立推算边界。完整 operation、raw stdout、
limits、result/error 先导出，再物理回读比较。已有 real local file-worker 矩阵
独立回归，不能用纯 decoder 测试冒充实际工具或模型执行。此修复是可信收据的
前置条件，目标 checker 仍须独立重验准备、授权、scope 和物理终态来源。

该 production decoder 已接到 isolated adapter 的真实零退出码结果路径。新增
27 个收据数据场景和 2 个 Turn 错误分类场景，完整 stdout bytes、operation、
limits 和结果先导出后回读比较。主控 Tools 回归 70 passed、0 failed、0 ignored；
已有真实本地 file-worker 与 exact-file boundary 共 7 个测试函数通过，原输入与
断言未修改。严格 Tools Clippy 通过。这些是 decoder 和本机进程证据，不是
真实模型目标 checker 的验收。

日志：`/tmp/kolyan-worker-receipt-main-v2.log`、
`/tmp/kolyan-worker-receipt-native-main-v1.log`、
`/tmp/kolyan-worker-receipt-strict-main-v1.log`。完整新增矩阵文件位于 native
临时根目录的 `kolyan-file-receipt-rvvOeJ/actual.jsonl` 与
`kolyan-file-receipt-boundary-aV4oIz/actual.jsonl`，真实进程轨迹由 native 日志
单独记录。正常 rename receipt 仍只证明历史提交，不证明当前文件未被再写。

Runtime owns the first released proof-read slice. Add a read-only
`inspect_effect_proof` entry in a dedicated module, internally reusing
`reconciliation::receipt::PreparedEvidence::from_facts` and `validate_receipt`.
It must not inspect through an executor, reauthorize, append or execute work.

The host request supplies ExecutionKey, exact effect ID, an exact physical
terminal coordinate (event ID and cursor), and explicit input/result byte bounds.
The reader independently loads execution admission, preparation, authorization,
start, receipt and terminal using exact IDs. Verify kind, execution/Turn identity,
payload binding and strict cursor order: admission < prepared < authorized <
started < receipt < terminal. It verifies the terminal coordinate, not an
untrusted bare cursor. Initial terminal kinds are actual stopped Runtime facts;
Server subsequently requires the appropriate verified successful attempt.

Expose an opaque, non-deserializable verified result with read-only preparation,
scope, definitive result and source-coordinate accessors. Do not expose a grant
issuance or execution method. A verified Failed receipt is distinct from an
uncertain/missing receipt; neither proves goal satisfaction. This proof alone
does not establish Task membership, checker acceptance or a goal verdict.

Bound effect/request identities according to existing valid identity contracts;
bound every loaded evidence payload before deserializing, including admission
and terminal. The caller may tighten limits but cannot exceed documented host
hard ceilings. No fallback to full-history unbounded reads. Report invalid
request, missing evidence, binding/ordering mismatch, bounds and storage failure
distinctly. Synchronous ledger access must be moved off async workers by hosts.

Add separate data-driven Runtime tests for valid completed/error receipts,
missing facts, wrong kind/identity/Session/Turn/Step, changed grant or revision,
changed result digest, cursor ordering, foreign terminal, bound limits and
SQLite reconstruction. Export all actual observations before comparisons;
prove zero writes/executor calls. This is accepted only as a prerequisite slice,
not as completed goal verification or production Agent delivery.

### Task integration and correction ownership

#### Concrete Task contract for the next implementation slice

The following contract is proposed for independent review before releasing the
Task write set. It extends the existing entities rather than introducing a
second goal journal or loop. The Runtime effect reader is its prerequisite.

```rust,ignore
struct GoalCheckerKey { kind: String, revision: String }
struct GoalCriterion {
    id: String, invocation_id: String, checker: GoalCheckerKey,
    predicate: serde_json::Value, predicate_digest: String,
}
// Add Goal(GoalCriterion) to CompletionCriterion.
enum GoalVerdict { Satisfied, Unsatisfied, Indeterminate, CheckerFailed }
struct GoalAssessment {
    criterion_id: String, predicate_digest: String,
    checker: GoalCheckerKey, attempt: AttemptBinding,
    source: ExecutionEvidence, verdict: GoalVerdict,
    reason: String, proof: serde_json::Value,
}
trait TaskGoalVerifier: Send + Sync {
    fn validate_criterion(&self, criterion: &GoalCriterion) -> Result<(), TaskError>;
    fn verify_assessment(&self, snapshot: &TaskSnapshot,
        assessment: &GoalAssessment) -> Result<(), TaskError>;
    fn verify_completion(&self, prefix: &TaskSnapshot,
        evidence: &[CompletionEvidence]) -> Result<(), TaskError>;
}
```

GoalCheckerKey names an exact host-registered implementation, never a model
plugin or arbitrary executable. The registry rejects unknown kind/revision and
validates the bounded predicate at registration before admitting an invocation.
The predicate digest is SHA-256 of the compact serialized immutable predicate;
the criterion ID, owner and checker key are also bound by the admitted Task.
No model may alter the predicate or select a replacement checker on recovery.

TaskGoalVerifier is an enforcing host port, not an observer. TaskCoordinator
receives its configured implementation through an explicit constructor/builder.
Without that implementation, any Task containing Goal criteria is rejected;
existing explicitly execution-only Tasks remain valid and are not silently
converted to semantic goals. The concrete implementation independently reloads
Runtime source evidence and invokes a registered deterministic checker.

Append and replay both enforce the port for registration, assessment and Task
completion. Unlike the existing post-apply input-source hook, goal validation
runs against the event-before prefix: envelope/bounds validation, goal source
verification, pure reducer apply, then append. Registration validates its
criteria directly because there is no prefix yet. Input-source checks retain
their necessary post-apply invocation binding check. The port receives the
already-replayed Task prefix, so its Runtime
reader must not call TaskCoordinator::snapshot or a helper that does so. That
would recursively verify the same assessment. Source loading uses exact lookup
or bounded query, not a full-history fallback. Physical Task membership and the
exact ExecutionBound/terminal/result binding are rechecked before a verdict.

Add a critical TaskEvent::GoalAssessed and a derived assessment history to
TaskSnapshot, with the exact journal FactRef for each assessment. Its causes
include the preceding Task fact and exact stopped-attempt observation. The
assessment is separate from AttemptOutcome::Completed: no retrofit changes the
physical attempt or its usage. An assessment cannot masquerade as an ordinary
AttemptObservation or mark_recovery event.

GoalAssessment.source is the exact physical terminal, equal to the latest
stopped observation's source. Checker-specific effect coordinates belong in
the strictly decoded proof. The observation FactRef is independently bound to
that attempt, not taken from a caller's arbitrary terminal_fact. The first
slice permits one assessment per exact criterion/attempt and identical fact
retry; it does not silently re-evaluate a transient failure under a new ID.
Storage faults, missing checker and unreadable/corrupt authoritative sources
reject append without creating a replay-unstable verdict. Indeterminate covers
deterministic incomplete coverage or uncertain effects. CheckerFailed may be
persisted only for a deterministic checker failure with reproducible inputs;
otherwise report an operational error outside authoritative goal history.

The first integration assesses only the criterion's exact root invocation and latest
physically completed attempt. No child or ordinary Continuation can satisfy it.
Root admission verifies that all Goal anchor IDs equal the unique actual root;
assessment rechecks its role and full binding. Physical observation validation
explicitly rejects GoalSatisfied evidence; it belongs only to goal completion.
The later correction slice extends this through explicit persisted correction
edges; it must not simply remove this ownership check. Multiple identical fact
retries remain idempotent; a changed payload under one fact ID conflicts. A new
assessment after Satisfied is rejected. A later non-satisfied assessment requires
new admitted correction evidence, not a different narrative about the same run.

Add CompletionEvidence::GoalSatisfied with criterion ID, physical source,
assessment FactRef and digest. It is accepted only when it exactly references
the latest independently reverified Satisfied assessment. Physical completion
records do not fabricate it. The reducer gathers this evidence separately from
attempt observations and requires it alongside all existing physical and child
consumption checks. Public complete_task and replay cannot bypass this check.

The digest is explicitly assessment_digest, over the complete typed assessment,
not merely its predicate. The Completed fact causes include every consumed
assessment FactRef. verify_completion independently revalidates every admitted
invocation's actual successful stopped outcome, as well as all GoalSatisfied
references. Rechecking only the root goal would leave a bypass through forged
child observations and the public coordinator completion method.

The concrete SourceReader owns only bounded Ledger reads and the authenticated
prefix/bindings, not a TaskExecutionService or coordinator. It independently
verifies ExecutionBound, the actual FinalAnswer Step and terminal, conflicting
terminals and unresolved effects. Runtime's selected effect helper does not
prove that an entire attempt succeeded. Search must freeze its through cursor
and enforce cumulative rows/bytes; exhausting the bounds is Indeterminate.
Do not copy the generic 256-byte identity bound onto composed receipt event IDs.

WaitingReason gains an explicit GoalAssessment/GoalUnmet reason. A completed
physical attempt with no assessment or a non-satisfied verdict does not project
as ordinary Ready. Cancellation and terminal states still dominate. Initial
goal evaluation returns this waiting disposition; automatic correction is not
enabled until the separate persisted budget/edge slice is verified.

refresh derives waits from assessment history on every replay; a transient push
into waiting is insufficient. TaskExecutionService collects goal evidence
separately. Runner finalization reports unmet/indeterminate goal waiting rather
than calling unconditional success or treating normal goal waiting as an
unexpected finalization error. Those actual consumers must be migrated before
the slice can claim acceptance.

Hard bounds for this slice: at most 128 Goal criteria, 64 KiB per predicate,
64 KiB per assessment proof, 8192 UTF-8 bytes per reason and 1024 referenced
source coordinates per assessment. Existing stricter transport/journal bounds
still apply. A bounded exhausted search is Indeterminate, not evidence that no
effect occurred. Proof shape, source coordinates and verdict must be recomputed
by the concrete reader/checker, not accepted from caller JSON.

Server owns these types and enforcing transition hooks, with no concrete Tools
dependency. The Agent host composes the first FileWriteCommittedV1 checker and
the Runtime reader. Its predicate binds workspace and parent directory identity
chains, leaf, expected UTF-8 byte length and lowercase SHA-256, excluding old
target inode and display-path aliases. It compares both prepared write content
and definitive result; a forged result hash, is_error flag, unrelated effect,
uncertain receipt or physically non-successful attempt cannot satisfy it.

#### 冻结的 Agent 文件目标 checker

`FileWriteCommittedPredicateV1` 为 deny_unknown_fields 的独立 typed predicate：
`schema_version` 固定 1，`tool_revision` 为受信任的精确适配器 revision，
`workspace/parent` 复用 Tools 的 ExactDirectoryBinding，另含 `leaf`、
`expected_bytes: u64` 与小写 `expected_sha256`。目标由可信宿主在准入前定义；
不能从模型已经生成的写入内容反推目标，使任何实际写入自动满足它。
目录 path 必须按 native physical 规则规范化、绝对、无 NUL、最多 128 components；
identity chain 长度等于 components 数，并验证 workspace 是 parent path 与
chain 的前缀。leaf 是单个普通 UTF-8 文件名，不额外套用通用 256 字节 ID 上限。
复用 Tools schema，不新造 worker request；历史 assessment 不 stat/open 文件。

`FileWriteCommittedChecker::new(trusted_tool_revisions: BTreeSet<String>)`
返回 `Result<Self, TaskError>`，只由 host 配置受信任的真实适配器版本。集合非空、
至多 128 项，revision 遵循 PreparedCall 的原界限；不能按 file.write 名字或
`file-worker-v2/` 前缀授予信任。registry key 的 kind 为 `file_write_committed`，
revision 为 domain-separated SHA-256，绑定 checker schema/实现版本及有序
信任集合。重建必须加载同一受信任配置；改变信任集合改变 key，不能在同 key
下悄悄接受不同适配器。predicate 的 tool_revision 必须在该集合内。

宿主从已经完成规范化和 protected-roots 合并的实际工具实例获取信任版本：
`IsolatedFileTools::adapter_revision(&self) -> Result<String, IsolatedFileError>`
公开既有精确 revision 计算，并由 prepare 使用同一方法；
`IsolatedToolSet::file_adapter_revision(&self) -> Result<String, IsolatedToolSetError>`
只转发内部文件实例。两个入口均为同步只读文件 I/O，宿主应在阻塞装配阶段
调用；读取 worker 二进制但不执行它，不准备假调用、不读取目标文件、不授予
权限。worker/配置改变后重新计算可能改变结果，不能将此方法当作历史证明。
历史 checker 仅消费保存的信任集合，绝不在 assessment 时重新访问当前 worker。
新增独立数据矩阵验证克隆稳定性、prepare 使用同一值、配置和 worker 字节
改变、ToolSet 合并后的实际值、读取失败与无目标文件副作用；旧测试保持不变。

该宿主版本读取前置能力已集成并独立验证：Tools 71 passed、0 failed，新增
18 行实际临时文件数据矩阵全部先导出、关闭并物理读回；当前主线原有 strict
receipt 校验模块保留。另跑真实 file_worker_process/exact_file_boundary 两目标，
7 个函数通过。日志分别为 `/tmp/kolyan-tools-revision-main-v1.log` 与
`/tmp/kolyan-tools-revision-native-main-v1.log`。这些验证不是具体文件目标 checker、
实际 Agent 宿主或真实模型目标验收，后续消费者仍须完成。

直接实现已冻结的 Server GoalChecker 接口，仅消费 VerifiedGoalSource。完整
source 上寻找匹配的 Completed、非 is_error receipt；严格解析真实 FileOperation
及直接保存的 ExactFileBinding，核对 scope execution/snapshot、工具名、精确
revision、FilesystemWrite / Create+Update / NonIdempotent 和 sandbox requirement。
物理 resource 必须等于 bound parent+leaf。prepared content 的 UTF-8 byte/hash
与目标一致，ToolResult call ID 与 prepared 相同；strict FileOperationResult
显示 path 与 prepared arguments.path 一致，但身份来自目录链/leaf。result
content 必须为空、byte/hash 同时等于目标与 prepared content，不比较旧 target
inode。不可用 edit/read 或错误结果冒充 write。

匹配存在只证明历史受管写入提交，不证明最后一次写入、当前文件内容或断电
持久性。多份匹配按 receipt cursor、event ID 确定性选一；Satisfied proof 严格
带版本、六个 effect source 坐标、prepared digest、完整 ToolResult digest 和
byte/hash。完整 source 无匹配是 Unsatisfied，proof 带扫描终点、检查数量及
失败/不匹配分类计数；不完整 source 是 Indeterminate，不能证明缺失。损坏的
绑定/result/claim/source 返回操作性 TaskError，不保存可纠正的假 miss。

Server verifier 公共端口另增加 `compute_assessment(&TaskSnapshot, criterion_id)`，
供 Service 自己计算再提交；Submitted assessment、append/replay/completion
仍全部独立重算。已冻结 VerifiedGoalSource getters 为 binding、terminal、
response、coverage、effects、through；均只读，没有 Deserialize 或 caller
constructor。第一 checker 及新数据测试归 Agent goals 模块；Server 不依赖
Tools，Agent 可依赖 Tools 的 schema。新增合法/错误/不确定/超限、物理绑定与
alias、旧 inode变化、版本变化、多 witness、Memory/SQLite 重建数据矩阵，
再接实际 Root Runner 和 MiniMax 工具写入目标；纯 checker 通过不是宿主完成。

The first Task integration dataset includes missing checker, unknown revision,
malformed/digest-mismatched predicate, forged Satisfied input, missing/foreign
proof, wrong terminal, definitive failure, exact success, duplicate/conflicting
assessment, direct coordinator completion bypass, replay tampering and
reconstruction between assessment and Task completion. All actual rows and
full source content are exported before comparison. Host live tests and bounded
correction remain required subsequent consumers; this slice alone does not
complete requirement 0035.

Additional mandatory negatives: verifier must not invoke a second snapshot;
public completion with forged physical observations; a valid target receipt
alongside an unresolved effect; deterministic CheckerFailed reconstruction and
operational storage failure followed by an identical retry. Each is tested at
append and replay, not merely through a test-only validator.

#### Released Server write set and source reader interface

After two independent source reviews, the enforcing Server slice is released.
It owns Task goal types, coordinator/reducer transitions, a concrete bounded
Ledger reader, checker registry, TaskExecutionService assessment/completion
consumers and independent data-driven tests. Agent-specific file predicates,
Runner admission/finalization and production assembly are separate writes;
the Server slice is not accepted as the whole goal capability without them.

```rust,ignore
GoalSourceReader<L>::inspect_stopped(
    &self, prefix: &TaskSnapshot, binding: &AttemptBinding,
    source: &ExecutionEvidence, limits: &GoalSourceLimits,
) -> Result<VerifiedGoalSource, GoalSourceError>;
trait GoalChecker: Send + Sync {
    fn key(&self) -> &GoalCheckerKey;
    fn validate_predicate(&self, criterion: &GoalCriterion) -> Result<(), TaskError>;
    fn assess(&self, criterion: &GoalCriterion, source: &VerifiedGoalSource)
        -> Result<ComputedGoalDecision, TaskError>;
}
```

VerifiedGoalSource has private fields and no Deserialize/caller constructor.
It exposes read-only exact binding, physical terminal, actual final response,
coverage and verified effect proofs. ComputedGoalDecision includes verdict,
bounded reason and strictly checked proof; the concrete LedgerTaskGoalVerifier
combines the reader and exact-key registry and compares the complete computed
decision against the candidate assessment. No network or executor port exists.
Unknown registry entries reject Task registration, not just eventual completion.

The reader's scan is scoped to the exact execution, using required query pages
of at most 1024. It has hard ceilings of 4096 total rows, 32 MiB cumulative
serialized evidence, 16 MiB per event and 16 MiB final response; caller limits
may tighten them. No full-history helper is used. Read to a bounded observed
end, freeze the observed last coordinate and validate the selected stopped
source within it, including conflicting later physical terminal facts and
unresolved effects. Exhaustion cannot imply absence or Satisfied. This is an
observed immutable-source proof, not a guarantee against arbitrary future
storage corruption; later completion/replay independently rechecks sources.

Effect proof reads also count toward cumulative resource ceilings; at most 128
effects are inspected for one source. Fully bound definitive Failed receipts
are not successful effects. A corrupted source is an operational validation
error, not a model-correctable goal miss. Deterministic coverage exhaustion or
uncertainty is explicitly classified and cannot lead to automatic correction.
The verifier must use the authenticated prefix, not recursively reload Task.

First implementation exposes an actual service assessment operation that
computes and appends its own decision; caller-supplied assessment submissions
still pass the same pre-apply verification. Goals are not generated by
record_stopped. Completion reconstructs GoalSatisfied from exact saved
assessment facts and rechecks all physical invocations. No public command path
is exempt. New tests may use an explicitly synthetic deterministic checker to
exercise the enforcing paths; they do not replace the subsequent real file
checker or MiniMax host gate.

Persist goal assessments separately from already-stopped attempts. Reuse Task
criteria, journal and completion; do not create a duplicate GoalTask entity.
New critical goal facts require independent source validation both on command
append and replay, including public coordinator completion paths. A caller's
serialized assessment cannot bypass the TaskExecutionService check.

The successor contract must bind root goal anchor, completed predecessor,
Unsatisfied assessment references, exact criterion IDs, successor invocation
and persisted correction ordinal. Ordinary continuations and unrelated children
are not correction evidence merely because they share a Task. Preserve source
and result-consumption proofs, shared budgets and cancellation boundaries.

Unsatisfied with admitted correction available yields an explicit goal wait;
without correction or with exhausted budgets it yields goal failure. Indeterminate
and CheckerFailed block success and automatic correction, with their own reasons.
Do not alter a completed attempt via mark_recovery or project these states as
ordinary Ready. Existing TaskLimits has no Task-total Step/elapsed ceiling;
such correction ceilings must be implemented, not assumed present.

Explicit physical Refused is already excluded from successful FinalAnswer
evidence. Ordinary FinalAnswer text that expresses refusal or falsely claims a
write can still pass execution-only criteria; the new goal gate rejects a
missing required effect in either case.

1. Settle the concrete predicate/evidence schema and test matrix using the above
   source seams. Review how assessment integrates with Task replay/completion;
   do not add an advisory verifier that the actual success path ignores.
2. Implement deterministic goal assessment, durable binding and explicit host
   result classification. Migrate all callers deliberately; retain old scenarios
   and assertions without compatibility adapters or hidden defaults.
3. Implement bounded correction with existing continuation proofs and budgets.
   Add restart/cancellation cases before enabling automatic correction.
4. Assemble the production host and one-shot CLI, then expose the minimal Server
   Agent operations with a separate documented protocol/schema increment.

Each increment requires its concrete decisions and case data before coding.
This draft establishes requirements; predicate shapes and host API names are
not settled by the conceptual labels above. Record those decisions here before
the corresponding implementation begins.

## Acceptance

Add separate module tests and data-driven integration/live fixtures. Preserve
existing execution-only, delegation and approval tests. Export actual input,
result, assessment, correction, receipts and fact coordinates before comparing.

Required cases include satisfied and unsatisfied deterministic goals; final
refusal; missing/wrong/foreign evidence; unknown predicate; checker error;
changed contract/verifier; duplicate assessment; recovery after assessment but
before Task completion; permitted continuation vs unrelated source; corrective
success; exhausted correction budget; cancellation; long approval/rebuilt host;
uncertain effect and no duplicate receipt-backed effect.

For the integrated host, both authorized MiniMax protocols must drive real
single/multiple-Step tasks and actual tools, with normal, denied, malformed,
goal-unmet and correction scenarios. Observing a final answer or test-host
script is not production-host acceptance. Each row states what it exercises.

Completion requires source-level integration through Task success, retained
negative cases, a working real host, offline regression and actual-model evidence.
A pure evaluator, interface, CLI stub or passing JSON-shape test alone is not
the requested business-goal capability.

## Runtime prerequisite integration receipt

The Runtime effect reader is implemented and independently integrated on Main
after `e93464c`, with the seven reviewed Runtime files and this specification.
Public APIs are `inspect_effect_proof`, EffectProofRequest/Coordinate/Sources,
VerifiedEffectProof and EffectProofError, exported from kolyan-runtime.
Private preparation/grant validation is reused, not copied into Server.

Main `cargo test --offline --locked -p kolyan-runtime -- --nocapture` exited 0:
61 passed, no failed or ignored, doc tests zero. Log:
`/tmp/kolyan-evolution-effect-proof-integrated-v1.log`. Added observations are
76 memory, 76 rebuilt SQLite and 8 actual DurableTurnDriver rows driven by an
explicitly scripted Provider/tool. They are not live model or physical file
write receipts. Inspection repeats produce zero new model/tool calls, writes
or unbounded reads. Existing 58 tests remain unchanged.

Actual complete JSONL is exported before comparison and read back from disk.
Main strengthened the new result projection from Debug to typed Result JSON;
preparation, scope, receipt, terminal and all before/after facts remain complete.
Artifacts from the independent Main run:

- `kolyan-effect-proof-fRD5Wr/actual.jsonl` (memory).
- `kolyan-effect-proof-cowJ4R/actual.jsonl` (rebuilt SQLite).
- `kolyan-effect-proof-runtime-DMZbjc/actual.jsonl` (actual Runtime chain).

Their temporary parent is
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/`;
the exact paths are also emitted in the log. Normal workspace all-target
Clippy passed in the `e93464c` pre-commit hook with these Runtime changes
present; no hook was bypassed. Task membership, goal checker, correction and
production host acceptance remain open and cannot be inferred from this gate.

### Server 目标证据集成验证

Server enforcing slice 已通过正常 hooks 分批提交为 `22dcb6d` 与 `763755a`，
尚未完成整体宿主验收。主控独立模块回归终态退出 0：Server 122 passed、Agent
98 passed，均 0 failed、0 ignored；日志 `/tmp/kolyan-task-goals-main-v1.log`。
两 crate 全目标严格 Clippy 终态退出 0，日志
`/tmp/kolyan-task-goals-main-strict-v1.log`。随后主控全仓回归在 `41ac3f4`
对应生产源码终态退出 0：82 个结果组，共 827 passed、0 failed、63 ignored；
日志 `/tmp/kolyan-task-goals-workspace-main-v1.log`。这份全仓结果不包含随后
集成的计量适配器或独立 MiniMax 自我迭代入口，也不证明 ignored 网络用例通过。

新增三组框架在 Memory/SQLite 各执行 13 行 source、25 行 assessment 和
6 行 Service reconstruction，共 88 个数据行；模型及工具为明确声明的本机
脚本适配器，不是网络模型或原生文件 worker。完整实际请求、facts、effects、
查询和结果先导出，flush/sync/关闭后物理读回，再比较。Service reconstruction
新增场景结束旧 Service 生命周期，重新打开 SQLite、重新装配真实 Service，
再完成及重复完成，核对无模型、工具或 Runtime 重入。

初次审查发现旧测试只重建 coordinator、仍调用原 Service 完成，以及导出
写入句柄未关闭；这些证据不足之处已修正并新增独立 Service 重建场景，原场景
和断言保留。新 production reader/registry 在 pre-append、replay 与 public
completion 均重新核验，不以 submitted verdict 或纯模型结束状态代替成功。
具体 FileWrite checker、Agent 目标准入/收尾、有限纠错及生产宿主与真实模型
验收仍须完成；这份回归不证明需求 0035 或演进计划整体完成。

### 文件写入目标验证器集成验证

主控核对隔离冻结清单后集成 11 个 Agent 文件，父 lib.rs 只合并新增导出，
保留 Main 已有能力；Cargo.lock 仅增加 Agent 对既有 Tools 的依赖边。
联合模块回归终态退出 0：Agent 100 passed、Server 122 passed，均无失败
或 ignored；日志 `/tmp/kolyan-file-goals-main-module-v1.log`。Agent、Server
和 Integration 全目标严格 Clippy 终态退出 0，日志
`/tmp/kolyan-file-goals-main-strict-v1.log`。

新增验证器使用 Server 已验证的历史 source，按预先声明的物理目录身份、
叶名称、工具版本、字节数和摘要核对受治理写入；不读当前文件、不把最终
自然语言回答当成功。24 行契约与 60 行历史数据共 84 行观察先导出并物理
读回，覆盖明确匹配、完整缺失、不完整来源与损坏证据。历史用例的模型及
工具为本机脚本适配器，不能声称已经验证原生 worker 或实际网络目标闭环。
Root 输入/收尾接线、纠错和真实宿主仍是未完成的后续工作。

### Root 目标准入和真实原生场景集成

Root 接线和新增审批矩阵已按隔离清单集成到主干，旧调用方只补显式空 goals，
不改旧输入与断言。主干 Agent 回归 101 passed、0 failed、0 ignored，日志
`/tmp/kolyan-root-goals-main-agent-v1.log`；workspace all-target check 退出 0，
日志 `/tmp/kolyan-root-goals-main-all-targets-v1.log`。新增模块矩阵覆盖 14 行
准入及收尾契约。新本机 worker 场景为 Memory/SQLite 各八行，另新增审批
数据矩阵各三行；最终回归与供应商网络结果另行登记，不能从这些结果推断。

集成过程中修改新测试输出配置暴露两次真实拒绝，失败日志和 JSONL 保留：
v1 的 None 违反 Root 显式输出预算契约；v2 的 2048 与测试宿主 reserve 64
不一致，被 Context 校验拒绝，未到网络模型。现统一从新增 dataset 的
output_limit_tokens 读取请求上限与 reserve，不放宽生产准入或目标断言。
v3 聚焦新矩阵终态退出 0：2 passed、0 failed、2 ignored，16 加 6 行本机
原生观察均通过；日志 `/tmp/kolyan-root-goals-main-targeted-v3.log`。

网络验收入口分别执行 MiniMax 两协议下具名/内联正确写入，以及具名/内联
审批暂停、重建恢复和已有文件替换。预期共十行场景；审批前零效果、实际
worker receipt、Goal 六个来源坐标、Satisfied 及重复收尾零重入均比较。
输出上限 2048 是显式测试数据，不是可信计数或保证模型完成的声明。已有
文件替换只检查实际历史提交和内容，不冒称独立测量了替换后的 inode。

完整 agent_root 离线 v3 终态退出 0：74 passed、0 failed、22 ignored，日志
`/tmp/kolyan-root-goals-main-offline-v3.log`；同源码 workspace 严格 Clippy
退出 0，日志 `/tmp/kolyan-root-goals-main-strict-v3.log`。

真实 MiniMax 首次矩阵终态退出 101，188.48 秒，两个测试函数均失败。十行
实际场景中两行取得 Satisfied，其余八行实际工具参数与文件都只有四字节
goal，缺目标规定的 LF。OpenAI 保存的 SDK 响应 arguments、参数 delta、
completed 与 worker bytes 一致，没有发现 Tools 后丢换行；Anthropic
本轨迹没有原始 SSE，只能证明中立 delta 与后续一致，不能冒称原始 wire。
两个实际轨迹为临时父目录下 .tmpmu64vk/actual.jsonl 和 .tmp6vOxWX/actual.jsonl，
日志 `/tmp/kolyan-root-goals-main-minimax-live-v1.log`。

新增数据修订 root-goals-v2-explicit-json-input 将实际用户输入移入 dataset，
明确 JSON 示例及五个目标字节，不预置模型调用、不修改工具参数或 Goal。
新数据 22 行本机聚焦与 workspace strict 均通过，日志
`/tmp/kolyan-root-goals-main-targeted-v4.log`、
`/tmp/kolyan-root-goals-main-strict-v4.log`。第二次网络终态退出 101，85.55 秒，
仍是两个失败测试函数。十行中七行实际取得 Satisfied；三行 OpenAI 场景
仍缺 LF并保持 Waiting/Unsatisfied。七行后置条件成功不能称为矩阵通过。
具名和内联审批均实际暂停、重建及恢复；整体目标验收仍未全通过。
轨迹为 .tmpqeHsd3/actual.jsonl 和 .tmp7kc3wT/actual.jsonl，日志
`/tmp/kolyan-root-goals-main-minimax-live-v2.log`。上述四个临时目录的父目录
均为 /var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/。

保留未满足证据，不追加随机重跑来冒充修复。有限纠错尚未实施，当前只能
正确区分物理 FinalAnswer 与业务目标完成；这份真实运行为后续纠错提供
实际失败输入，不使需求 0035 或整个演进计划完成。

随后主控同一集成源码全仓回归终态退出 0，82 个结果组共 862 passed、
0 failed、66 ignored；日志 `/tmp/kolyan-evolution-root-goals-workspace-main-v1.log`。
此结果包含当前 Root 接线、新数据和已集成截止时间/生成消费者，不包括
隔离开发中的 Skills 或生产 AgentHost，也不改变上述网络矩阵未全通过结论。

### 无执行适配器的持久拒绝

2026-10-03，Main 首批合入 Core `reject_pending_approval` 与 Session
`deny_pending`。该路径核验完整历史 scope 和待审批身份，持久记录拒绝及
Session 失败，不创建 Provider/Tool adapter，不把拒绝替换为 Task 取消。
原有 Accept 的权限核验和执行路径保持不变。重复拒绝在已终态 Session
明确报冲突，账本不增写；这不是成功的再次执行。

冻结清单 `/private/tmp/kolyan-agent-host-skills-freeze-v1.json` 的 40 个
文件摘要均实读匹配。本批只取 Core/Server 的精确 hunks 和新增七行
scope/审批身份数据测试，不覆盖 Main 的 Task 持久预算实现。
联合模块门禁 `/tmp/kolyan-pending-denial-main-module-v1.log` 退出 0：
Core 108、Server 127 passed，均零失败。

额外新增的独立服务数据行验证：真实 SQLite、文件 Session、Runtime 和
受控模型先产生审批，销毁服务并等到原截止时间之后，再重建并持久拒绝。
完整八行预算服务矩阵通过，日志
`/tmp/kolyan-pending-denial-main-budget-service-v1.log`；实际轨迹在
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-task-budget-service-AgRo5t/actual.jsonl`。
拒绝后原 reservation 与 cutoff 不变，模型请求仍仅一份，没有文件效果，
Session 与对应 attempt 实际为 Failed。所有原七行预期保留。

本记录是 Core/Server 和持久预算的离线联合验证，不是生产 AgentHost、
Agent 根/子拒绝或 MiniMax 验收。那些消费者仍待分批集成与 Main 门禁。

### Agent 历史拒绝消费者

随后 Main 精确合入根/子拒绝、历史 owner/input/admission 核验和独立数据
测试；当前 Accept 仍走当前权限、Skills 与来源重验，不使用历史权限执行。
根拒绝只有核验对应持久 ApprovalRejected 后才允许历史收尾；子拒绝返回
已核验的物理失败，不隐式消费结果、恢复父调用或打开下一个模型。

`/tmp/kolyan-agent-denial-main-module-v1.log` 退出 0：Agent 109 passed、
零失败。新四行根场景和九行子场景保留实际请求/事件，覆盖重建、撤权、
Provider factory 不可用、错误 owner/checkpoint/scope，以及 RootOnly 与
AllInvocations 取消策略的区别。拒绝不增加模型请求、不产生工具效果；
所有旧测试断言保持。这个消费者门禁不替代生产 Host 或真实模型验收。

### 生产宿主库装配

Main 随后合入 `kolyan-agent-host`。宿主复用真实 Runner、Task/Session/
Runtime、SQLite、Required ArtifactStore、原生四工具与独立 Goal checker，
提供 start/query/cancel/审批决定；不复制模型 loop、不增加自动纠错。
Provider 由精确配置选择，密钥仅在执行 factory 中从环境读取。query、
cancel、Deny 与纯库装配不为此创建 HTTP client。

`AgentHost::open_with_skills(config, HostSkillsConfig { namespace, limits, policy })`
使用同一 `facts.sqlite` 与 Required 内容仓，`skill_catalog()` 只提供可信
管理入口。Runner.with_skills 在 Arc 和 child verifier attach 前接入。
重建显式传入当前 ACL，历史 provenance 不成为当前执行许可；未配置 Skills
明确不装配这一能力。HostProvider 转发真实 SDK opaque prepared plan，
不重新映射或混用两协议的计量结果。

Main 库门禁 `/tmp/kolyan-agent-host-main-module-v1.log` 退出 0，三项
测试通过；其中共享 Skills 仓重建的四行 JSONL 已关闭并物理回读，未产生
独立 skills 存储。这仅证明基础装配及仓重建，实际 localhost/native 宿主
场景、Main 完整联合门和唯一 MiniMax Skills 矩阵仍待下一批验证。
