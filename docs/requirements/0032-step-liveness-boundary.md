# Requirement 0032：独立 Step 的取消与截止时间边界

## 状态与定位

状态：主控已审查并集成；独立模块与受影响跨层回归通过，真实模型回归待终态。

基线：`304309523a7b1fc6dee58fdd9004cb79eec48fe1`。
本需求是 [0031 Agent 演进计划](0031-agent-evolution-program.md) 中 E1 的独立支持切片，
不是目标验收、生产 Agent 宿主、上下文工程或整个 Agent 闭环的替代。

本需求细化并升级 [0003 Step 流与执行控制](0003-step-stream-control.md)：
取消与 Step deadline 必须覆盖 Provider 开启阶段，不再保留该阶段仅由 HTTP timeout
负责的例外。当前 Core 已使用 Tokio；实现复用已有依赖，不增加第二种异步执行器。

## 已核实的源码问题

以下是基线源码事实，不是已经执行的复现结果：

| 位置 | 实际行为 | 独立调用的缺口 |
| --- | --- | --- |
| `step.rs::StepExecutor::start_with_control` | 直接等待 `provider.stream()` | 开启阶段无 Step 取消或 deadline 竞争；预取消、预过期仍可能调用 Provider |
| `step.rs::ControlledStepStream::poll_next` | 轮询时检查 `Instant::now()` | Provider 返回 Pending 且不唤醒时，deadline 自身不能唤醒消费者 |
| `step.rs::StepControlState` | 一个 `AtomicWaker` | 多个执行共享控制时，后注册的消费者可覆盖先前的唤醒 |
| `step.rs::ControlledStepStream` | 终止后仍持有底层流 | 调用方保留已终止的句柄时，底层资源不能及时释放 |
| `step.rs::RecordingStepStream` | Recorder 失败后仅标记 terminal | 同样继续持有底层流 |
| `step.rs::ControlledStepStream` | Completed 后等待 EOF 前继续检查控制 | 可产生 Completed 后第二个 Cancelled/TimedOut 生命周期事件 |

`turn/engine.rs` 已用独立取消竞争与 `timeout_at` 包装整个 Step 调用。
该保护保持不变；不能用 Turn 测试通过替代独立 Step 的验收。
既有 Step 测试覆盖流顺序、已过期 deadline、取消、验证与 Recorder 失败，
但没有证明开启阶段和永久 Pending 流的活性。

## 职责与非目标

- Step 仍代表一次 Provider 调用，不执行工具，不循环调用模型。
- 只管理当前调用的本地 future、stream、控制等待和 timer。
- 不新增重试、后台任务、持久控制、Ledger、Session 或 Provider 协议逻辑。
- 不修改 Turn 的预算、取消优先级、checkpoint 或错误映射。
- 不改变请求内容、Provider 事件顺序、模型结果分类和验证器职责。
- 不新增生产日志文件；测试证据仅由测试代码写临时目录。

## 对外契约

保留 `StepRequest`、`StepExecutionOptions::deadline: Option<Instant>`、
`StepExecution`、`StepEvent`、`StepError` 和已有 Executor 方法签名。
`StepControl` 继续是可克隆、幂等取消的进程内控制，不是权限或恢复凭据。

### 一个绝对截止时间

同一个 deadline 覆盖开启、流消费、Completed 后的 EOF 验证，不在阶段切换时重置。
到期的停止不依赖 Provider 产生新事件或自行唤醒。
配置 deadline 的执行要求有启用时间驱动的 Tokio runtime；不承诺在阻塞线程、
不返回的同步回调或运行时不再调度时仍能硬实时停止。
无 deadline 不引入隐藏默认期限。

### 开启阶段

1. 先执行已有请求校验；空 `step_id` 仍返回 `InvalidRequest`。
2. 在构造/调用 Provider 前检查已取消与已过期；停止时 Provider 调用数为零。
3. 未停止时最多调用一次 `ModelProvider::stream`，其 future 同时受控制和 timer 约束。
4. 开启成功移交流时继续使用同一个 deadline，不重新计时。
5. 取消或到期返回可消费的 `StepExecution`，事件为
   `Started → Cancelled/TimedOut → EOF`；`execute*` 聚合为对应 `StepError`。
   保留现有“已过期请求 start 成功、聚合返回 TimedOut”的场景与断言。
6. Provider 开启错误仍原样返回 `StepError::Provider`，不伪造 Started 或重试。

`Started` 表示本地 Step 生命周期开始，不是网络已发送、HTTP 已接受或模型已开始执行的证据。
开启中取消后返回的生命周期流不会继续持有被停止的开启 future。

### 消费阶段与 EOF 验证

| 状态 | 控制/Provider 结果 | 输出及资源处理 |
| --- | --- | --- |
| Started 尚未输出 | 已停止 | 先输出一次 Started，再输出固定的停止原因 |
| 尚未 Completed | 取消或 deadline 到期 | 一次 Cancelled/TimedOut，立即释放底层流，随后 EOF |
| 正常事件消费 | Provider 事件 | 保持原始顺序与内容，沿用既有映射 |
| 收到 Completed | Validator 接受 | 输出一次 Completed，继续验证 Provider EOF |
| Completed 后等待 EOF | 取消或 deadline 到期 | 返回 `Err(Cancelled/TimedOut)`，释放底层流；不得再输出生命周期终止事件 |
| Completed 后 | 正常 EOF | 释放底层流，结束；聚合器此时才确认成功 |
| Completed 后 | 额外事件或 Provider 错误 | 原有 Protocol 错误，释放底层流并结束 |
| 任意活动状态 | Validator/Recorder/Provider 错误 | 保留类型与原因，释放被持有的底层执行并结束 |

Completed 是最后一个业务事件，不意味着 EOF 已经验证。
后续 EOF 验证失败通过错误表达，不产生第二个 Completed、Cancelled 或 TimedOut。
聚合器不得提前把尚未验证 EOF 的 Completed 当作执行成功。

在同一次轮询中同时观察到取消与 deadline 到期时，取消优先；二者均优先于新 Provider
事件。已输出终止或错误后不接受后来的控制更改，不改变原停止原因。
Started/终止事件仍经过现有 Recorder；Recorder 拒绝时显式返回 Recording 错误，
不能假装已经成功记录停止事实。

### 控制与释放

- 取消状态持久保存在共享控制对象中，注册前发生的取消不能丢失。
- 多个执行共享同一 StepControl 时，取消必须唤醒全部等待者；不存在单消费者假设。
- 开启 future 被停止或丢弃、流被丢弃、终止事件/错误被返回时，释放相应底层资源。
- 不因持有 StepControl 克隆而继续持有 Provider 流。
- 单独丢弃某个控制克隆不取消其他持有者；不使用引用计数猜测调用方意图。
- 本地 Drop 不证明远端模型已停止、网络计费已停止或 Provider 内部自建任务已结束。
- 同步 Validator/Recorder 的不可抢占限制保持明确；不得创建后台任务来隐藏该限制。

## 最小实现结构

- `step.rs` 保留公共类型与主执行入口，统一开启/消费阶段的停止逻辑。
- Step 控制内部可移入 `step/control.rs`，复用广播通知和注册后再次检查状态，
  保留 `StepControl` 的现有对外名称与导出路径，不改 TurnControl。
- 流持有可释放的底层对象，并显式注册 deadline timer；terminal/error 路径先释放资源。
- Completed 后状态单独处理，防止二次生命周期终止。
- 不修改 Cargo、公共根导出、Turn、Provider 或其他模块。
- 生产源码英文注释；测试实现与场景数据独立文件，遵循
  [代码规范](../architecture/code-conventions.md)。

## 复现与新增验收

实现前先运行新增故障复现，保留原基线失败和观察记录。
Provider 替身只在测试中提供显式开启门、永久 Pending、完成后 Pending、事件脚本和
Drop 标记；不请求真实模型、不制造工具效果。
取消同步依赖“已实际轮询”的通知，不用任意 sleep 猜测执行阶段。
deadline 测试让 Provider 不产生唤醒，外层独立 watchdog 仅防止测试无限挂起，
不能代替被测 Step 的 timer 或作为通过依据。

| Case ID | 设置 | 必须比较的证据 |
| --- | --- | --- |
| pre_cancel | 调用前取消 | 调用数 0；Started/Cancelled/EOF；聚合 Cancelled |
| pre_deadline | 调用前已过期 | 调用数 0；Started/TimedOut/EOF；聚合 TimedOut |
| opening_cancel | 开启 future 已 Pending 后取消 | 开启次数 1；future Drop；唯一停止事件 |
| opening_deadline | 开启 future 永久 Pending | deadline 自行结束；future Drop；无重试 |
| pending_cancel | 成功开启后流永久 Pending | 实际进入 Pending 后取消；流 Drop；Cancelled/EOF |
| pending_deadline | 成功开启后流不产生任何唤醒 | Step timer 完成等待；TimedOut；流 Drop |
| shared_cancel | 两执行共享控制且都已 Pending | 两等待者均结束；各自底层资源释放 |
| registered_cancel | 注册前或注册期间取消 | 无信号丢失；重复取消幂等 |
| completed_pending_cancel | Completed 后无 EOF | 只有一个 Completed；随后 Cancelled 错误；无第二终止事件 |
| completed_pending_deadline | Completed 后无 EOF | deadline 有界失败；TimedOut 错误；不能聚合成功 |
| cancelled_and_expired | 同时可观察两种停止 | 取消优先；终止原因不可后续改变 |
| retained_terminal | 保留已终止 stream 对象 | 不等句柄 Drop 即已释放 Provider 流 |
| recorder_rejects | Recorder 拒绝实际事件 | Recording 错误；立即释放；不继续消费 |
| validator_rejects | Validator 拒绝实际 Completed | Validation 错误；立即释放；无 Completed |
| caller_drop | 开启等待或流消费时丢弃 | 对应资源 Drop；无后台续跑 |
| normal_and_errors | 正常完成、原始 Provider 错误、终止后事件 | 保持已有分类、顺序和协议拒绝语义 |

场景参数、预期序列及计数放独立数据文件；测试框架保存实际调用次数、轮询阶段、
事件/错误、资源释放状态和 watchdog 状态。
全部场景先观察并导出实际行，再集中比较，不能首个失败中断剩余证据导出。
已有测试函数、场景和断言保持不变，只添加必要测试模块声明。

## 门禁与验收记录

释放后依次执行：基线故障复现、实现后的新增 Step/control 测试、Core 全部单测、
Core all-targets 严格 clippy、格式与源码布局检查。
Turn 既有取消/deadline 测试属于受影响回归，不修改其生产逻辑或断言。
主代理统一协调跨模块与工作区验收，本切片不另启真实模型矩阵。

运行记录应追加明确的源码版本、实际命令、退出码、测试范围、日志和未解决限制，
保留失败复现，不用后续成功覆盖它。

### 2026-10-02 独立工作树实现与模块门禁

工作树：`/tmp/kolyan-evolution-worktrees.3FHbOB/step-liveness`，
detached 基线 `304309523a7b1fc6dee58fdd9004cb79eec48fe1` 加本需求未提交差异。
全部 Cargo 门禁使用独立 `CARGO_TARGET_DIR=/private/tmp/kolyan-step-liveness-target`，
使用仓库固定工具链，未修改 Main、Turn、Provider、Cargo 或已有测试断言。

| 实际命令 | 退出码 | 实际范围 | 日志 |
| --- | --- | --- | --- |
| `cargo test --offline --locked -p kolyan-core standalone_liveness_fault_matrix -- --nocapture`（生产修改前） | 101 | 1 测试失败；全部 20 行已导出，17 行不满足新契约、3 行满足 | `/tmp/kolyan-step-liveness-baseline-v1.log` |
| 同上（生产修改后） | 0 | 1 测试通过，原 20 行均满足契约 | `/tmp/kolyan-step-liveness-fixed-v1.log` |
| `cargo test --offline --locked -p kolyan-core step:: -- --nocapture` | 0 | 12 测试通过；新增 3 函数共 32 行，另含原 9 测试 | `/tmp/kolyan-step-liveness-module-v2.log` |
| `cargo test --offline --locked -p kolyan-core --lib -- --nocapture` | 0 | 101 测试通过，0 failed/ignored/filtered；含原 Turn 回归 | `/tmp/kolyan-step-liveness-core-full-v1.log` |
| `cargo clippy --offline --locked -p kolyan-core --all-targets -- -D warnings` | 0 | Core all-targets 严格检查 | `/tmp/kolyan-step-liveness-clippy-v1.log` |
| `cargo fmt -p kolyan-core -- --check` | 0 | Core 格式 | 无输出 |
| `bash scripts/check-source-layout.sh` | 0 | 仓库源码布局 | 无输出 |

原故障轨迹保留在
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-step-liveness-matrix-74338-1790933897545009000-0.jsonl`。
新增轨迹的实际路径均由对应日志 `STEP_LIVENESS_EVIDENCE` 行记录；每个测试先完整
导出所属矩阵，再比较。广播测试在再次手动轮询前读取独立消费者的真实 wake 计数，
不靠手动轮询救活未被唤醒的消费者；资源计数在保留终止句柄时读取。

额外复现了基线忽略 Completed 后 Provider Started 的行为；新场景保留该真实观察，
实现按原“Completed 后事件必须拒绝”的契约返回 Protocol，不改旧场景。

状态仍为实施中、模块门禁通过，等待主代理审查、集成和工作区验收；
本切片未执行真实模型、提交或推送，不宣称整个 Agent 目标完成。
同步回调不可抢占、运行时必须继续调度、远端停止不可由 Drop 证明的限制保持不变。

### 主控集成验证

集成基线 Main `0f4f053` 加本需求及 0033 的已审查源码差异；由主控通过
apply_patch 集成，不依赖工作树测试回执作完成证明。未修改旧测试断言。

- `cargo test --offline --locked -p kolyan-core -p kolyan-agent -- --nocapture`
  退出 0：Core 101、Agent 97；日志 `/tmp/kolyan-evolution-integrated-core-agent-v1.log`。
- `cargo test --offline --locked -p kolyan-integration-tests --test agent_root
  --test core_step --test core_turn --test turn_contract --test durable_approval
  --test turn_resume -- --nocapture` 退出 0：69 passed、0 failed、32 ignored。
  日志 `/tmp/kolyan-evolution-cross-layer-v1.log`。ignored 网络测试没有被算作通过。
- Core/Agent all-targets 严格 Clippy 退出 0，日志
  `/tmp/kolyan-evolution-integrated-strict-v1.log`。

新 32 行故障/广播/聚合观察和完整内容回归在主控测试中再次导出，完整路径由上述
日志的 `STEP_LIVENESS_EVIDENCE` 记录。真实 MiniMax root/审批重建回归另有独立
日志 `/tmp/kolyan-evolution-minimax-root-approval-v1.log`；未取得终态前不计为通过。
这不是全工作区或整个 0031 验收。

### 2026-10-02 主审后的完整观察补充

主审确认前一版 Actual 仅保存语义事件名称，不能作为完整流内容证据。
保留其基线失败与成功轨迹，不补造旧运行的内容。本次仅补测试观察与断言，
生产实现、原两份场景数据、语义标签比较及原有测试断言保持不变。

测试独立 `liveness/capture.rs` 穷举 StepEvent 字段生成完整 JSON：Started 的身份、
文本/思考增量、工具开始/参数增量/完成、usage、Provider metadata、Completed 的完整
StepResult/ModelResponse 和 Cancelled/TimedOut。错误包含明确类型、完整显示原因、
因果链与 Provider kind/phase/message/provider/status/diagnostics，不用事件名称替代内容。
仅记录被实际观察到的事件，不为 Recorder 拒绝或未返回的事件合成流内容。

每行保存输入模型请求；测试 Provider 在真实 `stream(request)` 入口另行捕获收到的请求。
预取消/预过期 Provider 请求列表为空，与请求意图明确区分。广播保存两执行各自请求与
完整流；聚合入口保存实际完整 result/error，不声称仅凭聚合值覆盖所有中间事件。

沿用原 20 + 4 + 8 行比较；额外导出一行经过真实 Step 映射的内容回归，
复用已有 DeltaProvider，断言原始文本、思考、usage 与完整 Completed 可还原。
33 行观察均先持久化，再执行对应比较；没有增加测试函数或修改旧场景。

| 实际命令 | 退出码 | 范围 | 日志 |
| --- | --- | --- | --- |
| `cargo test --offline --locked -p kolyan-core step::tests::liveness -- --nocapture` | 0 | 3 passed、0 failed/ignored、98 filtered；32 原行 + 1 内容回归 | `/tmp/kolyan-step-liveness-full-content-v2.log` |
| `cargo clippy --offline --locked -p kolyan-core --all-targets -- -D warnings` | 0 | Core all-targets | `/tmp/kolyan-step-liveness-full-content-clippy-v1.log` |

四份完整内容证据由 v2 日志的 `STEP_LIVENESS_EVIDENCE` 行定位。
此补充不把旧标签轨迹升级为完整内容验收，也不代替主代理的集成与工作区门禁。
