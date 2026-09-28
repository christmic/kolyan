# 0025 架构整改与工程可读性

状态：R0–R5 本轮代码整改已落地；真实验收未全部通过，详见末尾结果；不能宣称 R1–R5 全部验收完成。更新：2026-09-28。

本文件记录当前代码审查后的整改范围、阶段与验收标准，不代表问题已修复。
工程结构和注释规则的唯一来源是 [代码规范](../architecture/code-conventions.md)。

## 目标与边界

### 2026-09-25 收尾实施顺序与约束

1. R2：新增 Core 的可失败事件记录端口，由 Runtime 注入逐事件 Ledger 写入；
   挂起和失败不丢已产生事实，恢复沿用账本游标。Trace 仅投影，不参与执行判定。
   随后统一实际工具调用的 Effect 收据、原子取消准入和崩溃后的不确定状态。
2. R3：冻结 Turn 输入边界，版本化提交 Session，拒绝并发输入覆盖挂起执行；
   完成提交可重放，取消不降为失败。
3. R4：服务进程装配真实执行链路，验证 RPC 启动、审批重启恢复、取消及游标续读。
4. R5：策略注入无进展判定，区分重复写、允许轮询和结果变化；保留完整请求证据。
5. 最后运行离线与真实矩阵，逐行报告通过、失败、跳过和未运行。
   R1 已观测的厂商结构化输出契约失败不通过删配置、放宽断言或隐式重试掩盖。

本清单是实施计划，不是完成声明；每批修改需附新增回归与运行证据。

R5 具体契约：策略提供重复阈值和显式轮询工具集合；只允许只读工具豁免。
判定使用名称、JSON 值相等的参数、完整工具结果及错误标志，不比较模型 call id。
连续相同调用产生相同结果达到阈值后，下一次相同调用在工具准入前以
`NoProgress` 结束。不同调用或结果变化重置连续计数；已有检查点中的消息历史
用于重建计数，不引入依赖进程内存的治理状态。MaxSteps 仍是兜底。

Session 投影明确区分 `ConversationOnly`（用户与最终回答）和 `FullTrajectory`
（包含工具调用、结果及签名思考）。服务进程选择后者；库的精简会话入口保留前者，
原始执行事实始终保存在 Ledger。每个 Turn 固定投影选择、版本及输入边界，
恢复不受后来装配时的投影选择影响。现有摘要投影测试保留，新增服务进程测试
验证完整上下文确实进入下一 Turn 的真实 HTTP 请求。

事件记录失败必须阻止继续执行；Trace 是可重复投影的观测，不参与效果重放判定。
工具收据及结果同次落账；通用 Effect Runtime 与真实 Tool adapter 使用相同
EffectRequest / Grant / Receipt 契约，进程崩溃测试交叉验证两条入口的恢复结果。

- 保留 Session → Turn → Step、Model / Provider / Protocol 分层。
- Core 不依赖数据库、具体模型、UI；Runtime 负责持久执行，Server 负责协调。
- `crates/kolyan-server` 为核心库；`services/kolyan-server` 为服务进程。
- TUI、Desktop、交互式 Agent 客户端统一通过 Server 通信。
- 一次性 CLI 在本进程装配执行能力，不要求启动 Server；复用核心实现，不复制循环逻辑。
- 当前只要求单进程协调，不引入分布式租约。
- 生产代码与测试代码必须分文件；模块单测仍归属于被测模块。

## 审查证据与未确定事项

| 编号 | 当前实现/证据 | 风险及要求 |
| --- | --- | --- |
| A01 | 两套 `sse.rs` 对每个分片使用 `from_utf8_lossy`，只按 `\n\n` 分帧 | 跨分片 UTF-8 和 CRLF 解析不完整 |
| A02 | OpenAI EOF 自动合成 Completed；工具参数解析失败退化为 `{}` | 残缺响应可能被当作可执行结果 |
| A03 | 文件工具只做词法路径检查，再拼接 root | 软链接可使实际访问超出工作区 |
| A04 | EffectStarted 与外部执行、结果落账分开；Turn Driver 未贯通 Effect 收据链路 | 崩溃后无法安全判断副作用是否执行 |
| A05 | 挂起/恢复使用固定事件后缀；Turn 完整事件主要在结束后持久化 | 多审批转换、挂起轨迹和崩溃证据不足 |
| A06 | 边界取消检查把账本读错误转换成 None | 无法确认控制状态时仍可能继续执行 |
| A07 | Session 恢复按当前 messages 长度截取 checkpoint；所有错误提交为 Failed | 输入边界漂移、取消被覆盖、重复提交风险 |
| A08 | RPC start 只做 admission；客户端入口基本为空 | 尚未形成经过服务进程的模型执行闭环 |
| A09 | 测试在矩阵首个失败处中断；轨迹主要做有序子序列匹配 | 未执行项和额外重复行为容易被遗漏 |

上述为静态审查发现，实施前需用最小确定性回归确认触发路径。
Qwen 重复调用的根因仍未确定：已有转储证明部分请求包含工具结果，不能据此
排除响应映射、兼容接口和提示词交互问题。MiniMax 结构化输出为空及 GLM
响应体读取失败也需保留原始证据；一次重跑通过不能证明原因或覆盖原失败。

## R0：代码组织与测试证据基线

- 按代码规范迁移 inline tests 到模块所属独立文件；保持测试名、断言、fixture 语义。
- 按职责拆分 Server、Provider、Storage 的大文件；不以行数机械拆分。
- 每个配置模型 × 协议 × 场景列出 Passed / Failed / Skipped / NotRun。
- 单行失败后继续收集独立场景；最终有失败必须返回非零。目标间使用
  `--no-fail-fast`，目标内部的矩阵 runner 也必须收集失败，不能仅依赖 Cargo 参数。
- 记录提交号、配置摘要、过滤器、尝试次数、耗时和日志路径。完整矩阵缺少凭据
  或存在过滤器时不得宣称全面通过；定向诊断可以过滤，但必须标明范围。
- 请求/响应日志按 execution、step、协议、attempt 关联；由测试代码写入临时目录，
  排除鉴权头、密钥和敏感字段。只截取部分内容时明确标注，不宣称是完整请求。
- 轨迹契约显式约束顺序、次数、工具参数/结果关联、禁止事件和终态。
  模型文本可做语义断言，但不能省略实际事件内容；重复工具调用不能被无条件忽略。

验收：迁移前后测试清单一致；普通测试通过；故意制造一个矩阵失败仍执行后续项，
并给出失败退出码。历史失败与复测结果分别记录。新增 runner 以确定性输入自测。

## R1：协议完整性与工具访问边界（优先）

- SSE 先缓存字节，再解码完整事件；支持 LF/CRLF、多行 data、注释、EOF 残帧，
  限定事件和缓冲区大小；两套协议共享规则，避免复制维护。
- 区分传输失败、SSE/JSON 解析失败、服务端错误事件、响应不完整和合法终态。
  缺少终态不能默认成功；厂商兼容必须有明确配置、完成证据和专属回归。
- 未完成或非法工具参数报错；不能用空对象替换后继续执行。
- 结构化输出须遵循请求格式与 schema；兼容提取策略应显式记录。
- 保留底层错误链、状态和编码等诊断；原始请求日志默认关闭，不能泄露凭据。
- 重试由明确层次负责，有次数/时间预算；流已经产生可见事件后不能透明重放，
  工具副作用更不能通过重试整个 Turn 处理。
- 文件读写验证实际目标及父目录。定义软链接和路径替换竞态的处理方式，
  不能把简单 canonicalize 检查宣称为无竞态沙箱。

验收：每个字节边界分片的中文、CRLF、截断、错误后 EOF、残缺工具参数、
结构化输出失败均有无网络回归；软链接逃逸在临时目录验证且无越界副作用。
再跑两套 Provider 和 Step 的全部适用真实矩阵，记录完整覆盖。

## R2：统一 Runtime 持久执行与恢复

- 明确 `ExecutionRuntime` 与 `DurableTurnDriver` 的唯一执行路径，消除平行状态规则；
  真实工具调用接入 Effect 准入、收据和恢复决策。
- 已完成且身份/输入匹配的 Effect 返回既有结果；只有 Started 的 Effect 进入
  Uncertain，按工具幂等契约或外部查询结果处理，禁止默认重放非幂等操作。
- 幂等键重复时校验输入和语义一致，冲突显式失败；记录原子追加/查询边界。
- 每次挂起、批准、恢复使用唯一转换标识；一个审批决策不能被重复消费。
- 逐边界持久化执行事实，挂起、失败均保留已发生事件；恢复后事件序号不冲突。
- 账本读取失败阻止新副作用；终态不能被后到事件覆盖。取消请求与实际停止
  分开表达，明确 Step/Tool 边界响应取消和可选运行中中断的责任。
- Ledger 是事实来源；Trajectory 为执行事实视图；Trace 为观测。
  Trace 写入失败不能把已完成副作用转换成可重试执行。
- 存储明确原子性、刷盘和损坏记录恢复保证；单进程并发也需要原子状态转换。

验收：副作用前、执行后入账前、审批挂起/恢复和 Trace 写入处注入失败；
证明重复审批不重复执行、Uncertain 不自动重放、取消不被覆盖。
分别标明实例重建测试与真实子进程终止/重启测试；真实审批矩阵必须覆盖多轮挂起。

## R3：Session 上下文与提交一致性

- 固定 Turn 的原始输入、Session 版本与上下文边界；禁止按恢复时消息数量猜切片。
- 定义同 Session 的并发准入策略；初期允许显式串行，版本冲突必须可见。
- 完成提交按 execution/turn 幂等；Ledger 与 Session 部分提交失败需要可恢复对账。
- 区分 Completed、Cancelled、Failed、Suspended 等结果，不将取消一律改成 Failed。
- 原始会话记录和模型上下文投影分离；工具历史保留/摘要策略显式定义，
  不能将所有工具结果一概视为污染，也不能把中间消息当作本轮用户输入。

验收：审批前已有工具结果、挂起期间新输入、重复提交、并发取消、版本冲突、
进程重建均有数据驱动用例；真实两轮 Session 验证上下文关联和终态。

## R4：Server 进程闭环与客户端边界

- RPC 的 admission 与真实执行明确区分；进程装配 Provider、Tool、Runtime、Session。
- 提供任务启动、状态查询、审批提交、取消、事件订阅和按 cursor 续读。
- 维护单进程任务所有权、异常收尾和关闭流程；校验 session/turn/execution 关联。
- 客户端依赖通信契约；一次性 CLI 直接复用本地执行能力。
- 跨进程契约落在 protocols/schemas，包含版本、错误、序号和重复请求语义。

验收：启动真实 Server 子进程，通过传输发起任务、获得模型流、执行工具、
挂起审批、重启进程、批准恢复并读取结果；补取消、异常断开和事件续读。
直接调用 Rust Service 的测试不能替代此验收；客户端目前是占位时明确说明。

## R5：无进展治理（在证据链建立后）

- 先用完整请求/响应回放定位重复调用；不把增大 max_steps 当作修复。
- 检测策略考虑工具名、规范化参数、结果、任务进展与工具属性。
  轮询和允许重复的读取不能因参数相同就被判错。
- 策略由配置/治理层提供，Turn 执行并输出明确的停止原因；max_steps 保留兜底。

验收：重复写入、合法轮询、相同参数但结果变化、无进展终止均有确定性测试；
真实模型重复场景保留原始轨迹及结果，不能用 runner 生成调用替代模型行为。

## 交付规则

实施顺序：R0 基线 → R1 → R2 → R3 → R4 → R5。组织迁移与行为修复分批提交。
每阶段记录需求编号、回归证据、真实矩阵范围及未完成项；后续阶段不能以
本阶段单测通过代替自己的验收。本文与旧文档冲突的边界以本文为准，实施时同步
更新对应旧规格，保留历史验证记录，不将旧结果改写为新版本的通过证据。

## R0 第一批：生产代码与测试分文件

- 11 处 inline tests 迁到模块所属独立文件；包含 Step、Turn、Model、Provider、
  Policy、Tools、Runtime、Ledger、Storage、Server。已有 engine/tests.rs 保留。
- 迁移前后 `cargo test --workspace --all-targets -- --list` 输出一致，共 119 个测试。
- 对比 Git 基线，生产代码除测试模块声明外不变；测试正文除格式化空白外不变。
- `scripts/check-source-layout.sh` 已接入统一 `scripts/check.sh`，检测生产文件中的
  inline tests 和常用 test 属性；这是轻量源码检查，不宣称覆盖全部 Rust 宏语法。
- 临时样例验证：混入测试实现退出码为 1；外置 tests.rs + mod tests 声明通过。
- 验证：`sh scripts/check.sh` 通过（结构、格式、Clippy、workspace 测试、构建）；
  `cargo test --offline --workspace --all-targets` 为 94 passed / 0 failed / 25 ignored。
  25 个真实网络/诊断用例未执行，不计为通过；本批仅迁移文件，无行为变更。
- 本批不改变执行语义；真实网络矩阵、按职责拆分大模块及 R1–R5 尚未完成。

## R1 第一批：共享 SSE 分帧

- 两套协议共用 `kolyan-protocol-sse`，按字节缓存完整行，严格校验 UTF-8，
  支持 LF/CRLF、多行 data、注释，每个事件最多 4 MiB。
- EOF 不补造分隔符；残帧报错。JSON 失败后丢弃同一分片中的后续事件；
  传输失败后停止读取，不再继续消费网络响应。
- 新增 9 项无网络回归：解码器 3 项，两套协议各 3 项；包括所有字节切分点、
  逐字节中文/emoji、非法编码、超限、截断和错误后事件隔离。
- 此批不代表 R1 完成：Provider 终态证据、工具参数完整性、schema 校验、
  工具路径边界及真实矩阵仍待后续验收；R2–R5 尚未实施。

## R1 第二批：终态、参数与访问边界（实施中）

- OpenAI 默认要求响应级终态；允许“completed text item + EOF”的兼容策略
  必须显式开启，不允许工具调用借此合成完成，不允许错误后再合成成功。
- 两套 Provider 拒绝残缺/非对象工具参数；OpenAI 不再静默丢弃 terminal 中
  的非法工具，未知 incomplete 原因报错。Anthropic 服务端 error 作为错误，
  缺失停止原因或未关闭工具的 message_stop 不算成功。
- 两套 Provider 共用中立 OutputValidator，在请求前编译 schema，成功终态
  校验结构化结果。禁用 schema HTTP/文件解析，不修补非法输出来伪造通过。
- 工具根目录在构造时打开；cap-std 相对句柄负责实际路径解析，避免先检查
  再按环境绝对路径打开的竞态。权限范围及非进程沙箱限制见工具 README。
- 真实测试已发现 MiniMax-M3 返回 schema 外的城市介绍，且 JSON 中包含未加
  引号的中文数值；这是实际响应证据，不归类为网络波动，不通过传输重试掩盖。
  此轮日志 `/tmp/kolyan-r1-live.log`；该运行启动于 schema 校验和工具修复之前，
  不作为这些后续改动的验收证据。完整矩阵仍待收尾。

## R1 矩阵与诊断记录

- 新增独立 `r1_matrix` 测试，不删除或放宽原有真实测试断言；使用已有四类
  fixture 和全部配置模型，覆盖 Step → Provider → Protocol。每行失败后继续，
  最终存在 Failed / NotRun 时失败退出；缺少密钥不再被算作成功。
- 测试框架启动时保存全部计划，逐行更新 report.json；请求、实际事件、配置、
  Git revision 由测试代码保存到独立临时目录，生产 Step/Turn 不写这些文件。
- 此次新增矩阵共 76 行：44 Passed、13 Failed、19 Skipped、0 NotRun；
  Skipped 均为配置能力不支持的 prompt_cache 场景，不计入通过数。
- 13 个失败均为 structured_output schema 不匹配：OpenAI 表面全部 10 个模型，
  Anthropic 表面的 MiniMax-M3、deepseek-v4-flash-0731、glm-5.3。
  未把失败转成跳过、未变更能力声明、未通过增大 token 或重试整轮掩盖。
- 证据目录：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-rWT9EY/`；
  日志：`/tmp/kolyan-r1-complete-matrix.log`。该运行包含 c780420 的 Provider/schema/工具修复；
  后续 HTTP 状态分类变更不在此运行中，应由新增无网络用例及下次矩阵验证。
- 两套 HTTP 客户端保留错误状态码和最多 4096 字节响应体，避免把 401/429/5xx
  误分类成传输错误；Provider 保留底层错误链。新增每协议两项无网络回归通过。
- 新矩阵框架的“首行失败后继续执行，未开始项保持 NotRun”确定性回归通过。
  旧全套真实测试仍独立运行，不能用此矩阵替代 Turn/Session/Server 全套验收。

## R2 第一批：失败关闭与终态保护

- 账本控制读取失败返回 BoundaryControl 错误；取消已提交时不能再准入成功终态。
  取消/超时落成对应账本终态，不再统一写 TurnFailed。
- Runtime/Coordinator 的重复事件 ID 必须绑定同样的 turn、事件类型和 payload。
- EffectPrepared 先校验输入再复用终态；Started 无终态时返回 Uncertain，禁止重放。
  EffectStarted 的唯一追加决定执行权；收据状态必须与实际 outcome 一致。
- 每个审批挂起/恢复使用独立标识；新增三次审批恢复回归。Coordinator 投影保留
  首个终态，防止取消被后来的完成事实覆盖。
- Trace 失败进入 Trajectory.trace_errors，账本已提交的成功执行不变成可重试失败。
- 模块回归：Runtime 8 项、Server 6 项通过；Runtime 边界集成 4 项通过。
  测试位于模块独立 tests.rs；本批新增，不删改旧验收断言。
- **R2 尚未完成**：实际 ToolExecutor 与 Effect 收据路径的统一、逐事件账本写入、
  挂起时的完整轨迹、原子取消/准入事务、真实子进程崩溃恢复仍需实施与验证。
  当前 Started 恢复测试为实例重建，不能声称已完成真实进程崩溃验收。

## 当前工程验证快照

- `5bd0389`：`scripts/check.sh` 全仓库格式、Clippy、测试、构建通过，
  130 passed / 0 failed / 26 ignored；日志 `/tmp/kolyan-r2-workspace-check.log`。
- 该计数的 ignored 是真实网络及诊断测试，不能计入通过数。
- 独立 R1 真实矩阵为 44 passed / 13 failed / 19 skipped，未达到全通过。
- 旧全套真实测试 `/tmp/kolyan-r1-live.log` 已观察到结构化输出失败和
  `qwen/qwen3.7-plus` 策略准入场景 Turn 超时；未把超时归因为模型或代码，
  尚需依据该场景请求/事件证据定位。该运行不是最终代码快照的完整验收。
- 仍需按 R2 → R3 → R4 → R5 完成余项，不能将此快照描述为整改全部完成。
- `e8d54c7`：补充 Turn 身份校验后再次运行统一检查通过，131 passed /
  0 failed / 26 ignored；日志 `/tmp/kolyan-hardening-final-check.log`。
- 旧结构化 fixture 的期望只写 `type: object`，不检查请求 schema 中的
  `name/province/coastal` 必填字段。因此旧 Provider 矩阵“通过”不等于符合
  结构化输出契约；新增 Provider 内 schema 校验暴露了这一验收缺口。

## R1 官方 SDK 对照收尾（2026-09-25）

详细需求、代码修复及可复现证据统一记录在
[0026 Provider 官方 SDK 对照整改](0026-provider-sdk-conformance.md)，不在这里重复维护。
已将厂商原生结构化契约失败与本地请求/流映射缺陷区分，未静默降级或放宽验证。
该工作不代表 R2–R5 完成；这些阶段仍按本文件各项验收要求推进。

## R2–R5 收尾实现（2026-09-25）

以下覆盖此前标记“尚未完成”的代码工作；前面的记录保留为历史快照。

| 范围 | 实现结果 | 验证边界 |
| --- | --- | --- |
| R2 逐事件事实 | Core 注入可失败 Recorder；Runtime 记录实际请求、完整 Step、工具结果；审批 attempt 持久唯一 claim | 记录失败阻止继续；Trace 不决定执行是否完成 |
| R2 工具收据 | 真实 ToolExecutor 使用共享 EffectRequest/Grant/Receipt；结果与收据同次追加 | Started 无收据为 Uncertain，不自动重复执行 |
| R2 原子准入 | 内存锁、文件 OS 锁、SQLite immediate 事务实现取消检查与准入原子化 | SQLite FULL 同步；文件日志损坏失败关闭，不宣称可自动修复尾部 |
| R3 会话一致性 | 输入/版本/投影边界冻结；独立文件句柄间加锁；提交 intent 和幂等对账 | 同 Session 串行；重复审批错误不覆盖原执行状态 |
| R3 上下文 | 会话展示消息与完整模型上下文分离；服务选择 FullTrajectory | 工具调用、结果和模型提供的签名思考保留；不补造未返回内容 |
| R4 服务闭环 | 实际进程装配 Provider/工具/Runtime/Session；RPC 执行、审批、取消、查询和 cursor 续读 | 本机可信 stdio，非 HTTP/WebSocket；查询耐久事实而非 UI token 推送 |
| R5 治理 | 策略提供重复阈值及只读轮询豁免；用调用参数和实际结果判断 | 当前 Turn 内、下一批工具准入前检查；不是任意循环自动证明器 |

新增测试未删除或降低原有断言。服务场景输入及事件精确次数、禁止事件见
`tests/fixtures/server_process.json`；崩溃位置与恢复结果见 `runtime_crash.json`
（均位于 integration-tests crate）。实际 JSONL 由测试写临时目录。

### 关键修复证据

真实 MiniMax 两轮会话曾因摘要投影遗漏上一轮工具结果而拒绝读文件；服务进程
现在显式选择完整投影，并验证第二轮实际请求含前一轮 ToolResult。
重复写场景曾遇到模型认为没有任务价值而不调用工具；保留该失败记录，新增输入
明确每次调用是独立耐久性采样，执行次数断言不变，不在代码中合成工具调用。
OpenAI 中间空工具参数及 incomplete 原因可选性的 SDK 对照见 0026，证据不重复维护。

### 确定性验收

- 全仓库统一检查（格式、Clippy、单元/集成测试、构建）：188 Passed / 0 Failed /
  31 Ignored。日志 `/tmp/kolyan-closure-final-check.log`；Ignored 不计入通过数。
- 服务进程离线 target：4 Passed / 0 Failed / 1 Ignored；其中一项为崩溃 worker
  入口，其余测试覆盖多场景。实际三个崩溃点、审批重启双 Turn、运行中取消均执行。
  日志 `/tmp/kolyan-closure-process-final.log`；主轨迹目录
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-process-offline-VKYJjR/`。
- 官方 SDK HTTP/SSE 差分 120/120，通过日志 `/tmp/kolyan-closure-sdk-transport.log`。
  已捕获真实响应回放 38/38，通过日志 `/tmp/kolyan-closure-sdk-replay-v2.log`。
  初次误选旧非流式捕获目录被测试明确拒绝；使用 capture_format=2 的完整流式证据
  重新验证，不修改断言。回放通过不意味着原生 schema 能力全部通过。

### 保留的明确边界

服务不是分布式系统，无分布式租约；外部副作用不承诺 exactly-once。
进程在任意位置被杀后不自动重新执行 claimed attempt；审批检查点可恢复，
不确定副作用需显式对账。当前事件接口是完整耐久事实的 cursor 查询，不是
逐 token UI 流。Session 分叉、上下文压缩、网络认证、UI 客户端不在本轮范围。
R5 在下一批准入边界治理，批次内部的重复调用仍受批次/工具预算约束。
真实模型的 schema 不合规不能通过删模型、伪造完成、静默重试或放宽断言闭合验收。

### 2026-09-26 复验与轨迹断言补强

环境重启后前一日临时证据已清理；上述路径是历史运行记录，不再作为当前可打开的
文件引用。今日结果另行保存在被 Git 忽略的 `target/acceptance-2026-09-26/`，
不包含凭据，不将测试生成数据提交仓库。

首轮服务矩阵 56 Passed / 1 Failed / 0 Skipped / 0 NotRun，
`server-initial/report.json` 保存原失败：MiniMax OpenAI 在一次写入后提前回答
“等待下一轮指示”，没有触发第三次重复调用。没有将此结果算作治理通过。
新输入明确要求本次任务内连续执行、不等待下一条用户消息；工具调用仍由模型产生。
曾尝试 required 前置条件，但确定性断言发现 Turn 既有语义只保留首次选择，后续
恢复 Auto；该尝试已撤回，未改变生产执行语义或提交该测试假设。

新增比较逐一核对 fixture 中的工具名、参数、实际输出、收据与真实模型调用的
call id 关联，以及同一 ToolResult 是否原样出现在下一条 ModelRequest 中。
这里的 ModelRequest 是调用 Provider 前的中立请求；实际 HTTP 编码另由协议
差分和回环服务捕获验证，不能将两者混称。失败提前退出时也导出 ledger.jsonl。
当前 trace matcher 不对模型自然语言做逐字比对，但不会省略它的实际内容。

用户随后确认了 tool_choice 的兼容选择要求，落在 0027 而不是协议解析器。
重复工具场景现在只声明首 Step 的 required 意图；Provider/Model 表选择 required、
auto 或字段省略。它不再假设后续 Step 也被强制 required，不修改 Turn 的收尾策略。
测试子进程显式开启实际 HTTP 请求体诊断，保存到 stderr.log，不含鉴权头；
生产默认仍关闭。厂商模型绑定、场景输入、预期结果和执行框架分别维护。

### 最终结果与未闭合项（09-28 归档）

全部证据放在忽略目录 `target/acceptance-2026-09-26/`，未提交生成数据或凭据。

| 检查 | 结果 | 证据 |
| --- | --- | --- |
| 最新统一检查 | 192 Passed / 0 Failed / 31 Ignored | check.log |
| tool_choice 真实兼容矩阵 | 95/95 Passed | tool-choice/report.json；规格 0027 |
| R1 Provider/Step 真实矩阵 | 44 Passed / 13 Failed / 19 Skipped / 0 NotRun | r1/report.json |
| Server 首轮及提示澄清复验 | 各 56 Passed / 1 Failed；失败为 MiniMax 重复写场景 | server-initial/、server-final/ |
| 加首步 required 意图后的最新 Server 矩阵 | 8 Passed / 49 Failed / 0 Skipped / 0 NotRun | server-latest/report.json |
| 原有 Turn/审批/权限/恢复真实目标 | 4 个测试函数通过、8 个失败；不是模型行数 | turn-live.log |

R1 的 13 个失败仍为结构化输出不满足请求 schema，19 个 Skipped 是原配置
声明不支持的缓存场景。最新 Server 矩阵首个失败是 MiniMax 在已发送 required 后
返回纯文本、拒绝重复写，未触发预期治理；其余为 1 行流式 TLS EOF、47 行握手失败。
该运行确实尝试完所有独立行，未把传输失败跳过或算作通过。
旧 Turn 目标的一些函数仍是首个模型失败即退出，因此不能把其后未执行模型算通过。
四个通过的函数分别验证事件流、文件读写、结束原因和多批次文件写入。

当前代码与确定性验收已交付，但全面真实验收没有闭合。后续需要分别处理
厂商原生 schema/required 契约不合规，以及 TLS 故障的端点/网络路径归因；
不能用重复改提示词、删模型、关闭校验或重试整个有副作用的 Turn 消除红项。

09-28 使用本地官方 openai-python 对同一 Qwen 接入做一次定向调用成功
（max_retries=0，证据 `sdk-connection/`），未复现长回归当时的 TLS 故障。
这不能证明 TLS 已修好，也不能据此认定是厂商或本地库的单方问题；保留原始
错误链和当时的失败结果，后续需要故障时刻的同路径连接证据。
