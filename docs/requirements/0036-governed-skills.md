# 可治理 Skills 的选择性加载

## 目标与实施状态

本需求落实阶段计划 E3：模型先看到有界元数据，按精确版本调用 skill.load，
正文作为真实 ToolResult 回填到现有 Turn 的下一 Step。Skills 提供任务知识，
不能授予权限、执行脚本或创建另一套 Agent loop。A 批基础能力已集成并通过
主干模块验证，B 批执行接线与独立数据门已接入主干；C 批 Host 场景与真实矩阵
已集成，但仍有真实行未达标，因此完整能力尚未验收，不把目录
类型或局部存储测试当作完整 Skills 能力。

## 数据与存储边界

SkillKey 由 id 和 revision 组成，不支持 latest。SkillDescriptorInput 包含
精确 key、标题与描述；注册绑定 metadata 与 UTF-8 body 的摘要、字节数和
Required ArtifactRef。复用真实 FactJournal CAS 和 ArtifactStore，不另造
数据库接口。宿主 namespace 必须稳定显式配置，不扫描全局环境目录，不
下载模型指定的路径或 URL。同 key 同内容幂等，同 key 异内容冲突；描述或
正文变化必须新 revision。

SkillCatalog::register 返回真实注册事实与不可变内容引用；revoke 指定精确
key、注册 FactRef、operation_id 和有界 reason。注册、撤销、绑定采用独立
domain stream 的 schema1 critical facts，分别为 agent.skill.registered、
agent.skill.revoked、agent.skill.bound。绑定 causes 包含精确注册和实际
Agent ownership 事实；未知 schema、损坏因果链与外来 namespace 均拒绝。
事实和内容按重建后实际读取校验，不相信提交的可序列化证明。

初始硬上限为 catalog 128 个版本、一次广告 16 项、metadata 总计 64 KiB、
单正文 32 KiB、完整 ToolResult 512 KiB。64 KiB 限制每份完整广告、绑定及
事实 payload，catalog 总量由版本和事实数共同约束。配置只能收紧，不截断内容；错误
区分非法输入、容量、冲突、撤销、权限、来源、完整性和真实存储失败。

## 选择与执行契约

SkillAccessPolicy 是经过校验的宿主 ACL，按精确 AgentKey 和逻辑 Session
授权，可进一步限定 Task/invocation，默认不允许。SkillScope 精确绑定
logical_session_id、task_id、invocation_id、private_session_id 和
agent_snapshot_digest。子调用不继承父绑定，必须按自己的权限和 scope
确定集合。元数据本身也是不可信内容，长度受限，不覆盖系统指令。

discover 只读元数据，不读正文；广告作为真实工具库存的一部分，在 Root、
Child、Continuation 输入来源保存前组装。bind 绑定实际 ownership 与原
广告，不重选版本；restore_binding 精确恢复来源并检查 scope。广告和绑定
为 private-fields opaque 对象，不提供可反序列化的授权凭据。

skill.load 仅接受 skill_id、revision、content_digest 三个严格字段，不接受
路径、URL、命令或权限。新增独立 Capability::SkillRead，Effect::Read；
不借 FilesystemRead、ProcessExecute 或 AgentDelegate 授权。现有 Core policy
签发动态 grant，实际 Skill executor 验证 scope、snapshot、policy、grant
及当前 ACL，然后读取对应 Required artifact，返回精确正文和来源摘要。
正文只在实际执行选中 load 时读取，不递归加载 references，不执行脚本。

Runner 的技能路由与 environment/delegation 库存分别识别，不放宽未知工具
检查。Root、Child、Continuation 的来源契约保存必需 nullable Skill binding
字段，未配置写 null；迁移全部调用方，不添加缺字段兼容。所有 start、
resume、pump 路径必须共用装配，不仅正常 Root 有守卫。

## 修改和撤销

新输入只广告允许且未撤销版本。已保存来源不能静默换版本、删除广告或重写
正文。模型开启前及每次 load 执行前重新检查 ACL、注册和撤销；变化则明确
拒绝，恢复不重新选择。历史内容、ToolResult 和 receipt 保留，撤销不能
收回已发送内容或回滚效果。首版检查与模型 HTTP 发送不是原子事务，不声明
检查后撤销绝对阻止已进入操作；强开启准入属于需求 0034 的独立边界。

## 分批交付与所有权

A 批先完成 Agent skills catalog、不可变 metadata、ACL、绑定和重建读取，
实际使用 Journal/ArtifactStore，并补 Memory/SQLite 数据门。写集仅新增
skills.rs、skills 子树和 lib.rs 模块导出；不修改 Runner、Policy、Server 或
Provider。这只是基础切片，不宣称模型已能加载 Skill。

B 批在主控冻结具体 Runner 接线 API 后新增 SkillRead policy 能力、load
adapter、开启守卫、输入广告与三类来源验证，迁移调用方。与生产宿主、
上下文接线的交叉文件由主控串行协调，禁止整文件覆盖已集成能力。

C 批补实际 Runner 到下一 Step 的消费、真实本机工具和 MiniMax 两协议
场景。所有批次遵守统一源码布局、测试分文件与编译缓存规范。

## 执行接线冻结契约

B 批使用现有 AgentRunner，新增 with_skills(Arc<SkillRuntime>) 显式配置；
默认 None 不广告、不加载。独立 skills 执行模块提供严格 SkillLoadInput、
skill_load_manifest 和真正读取 adapter；不修改 EnvironmentToolFactory
库存定义，也不把 skill.load 加进四种 EnvironmentTool。RoutedTools 分别
识别 skill.load、agent.invoke 和环境工具，未知工具仍拒绝。SkillRead 是
Policy 的独立 capability，ACL 控制知识可见性，Core 的动态 grant 控制
这次实际读取，不能只检查工具名字或 schema。

RootInput、ChildInput 与 Continuation projection 的 Body 必须增加
skill_binding: Option<FactRef>。这是必需 nullable 字段：没有配置明确 null，
反序列化缺字段拒绝；禁止用 Option 的默认缺字段行为建立兼容分支。输入
来源 causes 追加精确 Skill binding fact，原 ownership、初始化和依赖证明
仍保留。先按实际 saved ownership discover/bind，再冻结真实工具库存和
输入来源；不能先冻结请求，再在发送前偷偷修改广告。

Root/Child/Continuation 各自绑定 SkillScope。初始装配和恢复装配共用按
saved binding、execution 和真实 input source 恢复的方法，不仅凭 snapshot
或 Session ID 推断。routed_tool_set 与 routed_provider 必须消费同一精确
绑定；新增广告守卫核对请求中 skill.load 的精确 schema，重新校验当前
ACL/撤销，但不得修改已冻结请求。空选择不广告 load。skill.load 的 schema
只允许保存集合中的精确 key/digest，并包含有界标题描述，不包含正文。

SkillRuntime 的读取方法只接受已验证 binding、精确 key/digest；先验证
当前来源，再通过实际 ArtifactStore 读取选中 Required 内容，校验 UTF-8
及摘要，返回正文和不可变来源。异步 Tool adapter 用 blocking worker 做
Journal/ArtifactStore I/O；prepare 不读正文。execute 校验真实执行坐标、
snapshot digest、当前 policy revision 和 issued grant，并再次检查 ACL。
完整序列化 ToolResult 同时满足 512 KiB 硬限制和 grant 输出限制，不能截断。
Future 被丢弃不承诺同步 I/O 回滚；读取无外部写效果，也不能继续开启模型。

本批迁移全部 start、resume、child driver、pump、Continuation 及测试
constructor，保留已有断言；新增数据集验证实际下一 Step 收到正文、精确
版本选择、foreign scope、未知工具、恶意正文、撤销后恢复、缺字段及边界。
生产宿主工作与 Runner resume 交叉文件由主控串行合并，不整文件覆盖。
Core、Runtime、Server 无需依赖具体 SkillCatalog；当前检查不宣称与 HTTP
开启原子。强开启准入继续按 0034 单独落实。

## 基础切片主干证据

A 批共 12 个源码/数据文件，逐文件冻结清单为
`/tmp/kolyan-skills-a-freeze-v1.json`，没有新增依赖或修改 Cargo.lock。
主干完整 Agent lib 实际运行 103 passed、0 failed、0 ignored，终态 exit0；
日志 `/tmp/kolyan-skills-a-main-module-v1.log`。主干 Agent all-targets 严格
Clippy exit0，日志 `/tmp/kolyan-skills-a-main-strict-v1.log`。

新增 37 个基础场景在 Memory/SQLite 各运行一次，另有两个强制同 head
CAS 场景各运行两个后端，共 78 行。实际观察先写入、flush/sync/关闭，
再物理回读全部比较。主干观察分别在
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-skills-a-FGs88s/actual.jsonl`
和 `kolyan-skills-cas-xgTv7W/actual.jsonl`（同一父目录）。覆盖恢复、撤销、
冲突、容量、损坏及 metadata 路径不读正文；尚不证明 ToolResult 回填、
动态 grant 或真实模型加载，后者必须由 B/C 实际执行证明。

相同冻结主干源码完整 workspace 回归终态 exit0，日志
`/tmp/kolyan-skills-a-main-workspace-v1.log`；统计为 82 个结果组，864 passed、
0 failed、66 ignored。包含本机长任务及原有整仓回归，显式 ignored 的
供应商网络用例未在这次运行，不能据此声称 B/C 或真实 Skills 矩阵通过。

## 正文读取切片主干证据

B 批首先接入实际 SkillLoadInput、只读正文与来源、SkillRead manifest 和
SkillExecutor，尚未接入 Runner 路由。现有真实 ArtifactStore、SQLite、Core
policy/prepared grant 校验完整 scope、snapshot、revision、当前 ACL、取消
及完整 ToolResult 输出限制；prepare 不读取正文，不借用环境工具权限。

主干 Agent 104、Policy 32 passed，0 failed、0 ignored，终态 exit0；日志
`/tmp/kolyan-skills-load-main-module-v1.log`。新增 13 行执行器数据先同步关闭
`kolyan-skills-adapter-QOS0zG/actual.jsonl`，物理回读后比较（macOS 临时目录）。
完整 workspace all-targets 严格 Clippy exit0，日志
`/tmp/kolyan-skills-load-main-strict-v1.log`；fmt 与 diff 检查通过。这批证明
实际正文读取和授权执行器，不证明 Runner 下一 Step 或真实供应商加载；
后者仍由接线与 C 批验证。

## Runner 接线与独立数据门证据

主干接线提交 `484b94d` 实现 Root、Child、Continuation 的精确来源绑定、
库存广告、实际读取路由及恢复开启守卫。未配置明确保存 null，缺字段拒绝；
历史恢复不重选 Skill，当前开启与读取重新检查 ACL 和撤销。已有测试只迁移
构造字段，原场景与断言保留；新增执行验证在独立测试文件和数据集中。

主干 Agent 107、Policy 32 passed，0 failed、0 ignored，终态 exit0；日志
`/tmp/kolyan-skills-b-main-module-v1.log`。其中 29 行执行数据包括 13 行读取
adapter、8 行 Runner 消费、6 行开启守卫及 2 行 Continuation。测试先输出
实际请求、ToolResult、来源和事实，再同步关闭 JSONL，物理回读后比较。
这证明真实 Runner/Core/存储路径，模型响应仍是显式 scripted fixture。

macOS 临时目录中的实际文件分别为 `kolyan-skills-adapter-ojpOpp/actual.jsonl`、
`kolyan-skills-consumer-EDs2YY/actual.jsonl`、
`kolyan-skills-continuation-slvvo5/actual.jsonl` 与
`kolyan-skill-opening-AmZqQC/actual.jsonl`（同一父目录）。
全部 workspace all-targets 严格 Clippy 终态 exit0，日志
`/tmp/kolyan-skills-b-main-strict-v1.log`；fmt 与差异检查通过。

本批不证明 MiniMax 的实际 Skill 加载、原生文件工具和 Goal 验证。C 批
仍须使用生产宿主跑两协议、具名及内联组合，并保留独立网络结果；确定性
撤销和恶意正文门不替代真实模型门。

相同主干源码执行原有 Agent 本机集成回归，终态 exit0：74 passed、
0 failed、22 ignored，58.32 秒；日志
`/tmp/kolyan-skills-b-main-agent-root-offline-v1.log`。此门包括真实本机工具、
审批重建及委派，但显式 ignored 的网络用例没有运行，不把旧场景回归
当作 C 批的新 Skill 网络验收。

相同冻结源码完整 workspace 回归终态 exit0：83 个结果组，876 passed、
0 failed、66 ignored，日志 `/tmp/kolyan-skills-b-main-workspace-v1.log`。
包含原有长任务、HTTP、审批及本机工具回归；ignored 的供应商网络门仍待
显式执行。源码布局检查通过，没有把新增测试实现放进生产文件。

## 验收

数据驱动覆盖幂等、冲突、重建、撤销、容量、未知 schema、损坏内容、外来
scope；未选正文零读取，选中仅对应读取，精确 UTF-8、摘要和输出边界。
真实执行必须展示 load 到下一 Step 的原文 ToolResult，再通过沙箱工具完成
明确文件后置条件。恶意正文不能扩大权限或委派，审批重建时变更拒绝且不
重放效果。完整实际观察先导出、关闭、物理回读再比较。

真实矩阵为 MiniMax 两协议与具名/内联组合，必须有实际 load、工具 receipt
及 Goal facts，不能用最终回答计通过；未发生预期交互明确失败。撤销竞态
和恶意内容先以确定性数据验证，真实模型行为另记。网络结果与离线证明
分开记录，密钥只在环境，项目本地私有配置与日志不提交。

## Production Host integration and actual model feedback

Main verified all nine C-source digests before import. The existing root target
only adds `mod skills`; the new framework, data and expectations remain separate.
No original input, expected result or assertion was weakened. The network runner
additionally persists its plan before execution and synchronizes/closes actual
JSONL after every row, retaining partial findings before the final comparison.

On 2026-10-03 the focused offline gate exited 0 in
`/tmp/kolyan-skills-c-main-offline-v1.log`: two tests passed, two entries ignored;
the helper subprocess separately executed all eight actual Host cases. Physical
readback at
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-skills-c-host-d0Qlg4/actual.jsonl`
confirmed complete requests, artifacts, receipts, facts and native outcomes.
Coverage includes both protocols, approval/rebuild/Accept, revoked or changed-ACL
Deny, malicious knowledge without shell/delegation authority, revoked load, and
revocation before the next generation. The latter retains two request intents
but one actual localhost HTTP opening; intent is not dispatch proof.

Main then ran the four actual MiniMax-M3 rows exactly once. The gate exited 101,
not success, in `/tmp/kolyan-skills-c-main-minimax-v1.log`. Actual trajectory:
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-skills-c-minimax-YlvrOu/actual.jsonl`.
OpenAI named/inline and Anthropic named each produced four Steps, exactly one
Skill load/write/read receipt, exact 20-byte file content, Satisfied and Completed.
Anthropic inline produced four Steps and all three receipts, but its actual write
argument included literal surrounding double quotes: 22 bytes instead of 20.
The checker retained Unsatisfied and the Task remained Waiting after a completed
physical FinalAnswer. This is an unmet model-generated task result, not a passing
row or evidence of protocol corruption. C's full live matrix remains unaccepted.

The private log records the actual protocol request bodies through the existing
diagnostic path, without credentials or authentication headers; these are distinct
from neutral ModelRequested facts in JSONL. The 22-byte receipt, subsequent read,
physical file and immutable goal predicate agree. No response repair, parameter
rewrite, expected-value change or scenario retry was used. Preserve this finding
for the separately specified bounded goal-correction increment 0038; it is not a
reason to block unrelated functional development or require self-modification.

The focused Skills strict gate exited 0 in
`/tmp/kolyan-skills-c-main-strict-v1.log`. The joint frozen Skills/approval-consumer
source passed workspace all-target strict Clippy in
`/tmp/kolyan-skills-budget-main-strict-v1.log`, then the full workspace regression
exited 0 in `/tmp/kolyan-skills-budget-main-workspace-v1.log`: 90 result groups,
900 passed, zero failed and 69 ignored, including explicit helper-subprocess
results. Formatting, source-layout and diff checks exited 0. That offline result
does not erase the separately executed 3/4 actual MiniMax outcome.
