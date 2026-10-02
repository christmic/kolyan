# Context Engineering: Accounting and Governed Reduction

## 状态、目标与依赖

状态：2026-10-02 计数适配器及共享映射切片已审查释放；宿主选择与准入接入待实施。
基线：Main `3043095`。本需求是 [0031 演进计划](0031-agent-evolution-program.md)
的 E2 主线能力，不是旧 0030 错误修补，也不是仅增加 counter/reducer trait。
0030 保留已有执行、权限、来源和真实验收契约；本需求不覆盖其失败记录。

首批结果必须是生产宿主可启用的模型感知预算和历史缩减消费者：实际映射请求、
具体计数适配器、有界选择、准入前持久化和真实执行链一致性。
未知计数不能伪装为精确 token；缩减不能修改权限、事实、收据或已准入输入。

首批不实现模型摘要、长期知识记忆、Turn 内隐式 compaction、服务端自动截断、
模型调用重试或新的执行循环。后续摘要压缩有独立来源和提交契约，不冒称已完成。

## 已有实现与实际接入缺口

- [context.rs](../../crates/kolyan-agent/src/context.rs) 有模型窗口、输出 reserve、
  字节/消息/block 上限及 Strict/Inspect。`SerializedByteEstimator` 返回 Unknown；
  它不是生产模型 tokenizer，估计不构成 token 上界。
- [projection.rs](../../crates/kolyan-agent/src/context/projection.rs) 已实现完整消息
  范围选择、完整源 digest、原始工具配对、首个用户目标锚点和当前用户尾部保护。
  它验证宿主提供的选择方案，但不会自行选择历史或持久化方案。
- [provider.rs](../../crates/kolyan-agent/src/provider.rs) 每次实际 opening 检查并
  记录原请求；若准备改变请求则拒绝。不得在此 wrapper 内偷偷缩减。
- [Session 执行](../../crates/kolyan-server/src/session_execution.rs) 在保存输入前
  装配历史并调用 `TurnPreparationHook`；现有 hook 接受不可变请求，只能观察/
  绑定，不能作为请求替换器。接入缩减须审查并迁移明确的准备边界。
- [Provider OpenAI](../../crates/kolyan-provider-openai/src/lib.rs) 与
  [Provider Anthropic](../../crates/kolyan-provider-anthropic/src/lib.rs) 已拥有真实
  parameter planning 和 wire mapping；计数不得复制另一套近似映射。
- Agent 已有准入前 root/continuation 来源准备和完整源归档。新的选择要接入这些
  真实消费者，而不是让测试手写 Agent-private source body。

## 本地参考证据与不能移植的假设

本轮只读检查的本地路径是设计证据，不是 Kolyan 的依赖或验收结果：

| 来源 | 实际消费者 | 可借鉴与限制 |
| --- | --- | --- |
| `codex/codex-rs/core/src/context_manager/history.rs:516` | History 的 token 估计 | 明确称字节 heuristic 为粗略计数，不是 tokenizer 精确计数 |
| `codex/codex-rs/core/src/session/turn.rs:1454` | 根据能力调用本地/远端 auto compaction | 有真正消费链；不能仅移植接口或默认启用所有端点 |
| `grok-build/crates/codegen/xai-chat-state/src/actor/state.rs:103` | Shared compaction 的 EstimatedItemTokenCounter | 实际实现是 bytes/4；trait 文案中的 trusted 不证明模型精确性 |
| `deepseek-tui/crates/tui/src/compaction.rs:794` | 压力判定结合历史计费 usage 与估计 | 原请求 usage 不等于新请求的准确计数 |
| `deepseek-tui/crates/tui/src/compaction.rs:1326` | 压缩前归档完整历史，再保存生成 handoff | 可借鉴持久化次序；摘要仍是模型生成的派生内容 |

上述仓库路径均相对 `/Users/christmix/OraculoSpace/`。后续实现要复核引用，
不能把本地源码观察写成厂商协议保证或 Kolyan 已通过的能力。

## 计数对象、证据等级与模型支持

计数对象是**经过实际 parameter planning 和生产 wire mapping 的请求语义**，
不是 neutral JSON 字节数，也不是单独 message 文本。至少覆盖 system、消息、
tool schemas、tool choice、reasoning、结构化输出及会改变模型输入的扩展。
图像、文件、opaque reasoning 和缓存字段的覆盖由对应 adapter 明确声明。

生成请求与计数 API 的 body 不要求逐字节相同：stream、输出限制等字段可能不属于
计数 endpoint 的输入。必须保存实际生成 wire digest、计数 body digest、映射版本
及有说明的字段投影，证明所有模型输入内容都被覆盖。禁止用“SDK 接受 JSON”
代替覆盖证明；未支持扩展、远端文件或隐式 conversation 状态必须拒绝对应可信声明。

必需区分以下证据，不用一个裸数字抹平可信等级：

| 等级 | 含义 | 可用于什么 |
| --- | --- | --- |
| ExactProviderCount | 官方精确计数契约适用的 endpoint/model，实际输入及 framing 覆盖完整且身份验证通过 | 可作为现有 Strict 的可信计数；不是所有兼容 endpoint 的通用能力 |
| ProviderReported | 已认证、支持的 endpoint 对精确计数输入报告的数字 | 明确命名的服务端报告预算规划；不声称计费完全相等或数学上界 |
| VerifiedModelCount / UpperBound | 指定模型版本、tokenizer/framing/模态规则及覆盖证据满足宿主要求 | 相应可信预算，UpperBound 还须独立上界依据 |
| Estimate | 字节、字符或不完整 tokenizer 估计 | 诊断与显式非 Strict 选择策略 |
| Unknown | 无支持、覆盖不完整、身份不匹配或证据缺失 | 明确拒绝需可信计数的准入；可另用宿主选定的字节预算 |

已有 Strict 契约不得被静默降低。若服务端计数只提供估计语义，不能直接转换为
现有 `TokenMeasurement::Trusted`；首批可交付 ProviderReported 预算规划，但必须
标明 assurance。安全余量是宿主策略，不把一个经验百分比变成严格上界。
ExactProviderCount 在已验证适用范围内可进入 Trusted，不因“来自服务端”统一降级。

### 两种官方协议与 MiniMax

- OpenAI Responses：本地官方 SDK
  `openai-python/src/openai/resources/responses/input_tokens.py:134`
  有 `/responses/input_tokens`，支持 input、instructions、tools 等真实计数输入。
- Anthropic Messages：本地官方 SDK
  `anthropic-sdk-python/src/anthropic/resources/messages/messages.py:1502`
  有 `/v1/messages/count_tokens`，支持 messages、system、tools、thinking 等字段。
- 首批交付这两套**具体异步计数 adapter**，不是宣称任意兼容端点都支持。
  支持登记绑定 endpoint 身份、协议、模型及 revision，默认 Unsupported。
- MiniMax 两协议默认 Unknown；未经验证不借用 OpenAI tokenizer，不将 bytes/4
  当 token 数。计数 endpoint 是否存在、涵盖何种字段、返回何种语义仍待独立验证。
  可实际使用明确标注的字节预算缩减，不冒称 Strict-token 验收。
- 本地 tokenizer 仅在模型词表/版本明确、framing 和模态覆盖可验证时进入可信登记。
  文本 BPE 计数可以是准确的文本计数，但不自动等于整个 Provider 请求计数。

官方 SDK 的存在证明可实现 adapter，不证明本需求已在厂商端验收。

### 官方计数语义核验（2026-10-02）

[OpenAI 官方 Counting tokens](https://developers.openai.com/api/docs/guides/token-counting)
明确说明 Responses input count 接受相同输入格式，返回模型将收到的精确计数，
包含角色和消息边界等 formatting tokens。该依据允许已验证的官方 Responses
endpoint/model 和完整匹配输入使用 ExactProviderCount。它不证明 MiniMax 或
其他兼容 endpoint 有相同实现，也不证明 Chat Completions framing 被覆盖。

[Anthropic 官方 Token counting](https://platform.claude.com/docs/en/build-with-claude/token-counting)
明确称计数为 estimate，生成时输入 token 可能有少量差异；计数可能包含系统额外
token，不能直接解释为计费数字。其计数端点对部分生成 API 输入不支持，包括
多数 server tools、MCP connector、URL/file 来源的 image/document；图像/PDF
可计数的 base64 路径须按对应版本验证。第一版保留 ProviderReported，不提升
Trusted；模型 revision 或内容来源变更必须重新核对 coverage。

本次只读取官方文档和本地 SDK，未调用计数或模型 API。官方契约依据、adapter
实现、localhost 验证和真实端点验收是不同证据，不能互相替代。

### 第一版计数字段投影与 coverage

协议身份必须来自实际生成 adapter，不能由 `openai_compat` 部署名字推断。
当前检查的 Main `kolyan-protocol-openai/src/client.rs:102` 固定生成
`/v1/responses`；未发现 Chat Completions adapter。若后续 MiniMax 使用 Chat，
其协议必须登记为 `openai_chat_completions`，不得用 Responses count 代替。

| 实际生成协议 | 第一版计数输入投影 | 有说明的排除项与拒绝项 |
| --- | --- | --- |
| `openai_responses` | 本地 SDK 的 12 个 typed 字段：`conversation,input,instructions,model,parallel_tool_calls,personality,previous_response_id,reasoning,text,tool_choice,tools,truncation`；原样复制生成请求中实际出现且有覆盖证据的字段 | `stream,max_output_tokens` 不送 count，输出 reserve 独立预算；cache key/retention 只在登记已证明不改变输入语义时排除，否则 coverage 为不完整；隐式远端历史在第一版拒绝可信计数 |
| `anthropic_messages` | 原样复制实际出现的 `model,messages,system,tools,tool_choice,thinking,output_config`，包含原 blocks、tools 中的 cache_control | `stream,max_tokens` 不送 count，输出 reserve 独立预算；不能丢掉嵌套 cache/control 或 reasoning blocks |
| `openai_chat_completions` | 第一版无受支持的服务端计数投影 | 禁止改成 Responses input 后计数并宣称覆盖 Chat framing；允许生成 wire 字节预算，token 保持 Unknown |

两套 count 都仅复制生成请求中存在的字段，不填 null、不添加默认 auto、不更改
工具 schema。`text` 必须完整复制，包含 structured format/schema/strict 及其他
实际 text 设置，不能仅复制 schema 或丢掉该字段。本地 SDK 的完整 12 字段
allowlist 不意味着 Kolyan 已发送全部字段；后续生成映射新增使用时必须增加
coverage revision 和数据，不以 unknown extension 自动 passthrough。
第一版不允许隐式服务端历史、远端可变 file 引用或未列明的扩展得到完整覆盖标签。
未知字段可继续由原生成能力表控制，但计数必须返回 CoverageUnsupported，不能
为了计数而删除实际生成字段。字节模式仍可以度量完整实际生成 body。

响应形状按本地官方 SDK：Responses 为 `object:"response.input_tokens"` 和
非负整数 `input_tokens`；Messages 为非负整数 `input_tokens`。第一版严格记录
响应 schema revision；额外字段不默认为授权或覆盖声明。零是一个合法计数值，
不是 Unknown 的替代值。

本地 Anthropic `MessageTokensCount` 文档描述 messages/system/tools 的总数；
其官方指南提供 estimate 限定，不能仅据 SDK 的字段名推断精确性。
OpenAI 官方精确计数、Anthropic 服务端估计和兼容端点 Unknown 分开登记。
只有匹配上述官方精确范围和完整 coverage 的 OpenAI 证据可构造 Trusted；
不能由 adapter 自称精确或仅凭 count endpoint 返回 200 作此升级。
模型 tokenizer/framing 的 VerifiedModelCount 仍须另有依据。

## 第一增量：计数与选择的真实生产消费者

### Mapping 和异步计数

Provider 层负责一个可复用的生产 planned-wire 构造路径；生成发送和计数投影都
消费该路径。保留原 parameter table 的能力拒绝，禁止复制 mapper 或删掉 tools
来获得计数成功。映射 API 的具体类型与可见性经 owner 共同审查后冻结。

计数运行在异步宿主准备阶段，有独立超时、取消、响应字节和请求次数上限。
不在现有同步 `ContextTokenCounter::count` 内阻塞网络。若复用该同步端口，
它只能读取已经验证且绑定当前 request 的计数证据，缺失或不匹配即拒绝；
不能用旧请求结果、当前时钟、模型名字相似性或默认值补足计数。

不新增 hidden retry。计数失败与模型 opening 失败分开保留原因；认证信息不进入
证据。缓存键包括 endpoint/model/revision、mapping revision 和完整计数输入 digest，
不能仅按 Session ID 或消息数量缓存。缓存命中必须重验精确身份和覆盖。

### 确定性历史选择

宿主显式启用版本化策略：预算 assurance、模型窗口、输出 reserve、额外余量、
字节/消息/block 上限、候选数、计数调用数和总时限全部有界。
输出 reserve 不削减已冻结的宿主权限或授权输出 ceiling。

首批建议默认资源配置：至多 8 个缩减候选，含初始请求至多 9 次计数，每次
计数 timeout 10 秒，总准备窗口 30 秒；总窗口始终再受剩余宿主执行期限约束。
完整 neutral source 和单个生成 wire 各至多 16 MiB，source 仍受现有 4096 消息、
16384 blocks 上限约束；单次计数响应至多 64 KiB。这些是待 review 的宿主限额，
不是模型参数或 token 保证。实际 adapter 还须拒绝负数、非整数、溢出或未知响应
schema；模型不能提升这些上限。允许配置更小值，增大硬上限须另审规格。

1. 先形成实际完整 source 并验证结构/资源边界，再对其计数。
2. 足够预算时保持完整请求，不为了产生“压缩事件”删历史。
3. 不足时按确定顺序移除最旧可省略的完整历史组，保留首个用户目标锚点、当前
   用户输入整段尾部以及所有保留工具调用的实际配对。必要时扩大组边界，不能
   跨省略片段把重用 call ID 错配成另一工具结果。
4. 不改变 retained message 的内容、顺序、角色、签名 reasoning 或 call identity；
   system、工具声明、模型选择、extensions 和当前权限始终原样保留。
5. 对候选重新消费生产 mapping 和计数，再调用现有投影验证器；有界候选耗尽、
   必须保留内容仍超限、计数覆盖丢失或取消则拒绝，不循环至成功。
6. 字节预算与 token 预算分别导出。未知 token 不变成零，也不变成成功的 Strict。

首批必须有真实宿主调用这个 selector，并把结果送入既有 Server/Runtime/Runner；
只有算法单测、counter trait 或测试外部手工裁剪不算该增量完成。

## 准入前持久化、来源与恢复

顺序必须明确：Task/权限上限及 Session 历史 → 完整 source → 计数与选择 → 保存
source/plan/provenance → 原子登记本 Turn 的不可变选定输入 → Runtime admission
→ Core 和 Provider 消费同一选定输入。任何必要证据写入失败都阻止执行。

现有 `TurnPreparationHook` 不是可变请求端口；本批将其迁移为下述异步准备返回
契约，并完整迁移调用方。不得以 optional compatibility/fallback 绕过必需来源。
Session 的 original current input 和 full trajectory 保留，选定上下文另有明确引用。
不能在 Storage 中用 selected messages 覆盖原完整历史。

### 已冻结的 Server 准备返回接口

```rust,ignore
pub type TurnPreparationFuture<'a> = Pin<Box<dyn Future<
    Output = Result<ModelRequest, ServerError>> + Send + 'a>>;
pub trait TurnPreparationHook: Send + Sync {
    fn prepare<'a>(&'a self, execution: &'a ExecutionRef,
        session_version: u64, request: &'a TurnRequest)
        -> TurnPreparationFuture<'a>;
}
```

这是可信宿主准备端口，不接受客户端或模型提供的任意替换请求。它只在新 Turn
合并历史后执行；resume、approval resume 和 recovery 消费已保存的选定输入，
不重新调用。hook 返回前须成功保存完整 source 和选择/计量来源；保存错误必须
传播。接口本身不提供 recorder 或证明已实现持久化，由实际 Agent 消费者完成。

Server 在开始 Session Turn 之前独立检查：除 messages 外 ModelRequest 全部字段
相同；选定消息是原始完整消息的保序子序列；原 current input 的完整尾部原样
保留。只允许省略历史，不允许新增、修改、重排消息或改变 system、模型、工具、
输出上限和扩展。更强的原始配对闭包与首目标保护由实际 Agent 的 project_context
验证，不能仅依赖 Server 的保序检查。Turn ID 与 max_steps/max_tool_calls 不交给
hook 修改。以后摘要生成或 Turn 内缩减必须另行扩展契约。

Server 对 prepare future 施加硬上限 30 秒，并取请求剩余 deadline 的更小值；
零剩余时间不调用 hook。等待消耗通过同一截止锚点的余额自然减少，不改写
request.config.deadline，也不能把计量时间加在执行窗口之外。超时、丢弃或拒绝不开始 Session Turn、Runtime、
Provider 或工具效果。丢弃 future 不证明远端 count 已停止；可能已保存的准备
artifact 保持未准入状态。并发 Session version 冲突在 begin_turn 时拒绝，不能
重新加载历史、再次计量后静默重试。调用方明确不配置 hook 的普通执行不是兼容
分支，但必须准备的 Agent host 不得选择无 hook 路径。

此切片新增独立数据矩阵：原样/删旧组，完整 source 与实际模型输入一致，原始
历史未被覆盖；错误修改字段、插入/重排/修改消息、删除当前尾部，保存拒绝、
计量 future pending 超时、零 deadline、并发 version 冲突，以及审批重建后
hook 调用次数不增加。全部实际请求、Session/ledger 和错误先导出后回读比较。
旧 preparation 拒绝用例只迁移 trait 签名，保留原场景和断言。Server seam 通过
不算实际 selector/count/provenance 宿主闭环完成。

Server seam 已接入主线并由主控独立验证：119 Server、98 Agent 单测通过，
0 failed/ignored。新增 37 场景在 Memory 与 SQLite 上分别执行，完整 74 行
先落盘、关闭、物理回读再比较；来源保存拒绝、资源限额、错误字段/消息、
并发版本冲突、deadline 等待，以及审批/显式 resume/committed recovery
宿主重建均覆盖。选定输入按 Runtime 已存不可变输入恢复，不重新调用 hook。
模型与工具为明确声明的本地 scripted adapters，不能记作真实 Provider 验收。

主控跨层门禁 66 passed、0 failed、22 ignored；ignored 网络/显式配置场景
未执行，不计入通过。严格 Server/Agent Clippy、fmt、source-layout 和 diff
检查通过。日志 `/tmp/kolyan-context-preparation-main-v1.log`、
`/tmp/kolyan-context-preparation-main-strict-v1.log`、
`/tmp/kolyan-context-preparation-cross-layer-main-v1.log`。
完整新增轨迹位于 native 临时根目录下
`kolyan-turn-preparation-XWzZeI/actual.jsonl` 和
`kolyan-turn-preparation-bVuKjb/actual.jsonl`。计量适配器与实际来源消费者
尚未通过主控集成验收；本段只证明 Server seam 及受影响跨层回归。

必需来源内容：schema/policy revision、逻辑/物理 owner、Session base version、
完整 source artifact/digest、selected request digest、保留/省略范围、计数证据及
模型/mapping 身份、budget assurance、reserve、选择原因和拒绝原因。
原始内容保存在受保护有界 artifact，不进入普通 Trace 或认证日志。
摘要、选择和计数证据都不是授权。

相同 owner/source/policy 重试须内容幂等；changed source 或并发 Session version
冲突须拒绝，不偷偷加载新历史重选。未准入的孤立准备 artifact 不代表执行事实。
恢复从 saved selected input 和精确证据重建，不重新选择、reprepare 或授新 grant。
取消/终态任务不能因准备成功被复活。

每个 Step 输入增长时仍须校验真实请求预算。首批允许异步计数后**原样**检查，
但不允许在 Provider 内缩减；超过预算明确停止并保留恢复证据。
Turn 内自动缩减需要后续单独的 immutable checkpoint 转换契约，不能声称首批已支持。

## 后续摘要压缩的边界

历史选择不是生成摘要。下一增量可在独立有界准备流程中生成摘要，但必须先保存
完整源，再保存生成请求、模型/revision、实际结果、来源范围和摘要 digest，验证
其选择结果后才提交。模型摘要是 untrusted derived content，不成为用户指令、权限、
工具效果或任务完成证明；失败、取消、超限时不覆盖原上下文。
生成摘要的 usage/费用单独记录；归档存在不等于可无限加载。

## 验收数据、真实接入与完成条件

独立 fixtures/expected 与框架分开；所有 actual 行、映射输入、计数结果、选择和
物理 ledger/source 证据先落盘，再比较。禁止修改旧场景或放宽配对、权限断言。

第一增量至少覆盖：

- 中文/emoji、控制字符、工具 schema、reasoning、结构化输出、cache/extension；
  明确验证哪些字段进入计数，未知模态/扩展不能获得可信标签。
- 两协议实际 Provider/SDK localhost 请求：生成 wire 和计数 body 经过生产路径，
  精确核对内容覆盖。localhost 数字是协议 fixture，不是真实模型 tokenizer 证据。
- 不缩减、恰好预算、超限触发缩减、锚点/当前尾部过大、候选/计数次数耗尽；
  多调用工具组、跨 Step 重用 call ID、孤立/重复结果、不可拆 reasoning。
- Unknown、能力未登记、模型/endpoint/mapping/revision 不匹配、计数错误、取消、
  响应超限、整数溢出及缓存错用；不自动退化成可信估计。
- 实际 Runner → Task/Session → Runtime → Core → Provider 的 selected request
  一致性；完整源保存、不同 Session/child 隔离、重建和审批恢复不重选、不重放效果。
- 准入前证据保存失败、并发 source/version 冲突、取消后 late count；零未授权
  模型执行、工具效果或子实例。计数请求与模型生成请求分别统计。

## 实现所有权、API 草案与首批交付切分

以下是供主控冻结的具体边界，不是已经导出的 Rust API；实现释放后由各 owner
落盘并完整迁移 callers。Server seams 由主控持有，本设计 owner 不编辑。

### A：shared planned-wire / count adapters

#### 已冻结的适配器接口

```rust,ignore
OpenAiClient::count_input_tokens(
    &InputTokenCountRequest, timeout: Duration,
) -> Result<InputTokenCountResponse, OpenAiError>;
AnthropicClient::count_tokens(
    &MessageCountTokensRequest, timeout: Duration,
) -> Result<MessageTokensCount, AnthropicError>;
provider.prepare_wire(&ModelRequest)
    -> Result<PreparedContextWire, ProviderError>;
provider.with_count_profile(CountProfile) -> Self;
provider.count_prepared(&PreparedContextWire, timeout: Duration)
    -> Result<ProviderInputCount, ProviderError>;
```

这组名称与职责已冻结，实际 Rust 类型定义由切片落盘并接受主控审查。
CountProfile 默认 Unsupported，登记须绑定无秘密 endpoint 身份、协议、模型及
revision；未登记或 coverage 不完整时 count_prepared 显式失败且零网络请求。
调用时还要核对 prepared 的身份、digest 和 mapping revision 与当前 Provider
一致，不能把另一实例的准备结果发送到当前端点。PreparedContextWire 字段私有，
不提供公开可篡改或可反序列化绕过构造校验的入口。

ProviderInputCount 保存报告数字、来源和精确计数输入 digest，不直接构造 Trusted。
每次 count 单次发送，timeout 覆盖发送及完整读取，响应硬上限 64 KiB；丢弃
future 释放本地等待，不宣称远端停止。没有隐式 retry 或默认 timeout 替换。
生成 stream 消费同一 prepare_wire 路径，现有参数表与协议映射行为保持一致。
新计数 DTO 按上述官方 SDK 投影完整字段，未覆盖扩展不能获得完整 coverage。

本切片只写 model accounting、两个 Provider 与两个 protocol 的模块、导出及
分离的数据驱动测试。必要 crate 依赖先报告，根 Cargo.lock 由主控处理。
本地 HTTP 验证必须保存实际生成/计数请求、完整响应与拒绝原因，再集中比较；
不调用真实计数端点、不修改现有场景。宿主选择与来源持久化是另一写集，
适配器门禁不能代替完整 E2 验收。

- 公共计数数据归 `kolyan-model` 的新 accounting 模块：协议、映射身份、coverage、
  计数证据和 assurance，不依赖 Agent/Server/Storage，不持有凭据。
- 两个 Provider owner 各自将现有规划/映射提取为一处 production prepared-wire
  路径；计数投影与实际生成发送共享它。新的 prepared-wire 对象由受校验的生产
  构造器产生，调用方不能改字段后复用原 digest。
- 两个 protocol owner 各自实现具体 count HTTP 方法和有界响应解析，不改 SSE
  loop，不把 count 错误混成模型 stream failure，也不为本批增加重试器。
- Chat 没有 count adapter 的结果是明确 Unsupported，不是另一个 endpoint 的
  成功结果。MiniMax 无支持时不发探测请求；网络验证须另获主控授权。

拟定数据形状：

```rust,ignore
enum ContextProtocol { OpenAiResponses, AnthropicMessages, OpenAiChatCompletions }
enum AccountingAssurance {
    WireBytes, ExactProviderCount, ProviderReported, VerifiedModelCount, UpperBound
}
struct MappingIdentity { endpoint_id: String, protocol: ContextProtocol,
    model: ModelRef, mapping_revision: String, coverage_revision: String }
// Provider constructor validates fields; immutable getters expose bound body/digests.
struct PreparedContextWire { /* private mapped body, neutral/wire digests, identity */ }
struct AccountingEvidence { identity: MappingIdentity, neutral_digest: String,
    generation_wire_digest: String, count_input_digest: Option<String>,
    generation_wire_bytes: u64, assurance: AccountingAssurance,
    input_tokens: Option<u64>, counter_revision: String }
```

WireBytes 的 `input_tokens` 必须 null；ExactProviderCount/ProviderReported 必须
有完整匹配的 count 投影/结果，前者另验证精确契约适用的 endpoint/model 身份。
构造和反序列化验证组合，不接受缺失字段、错协议或默认 assurance。
Unknown/Unsupported/错误使用显式结果类型，不能构造一份成功的零计数 evidence。
端点身份是宿主配置的无秘密身份，不把完整带认证 URL 写入 artifact。

### B：实际 host selector / provenance

#### 已冻结的纯候选生成切片

```rust,ignore
pub struct SelectionPolicy {
    pub id: String,
    pub revision: String,
    pub source_bounds: ContextPolicy,
    pub max_reduced_candidates: u8, // 0..=8
}
pub fn selection_candidates(source: &ModelRequest, policy: &SelectionPolicy)
    -> Result<Vec<ContextProjectionPlan>, ContextError>;
```

复用 context_source_digest 的原源验证，提取内部原始配对遍历供 projection 和
selector 共用，不改变旧校验。以实际消息位置构造每个调用—结果的半开区间，
合并重叠而非仅相邻区间，形成完整连续组；多调用与分散结果形成配对闭包。
完成配对后释放 ID，重用 ID 不连接旧批次。首个非纯 ToolResult 用户锚点与
最新此类用户输入起整个尾部必须保留，与其相交的组同样全部保留。

候选零是完整请求。可选组数 G，缩减候选数 N=min(G, max_reduced_candidates)；
第 k 个缩减候选省略最旧 ceil(k*G/N) 个可选组，k=1..N。N=0 只返回完整候选。
这是版本化的有界前缀批量规则，最多九个候选；允许缩减时包含必要组最小候选。
它不承诺枚举所有选择、最优保留量或 token 单调。输出范围排序且合并相邻范围。

纯函数不计数、不判断 fit、不调用 Provider、无持久化或授权。实际宿主逐候选
消费生产映射与计量，选第一个满足显式预算者；计量错误不能当成预算不足继续
删历史。必要最小候选仍超限，宿主明确拒绝。无用户锚点或源结构不合法先拒绝。
每个候选必须通过现有 project_context 配对与保护校验，未知 token 保持 Unknown。

释放写集为 context/selection.rs、分离测试和 JSON 数据、context.rs 模块导出及
projection.rs 的内部配对 helper。保留全部旧测试输入与断言。新增数据至少覆盖
重用 ID、多调用逆序结果、中间空消息、尾部混合文本/结果、同一首尾锚点、
十组八候选的 2/3/4/5/7/8/9/10 删除前缀、零候选、非法限制和完整 opaque 内容。
完整源、policy、候选、投影和错误先导出后比较。宿主接入属于主控写集；
这个切片通过不能被记作 E2 完成。

纯候选切片已集成为 Main `937c003`。主控独立运行 Agent 回归得到 98 passed、
0 failed、0 ignored；严格 Clippy、fmt、source-layout 与 diff 检查通过。新增
20 个数据场景产生 36 个候选，完整源、policy、plan、projection 与错误先导出
再物理回读比较。主控实际文件：
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-context-selection-i8C7tp/actual.jsonl`。
日志 `/tmp/kolyan-context-selection-main-v1.log` 与
`/tmp/kolyan-context-selection-main-strict-v1.log`。这证明纯候选算法和现有
Agent 回归，不证明计量适配器、来源持久化或实际 host 已完成。

本 design owner 的下一有界写集拟为 Agent 新 `context/selection.rs`、独立
`context/selection/tests.rs` 与 JSON fixtures、所属模块声明，以及本规格。
不占 Provider/SDK、Server、Storage 或共享 accounting 源码。需要的 Server seam
和生产宿主装配由主控独立实现；只写算法而未接入宿主不算完成。

API 意图如下；实际类型需与 A 和主控一起冻结，不能各 owner 独立猜结构：

```rust,ignore
// Pure deterministic grouping; only whole original pairs may be omitted.
selection_candidates(source: &ModelRequest, policy: &SelectionPolicy)
    -> Result<Vec<ContextProjectionPlan>, ContextError>;
// Concrete host consumer invokes production mapping/accounting, not a copied loop.
prepare_selected_context(source: OwnedContextSource, policy: SelectionPolicy,
    accounting: &ConfiguredContextAccounting, control: PreparationControl)
    -> PreparationFuture<Result<SelectedContextPreparation, ContextPreparationError>>;
```

`OwnedContextSource` 绑定实际 owner、Session base version、原完整 ModelRequest；
`SelectionPolicy` 必需选择 WireBytes 或 ProviderReported 等 assurance，不从
计数是否成功推断模式。`SelectedContextPreparation` 包含选定 request、完整源、
plan、每次候选 accounting evidence 和 provenance；返回值本身不是持久化 receipt。
主控的 admission consumer 必须保存完整证据并取得精确 FactRef，才登记本 Turn。

WireBytes 模式的具体执行：生产 mapper 生成完整 body → 按序列化 UTF-8 字节
检查候选 → 选择首个满足限额的完整历史候选 → 保存来源 → 实际准入并执行。
它不调用 count endpoint，不需要伪造 counter，也不跳过 MiniMax；token budget
字段保持 Unknown，不能通过现有 Strict 分支。Server/Agent 的 provenance reader
须区分本模式，不能因 token 未知把明确授权的字节模式误标为 Strict 成功。
输出 token reserve 仍独立遵循原宿主/模型配置，不从 wire 字节推导 token。

### 第一版独立 fixture 行

数据固定 case_id、protocol、生成输入、宿主模式/限额、count 响应或故障、expected
coverage/assurance/选择范围、是否准入及效果计数；不以模型回答字符串作预算证明。

| case_id | 必须证明的实际结果 |
| --- | --- |
| responses_exact_projection | 完整 12 字段投影规则及实际出现字段深度相等；text/structured settings 不丢，排除项逐项有原因 |
| official_responses_exact_scope | 精确计数身份和 coverage 成立才可进入 Trusted；localhost 只证明此门控，不冒充真实精确性验收 |
| compatible_responses_not_exact | 相同 shape/200 不足以取得 ExactProviderCount，未登记 endpoint 保持 Unknown |
| messages_nested_tools_thinking | tools/cache 和 thinking 原样投影；报告值不变成 Trusted |
| chat_count_protocol_mismatch | 不发 Responses count；返回 Unsupported，不伪造计数 |
| unknown_extension_coverage | 不删生成扩展；可信 count 不准入，WireBytes 仍测完整 body |
| reported_zero_valid | 合法零报告保留 ProviderReported，不当 Unknown/Exact |
| reported_schema_failure | 负数、小数、溢出、错误 object/未知 schema 分别拒绝 |
| minimax_byte_fit_unchanged | 不请求 count，完整请求进入实际 host，token Unknown |
| minimax_byte_reduce_real_host | 超字节预算删旧完整组，actual Core/Provider 同 selected request |
| anchors_cannot_fit | 必要尾部仍过大，不准入、不调用模型或工具 |
| original_pair_reused_id | 不跨省略历史错配相同 call ID，所选原始 pair 完整 |
| rebuilt_approval_same_selection | 重建消费原 selected input/FactRefs；效果只按原授权执行一次 |
| source_write_or_version_failure | 保存失败或竞争冲突没有 execution admission，不重选新历史 |
| count_cancel_or_limit | late count、候选/次数/时间耗尽显式失败，无隐藏重试 |

释放顺序建议：主控冻结 A 的证据和 projection 类型、B 的返回契约及 Server source
持久化 seam → A/B 分离 worktree 并行 → 先 WireBytes 的 MiniMax-compatible 实际
宿主闭环离线验收 → 两套 ProviderReported count adapter 的 localhost 验收 → 主控
整合生产宿主 → 明确授权的计数支持核验和实际 MiniMax 缩减任务。未知计数不阻止
合法字节消费者交付，但不得据此宣称模型精确预算验收。

主控授权后才运行支持端点的实际计数和长任务缩减场景。按模型/协议分别报告
ProviderReported、可信模型计数、Unknown 与字节预算；实际 usage 的差异保留，
不把单次相等当作通用上界。MiniMax 保留 named/inline、委派与真实收据条件，
不能靠去除能力获得“上下文验收”。原失败矩阵不被新运行覆盖。

完成顺序：契约审查 → disjoint implementation ownership → 具体 adapters/selector
及生产宿主接入 → 数据门与跨层回归 → 授权真实场景 → 主控完整门禁及正常 hooks。
未接入、未运行、Unknown 和未通过项都明确列出；文档、局部通过或单次回答不构成
完整 E2 或整个 0031 的完成。实现 gate 与网络 receipts 待实际运行后记录。

### 统一的执行截止锚点

已确认当前 preparation 扣除准备耗时后，Session begin 与 Coordinator 操作
仍发生在 Runtime 重新计算 deadline 前，会赠送额外执行时间。该修复必须
贯穿入口、准入及 Core，不只在 begin 后再次减去一个 Duration。

冻结 Core `TurnDeadline` 为 private-fields、Clone、无 Serialize/Deserialize 的
中立执行时间值。`capture(Option<Duration>, Option<u64>)`、
`tighten_absolute(self, Option<u64>)` 返回 `Result<_, TurnDeadlineError>`；
另有 `validate_duration(&self, Option<Duration>)`、`remaining()` 与
`deadline_at_ms()`。原 Duration 绑定不变，None 表示无限、Some ZERO 表示
耗尽；收紧只取交集，None 不能移除上限。错误明确区分 ClockBeforeUnixEpoch、
UnixOverflow、InstantOverflow、DurationMismatch，不能默认成无限或 timeout。

入口在实际首次 poll、任何 Session/Coordinator/admission I/O 前捕获一次。
同时保存当前进程单调截止点及向下取整的 Unix 毫秒截止时间。使用 checked
时间加法、转换与 Instant::checked_add；精确 Unix Duration 加相对 Duration
后再 floor 到毫秒，禁止各自取整后相加。单调点与 Unix 对应的更紧 ceiling
取交集，时钟采样跨度采用保守映射，不让转换或后续收紧扩大任何已有窗口。
无任何上限不需读取 Unix 时间。新增独立纯时间算法用例覆盖亚毫秒、采样
跨度、epoch 前、整数与 Instant 溢出、零和 None，不修改真实系统时钟。

外部传入的 Unix 毫秒 absolute ceiling 是已声明的 durable 身份，不扣采样
跨度改写它。没有更紧的相对窗口时原毫秒值必须保持；有相对窗口则与其
保守生成的 Unix 截止值取 min。采样跨度用于相对锚点的保守序列化与外部
absolute 到本进程 Instant 的保守映射；tighten_absolute 同时分别收紧
单调点和原始 Unix ceiling，不能把单调映射误差再次扣成新的 durable 时间。
旧 absolute ceiling 重复收紧与精确毫秒断言保留，新增注入采样跨度用例。

`TurnExecutor` 增加只读 `absolute_deadline_at_ms()`，以及显式
`start_resumable_with_deadline(request, deadline)` 和
`start_resumable_with_control_and_deadline(request, control, deadline)`。
原 standalone 入口在自身入口 capture 后共用执行路径；显式路径只验证原
Duration 并与 executor ceiling 再取交集，不进入重新锚定的 RunState::new。
单调点仅 Core 内部可读。checked checkpoint 恢复从已验证的绝对 deadline
映射当前进程时钟，不从 request Duration 重获预算。

Runtime 已进入 resume 后，checked Core merge 返回实际 TurnError::TimedOut
时沿用 persist_error 保存真实 timeout，以供 Session 收尾；其他 merge 错误
不借此制造 terminal。

Server 的 crate-internal `ExecutionService::start_with_deadline` 与 Runtime
跨 crate 公开的 `DurableTurnDriver::start_with_deadline` 沿用现有 start 参数，
末尾增加 TurnDeadline。独立 public start 各在自己的入口 capture；Session
start 在 load 前 capture。prepare 使用 min(30 秒, 当前余额)，不改写原
request.config.deadline。Session load、计量、来源保存、begin、Coordinator
及 admission 均耗用同一窗口。InputAdmission::new 接收 &TurnDeadline，
直接保存有效 deadline_at_ms；准入与 Core 消费同一收紧后的锚点，durable
schema 不变，不保存 Instant 或第二份相对预算。更紧 executor 上限同时
约束 admission 和 Core。TurnError/RuntimeError/ServerError 增加 typed
Deadline cause，时钟/绑定错误不冒充已经执行或 StorageConflict。

登记前耗尽沿用 PreparationFailure::DeadlineExpired，无 Session Turn 或
Runtime terminal。begin 成功后耗尽仍交给真实 Runtime 准入和 Core timeout，
在 Step/模型/工具前停止，再关闭 Session；不能直接返回留下 Running，
也不能伪造 TurnTimedOut。取消先赢交接时，Server 私有 helper 精确核对
`{execution_id}/execution-cancelled` 的 ID、key、kind、execution/turn 与
既有 null payload，及 Session execution 绑定，再关闭 Cancelled。错误字符串
或 Task 状态不是取消证据。不制造 TurnCancelled，不覆盖已提交终态。
commit_result 与重建 reconciliation 共用该核对，保存失败保留真实事实
和错误以便重建收尾。跨进程仍明确依赖可信墙钟，不声称 Instant 跨重启防回拨。

该核对还要求 Coordinator 重建状态为 Cancelled。缺少 canonical cancellation
返回 false；字段损坏返回 typed LedgerError::Conflict，不关闭 Session。
commit_result、reconcile、load_reconciled 共用此规则；load_reconciled 跳过
active execution，既有 Core terminal 优先，不只 Completed。

新增 Memory/SQLite 数据覆盖 SlowSessionStore load/begin、Coordinator/admission
延迟、begin 后耗尽、交接取消、无/零 deadline、standalone 入口、更紧 executor、
审批恢复不刷新窗口与收尾存储失败重建。完整实际时序、Session/Ledger、
admission/checkpoint 和调用计数导出关闭物理读回；旧场景及断言不静默降级。
写集限定 Core deadline/turn、Runtime driver/admission、Server preparation/
session/start 及独立测试；不触及 Task、Agent、Provider、Tools。该修复是
E2 异步消费者的必要时间边界，不代表上下文或演进计划已经验收。

### 计量适配器主干验证

主控已按冻结清单逐文件校验并集成 Model、两套 Protocol 与两套 Provider
的 48 个文件。集成后独立五 crate 回归终态退出 0，共 88 passed、0 failed、
0 ignored，doc tests 为零；日志 `/tmp/kolyan-context-counting-main-v1.log`。
五 crate 全目标严格 Clippy 终态退出 0，日志
`/tmp/kolyan-context-counting-main-strict-v1.log`；格式与差异检查通过。
Cargo.lock 只补相应本地 crate 的依赖边，没有新增第三方 package。

这些用例验证 localhost 协议请求、响应边界和计量证据绑定，不是供应商网络
计数验收。默认不支持计数的 MiniMax 配置保持不支持，不能凭兼容端点获得
可信 token 数。生产 Agent 的逐 Step 计量消费者与受控缩减仍未集成；统一
deadline 的后续集成证据见下文。适配器通过不代表 E2 已完成。

### 已准备请求的真实生成消费

下一适配器批次新增协议明确的 opaque `PreparedOpenAiGeneration` 与
`PreparedAnthropicGeneration`，private fields、不可 Deserialize/Clone；各
Provider 暴露 `prepare_generation(&ModelRequest)`、对象只读 `wire()` 和
消费该对象的 `stream_prepared(prepared)`。对象保留真实 planner 的请求、
映射与中立 output validator 所需输入；准备无网络、无计数、不选择历史。
`wire()` 返回同一次映射产生的 PreparedContextWire，可交原 count_prepared。

生成前重新核对实际 Provider 的 opaque owner、精确配置身份、profile 和
全部摘要；不把 profile 未注册或 coverage 不完整误当作禁止生成。这些状态
禁止可信计数，但合法 MiniMax 请求仍可生成且零 count。需要分离 count
准入与 generation 绑定核验，不能从 count 接口静默忽略 Unsupported 错误。
改变 parameter table、Provider 实例或计量配置后，旧准备不能用于新消费。
正常 ModelProvider::stream 共用同一准备和消费实现，不能再 map 第二次，
不能复制 SSE loop、放宽协议终态、修补工具参数或隐藏网络重试。

实际传出的生成 body 必须与 wire 中固定的完整 generation body 字节摘要
一致，保留 tools、schema、cache、reasoning、厂商扩展和字段省略规划。
原 stream 仍是合法低层生成调用；新增消费口不自行授予发送权限，不承诺
跨崩溃 exactly-once。Runtime 的模型开启准入和 Agent 的真实预算消费者在
后续批次装配，不能凭此适配器宣称取消线性化已经实现。

新增 localhost 数据覆盖同一准备的 count/GEN 原始请求摘要、两协议完整
事件、未知计数合法生成、错误 owner、修改配置、计数配置变更和结构化输出。
错误绑定必须零 GEN，所有原始 HTTP body、完整响应事件和观察先写 JSONL、
关闭物理读回再比较；原 88 个模块测试及旧断言保持，真实 MiniMax 网络由
主控整合消费者后单独启动。本批写集仅 Model accounting 的分离核验、两套
Provider 的准备/消费及必要 Protocol body 消费口和独立 tests，不改 Core、
Runtime、Server、Agent、源码候选或现有源数据。

### 截止时间与生成消费者的主干验证

统一锚点已逐文件集成，保留 Server 现有 GoalSource 导出及旧 absolute 精确
断言。主干 workspace all-target check 退出 0；Core 102、Runtime 61、Server
124 tests passed，均 0 failed、0 ignored，日志分别为
`/tmp/kolyan-deadline-main-all-targets-v1.log` 与
`/tmp/kolyan-deadline-main-module-v1.log`。新注入时钟 20 行及 Memory/SQLite
各 25 行观察实际落盘后物理回读。新增矩阵未逐项穷举 cancellation 的全部
ID/key/kind/execution 损坏及 active-skip，不能由源码检查冒称这些均有数据验证。

已准备生成消费者另集成 18 个差异文件，普通 stream 共用 prepare_generation
与 stream_prepared。五个模型/协议/Provider crate 91 tests passed，另 Core
102 passed，均 0 failed、0 ignored；日志
`/tmp/kolyan-prepared-generation-main-module-v1.log`。45 行新增证据验证完整
固定生成 body、count/GEN 绑定与未知计数合法生成；它们是 localhost 证据，
尚不是供应商网络验收。Core 原仅测试使用的 RunState 构造器移到独立测试
文件，不修改测试输入和断言。

同一集成源码的 workspace all-target 严格 Clippy 退出 0，日志
`/tmp/kolyan-evolution-integrated-strict-v1.log`；fmt、布局与 diff 检查通过。
实际 Agent 计量策略、模型开启准入、缩减消费者及完整真实矩阵仍需后续交付。

## 模型开启证明与重试边界

下一批采用强制的新 durable opening 协议，不保留旧执行缺字段回退。
InputAdmission 增加 required opening_protocol，唯一合法值 1；旧测试构造器
显式迁移，场景和断言保留。Standalone Core 和低层 SDK 不被迫使用持久
协议，但生产 durable start/resume 必须执行，不能注册一个默认无操作的
绑定实现绕过。沿用现有 ModelProvider 的 stream future，覆盖真实 prepare、
count、准入和生成，不增设另一套 Core 开启 trait 或模型 loop。

ModelOpeningEventRef 使用 event_id/cursor，严格区别于 Journal 的 FactRef。
Runtime recorder 保存每次实际 ModelRequested 的 exact ledger coordinate、
step_id 和完整 neutral digest；禁止按 latest request 猜绑定。ModelContextPrepared
使用 critical schema1 的 model.context_prepared fact，subject 为精确 execution。
subject.kind 必须是现有 Journal 支持的 runtime.execution，subject.id 为
execution_id；不使用会被 Journal 拒绝的裸 execution kind。
payload 必须包含协议版本、ExecutionKey、step_id、model_requested coordinate、
neutral_digest、完整 mapping identity、count_profile_digest、generation_wire_digest、
generation_wire_bytes、count_input_digest、unsupported_count_fields 和 accounting。
所有字段严格解析，未知字段拒绝，nullable 字段缺失也拒绝。

accounting 为两个明确分支：ProviderReported 携带 counter_revision、
input_tokens 和宿主 max_input_tokens；WireBytes 携带 policy_revision 和
max_generation_wire_bytes。前者绑定实际 count report 的身份与所有摘要，
要求 coverage 完整并满足显式预算，不因此变为 Strict 的可信计数；后者
明确是字节策略，MiniMax 不支持计数时不探测未知 endpoint、不捏造 token。
将来 ExactProviderCount 需要独立已验证保证等级，不在首版悄悄提升。
完整 generation/count body 不写入单事实：最大请求可达 16 MiB，而 Fact
payload 有 128 KiB 上限。摘要事实不能作为重建并重发未知 GEN 的来源。

ModelOpeningAdmitted 是 ledger kind，稳定 ID/idempotency key 为
execution_id/model-opening/step_id。payload 为 schema_version、opening_protocol、
execution、step_id、model_requested、preparation FactRef、neutral_digest、
generation_wire_digest、generation_wire_bytes、count_profile_digest 和必需
nullable deadline_at_ms。Runtime 核对当前真实 prepared object 与已回读的
准备事实，再通过 append_unless_cancelled 提交。确认成功后只返回私有、
不可 Clone/Deserialize 的单次 permit；重复准入、未知提交结果不发新 permit。
permit 消费前重新检查同一 TurnDeadline，不扩大截止时间。

durable StepCompleted 由 recorder 注入 opening_protocol、model_requested
和 model_opening coordinate；不改 Core StepResult，也不从模型响应取引用。
顺序要求 InputAdmission < StepStarted < ModelRequested < ModelOpeningAdmitted
< StepCompleted < physical terminal。跨 Journal 的因果关系按 exact reference
回读验证，不能把 Journal position 与 ledger cursor 数值比较。SSE completed、
usage 或普通 Turn terminal 不能代替 Core 验证 EOF 后的 StepCompleted。

共享只读 inspect_model_openings(ledger, facts, request) 返回 opaque
VerifiedModelOpenings；request 包含 execution、exact through coordinate 和
显式行数/总字节/单 payload 限制。它实际分页读取固定 prefix、回读精确
准备 FactRef，核对每个 Step 唯一 request/opening/completion 链与 typed
StepResult，不调用 Task snapshot、模型、reconciliation 或任何写操作。
状态为 NotAdmitted、Completed、Uncertain；错误区分 InvalidRequest、
UnsupportedProtocol、MissingEvidence、BindingMismatch、OrderingMismatch、
InvalidPayload、BoundsExceeded 和保留真实 Ledger/Fact cause 的 Storage。
Verified 结果不可反序列化或由调用方构造。

NotAdmitted 必须有真实新协议准入、完整固定 prefix 和合法事件顺序；
空账本、缺 marker、未知 schema、损坏或读不完不能推导为“没调用模型”。
准入后缺可信 completion 一律 Uncertain，即使没有工具效果、超时/取消，
或网络是否发送不明也禁止自动重发。恢复入口、Task 失败投影与 retry 必须
消费同一个 inspector；当前 retry 只检查 tool effects，存在 unknown GEN
被误认为 safe retry 的源码反例，不能仅新增错误名称来声称已堵住。

重试核对 frozen ledger prefix，并保留授权前后重新读取精确比较。公开
coordinator authorize_retry 和 safe_to_retry=true 旁路同步收窄为可信验证
结果的消费者；不以仅 service façade 检查宣称全部入口 enforcing。模型
completed 仍不能跳过 tool effect、取消、预算和 idle 检查。本协议不承诺
跨崩溃 exactly-once，也不将 Task cancel 与 ledger cancel 宣称为原子事务。

先交付严格数据契约和真实 Ledger/Journal inspector 的 Memory/SQLite 新
数据门，再由主控装配强制 attempt binding、写入、实际 counted consumer
及 Task retry/failure 消费。基础切片未接线前不得计为开启准入已实现。
新增场景包含 count await 取消、准入后崩溃、timeout/cancel 缺 completion、
全部工具 NotCommitted 但模型 unknown、可信完成、marker/来源损坏及授权
竞态；逐行完整事实和错误导出后物理回读，保持旧场景和断言。

### 执行器接线保留契约

主控新增 StepExecutor::try_map_provider 与 TurnExecutor::try_map_model_provider，
两者消费已有执行器并调用一次 fallible wrapper，成功返回新 Provider 类型
的执行器。映射仅替换 Provider，原 validator、Step/Turn recorder、tool
executor、dispatch、timeout、Policy、boundary control、ExecutionKey、
snapshot digest 和 absolute deadline 必须原样移动，不重新构造默认执行器。
失败原样返回 wrapper 错误，不默认降级、不调用模型/工具，也不触发重试。
这些方法本身不发 opening permit；Runtime 下一批必须实际使用它们绑定
对应 attempt。新增独立测试覆盖字段完整保留、真实 Step 校验/记录仍生效
和失败零 opening，不能用它们的局部通过替代完整开启准入验收。

主干此接入点及新增四个独立测试的完整 Core lib 回归为 106 passed、
0 failed、0 ignored，终态 exit0；日志
`/tmp/kolyan-provider-rebinding-main-module-v2.log`。同一最终测试源码的
workspace all-targets 严格 Clippy exit0，日志
`/tmp/kolyan-provider-rebinding-main-strict-v2.log`。这是配置保留与实际 Step
行为的证据，尚未证明 Runtime 已强制绑定模型开启协议。

恢复同样必须共用一个锚点：TurnDeadline::restore 对已验证 checkpoint 的
绝对截止时间采样一次；Runtime 在外部等待/证据验证前建立锚点，纯 merge、
持久提交、模型开启和 Core 恢复沿用同一对象，不在后续恢复流程重新映射。
新增 merge_resume_with_control_and_deadline、
resume_checkpoint_with_control_and_deadline；旧便利入口仍执行相同契约，
只是负责建立原始锚点，不建立旧格式回退。传入锚点必须与 checkpoint 的
持久 cutoff 完全一致、不能带新的相对 duration；不匹配先拒绝且无效果。
这避免 count 和 Core Step 分别持有不同恢复时钟样本；没有截止时间仍
显式无限，不从系统当前时间制造新的预算。

该恢复接线的主干模块回归终态 exit0：Core 108、Runtime 61、Server 124
passed，均 0 failed、0 ignored；日志
`/tmp/kolyan-shared-resume-anchor-main-module-v1.log`。新增独立锚点测试
核对无限、耗尽、未来 cutoff 的精确 Instant 保留，以及三种 cutoff 不匹配
和新相对窗口拒绝；旧 checkpoint 精确断言未改。workspace all-targets
严格 Clippy exit0，日志 `/tmp/kolyan-shared-resume-anchor-main-strict-v1.log`。
这不是完整 counted opening 的验收；真实计量与持久准入仍需下一批接线。

随后相同主干源码的完整 agent_root 离线集成回归 exit0，74 passed、
0 failed、22 ignored，日志 `/tmp/kolyan-opening-seams-agent-root-offline-v1.log`。
它验证已有原生工具、审批/重建、子调用及 Goal 场景仍可运行；忽略的
供应商网络未执行，隔离开发中的新 Host/Skills load/Opening inspector
尚未纳入该次回归，不能据此扩大完成声明。

### 适配器统一准备与计量接口

Model 层新增 PreparedModelProvider，扩展已有 ModelProvider 的适配器能力，
不是 Core 的第二套执行或开启接口。关联类型 Prepared 保留各协议真实的
owned generation plan；PreparedModelGeneration 仅提供只读 wire。prepare_generation
不执行 I/O；count_prepared 借用同一个 owned plan，返回带真实身份与摘要的
ProviderInputCount；stream_prepared 消费它并使用现有 SDK 发送固定 body。
三者没有默认实现或普通 stream 回退，不把 count 支持、精度或发送许可混同。

PreparedContextWire 提供当前私有 CountProfile 的有界 JSON 摘要，供准备事实
绑定；默认不支持与显式注册、counter revision 变化必须产生不同的摘要。
协议适配器实现只转发现有 preparation、count 与 GEN 管线，不重写映射、
SSE 解析或网络重试。MiniMax 未注册计量时仍使用明确字节预算，不试探接口。

新增独立 localhost 集成框架，通过泛型接口运行两套真实 SDK，数据驱动覆盖
未注册、注册、同实例 clone、外部 owner、profile 变更及调用方输入变更。
记录实际 count/GEN HTTP body、原始字节、摘要、profile 与完整事件，写入临时
JSONL 后 flush、sync、close 并物理回读比较。验证零请求拒绝、count 与 GEN
引用同一准备对象及 GEN 发送字节未变化；count 沿用 typed DTO 序列化，
严格比较完整解码 body 与 canonical digest，不要求 JSON 对象键序相同。
保留旧测试和数据。这是本地 HTTP 证据，
不是实际供应商计量或完整 Runtime 开启准入验收。

此接口的主干模块回归终态 exit0：Model 26、Anthropic Provider 20、OpenAI
Provider 23 passed，均 0 failed、0 ignored；日志
`/tmp/kolyan-prepared-port-main-modules-v1.log`。新增最终 localhost 门终态
exit0、2 tests passed，按同一个框架实际跑 6 场景乘两协议共 12 行；日志
`/tmp/kolyan-prepared-port-main-http-v4.log`。逐行完整事件与 HTTP 观测由测试
写入 `kolyan-prepared-port-Lr6F0b/actual.jsonl` 和
`kolyan-prepared-port-woLN0d/actual.jsonl`，同步关闭并物理回读后比较。
相同最终源码 workspace all-targets 严格 Clippy exit0，日志
`/tmp/kolyan-prepared-port-main-strict-v2.log`；fmt、源码布局和 diff 检查通过。

首次新门因测试服务给 Anthropic 返回 OpenAI 的 object 字段而失败，实际
decode error 与请求、响应均已保留；修正仅复用各协议已有 full_fields
计量响应数据。另一次新断言误把 count DTO 对象键序与 canonical JSON 键序
视为相同，已按原合同改为完整 decoded body/digest 校验；GEN 仍比较实际
原始字节。这两项是新增测试框架问题，不修改生产解析器、旧场景或旧断言；
失败日志 v1/v3 保留，不追溯记为通过。

### 开启证明只读切片

新增 inspect_model_openings 从可信 Ledger 的精确有界前缀和实际 FactJournal
读取、校验完整请求/准备/开启/完成链，返回不可反序列化、无公开构造器的
验证结果。NotAdmitted、Completed 与 Uncertain 区分模型开启状态，不是
工具安全、任务完成或重试许可。存储错误、边界耗尽及缺少协议标记均拒绝，
不能把“没有看到”当作“没有开启”。首批只是只读基础，尚未接入强制写入器
或 Task 重试消费，已有 Runtime 执行不会因此自动获得开启证明。

审查增加 terminal-before-admission 的独立反例：ExecutionStarted(1)、
TurnFailed(2)、ExecutionInputAdmitted(3)，查询至(3)，没有 Step。修复前
两后端错误返回已验证 no_steps；修复后均 OrderingMismatch，不返回证明。
修复前失败日志 `/tmp/kolyan-opening-proof-terminal-order-repro-v1.log` 和
全部旧证据保留，原 162 个场景结果不变。冻结清单
`/tmp/kolyan-opening-proof-freeze-v2.sha256` 的 12 项主控物理核对通过；
10 个新文件精确导入，两个现有导出/事件枚举只合并必要 hunk。

主干 Ledger 28、Runtime 63 passed，0 failed、0 ignored，终态 exit0；日志
`/tmp/kolyan-opening-proof-main-module-v1.log`。Memory 和重新打开的 SQLite
各输出 163 行完整来源及结果，分别位于 macOS 临时目录
`kolyan-model-opening-proof-3KIY6G/actual.jsonl` 与
`kolyan-model-opening-proof-m24eiv/actual.jsonl`，同步关闭后物理回读比较。
这些是手工构造的协议数据，不是生产 count/GEN 或 MiniMax 验收。
相同源码完整 workspace all-targets 严格 Clippy 终态 exit0，日志
`/tmp/kolyan-opening-proof-main-strict-v1.log`；fmt、源码布局和差异检查通过。

### 实际开启绑定的接线设计

后续 Runtime 的 BindOpeningAttempt 消费 Provider，返回 associated Bound
Provider；不在 Core 新增第二个模型接口，不提供 blanket/default Allow。
这是待实现的接口方向，不是已存在的公共 API。Runtime 通过已实现的
try_map_model_provider 在实际 attempt 上绑定，原配置与共享 deadline 保留。

实际装配顺序为 AdvertisementProvider<ContextPreparingProvider<
OpeningModelProvider<HostProvider>>>。外层显式转发绑定且完整保留原字段，
原 stream 中的异步 Skills 当前来源检查、广告检查、上下文校验与 recorder
确认照常发生。不能为满足同步 prepare_generation 而跳过异步守卫或阻塞
执行线程；guard wrapper 只承担绑定和原 stream，不另暴露绕过守卫的同步
prepared 入口。真实 HostProvider 实现 SDK 的 PreparedModelProvider。

开启层在这些守卫之后准备一次 SDK owned generation plan，计量或采用显式
WireBytes 政策，写入并回读准备事实，append_unless_cancelled 后消费私有
一次性 permit，使用同一截止锚点及同一原始 plan 发送 GEN。Root、Child、
恢复和 pump 必须沿用统一装配；重建不复用旧 bound provider 或旧 permit。
基础写入器与绑定接口见下节；强制 marker/完成注入及重试拒绝消费者仍待
实际驱动接线与验收，不能由基础模块通过推断已生效。

### Runtime opening writer integration

Main verified all ten frozen source digests and imported nine new files plus
only the Runtime module/export hunk. The existing proof reader and Ledger enum
were reused without replacing their source. No SDK mapper, SSE loop, Core model
interface or automatic retry was added.

`BindOpeningAttempt` consumes a provider and returns its associated bound provider;
there is no blanket or default binding. `OpeningModelProvider` prepares once,
checks the explicitly selected ProviderReported or WireBytes policy, persists and
physically rereads the exact critical preparation fact, and atomically admits
against cancellation before consuming a private one-shot permit. It moves the
same owned plan to `stream_prepared`. Unsupported count never falls back to
generation or to an invented token estimate. Typed accounting, count timeout,
deadline, cancellation and storage causes remain available for the Driver.

The source publication methods validate actual committed Ledger coordinates;
the completion sources have private fields and cannot be deserialized. Their
existence does not prove a production recorder has called them. The current
registry starts from the exact original input admission. A legitimate tighter
resume cutoff needs an authenticated resume-anchor contract before mandatory
Driver binding; silently restoring the wider initial cutoff is forbidden.

Main module verification exited 0 in
`/tmp/kolyan-opening-consumer-main-module-v1.log`: Core 108, Ledger 28 and Runtime
65 passed, zero failed or ignored. Twenty independent cases ran for each of
Memory and reopened SQLite. Complete 40 rows were synchronized, closed and
physically reread at the macOS temporary paths
`kolyan-opening-consumer-YQHhZ4/actual.jsonl` and
`kolyan-opening-consumer-GEN6Lt/actual.jsonl`. Each backend includes seven
Uncertain, twelve NotAdmitted and one Completed observations. Lost or damaged
opening acknowledgements remain Uncertain with no new GEN; source write/read
failure, budget rejection, count cancellation and timeout prevent GEN.

The matrix uses a test-only prepared provider and manual input/request/completion
recorder fixtures. It exercises the production writer, but is not real SDK HTTP,
Core EOF, mandatory Driver or MiniMax acceptance. Existing test assertions were
not weakened. Workspace/all-target strict Clippy exited 0 in
`/tmp/kolyan-opening-consumer-main-strict-v1.log`. Root/child/resume/pump binding,
actual recorder injection, failure/retry consumers and real Host gates remain
required; this receipt does not close E2 or the whole opening increment.

Full regression of frozen Main `e78169e` subsequently exited 0 in
`/tmp/kolyan-opening-consumer-main-workspace-v1.log`: 88 result groups, 892
passed, zero failed and 67 ignored. The terminal receipt and complete log were
checked without restarting the live gate. Ignored provider-network cases were
not executed. This is the integrated foundation regression, not acceptance of
the still-unwired mandatory opening consumer.

### Mandatory Driver binding and authenticated resume deadlines

The next Runtime slice is released for implementation. Add mandatory
`ModelOpeningServices::new(Arc<dyn FactJournal>, ModelOpeningInspectionLimits)`
to Driver construction. There is no default journal, unlimited inspection
fallback or no-op binding. All model-driving start/resume/recovery paths require
`BindOpeningAttempt`; standalone Core/SDK and pure approval Deny remain separate.
Server passes the actual configured journal; Agent guard wrappers forward the
binding without dropping their asynchronous checks. Host explicitly chooses the
opening accounting policy instead of promoting its Inspect estimator to tokens.

Replace the attempt's initial-only constructor with `from_prefix` and a required
exact through-coordinate. The reader exposes the authenticated effective deadline,
its Ledger source, and the actual typed completed StepResult. Without a validated
merge, the opening cutoff must still exactly equal initial admission. A tighter
cutoff becomes effective only through a strictly validated TurnCheckpointMerged:
schema, canonical identity/digest, publication source, independently admitted
scope/ceilings, exact completed Step history and physical event ordering. Each
merge can only tighten the previously effective cutoff; an opening must exactly
match that current cutoff. Merely checking `opening <= initial` is insufficient.
The retained no-merge `initial=null/opening=1000` negative must still reject.

Approval/external/committed recovery inspect the complete bounded prefix before
hydration, external recovery or effects. Uncertain openings forbid redispatch.
Validated checkpoint restoration, merge, commit, binding, count, GEN and Core use
the same control and deadline anchor; bind only after the durable merged source.
Checkpoint history comparison consumes the reader's authenticated typed results,
not a three-field payload equality that would reject new opening references.

Recorder append returns the exact acknowledgement. It publishes ModelRequested
from that source, injects private opening references only into Core's actual
StepCompleted after validated stream EOF, then verifies the completion ack.
The Driver consumes retained typed causes, not only their Provider error string.
Count timeout is distinct from execution deadline; storage cause and failed
physical closure must both remain visible. RuntimeError gains typed Opening,
OpeningClosure and OpeningUncertain outcomes. No pre-start terminal is invented.

The Runtime worker owns Driver/recorder plus narrow reader/attempt migrations and
independent tests. Server constructor/bounds and module fixture migration form a
separate reviewed write set; Main owns Agent/Host/service entrypoints and central
integration. Existing 163 reader and 20 consumer expectations remain intact;
synthetic test providers explicitly exercise prepared/count/admitted generation,
never implement a pass-through Bind or fabricate proof. Actual Host SDK, guards,
root/child/resume/pump and public retry/failure enforcement are acceptance gates,
not consequences of this specification or a private Runtime-only test pass.
