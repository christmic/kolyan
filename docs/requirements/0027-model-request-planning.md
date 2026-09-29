# 多厂商模型参数表与调用前请求规划

## 目标与边界

执行 [能力设计规范](../architecture/model-capabilities.md#调用前参数规划)：将厂商、
接入协议和模型的参数差异变成配置，在 HTTP 调用前执行。协议解析不包含模型名单，
不通过删除必需能力或放宽响应校验掩盖厂商错误。

## 实现契约

- `kolyan-model` 提供可序列化 ParameterTable、参数规则与纯 RequestPlanner。
  一张表绑定一个 Provider 接入身份和协议，挂在对应客户端实例；模型必须显式登记。
  defaults 与精确模型 overrides 按键合并，单项规则整体替换。
- 规则包含支持状态、JSON Schema 类型/值域、默认值、允许省略标志。支持状态为
  supported/unsupported/unknown；未知模型拒绝，未知可选参数不默认放行。
- 中立调优参数覆盖 reasoning 的 effort/budget、cache 的 key/retention/breakpoints、
  max_output_tokens 和 tool_choice；工具定义、结构化输出和多模态输入是必需能力，
  只检查是否支持，不允许当作普通可选参数删除。非 auto 的工具选择同样不可静默删除。
- 命名空间扩展通过 `extensions.<namespace>.<name>` 登记，并声明 wire_name。
  仅登记、支持、通过类型和值域验证的扩展进入 HTTP JSON body；不允许覆盖协议保留
  字段或与另一个生效扩展映射到同一字段。此机制对应 SDK 的显式 extra_body，
  不是任意 ModelRequest.extensions 自动透传。
- 原请求不变，规划生成有效请求、扩展字段及不含参数值的处理记录。移除最后一个
  reasoning/cache 子参数后要移除整个容器，不允许适配器重新注入空对象或默认值。
- 显式 null 的扩展值与未提供区分：前者按 schema 校验后发送，不能被默认值替换。
  支持的 reasoning/cache 参数必须与模型的 feature 声明一致；矛盾配置本地拒绝。
- 两个 Provider 均提供 `with_parameter_table`，配置后所有 stream 调用自动规划，
  不能要求上层逐次记住手动调用。未配置表的直接协议适配路径保留，用于官方 SDK
  对照；其非空扩展请求必须拒绝，不能静默忽略。
- Anthropic 的 max_tokens 是协议必需字段；配置模式中必须由请求或表的受支持默认值
  提供，不能在被禁用后重新补 4096。表不能突破协议本身的约束。
- 参数处理记录作为 Provider metadata 事件暴露，无生产文件 I/O。配置/环境读取属于
  装配层；内核、规划器和协议层不读取全局配置或 API Key 环境变量。

## 验收

### 2026-09-26：tool_choice 的表驱动兼容选择

能力事实复用同一参数规则：support 表示字段是否支持，schema 表示该字段支持哪些
中立取值；模型级规则覆盖 Provider 默认，不再增加一份厂商名单。
`tool_choice` 不维护独立降级开关；有效规则中的 `support`、`schema` 和
`omittable` 是唯一事实源。当调用方请求 required 时：

1. 字段及 required 值均支持：保持 required（Anthropic 映射为 any）。
2. 字段支持、required 不支持但 auto 支持：映射为 auto。
3. 字段明确不支持，或字段支持但无 required/auto 兼容值：允许省略时不发送。
4. 不允许省略且无可用值则本地报错；不会试探性发请求后根据报错重试。

`constraints.required_max_tools` 表达 required 的请求级条件。工具数量超过该接入声明
的上限时，required 在本次请求中视为不可用，并按同一 schema 选择 auto 或省略；
规划器不删除工具定义，也不替调用方猜测应保留哪个工具。该约束仍属于精确的
Provider + 协议 + Model 能力事实，不在 Provider 适配器中硬编码模型名称。

未知状态不等同于明确不支持：required 遇到 unknown 必须本地报错，要求先补能力
事实。兼容映射只适用于 required；none 和指定工具不静默放宽。
处理记录区分 mapped_to_auto 与省略；原请求保留，适配器不得重新补回省略字段。
这属于 Provider/Model 兼容，不替 Turn 猜测业务意图。Turn 必须在同一执行中保留
调用方的中立 tool_choice，每个 Step 都重新经过同一能力表；不得在首个工具结果后
隐式改成 Auto。希望模型可结束并输出最终回答的调用方应选择 Auto；显式 Required
会持续到 Turn 的治理终态、上限、取消或失败。不能把模型拒绝执行记作通过。

验收新增 required 支持、仅 auto、字段不支持/未知、禁止省略、显式 none/指定工具
不降级，以及精确模型覆盖；两套协议捕获实际 HTTP body 验证值和字段缺失。
真实参数矩阵增加同样的兼容配置场景，不替换既有用例。

项目级已验证差异保存在 integration-tests 的 `tests/config/provider-parameters.json`：
命名 profile 只保存一次规则，绑定键包含 Provider 接入身份、协议及精确模型。
当前默认 thinking 模式下，Qwen 的 qwen3.8-max/qwen3.8-flash/qwen3.7-plus
在两套协议均明确拒绝 required/指定工具，而 auto 成功；qwen3.7-max 则接受 required。
因此只为有直接响应证据的六个组合配置 auto-only，不按厂商或模型名前缀推断。
这不是所有 reasoning 模式通用的模型属性；切换接入模式时应使用对应已验证参数表，
不偷偷关闭 thinking。Server 进程测试和 Provider 参数矩阵共用该配置装配入口。
场景预期仍在 fixture 中按 profile 定义，配置不保存 API Key 或测试断言。

2026-09-29 的 Server 真实矩阵进一步证明，Qwen OpenAI 接入下 qwen3.7-max、
deepseek-v4.1-flash、deepseek-v4-pro、deepseek-v4-flash-0731、glm-5.3 和 glm-5.2
在 required 搭配两个工具时返回“Please set one tool in required mode”，而单工具参数
矩阵接受 required；对应六个精确绑定使用 required_max_tools=1。相同模型的 Anthropic
接入不应用该约束。

结构化输出同样是精确的 Provider + 协议 + Model 特性，而不是“协议字段存在即支持”。
2026-09-30 对每个声明支持组合固定采样 3 次；OpenAI 接入下 qwen3.7-plus、
qwen3.7-max、deepseek-v4-pro、glm-5.2，以及 Anthropic 接入下 qwen3.8-max、
qwen3.8-flash、qwen3.7-plus、qwen3.7-max、deepseek-v4-pro、glm-5.2 均为 3/3。
其余实际配置组合为 0/3 或服务端明确拒绝，能力表不声明 structured_output，Planner
在 HTTP 前返回 Unsupported。采样阈值属于真实测试规格，不会让生产 Provider 重试、
修补非法 JSON 或降级为普通文本。

1. 独立模块单测：覆盖合并、精确身份、未知项、默认值、值域、必需能力、输入不变、
   命名空间、保留字段和扩展冲突；配置失败不得包含参数值。
2. 数据驱动回环集成：同一场景经两套真实 Provider/HTTP 路径，捕获请求 JSON，
   比较传入/省略字段及处理记录，验证不支持的值不会到达网络。
3. 真实配置矩阵：全部当前厂商/模型组合，通过生产参数表调用文本/工具场景，
   记录请求规划与真实事件；失败不静默跳过，缺凭据计未执行。原有测试保持独立。
4. 全仓格式、Clippy、测试和构建；已知厂商结构化能力问题仍以 0026 的记录为准，
   不能用本需求的参数筛选将这些用例假装成通过。

## 实现与配置入口

状态：已实现并完成下述验收。测试框架、场景数据、连接配置和凭据保持分离。

- `kolyan-model/src/planning.rs`：中立契约、请求规划和无值处理记录；`planning/rules.rs`
  负责配置校验；模块单测独立在 `planning/tests.rs`。
- 两套 Provider 的 `with_parameter_table(table)` 在装配时校验，stream 时自动应用。
  调用方从项目配置读取 JSON/TOML 后反序列化即可；库不扫描全局文件或环境变量。
- `schemas/parameter-table.schema.json` 描述配置结构，额外语义检查由规划器执行。
  未配置表的基础适配路径保留用于协议对照，不等于自动开启厂商参数推断。
- 协议请求类型声明 RESERVED_FIELDS，扩展不能覆盖这些字段；通过显式
  `stream_*_with_extensions` 写入 JSON body。依据本地官方 SDK `_base_client.py`
  中 extra_body/extra_json 机制，版本沿用需求 0026 的固定参考；不修改解析规则。
- 测试参数表位于 `tests/config/request-parameters.json`，场景及字段/决策预期位于
  `tests/fixtures/request_planning.json`。这是刻意禁用调优参数的验证配置，不宣称
  所有厂商实际都不支持这些参数。厂商目录与能力数据仍需由接入方显式维护。

## 验收记录（2026-09-25）

- 新增模型层单测 15 个；模型 crate 共 18/18 通过。包含 nullable 扩展、声明冲突、
  必需多模态/工具历史、工具和 schema 原样保留、公开配置 Schema 往返验证。
- 17 个数据场景展开为两协议 31 行，回环 HTTP 集成全部通过。负例检查监听端口
  没有收到连接，正例逐项比较实际 body 的存在/缺失字段及规划决策。
  证据：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-request-planning-1CiKNL/`。
- 最终代码的真实矩阵：19 组合 × 文本/工具两场景，38 passed / 0 failed /
  0 skipped / 0 not-run，87.50 秒；每行都有输入、参数表、完整事件、决策和状态。
  证据：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-c63FYr/`；
  日志 `/tmp/kolyan-planning-final-live.log`。此前首轮亦为 38/38，87.00 秒。
- 全仓统一检查 168 passed / 0 failed / 30 ignored，格式、Clippy 与构建通过；
  ignored 不算执行。日志 `/tmp/kolyan-planning-final-check.log`。最后移除模块单测
  对集成测试 fixture 的路径依赖后，另跑模型单测 18/18（仅测试调整，生产代码未变）。
- 官方 SDK HTTP/SSE 差分仍为 120/120，真实原始响应回放仍为 38/38：
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-transport-h5fxm7ig/`、
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-replay-pGRVnT/`。

本轮真实网络验收聚焦参数规划后的文本/工具请求，没有把厂商结构化输出问题计为
通过，也未重跑整个 Session/Server 真实矩阵。相关外部输出契约限制仍见需求 0026。

## tool_choice 补充验收（2026-09-26，归档于 09-28）

- 生产规划器、模型覆盖及负例：模型模块 21/21 通过；新增 3 项测试，旧断言保留。
- 实际 HTTP 回环矩阵从 31 行增加到 49 行，两协议全部通过；另有精确配置绑定测试。
  原样检查 required / Anthropic any、降级 auto、字段彻底缺失及拒绝前不发网络。
- 首轮真实矩阵 89 Passed / 6 Failed：三个 Qwen 模型 × 两协议均明确返回
  thinking 模式不允许 required/指定工具。未改解析器，依证据补精确接入 profile。
- 复验 19 组合 × 5 场景 = **95 Passed / 0 Failed / 0 Skipped / 0 NotRun**，
  181.30 秒。六个组合的 required 意图在 HTTP 之前变为 auto，其余保持 required；
  配置禁用字段的场景无 tool_choice。原始 HTTP 请求体、决策和实际输出均保存。
- 全仓检查 **192 Passed / 0 Failed / 31 Ignored**；ignored 不计入通过。
  生产代码提交 a9060dc，测试/配置提交 563d691；此处不代表架构真实矩阵全部通过。

可打开的本地证据位于被忽略的 `target/acceptance-2026-09-26/`：
`tool-choice/report.json` 与 `requests-and-results.log` 为复验，
`tool-choice-initial/` 保留初始六项失败。配置仅反映所测试端点当前默认模式，
接受参数不等于服务端永远遵守约束：MiniMax 重复写场景仍观察到发送 required 后
返回纯文本，独立保留为未通过的响应契约证据，不把它伪装成参数规划成功的工具调用。

## 单一能力事实收敛（2026-09-29）

移除与 `support/schema/omittable` 重复的降级字段。required 的 wire 选择现在完全由
有效 Provider/Model 规则推导，不允许同一事实分两处维护。模型单测 21/21、两协议
回环请求捕获 2/2、全仓检查均通过；真实矩阵 19 个接入组合 × 5 个场景 = 95/95
通过，证据目录为测试输出的 `kolyan-r1-matrix-SaAeFN`。MiniMax 的 usage 字段出现
既有统计告警，但未影响 tool_choice 的实际请求与响应验收。

09-30 在加入工具数量约束和结构化能力绑定后再次得到 95/95，证据目录
`kolyan-r1-matrix-mgLvtn`；Provider/Step 能力矩阵为 48 Passed、28 capability
Skipped、0 Failed，证据目录 `kolyan-r1-matrix-vPudq0`。
