# Model Capabilities

能力属于具体的 `Provider + Model`，而不是单独属于 Provider。

## 能力集合

- `text_input`
- `text_output`
- `image_input`
- `document_input`
- `tool_use`
- `parallel_tool_use`
- `structured_output`
- `reasoning`
- `prompt_caching`
- `streaming`

## 调用前参数规划

协议允许某个字段，不代表每个厂商、接入端点或模型都支持该字段。必须保留
可扩展参数表；不得把模型名单或厂商分支写进协议解析器。

```text
ModelRequest
    ↓ 解析 Provider 接入配置 + 协议 + 精确模型的参数表
请求规划（能力检查、参数筛选、默认值、值域约束）
    ↓ 仅保留允许发送的参数
Provider adapter（中立字段映射）
    ↓
Protocol client（官方 wire format）
```

### 参数表契约

- 能力集合表达“能做什么”，参数表表达“该接入下该模型允许传什么”。两者不能
  互相替代；例如支持 reasoning 不意味着同时支持 effort 和 budget_tokens。
- 配置以 Provider 接入身份、协议、模型为选择条件；同一模型通过不同协议或
  不同端点接入可以不同。应用配置负责装载，模型层负责纯请求规划，不读取环境变量。
- Provider 级默认规则与模型级覆盖合成一份有效表；精确模型规则优先，不能通过
  覆盖突破协议本身的字段约束。未知模型不自动继承“全部支持”。
- 每项描述：中立参数或命名空间扩展键、支持状态（支持/不支持/未知）、合法类型/
  值域、可选默认值、是否允许省略。禁止未经登记的任意字段透传。
- 对标记可省略的参数：不支持或未知就不传，连默认值也不注入；支持且缺省时
  才按表应用默认值。提供的值超出已声明值域应在本地报错，不偷偷截断或改写。
- 对完成任务必需的能力（如明确要求结构化输出、工具调用）：不支持时在调用前
  拒绝，不能删除 schema/tools 后仍宣称执行了原请求。是否允许降级由调用策略
  显式声明，而非协议层临时决定。
- tool_choice 直接由同一规则决定：required 被值域支持则原样发送；字段支持但只
  支持 auto 则发送 auto；字段明确不支持则按 omittable 省略。unknown 不等于
  unsupported，required 遇到未知事实时本地拒绝。字段支持与值支持分别来自
  support/schema，不维护第二份模型名单或降级开关。none 和指定工具不借此放宽。
  具体契约见需求 0027。
- 规划保留原请求，生成有效请求和参数处理记录（省略键及原因，不记录敏感值），
  便于测试和上层追踪。正常 Step 不负责将这些记录写文件。

### 与严格协议的关系

“不支持就不传”在调用前完成；Provider 对真正进入映射的无效参数保留防御校验。
严格遵循协议不意味着一律发送所有可选字段，也不意味着取消多厂商扩展点。
厂商/模型参数差异由参数表管理；响应帧、终态和输出校验不由该表随意放宽。

### 落地状态与验收

已提供 `ParameterTable`、`RequestPlanner` 和两套 Provider 的
`with_parameter_table` 入口；配置后每次 stream 都先执行生产规划器。表中支持的
reasoning/cache 参数必须与该模型的 feature 声明一致，配置矛盾会在装配时拒绝。
测试配置中的旧 capabilities 仅供既有用例断言，不能替代生产参数表。
实现与数据驱动验证见 [需求 0027](../requirements/0027-model-request-planning.md)，
包括：同模型不同接入、默认与覆盖优先级、可选参数省略、必需能力拒绝、
未知模型/参数、类型/值域，以及实际 wire request 不含被省略字段。
用本地请求捕获作确定性断言，再用真实厂商矩阵验证接入；不能只靠模型回答判断
参数是否发送，也不能用能力表把普通 Provider 失败静默跳过。

装配时由调用方读取项目配置并反序列化 `ParameterTable`，然后调用
`OpenAiProvider::new(client).with_parameter_table(table)?` 或 Anthropic 对应入口。
基础 `new(client)` 保留无厂商策略的协议适配用途（例如官方 SDK 对照），不允许
静默忽略非空 extensions。没有隐式全局表或根据模型名猜测参数的降级路径。
配置格式见 [JSON Schema](../../schemas/parameter-table.schema.json)。

模型目录只保存稳定的描述信息：模型 ID、上下文窗口、最大输出、能力和生命周期。不要在协议 SDK 中硬编码具体模型清单；模型会新增、弃用和退役。

## Provider 扩展

无法归一化的能力放在 Provider extension 中，例如：

- OpenAI `previous_response_id`、Hosted Tools、encrypted reasoning content；
- Anthropic `cache_control`、thinking signature、server tools。

扩展必须显式命名，例如 `openai.*` 或 `anthropic.*`，不允许混入通用字段。
