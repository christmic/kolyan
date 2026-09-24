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
