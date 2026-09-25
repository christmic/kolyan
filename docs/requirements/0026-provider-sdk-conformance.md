# Provider 官方 SDK 对照整改

## 严格协议收尾（当前实施范围）

用户要求以官方机制为准，不保留猜测性的兼容修复。本节优先于下面历史验证记录。

- 移除 synthetic completion 和从任意增量/Markdown/子串恢复 JSON 的路径。
  OpenAI 仅在收到终态时形成结果；若 completed.output 缺失，按官方
  ResponseStreamState 使用按 output_index 保存的 output_item.done，而非拼凑文本。
- SSE EOF 不自行产生事件；参照 SDK 丢弃未以空行提交的帧，由上层判断是否缺终态。
  保留严格 UTF-8；去除 SDK 没有的隐式 4MiB 协议限制。支持 LF/CRLF/CR、id/retry。
- HTTP 解压由 HTTP 层执行，覆盖 gzip/deflate；不得将压缩字节直接交给 SSE。
- 模型输出 JSON 解析/Schema 校验单独标为 InvalidOutput/Validate，不能标为 Decode。
  Provider 的输出契约校验与 SDK create 的传输成功分别统计。
- 诊断脚本不能提前 response.read()；在 SDK 原生 HTTP 客户端的字节流上旁路记录，
  保留编码及分块证据。增加本地官方 SDK 的同数据、同故障差分，而不是只对比文本。
- 验收：无网络回归、官方 SDK 差分、全模型真实 Provider 矩阵与工具回归。
  厂商违反输出契约必须保留失败，不删除模型、放宽断言或注入提示词。

### 分层与验收边界

1. HTTP/SSE：HTTP 解压后才解码 SSE；分块不改变 UTF-8/帧边界语义。
   首个致命错误终止流，已经成功解码的前序事件仍交付。Anthropic 的 ping/
   未知事件按 Messages SDK 忽略；API error 不冒充 JSON 解码错误。
2. Provider：聚合 Responses/Messages 到中立 ModelEvent。必须收到协议终态，
   EOF 不等于完成；收到完成或错误后不再等待下一次网络事件/连接关闭。
   这属于 Kolyan 的执行契约，不能用 SDK create 允许遍历到 EOF 推导模型已完成。
3. 输出校验：SDK create 不负责验证业务 JSON Schema；测试脚本额外执行的
   json.loads/jsonschema 与 Kolyan OutputValidator 对照。其失败只能说明本次
   返回内容不满足请求契约，不能描述成“官方 SDK 解码失败”。

“对齐”限于已实现的 Responses/Messages 请求映射、流解析和聚合机制，不表示
复制整个 SDK 的其他 API、Python 类型宽容性、HTTP 连接池或默认重试策略。
差分固定重试为 0，隔离单次请求行为；既有可配置连接重试策略本轮未扩展。
模型内容错误和已输出的流不能通过自动重放掩盖。

### 严格版本验收记录（2026-09-25）

- HTTP/SSE 差分框架以 `tests/fixtures/sdk_transport.json` 为唯一场景数据：
  10 个场景 × 两套协议 × identity/gzip/deflate × Content-Length/chunked，
  共 120/120 通过。覆盖单字节分块、Unicode、CR、多行、JSON/UTF-8 错误、
  未结束帧、API error、忽略事件、HTTP 截断、超时和 429。
  同一个本地 HTTP 服务先喂官方 SDK，再喂 Rust；事件顺序与错误分类直接对比，
  不通过实际厂商的随机输出作为传输层断言。
- 重新以官方默认 HTTP 客户端抓取 38 份真实响应（19 组合 × 两种 Schema），
  38 次传输成功；测试层 Schema 校验 12 成功、26 失败。捕获不提前 read()，
  记录原始压缩字节及读取分块长度；凭据不进入证据文件。
- 38/38 原始响应回放通过：请求字段、文本内容和 Schema 接受/拒绝结果一致。
  回放复制 HTTP 状态、Content-Type、Content-Encoding 和数据读取分块；不重现
  原请求的延迟和 HTTP chunk 帧，后两者分别由故障差分场景覆盖。
- 新增回归确保大于 4MiB 的合法 SSE 不被私有上限拒绝、id/retry 状态、
  先交付有效前缀再报错、InvalidOutput/Validate 分类、终态后不再轮询网络。
- 最终统一检查通过：152 passed / 0 failed / 29 ignored；格式、Clippy、构建通过。
  ignored 不计为已执行。日志 `/tmp/kolyan-strict-check-final.log`。
- 终态轮询修复后的全模型工具身份真实矩阵：19 passed / 0 failed / 0 skipped，
  耗时 32.24 秒；日志 `/tmp/kolyan-strict-tools-final.log`，轨迹目录
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-j4kS3W/`。
- 严格 Provider 全矩阵：44 passed / 13 failed / 19 skipped / 0 not-run，
  耗时 337.91 秒。13 行全部是 InvalidOutput/Validate，没有 Decode 失败；
  失败组合与官方 SDK 捕获的 Schema 失败组合一致。19 个 skip 均为原配置声明
  不支持显式缓存，未新增排除或放宽断言。日志 `/tmp/kolyan-strict-live-matrix.log`；
  逐行请求、流和状态见
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-BQRdd3/`。
  此矩阵在后续“终态后不再等待网络”与非法 response 类型防 panic 修复前启动；
  后两项由独立单测、最终全仓库检查和 38 份原始响应回放补验。
- 十步工具闭环真实回归通过：未设置模型过滤，覆盖全部 19 个配置组合，
  耗时 501.94 秒。日志 `/tmp/kolyan-strict-turn.log`；沿用既有轨迹契约，未修改
  输入、预期或断言。此运行同样在上述两个边界补丁前启动，不能当作补丁后的
  整套重跑；补丁后的证据范围如上明确列出。

证据（临时目录，不提交）：

- SDK 原始响应：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-reference-ifcxfzoo/`。
- HTTP/SSE 差分：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-transport-k19s5h7j/`。
- 真实响应回放（含终态轮询/非法类型修复）：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-replay-X6Rf8R/`。

复现传输差分（无真实模型调用）：

```sh
uv run --python 3.12 \
  --with /Users/christmix/OraculoSpace/openai-python \
  --with /Users/christmix/OraculoSpace/anthropic-sdk-python \
  crates/kolyan-integration-tests/tests/provider/sdk_transport.py
```

## 范围与验收

继续 R1，依据本地官方 SDK 的请求类型、序列化、流聚合和真实请求证据修复，
不根据模型文本猜测服务端内部实现。原始网络证据只保存在临时目录，不提交。
不降低 Schema 校验，不把普通错误改成跳过，不重试已输出事件的模型执行。

调用前必须保留多厂商/多模型的参数表筛选边界，规则与实现状态以
[Model Capabilities](../architecture/model-capabilities.md#调用前参数规划) 为唯一来源。
下文“不能映射时拒绝”指实际进入 Provider 的无效请求，并非要求把厂商不支持的
可选参数一律传入；可省略参数应在此前按配置剔除。

1. OpenAI 缓存保留参数使用 `prompt_cache_retention`。官方 SDK 也存在
   `prompt_cache_options.ttl`，但该字段接受 `30m`，不是 `in_memory/24h`。
   Message 断点位于 content block，值为 `mode: explicit`，不是顶层 item 的
   `enabled: true`。无网络序列化回归覆盖保留时间及断点位置；不能映射的请求
   明确报 Unsupported，不能悄悄忽略。reasoning budget 同样不能发给 OpenAI。
2. Anthropic thinking/signature/redacted_thinking 必须随最终响应保存并原样回填；
   增量事件不是会话回放内容的替代品。参考官方 `_messages.py` 累积块实现。
3. 分别验证原始 Schema 与关闭 additionalProperties 的严格 Schema，使用官方
   SDK 对相同配置端点发请求；记录 wire request、HTTP 状态、响应体与解析事件。
   SDK 调用成功只表示传输成功，不表示 JSON 满足 Schema。
4. 明确区分官方原生协议与兼容端点行为。若引入兼容策略，必须显式配置，不能
   暗中修改请求语义或声称服务端提供了原生 Schema 强制约束。

## 本地参考

- `openai-python`：`src/openai/types/responses/response_create_params.py`、
  `response_format_text_json_schema_config_param.py`、`lib/_parsing/_responses.py`、
  `lib/_pydantic.py`（查阅版本 `69a2c1db`）。create 接口按参数发送 schema；
  parse 的模型类型转换才会补严格 Schema，不能混为一谈。
- `anthropic-sdk-python`（`0af01906`）：`src/anthropic/types/output_config_param.py`、
  `json_output_format_param.py`、`lib/streaming/_messages.py`。
- 对照脚本：`crates/kolyan-integration-tests/tests/provider/sdk_reference.py`。
  使用本地 SDK 安装路径执行，不修改 SDK 工作树；凭据从项目配置指向的环境变量读。

## 历史验证记录（严格收尾前的快照，不代表当前实现）

### 官方 SDK 对照

2026-09-25 全配置 19 个模型/协议组合 × 原始/关闭额外属性两种 Schema，
38 次请求，禁用 SDK 重试。38 次传输成功，12 次 Schema 校验通过、26 次失败。

| 协议/端点 | Schema 通过 | Schema 失败 |
| --- | ---: | ---: |
| MiniMax OpenAI | 0 | 2 |
| Qwen OpenAI（9 个模型） | 0 | 18 |
| MiniMax Anthropic | 0 | 2 |
| Qwen Anthropic（8 个模型） | 12 | 4 |

Qwen Anthropic 失败组合为 deepseek-v4-flash-0731、glm-5.3；其余六个模型两种
Schema 均通过。这里只描述观测行为，不猜测服务端内部是丢字段还是实现不支持。
因此本地 SDK 已独立复现 13 个组合不满足原生结构化契约，不能归因于 Rust 解码。

证据目录：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-reference-0b569v84/`。
每行包含 request.json、http.json、response.bin、events.jsonl、output.txt、result.json；
汇总为 summary.json。原始 Schema 少 additionalProperties，关闭版本仍失败；
官方严格模式要求见 [Structured Outputs](https://developers.openai.com/api/docs/guides/structured-outputs)。

运行：

```sh
uv run --python 3.12 --with jsonschema \
  --with /Users/christmix/OraculoSpace/openai-python \
  --with /Users/christmix/OraculoSpace/anthropic-sdk-python \
  crates/kolyan-integration-tests/tests/provider/sdk_reference.py --all-models
```

### 确定性修复

- OpenAI 缓存保留时间、消息断点、reasoning effort 请求映射；不能映射的 budget
  和断点作用域请求明确报错。工具增量 item_id 根据 added 事件映射为 call_id。
- Anthropic 按 index 累积内容块；思考、签名、redacted_thinking 保存并回填，
  最终内容按 index 排序；参数增量不能串到另一个工具调用。
- Anthropic effort 写入 output_config，未指定预算使用 adaptive；消息缓存断点落实。
- 流在首个完成/错误后结束；Anthropic 未收到 message_stop 的 EOF 报错。
- SSE 对齐 SDK 的 LF/CRLF/CR 三种换行，保留更严格的大小限制及截断错误。
- Transport 的 Open/Stream 阶段准确区分；Schema 错误仅报告位置，不回显用户值。
- Anthropic message_delta 的累计 input/cache/output 用量覆盖已有值，省略字段保留
  起始值；OpenAI 文件 URL 与 Base64 分别使用 file_url/file_data，思考摘要不只取首段。
- OpenAI error SSE 为失败而非元数据；refusal 的增量、最终文本和终态均保留。
  两套协议的工具调用缺失身份或工具名时拒绝，不能交给 Turn 执行。

真实 Rust Provider 矩阵完成：44 passed、13 failed、19 skipped、0 not-run；
13 个失败均为原生 Schema 契约，组合与官方 SDK 对照完全一致。19 个 skipped 是
既有配置声明不支持显式缓存的测试，未新增跳过条件。
证据：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-YGQuJ5/`；
日志：`/tmp/kolyan-provider-sdk-r1.log`。此轮包含流签名/工具身份修复，后续字段映射
补充由新增无网络测试覆盖。十步工具回放已通过，覆盖所有 19 个配置组合，耗时
524.80 秒，日志 `/tmp/kolyan-sdk-turn-replay.log`；该运行覆盖核心流与思考回放修复，
后续 refusal/工具身份校验由定向测试与全模型工具矩阵补验。未切换结构化模式、未放宽校验。
原生端点不遵守 Schema 仍是外部限制。提示词辅助＋本地校验属于另一个显式
兼容策略，不能冒充原生 constrained decoding；默认行为保持原生。

### 原始响应差分回放

新增 `sdk_replay` 验证捕获的 38 份真实响应，使用回环 HTTP 服务送入实际
Kolyan protocol/provider 路径，不重新请求厂商。逐份核对：

- HTTP 层实际序列化请求与官方 SDK 请求一致（仅规范化显式 auto 和 system 字符串/
  单文本块这两种官方等价形式）；不是只检查 ModelRequest。
- 所有文本增量拼接结果与官方 SDK 输出逐字相同。
- Schema 校验结果一致：12 成功、26 被正确拒绝。拒绝不计为真实能力通过，
  这里只验证两种实现处理同一证据的一致性。

38/38 差分通过，最终回放目录
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-replay-G5FniO/`；
日志 `/tmp/kolyan-provider-final-replay.log`。请求与流文件由测试代码保存，生产 Step
不参与文件 I/O。运行时设置 KOLYAN_SDK_EVIDENCE 为 SDK 证据目录，再执行
`cargo test -p kolyan-integration-tests --test sdk_replay -- --ignored --nocapture`。

全仓库统一检查通过：145 passed / 0 failed / 27 ignored（本次检查时的快照，
之后新增一个显式 ignored 的全模型工具回归入口，不增加离线通过数）；格式、Clippy、
构建均通过。日志 `/tmp/kolyan-provider-acceptance.log`。ignored 不算通过。

收尾版本的全模型工具身份真实矩阵：19 passed / 0 failed / 0 skipped，耗时
28.11 秒；证据目录 `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-tr1d9Z/`，
日志 `/tmp/kolyan-provider-tool-identity.log`。每行均有请求、完整 Step 事件流与汇总状态。

## R1–R5 收尾新增的 SDK 机制对照

MiniMax 的真实工具响应暴露了本地过早解析：`output_item.done.arguments`
为空，但后续 arguments delta 和 `response.completed.output` 包含完整 JSON。
本地官方 openai-python（69a2c1db，3.16.2）独立调用 3 次均复现该顺序。
其 `_responses.py` 累积中间事件，并以响应级完成对象确定最终结果；未声明 strict
的工具也不会在该中间事件强制做 JSON 参数解析。

整改：中间 item.done 保留为 Provider 事件；在合法 response.completed 中严格校验
最终工具身份和 JSON 对象，再发布 ToolCallCompleted。没有补空对象、修 JSON 或
合成完成。新增离线回归及真实 Server 审批恢复场景覆盖此事件顺序。

另外，官方 `IncompleteDetails.reason` 为 Optional，且包含 max_messages、steered。
合法 response.incomplete 缺少原因或带其他原因时映射为中立 Incomplete，而不是
协议解码失败或 FinalAnswer；缺少响应身份/输出数组的畸形响应仍拒绝。

诊断脚本：`tests/provider/sdk_tool_reference.py`（位于 integration-tests crate），
使用官方本地 SDK，禁用自动重试，记录脱敏 HTTP 信息、实际请求、原始响应和事件。
三次独立诊断不是重试通过阈值。证据目录：
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-sdk-tool-s7j9b17x/`。
首例实际 max_output_tokens 为配置的 1024，输出 42 tokens、状态 completed；
不是 token 截断导致的空参数，不能靠增加预算解释或修复。

本轮 Provider/Step 76 行复验仍为 44 Passed / 13 Failed / 19 Skipped / 0 NotRun；
失败均为结构化输出 schema 不匹配，未静默省略所请求的结构化约束。
证据目录：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-wfMItV/`；
日志 `/tmp/kolyan-r1-live-final.log`。这是包含最终工具参数修复的快照，
不包含随后新增的 incomplete 可选原因映射；后者另有确定性回归。
