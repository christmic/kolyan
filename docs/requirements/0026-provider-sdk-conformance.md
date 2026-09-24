# Provider 官方 SDK 对照整改

## 范围与验收

继续 R1，依据本地官方 SDK 的请求类型、序列化、流聚合和真实请求证据修复，
不根据模型文本猜测服务端内部实现。原始网络证据只保存在临时目录，不提交。
不降低 Schema 校验，不把普通错误改成跳过，不重试已输出事件的模型执行。

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

## 验证记录

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
uv run --python 3.12 --with httpx --with jsonschema \
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

真实 Rust Provider 矩阵完成：44 passed、13 failed、19 skipped、0 not-run；
13 个失败均为原生 Schema 契约，组合与官方 SDK 对照完全一致。19 个 skipped 是
既有配置声明不支持显式缓存的测试，未新增跳过条件。
证据：`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-YGQuJ5/`；
日志：`/tmp/kolyan-provider-sdk-r1.log`。此轮包含流签名/工具身份修复，后续字段映射
补充由新增无网络测试覆盖。十步工具回放尚在执行；未切换结构化模式、未放宽校验。
原生端点不遵守 Schema 仍是外部限制。提示词辅助＋本地校验属于另一个显式
兼容策略，不能冒充原生 constrained decoding；默认行为保持原生。
