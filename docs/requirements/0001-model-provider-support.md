# Requirement 0001：OpenAI 与 Anthropic 模型 Provider

## 目标

为 Kolyan 提供 OpenAI Responses API 和 Anthropic Messages API 的 Rust 支持，并让上层 Agent Kernel 不依赖任一供应商协议。

## 范围

第一阶段支持：

- 文本输入和文本输出；
- Function Tool 定义、Tool Call、Tool Result；
- OpenAI Responses API；
- Anthropic Messages API；
- 非流式响应；
- SSE 流式响应；
- Token Usage；
- Stop Reason；
- Reasoning / Thinking；
- 图片输入；
- 请求能力校验；
- Provider 错误归一化。

第一阶段不支持：

- OpenAI Hosted Tools；
- Anthropic Server Tools；
- MCP Connector；
- Realtime / Audio；
- Batch API；
- 自动模型路由；
- Provider 会话状态托管。

## 设计约束

1. `kolyan-model` 只定义 Provider-neutral 类型和接口。
2. `kolyan-protocol-openai` 只负责 OpenAI Responses wire protocol。
3. `kolyan-protocol-anthropic` 只负责 Anthropic Messages wire protocol。
4. Provider adapter 负责两套类型之间的映射。
5. 具体模型能力属于 `Provider + Model`，不能只属于 Provider。
6. Provider 特有字段通过扩展或 opaque metadata 保留，不能静默丢失。
7. 官方 SDK 是协议参考，不作为 Rust 运行时依赖。

## 验收标准

- 可以通过统一 `ModelProvider` 接口调用两类 Provider；
- 同一个 `ModelRequest` 可以被转换为两种官方协议请求；
- 文本、Tool Call、Tool Result、Reasoning 和 Usage 可以双向映射；
- 流事件可以聚合为统一 `ModelResponse`；
- 无 API Key 时可以使用 fixture / mock 完成协议测试；
- Provider 协议错误、传输错误和能力错误可以区分。

## 当前实现状态

已完成协议客户端、SSE 流封装、Provider-neutral 模型类型、OpenAI Responses 适配器和 Anthropic Messages 适配器。适配器统一暴露 `ModelProvider::stream`，协议客户端同时提供非流式请求方法。真实网络回归测试和完整能力矩阵留在后续 fixture/evals 任务中，不在本次提交中引入 API Key。
