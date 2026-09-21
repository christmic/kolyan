# Model Provider 设计

## 分层

```text
kolyan-core
    ↓
kolyan-model
    ├── kolyan-provider-openai
    │       ↓
    │   kolyan-protocol-openai
    └── kolyan-provider-anthropic
            ↓
        kolyan-protocol-anthropic
```

`kolyan-model` 不依赖 HTTP、供应商 SDK 或具体模型。协议 crate 只表达官方 wire format；Provider crate 负责归一化。

## 目录约定

```text
kolyan-model/src/
  types.rs message.rs tool.rs event.rs provider.rs capability.rs catalog.rs usage.rs error.rs

kolyan-protocol-openai/src/
  client.rs config.rs request.rs error.rs sse.rs responses/{request,response,event,stream}.rs

kolyan-protocol-anthropic/src/
  client.rs config.rs request.rs error.rs sse.rs messages/{request,message,content,event,stream}.rs

kolyan-provider-openai/src/
  provider.rs mapping.rs capabilities.rs error.rs

kolyan-provider-anthropic/src/
  provider.rs mapping.rs capabilities.rs error.rs
```

## 调用边界

Provider 以流为核心调用接口；完整响应由公共聚合器从流事件构造，避免维护两套执行路径。

```text
ModelProvider::stream(ModelRequest)
    → ModelEvent
    → aggregate
    → ModelResponse
```

## 官方协议映射

### OpenAI Responses

- `ModelRequest.system` → `instructions`；
- `Message` → `input` items；
- `ToolCall` → `function_call` output item；
- `ToolResult` → `function_call_output` input item；
- SSE → `ModelEvent`；
- `response.completed` / `response.failed` → 终止事件。

### Anthropic Messages

- `ModelRequest.system` → 顶层 `system`；
- `Message` → `messages`；
- `ToolCall` → `tool_use` content block；
- `ToolResult` → `tool_result` content block；
- `thinking` → `Reasoning`；
- SSE → `ModelEvent`；
- `message_stop` → 终止事件。

## 状态保留

Provider-specific 的 reasoning 签名、加密内容、响应 ID 和原始事件不能被核心解释，但需要通过 opaque metadata 保留，以便下一次请求正确回放。
