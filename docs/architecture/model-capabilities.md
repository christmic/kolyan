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

## 请求校验

```text
ModelRequest
    ↓ 推导请求需要的能力
ModelDescriptor.features
    ↓
缺失 → 本地 CapabilityError
具备 → 进入 Provider adapter
```

模型目录只保存稳定的描述信息：模型 ID、上下文窗口、最大输出、能力和生命周期。不要在协议 SDK 中硬编码具体模型清单；模型会新增、弃用和退役。

## Provider 扩展

无法归一化的能力放在 Provider extension 中，例如：

- OpenAI `previous_response_id`、Hosted Tools、encrypted reasoning content；
- Anthropic `cache_control`、thinking signature、server tools。

扩展必须显式命名，例如 `openai.*` 或 `anthropic.*`，不允许混入通用字段。
