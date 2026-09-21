# 多语言边界

## 目标

Kolyan 未来支持多个 Rust 模块和多种语言，但不同语言不直接耦合 Rust 内部实现。

## 边界

```text
Rust Kernel
    ↓
Message / Event / Tool Protocol
    ↓
Python / TypeScript / Shell / External Agent
```

优先支持以下接入方式：

1. JSON / JSONL：适合本地进程、脚本和 Codex/Claude Code 协作；
2. HTTP / WebSocket：适合服务化和 UI；
3. Rust bindings：适合需要低延迟或类型安全的调用；
4. Python / TypeScript bindings：在协议稳定后再实现。

## 稳定对象

跨语言对象包括：

- `Message`
- `ToolCall`
- `ToolResult`
- `TurnEvent`
- `TurnStatus`
- `RunResult`

这些对象的字段变化必须同步更新 `protocols/`、`schemas/` 和示例；Rust struct 本身不是跨语言 API。

## 当前不做

- 不为每种语言复制一套 Agent Loop；
- 不让 Python/TypeScript 依赖 Rust 私有模块；
- 不在协议未稳定前生成大量 bindings；
- 不为了“多语言”提前引入微服务。
