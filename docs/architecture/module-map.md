# 模块地图

## Rust workspace

| 模块 | 职责 | 明确不负责 |
| --- | --- | --- |
| `kolyan-types` | 消息、Tool、事件和结果类型 | 执行流程、存储 |
| `kolyan-core` | Step、最小 Agent Loop | Turn、Session、供应商、数据库、UI |
| `kolyan-runtime` | 执行生命周期、取消、重试、限制 | 具体模型实现 |
| `kolyan-model` | LLM trait 和模型适配器 | Turn 状态机 |
| `kolyan-protocol-openai` | OpenAI Responses wire protocol | Provider-neutral 抽象 |
| `kolyan-protocol-anthropic` | Anthropic Messages wire protocol | Provider-neutral 抽象 |
| `kolyan-provider-openai` | OpenAI 协议到 Kolyan 类型的映射 | Agent Loop |
| `kolyan-provider-anthropic` | Anthropic 协议到 Kolyan 类型的映射 | Agent Loop |
| `kolyan-tools` | Tool trait、注册、执行 | 长期记忆 |
| `kolyan-policy` | 权限、审批、治理策略 | 模型推理 |
| `kolyan-trace` | 执行轨迹和观测 | 作为 Ledger 权威存储 |
| `kolyan-storage` | Session、Ledger、持久化 | Agent Loop |
| `kolyan-cli` | 命令行入口 | 核心业务规则 |

## 应用和其他语言

| 目录 | 职责 |
| --- | --- |
| `apps/kolyan-agent` | 个人 Agent 产品行为和默认配置 |
| `apps/kolyan-tui` | 终端交互界面 |
| `apps/kolyan-server` | 可选 HTTP / WebSocket 入口 |
| `bindings/` | Python、TypeScript 等绑定 |
| `protocols/` | 跨语言事件、消息和 Tool 契约 |
| `schemas/` | 机器可读协议定义 |
| `evals/` | 场景、行为和回归评测 |

## 修改决策

先回答“这是内核规则、运行时能力、适配器还是产品行为”，再选择目录。无法归类时先更新 ADR，不直接把代码放进 `kolyan-core`。
