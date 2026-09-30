# 模块地图

## Rust workspace

| 模块 | 职责 | 明确不负责 |
| --- | --- | --- |
| `kolyan-types` | 预留的跨模块基础类型，避免重复定义已有领域契约 | 执行流程、存储 |
| `kolyan-core` | Step、Turn、工具循环及执行边界 | Session 存储、供应商、数据库、UI |
| `kolyan-runtime` | 执行生命周期、取消、重试、限制 | 具体模型实现 |
| `crates/kolyan-server` | Server 门面、ExecutionCoordinator、ExecutionService 和 Server 核心适配器 | Turn/Step 逻辑、模型、租约实现 |
| `kolyan-model` | 中立 LLM trait、请求/响应、消息、工具及模型事件契约 | 具体协议适配、Turn 状态机 |
| `kolyan-protocol-openai` | OpenAI Responses wire protocol | Provider-neutral 抽象 |
| `kolyan-protocol-anthropic` | Anthropic Messages wire protocol | Provider-neutral 抽象 |
| `kolyan-protocol-sse` | 两套协议共用的有界字节分帧、UTF-8 和 EOF 校验 | HTTP、模型事件、完成状态推断 |
| `kolyan-provider-openai` | OpenAI 协议到 Kolyan 类型的映射 | Agent Loop |
| `kolyan-provider-anthropic` | Anthropic 协议到 Kolyan 类型的映射 | Agent Loop |
| `kolyan-tools` | Tool trait、注册、执行 | 长期记忆 |
| `kolyan-agent` | Immutable definitions, named/inline resolution and permission snapshots; execution integration is in progress | HTTP transport, SDK or sandbox internals |
| `kolyan-sandbox` | Host-admitted macOS process isolation and cleanup | Permission issuance or Agent scheduling |
| `kolyan-tool-worker` | One-shot trusted file helper with bounded typed stdin | Authorization, ambient shell execution or self-established sandbox |
| `kolyan-policy` | 权限、审批、治理策略 | 模型推理 |
| `kolyan-trace` | 执行轨迹和观测 | 作为 Ledger 权威存储 |
| `kolyan-ledger` | 执行事实、定向查询与存储适配器 | 任务调度、审批决策 |
| `kolyan-storage` | Session 历史与持久化 | Ledger 事实、Agent Loop |
| `kolyan-cli` | 一次性命令，本进程复用执行能力，无需常驻 Server | 复制核心业务规则 |
| `kolyan-integration-tests` | 跨模块、Provider 和真实网络集成测试 | 业务实现、生产运行时 |

## 应用和其他语言

| 目录 | 职责 |
| --- | --- |
| `apps/kolyan-agent` | 个人 Agent 客户端，目标通过 Server 通信；当前入口占位 |
| `apps/kolyan-tui` | 终端客户端，目标通过 Server 通信；当前入口占位 |
| `services/kolyan-server` | 常驻 Server 进程、JSON-RPC 与最小 HTTP 入口；未来扩展 WebSocket 适配 |
| `bindings/` | Python、TypeScript 等绑定 |
| `protocols/` | 跨语言事件、消息和 Tool 契约 |
| `schemas/` | 机器可读协议定义 |
| `evals/` | 场景、行为和回归评测 |

## 修改决策

先回答“这是内核规则、运行时能力、适配器还是产品行为”，再选择目录。无法归类时先更新 ADR，不直接把代码放进 `kolyan-core`。
