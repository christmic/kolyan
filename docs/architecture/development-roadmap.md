# 开发路线

## Phase 0：空间初始化

- Rust workspace 和模块目录
- 架构文档、模块地图、ADR
- 协议和 Schema 目录
- AI 协作约定

## Phase 1：最小内核

- `kolyan-types`
- `kolyan-core`
- 内存中的最小 `Step`
- Step 成功/失败执行闭环
- 固定输入、模型调用、Tool 调用、结束状态

## Phase 2：基础适配器

- `kolyan-protocol-openai`
- `kolyan-protocol-anthropic`
- `kolyan-provider-openai`
- `kolyan-provider-anthropic`
- `kolyan-tools`
- `kolyan-cli`
- 最小可运行示例

## Phase 3：协作和可观测性

- `kolyan-trace`
- JSONL 事件输出
- 场景测试和 `evals`
- 面向 Codex/Claude Code 的可读状态输出

## Phase 4：Runtime 能力

- 取消、重试、限制
- 策略、权限和审批
- Session 边界
- Ledger 和持久化

## Phase 5：个人 Agent 产品

- `apps/kolyan-agent`
- 工作区和文件工具
- 个人偏好和记忆
- 日常任务和自动化

## Phase 6：多语言接入

- JSON Schema
- Python / TypeScript bindings
- HTTP / WebSocket API
