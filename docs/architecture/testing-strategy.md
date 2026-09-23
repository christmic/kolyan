# 测试策略

## 分层

```text
模块单测
  → 各 crate 的 src/**/*.rs
  → MockProvider / 纯函数 / 状态和映射逻辑

跨模块集成测试
  → crates/kolyan-integration-tests/tests/
  → Provider 兼容性、Step、未来 Turn/Session

真实网络回归测试
  → 集成测试中的 #[ignore] 用例
  → 项目内配置 + 环境变量注入 API Key
```

## 集成测试目录约定

```text
kolyan-integration-tests/
├── tests/
│   ├── provider/       # OpenAI / Anthropic 协议兼容性
│   ├── core/           # Step 等核心模块的跨 crate 测试
│   ├── common/         # 配置加载、fixture、断言
│   ├── fixtures/       # 请求和期望数据
│   └── config/         # Provider 地址、模型和 API key 环境变量名
```

测试配置可以提交，但只允许提交 endpoint、端口、模型、能力和环境变量名；API Key 只能通过环境变量提供，禁止写入仓库。

## 命名和职责

- `kolyan-model`、`kolyan-core` 等模块的局部逻辑测试放在模块内部；
- Provider 真实兼容测试放在 `tests/provider/`；
- Step 真实调用测试放在 `tests/core/step.rs`；
- 所有真实网络用例必须显式 `#[ignore]`，普通 workspace 测试不能访问网络；
- 已设置 API Key 的 live test 遇到 Provider 或聚合错误必须失败，不能静默跳过；
- 缺少某个 Provider 的 API Key 时，只跳过该 Provider 的矩阵行。

## 当前真实测试

- `provider/openai_compat.rs`：OpenAI-compatible 请求、流、Tool、Structured Output、Prompt Cache；
- `provider/anthropic_compat.rs`：Anthropic-compatible 请求、流、Tool、Structured Output、Prompt Cache；
- `core/step.rs`：使用 MiniMax 两种协议真实执行同一组 Step fixtures。
- `core/turn.rs`：覆盖单步、事件流、多步工具循环、批次/并行工具、文件读写、结束原因和全模型双协议矩阵；`core/durable_approval.rs` 覆盖真实审批恢复、拒绝和过期。

## Turn 变更验收规则

涉及 Turn 主流程、事件、工具批次、审批、取消、deadline 或预算的变更，必须同时完成：

1. 模块单测：验证状态机、错误和边界条件；
2. 确定性跨 crate 测试：使用手工构造 Provider/Tool 输入验证编排轨迹；
3. 真实网络矩阵：执行 `core_turn` 和受影响的 `durable_approval`，覆盖配置中的所有模型与 OpenAI/Anthropic 两种协议；
4. 在提交说明或项目记忆中记录命令、覆盖范围、耗时和结果。真实矩阵未执行时，不得宣称 Turn 测试全面通过。

取消、超时、预算上限和多审批等无法稳定由真实模型自然触发的分支，必须由确定性跨 crate 测试覆盖；真实矩阵仍需验证新增配置不破坏正常模型闭环。

## 最近一次 Turn 验证记录

2026-09-23，在项目根目录通过 zsh 登录环境执行：

```text
cargo test -p kolyan-integration-tests --test core_turn -- --ignored --nocapture
结果：8 passed，覆盖全部配置模型 × OpenAI/Anthropic 两种协议，622.42s

cargo test -p kolyan-integration-tests --test durable_approval -- --ignored --nocapture
结果：2 passed，覆盖审批恢复、拒绝和过期，92.18s

cargo test -q
结果：普通 workspace 测试通过；真实网络用例仍只通过 --ignored 显式执行
```

本次 Turn 新增的多审批、工具预算、deadline 和取消传播由 `kolyan-core` 的
确定性测试覆盖；真实模型矩阵用于验证这些 Turn 配置不破坏正常闭环。
