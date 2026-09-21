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
