# 测试策略

## 分层

```text
模块单测
  → 各 crate 的 src/**/*.rs
  → MockProvider / 纯函数 / 状态和映射逻辑

跨模块集成测试
  → crates/kolyan-integration-tests/tests/
  → Provider 兼容性、Step、Turn；Session 后续加入

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
- `core/turn_resume.rs`：18 个数据驱动恢复/取消/预算/错误场景，加取消竞态、已完成工具事件保留和拒绝批次无开始事件测试；真实矩阵覆盖多审批与普通工具交替，以及审批挂起后外部取消。
- `runtime/boundary.rs`：使用真实文件 Ledger，关闭并重开 Runtime，验证收据重放不重复执行，以及持久取消阻止新的准入。
- `DurableTurnDriver` 真实集成覆盖实际 Core `TurnExecutor`、真实文件 Ledger、Step/Terminal 边界持久化和重开后的 Ledger replay。

2026-09-23 的 Runtime v0 验证：

```text
cargo test --workspace
结果：全部通过；Runtime 2、Ledger 3、新 Runtime 集成测试 2。

cargo clippy --workspace --all-targets -- -D warnings
结果：通过。

cargo test -p kolyan-integration-tests --test runtime_boundary
结果：2 passed；使用真实文件 Ledger，重开 Runtime 后不重复执行已完成 Effect，
并验证持久取消在新 Effect 到达前生效。
```

## Turn 变更验收规则

涉及 Turn 主流程、事件、工具批次、审批、取消、deadline 或预算的变更，必须同时完成：

1. 模块单测：验证状态机、错误和边界条件；
2. 确定性跨 crate 测试：使用手工构造 Provider/Tool 输入验证编排轨迹；
3. 真实网络矩阵：执行 `core_turn` 及受影响的 `durable_approval`、`turn_resume`、`policy_authorization`，覆盖配置中的所有模型与 OpenAI/Anthropic 两种协议；
4. 在提交说明或项目记忆中记录命令、覆盖范围、耗时和结果。真实矩阵未执行时，不得宣称 Turn 测试全面通过。

取消、超时、预算上限和多审批等无法稳定由真实模型自然触发的分支，必须由确定性跨 crate 测试覆盖；真实矩阵仍需验证新增配置不破坏正常模型闭环。

`turn_resume` 的完整 live 矩阵要求所有配置的 API key 均可用，缺失则失败，不能把部分矩阵报告为全部通过。核心取消竞态用内存控制端口验证，不要求额外引入数据库。

区分执行器重建与进程崩溃：当前审批测试序列化 checkpoint 后在同一测试进程中重建执行器，验证无执行器内存依赖；不将其称为真实进程崩溃恢复。

## 0015 Turn 验证记录（收尾前基线）

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

## 0016 Turn 边界与恢复收尾验证

2026-09-23，项目根目录执行。真实测试加载用户 shell 环境中的凭据，不记录
凭据值；MiniMax/Qwen 两组凭据均存在，未设置模型过滤器。

```text
./scripts/check.sh
结果：格式、Clippy（warnings as errors）、workspace 测试和构建检查均通过。
普通测试 76 passed；其中 Core 38、Policy 8、Storage 3，
Turn 旧契约 8、新恢复/边界测试 4（包含 18 个数据场景）。
真实网络测试不在普通测试中执行。

cargo test -p kolyan-integration-tests --test turn_resume -- --ignored --nocapture
结果：42/42 case/model/protocol 行通过，0 跳过；完整复测 337.64s。
覆盖 21 个配置的模型/协议组合 × 2 场景：多次审批与普通批次交替、挂起后取消。
此前完整运行同样 42/42 通过，330.85s。

cargo test -p kolyan-integration-tests --test durable_approval -- --ignored --nocapture
结果：2 passed，109.01s；审批恢复、拒绝、过期。

cargo test -p kolyan-integration-tests --test policy_authorization -- --ignored --nocapture
结果：1 passed，210.35s；批准范围内写入、拒绝越界写入。

cargo test -p kolyan-integration-tests --test core_turn -- --ignored --nocapture
结果：7 passed / 1 failed，545.56s；十步全模型矩阵通过。
失败项是多批次写入：qwen3.8-flash 把第一批拆成两个 Step，并重复了一次写入，
与输入要求和 JSONL 批次契约不符。未改变预期，单独复跑该项完整模型矩阵。

cargo test -p kolyan-integration-tests --test core_turn turn_multi_batch_file_writes_run_across_configured_models -- --ignored --nocapture
结果：1 passed，178.05s；该用例的全部配置模型/协议通过，无模型过滤。
7 filtered out 是本次未重复执行的其他测试函数，不是跳过模型。
因此受影响用例均取得通过结果，但不能将前一次完整运行重写为 8/8 一次通过。
```

复测中发现的问题及处理：

- 十步输入曾出现模型生成未声明的 `shell_query`、提前结束；程序正确报错，
  未跳过模型或吞掉错误。强化精确工具名、逐项进度和第十项的输入说明，
  不改变至少 11 个 Step / 10 次工具调用的断言，也不由代码生成模型调用。
- 旧审批轨迹错误地把工具开始放在审批之前，或为拒绝调用要求开始事件；
  按真实生命周期修正预期，保留原有副作用验证，并新增拒绝批次的确定性回归。
- 旧审批 runner 最多等三秒才检查模型是否进入审批，对真实网络不成立；
  改为使用 Provider 配置的超时，同时限制整个 Turn，仍不允许无限等待。
- 真实模型并不保证每次都遵循指定的批次数。上面的多批次失败保留为失败，
  不通过放宽 JSONL、代码代替模型调用或静默重试将它报告为通过；
  手动复跑的结果与原始运行分别记录。

失败轨迹保留在操作系统临时目录，文件名为
`kolyan-turn-trace-qwen-qwen3.8-flash-multi_batch-21530-1790134773117421000.jsonl`。

新恢复用例只由测试代码写入并读取临时 JSONL，再按数据契约比较。实际记录
保留请求、文本/思考增量、工具参数和结果、准入边界、审批快照及最终状态。
预期定义位于集成测试 crate 的 `tests/expected/turn/turn_resume*.jsonl`，
输入和场景定义位于该 crate 的 `tests/fixtures/turn_resume*.json`；实际文件绝对路径由
runner 输出。成功轨迹也保留在操作系统临时目录，未加入 Git。

本次未重跑独立 Provider/Step 真实套件；其代码未变，普通回归已执行。
执行器重建和内存准入验证不代表已经实现持久 Runtime 或进程崩溃恢复。
