# Agent 调用准备错误分类契约

## 状态与范围

状态：主控已审查并集成；独立模块及受影响跨层回归通过，真实模型回归待终态。基线为 `3043095`。

本需求是阶段性增强方案 0031 的 E1 支撑切片，不是完整 Agent 目标的替代。
0031 当前唯一来源在主仓库 `docs/requirements/0031-agent-evolution-program.md`；
独立 worktree 的基线尚未包含该文档，不在此复制其内容。
[0030](0030-governed-agent-execution.md) 继续拥有权限、恢复及剩余真实验收要求。

仅修改 `kolyan-agent` 的 `runner/routing.rs` 和新增独立测试、数据。
不修改 Core/Step/context、协议 schema、权限交集、默认错误策略或旧测试断言。
本批不启动模型、不提交或推送；实施范围已由主控释放。

## 已核实问题

`RoutedTools::prepare` 对 `prepare_agent_invocation` 的所有错误调用
`delegation::denied`，统一生成 `ToolError::PolicyDenied`。严格字段解析失败、
不存在的确切 revision、inline 内容冲突和 Prepared 完整性错误因此失去分类。

`InvokePrepareError::Invalid` 本身也不是纯粹的模型输入类别：它同时承载
宿主 limits、父 binding 校验、执行 Session 不符和调用内容错误。
不能直接将全部 Invalid 改成普通反馈，也不能根据错误字符串判断来源。

Core 已支持准备失败的 `ContinueBatch`：以原 call ID 回填 `is_error=true`，
并保留预算计费。`Uncertain`、`Cancelled`、`TimedOut`、`InvalidBatch` 始终致命。
本批修正错误归因，不新增另一条重试或执行循环。

## 分类方案

路由先检查可信宿主上下文，再调用现有纯准备函数，最后按错误 enum 映射。
新增 helper 私有化；不修改供其他真实授权路径使用的 `delegation::denied`。

### 宿主前置检查

复用 `InvokePrepareLimits::validate`、`AgentInvocationBinding::validate`、
现有 execution identity 校验和 `AgentPermissions::validate`，不复制其规则。
这些可信状态无效时返回 `InvalidBatch`，不能作为模型可纠正输入反馈。
宿主 execution Session 与 saved binding 不符属于所有权拒绝，保持
`PolicyDenied`。缺少 delegation 配置仍是宿主未开放该能力，保持原拒绝。

前置检查不读取或写入新的事实、不创建实例、不重新选择父 definition，
不构造 grant。现有纯准备函数仍执行原校验，前置检查只是提供明确错误来源。

### 准备错误映射

| 已验证宿主前提下的 typed cause | Core 分类 | 契约 |
| --- | --- | --- |
| `InvokePrepareError::Invalid` | `Failed` | 字段类型、未知/缺失字段、空/超限输入、子数或完整准备字节超限；保留原原因 |
| `AgentError::Invalid` | `Failed` | 调用提供的目标身份或 requested permissions 无效；不得转换类型 |
| `AgentError::NotFound` | `Failed` | 请求的确切 revision 不存在；不回退 latest 或其他 revision |
| `AgentError::Conflict` | `Failed` | inline 与已注册确切 revision 内容冲突；不能覆盖 catalog |
| `AgentError::PermissionDenied` | `PolicyDenied` | named/inline/self 禁止或 requested permissions 扩大，拒绝不变 |
| `AgentError::SnapshotMismatch` | `InvalidBatch` | 冻结宿主身份/快照完整性问题，不可普通纠错 |
| `AgentError::Capacity` | `InvalidBatch` | 纯准备不注册 definition，此路径不应产生容量错误；显式失败而非隐藏异常 |
| `PreparedError::Invalid` / `BindingMismatch` | `InvalidBatch` | Prepared 契约或完整性失败，保留致命语义 |
| `PreparedError::Denied` | `PolicyDenied` | 保留 typed 拒绝；当前构造 PreparedCall 不签发 grant，该分支不冒称已有正常产生路径 |

使用 exhaustive match，不按 Display 文本或 Debug 解析分类。
现有 `ToolError` 没有独立 InvalidInput 变体，本批使用 `Failed` 承载可纠正准备错误，
消息保留 `InvokePrepareError` 原有来源前缀，不扩展跨 crate 错误协议。
路由渲染后的 cause（含来源前缀、不含 Core Display）最多 1280 UTF-8 字节，
超过时保留显式 ` [truncated]`；不会缩短已有 1024 字节解析载荷及其来源前缀。
不存在确切目标是此调用的解析失败，不等于整个 `agent.invoke` 工具不可用；
因此不使用仅携带工具名称、丢失解析原因的 `Unavailable`。

PreparedCall 对空或超长原生 call ID 的既有拒绝仍属于 `InvalidBatch`。
本批不扩大、收紧或重新定义 Provider-native call ID 的边界。

## 必须保留的不变量

- strict Serde、完整 schema、字段定位及 1024 字节 UTF-8 错误载荷界限不变；
  该界限不含既有 Display 前缀及 ToolResult 封装。
- 不把空字符串转换成数组，不填默认权限，不裁剪权限扩大，不修写模型参数。
- 失败准备没有 child instance、准入、私有上下文初始化、grant、工具执行或效果。
  诊断事件与 Core 原有预算计费不属于外部效果，不要求其消失。
- `FailTurn` 默认不变；只有宿主原先明确选择 `ContinueBatch` 才有普通错误反馈。
- 动态 Policy 拒绝仍由 Core 原有规划路径决定；执行 scope、digest、grant、
  当前权限重验证及审批路径不改。
- 模型纠正必须是后续真实新调用，不能重放失败调用或依据自然语言宣称成功。
- 环境工具的 prepare/execute 仍委托原 executor，本批只处理 `agent.invoke`。

## 新增数据驱动验收

新增独立模块测试文件及 strict typed JSON cases，生产父文件只声明测试模块。
原 `invoke`、`routing`、`error_feedback` 场景、输入及断言保持不变。

1. 分类数据：named_targets 空字符串、tools 对象、错误布尔、未知/缺失字段、
   非法目标身份、空/超限 input、子数超限、缺失 revision、inline 冲突、合法空权限。
2. 权限对照：named 未授权、inline/self 关闭、requested 权限扩大均为
   `PolicyDenied`；合法且当前权限交集允许的输入仍能准备。
3. 宿主与完整性：损坏 binding、无效宿主 limits/permissions/execution identity、
   foreign Session 和 Prepared typed 错误分别符合上表；不把这些拒绝改成合法输入。
4. 实际链路：通过真实 RoutedTools 与现有 Turn/Runner 链，比较事件类别及下一
   Provider 请求的原 call ID、原错误原因、`is_error`；比较失败调用的零实例、
   零 grant、零执行、预算计费。保留 FailTurn 和致命错误停止对照。

全部案例先执行、导出完整 actual JSONL 并同步完成，再从物理文件读取比较。
记录 fixture ID、原调用、typed 分类、错误文本、请求、事件及效果观察；
不只检查 `.is_err()` 或“包含某字符串”。原输入执行前后须相等。
合成 Provider 只证明确定性本地链路，不冒称真实模型纠正或 OS 执行验收。

## 验证与交付

释放后执行新增聚焦测试、Agent 模块回归、受影响的原错误反馈测试、严格
all-target Clippy、格式及源码布局检查，报告实际终态、计数与临时轨迹路径。
需要跨 crate 或真实 Provider 验收时由主控协调 integration-tests 的独立范围，
不得在本模块添加反向依赖或重复启动模型矩阵。

完成本批仅表示准备错误分类得到验证；0030 剩余目标、0031 goal/host/context
主线及真实模型可靠性仍须各自的证据，不能凭此切片宣称完整 Agent 已验收。

## 独立 worktree 验证回执

基线回归在修复前导出全部 21 行，退出 101：`named-targets-string` 实际类别
为 `PolicyDenied` 而非要求的 `Failed`，字段路径及原解析错误完整保留。
日志 `/tmp/kolyan-agent-errors-baseline-v3.log`，实际轨迹
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-agent-preparation-errors-X2yr5c/actual.jsonl`。
先前 v2 还暴露新测试装配中两份 Policy revision 不一致；仅修新测试装配，
其失败日志和轨迹仍保留，不据此改变生产授权校验。

最新冻结源码执行 `cargo test -p kolyan-agent -- --nocapture` 退出 0，
模块 97 项通过，doc-test 0 项；日志 `/tmp/kolyan-agent-errors-agent-final.log`。
新增两个测试函数：路由/实际 Turn 27 行、纯 typed mapper 6 行。
后者明确不证明实际宿主准入；两者均全量导出后读取物理文件比较。
实际 JSONL 分别为临时目录 `kolyan-agent-preparation-errors-NxPCh4` 和
`kolyan-agent-preparation-errors-r0Jig8` 下的 `actual.jsonl`，父目录与上述基线相同。

`cargo clippy -p kolyan-agent --all-targets -- -D warnings` 退出 0，日志
`/tmp/kolyan-agent-errors-strict-final.log`。`cargo fmt -p kolyan-agent --check`、
`git diff --check`、`bash scripts/check-source-layout.sh` 均退出 0。
独立 target 为 `/private/tmp/kolyan-agent-errors-target`；原测试与数据文件未改。
未运行模型、未提交推送，也未将本地门禁称为主控整仓或真实模型验收。

### 完整事件补充回执

主控审阅后，仅增强本批新增测试：保留原 `events` 格式及全部比较，另记录
`full_events`。独立 `preparation_error_tests/events.rs` 对每个 TurnEvent 和
Completed 的每个 TurnOutcome 穷尽匹配，所有原字段使用 typed Serde 值；
完整事件不以 Debug 替代 response、call、error、wait 或 outcome。
原合成 Provider 响应独立记录到 `responses`，全部案例导出后从物理 JSONL
回读 StepResult 并比较完整 ModelResponse；ToolCallRequested 的完整调用与
实际响应及原调用对照。不改变 fixture、旧格式断言或生产分类实现。

聚焦两个新测试退出 0，日志 `/tmp/kolyan-agent-errors-full-events-focused-v2.log`；
全 Agent 回归退出 0，97 项通过、doc-test 0 项，日志
`/tmp/kolyan-agent-errors-full-events-agent.log`。最新实际 JSONL 为上述临时父目录
下 `kolyan-agent-preparation-errors-M7gsat/actual.jsonl`（27 行，含完整事件）及
`kolyan-agent-preparation-errors-qF3vU1/actual.jsonl`（6 行分类数据）。
严格 all-target Clippy 退出 0，日志 `/tmp/kolyan-agent-errors-full-events-strict.log`；
格式、diff 与源码布局检查退出 0。源码重新冻结，仍待主控独立集成验收。

### 主控集成回执

Main `0f4f053` 加 0032/0033 已审查源码差异，主控独立运行 Core/Agent 全量测试
退出 0：101/97 项通过。日志 `/tmp/kolyan-evolution-integrated-core-agent-v1.log`。
新增 27 行路由/Turn 观察及 6 行 mapper 观察重新全量导出后比较，实际 JSONL
为临时目录 `kolyan-agent-preparation-errors-Uo5jcK` 与
`kolyan-agent-preparation-errors-Va1LK6` 下的 `actual.jsonl`；以日志对应行作为
具体函数与文件的绑定，不按输出顺序猜测。

受影响六个集成 target 退出 0：69 passed、0 failed、32 ignored；日志
`/tmp/kolyan-evolution-cross-layer-v1.log`。Core/Agent all-targets 严格 Clippy
退出 0：`/tmp/kolyan-evolution-integrated-strict-v1.log`。
ignored 网络场景没有算作通过。MiniMax root/审批重建正在独立回归，日志
`/tmp/kolyan-evolution-minimax-root-approval-v1.log`；终态另记，不覆盖旧失败矩阵。
