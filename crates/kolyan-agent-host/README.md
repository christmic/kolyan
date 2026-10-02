# kolyan-agent-host

可复用的个人 Agent 宿主装配库。复用 AgentRunner、Server、Runtime、
真实协议 Provider、原生工具和持久化仓，不包含另一个模型循环。

HTTP 服务、一次性 CLI 和交互式客户端在各自入口调用本库；本库不负责
传输协议、用户鉴权或分布式租约。当前库接口已接入，产品入口仍需后续装配。

配置、历史审批拒绝、Skills 装配和验收状态统一维护在
[宿主需求与技术方案](../../docs/requirements/0035-goal-verification-and-agent-host.md)。
跨模块场景位于 `kolyan-integration-tests/tests/agent_host/`，真实模型验收
与 localhost SDK、原生 OS 验证分开，不将离线通过当作网络通过。
