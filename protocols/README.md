# Protocols

这里保存跨语言、跨进程的稳定协议说明。

协议变更需要同步更新：

1. 本目录的描述；
2. `schemas/` 中的机器可读定义；
3. `examples/` 中的样例；
4. 相关 `evals/` 或集成测试。

## Server HTTP

[最小 HTTP API v1](server-http.md) 只覆盖 Session、Turn、审批、取消与完整结果，不提供事件流；
[OpenAPI](../schemas/server-http.openapi.json) 维护机器可读形状。
HTTP 监听已实现并完成本期验收，详见 [需求 0028](../docs/requirements/0028-http-server-boundary.md)；不改变已有 stdio 执行协议。

## 模型参数表配置

配置形状见 [parameter-table.schema.json](../schemas/parameter-table.schema.json)，
语义与验收唯一来源为 [需求 0027](../docs/requirements/0027-model-request-planning.md)。
配置不是 Server RPC；由调用方装载并绑定到 Provider 实例。JSON Schema 约束形状，
RequestPlanner 进一步检查协议身份、参数注册、特性一致性和 wire binding 冲突。
