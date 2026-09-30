# Schemas

这里保存 JSON Schema、OpenAPI 等机器可读契约。

当前包含 [Provider 模型参数表](parameter-table.schema.json) 配置契约；
以及 [最小 HTTP API OpenAPI](server-http.openapi.json) 契约（本期验收已完成，见
[需求 0028](../docs/requirements/0028-http-server-boundary.md)）。
其他跨进程协议按对应需求逐步补充，不以 Rust 类型存在代替 Schema 已落地。
