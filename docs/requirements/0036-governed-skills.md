# 可治理 Skills 的选择性加载

## 目标与实施状态

本需求落实阶段计划 E3：模型先看到有界元数据，按精确版本调用 skill.load，
正文作为真实 ToolResult 回填到现有 Turn 的下一 Step。Skills 提供任务知识，
不能授予权限、执行脚本或创建另一套 Agent loop。规格已批准分批实现，尚未
交付；不把目录类型或局部存储测试当作完整 Skills 能力。

## 数据与存储边界

SkillKey 由 id 和 revision 组成，不支持 latest。SkillDescriptorInput 包含
精确 key、标题与描述；注册绑定 metadata 与 UTF-8 body 的摘要、字节数和
Required ArtifactRef。复用真实 FactJournal CAS 和 ArtifactStore，不另造
数据库接口。宿主 namespace 必须稳定显式配置，不扫描全局环境目录，不
下载模型指定的路径或 URL。同 key 同内容幂等，同 key 异内容冲突；描述或
正文变化必须新 revision。

SkillCatalog::register 返回真实注册事实与不可变内容引用；revoke 指定精确
key、注册 FactRef、operation_id 和有界 reason。注册、撤销、绑定采用独立
domain stream 的 schema1 critical facts，分别为 agent.skill.registered、
agent.skill.revoked、agent.skill.bound。绑定 causes 包含精确注册和实际
Agent ownership 事实；未知 schema、损坏因果链与外来 namespace 均拒绝。
事实和内容按重建后实际读取校验，不相信提交的可序列化证明。

初始硬上限为 catalog 128 个版本、一次广告 16 项、metadata 总计 64 KiB、
单正文 32 KiB、完整 ToolResult 512 KiB。配置只能收紧，不截断内容；错误
区分非法输入、容量、冲突、撤销、权限、来源、完整性和真实存储失败。

## 选择与执行契约

SkillAccessPolicy 是经过校验的宿主 ACL，按精确 AgentKey 和逻辑 Session
授权，可进一步限定 Task/invocation，默认不允许。SkillScope 精确绑定
logical_session_id、task_id、invocation_id、private_session_id 和
agent_snapshot_digest。子调用不继承父绑定，必须按自己的权限和 scope
确定集合。元数据本身也是不可信内容，长度受限，不覆盖系统指令。

discover 只读元数据，不读正文；广告作为真实工具库存的一部分，在 Root、
Child、Continuation 输入来源保存前组装。bind 绑定实际 ownership 与原
广告，不重选版本；restore_binding 精确恢复来源并检查 scope。广告和绑定
为 private-fields opaque 对象，不提供可反序列化的授权凭据。

skill.load 仅接受 skill_id、revision、content_digest 三个严格字段，不接受
路径、URL、命令或权限。新增独立 Capability::SkillRead，Effect::Read；
不借 FilesystemRead、ProcessExecute 或 AgentDelegate 授权。现有 Core policy
签发动态 grant，实际 Skill executor 验证 scope、snapshot、policy、grant
及当前 ACL，然后读取对应 Required artifact，返回精确正文和来源摘要。
正文只在实际执行选中 load 时读取，不递归加载 references，不执行脚本。

Runner 的技能路由与 environment/delegation 库存分别识别，不放宽未知工具
检查。Root、Child、Continuation 的来源契约保存必需 nullable Skill binding
字段，未配置写 null；迁移全部调用方，不添加缺字段兼容。所有 start、
resume、pump 路径必须共用装配，不仅正常 Root 有守卫。

## 修改和撤销

新输入只广告允许且未撤销版本。已保存来源不能静默换版本、删除广告或重写
正文。模型开启前及每次 load 执行前重新检查 ACL、注册和撤销；变化则明确
拒绝，恢复不重新选择。历史内容、ToolResult 和 receipt 保留，撤销不能
收回已发送内容或回滚效果。首版检查与模型 HTTP 发送不是原子事务，不声明
检查后撤销绝对阻止已进入操作；强开启准入属于需求 0034 的独立边界。

## 分批交付与所有权

A 批先完成 Agent skills catalog、不可变 metadata、ACL、绑定和重建读取，
实际使用 Journal/ArtifactStore，并补 Memory/SQLite 数据门。写集仅新增
skills.rs、skills 子树和 lib.rs 模块导出；不修改 Runner、Policy、Server 或
Provider。这只是基础切片，不宣称模型已能加载 Skill。

B 批在主控冻结具体 Runner 接线 API 后新增 SkillRead policy 能力、load
adapter、开启守卫、输入广告与三类来源验证，迁移调用方。与生产宿主、
上下文接线的交叉文件由主控串行协调，禁止整文件覆盖已集成能力。

C 批补实际 Runner 到下一 Step 的消费、真实本机工具和 MiniMax 两协议
场景。所有批次遵守统一源码布局、测试分文件与编译缓存规范。

## 验收

数据驱动覆盖幂等、冲突、重建、撤销、容量、未知 schema、损坏内容、外来
scope；未选正文零读取，选中仅对应读取，精确 UTF-8、摘要和输出边界。
真实执行必须展示 load 到下一 Step 的原文 ToolResult，再通过沙箱工具完成
明确文件后置条件。恶意正文不能扩大权限或委派，审批重建时变更拒绝且不
重放效果。完整实际观察先导出、关闭、物理回读再比较。

真实矩阵为 MiniMax 两协议与具名/内联组合，必须有实际 load、工具 receipt
及 Goal facts，不能用最终回答计通过；未发生预期交互明确失败。撤销竞态
和恶意内容先以确定性数据验证，真实模型行为另记。网络结果与离线证明
分开记录，密钥只在环境，项目本地私有配置与日志不提交。
