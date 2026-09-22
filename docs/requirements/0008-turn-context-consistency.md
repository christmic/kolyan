# Requirement 0008：Turn 上下文与轨迹一致性

## 目标

确保一个 ToolCall 批次的执行结果能够完整、稳定、正确地进入下一次模型请求，避免出现结果丢失、错配、重复回填或半截上下文。

## 上下文结构

```text
Assistant(ToolCall[])
    ↓
ToolResult(call_id=a)
ToolResult(call_id=b)
    ↓
下一次 Model Request
```

Turn 负责维护这个上下文边界。ToolExecutor 只返回单个工具的结果，不直接修改模型消息。

## 批次结果契约

ToolCallBatch 完成执行后，必须验证：

1. 结果数量与调用数量一致；
2. 每个结果的 call_id 都属于原始批次；
3. 每个 call_id 只能出现一次；
4. ToolResult.call_id 必须与调度记录一致；
5. 每个原始 ToolCall 都有成功结果或错误结果；
6. 验证通过前不能请求下一次模型；
7. 并行结果按原始 ToolCall 顺序进入上下文。

## 回填规则

Turn 统一执行以下操作：

1. 追加模型产生的 Assistant ToolCall 消息；
2. 按批次顺序追加每个 ToolResult 消息；
3. 将完整上下文交给下一次 Step；
4. 下一次 Step 只能读取已经完整回填的批次。

FailTurn 发生错误时，当前批次不向下一次模型请求提交半截上下文；ContinueBatch 将失败调用转换为 is_error=true 的 ToolResult，并与其他结果一起提交。

## 测试要求

### 单元测试

- 空批次；
- 重复 ToolCall ID；
- 未知 ToolResult ID；
- ToolResult 内部 ID 不匹配；
- 多个成功结果完整回填；
- ContinueBatch 的失败结果参与回填；
- FailTurn 不产生下一次模型请求。

### 真实测试

使用并行文件写入场景，数据驱动地验证：

- 模型产生两个独立 ToolCall；
- 两个工具均执行成功；
- 下一次模型请求包含两个完整 ToolResult；
- 文件结果、Turn 事件轨迹和最终结果均满足契约；
- 覆盖所有配置模型和 OpenAI/Anthropic 两种协议。
