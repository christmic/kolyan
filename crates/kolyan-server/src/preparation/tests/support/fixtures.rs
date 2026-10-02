//! Complete neutral requests, histories and exported Turn configuration.

use super::*;

pub(super) fn key() -> ExecutionRef {
    ExecutionRef {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    }
}
pub(super) fn text(value: &str) -> Message {
    Message {
        role: MessageRole::User,
        content: vec![ContentBlock::Text { text: value.into() }],
    }
}
pub(super) fn history() -> Vec<Message> {
    serde_json::from_value(json!([
        {"role":"user","content":[{"type":"text","text":"original goal"}]},
        {"role":"assistant","content":[{"type":"text","text":"old one"}]},
        {"role":"assistant","content":[{"type":"reasoning","text":"思考","opaque":{"signature":"actual","nested":[1,null]}}]},
        {"role":"assistant","content":[{"type":"tool_call","call":{"id":"old","name":"file.read","arguments":{"path":"safe/old"}}}]},
        {"role":"user","content":[{"type":"tool_result","result":{"call_id":"old","content":"actual old result","is_error":false}}]},
        {"role":"assistant","content":[{"type":"text","text":"old two"}]}
    ])).unwrap()
}
pub(super) fn request() -> TurnRequest {
    TurnRequest{turn_id:"t".into(),config:TurnConfig{max_steps:3,max_tool_calls:Some(2),deadline:None},
        model_request:serde_json::from_value(json!({
            "request_id":"original","model":{"provider":"fixture","model":"m"},
            "system":[{"text":"System 中文 🦀","cache":true}],
            "messages":[{"role":"user","content":[{"type":"text","text":"current first"}]},
                {"role":"user","content":[{"type":"text","text":"current complete tail"}]}],
            "tools":[{"name":"file.write","description":"Write","input_schema":{"type":"object"}}],
            "tool_choice":"auto","output_format":{"name":"reply","schema":{"type":"object"},"strict":true},
            "prompt_cache":{"breakpoints":["system","tools","messages"]},
            "reasoning":{"effort":"low","budget_tokens":8},"max_output_tokens":32,
            "extensions":{"opaque":["中文",null,{"nested":true}]}
        })).unwrap()}
}
pub(super) fn turn_value(request: &TurnRequest) -> Value {
    json!({"turn_id":request.turn_id,"model_request":request.model_request,"config":{
        "max_steps":request.config.max_steps,"max_tool_calls":request.config.max_tool_calls,
        "deadline_ns":request.config.deadline.map(|value|value.as_nanos())}})
}
