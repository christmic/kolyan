//! Complete serde wire shape. Host permissions and byte limits remain preflight
//! checks; JSON Schema character lengths are not UTF-8 byte authority.

use serde_json::{Value, json};

pub(super) fn input_schema() -> Value {
    let key = json!({"type":"object","additionalProperties":false,
        "required":["definition_id","revision"],"properties":{
            "definition_id":{"type":"string"},"revision":{"type":"string"}}});
    let permissions = json!({"type":"object","additionalProperties":false,
        "description":"Inside an inline definition this is its immutable static ceiling; in child permissions this is the requested invocation ceiling. Neither is a grant. Actual authority is independently bounded by the parent, host, definition and policy.",
        "required":["tools","delegation"],"properties":{
            "tools":{"type":"array","description":"An array of environment tool names, not an object or string. Use [] for no tools. An inline definition declares a static ceiling; requested child permissions must independently fit the effective ceiling. agent.invoke is configured through delegation, not listed here.",
                "examples":[[]],"items":{"enum":["file.read","file.write","file.edit","shell"]}},
            "delegation":{"type":"object","additionalProperties":false,
                "required":["named_targets","allow_inline","allow_self"],"properties":{
                    "named_targets":{"type":"array","description":"An array of exact definition_id/revision objects for future named delegation, not the target of this call. Use [] for no named targets; never use an empty string or an object in place of the array. A static inline definition ceiling does not grant these targets to the invocation.",
                        "examples":[[]],"items":key},
                    "allow_inline":{"type":"boolean"},"allow_self":{"type":"boolean"}}}}});
    let definition = json!({"type":"object","additionalProperties":false,
        "required":["definition_id","revision","model","instructions","permissions"],
        "properties":{
            "definition_id":{"type":"string"},"revision":{"type":"string"},
            "display_name":{"type":["string","null"]},
            "model":{"type":"object","additionalProperties":false,
                "required":["provider","model"],"properties":{
                    "provider":{"type":"string"},"model":{"type":"string"}}},
            "instructions":{"type":"string"},"permissions":permissions}});
    json!({"type":"object","additionalProperties":false,
    "required":["children","parallel"],"properties":{
        "parallel":{"type":"boolean"},
        "children":{"type":"array","minItems":1,"maxItems":8,"items":{
            "type":"object","additionalProperties":false,"required":["target","input","permissions"],
            "properties":{
                "input":{"type":"string"},"permissions":permissions,
                "target":{"oneOf":[
                    {"type":"object","additionalProperties":false,"required":["kind"],
                        "properties":{"kind":{"const":"self_call"}}},
                    {"type":"object","additionalProperties":false,"required":["kind","value"],
                        "properties":{"kind":{"const":"named"},"value":key}},
                    {"type":"object","additionalProperties":false,"required":["kind","value"],
                        "properties":{"kind":{"const":"inline"},"value":definition}}
                ]}
            }}}}})
}

#[cfg(test)]
mod tests;
