//! Accumulate indexed wire blocks; preserve signed thinking for later requests.

use std::collections::BTreeMap;

use super::*;

#[derive(Default)]
pub(super) struct Blocks(BTreeMap<u64, (Value, String)>);

impl Blocks {
    pub(super) fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    pub(super) fn event(
        &mut self,
        event: &MessageStreamEvent,
    ) -> Result<(Option<ModelEvent>, Option<ContentBlock>), ProviderError> {
        let index = event
            .fields
            .get("index")
            .and_then(Value::as_u64)
            .unwrap_or(0);
        match event.kind.as_str() {
            "content_block_start" => {
                let block = event
                    .fields
                    .get("content_block")
                    .filter(|block| block.is_object())
                    .ok_or_else(|| protocol_error("missing content block"))?
                    .clone();
                if self.0.contains_key(&index) {
                    return Err(protocol_error("duplicate content block start"));
                }
                if block["type"] == "tool_use"
                    && (string_value(&block, "id").is_empty()
                        || string_value(&block, "name").is_empty())
                {
                    return Err(protocol_error("tool_use requires nonempty id and name"));
                }
                let mapped = (block["type"] == "tool_use").then(|| ModelEvent::ToolCallStarted {
                    id: string_value(&block, "id"),
                    name: string_value(&block, "name"),
                });
                self.0.insert(index, (block, String::new()));
                Ok((mapped, None))
            }
            "content_block_delta" => {
                let (block, arguments) = self
                    .0
                    .get_mut(&index)
                    .ok_or_else(|| protocol_error("delta without matching content block"))?;
                let delta = event
                    .fields
                    .get("delta")
                    .ok_or_else(|| protocol_error("missing content delta"))?;
                let mapped = match delta["type"].as_str() {
                    Some("text_delta") if block["type"] == "text" => {
                        let text = string_value(delta, "text");
                        block["text"] = json!(format!("{}{}", string_value(block, "text"), text));
                        Some(ModelEvent::TextDelta(text))
                    }
                    Some("thinking_delta") if block["type"] == "thinking" => {
                        let text = string_value(delta, "thinking");
                        block["thinking"] =
                            json!(format!("{}{}", string_value(block, "thinking"), text));
                        Some(ModelEvent::ReasoningDelta(text))
                    }
                    Some("signature_delta") if block["type"] == "thinking" => {
                        block["signature"] = delta["signature"].clone();
                        None
                    }
                    Some("input_json_delta") if block["type"] == "tool_use" => {
                        let partial = string_value(delta, "partial_json");
                        arguments.push_str(&partial);
                        Some(ModelEvent::ToolCallArgumentsDelta {
                            id: string_value(block, "id"),
                            delta: partial,
                        })
                    }
                    Some(
                        "text_delta" | "thinking_delta" | "signature_delta" | "input_json_delta",
                    ) => {
                        return Err(protocol_error("delta does not match content block type"));
                    }
                    _ => None,
                };
                Ok((mapped, None))
            }
            "content_block_stop" => {
                let (block, arguments) = self
                    .0
                    .remove(&index)
                    .ok_or_else(|| protocol_error("stop without matching content block"))?;
                let content = match block["type"].as_str() {
                    Some("text") => ContentBlock::Text {
                        text: string_value(&block, "text"),
                    },
                    Some("thinking" | "redacted_thinking") => ContentBlock::Reasoning {
                        text: string_value(&block, "thinking"),
                        opaque: Some(block),
                    },
                    Some("tool_use") => {
                        let arguments = if arguments.is_empty() {
                            block["input"].clone()
                        } else {
                            serde_json::from_str::<Value>(&arguments).map_err(|error| {
                                protocol_error(format!("invalid tool arguments: {error}"))
                            })?
                        };
                        if !arguments.is_object() {
                            return Err(protocol_error("tool arguments must be an object"));
                        }
                        let call = ToolCall {
                            id: string_value(&block, "id"),
                            name: string_value(&block, "name"),
                            arguments,
                        };
                        return Ok((
                            Some(ModelEvent::ToolCallCompleted(call.clone())),
                            Some(ContentBlock::ToolCall { call }),
                        ));
                    }
                    _ => return Ok((None, None)),
                };
                Ok((None, Some(content)))
            }
            _ => unreachable!("only content block events are routed here"),
        }
    }
}
