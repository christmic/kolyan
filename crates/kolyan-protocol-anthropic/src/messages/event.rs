use serde::Deserialize;
use serde_json::Value;

#[derive(Debug, Clone, Deserialize)]
pub struct MessageStreamEvent {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub fields: std::collections::BTreeMap<String, Value>,
}
