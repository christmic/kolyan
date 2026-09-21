use serde::Deserialize;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Deserialize)]
pub struct Response {
    pub id: String,
    pub model: String,
    #[serde(default)]
    pub output: Vec<ResponseOutputItem>,
    pub status: String,
    #[serde(default)]
    pub usage: Option<Value>,
    #[serde(default)]
    pub incomplete_details: Option<Value>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ResponseOutputItem {
    #[serde(rename = "type")]
    pub kind: String,
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}
