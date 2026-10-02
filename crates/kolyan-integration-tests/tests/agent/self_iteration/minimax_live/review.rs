//! Bounded final-Text verdicts, admitted receipts and current candidate digests.

mod provenance;
mod receipt_tests;
mod tests;

use std::collections::BTreeMap;

use kolyan_ledger::{LedgerEvent, LedgerEventKind};
use kolyan_model::{ContentBlock, ModelResponse, ToolResult};
use kolyan_policy::{PreparedCall, PreparedGrant, ToolExecutionScope};
use kolyan_server::AttemptBinding;
use serde::{
    Deserialize, Serialize,
    de::{self, MapAccess, Visitor},
};
use serde_json::{Value, json};

use super::{TaskPlan, baseline::Inventory};

const MAX_TEXT_BYTES: usize = 32768;

#[derive(Clone, Copy, PartialEq, Eq, Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Verdict {
    Accept,
    Repair,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Review {
    schema_version: u32,
    #[serde(deserialize_with = "unique_digests")]
    candidate_digests: BTreeMap<String, String>,
    pub verdict: Verdict,
    findings: Vec<Finding>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Finding {
    path: String,
    issue: String,
}

fn unique_digests<'de, D: serde::Deserializer<'de>>(
    d: D,
) -> Result<BTreeMap<String, String>, D::Error> {
    struct Unique;
    impl<'de> Visitor<'de> for Unique {
        type Value = BTreeMap<String, String>;
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("unique candidate path SHA-256 map")
        }
        fn visit_map<A: MapAccess<'de>>(self, mut map: A) -> Result<Self::Value, A::Error> {
            let mut result = BTreeMap::new();
            while let Some((key, value)) = map.next_entry::<String, String>()? {
                if result.insert(key, value).is_some() {
                    return Err(de::Error::custom("duplicate candidate key"));
                }
            }
            Ok(result)
        }
    }
    d.deserialize_map(Unique)
}

pub(super) fn digests(
    plan: &TaskPlan,
    inventory: &Inventory,
) -> Result<BTreeMap<String, String>, String> {
    plan.allowlist
        .iter()
        .map(|path| {
            let file = inventory
                .get(path)
                .ok_or_else(|| format!("review candidate absent: {path}"))?;
            if file.kind != "file" {
                return Err("review candidate is not a regular file".into());
            }
            Ok((path.clone(), file.sha256.clone()))
        })
        .collect()
}

/// Must be invoked only after all four fresh successful read receipts are verified.
pub(super) fn parse(
    response: &ModelResponse,
    expected: &BTreeMap<String, String>,
) -> Result<Review, String> {
    let texts = response
        .content
        .iter()
        .filter_map(|b| {
            if let ContentBlock::Text { text } = b {
                Some(text)
            } else {
                None
            }
        })
        .collect::<Vec<_>>();
    if texts.len() != 1 || texts[0].len() > MAX_TEXT_BYTES {
        return Err("review requires one bounded final Text JSON".into());
    }
    let review: Review =
        serde_json::from_str(texts[0]).map_err(|e| format!("invalid review JSON: {e}"))?;
    if review.schema_version != 1
        || expected.len() != 4
        || review.candidate_digests != *expected
        || expected.values().any(|sha| {
            sha.len() != 64
                || !sha
                    .bytes()
                    .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        })
        || review.findings.len() > 16
        || review
            .findings
            .iter()
            .any(|f| !expected.contains_key(&f.path) || f.issue.trim().is_empty())
        || (review.verdict == Verdict::Accept) != review.findings.is_empty()
    {
        return Err("review schema, digest or findings contract rejected".into());
    }
    Ok(review)
}

pub(super) fn receipt_checks(
    events: &[LedgerEvent],
    binding: &AttemptBinding,
    expected: &BTreeMap<String, String>,
) -> Vec<Value> {
    expected.iter().map(|(path, digest)| {
        let mut rejected=Vec::new();
        for event in events.iter().filter(|e| e.kind==LedgerEventKind::EffectReceipt && e.execution_id==binding.execution.execution_id && e.payload["input"]["prepared"]["call"]["arguments"]["path"]==*path) {
            match read_receipt(events,event,binding,path,digest) {
                Ok(())=>return json!({"path":path,"sha256":digest,"receipt":event,"verified":true}),
                Err(error)=>rejected.push(json!({"event_id":event.event_id,"cursor":event.cursor,"error":error})),
            }
        }
        json!({"path":path,"sha256":digest,"verified":false,"error":"missing fresh successful exact admitted model-generated read receipt","rejected":rejected})
    }).collect()
}

pub(super) fn require_receipts(checks: &[Value]) -> Result<(), String> {
    if checks.len() != 4
        || checks.iter().any(|c| c["verified"] != true)
        || checks
            .iter()
            .filter_map(|c| c["path"].as_str())
            .collect::<std::collections::BTreeSet<_>>()
            .len()
            != 4
    {
        return Err(
            "review missing fresh successful reads of all four candidates; no repair permitted"
                .into(),
        );
    }
    Ok(())
}

fn read_receipt(
    events: &[LedgerEvent],
    event: &LedgerEvent,
    binding: &AttemptBinding,
    path: &str,
    digest: &str,
) -> Result<(), String> {
    let prepared: PreparedCall = serde_json::from_value(event.payload["input"]["prepared"].clone())
        .map_err(|e| e.to_string())?;
    let scope: ToolExecutionScope = serde_json::from_value(event.payload["input"]["scope"].clone())
        .map_err(|e| e.to_string())?;
    let grant: PreparedGrant = serde_json::from_value(event.payload["prepared_grant"].clone())
        .map_err(|e| e.to_string())?;
    if prepared.call().name != "file.read"
        || prepared.call().arguments["path"] != path
        || scope.execution.session_id != binding.execution.session_id
        || scope.execution.turn_id != binding.execution.turn_id
        || scope.execution.execution_id != binding.execution.execution_id
        || event.turn_id != binding.execution.turn_id
        || scope.agent_snapshot_digest.as_deref() != Some(binding.constraints_digest.as_str())
    {
        return Err("read receipt foreign scope/path/tool".into());
    }
    provenance::verify(events, event, &prepared, &grant, &scope)?;
    let output: ToolResult =
        serde_json::from_value(event.payload["output"].clone()).map_err(|e| e.to_string())?;
    let file: kolyan_tools::FileOperationResult =
        serde_json::from_str(&output.content).map_err(|e| e.to_string())?;
    let content = file.content.as_deref().ok_or("read content missing")?;
    if output.is_error
        || output.call_id != prepared.call().id
        || file.path != path
        || file.sha256 != digest
        || file.bytes != content.len()
        || super::baseline::digest(content.as_bytes()) != digest
    {
        return Err("read output/effect/model call is not proven".into());
    }
    Ok(())
}
