//! Public reader reuses Runtime admission schema and never writes.
use super::*;
use kolyan_core::{ToolDispatchPolicy, TurnConfig, TurnRequest};
use kolyan_ledger::{InMemoryLedger, LedgerError, LedgerEvent, LedgerQuery, LedgerStore};
use kolyan_model::{ModelRef, ToolChoice};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    Read,
    Schema,
    Unknown,
    Missing,
    Scope,
}
#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Cap {
    Maximum,
    One,
    Zero,
    AboveMaximum,
    Exact,
    BelowExact,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    cap: Cap,
    expected: Expected,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    ExactInput,
    Refused,
}

struct Fault {
    ledger: InMemoryLedger,
    mode: Mutation,
}
impl LedgerStore for Fault {
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        let mut rows = self.ledger.query(q)?;
        for row in &mut rows {
            match self.mode {
                Mutation::Schema => row.payload["schema_version"] = json!(2),
                Mutation::Unknown => row.payload["foreign"] = json!(true),
                Mutation::Missing => {
                    row.payload
                        .as_object_mut()
                        .unwrap()
                        .remove("agent_snapshot_digest");
                }
                Mutation::Scope => row.payload["key"]["session_id"] = json!("foreign"),
                Mutation::Read => {}
            }
        }
        Ok(rows)
    }
    fn append(&self, _: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        panic!("reader writes")
    }
    fn events_after(&self, _: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        panic!("reader audits")
    }
    fn claim(&self, _: &str) -> Result<bool, LedgerError> {
        panic!("reader claims")
    }
}
#[test]
fn verified_input_public_port_is_readonly_exact_and_bounded() {
    let ledger = InMemoryLedger::default();
    let key = RuntimeTurnKey {
        session_id: "s".into(),
        turn_id: "t".into(),
        execution_id: "e".into(),
    };
    let request = TurnRequest {
        turn_id: "t".into(),
        config: TurnConfig {
            max_steps: 3,
            ..Default::default()
        },
        model_request: ModelRequest {
            request_id: "r".into(),
            model: ModelRef::new("fixture", "model"),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(30),
            extensions: json!(null),
        },
    };
    let saved = InputAdmission::new(
        key.clone(),
        &request,
        ToolDispatchPolicy::default(),
        None,
        Some("a".repeat(64)),
        &kolyan_core::TurnDeadline::capture(request.config.deadline, None).unwrap(),
    )
    .unwrap();
    saved.persist(&ledger).unwrap();
    let before = ledger.events_after(0).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-verified-input-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    use std::io::Write;
    println!(
        "VERIFIED_INPUT_TRACE={}",
        root.join("actual.jsonl").display()
    );
    let baseline = verified_execution_input(&ledger, &key, 16 * 1024 * 1024).unwrap();
    let exact = serde_json::to_vec(&baseline)
        .unwrap()
        .len()
        .max(serde_json::to_vec(&before[0]).unwrap().len());
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let mut observations = Vec::new();
    for case in cases {
        let max_bytes = match case.cap {
            Cap::Maximum => 16 * 1024 * 1024,
            Cap::One => 1,
            Cap::Zero => 0,
            Cap::AboveMaximum => 16 * 1024 * 1024 + 1,
            Cap::Exact => exact,
            Cap::BelowExact => exact - 1,
        };
        let result = verified_execution_input(
            &Fault {
                ledger: ledger.clone(),
                mode: case.mutation,
            },
            &key,
            max_bytes,
        );
        let after = ledger.events_after(0).unwrap();
        writeln!(output,"{}",json!({"case":case,"max_bytes":max_bytes,"exact_cap":exact,"before":before,"result":result.as_ref().map_err(ToString::to_string),"after":after})).unwrap();
        observations.push((case, result, after));
    }
    output.sync_all().unwrap();
    for (case, result, after) in observations {
        if matches!(case.expected, Expected::ExactInput) {
            let actual = result.unwrap_or_else(|error| panic!("{}: {error}", case.id));
            assert_eq!(actual.model_request, request.model_request);
            assert_eq!(actual.agent_snapshot_digest, saved.agent_snapshot_digest);
            assert_eq!(actual.cursor, before[0].cursor);
        } else {
            assert!(result.is_err(), "{}", case.id);
        }
        assert_eq!(after, before, "{}", case.id);
    }
}
