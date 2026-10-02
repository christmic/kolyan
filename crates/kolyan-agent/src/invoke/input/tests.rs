//! Strict decode evidence only; grants, instance admission and execution are not simulated.

use std::fs;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    pointer: Option<String>,
    value: Value,
    remove: bool,
    inline: bool,
    repeat: usize,
    path: Option<String>,
    #[serde(default)]
    components: Vec<String>,
    reason: Option<String>,
    truncated: bool,
}

#[test]
fn strict_input_paths_export_all_outcomes_then_compare_physical_jsonl() {
    let base: Value = serde_json::from_str(include_str!("../tests/prepare.json")).unwrap();
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-invoke-input-path-")
        .tempdir()
        .unwrap()
        .keep();
    let trace = directory.join("actual.jsonl");
    let mut rows = Vec::new();
    for case in &cases {
        let mut input = base["call"]["arguments"].clone();
        if case.inline {
            input["children"][0]["target"] =
                json!({"kind":"inline","value":base["definitions"][1]});
        }
        if let Some(pointer) = &case.pointer {
            if case.remove {
                let (parent, key) = pointer.rsplit_once('/').unwrap();
                input
                    .pointer_mut(parent)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .remove(key);
            } else {
                let value = if case.repeat == 0 {
                    case.value.clone()
                } else {
                    json!(case.value.as_str().unwrap().repeat(case.repeat))
                };
                let (parent, key) = pointer.rsplit_once('/').unwrap();
                input
                    .pointer_mut(parent)
                    .unwrap()
                    .as_object_mut()
                    .unwrap()
                    .insert(key.into(), value);
            }
        }
        let original = input.clone();
        let result = decode(input.clone());
        let (category, message, decoded) = match result {
            Ok(input) => ("valid", None, Some(serde_json::to_value(input).unwrap())),
            Err(InvokePrepareError::Invalid(message)) => ("invalid", Some(message), None),
            Err(error) => ("unexpected", Some(error.to_string()), None),
        };
        rows.push(json!({"case_id":case.id,"input":input,"original":original,
            "input_bytes":serde_json::to_vec(&input).unwrap().len(),
            "category":category,"message":message,"decoded":decoded,
            "meaning":"strict_decoder_unit_not_execution_evidence"}));
    }
    let contents = rows
        .iter()
        .map(|row| format!("{row}\n"))
        .collect::<String>();
    fs::write(&trace, contents).unwrap();
    eprintln!("INVOKE_INPUT_PATH_EVIDENCE={}", trace.display());
    let physical = fs::read_to_string(&trace).unwrap();
    let actual: Vec<Value> = physical
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), cases.len());
    for (case, actual) in cases.iter().zip(actual) {
        assert_eq!(actual["case_id"], case.id, "{actual}");
        assert_eq!(actual["input"], actual["original"], "{actual}");
        assert!(
            actual["input_bytes"].as_u64().unwrap() <= 64 * 1024,
            "{actual}"
        );
        if let Some(path) = &case.path {
            assert_eq!(actual["category"], "invalid", "{actual}");
            let message = actual["message"].as_str().unwrap();
            assert!(message.contains(path), "{actual}");
            for component in &case.components {
                assert!(message.contains(component), "{actual}");
            }
            assert!(message.contains(case.reason.as_ref().unwrap()), "{actual}");
            assert!(message.len() <= MAX_ERROR_BYTES, "{actual}");
            assert_eq!(message.ends_with(TRUNCATION), case.truncated, "{actual}");
        } else {
            assert_eq!(actual["category"], "valid", "{actual}");
            assert!(actual["message"].is_null(), "{actual}");
            assert_eq!(actual["decoded"], actual["input"], "{actual}");
            assert_eq!(
                actual["decoded"]["children"][0]["permissions"]["delegation"]["named_targets"],
                json!([])
            );
        }
    }
}
