//! Export every coordinate probe before comparing results, including refusals.

use std::{fs, io::Write, panic::catch_unwind};

use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    response: Value,
    expected_id: Option<String>,
}

#[test]
fn rpc_approval_coordinates_require_compound_waiting_and_exact_nonempty_id() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../fixtures/server_rpc_wait_coordinates.json"
    ))
    .unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-rpc-wait-coordinates-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = fs::File::create(root.join("actual.jsonl")).unwrap();
    let mut observations = Vec::new();
    for case in cases {
        let actual = catch_unwind(|| super::pending_approval_id(&case.response));
        let actual_id = actual.ok();
        writeln!(export, "{}", json!({"case":case.id,"response":case.response,"actual_id":actual_id,"expected_id":case.expected_id})).unwrap();
        observations.push((case.id, actual_id, case.expected_id));
    }
    export.sync_all().unwrap();
    eprintln!("RPC waiting coordinate evidence: {}", root.display());
    for (id, actual, expected) in observations {
        assert_eq!(actual, expected.map(Value::String), "{id}");
    }
}
