use super::super::*;
use super::support::*;
use kolyan_server::{GoalChecker, GoalCriterion};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{collections::BTreeSet, io::Write};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    pointer: String,
    value: Value,
    expected: bool,
}

#[test]
fn strict_predicate_and_trust_contract_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("contract.json")).unwrap();
    let root = tempfile::tempdir().unwrap().keep();
    let path = root.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let mut input = json!(predicate());
        if case.pointer == "/extra" {
            input["extra"] = case.value.clone();
        } else if !case.pointer.is_empty() {
            *input.pointer_mut(&case.pointer).unwrap() = if case.value == "LONG_FILENAME" {
                json!("a".repeat(300))
            } else {
                case.value.clone()
            };
        }
        let before = input.clone();
        let result = GoalCriterion::new(
            "goal".into(),
            "root".into(),
            checker().key().clone(),
            input.clone(),
        )
        .and_then(|criterion| checker().validate_predicate(&criterion));
        writeln!(export,"{}",json!({"case":case.id,"input":input,"before":before,"accepted":result.is_ok(),"error":result.err().map(|e|e.to_string())})).unwrap();
    }
    let revisions: [BTreeSet<String>; 4] = [
        BTreeSet::new(),
        ["".into()].into(),
        ["x".repeat(1025)].into(),
        (0..129).map(|i| format!("revision-{i}")).collect(),
    ];
    for (i, input) in revisions.iter().enumerate() {
        let result = FileWriteCommittedChecker::new(input.clone());
        writeln!(export,"{}",json!({"case":format!("invalid_trust-{i}"),"input":input,"accepted":result.is_ok(),"error":result.err().map(|e|e.to_string())})).unwrap();
    }
    let other =
        FileWriteCommittedChecker::new([REVISION.into(), "another-exact-revision".into()].into())
            .unwrap();
    writeln!(export,"{}",json!({"case":"stable_and_changed_trust","original":checker().key(),"reconstructed":checker().key(),"changed":other.key()})).unwrap();
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("file_goal_contract {}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len() + 5);
    for (case, row) in cases.iter().zip(&rows) {
        assert_eq!(row["accepted"], case.expected, "{}: {row}", case.id);
        assert_eq!(row["input"], row["before"]);
    }
    for row in &rows[cases.len()..cases.len() + 4] {
        assert_eq!(row["accepted"], false);
    }
    let last = rows.last().unwrap();
    assert_eq!(last["original"], last["reconstructed"]);
    assert_ne!(last["original"], last["changed"]);
}
