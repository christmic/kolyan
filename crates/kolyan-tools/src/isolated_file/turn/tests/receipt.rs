use std::io::Write;

use serde_json::{Value, json};

use super::*;

#[test]
fn unverifiable_committed_receipt_stays_uncertain_at_turn_boundary() {
    let directory = tempfile::Builder::new()
        .prefix("kolyan-file-receipt-boundary-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    println!("FILE_RECEIPT_BOUNDARY_TRACE={}", path.display());
    let cases: Vec<Value> = serde_json::from_str(include_str!("receipt_cases.json")).unwrap();
    for case in &cases {
        let kind = case["expected"].as_str().unwrap();
        let reason = case["reason"].as_str().unwrap().to_string();
        let input = match case["input_kind"].as_str().unwrap() {
            "uncertain" => IsolatedFileError::Uncertain(reason),
            "invalid" => IsolatedFileError::Invalid(reason),
            other => panic!("unknown boundary kind {other}"),
        };
        let error = tool_error(input);
        writeln!(file, "{}", json!({"case":kind,"input":case,"error":error})).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["input"], *case);
        let error: ToolError = serde_json::from_value(row["error"].clone()).unwrap();
        let (kind, message) = match error {
            ToolError::Uncertain { message } => ("uncertain", message),
            ToolError::Failed { message } => ("failed", message),
            other => panic!("unexpected receipt boundary {other}"),
        };
        assert_eq!(kind, case["expected"].as_str().unwrap());
        assert!(message.contains(case["reason"].as_str().unwrap()));
    }
}
