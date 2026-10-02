use std::io::Write;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    operation: FileOperation,
    observed_content: String,
    mutation: String,
    read_limit: usize,
    write_limit: usize,
    expected: String,
}

#[test]
fn worker_receipt_matrix_exports_complete_stdout_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let directory = tempfile::Builder::new()
        .prefix("kolyan-file-receipt-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    println!("FILE_RECEIPT_TRACE={}", path.display());
    for case in &cases {
        let receipt = FileOperationResult {
            path: case.operation.path().into(),
            bytes: case.observed_content.len(),
            sha256: format!("{:x}", Sha256::digest(case.observed_content.as_bytes())),
            content: matches!(case.operation, FileOperation::Read(_))
                .then(|| case.observed_content.clone()),
        };
        let mut value = serde_json::to_value(receipt).unwrap();
        match case.mutation.as_str() {
            "none" | "duplicate_field" | "malformed_json" | "invalid_utf8" => {}
            "wrong_bytes" => value["bytes"] = json!(case.observed_content.len() + 1),
            "wrong_digest" => value["sha256"] = json!("0".repeat(64)),
            "wrong_path" => value["path"] = json!("other"),
            "missing_content" => {
                value.as_object_mut().unwrap().remove("content");
            }
            "extra_content" => value["content"] = json!("unexpected"),
            "uppercase_digest" => {
                value["sha256"] = json!(value["sha256"].as_str().unwrap().to_uppercase())
            }
            "unknown_field" => value["secret_extra"] = json!("rejected"),
            "negative_bytes" => value["bytes"] = json!(-1),
            "overflow_bytes" => value["bytes"] = json!("18446744073709551616"),
            other => panic!("unknown receipt mutation {other}"),
        }
        let stdout = match case.mutation.as_str() {
            "duplicate_field" => format!(
                "{{\"bytes\":0,{}",
                &serde_json::to_string(&value).unwrap()[1..]
            )
            .into_bytes(),
            "malformed_json" => b"{broken".to_vec(),
            "invalid_utf8" => vec![0xff],
            "overflow_bytes" => serde_json::to_string(&value)
                .unwrap()
                .replace("\"18446744073709551616\"", "18446744073709551616")
                .into_bytes(),
            _ => serde_json::to_vec(&value).unwrap(),
        };
        let limits = FileOperationLimits {
            max_read_bytes: case.read_limit,
            max_write_bytes: case.write_limit,
        };
        let result = decode(&case.operation, &stdout, limits);
        let (kind, error) = match result.as_ref().err() {
            None => ("passed", Value::Null),
            Some(IsolatedFileError::Uncertain(reason)) => ("uncertain", json!({"reason":reason})),
            Some(IsolatedFileError::Invalid(reason)) => ("invalid", json!({"reason":reason})),
            Some(other) => ("unexpected", json!({"reason":other.to_string()})),
        };
        writeln!(file,"{}",json!({"case":case.id,"source":"synthetic_zero_exit_worker_stdout",
            "operation":case.operation,"stdout":stdout,"limits":{"read":case.read_limit,"write":case.write_limit},
            "kind":kind,"result":result.as_ref().ok(),"error":error})).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let physical = std::fs::read_to_string(&path).unwrap();
    let rows: Vec<Value> = physical
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["case"], case.id);
        assert_eq!(row["kind"], case.expected, "{}: {}", case.id, row);
        let bytes: Vec<u8> = serde_json::from_value(row["stdout"].clone()).unwrap();
        let result = decode(
            &case.operation,
            &bytes,
            FileOperationLimits {
                max_read_bytes: case.read_limit,
                max_write_bytes: case.write_limit,
            },
        );
        if case.expected == "passed" {
            assert_eq!(
                serde_json::to_value(result.unwrap()).unwrap(),
                row["result"]
            );
        } else {
            assert!(row["result"].is_null());
            assert!(!row["error"]["reason"].as_str().unwrap().is_empty());
        }
    }
}
