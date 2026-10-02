//! Read-only revision APIs through actual file and merged tool-set adapters.
#![cfg(target_os = "macos")]

mod support;

use std::io::Write;

use serde::Deserialize;
use serde_json::Value;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    action: String,
    changed: bool,
    read_error: bool,
}

#[tokio::test]
async fn revision_matrix_exports_closes_and_rereads_before_comparison() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("adapter_revision_tests/cases.json")).unwrap();
    let output = tempfile::Builder::new()
        .prefix("kolyan-adapter-revision-")
        .tempdir()
        .unwrap()
        .keep();
    let path = output.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let row = support::run(case).await;
        serde_json::to_writer(&mut file, &row).unwrap();
        writeln!(file).unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    println!(
        "adapter revision actual rows: {} at {}",
        rows.len(),
        path.display()
    );
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["case_id"], case.id);
        assert_eq!(row["before"], row["after"], "{} effects", case.id);
        assert_eq!(row["clone_revision"], row["revision"], "{} clone", case.id);
        if case.read_error {
            assert_eq!(row["after_revision"], Value::Null);
            assert_eq!(row["error"], "io_not_found");
            let expected = if case.action == "merged_worker_removed" {
                "failed"
            } else {
                "io_not_found"
            };
            assert_eq!(row["prepare_error"]["kind"], expected);
            assert!(
                row["prepare_error"]["message"]
                    .as_str()
                    .unwrap()
                    .contains("No such file")
            );
        } else {
            assert_eq!(row["error"], Value::Null);
            assert_eq!(row["prepare_error"], Value::Null);
            assert_eq!(
                row["revision"] != row["after_revision"],
                case.changed,
                "{} change",
                case.id
            );
            assert_eq!(
                row["prepared"]["tool_revision"], row["after_revision"],
                "{} prepare",
                case.id
            );
            if !row["merged_expected"].is_null() {
                assert_eq!(
                    row["merged_expected"], row["after_revision"],
                    "{} actual merged instance",
                    case.id
                );
            }
        }
    }
}
