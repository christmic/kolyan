//! Synthetic comparison failures must retain later children and the summary.

use std::panic::{AssertUnwindSafe, catch_unwind};

use serde::Deserialize;
use serde_json::json;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    receipts: Vec<usize>,
    succeeds: bool,
}

#[test]
fn topology_exports_all_observations_before_receipt_assertions() {
    let cases: Vec<Case> = serde_json::from_str(include_str!(
        "../../../../fixtures/agent/topology_comparison.json"
    ))
    .unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-topology-comparison-")
        .tempdir()
        .unwrap()
        .keep();
    for case in cases {
        let path = root.join(format!("{}.jsonl", case.id));
        let evidence = Evidence::new(&path);
        let observations = case
            .receipts
            .iter()
            .enumerate()
            .map(|(index, &file_receipts)| InvocationObservation {
                row: json!({"event":"invocation_effect_comparison","index":index,"file_receipts":file_receipts}),
                file_receipts,
                requires_receipt: true,
            })
            .collect::<Vec<_>>();
        let summary = json!({"event":"topology_comparison","case":case.id,
            "file_receipts":case.receipts.iter().sum::<usize>(),"source":"synthetic_framework_only"});
        let result = catch_unwind(AssertUnwindSafe(|| {
            export_then_compare(&evidence, &observations, summary.clone());
        }));
        println!("TOPOLOGY_COMPARISON_TRACE={}", path.display());
        let exported = std::fs::read_to_string(&path)
            .unwrap()
            .lines()
            .map(|line| serde_json::from_str::<Value>(line).unwrap())
            .collect::<Vec<_>>();
        assert_eq!(result.is_ok(), case.succeeds, "{}", case.id);
        assert_eq!(exported.len(), observations.len() + 1, "{}", case.id);
        for (actual, observation) in exported.iter().zip(&observations) {
            assert_eq!(actual["index"], observation.row["index"]);
            assert_eq!(actual["file_receipts"], observation.file_receipts);
        }
        let actual_summary = exported.last().unwrap();
        for (key, value) in summary.as_object().unwrap() {
            assert_eq!(&actual_summary[key], value);
        }
    }
}
