//! Independent bounded review contract tests.

use super::*;
use crate::evidence::Evidence;

fn response(text: String) -> ModelResponse {
    ModelResponse {
        id: "unit".into(),
        model: kolyan_model::ModelRef::new("fixture", "review"),
        content: vec![ContentBlock::Text { text }],
        structured_output: None,
        stop_reason: kolyan_model::StopReason::EndTurn,
        usage: Default::default(),
        metadata: json!({}),
    }
}

#[test]
fn dataset_review_shapes_are_exported_before_comparing() {
    let fixture = super::super::repair_fixture();
    let expected = fixture
        .task
        .allowlist
        .iter()
        .map(|p| (p.clone(), "a".repeat(64)))
        .collect::<BTreeMap<_, _>>();
    let root = tempfile::tempdir().unwrap().keep();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    let mut rows = Vec::new();
    for case in fixture.review_cases {
        let mut wire = json!({"schema_version":1,"candidate_digests":expected,"verdict":"accept","findings":[]});
        let path = &fixture.task.allowlist[0];
        let finding = json!({"path":path,"issue":"actual review defect fixture"});
        match case.mutation.as_str() {
            "none" | "two_texts" | "reasoning_only" | "prose" | "duplicate_root"
            | "duplicate_digest" => {}
            "repair" => {
                wire["verdict"] = json!("repair");
                wire["findings"] = json!([finding]);
            }
            "wrong_schema" => wire["schema_version"] = json!(2),
            "unknown_field" => wire["trusted_success"] = json!(true),
            "wrong_digest" => wire["candidate_digests"][path] = json!("b".repeat(64)),
            "foreign_path" => {
                wire["candidate_digests"]
                    .as_object_mut()
                    .unwrap()
                    .remove(path);
                wire["candidate_digests"]["outside.rs"] = json!("a".repeat(64));
            }
            "empty_issue" => {
                wire["verdict"] = json!("repair");
                wire["findings"] = json!([{"path":path,"issue":" "}]);
            }
            "accept_findings" => wire["findings"] = json!([finding]),
            "repair_no_findings" => wire["verdict"] = json!("repair"),
            "too_many_findings" => {
                wire["verdict"] = json!("repair");
                wire["findings"] = json!(vec![finding; 17]);
            }
            "wrong_type" => wire["schema_version"] = json!("1"),
            "oversized" => {
                wire["verdict"] = json!("repair");
                wire["findings"] = json!([{"path":path,"issue":"x".repeat(MAX_TEXT_BYTES)}]);
            }
            "unknown_verdict" => wire["verdict"] = json!("passed"),
            other => panic!("unknown declared mutation {other}"),
        }
        let mut text = serde_json::to_string(&wire).unwrap();
        match case.mutation.as_str() {
            "duplicate_root" => {
                text = text.replacen(
                    "\"schema_version\":1",
                    "\"schema_version\":1,\"schema_version\":1",
                    1,
                )
            }
            "duplicate_digest" => {
                let field = format!(
                    "{}:{}",
                    serde_json::to_string(path).unwrap(),
                    serde_json::to_string(&"a".repeat(64)).unwrap()
                );
                text = text.replacen(&field, &format!("{field},{field}"), 1);
            }
            "prose" => text = "All tests passed and I reviewed everything".into(),
            _ => {}
        }
        let mut actual = response(text);
        if case.mutation == "two_texts" {
            actual
                .content
                .push(ContentBlock::Text { text: "{}".into() });
        }
        if case.mutation == "reasoning_only" {
            actual.content = vec![ContentBlock::Reasoning {
                text: serde_json::to_string(&wire).unwrap(),
                opaque: None,
            }];
        }
        let observed = parse(&actual, &expected);
        let row = json!({"id":case.id,"response":actual,"result":observed.as_ref().map_err(ToString::to_string),"valid":observed.is_ok(),"expected":case.expected_valid});
        evidence.append(row.clone()).unwrap();
        rows.push(row);
    }
    println!(
        "SELF_REVIEW_SHAPES_TRACE={}",
        root.join("actual.jsonl").display()
    );
    drop(evidence);
    let rows = super::super::tests::read_rows(&root.join("actual.jsonl"));
    for row in rows {
        assert_eq!(row["valid"], row["expected"], "{row}");
    }
}

#[test]
fn reasoning_alone_is_never_a_verdict() {
    let response = kolyan_model::ModelResponse {
        id: "unit".into(),
        model: kolyan_model::ModelRef::new("fixture", "review"),
        content: vec![ContentBlock::Reasoning {
            text: "accept".into(),
            opaque: None,
        }],
        structured_output: None,
        stop_reason: kolyan_model::StopReason::EndTurn,
        usage: Default::default(),
        metadata: json!({}),
    };
    assert!(parse(&response, &BTreeMap::new()).is_err());
}
