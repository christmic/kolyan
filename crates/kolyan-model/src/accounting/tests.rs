use super::*;
use serde::Deserialize;
use serde_json::json;
use std::{io::Write, sync::Arc};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    mode: String,
    expected: String,
}

#[test]
fn immutable_accounting_binding_exports_before_compare() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-accounting-binding-")
        .tempdir()
        .unwrap()
        .keep();
    let mut file = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    let request: ModelRequest = serde_json::from_value(
        json!({"request_id":"fixture","model":{"provider":"fixture","model":"m"},
        "system":[],"messages":[],"tools":[],"tool_choice":"auto","extensions":null}),
    )
    .unwrap();
    for row in &rows {
        let owner = Arc::new(());
        let identity = MappingIdentity::new(
            endpoint_identity("http://localhost:1", "responses/v1"),
            ContextProtocol::OpenAiResponses,
            request.model.clone(),
            "mapping-v1".into(),
            "coverage-v1".into(),
        )
        .unwrap();
        let profile = if row.mode == "unsupported" {
            CountProfile::default()
        } else {
            CountProfile::registered(identity.clone(), "counter-v1".into()).unwrap()
        };
        let coverage = CountCoverage::new(if row.mode == "coverage" {
            vec!["future".into()]
        } else {
            vec![]
        });
        let mut prepared = PreparedContextWire::new(
            owner.clone(),
            identity.clone(),
            profile.clone(),
            &request,
            json!({"model":"m","input":"中文🦀","stream":true}),
            json!({"model":"m","input":"中文🦀"}),
            coverage,
        )
        .unwrap();
        let mut target = identity.clone();
        let mut target_profile = profile.clone();
        let mut target_owner = owner.clone();
        match row.mode.as_str() {
            "foreign" => target_owner = Arc::new(()),
            "endpoint" => {
                target.endpoint_id = endpoint_identity("http://localhost:2", "responses/v1")
            }
            "protocol" => target.protocol = ContextProtocol::AnthropicMessages,
            "model" => target.model.model = "other".into(),
            "mapping" => target.mapping_revision = "mapping-v2".into(),
            "coverage_revision" => target.coverage_revision = "coverage-v2".into(),
            "profile" => {
                target_profile =
                    CountProfile::registered(identity.clone(), "counter-v2".into()).unwrap()
            }
            "body" => prepared.generation_body["input"] = json!("tampered"),
            "count_body" => prepared.count_body["input"] = json!("tampered"),
            "size" => prepared.generation_wire_bytes += 1,
            _ => {}
        }
        let result = prepared.verify_for(&target_owner, &target, &target_profile);
        let class = if result.is_ok() { "success" } else { "refused" };
        let error = result.err().map(|error| error.to_string());
        let report = if class == "success" {
            Some(serde_json::to_value(prepared.reported_count(0).unwrap()).unwrap())
        } else {
            None
        };
        let actual = json!({"case":row.id,"class":class,"error":error,"identity":identity,
            "neutral_request":request,"profile":profile,"target_identity":target,"target_profile":target_profile,
            "owner":{"same":Arc::ptr_eq(&owner,&target_owner)},
            "prepared":{"neutral_digest":prepared.neutral_digest(),"generation_wire_digest":prepared.generation_wire_digest(),
                "generation_wire_bytes":prepared.generation_wire_bytes(),"count_input_digest":prepared.count_input_digest(),"coverage":prepared.coverage()},
            "body":prepared.generation_body(),"count_body":prepared.count_body(),"report":report});
        writeln!(file, "{}", actual).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let all: Vec<serde_json::Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(all.len(), rows.len());
    eprintln!("accounting evidence: {}", dir.display());
    for (row, actual) in rows.iter().zip(&all) {
        assert_eq!(actual["case"], row.id);
        assert_eq!(
            actual["class"],
            row.expected,
            "{} {}",
            row.id,
            dir.display()
        );
        if row.expected == "success" {
            assert_eq!(actual["report"]["input_tokens"], 0);
            assert_eq!(actual["report"]["source"], "provider_reported");
            assert!(actual["report"].get("trusted").is_none());
            assert!(actual["report"].get("assurance").is_none());
        }
    }
}
mod bounds;
