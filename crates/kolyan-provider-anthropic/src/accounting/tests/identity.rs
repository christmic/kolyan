//! Prepared binding/report diagnostics with invented endpoint secrets; no HTTP.
use super::super::*;
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    base_url: String,
    target_base_url: String,
    expected: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    forbidden_fragments: Vec<String>,
    rows: Vec<Row>,
}
fn client(base: &str) -> AnthropicClient {
    let mut config = AnthropicConfig::new("FICT-API-KEY-1");
    config.base_url = base.into();
    AnthropicClient::new(config).unwrap()
}
#[tokio::test]
async fn opaque_prepared_binding_and_reports_export_then_reload() {
    let data: Dataset = serde_json::from_str(include_str!("identity_cases.json")).unwrap();
    let fixture: Value = serde_json::from_str(include_str!("../cases.json")).unwrap();
    let request: ModelRequest = serde_json::from_value(fixture["request"].clone()).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-anthropic-opaque-prepared-")
        .tempdir()
        .unwrap()
        .keep();
    let path = dir.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for row in &data.rows {
        let host = AnthropicProvider::new(client(&row.base_url));
        let identity = host.prepare_wire(&request).unwrap().identity().clone();
        let host = host.with_count_profile(
            CountProfile::registered(identity, "fixture-count-v1".into()).unwrap(),
        );
        let prepared = host.prepare_wire(&request).unwrap();
        let mut target = host.clone();
        // Test-only replacement preserves the opaque owner so the endpoint check,
        // rather than the independent foreign-owner check, is exercised.
        target.client = client(&row.target_base_url);
        let target_identity = target.prepare_wire(&request).unwrap().identity().clone();
        let verified = prepared.verify_for(
            &target.accounting_owner,
            &target_identity,
            &target.count_profile,
        );
        let (class, error) = match verified {
            Ok(()) => ("success", None),
            Err(_) => {
                // The actual async Provider boundary must refuse before any SDK I/O.
                match target
                    .count_prepared(&prepared, Duration::from_secs(1))
                    .await
                {
                    Err(error) => (
                        "refused",
                        Some(
                            json!({"message":error.to_string(),"kind":error.kind,"phase":error.phase}),
                        ),
                    ),
                    Ok(report) => (
                        "unexpected_success",
                        Some(json!({"unexpected_report":report})),
                    ),
                }
            }
        };
        let public = json!({"identity":prepared.identity(),"target_identity":target_identity,
            "profile":host.count_profile,"target_profile":target.count_profile,
            "constructed_report_not_http_count":prepared.reported_count(0).unwrap(),
            "debug_identity":format!("{:?}",prepared.identity()),"error":error});
        let actual = json!({"case":row.id,"fixture_only":true,"neutral_request":request,
            "source_endpoint_fixture":row.base_url,"target_endpoint_fixture":row.target_base_url,
            "owner":{"same":Arc::ptr_eq(&host.accounting_owner,&target.accounting_owner)},
            "generation_body":prepared.generation_body(),"count_body":prepared.count_body(),
            "class":class,"public":public});
        writeln!(export, "{actual}").unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let actuals: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    eprintln!("opaque prepared evidence: {}", dir.display());
    assert_eq!(actuals.len(), data.rows.len());
    for (row, actual) in data.rows.iter().zip(&actuals) {
        assert_eq!(actual["case"], row.id);
        assert_eq!(actual["class"], row.expected);
        assert_eq!(actual["owner"]["same"], true);
        assert_eq!(
            actual["public"]["identity"] == actual["public"]["target_identity"],
            row.expected == "success"
        );
        let diagnostic = serde_json::to_string(&actual["public"]).unwrap();
        for secret in &data.forbidden_fragments {
            assert!(!diagnostic.contains(secret), "{} leaked {}", row.id, secret);
        }
        if row.expected == "refused" {
            assert_eq!(actual["public"]["error"]["kind"], "unsupported");
            assert_eq!(actual["public"]["error"]["phase"], "validate");
        }
    }
}
