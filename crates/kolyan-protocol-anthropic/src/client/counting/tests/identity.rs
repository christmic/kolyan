//! Pure configuration fixtures with invented secrets; no HTTP is sent.
use crate::{AnthropicClient, AnthropicConfig};
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    base_url: String,
    target_base_url: String,
    version: String,
    target_version: String,
    api_key: String,
    target_api_key: String,
    expected_same: bool,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    forbidden_fragments: Vec<String>,
    rows: Vec<Row>,
}
fn client(base: &str, version: &str, key: &str) -> AnthropicClient {
    let mut config = AnthropicConfig::new(key);
    config.base_url = base.into();
    config.version = version.into();
    AnthropicClient::new(config).unwrap()
}
#[test]
fn opaque_endpoint_binding_exports_then_reloads_all_cases() {
    let data: Dataset = serde_json::from_str(include_str!("identity_cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-anthropic-opaque-endpoint-")
        .tempdir()
        .unwrap()
        .keep();
    let path = dir.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for row in &data.rows {
        let source = client(&row.base_url, &row.version, &row.api_key);
        let target = client(
            &row.target_base_url,
            &row.target_version,
            &row.target_api_key,
        );
        let identity = source.count_endpoint_identity();
        let actual = json!({"case":row.id,"fixture_only":true,
            "input":{"base_url":row.base_url,"version":row.version,"api_key":row.api_key},
            "target":{"base_url":row.target_base_url,"version":row.target_version,"api_key":row.target_api_key},
            "public":{"identity":identity,"repeated":source.count_endpoint_identity(),
                "clone":source.clone().count_endpoint_identity(),"target":target.count_endpoint_identity()}});
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
    eprintln!("opaque endpoint evidence: {}", dir.display());
    assert_eq!(actuals.len(), data.rows.len());
    for (row, actual) in data.rows.iter().zip(&actuals) {
        assert_eq!(actual["case"], row.id);
        let public = &actual["public"];
        for field in ["identity", "repeated", "clone", "target"] {
            let text = public[field].as_str().unwrap();
            assert_eq!(text.len(), 64);
            assert!(
                text.bytes()
                    .all(|byte| byte.is_ascii_hexdigit() && !byte.is_ascii_uppercase())
            );
        }
        assert_eq!(public["identity"], public["repeated"]);
        assert_eq!(public["identity"], public["clone"]);
        assert_eq!(
            public["identity"] == public["target"],
            row.expected_same,
            "{}",
            row.id
        );
        let diagnostic = serde_json::to_string(public).unwrap();
        for secret in &data.forbidden_fragments {
            assert!(!diagnostic.contains(secret), "{} leaked {}", row.id, secret);
        }
    }
}
