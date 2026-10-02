use std::fs::{self, File};
use std::io::Write;
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    base: Value,
    golden: Golden,
    rows: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Golden {
    canonical: String,
    hash: String,
    bytes: usize,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    path: Option<String>,
    value: Option<Value>,
    repeat: Option<String>,
    count: Option<usize>,
    max_bytes: usize,
    expect: Expected,
    message: Option<String>,
}

#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    Golden,
    Changed,
    Error,
}

#[test]
fn complete_grant_fingerprint_data_export_before_comparison() {
    let data: Dataset = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let nonce = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let directory = std::env::temp_dir().join(format!(
        "kolyan-grant-fingerprint-{}-{nonce}",
        std::process::id()
    ));
    fs::create_dir(&directory).unwrap();
    let path = directory.join("actual.jsonl");
    let mut export = File::create(&path).unwrap();
    for case in &data.rows {
        let mut input = data.base.clone();
        if let Some(pointer) = &case.path {
            let value = if let Some(repeat) = &case.repeat {
                Value::String(repeat.repeat(case.count.unwrap()))
            } else {
                case.value.clone().unwrap_or(Value::Null)
            };
            *input.pointer_mut(pointer).unwrap() = value;
        }
        let grant: PreparedGrant = serde_json::from_value(input.clone()).unwrap();
        let result = grant.fingerprint(case.max_bytes);
        let result = match result {
            Ok(hash) => json!({"hash":hash}),
            Err(error) => {
                json!({"error":error.to_string(),"invalid":matches!(error,PreparedError::Invalid(_))})
            }
        };
        writeln!(export, "{}", json!({"case":case.id,"input":input,
            "after":serde_json::to_value(&grant).unwrap(),"max_bytes":case.max_bytes,"actual":result})).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("GRANT_FINGERPRINT_TRACE={}", path.display());
    let actual: Vec<Value> = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), data.rows.len());
    let canonical = canonical::bytes(&data.base, MAX_BYTES).unwrap();
    assert_eq!(canonical, data.golden.canonical.as_bytes());
    assert_eq!(DOMAIN.len() + canonical.len(), data.golden.bytes);
    for (case, row) in data.rows.iter().zip(actual) {
        assert_eq!(row["case"], case.id);
        assert_eq!(row["input"], row["after"], "{}", case.id);
        match case.expect {
            Expected::Golden => assert_eq!(row["actual"]["hash"], data.golden.hash, "{}", case.id),
            Expected::Changed => {
                let hash = row["actual"]["hash"].as_str().unwrap();
                assert_eq!(hash.len(), 64);
                assert!(
                    hash.bytes()
                        .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
                );
                assert_ne!(hash, data.golden.hash, "{}", case.id);
            }
            Expected::Error => {
                assert_eq!(row["actual"]["invalid"], true, "{}", case.id);
                assert!(
                    row["actual"]["error"]
                        .as_str()
                        .unwrap()
                        .contains(case.message.as_ref().unwrap()),
                    "{}: {}",
                    case.id,
                    row["actual"]
                );
                assert!(row["actual"].get("hash").is_none());
            }
        }
    }
}
