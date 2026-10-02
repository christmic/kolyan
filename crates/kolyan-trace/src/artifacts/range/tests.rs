use std::fs::{self, File};
use std::io::Write;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;
use crate::Retention;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    bytes: Vec<u8>,
    offset: u64,
    length: u64,
    cap: u64,
    returned_cap: u64,
    fault: Option<Fault>,
    expected: Expected,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Fault {
    Missing,
    CorruptLast,
    Grow,
    Shrink,
    ReferenceLength,
    InvalidDigest,
    Symlink,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    bytes: Option<Vec<u8>>,
    eof: Option<bool>,
    verified: Option<u64>,
    error: Option<String>,
    message: Option<String>,
}

fn observe(case: &Case) -> Value {
    let directory = tempfile::tempdir().unwrap();
    let store = ArtifactStore::new(directory.path(), 64).unwrap();
    let mut reference = store.put(&case.bytes, Retention::Required).unwrap();
    let mut downgraded = reference.clone();
    downgraded.retention = Retention::Optional;
    let path = directory.path().join(&reference.digest);
    match case.fault {
        None => {}
        Some(Fault::Missing) => fs::remove_file(&path).unwrap(),
        Some(Fault::CorruptLast) => {
            let mut bytes = case.bytes.clone();
            *bytes.last_mut().unwrap() ^= 1;
            fs::write(&path, bytes).unwrap();
        }
        Some(Fault::Grow) => {
            let mut bytes = case.bytes.clone();
            bytes.push(0);
            fs::write(&path, bytes).unwrap();
        }
        Some(Fault::Shrink) => fs::write(&path, &case.bytes[..case.bytes.len() - 1]).unwrap(),
        Some(Fault::ReferenceLength) => reference.byte_length -= 1,
        Some(Fault::InvalidDigest) => reference.digest = "../escape".into(),
        Some(Fault::Symlink) => {
            #[cfg(unix)]
            {
                let outside = directory.path().join("outside");
                fs::write(&outside, &case.bytes).unwrap();
                fs::remove_file(&path).unwrap();
                std::os::unix::fs::symlink(outside, &path).unwrap();
            }
            #[cfg(not(unix))]
            panic!("symlink matrix requires Unix");
        }
    }
    // Every row exercises a fresh store, including its durable Required pin.
    let reopened = ArtifactStore::new(directory.path(), 64).unwrap();
    let result = reopened.read_range(
        &reference,
        case.offset,
        case.length,
        ArtifactRangeLimits {
            max_verified_bytes: case.cap,
            max_returned_bytes: case.returned_cap,
        },
    );
    let result = match result {
        Ok(range) => json!({"range":range}),
        Err(error) => {
            let kind = match &error {
                ArtifactError::Invalid(_) => "invalid",
                ArtifactError::Integrity => "integrity",
                ArtifactError::Required => "required",
                ArtifactError::Io(_) => "io",
            };
            json!({"error":kind,"message":error.to_string()})
        }
    };
    let removal = reopened.remove(&downgraded);
    json!({"case":case.id,"reference":reference,"input_bytes":case.bytes,
        "offset":case.offset,"length":case.length,"cap":case.cap,
        "returned_cap":case.returned_cap,"actual":result,
        "required_removal_refused":matches!(removal, Err(ArtifactError::Required))})
}

#[cfg(unix)]
#[test]
fn complete_integrity_then_raw_ranges_exported_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-artifact-range-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    let mut export = File::create(&path).unwrap();
    for case in &cases {
        writeln!(export, "{}", observe(case)).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("ARTIFACT_RANGE_TRACE={}", path.display());
    let rows: Vec<Value> = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["case"], case.id);
        assert_eq!(row["input_bytes"], json!(case.bytes));
        assert_eq!(row["required_removal_refused"], true, "{}", case.id);
        let actual = &row["actual"];
        if let Some(error) = &case.expected.error {
            assert_eq!(actual["error"], *error, "{}: {actual}", case.id);
            assert!(actual.get("range").is_none());
            if let Some(message) = &case.expected.message {
                assert!(
                    actual["message"].as_str().unwrap().contains(message),
                    "{}: {actual}",
                    case.id
                );
            }
        } else {
            let range = &actual["range"];
            assert_eq!(
                range["bytes"],
                json!(case.expected.bytes.as_ref().unwrap()),
                "{}",
                case.id
            );
            assert_eq!(range["offset"], case.offset);
            assert_eq!(range["total_bytes"], case.bytes.len() as u64);
            assert_eq!(range["verified_bytes"], case.expected.verified.unwrap());
            assert_eq!(range["eof"], case.expected.eof.unwrap());
        }
    }
}
