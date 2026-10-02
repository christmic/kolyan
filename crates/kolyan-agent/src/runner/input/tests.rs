//! Archive boundaries are data driven; actual evidence is exported first.

use super::*;
pub(in crate::runner) mod refusals;
use serde_json::json;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    bytes: usize,
    accepted: bool,
}

#[test]
fn source_archives_preserve_large_inputs_and_refuse_host_overflow() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/archive.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-agent-input-archives-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    println!("AGENT_INPUT_ARCHIVE_TRACE={}", path.display());
    let mut output = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let root = tempfile::tempdir().unwrap();
        let store = ArtifactStore::new(root.path(), MAX_INPUT_DOCUMENT_BYTES as u64).unwrap();
        let value = json!({"input":"x".repeat(case.bytes)});
        let result = archive(&store, &value);
        let row = match result {
            Ok(reference) => {
                let reopened =
                    ArtifactStore::new(root.path(), MAX_INPUT_DOCUMENT_BYTES as u64).unwrap();
                let actual = reopened
                    .read(&reference, MAX_INPUT_DOCUMENT_BYTES as u64)
                    .unwrap();
                let decoded: serde_json::Value = serde_json::from_slice(&actual).unwrap();
                json!({"id":case.id,"accepted":true,"reference":reference,"roundtrip":decoded==value,"bytes":actual.len()})
            }
            Err(error) => json!({"id":case.id,"accepted":false,"error":error.to_string()}),
        };
        writeln!(output, "{row}").unwrap();
        output.sync_all().unwrap();
    }
    let actual = std::fs::read_to_string(path).unwrap();
    assert_eq!(actual.lines().count(), cases.len());
    for (line, case) in actual.lines().zip(cases) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert_eq!(row["id"], case.id);
        assert_eq!(row["accepted"], case.accepted, "{row}");
        if case.accepted {
            assert_eq!(row["roundtrip"], true);
            assert_eq!(row["reference"]["retention"], "required");
        } else {
            assert!(row["error"].as_str().unwrap().contains("host ceiling"));
        }
    }
}
