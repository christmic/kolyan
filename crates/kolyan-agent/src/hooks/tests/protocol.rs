use super::super::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
};

#[derive(Deserialize)]
struct Case {
    id: String,
    phase: HookPhase,
    stdout: String,
    valid: bool,
}

#[test]
fn strict_output_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("protocol.json")).unwrap();
    let proof = tempfile::Builder::new()
        .prefix("kolyan-hook-protocol-")
        .tempdir()
        .unwrap()
        .keep();
    let path = proof.join("actual.jsonl");
    let mut file = File::create(&path).unwrap();
    for case in &cases {
        let result = HookReply::parse(case.stdout.as_bytes(), case.phase);
        writeln!(file,"{}",json!({"id":case.id,"stdout":case.stdout,"phase":case.phase,"reply":result.as_ref().ok(),"error":result.as_ref().err().map(ToString::to_string),"actual":result.is_ok(),"expected":case.valid})).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    println!("HOOK_PROTOCOL_ACTUAL {}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for row in rows {
        assert_eq!(row["actual"], row["expected"], "{row}");
    }
}
