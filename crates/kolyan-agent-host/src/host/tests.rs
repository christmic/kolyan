//! Pure host resource admission, not provider or native worker acceptance.

use std::io::Write;

use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    path: String,
    allowed: bool,
}

#[test]
fn resource_admission_exports_before_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/resources.json")).unwrap();
    let evidence = tempfile::tempdir().unwrap().keep();
    let resources = tempfile::tempdir().unwrap();
    let workspace = resources.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(workspace.join("child")).unwrap();
    let workspace = workspace.canonicalize().unwrap();
    let path = evidence.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let result = scope(&workspace, &case.path);
        let row = json!({"case_id":case.id,"input":case.path,
            "allowed":result.is_ok(),"physical":result.as_ref().ok(),
            "error":result.err().map(|e|e.to_string())});
        writeln!(export, "{row}").unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("host resource evidence={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["case_id"], case.id);
        assert_eq!(row["allowed"], case.allowed, "{}: {row}", case.id);
        if case.allowed {
            assert!(
                std::path::Path::new(row["physical"].as_str().unwrap()).starts_with(&workspace)
            );
        }
    }
}

#[cfg(unix)]
#[test]
fn control_and_scope_symlinks_are_not_control_authority() {
    use std::os::unix::fs::symlink;
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().join("workspace");
    let outside = root.path().join("outside");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&outside).unwrap();
    symlink(&outside, workspace.join("escape")).unwrap();
    let result = scope(&workspace.canonicalize().unwrap(), "escape");
    let control = control_directory(&workspace.join("escape"));
    let evidence = tempfile::tempdir().unwrap().keep().join("actual.jsonl");
    let mut export = std::fs::File::create(&evidence).unwrap();
    writeln!(
        export,
        "{}",
        json!({"scope":result.as_ref().map(|p|p.display().to_string()).map_err(|e|e.to_string()),
        "control":control.as_ref().map_err(|e|e.to_string())})
    )
    .unwrap();
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let row: Value =
        serde_json::from_str(std::fs::read_to_string(&evidence).unwrap().trim()).unwrap();
    assert!(row["scope"]["Err"].is_string());
    assert!(row["control"]["Err"].is_string());
}
