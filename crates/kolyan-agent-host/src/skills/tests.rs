//! Real shared SQLite/artifact reconstruction, not model or native-tool acceptance.

use std::io::Write;

use kolyan_agent::{SkillDescriptorInput, SkillKey};
use serde::Deserialize;
use serde_json::{Value, json};

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    namespace: String,
    body: String,
    revoke: bool,
    same_fact: bool,
    conflict: bool,
}

#[test]
fn shared_skill_stores_rebuild_without_shadow_catalog() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-host-skills-")
        .tempdir()
        .unwrap()
        .keep();
    let path = evidence.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let state = tempfile::tempdir().unwrap();
        let journal_path = state.path().join("facts.sqlite");
        let artifacts_path = state.path().join("artifacts");
        let descriptor = SkillDescriptorInput {
            key: SkillKey::new("recipe".into(), "r2".into()).unwrap(),
            title: "Recipe".into(),
            description: "Bounded knowledge, no authority".into(),
        };
        let original = {
            let journal = SqliteFactJournal::open(&journal_path).unwrap();
            let artifacts =
                Arc::new(ArtifactStore::new(&artifacts_path, 16 * 1024 * 1024).unwrap());
            let (catalog, _) = assemble(
                HostSkillsConfig {
                    namespace: case.namespace.clone(),
                    limits: SkillLimits::default(),
                    policy: SkillAccessPolicy::new("acl".into(), "r1".into(), vec![]).unwrap(),
                },
                &journal,
                artifacts,
            )
            .unwrap();
            let registered = catalog
                .register(descriptor.clone(), "Exact trusted recipe\n")
                .unwrap();
            if case.revoke {
                catalog
                    .revoke(
                        &descriptor.key,
                        registered.reference(),
                        "revoke",
                        "current host revocation",
                    )
                    .unwrap();
            }
            registered
        };
        let journal = SqliteFactJournal::open(&journal_path).unwrap();
        let artifacts = Arc::new(ArtifactStore::new(&artifacts_path, 16 * 1024 * 1024).unwrap());
        let (rebuilt, _) = assemble(
            HostSkillsConfig {
                namespace: case.namespace.clone(),
                limits: SkillLimits::default(),
                policy: SkillAccessPolicy::new("acl".into(), "r2".into(), vec![]).unwrap(),
            },
            &journal,
            artifacts.clone(),
        )
        .unwrap();
        let actual = rebuilt.register(descriptor, &case.body);
        let body = artifacts
            .read(original.metadata().body(), 32 * 1024)
            .unwrap();
        writeln!(export, "{}", json!({"case_id":case.id,"original":original,
            "actual":actual.as_ref().ok(),"conflict":matches!(&actual, Err(kolyan_agent::SkillError::Conflict)),
            "same_fact":actual.as_ref().is_ok_and(|v|v.reference()==original.reference()),
            "historical_body_bytes":body,
            "error":actual.err().map(|e|e.to_string()),"namespace":rebuilt.namespace(),
            "store_files":std::fs::read_dir(state.path()).unwrap().map(|e|e.unwrap().file_name().to_string_lossy().into_owned()).collect::<Vec<_>>() })).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    eprintln!("host Skills evidence={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["same_fact"], case.same_fact, "{}: {row}", case.id);
        assert_eq!(row["conflict"], case.conflict, "{}: {row}", case.id);
        assert_eq!(row["namespace"], case.namespace);
        assert_eq!(
            row["historical_body_bytes"],
            json!(b"Exact trusted recipe\n".to_vec())
        );
        assert!(
            !row["store_files"]
                .as_array()
                .unwrap()
                .iter()
                .any(|v| v == "skills")
        );
        assert_eq!(row["original"]["metadata"]["body"]["retention"], "required");
    }
}
