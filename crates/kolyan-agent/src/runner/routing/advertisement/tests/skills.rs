//! SQLite historical binding recovery and actual pre-opening guard, no network.
use super::*;
use crate::*;
use kolyan_ledger::{FactJournal, SqliteFactJournal};
use std::{
    fs::File,
    io::{BufRead, BufReader, Write},
    sync::Arc,
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    dispatches: usize,
}
#[derive(Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    None,
    Revoke,
    Acl,
    Missing,
    Changed,
    Duplicate,
}

async fn run(case: &Case, path: &std::path::Path) -> Value {
    std::fs::create_dir_all(path).unwrap();
    let journal = Arc::new(SqliteFactJournal::open(path.join("facts.sqlite")).unwrap());
    let artifacts = Arc::new(kolyan_trace::ArtifactStore::new(path.join("bodies"), 65536).unwrap());
    let catalog = SkillCatalog::new(
        journal.clone(),
        artifacts.clone(),
        "opening.fixture".into(),
        SkillLimits::default(),
    )
    .unwrap();
    let selected = catalog
        .register(
            SkillDescriptorInput {
                key: SkillKey::new("guide".into(), "1".into()).unwrap(),
                title: "guide".into(),
                description: "Knowledge only".into(),
            },
            "exact untrusted text\n",
        )
        .unwrap();
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "agent".into(),
        revision: "1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("fixture", "no-network"),
        instructions: "fixture".into(),
        permissions: AgentPermissions::default(),
    })
    .unwrap();
    let saved = AgentInvocationBinding {
        task_id: "task".into(),
        invocation_id: "root".into(),
        logical_session_id: "session".into(),
        private_session_id: "session".into(),
        context_kind: BindingContextKind::Root,
        snapshot: AgentSnapshot::new(
            definition.clone(),
            "instance".into(),
            AgentPermissions::default(),
        )
        .unwrap(),
    };
    let ownership = AgentInvocationBindingStore::new(journal.clone())
        .save(&saved)
        .unwrap();
    let policy = SkillAccessPolicy::new(
        "acl".into(),
        "1".into(),
        vec![SkillAccessRuleInput {
            agent: definition.key(),
            logical_session_id: "session".into(),
            task_id: Some("task".into()),
            invocation_id: Some("root".into()),
            skills: [selected.metadata().descriptor().key.clone()].into(),
        }],
    )
    .unwrap();
    let runtime = SkillRuntime::new(catalog.clone(), policy.clone());
    let scope = SkillScope::from_binding(&saved).unwrap();
    let binding = runtime
        .bind(
            &runtime.discover(&saved.snapshot, &scope).unwrap(),
            &ownership,
        )
        .unwrap();
    if matches!(case.mutation, Mutation::Revoke) {
        catalog
            .revoke(
                &selected.metadata().descriptor().key,
                selected.reference(),
                "revoke.1",
                "host revoked",
            )
            .unwrap();
    }
    // Reconstruct stores and runtime rather than accepting a serialized verified wrapper.
    let rebuilt_journal = Arc::new(SqliteFactJournal::open(path.join("facts.sqlite")).unwrap());
    let rebuilt_catalog = SkillCatalog::new(
        rebuilt_journal.clone(),
        artifacts,
        "opening.fixture".into(),
        SkillLimits::default(),
    )
    .unwrap();
    let rebuilt = Arc::new(SkillRuntime::new(
        rebuilt_catalog.clone(),
        if matches!(case.mutation, Mutation::Acl) {
            SkillAccessPolicy::deny_all()
        } else {
            policy
        },
    ));
    let restored = rebuilt
        .restore_binding(binding.reference(), &scope)
        .unwrap();
    let historical_equal = restored == binding;
    let mut request = crate::runner::tests::support::Harness::new()
        .request(&case.id, false)
        .turn
        .model_request;
    request.tools = vec![crate::skills::skill_load_definition(&restored).unwrap()];
    match case.mutation {
        Mutation::Missing => request.tools.clear(),
        Mutation::Changed => request.tools[0].input_schema = json!({"type":"object"}),
        Mutation::Duplicate => request.tools.push(request.tools[0].clone()),
        _ => {}
    }
    let before = rebuilt_journal
        .read(rebuilt_catalog.stream_id(), 0, 1024)
        .unwrap();
    let provider = AdvertisementProvider {
        inner: DispatchProbe::default(),
        expected: None,
        skills: Some((rebuilt, restored)),
    };
    let error = match provider.stream(request.clone()).await {
        Ok(_) => panic!("probe returns error"),
        Err(e) => e,
    };
    json!({"id":case.id,"saved":binding,"ownership":ownership,"scope":scope,"selected":selected,
        "historical_equal":historical_equal,"request":request,"dispatched":provider.inner.0.lock().unwrap().clone(),
        "error":{"kind":format!("{:?}",error.kind),"phase":format!("{:?}",error.phase),"message":error.message},
        "before":before,"after":rebuilt_journal.read(rebuilt_catalog.stream_id(),0,1024).unwrap()})
}

#[tokio::test]
async fn restored_skill_binding_guards_actual_opening_data_matrix() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("skills.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skill-opening-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    for case in &cases {
        writeln!(output, "{}", run(case, &root.join(&case.id)).await).unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("SKILL_OPENING_ACTUAL={}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|line| serde_json::from_str(&line.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), cases.len());
    for (row, case) in rows.iter().zip(&cases) {
        assert_eq!(row["id"], case.id);
        assert_eq!(row["historical_equal"], true);
        assert_eq!(row["before"], row["after"]);
        assert_eq!(
            row["dispatched"].as_array().unwrap().len(),
            case.dispatches,
            "{}: {row}",
            case.id
        );
        if case.dispatches == 1 {
            assert_eq!(row["dispatched"][0], row["request"]);
        } else {
            assert_eq!(row["error"]["phase"], "Validate");
        }
    }
}
