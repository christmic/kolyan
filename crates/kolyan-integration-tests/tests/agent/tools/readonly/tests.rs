//! Attest only an actually enforced read-only assembly and reject forged claims.

use std::{fs, os::unix::fs::DirBuilderExt, sync::Arc};

use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector,
    EnvironmentTool, EnvironmentToolFactory,
};
use kolyan_model::{ModelRef, ToolCall};
use kolyan_server::ExecutionRef;
use serde_json::json;

use super::{
    super::{Evidence, Tools, initialize_worker},
    *,
};

#[tokio::test]
async fn real_factory_attests_only_read_and_rejects_mislabeled_writable_authority() {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir_all(root.path().join("workspace/safe")).unwrap();
    fs::create_dir(root.path().join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.path().join("staging"))
        .unwrap();
    fs::write(
        root.path().join("workspace/safe/proof.txt"),
        "physical-read-proof",
    )
    .unwrap();
    let evidence = Arc::new(Evidence::new(&root.path().join("actual.jsonl")));
    initialize_worker(
        root.path(),
        &evidence,
        &super::super::worker::WorkerRun::prepare().await,
    )
    .unwrap();
    let factory = Tools {
        root: root.path().into(),
        dataset: crate::data::dataset(),
        evidence: evidence.clone(),
    };
    let execution = ExecutionRef {
        session_id: "factory-session".into(),
        turn_id: "factory-turn".into(),
        execution_id: "factory-execution".into(),
    };
    let mut read_executor = None;
    for tool in [
        EnvironmentTool::Read,
        EnvironmentTool::Write,
        EnvironmentTool::Edit,
        EnvironmentTool::Shell,
    ] {
        let permissions = AgentPermissions {
            tools: [tool].into(),
            ..Default::default()
        };
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "fixed-adapter".into(),
            revision: "r1".into(),
            display_name: None,
            model: ModelRef::new("fixture", "factory"),
            instructions: "Use only the saved ceiling.".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let snapshot = AgentCatalog::new(1)
            .unwrap()
            .resolve(
                &AgentSelector::Inline(definition),
                format!("factory-{}", tool.name()),
                &permissions,
                &permissions,
            )
            .unwrap();
        assert_eq!(
            factory.enforces_read_only_parallel(&snapshot),
            tool == EnvironmentTool::Read
        );
        let set = factory.build(&snapshot, &execution).unwrap();
        if tool == EnvironmentTool::Read {
            read_executor = Some(set.executor);
        }
    }
    let executor = read_executor.unwrap();
    let read = executor
        .prepare(ToolCall {
            id: "read".into(),
            name: "file.read".into(),
            arguments: json!({"path":"safe/proof.txt"}),
        })
        .await
        .unwrap();
    assert!(executor.inner.validate(&read).is_ok());
    assert!(
        executor
            .prepare(ToolCall {
                id: "write".into(),
                name: "file.write".into(),
                arguments: json!({"path":"safe/proof.txt","content":"forbidden"})
            })
            .await
            .is_err()
    );
    let mut forged_claim = read.claim().clone();
    forged_claim
        .capabilities
        .insert(Capability::FilesystemWrite);
    forged_claim.effects.insert(Effect::Update);
    let forged = PreparedCall::new(
        read.call().clone(),
        read.tool_revision().into(),
        forged_claim,
        read.requirements().clone(),
    )
    .unwrap();
    evidence.append(json!({"event":"read_only_authority_checks","actual_prepared":read,"mislabeled_writable_prepared":forged})).unwrap();
    assert!(executor.inner.validate(&forged).is_err());
    assert_eq!(
        fs::read_to_string(root.path().join("workspace/safe/proof.txt")).unwrap(),
        "physical-read-proof"
    );
}
