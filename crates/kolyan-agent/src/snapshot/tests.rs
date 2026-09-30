use kolyan_model::ModelRef;

use super::*;
use crate::{AgentDefinitionInput, EnvironmentTool};

fn snapshot() -> AgentSnapshot {
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "root".into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("provider", "model"),
        instructions: "Follow host authority.".into(),
        permissions: AgentPermissions::default(),
    })
    .unwrap();
    AgentSnapshot::new(definition, "instance".into(), AgentPermissions::default()).unwrap()
}

#[test]
fn recomputed_digest_cannot_hide_invalid_identity_or_permission_expansion() {
    let mut expanded = snapshot().0;
    expanded.permissions.tools.insert(EnvironmentTool::Shell);
    expanded.digest = binding_digest(&expanded).unwrap();
    assert_eq!(
        AgentSnapshot::try_from(expanded),
        Err(AgentError::PermissionDenied)
    );

    let mut wrong_identity = snapshot().0;
    wrong_identity.identity.definition_id = "other".into();
    wrong_identity.digest = binding_digest(&wrong_identity).unwrap();
    assert_eq!(
        AgentSnapshot::try_from(wrong_identity),
        Err(AgentError::SnapshotMismatch)
    );

    let mut missing_identity = snapshot().0;
    missing_identity.identity.instance_id.clear();
    missing_identity.digest = binding_digest(&missing_identity).unwrap();
    assert!(AgentSnapshot::try_from(missing_identity).is_err());
}

#[test]
fn different_instances_produce_different_snapshot_bindings() {
    let first = snapshot();
    let second = AgentSnapshot::new(
        first.definition().clone(),
        "second-instance".into(),
        first.permissions().clone(),
    )
    .unwrap();
    assert_eq!(
        first.definition().digest().unwrap(),
        second.definition().digest().unwrap()
    );
    assert_ne!(first.digest(), second.digest());
}
