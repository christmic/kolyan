use std::collections::BTreeSet;

use kolyan_model::ModelRef;

use super::*;
use crate::{AgentDefinitionInput, DelegationCeiling, EnvironmentTool};

mod self_resolution;

fn permissions() -> AgentPermissions {
    AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
            EnvironmentTool::Shell,
        ]
        .into(),
        delegation: DelegationCeiling {
            named_targets: [AgentKey::new("child", "1").unwrap()].into(),
            allow_inline: true,
            allow_self: true,
        },
    }
}

fn input(id: &str) -> AgentDefinitionInput {
    AgentDefinitionInput {
        definition_id: id.into(),
        revision: "1".into(),
        display_name: None,
        model: ModelRef::new("test-provider", "test-model"),
        instructions: "Analyze the bounded task.".into(),
        permissions: permissions(),
    }
}

fn definition(id: &str) -> AgentDefinition {
    AgentDefinition::new(input(id)).unwrap()
}

fn setup() -> (AgentCatalog, AgentSnapshot) {
    let mut catalog = AgentCatalog::new(3).unwrap();
    catalog.register(definition("root")).unwrap();
    catalog.register(definition("child")).unwrap();
    let root = catalog
        .resolve(
            &AgentSelector::Named(AgentKey::new("root", "1").unwrap()),
            "root-instance",
            &permissions(),
            &permissions(),
        )
        .unwrap();
    (catalog, root)
}

#[test]
fn registration_is_idempotent_and_conflicts_do_not_replace_content() {
    let mut catalog = AgentCatalog::new(1).unwrap();
    assert_eq!(
        catalog.register(definition("root")).unwrap(),
        Registration::Inserted
    );
    assert_eq!(
        catalog.register(definition("root")).unwrap(),
        Registration::Unchanged
    );
    let mut changed = input("root");
    changed.instructions = "Different instructions".into();
    assert_eq!(
        catalog.register(AgentDefinition::new(changed).unwrap()),
        Err(AgentError::Conflict)
    );
    assert_eq!(
        catalog.register(definition("child")),
        Err(AgentError::Capacity)
    );
    assert_eq!(
        catalog
            .definition(&AgentSelector::Named(AgentKey::new("root", "1").unwrap()))
            .unwrap(),
        definition("root")
    );
}

#[test]
fn exact_revision_and_inline_have_identical_snapshot_contracts() {
    let (catalog, _) = setup();
    let named = catalog
        .resolve(
            &AgentSelector::Named(AgentKey::new("child", "1").unwrap()),
            "instance",
            &permissions(),
            &permissions(),
        )
        .unwrap();
    let inline = catalog
        .resolve(
            &AgentSelector::Inline(definition("child")),
            "instance",
            &permissions(),
            &permissions(),
        )
        .unwrap();
    assert_eq!(named, inline);
    assert_eq!(inline.definition().display_name(), None);
    assert_eq!(inline.identity().definition_id, "child");
    assert_eq!(
        catalog.resolve(
            &AgentSelector::Named(AgentKey::new("child", "2").unwrap()),
            "instance",
            &permissions(),
            &permissions()
        ),
        Err(AgentError::NotFound)
    );
    let mut changed = input("child");
    changed.display_name = Some("Same identity, changed content".into());
    assert_eq!(
        catalog.resolve(
            &AgentSelector::Inline(AgentDefinition::new(changed).unwrap()),
            "instance",
            &permissions(),
            &permissions()
        ),
        Err(AgentError::Conflict)
    );
}

#[test]
fn snapshot_roundtrip_rejects_tampered_bindings_and_unknown_schema() {
    let (_, root) = setup();
    let encoded = serde_json::to_value(&root).unwrap();
    assert_eq!(
        serde_json::from_value::<AgentSnapshot>(encoded.clone()).unwrap(),
        root
    );
    for (field, replacement) in [
        ("digest", serde_json::json!("0".repeat(64))),
        ("schema_version", serde_json::json!(2)),
        (
            "identity",
            serde_json::json!({"definition_id":"root","revision":"2","instance_id":"root-instance"}),
        ),
        (
            "permissions",
            serde_json::json!({"tools":[],"delegation":{"named_targets":[],"allow_inline":false,"allow_self":false}}),
        ),
    ] {
        let mut altered = encoded.clone();
        altered[field] = replacement;
        assert!(
            serde_json::from_value::<AgentSnapshot>(altered).is_err(),
            "{field}"
        );
    }
    let mut unknown = encoded;
    unknown["extra"] = serde_json::json!(true);
    assert!(serde_json::from_value::<AgentSnapshot>(unknown).is_err());
}

#[test]
fn definition_validation_is_enforced_on_deserialization() {
    let mut invalid_inputs = Vec::new();
    let mut value = input("root");
    value.definition_id.clear();
    invalid_inputs.push(value);
    let mut value = input("root");
    value.revision = "latest revision".into();
    invalid_inputs.push(value);
    let mut value = input("root");
    value.display_name = Some(" ".into());
    invalid_inputs.push(value);
    let mut value = input("root");
    value.model.provider.clear();
    invalid_inputs.push(value);
    let mut value = input("root");
    value.model.model = "a\nb".into();
    invalid_inputs.push(value);
    let mut value = input("root");
    value.instructions = "a".repeat(65537);
    invalid_inputs.push(value);
    let mut value = input("root");
    value.instructions = "\0".into();
    invalid_inputs.push(value);
    let mut value = input("root");
    value.permissions.delegation.named_targets.insert(AgentKey {
        definition_id: "".into(),
        revision: "1".into(),
    });
    invalid_inputs.push(value);
    for value in invalid_inputs {
        assert!(AgentDefinition::new(value.clone()).is_err());
        assert!(
            serde_json::from_value::<AgentDefinition>(serde_json::to_value(value).unwrap())
                .is_err()
        );
    }
}

#[test]
fn every_definition_field_participates_in_digest() {
    let baseline = definition("root").digest().unwrap();
    let mut variants = Vec::new();
    let mut value = input("root");
    value.revision = "2".into();
    variants.push(value);
    let mut value = input("root");
    value.display_name = Some("Root".into());
    variants.push(value);
    let mut value = input("root");
    value.model.model = "other-model".into();
    variants.push(value);
    let mut value = input("root");
    value.instructions = "Other instructions".into();
    variants.push(value);
    let mut value = input("root");
    value.permissions.tools.clear();
    variants.push(value);
    for value in variants {
        assert_ne!(
            AgentDefinition::new(value).unwrap().digest().unwrap(),
            baseline
        );
    }
}

#[test]
fn intersections_are_explicit_and_do_not_silently_accept_expansion() {
    let (catalog, _) = setup();
    let host = AgentPermissions {
        tools: [EnvironmentTool::Read].into(),
        delegation: DelegationCeiling::default(),
    };
    let narrowed = permissions().intersection(&host).unwrap();
    assert_eq!(narrowed, host);
    assert_eq!(
        catalog.resolve(
            &AgentSelector::Inline(definition("root")),
            "instance",
            &host,
            &permissions()
        ),
        Err(AgentError::PermissionDenied)
    );
    assert_eq!(
        catalog
            .resolve(
                &AgentSelector::Inline(definition("root")),
                "instance",
                &permissions(),
                &host
            )
            .unwrap()
            .permissions(),
        &host
    );
    let mut invalid = host.clone();
    invalid.delegation.named_targets.insert(AgentKey {
        definition_id: " ".into(),
        revision: "1".into(),
    });
    assert!(host.intersection(&invalid).is_err());
}

#[test]
fn named_inline_and_self_admission_are_independent() {
    let (catalog, root) = setup();
    let none = AgentPermissions::default();
    for selector in [
        AgentSelector::Named(AgentKey::new("child", "1").unwrap()),
        AgentSelector::Inline(definition("unregistered")),
        AgentSelector::Inline(definition("root")),
    ] {
        let child = catalog
            .resolve_child(&root, &selector, "new-instance", &permissions(), &none)
            .unwrap();
        assert_ne!(child.identity().instance_id, root.identity().instance_id);
        let mut host = permissions();
        host.delegation = DelegationCeiling::default();
        assert_eq!(
            catalog.resolve_child(&root, &selector, "new-instance", &host, &none),
            Err(AgentError::PermissionDenied)
        );
    }
    let target = AgentSelector::Inline(definition("root"));
    assert!(
        catalog
            .resolve_child(&root, &target, "root-instance", &permissions(), &none)
            .is_err()
    );
    let mut changed = input("root");
    changed.instructions = "Changed self".into();
    let empty = AgentCatalog::new(1).unwrap();
    assert_eq!(
        empty.resolve_child(
            &root,
            &AgentSelector::Inline(AgentDefinition::new(changed).unwrap()),
            "child-instance",
            &permissions(),
            &none
        ),
        Err(AgentError::PermissionDenied)
    );
}

#[test]
fn parent_child_and_host_all_bound_child_permissions() {
    let (mut catalog, root) = setup();
    let mut restricted = input("restricted");
    restricted.permissions = AgentPermissions::default();
    catalog
        .register(AgentDefinition::new(restricted).unwrap())
        .unwrap();
    assert_eq!(
        catalog.resolve_child(
            &root,
            &AgentSelector::Inline(
                catalog.definitions[&AgentKey::new("restricted", "1").unwrap()].clone()
            ),
            "child-instance",
            &permissions(),
            &permissions()
        ),
        Err(AgentError::PermissionDenied)
    );
    let restricted_parent = catalog
        .resolve(
            &AgentSelector::Inline(definition("root")),
            "restricted-parent",
            &permissions(),
            &AgentPermissions {
                tools: BTreeSet::new(),
                delegation: permissions().delegation,
            },
        )
        .unwrap();
    let child = AgentSelector::Named(AgentKey::new("child", "1").unwrap());
    assert_eq!(
        catalog.resolve_child(
            &restricted_parent,
            &child,
            "child-instance",
            &permissions(),
            &permissions()
        ),
        Err(AgentError::PermissionDenied)
    );
    let mut host = permissions();
    host.tools.clear();
    assert_eq!(
        catalog.resolve_child(&root, &child, "child-instance", &host, &permissions()),
        Err(AgentError::PermissionDenied)
    );
    let mut host = permissions();
    host.delegation.named_targets.clear();
    assert_eq!(
        catalog.resolve_child(
            &root,
            &child,
            "child-instance",
            &host,
            &AgentPermissions::default()
        ),
        Err(AgentError::PermissionDenied)
    );
}

#[test]
fn bounds_and_four_tool_inventory_are_closed() {
    assert!(AgentCatalog::new(0).is_err());
    assert!(AgentCatalog::new(4097).is_err());
    let mut value = permissions();
    value.delegation.named_targets = (0..257)
        .map(|index| AgentKey::new(format!("agent-{index}"), "1").unwrap())
        .collect();
    assert!(value.validate().is_err());
    assert!(serde_json::from_str::<EnvironmentTool>("\"agent.invoke\"").is_err());
    assert!(serde_json::from_str::<EnvironmentTool>("\"shell.query\"").is_err());
    assert_eq!(EnvironmentTool::Shell.name(), "shell");
    let (catalog, _) = setup();
    assert!(
        catalog
            .resolve(
                &AgentSelector::Inline(definition("root")),
                "",
                &permissions(),
                &permissions()
            )
            .is_err()
    );
}
