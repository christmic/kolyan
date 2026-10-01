use super::*;

#[test]
fn saved_parent_is_authoritative_despite_conflicting_catalog_content() {
    let (original, parent) = setup();
    let encoded = serde_json::to_value(&parent).unwrap();
    let restored: AgentSnapshot = serde_json::from_value(encoded).unwrap();
    drop(original);

    let mut replacement = AgentCatalog::new(1).unwrap();
    let mut changed = input("root");
    changed.instructions = "Replacement catalog must never change saved self content.".into();
    replacement
        .register(AgentDefinition::new(changed).unwrap())
        .unwrap();
    assert_eq!(
        replacement.resolve_child(
            &restored,
            &AgentSelector::Named(restored.definition().key()),
            "child-instance",
            &permissions(),
            &permissions()
        ),
        Err(AgentError::PermissionDenied)
    );

    let child =
        crate::resolve_self(&restored, "child-instance", &permissions(), &permissions()).unwrap();
    assert_eq!(child.definition(), parent.definition());
    assert_eq!(
        child.definition().digest().unwrap(),
        parent.definition().digest().unwrap()
    );
    assert_eq!(child.identity().revision, parent.identity().revision);
    assert_eq!(
        child.identity().definition_id,
        parent.identity().definition_id
    );
    assert_eq!(child.identity().instance_id, "child-instance");
    assert_ne!(child.digest(), parent.digest());
    assert_eq!(restored, parent);
}

#[test]
fn no_registered_definition_is_needed_and_requested_narrowing_is_exact() {
    let (_, parent) = setup();
    let requested = AgentPermissions {
        tools: [EnvironmentTool::Read].into(),
        delegation: DelegationCeiling::default(),
    };
    let child = crate::resolve_self(&parent, "narrow-child", &permissions(), &requested).unwrap();
    assert_eq!(child.permissions(), &requested);
    // Authority to make this call does not automatically grant recursive calls.
    assert_eq!(
        crate::resolve_self(
            &child,
            "grandchild",
            &permissions(),
            &AgentPermissions::default()
        ),
        Err(AgentError::PermissionDenied)
    );
}

#[test]
fn host_parent_and_definition_self_switches_all_gate_admission() {
    let (catalog, parent) = setup();
    let mut no_self = permissions();
    no_self.delegation.allow_self = false;
    assert_eq!(
        crate::resolve_self(&parent, "child", &no_self, &AgentPermissions::default()),
        Err(AgentError::PermissionDenied)
    );

    let restricted_parent = catalog
        .resolve(
            &AgentSelector::Inline(definition("root")),
            "restricted-parent",
            &permissions(),
            &no_self,
        )
        .unwrap();
    assert_eq!(
        crate::resolve_self(
            &restricted_parent,
            "child",
            &permissions(),
            &AgentPermissions::default()
        ),
        Err(AgentError::PermissionDenied)
    );

    let mut original_definition = input("no-self-definition");
    original_definition.permissions = no_self.clone();
    let selector = AgentSelector::Inline(AgentDefinition::new(original_definition).unwrap());
    let parent = catalog
        .resolve(&selector, "parent", &permissions(), &no_self)
        .unwrap();
    assert_eq!(
        crate::resolve_self(
            &parent,
            "child",
            &permissions(),
            &AgentPermissions::default()
        ),
        Err(AgentError::PermissionDenied)
    );
}

#[test]
fn host_and_parent_tool_restrictions_reject_expansion_without_silent_clipping() {
    let (catalog, parent) = setup();
    let mut restricted = permissions();
    restricted.tools = [EnvironmentTool::Read].into();
    assert_eq!(
        crate::resolve_self(&parent, "child", &restricted, &permissions()),
        Err(AgentError::PermissionDenied)
    );
    let narrowed_parent = catalog
        .resolve(
            &AgentSelector::Inline(definition("root")),
            "narrow-parent",
            &permissions(),
            &restricted,
        )
        .unwrap();
    assert_eq!(
        crate::resolve_self(&narrowed_parent, "child", &permissions(), &permissions()),
        Err(AgentError::PermissionDenied)
    );
    let child =
        crate::resolve_self(&narrowed_parent, "child", &permissions(), &restricted).unwrap();
    assert_eq!(child.permissions(), &restricted);
}

#[test]
fn original_definition_ceiling_is_retained_and_delegation_cannot_expand() {
    let catalog = AgentCatalog::new(1).unwrap();
    let mut saved = input("saved");
    saved.permissions.tools = [EnvironmentTool::Read].into();
    saved.permissions.delegation.allow_inline = false;
    saved.permissions.delegation.named_targets.clear();
    let selector = AgentSelector::Inline(AgentDefinition::new(saved.clone()).unwrap());
    let parent = catalog
        .resolve(&selector, "parent", &permissions(), &saved.permissions)
        .unwrap();
    for requested in [
        permissions(),
        AgentPermissions {
            tools: saved.permissions.tools.clone(),
            delegation: DelegationCeiling {
                allow_inline: true,
                ..saved.permissions.delegation.clone()
            },
        },
        AgentPermissions {
            tools: saved.permissions.tools.clone(),
            delegation: DelegationCeiling {
                named_targets: [AgentKey::new("other", "1").unwrap()].into(),
                ..saved.permissions.delegation.clone()
            },
        },
    ] {
        assert_eq!(
            crate::resolve_self(&parent, "child", &permissions(), &requested),
            Err(AgentError::PermissionDenied)
        );
    }
    let child = crate::resolve_self(&parent, "child", &permissions(), &saved.permissions).unwrap();
    assert_eq!(child.definition().permissions(), &saved.permissions);
    assert_eq!(child.permissions(), &saved.permissions);
}

#[test]
fn same_instance_invalid_identity_and_invalid_permissions_are_rejected() {
    let (_, parent) = setup();
    for instance in [parent.identity().instance_id.as_str(), "", "bad identity"] {
        assert!(matches!(
            crate::resolve_self(&parent, instance, &permissions(), &permissions()),
            Err(AgentError::Invalid(_))
        ));
    }
    let mut invalid = permissions();
    invalid.delegation.named_targets.insert(AgentKey {
        definition_id: String::new(),
        revision: "1".into(),
    });
    assert!(matches!(
        crate::resolve_self(&parent, "child", &invalid, &permissions()),
        Err(AgentError::Invalid(_))
    ));
    assert!(matches!(
        crate::resolve_self(&parent, "child", &permissions(), &invalid),
        Err(AgentError::Invalid(_))
    ));
}

#[test]
fn self_snapshots_keep_existing_strict_serialization_and_digest_contract() {
    let (_, parent) = setup();
    let child = crate::resolve_self(&parent, "child", &permissions(), &permissions()).unwrap();
    let encoded = serde_json::to_value(&child).unwrap();
    assert_eq!(
        serde_json::from_value::<AgentSnapshot>(encoded.clone()).unwrap(),
        child
    );
    for (field, value) in [
        ("digest", serde_json::json!("0".repeat(64))),
        ("schema_version", serde_json::json!(2)),
        (
            "identity",
            serde_json::json!({"definition_id":"root","revision":"changed","instance_id":"child"}),
        ),
        (
            "permissions",
            serde_json::json!({"tools":[],"delegation":{"named_targets":[],"allow_inline":false,"allow_self":false}}),
        ),
    ] {
        let mut changed = encoded.clone();
        changed[field] = value;
        assert!(
            serde_json::from_value::<AgentSnapshot>(changed).is_err(),
            "{field}"
        );
    }
    let mut unknown = encoded;
    unknown["unknown"] = serde_json::json!(true);
    assert!(serde_json::from_value::<AgentSnapshot>(unknown).is_err());
}
