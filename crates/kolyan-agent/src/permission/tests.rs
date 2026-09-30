use super::*;

#[test]
fn tool_and_delegation_authority_do_not_imply_each_other() {
    let tools = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
            EnvironmentTool::Shell,
        ]
        .into(),
        delegation: DelegationCeiling::default(),
    };
    let delegation = AgentPermissions {
        tools: BTreeSet::new(),
        delegation: DelegationCeiling {
            named_targets: [AgentKey::new("child", "1").unwrap()].into(),
            allow_inline: true,
            allow_self: true,
        },
    };
    assert_eq!(
        tools.intersection(&delegation).unwrap(),
        AgentPermissions::default()
    );
    assert_eq!(
        delegation.require_subset_of(&tools),
        Err(AgentError::PermissionDenied)
    );
    assert_eq!(
        tools.require_subset_of(&delegation),
        Err(AgentError::PermissionDenied)
    );
}

#[test]
fn delegation_intersection_preserves_exact_revision_and_independent_switches() {
    let first = DelegationCeiling {
        named_targets: [
            AgentKey::new("child", "1").unwrap(),
            AgentKey::new("child", "2").unwrap(),
        ]
        .into(),
        allow_inline: true,
        allow_self: false,
    };
    let second = DelegationCeiling {
        named_targets: [AgentKey::new("child", "2").unwrap()].into(),
        allow_inline: false,
        allow_self: true,
    };
    assert_eq!(
        first.intersection(&second).unwrap(),
        DelegationCeiling {
            named_targets: [AgentKey::new("child", "2").unwrap()].into(),
            allow_inline: false,
            allow_self: false,
        }
    );
}
