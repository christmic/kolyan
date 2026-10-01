use super::*;

use kolyan_core::ToolExecutor;
use kolyan_model::ToolCall;
use kolyan_policy::{PolicyContext, PolicyDecisionKind};
use serde_json::json;

mod advertising;

struct Fixture {
    _directory: tempfile::TempDir,
    config: config::Config,
    path: PathBuf,
}

fn fixture() -> Fixture {
    let directory = tempfile::tempdir().unwrap();
    let root = directory.path();
    std::fs::create_dir(root.join("workspace")).unwrap();
    std::fs::create_dir(root.join("workspace/safe")).unwrap();
    std::fs::create_dir(root.join("state")).unwrap();
    let path = root.join("server.json");
    // No Provider calls or effect execution: current_exe is a trusted admission
    // fixture only. Real production worker effects belong to process fixtures.
    let mut value: serde_json::Value =
        serde_json::from_str(include_str!("../../../../examples/server.example.json")).unwrap();
    value["workspace"] = json!(root.join("workspace"));
    value["ledger_path"] = json!(root.join("state/ledger.sqlite"));
    value["session_root"] = json!(root.join("state/sessions"));
    value["staging_root"] = json!(root.join("staging"));
    value["worker_path"] = json!(std::env::current_exe().unwrap());
    value["tool_scope"] = json!("safe");
    value["allow_shell"] = json!(false);
    std::fs::write(&path, serde_json::to_vec(&value).unwrap()).unwrap();
    let config = serde_json::from_value(value).unwrap();
    Fixture {
        _directory: directory,
        config,
        path,
    }
}

#[test]
fn required_worker_and_staging_have_no_discovery_or_defaults() {
    let fixture = fixture();
    let original: serde_json::Value =
        serde_json::from_slice(&std::fs::read(fixture.path).unwrap()).unwrap();
    for field in ["worker_path", "staging_root"] {
        let mut value = original.clone();
        value.as_object_mut().unwrap().remove(field);
        assert!(
            serde_json::from_value::<config::Config>(value).is_err(),
            "missing {field}"
        );
    }
    let mut value = original;
    value.as_object_mut().unwrap().remove("allow_shell");
    assert!(
        !serde_json::from_value::<config::Config>(value)
            .unwrap()
            .allow_shell
    );
}

#[test]
fn unsafe_scope_and_state_ancestor_are_refused_before_provider_or_persistence() {
    for scope in ["", "../safe", "safe/../safe", "/safe", "safe\n", ".git"] {
        let mut fixture = fixture();
        fixture.config.tool_scope = scope.into();
        assert!(App::new(fixture.config, &fixture.path).is_err(), "{scope}");
        assert!(
            !fixture
                .path
                .parent()
                .unwrap()
                .join("state/ledger.sqlite")
                .exists()
        );
    }
    for state in ["", "workspace", "workspace/safe"] {
        let mut fixture = fixture();
        let state = fixture.path.parent().unwrap().join(state);
        fixture.config.ledger_path = state.join("ledger.sqlite");
        fixture.config.session_root = state.join("sessions");
        assert!(environment(&mut fixture.config, &fixture.path).is_err());
    }
}

#[test]
fn missing_config_worker_and_overlapping_or_nonprivate_staging_fail_closed() {
    let mut fixture = fixture();
    let missing = fixture.path.with_file_name("absent.json");
    assert!(environment(&mut fixture.config, &missing).is_err());
    let mut fixture = self::fixture();
    fixture.config.worker_path = fixture.path.with_file_name("absent-worker");
    assert!(environment(&mut fixture.config, &fixture.path).is_err());
    for stage in ["workspace/staging", "state/staging", "state", ""] {
        let mut fixture = self::fixture();
        fixture.config.staging_root = fixture.path.parent().unwrap().join(stage);
        assert!(
            environment(&mut fixture.config, &fixture.path).is_err(),
            "stage={stage}"
        );
    }
    let mut fixture = self::fixture();
    fixture.config.session_root = fixture.config.workspace.join("sessions");
    assert!(environment(&mut fixture.config, &fixture.path).is_err());
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mut fixture = self::fixture();
        std::fs::create_dir(&fixture.config.staging_root).unwrap();
        std::fs::set_permissions(
            &fixture.config.staging_root,
            std::fs::Permissions::from_mode(0o755),
        )
        .unwrap();
        assert!(environment(&mut fixture.config, &fixture.path).is_err());
    }
}

#[cfg(unix)]
#[test]
fn redirected_scope_and_control_leaf_are_refused() {
    use std::os::unix::fs::symlink;
    let mut fixture = fixture();
    symlink(
        fixture.path.parent().unwrap().join("state"),
        fixture.config.workspace.join("redirected"),
    )
    .unwrap();
    fixture.config.tool_scope = "redirected".into();
    assert!(environment(&mut fixture.config, &fixture.path).is_err());
    let mut fixture = self::fixture();
    symlink(&fixture.path, &fixture.config.ledger_path).unwrap();
    assert!(environment(&mut fixture.config, &fixture.path).is_err());
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn real_prepared_claims_match_physical_policy_without_expanding_safe_shell() {
    let mut fixture = fixture();
    let workspace = fixture.config.workspace.canonicalize().unwrap();
    std::fs::write(workspace.join("safe/read.txt"), "readable").unwrap();
    std::fs::write(workspace.join("sibling.txt"), "not scoped").unwrap();
    let (tools, policy) = environment(&mut fixture.config, &fixture.path).unwrap();
    let definitions = IsolatedToolSet::tool_definitions();
    assert_eq!(
        definitions
            .iter()
            .map(|tool| tool.name.as_str())
            .collect::<Vec<_>>(),
        ["file.read", "file.write", "file.edit", "shell"]
    );
    for definition in definitions {
        assert_eq!(definition.input_schema["additionalProperties"], false);
    }
    for (name, arguments, effects, expected) in [
        (
            "file.read",
            json!({"path":"safe/read.txt"}),
            vec![Effect::Read],
            PolicyDecisionKind::Allow,
        ),
        (
            "file.write",
            json!({"path":"safe/new.txt", "content":"new"}),
            vec![Effect::Create, Effect::Update],
            PolicyDecisionKind::RequireApproval,
        ),
        (
            "file.edit",
            json!({"path":"safe/read.txt", "old_text":"readable", "new_text":"edited"}),
            vec![Effect::Read, Effect::Update],
            PolicyDecisionKind::RequireApproval,
        ),
    ] {
        let prepared = tools
            .prepare(ToolCall {
                id: name.into(),
                name: name.into(),
                arguments,
            })
            .await
            .unwrap();
        assert_eq!(prepared.claim().effects, effects.into_iter().collect());
        assert!(
            prepared
                .claim()
                .resource
                .path
                .as_deref()
                .unwrap()
                .starts_with(workspace.join("safe").to_str().unwrap())
        );
        assert!(prepared.requirements().process_sandbox);
        assert_eq!(
            policy
                .decide_prepared(&prepared, &PolicyContext::default())
                .kind,
            expected
        );
    }
    let out = tools
        .prepare(ToolCall {
            id: "outside-scope".into(),
            name: "file.read".into(),
            arguments: json!({"path":"sibling.txt"}),
        })
        .await
        .unwrap();
    assert_eq!(
        policy.decide_prepared(&out, &PolicyContext::default()).kind,
        PolicyDecisionKind::Deny
    );
    let shell = tools
        .prepare(ToolCall {
            id: "shell".into(),
            name: "shell".into(),
            arguments: json!({"command":"printf harmless", "path":"safe"}),
        })
        .await
        .unwrap();
    assert_eq!(
        policy
            .decide_prepared(&shell, &PolicyContext::default())
            .kind,
        PolicyDecisionKind::Deny
    );
    // Enabling execute/delete does not widen a narrow filesystem scope: Shell
    // honestly claims the entire workspace, not just its requested cwd.
    fixture.config.allow_shell = true;
    let (_, narrow) = environment(&mut fixture.config, &fixture.path).unwrap();
    assert_eq!(
        narrow
            .decide_prepared(&shell, &PolicyContext::default())
            .kind,
        PolicyDecisionKind::Deny
    );
    fixture.config.tool_scope = ".".into();
    let (_, full) = environment(&mut fixture.config, &fixture.path).unwrap();
    assert_eq!(
        full.decide_prepared(&shell, &PolicyContext::default()).kind,
        PolicyDecisionKind::RequireApproval
    );
    assert_eq!(
        shell.claim().effects,
        [
            Effect::Read,
            Effect::Create,
            Effect::Update,
            Effect::Delete,
            Effect::Execute
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        shell.claim().capabilities,
        [
            Capability::FilesystemRead,
            Capability::FilesystemWrite,
            Capability::ProcessExecute
        ]
        .into_iter()
        .collect()
    );
    assert_eq!(
        std::fs::read_to_string(workspace.join("safe/read.txt")).unwrap(),
        "readable"
    );
    assert!(
        !workspace.join("safe/new.txt").exists(),
        "preparation must not write"
    );
}

#[cfg(target_os = "macos")]
#[tokio::test]
async fn loaded_config_and_entire_state_are_protected_without_denying_workspace() {
    use std::os::unix::fs::PermissionsExt;
    let mut fixture = fixture();
    let inside = fixture.config.workspace.join("safe/server.json");
    std::fs::copy(&fixture.path, &inside).unwrap();
    let (tools, _) = environment(&mut fixture.config, &inside).unwrap();
    assert_eq!(
        std::fs::metadata(&fixture.config.staging_root)
            .unwrap()
            .permissions()
            .mode()
            & 0o7777,
        0o700
    );
    let protected = tools
        .prepare(ToolCall {
            id: "config".into(),
            name: "file.read".into(),
            arguments: json!({"path":"safe/server.json"}),
        })
        .await;
    assert!(protected.is_err());
    std::fs::write(
        fixture.config.workspace.join("safe/ordinary.txt"),
        "ordinary",
    )
    .unwrap();
    assert!(
        tools
            .prepare(ToolCall {
                id: "ordinary".into(),
                name: "file.read".into(),
                arguments: json!({"path":"safe/ordinary.txt"})
            })
            .await
            .is_ok()
    );
    // Symlink aliases resolve to the actual state referents and are refused.
    for name in [
        "ledger.sqlite",
        "ledger.sqlite-wal",
        "ledger.sqlite-shm",
        "ledger.sqlite-journal",
    ] {
        let target = fixture.config.ledger_path.parent().unwrap().join(name);
        std::fs::write(&target, "control").unwrap();
        let alias = format!("safe/{name}");
        std::os::unix::fs::symlink(&target, fixture.config.workspace.join(&alias)).unwrap();
        assert!(
            tools
                .prepare(ToolCall {
                    id: name.into(),
                    name: "file.read".into(),
                    arguments: json!({"path":alias})
                })
                .await
                .is_err()
        );
    }
}

#[cfg(not(target_os = "macos"))]
#[test]
fn unsupported_platform_has_no_unisolated_fallback() {
    let mut fixture = fixture();
    assert!(environment(&mut fixture.config, &fixture.path).is_err());
}
