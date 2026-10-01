#![cfg(target_os = "macos")]

use super::*;

use serde_json::json;

struct TestWorkspace {
    root: tempfile::TempDir,
    _staging: tempfile::TempDir,
}

impl TestWorkspace {
    fn path(&self) -> &std::path::Path {
        self.root.path()
    }
}

fn setup() -> (TestWorkspace, IsolatedFileTools) {
    let root = tempfile::tempdir().unwrap();
    use std::os::unix::fs::PermissionsExt;
    let staging = tempfile::Builder::new()
        .permissions(std::fs::Permissions::from_mode(0o700))
        .tempdir()
        .unwrap();
    let tools = IsolatedFileTools::new(IsolatedFileConfig {
        workspace: root.path().into(),
        staging_root: staging.path().into(),
        worker: std::env::current_exe().unwrap(),
        protected_roots: vec![],
        file_limits: FileOperationLimits::default(),
        max_output_bytes: 65536,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    (
        TestWorkspace {
            root,
            _staging: staging,
        },
        tools,
    )
}

fn call(name: &str, arguments: serde_json::Value) -> ToolCall {
    ToolCall {
        id: "prepared-file-fixture".into(),
        name: name.into(),
        arguments,
    }
}

#[test]
fn read_claims_are_read_only() {
    let (root, tools) = setup();
    std::fs::write(root.path().join("note"), "content").unwrap();
    let prepared = tools
        .prepare(call("file.read", json!({"path":"note"})))
        .unwrap();
    assert_eq!(
        prepared.claim().capabilities,
        [Capability::FilesystemRead].into_iter().collect()
    );
    assert_eq!(
        prepared.claim().effects,
        [Effect::Read].into_iter().collect()
    );
    assert_eq!(prepared.claim().idempotency, Idempotency::Idempotent);
}

#[test]
fn write_requires_create_and_update_regardless_of_existence() {
    let (root, tools) = setup();
    let input = call("file.write", json!({"path":"note","content":"replacement"}));
    let missing = tools.prepare(input.clone()).unwrap();
    std::fs::write(root.path().join("note"), "original").unwrap();
    let existing = tools.prepare(input).unwrap();
    for prepared in [&missing, &existing] {
        assert_eq!(
            prepared.claim().capabilities,
            [Capability::FilesystemWrite].into_iter().collect()
        );
        assert_eq!(
            prepared.claim().effects,
            [Effect::Create, Effect::Update].into_iter().collect()
        );
        assert_eq!(prepared.claim().idempotency, Idempotency::NonIdempotent);
    }
    assert_eq!(missing.claim(), existing.claim());
    // Claims stay conservative, while exact object identity now binds authority.
    assert_ne!(missing.digest(), existing.digest());
    assert_ne!(missing.execution_binding(), existing.execution_binding());
    assert_eq!(
        std::fs::read(root.path().join("note")).unwrap(),
        b"original"
    );
}

#[test]
fn edit_requires_read_and_write_capabilities_and_effects() {
    let (root, tools) = setup();
    std::fs::write(root.path().join("note"), "alpha keep").unwrap();
    let prepared = tools
        .prepare(call(
            "file.edit",
            json!({
                "path":"note","old_text":"alpha","new_text":"beta",
            }),
        ))
        .unwrap();
    assert_eq!(
        prepared.claim().capabilities,
        [Capability::FilesystemRead, Capability::FilesystemWrite]
            .into_iter()
            .collect()
    );
    assert_eq!(
        prepared.claim().effects,
        [Effect::Read, Effect::Update].into_iter().collect()
    );
    assert_eq!(prepared.claim().idempotency, Idempotency::NonIdempotent);
    assert_eq!(
        std::fs::read(root.path().join("note")).unwrap(),
        b"alpha keep"
    );
}

#[test]
fn preparation_binds_physical_alias_and_rejects_symlink_replacements() {
    use std::os::unix::fs::symlink;
    let (root, tools) = setup();
    std::fs::write(root.path().join("note"), "content").unwrap();
    symlink("note", root.path().join("alias")).unwrap();
    let prepared = tools
        .prepare(call("file.read", json!({"path":"alias"})))
        .unwrap();
    let binding: ExactFileBinding =
        serde_json::from_value(prepared.execution_binding().clone()).unwrap();
    assert_eq!(binding.leaf, "note");
    assert_eq!(
        binding.workspace.physical_path,
        root.path().canonicalize().unwrap()
    );
    assert_eq!(prepared.call().arguments["path"], "alias");
    assert!(
        tools
            .prepare(call(
                "file.write",
                json!({"path":"alias","content":"other"})
            ))
            .is_err()
    );
    assert!(
        tools
            .prepare(call(
                "file.edit",
                json!({"path":"alias","old_text":"content","new_text":"other"})
            ))
            .is_err()
    );
    assert_eq!(
        std::fs::read_to_string(root.path().join("note")).unwrap(),
        "content"
    );
}

#[test]
fn host_stage_plan_is_exact_private_and_outside_workspace() {
    let (root, tools) = setup();
    let prepared = tools
        .prepare(call("file.write", json!({"path":"note","content":"new"})))
        .unwrap();
    let binding = serde_json::from_value(prepared.execution_binding().clone()).unwrap();
    let (request, _pinned, directory) = tools
        .worker_request(tools.operation(prepared.call()).unwrap(), binding)
        .unwrap();
    let stage = request.staging.as_ref().unwrap();
    use std::os::unix::fs::PermissionsExt;
    assert_eq!(
        std::fs::metadata(&stage.parent.physical_path)
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );
    assert!(
        stage
            .parent
            .physical_path
            .starts_with(&tools.config.staging_root)
    );
    assert!(
        !stage
            .parent
            .physical_path
            .starts_with(root.path().canonicalize().unwrap())
    );
    assert!(
        request
            .binding
            .protected_roots
            .contains(&tools.config.staging_root)
    );
    assert!(!stage.parent.physical_path.join(&stage.leaf).exists());
    assert!(directory.is_some());
    crate::exact_file::execute_exact(&request, tools.config.file_limits).unwrap();
    assert_eq!(
        std::fs::read_to_string(root.path().join("note")).unwrap(),
        "new"
    );
    drop(directory);
    assert!(!stage.parent.physical_path.exists());
}

#[test]
fn parent_replacement_changes_explicit_binding_digest() {
    let (root, tools) = setup();
    std::fs::create_dir(root.path().join("parent")).unwrap();
    std::fs::write(root.path().join("parent/note"), "old").unwrap();
    let input = call("file.read", json!({"path":"parent/note"}));
    let old = tools.prepare(input.clone()).unwrap();
    std::fs::rename(
        root.path().join("parent"),
        root.path().join("original-parent"),
    )
    .unwrap();
    std::fs::create_dir(root.path().join("parent")).unwrap();
    std::fs::write(root.path().join("parent/note"), "new").unwrap();
    let new = tools.prepare(input).unwrap();
    assert_eq!(old.claim(), new.claim());
    assert_eq!(old.tool_revision(), new.tool_revision());
    assert_ne!(old.execution_binding(), new.execution_binding());
    assert_ne!(old.digest(), new.digest());
}
