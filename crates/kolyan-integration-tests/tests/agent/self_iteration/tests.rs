//! No-network checks; these never author the actual candidate worktree.

#[test]
fn state_data_accepts_reordering_but_rejects_duplicates_unknown_fields_and_wrong_semantics() {
    let task = plan();
    let mut cases = serde_json::to_value(&task.expected_states)
        .unwrap()
        .as_array()
        .unwrap()
        .clone();
    cases.reverse();
    let value = serde_json::json!({"schema_version":1,"cases":cases});
    assert!(super::validation::valid_state_data(
        &value.to_string(),
        &task.expected_states
    ));
    let mut wrong = value.clone();
    wrong["cases"][0] = wrong["cases"][1].clone();
    assert!(!super::validation::valid_state_data(
        &wrong.to_string(),
        &task.expected_states
    ));
    let mut wrong = value.clone();
    wrong["extra"] = serde_json::json!(true);
    assert!(!super::validation::valid_state_data(
        &wrong.to_string(),
        &task.expected_states
    ));
    let mut wrong = value;
    wrong["cases"][0]["terminal"] = serde_json::json!(true);
    assert!(!super::validation::valid_state_data(
        &wrong.to_string(),
        &task.expected_states
    ));
}

use std::collections::BTreeSet;

use kolyan_model::ToolCall;
use serde_json::json;

use super::{baseline, plan, tools};

#[test]
fn every_stage_receives_complete_scope_verified_modules_schema_and_phase_limits() {
    let root = tempfile::tempdir().unwrap();
    let workspace = root.path().canonicalize().unwrap();
    std::process::Command::new("/usr/bin/git")
        .arg("init")
        .arg(&workspace)
        .output()
        .unwrap();
    let plan = super::Plan {
        task: plan(),
        run: super::RunConfig {
            worktree: workspace.clone(),
            host_private: workspace.clone(),
            branch: "context-unit".into(),
            head: "0".repeat(40),
            source_snapshot_sha256: "0".repeat(64),
            baseline_manifest: "unit-baseline".into(),
            baseline_manifest_sha256: "0".repeat(64),
            baseline_files: 0,
            baseline_server_tests: 0,
        },
    };
    for path in [
        &plan.allowlist[0],
        &plan.allowlist[1],
        &plan.task_facts.module_entry,
        &plan.task_facts.public_export_entry,
    ] {
        let file = workspace.join(path);
        std::fs::create_dir_all(file.parent().unwrap()).unwrap();
        std::fs::write(file, format!("unit module observation for {path}")).unwrap();
    }
    let initial = baseline::collect(&workspace).unwrap();
    let observed = plan
        .stages
        .iter()
        .map(|stage| {
            let spec = super::input::specification(&plan, stage, &initial, &initial).unwrap();
            let prompt = super::input::prompt(&spec, stage).unwrap();
            (spec, prompt)
        })
        .collect::<Vec<_>>();
    println!("{}", serde_json::to_string(&observed).unwrap());
    for (stage, (spec, prompt)) in plan.stages.iter().zip(&observed) {
        assert_eq!(spec["case_revision"], plan.case_revision);
        assert_eq!(spec["current_stage"]["id"], stage.id);
        assert_eq!(spec["current_stage"]["writable"], stage.writable);
        assert_eq!(spec["all_stage_restrictions"].as_array().unwrap().len(), 4);
        assert_eq!(
            spec["target_dataset_example"]["cases"]
                .as_array()
                .unwrap()
                .len(),
            7
        );
        for (index, path) in plan.allowlist.iter().enumerate() {
            assert_eq!(
                spec["complete_candidate_write_allowlist"][index]["path"],
                *path
            );
            assert_eq!(
                spec["complete_candidate_write_allowlist"][index]["initially_present"],
                index < 2
            );
            assert!(prompt.contains(path));
        }
        assert_eq!(
            spec["actual_module_entries"][0]["path"],
            plan.task_facts.module_entry
        );
        assert!(prompt.contains("crates/kolyan-server/src/tasks.rs"));
        assert!(!prompt.contains("crates/kolyan-server/src/tasks/mod.rs"));
    }
    let new = workspace.join(&plan.allowlist[2]);
    std::fs::create_dir_all(new.parent().unwrap()).unwrap();
    std::fs::write(&new, "unit-only new file state; not actual candidate").unwrap();
    let current = baseline::collect(&workspace).unwrap();
    let updated = super::input::specification(&plan, &plan.stages[3], &initial, &current).unwrap();
    assert_eq!(
        updated["complete_candidate_write_allowlist"][2]["initially_present"],
        false
    );
    assert_eq!(
        updated["complete_candidate_write_allowlist"][2]["currently_present"],
        true
    );
    std::fs::write(
        workspace.join(&plan.task_facts.module_entry),
        "changed unit observation",
    )
    .unwrap();
    assert!(super::input::specification(&plan, &plan.stages[0], &initial, &initial).is_err());
}

#[tokio::test]
async fn actual_factory_constructs_three_file_tools_and_prepares_without_provider_or_effect() {
    use kolyan_agent::{
        AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentPermissions, AgentSelector,
        EnvironmentTool, EnvironmentToolFactory,
    };
    use kolyan_core::ToolExecutor;
    use std::{os::unix::fs::PermissionsExt, sync::Arc};
    let root = tempfile::Builder::new()
        .prefix("kolyan-self-iteration-factory-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let workspace = root.join("worktree");
    let control = root.join("control");
    for path in [
        &workspace,
        &control,
        &control.join("state"),
        &control.join("staging"),
        &workspace.join(".git"),
    ] {
        std::fs::create_dir(path).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).unwrap();
    }
    let evidence = Arc::new(super::super::evidence::Evidence::new(
        &control.join("actual.jsonl"),
    ));
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    super::super::tools::worker::initialize_worker(&control, &evidence, &installation).unwrap();
    let plan = super::Plan {
        task: plan(),
        run: super::RunConfig {
            worktree: workspace.clone(),
            host_private: root.clone(),
            branch: "unit-factory".into(),
            head: "0".repeat(40),
            source_snapshot_sha256: "0".repeat(64),
            baseline_manifest: "unit-baseline".into(),
            baseline_manifest_sha256: "0".repeat(64),
            baseline_files: 0,
            baseline_server_tests: 0,
        },
    };
    let path = workspace.join(&plan.allowlist[0]);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "original unit fixture").unwrap();
    let permissions = AgentPermissions {
        tools: [
            EnvironmentTool::Read,
            EnvironmentTool::Write,
            EnvironmentTool::Edit,
        ]
        .into(),
        delegation: Default::default(),
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "factory-smoke".into(),
        revision: "r1".into(),
        display_name: None,
        model: kolyan_model::ModelRef::new("fixture", "no-provider"),
        instructions: "Only unit preparation; no model or effect.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let key = definition.key();
    let mut catalog = AgentCatalog::new(1).unwrap();
    catalog.register(definition).unwrap();
    let snapshot = catalog
        .resolve(
            &AgentSelector::Named(key),
            "factory-smoke-instance",
            &permissions,
            &permissions,
        )
        .unwrap();
    let factory = tools::Factory {
        plan: plan.clone(),
        control: control.clone(),
        evidence: evidence.clone(),
    };
    let set = factory
        .build(
            &snapshot,
            &kolyan_server::ExecutionRef {
                session_id: "factory-session".into(),
                execution_id: "factory-execution".into(),
                turn_id: "factory-turn".into(),
            },
        )
        .unwrap();
    let definitions: BTreeSet<_> = set
        .definitions
        .iter()
        .map(|tool| tool.name.as_str())
        .collect();
    let mut observations = Vec::new();
    for (name, arguments) in [
        ("file.read", json!({"path":plan.allowlist[0]})),
        (
            "file.write",
            json!({"path":plan.allowlist[0],"content":"not executed"}),
        ),
        (
            "file.edit",
            json!({"path":plan.allowlist[0],"old_text":"original","new_text":"not executed"}),
        ),
    ] {
        let prepared = set
            .executor
            .prepare(ToolCall {
                id: format!("smoke-{name}"),
                name: name.into(),
                arguments,
            })
            .await;
        observations.push(json!({"tool":name,"prepared":prepared.as_ref().ok(),"error":prepared.as_ref().err().map(|e|format!("{e:?}")),"decision":prepared.as_ref().ok().map(|p|set.policy.decide_prepared(p,&Default::default()))}));
    }
    evidence.append(json!({"event":"self_iteration_factory_smoke","definitions":definitions,"preparations":observations,"provider_requests":0,"execute_invocations":0,"physical_content":std::fs::read_to_string(&path).unwrap()})).unwrap();
    println!(
        "SELF_ITERATION_FACTORY_TRACE={}",
        control.join("actual.jsonl").display()
    );
    assert_eq!(definitions, ["file.read", "file.write", "file.edit"].into());
    assert!(
        observations
            .iter()
            .all(|row| row["error"].is_null() && row["decision"]["kind"] == "allow")
    );
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "original unit fixture"
    );
}

#[tokio::test]
async fn edit_manifest_accepts_real_preparation_read_write_claim_but_not_other_paths() {
    use kolyan_core::ToolExecutor;
    use std::os::unix::fs::PermissionsExt;
    let root = tempfile::tempdir().unwrap();
    let root = root.path().canonicalize().unwrap();
    let workspace = root.join("workspace");
    let stage = root.join("staging");
    std::fs::create_dir(&workspace).unwrap();
    std::fs::create_dir(&stage).unwrap();
    std::fs::set_permissions(&stage, std::fs::Permissions::from_mode(0o700)).unwrap();
    let worker = root.join("prepare-only-worker");
    std::fs::write(
        &worker,
        "explicit test preparation identity; never executable or launched",
    )
    .unwrap();
    let plan = super::Plan {
        task: plan(),
        run: super::RunConfig {
            worktree: workspace.clone(),
            host_private: root.clone(),
            branch: "test-fixture".into(),
            head: "0".repeat(40),
            source_snapshot_sha256: "0".repeat(64),
            baseline_manifest: "baseline".into(),
            baseline_manifest_sha256: "0".repeat(64),
            baseline_files: 0,
            baseline_server_tests: 0,
        },
    };
    let path = workspace.join(&plan.allowlist[0]);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, "original unit fixture").unwrap();
    let inner = kolyan_tools::IsolatedFileTools::new(kolyan_tools::IsolatedFileConfig {
        workspace: workspace.clone(),
        staging_root: stage,
        worker,
        protected_roots: vec![],
        file_limits: kolyan_tools::FileOperationLimits {
            max_read_bytes: 262144,
            max_write_bytes: 262144,
        },
        max_output_bytes: 1048576,
        timeout: std::time::Duration::from_secs(30),
    })
    .unwrap();
    let prepared = ToolExecutor::prepare(&inner, ToolCall {
        id:"unit-edit".into(),name:"file.edit".into(),
        arguments:json!({"path":plan.allowlist[0],"old_text":"original","new_text":"replacement"}),
    }).await.unwrap();
    assert_eq!(
        prepared.claim().capabilities,
        [
            kolyan_policy::Capability::FilesystemRead,
            kolyan_policy::Capability::FilesystemWrite
        ]
        .into()
    );
    assert_eq!(
        prepared.claim().effects,
        [kolyan_policy::Effect::Read, kolyan_policy::Effect::Update].into()
    );
    let policy = tools::policy(&plan, &workspace);
    assert_eq!(
        policy.decide_prepared(&prepared, &Default::default()).kind,
        kolyan_policy::PolicyDecisionKind::Allow
    );
    std::fs::write(workspace.join("outside-allowlist"), "original").unwrap();
    let foreign = ToolExecutor::prepare(&inner, ToolCall {id:"foreign".into(),name:"file.edit".into(),arguments:json!({"path":"outside-allowlist","old_text":"original","new_text":"replacement"})}).await.unwrap();
    assert_eq!(
        policy.decide_prepared(&foreign, &Default::default()).kind,
        kolyan_policy::PolicyDecisionKind::Deny
    );
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "original unit fixture"
    );
}

#[test]
fn receipt_model_query_requires_exact_structured_call_and_step() {
    let call = ToolCall {
        id: "call-exact".into(),
        name: "file.edit".into(),
        arguments: json!({"path":"candidate.rs","old_text":"old","new_text":"new"}),
    };
    let step = kolyan_core::StepResult {
        step_id: "step-2".into(),
        outcome: kolyan_core::StepOutcome::ToolCalls,
        response: kolyan_model::ModelResponse {
            id: "response".into(),
            model: kolyan_model::ModelRef::new("fixture", "query"),
            content: vec![kolyan_model::ContentBlock::ToolCall { call: call.clone() }],
            structured_output: None,
            stop_reason: kolyan_model::StopReason::ToolUse,
            usage: Default::default(),
            metadata: json!({}),
        },
    };
    let actual = serde_json::to_value(&step).unwrap();
    assert!(super::driver::exact_step_call(&actual, "step-2", &call));
    assert!(!super::driver::exact_step_call(&actual, "step-1", &call));
    for field in ["id", "name", "arguments"] {
        let mut changed = actual.clone();
        changed["response"]["content"][0]["call"][field] = if field == "arguments" {
            json!({"path":"other"})
        } else {
            json!("other")
        };
        assert!(!super::driver::exact_step_call(&changed, "step-2", &call));
    }
    let mut prose = step;
    prose.response.content = vec![
        kolyan_model::ContentBlock::Text {
            text: serde_json::to_string(&call).unwrap(),
        },
        kolyan_model::ContentBlock::Reasoning {
            text: call.id.clone(),
            opaque: None,
        },
    ];
    assert!(!super::driver::exact_step_call(
        &serde_json::to_value(prose).unwrap(),
        "step-2",
        &call
    ));
    assert!(!super::driver::exact_step_call(&json!({}), "step-2", &call));
}

#[test]
fn general_task_fixture_has_no_run_coordinates_and_run_config_is_strict() {
    let task: serde_json::Value =
        serde_json::from_str(include_str!("../../fixtures/agent/self_iteration.json")).unwrap();
    for field in [
        "worktree",
        "host_private",
        "baseline_files",
        "baseline_manifest",
        "branch",
        "head",
    ] {
        assert!(task.get(field).is_none());
    }
    let outside = tempfile::NamedTempFile::new().unwrap();
    assert!(super::RunConfig::load(outside.path()).is_err());
    let missing = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("tests/config/missing-self-iteration.local.json");
    assert!(super::RunConfig::load(&missing).is_err());
    let unknown = json!({"api_key":"must-not-be-config"});
    assert!(serde_json::from_value::<super::RunConfig>(unknown).is_err());
}

#[test]
fn experiment_plan_has_exact_candidate_scope_and_real_single_deployment() {
    let plan = plan();
    assert_eq!(plan.schema_version, 1);
    assert_eq!(plan.allowlist.len(), 4);
    assert_eq!(plan.expected_states.len(), 7);
    assert_eq!(
        plan.expected_states
            .iter()
            .map(|c| &c.state)
            .collect::<BTreeSet<_>>()
            .len(),
        7
    );
    assert_eq!(
        plan.expected_states
            .iter()
            .filter(|c| c.terminal)
            .map(|c| c.state.as_str())
            .collect::<BTreeSet<_>>(),
        ["Completed", "Failed", "Cancelled"].into()
    );
    assert_eq!(
        plan.stages
            .iter()
            .map(|s| s.id.as_str())
            .collect::<Vec<_>>(),
        ["inspect", "implement", "tests", "review"]
    );
    assert_eq!(
        plan.stages.iter().map(|s| s.writable).collect::<Vec<_>>(),
        [false, true, true, false]
    );
    assert_eq!(
        super::super::deployments()
            .iter()
            .filter(|d| d.family == plan.family
                && d.surface == plan.surface
                && d.model == plan.model)
            .count(),
        1
    );
}

#[test]
fn exact_candidate_guard_rejects_shell_parent_paths_and_readonly_writes() {
    let plan = plan();
    let mut call = ToolCall {
        id: "actual-id".into(),
        name: "file.write".into(),
        arguments: json!({"path":plan.allowlist[0],"content":"unit-only guard input; never executed"}),
    };
    assert!(tools::permitted(&plan.allowlist, true, &call).is_ok());
    assert!(tools::permitted(&plan.allowlist, false, &call).is_err());
    call.arguments["path"] = json!("crates/kolyan-server/src/tasks/types.rs/child");
    assert!(tools::permitted(&plan.allowlist, true, &call).is_err());
    call.arguments["path"] = json!("../outside");
    assert!(tools::permitted(&plan.allowlist, true, &call).is_err());
    call.name = "shell".into();
    assert!(tools::permitted(&plan.allowlist, true, &call).is_err());
}

#[test]
fn inventory_detects_symlink_retarget_even_when_referent_bytes_match() {
    let root = tempfile::tempdir().unwrap();
    std::process::Command::new("/usr/bin/git")
        .arg("init")
        .arg(root.path())
        .output()
        .unwrap();
    std::fs::write(root.path().join("one"), "same bytes").unwrap();
    std::fs::write(root.path().join("two"), "same bytes").unwrap();
    std::os::unix::fs::symlink("one", root.path().join("link")).unwrap();
    let before = baseline::collect(root.path()).unwrap();
    assert_eq!(before["link"].sha256, baseline::digest(b"same bytes"));
    std::fs::remove_file(root.path().join("link")).unwrap();
    std::os::unix::fs::symlink("two", root.path().join("link")).unwrap();
    let after = baseline::collect(root.path()).unwrap();
    assert_eq!(baseline::changed(&before, &after), ["link".into()].into());
    std::fs::remove_file(root.path().join("link")).unwrap();
    std::os::unix::fs::symlink("/etc/passwd", root.path().join("link")).unwrap();
    assert!(
        baseline::collect(root.path())
            .unwrap_err()
            .contains("outside worktree")
    );
}

#[test]
#[ignore = "Requires the explicit authorized untouched worktree RunConfig; read-only preflight, no network"]
fn human_provided_initial_manifest_is_checked_without_mutating_worktree() {
    let path =
        std::env::var_os("KOLYAN_SELF_ITERATION_RUN_CONFIG").expect("explicit RunConfig required");
    let run = super::RunConfig::load(std::path::Path::new(&path)).unwrap();
    let plan = super::Plan { task: plan(), run };
    let before = baseline::collect(&plan.run.worktree).unwrap();
    baseline::verify_initial(&plan, &before).unwrap();
    assert_eq!(before.len(), plan.run.baseline_files);
    assert!(!before.contains_key(&plan.allowlist[2]));
    assert!(!before.contains_key(&plan.allowlist[3]));
}
