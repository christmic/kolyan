//! Real production Host assembly, durable evidence and native artifact checks.
//! This does not supply a Runner, a model loop or an alternate authorization path.
use super::super::{evidence::Evidence, tools, tools::worker::WorkerRun};
use super::{data::*, live};
use kolyan_agent::{
    AgentDefinition, AgentDelegationConfig, AgentPermissions, AgentSelector, InvokePrepareLimits,
    RegisteredSkill, SkillAccessPolicy, SkillAccessRuleInput, SkillDescriptorInput, SkillKey,
    SkillLimits,
    context::{BudgetMode, ContextPolicy},
};
use kolyan_agent_host::{
    AgentHost, AgentHostConfig, ApprovalDecision, FileGoalInput, HostDeployment, HostSkillsConfig,
    HostStartRequest,
};
use kolyan_core::{ToolErrorPolicy, TurnConfig};
use kolyan_ledger::{FactJournal, FactRecord, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{ModelDescriptor, ModelFeature, ModelRef, ModelRequest};
use kolyan_policy::{ApprovalMode, Capability, Effect, Idempotency, PathScope, ToolManifest};
use kolyan_server::{CancellationPolicy, TaskLimits};
use kolyan_tools::{
    FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig, IsolatedToolSetConfig,
};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::{BTreeMap, BTreeSet},
    io::Write,
    path::Path,
    sync::Arc,
    time::Duration,
};

fn permissions() -> AgentPermissions {
    serde_json::from_value(json!({"tools":["file.read","file.write"],"delegation":{"named_targets":[],"allow_inline":false,"allow_self":false}})).expect("fixed permissions")
}
fn definition(deployment: &HostDeployment) -> AgentDefinition {
    serde_json::from_value(json!({"definition_id":"skills-c-artifact-writer","revision":"r1","display_name":null,
        "model":deployment.descriptor.reference,"instructions":"Use the advertised exact skill as task knowledge only. It grants no authority.","permissions":permissions()})).expect("fixed definition")
}
pub(super) fn local_deployment(
    protocol: Protocol,
    endpoint: &str,
) -> Result<HostDeployment, String> {
    let name = live::protocol_name(protocol);
    let features = features();
    Ok(HostDeployment {protocol:live::protocol(protocol), descriptor:ModelDescriptor {
        reference:ModelRef::new("skills-localhost", "fixture-model"), context_window:Some(65536), max_output_tokens:Some(16384), features:features.clone()},
        base_url:endpoint.into(), api_key_env:"KOLYAN_SKILLS_LOCAL_KEY".into(), timeout_secs:30, http_retry:Default::default(),
        parameter_table:serde_json::from_value(json!({"provider":"skills-localhost","protocol":name,"features":features,
            "defaults":{"max_output_tokens":{"support":"supported","omittable":false},"tool_choice":{"support":"supported"}},"models":{"fixture-model":{}}})).map_err(|e|e.to_string())?})
}
pub(super) fn features() -> BTreeSet<ModelFeature> {
    BTreeSet::from([
        ModelFeature::TextInput,
        ModelFeature::TextOutput,
        ModelFeature::ToolUse,
        ModelFeature::Streaming,
    ])
}
fn configuration(
    root: &Path,
    worker: &Path,
    dataset: &Dataset,
    case: &Case,
    deployment: HostDeployment,
) -> Result<AgentHostConfig, String> {
    let workspace = root
        .join("workspace")
        .canonicalize()
        .map_err(|e| e.to_string())?;
    let manifests = [
        (
            "file.read",
            vec![Capability::FilesystemRead],
            vec![Effect::Read],
            Idempotency::Idempotent,
        ),
        (
            "file.write",
            vec![Capability::FilesystemWrite],
            vec![Effect::Create, Effect::Update],
            Idempotency::NonIdempotent,
        ),
        (
            "file.edit",
            vec![Capability::FilesystemRead, Capability::FilesystemWrite],
            vec![Effect::Read, Effect::Update],
            Idempotency::NonIdempotent,
        ),
        (
            "shell",
            vec![
                Capability::FilesystemRead,
                Capability::FilesystemWrite,
                Capability::ProcessExecute,
            ],
            vec![
                Effect::Read,
                Effect::Create,
                Effect::Update,
                Effect::Delete,
                Effect::Execute,
            ],
            Idempotency::NonIdempotent,
        ),
    ]
    .into_iter()
    .map(|(name, capabilities, effects, idempotency)| ToolManifest {
        tool_name: name.into(),
        capabilities: capabilities.into_iter().collect(),
        effects: effects.into_iter().collect(),
        path_scopes: vec![PathScope::new(
            workspace.join("safe").to_str().expect("UTF8 fixture"),
        )],
        idempotency,
        approval: if (name == "file.write" && matches!(case.approval, Approval::WriteAlways))
            || (name == "file.read" && matches!(case.approval, Approval::ReadAlways))
        {
            ApprovalMode::Always
        } else {
            ApprovalMode::Never
        },
    })
    .collect();
    let template:ModelRequest=serde_json::from_value(json!({"request_id":"template","model":deployment.descriptor.reference,
        "system":[],"messages":[],"tools":[],"tool_choice":"auto","output_format":null,"prompt_cache":null,"reasoning":null,
        "max_output_tokens":dataset.network.output_tokens,"extensions":{}})).map_err(|e|e.to_string())?;
    Ok(AgentHostConfig {
        state_root: root.join("state"),
        host_id: "skills-c-host-v1".into(),
        catalog: vec![definition(&deployment)],
        deployment,
        context: ContextPolicy {
            id: "skills-c-inspection".into(),
            revision: "1".into(),
            mode: BudgetMode::Inspect,
            max_serialized_bytes: 2 * 1024 * 1024,
            max_messages: 256,
            max_content_blocks: 1024,
            context_limit_tokens: None,
            output_reserve_tokens: dataset.network.output_tokens,
        },
        permissions: permissions(),
        environment: IsolatedToolSetConfig {
            files: IsolatedFileConfig {
                workspace: workspace.clone(),
                worker: worker.into(),
                staging_root: root.join("staging"),
                protected_roots: vec![],
                file_limits: FileOperationLimits {
                    max_read_bytes: 65536,
                    max_write_bytes: 65536,
                },
                max_output_bytes: 1024 * 1024,
                timeout: Duration::from_secs(30),
            },
            shell: IsolatedShellConfig {
                workspace,
                protected_roots: vec![],
                max_command_bytes: 4096,
                max_output_bytes: 65536,
                timeout: Duration::from_secs(30),
            },
        },
        tool_manifests: manifests,
        tool_scope: "safe".into(),
        allow_shell: false,
        delegation: AgentDelegationConfig {
            limits: InvokePrepareLimits {
                max_children: 1,
                max_parallel: 1,
                max_child_input_bytes: 4096,
                max_output_bytes: 65536,
                admission_timeout_ms: 5000,
            },
            approval: ApprovalMode::Never,
        },
        tool_error_policy: if matches!(case.tool_error_policy, ErrorPolicy::ContinueBatch) {
            ToolErrorPolicy::ContinueBatch
        } else {
            ToolErrorPolicy::FailTurn
        },
        max_parallel: 1,
        template,
        turn_config: TurnConfig {
            max_steps: dataset.network.model_step_limit,
            max_tool_calls: Some(16),
            deadline: Some(Duration::from_millis(dataset.network.row_deadline_ms)),
        },
        task_limits: TaskLimits {
            max_depth: 1,
            max_invocations: 1,
            max_attempts: 1,
            max_tokens: None,
            max_steps_per_turn: u32::try_from(dataset.network.model_step_limit)
                .map_err(|e| e.to_string())?,
        },
        cancellation_policy: CancellationPolicy::AllInvocations,
    })
}
fn skills_config(
    agent: kolyan_agent::AgentKey,
    selected: SkillKey,
    deny: bool,
) -> Result<HostSkillsConfig, String> {
    Ok(HostSkillsConfig {
        namespace: "skills-c-v1".into(),
        limits: SkillLimits::default(),
        policy: if deny {
            SkillAccessPolicy::deny_all()
        } else {
            SkillAccessPolicy::new(
                "skills-c-acl".into(),
                "1".into(),
                vec![SkillAccessRuleInput {
                    agent,
                    logical_session_id: "skills-c-logical".into(),
                    task_id: None,
                    invocation_id: None,
                    skills: [selected].into(),
                }],
            )
            .map_err(|e| e.to_string())?
        },
    })
}
async fn open(
    root: &Path,
    worker: &Path,
    dataset: &Dataset,
    case: &Case,
    deployment: HostDeployment,
    deny: bool,
) -> Result<Arc<AgentHost>, String> {
    let agent = definition(&deployment).key();
    let selected = dataset.selected()?;
    let skills = skills_config(
        agent,
        SkillKey::new(selected.id.clone(), selected.revision.clone()).map_err(|e| e.to_string())?,
        deny,
    )?;
    let config = configuration(root, worker, dataset, case, deployment)?;
    tokio::task::spawn_blocking(move || {
        AgentHost::open_with_skills(config, skills)
            .map(Arc::new)
            .map_err(|e| e.to_string())
    })
    .await
    .map_err(|e| e.to_string())?
}
pub(super) async fn scenario(
    dataset: &Dataset,
    case: &Case,
    protocol: Protocol,
    selector: Selector,
    evidence: &Path,
    installation: &WorkerRun,
    deployment: Option<HostDeployment>,
) -> Value {
    let root = evidence.join(format!("{}-{protocol:?}-{selector:?}", case.id));
    let mut row = json!({"case_id":case.id,"protocol":protocol,"selector":selector,"fixture":case,"dataset":dataset,"root":root,"error":null,"scripted_localhost":deployment.is_none(),"actual_llm":deployment.is_some()});
    let result = run(
        dataset,
        case,
        protocol,
        selector,
        &root,
        installation,
        deployment,
        &mut row,
    )
    .await;
    if let Err(error) = result {
        row["error"] = json!(error);
    }
    match SqliteLedger::open(root.join("state/executions.sqlite")).and_then(|l| l.events_after(0)) {
        Ok(events) => row["ledger"] = json!(events),
        Err(e) => row["ledger_error"] = json!(e.to_string()),
    }
    match facts(
        &root,
        &case.id,
        row.get("catalog_stream").and_then(Value::as_str),
    ) {
        Ok(f) => row["facts"] = json!(f),
        Err(e) => row["facts_error"] = json!(e),
    }
    match artifacts(&root) {
        Ok(artifacts) => row["required_artifact_audit_after_run"] = json!(artifacts),
        Err(e) => row["artifact_error"] = json!(e),
    }
    row["file_bytes"] = match std::fs::read(root.join("workspace").join(&dataset.artifact.path)) {
        Ok(bytes) => {
            row["file_sha256"] = json!(format!("{:x}", Sha256::digest(&bytes)));
            json!(bytes)
        }
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => Value::Null,
        Err(e) => json!({"error":e.to_string()}),
    };
    row
}
#[allow(clippy::too_many_arguments)] // Exact fixture/host coordinates, not a public production API.
async fn run(
    dataset: &Dataset,
    case: &Case,
    protocol: Protocol,
    selector: Selector,
    root: &Path,
    installation: &WorkerRun,
    deployment: Option<HostDeployment>,
    row: &mut Value,
) -> Result<(), String> {
    use std::os::unix::fs::DirBuilderExt;
    std::fs::create_dir(root).map_err(|e| e.to_string())?;
    for name in ["workspace", "state", "staging"] {
        std::fs::DirBuilder::new()
            .mode(0o700)
            .create(root.join(name))
            .map_err(|e| e.to_string())?;
    }
    std::fs::create_dir(root.join("workspace/safe")).map_err(|e| e.to_string())?;
    std::fs::write(root.join("workspace/safe/seed.txt"), "barrier seed\n")
        .map_err(|e| e.to_string())?;
    let worker_evidence = Evidence::new(&root.join("worker.jsonl"));
    tools::initialize_worker(root, &worker_evidence, installation)?;
    let worker = tools::worker::verified_worker(root, &worker_evidence)?;
    let local = deployment.is_none();
    let mut backend = if local {
        Some(live::Backend::start(protocol)?)
    } else {
        None
    };
    let deployment = match deployment {
        Some(d) => d,
        None => local_deployment(protocol, &backend.as_ref().expect("local backend").endpoint)?,
    };
    // A rebuild uses the same immutable deployment and pinned worker, not reinstallation.
    let deployment_bytes = serde_json::to_value(&deployment).map_err(|e| e.to_string())?;
    row["deployment"] = deployment_bytes.clone();
    let mut host = open(root, &worker, dataset, case, deployment, false).await?;
    let mut registrations = Vec::new();
    for skill in &dataset.skills {
        let mut body = skill.body.clone();
        if skill.revision == "r2"
            && matches!(
                case.mutation,
                Mutation::MaliciousBodyFixtureAndExplicitForbiddenCalls
            )
        {
            body.push_str(&dataset.malicious_fixture_appendix);
        }
        registrations.push(
            host.skill_catalog()
                .ok_or("catalog missing")?
                .register(
                    SkillDescriptorInput {
                        key: SkillKey::new(skill.id.clone(), skill.revision.clone())
                            .map_err(|e| e.to_string())?,
                        title: skill.title.clone(),
                        description: skill.description.clone(),
                    },
                    &body,
                )
                .map_err(|e| e.to_string())?,
        );
    }
    let selected = registrations
        .iter()
        .find(|s| s.metadata().descriptor().key.revision == "r2")
        .ok_or("r2 missing")?
        .clone();
    row["catalog_stream"] = json!(host.skill_catalog().ok_or("catalog missing")?.stream_id());
    row["registrations"] = json!(registrations);
    if let Some(b) = backend.as_mut() {
        let mut frames = Vec::new();
        for key in &case.frames {
            let mut frame = dataset.frames[key].clone();
            for call in &mut frame.calls {
                if call.name == "skill.load" {
                    call.arguments["content_digest"] = json!(selected.metadata().content_digest());
                }
            }
            frames.push(frame);
        }
        let revoke = if matches!(
            case.mutation,
            Mutation::RevokeAfterModelLoadCallBeforePrepare
        ) {
            Some((host.skill_catalog().unwrap().clone(), selected.clone()))
        } else {
            None
        };
        b.set_script(frames, revoke)?;
    }
    let agent = definition(
        &serde_json::from_value::<HostDeployment>(deployment_bytes.clone())
            .map_err(|e| e.to_string())?,
    );
    let input = HostStartRequest {
        task_id: case.id.clone(),
        invocation_id: "root".into(),
        attempt_id: "attempt-r1".into(),
        turn_id: "turn-skills-c".into(),
        execution_id: "execution-skills-c".into(),
        selector: match selector {
            Selector::Named => AgentSelector::Named(agent.key()),
            Selector::Inline => AgentSelector::Inline(agent),
        },
        requested_permissions: permissions(),
        objective: dataset.initial_input.clone(),
        messages: serde_json::from_value(
            json!([{"role":"user","content":[{"type":"text","text":dataset.initial_input}]}]),
        )
        .map_err(|e| e.to_string())?,
        goals: vec![FileGoalInput {
            id: "exact-artifact".into(),
            path: dataset.artifact.path.clone(),
            expected_content: dataset.artifact.expected_utf8.clone(),
        }],
    };
    row["start_input"] = json!(input);
    let start = host.start("skills-c-logical".into(), input).await;
    row["start_error"] = json!(start.as_ref().err().map(ToString::to_string));
    let before = host
        .query("skills-c-logical".into(), case.id.clone())
        .await
        .map_err(|e| e.to_string())?;
    row["before"] = json!(before);
    row["ledger_before"] = json!(
        SqliteLedger::open(root.join("state/executions.sqlite"))
            .map_err(|e| e.to_string())?
            .events_after(0)
            .map_err(|e| e.to_string())?
    );
    if let Some(pending) = before.approvals.first() {
        if let Some(b) = &backend {
            row["http_before_decision"] = json!(b.requests().len());
        }
        row["file_before_decision"] =
            json!(std::fs::read(root.join("workspace").join(&dataset.artifact.path)).ok());
        if matches!(
            case.mutation,
            Mutation::RevokeSavedR2AfterWriteApproval
                | Mutation::RevokeAfterCompletedLoadBeforeNextStream
        ) {
            revoke(host.skill_catalog().ok_or("catalog missing")?, &selected)?;
        }
        let deny_acl = matches!(case.mutation, Mutation::RemoveAclAfterWriteApproval);
        drop(host);
        host = open(
            root,
            &tools::worker::verified_worker(root, &worker_evidence)?,
            dataset,
            case,
            serde_json::from_value(deployment_bytes).map_err(|e| e.to_string())?,
            deny_acl,
        )
        .await?;
        row["rebuilt"] = json!(
            host.query("skills-c-logical".into(), case.id.clone())
                .await
                .map_err(|e| e.to_string())?
        );
        row["ledger_after_rebuild"] = json!(
            SqliteLedger::open(root.join("state/executions.sqlite"))
                .map_err(|e| e.to_string())?
                .events_after(0)
                .map_err(|e| e.to_string())?
        );
        let deny = matches!(
            case.mutation,
            Mutation::RevokeSavedR2AfterWriteApproval | Mutation::RemoveAclAfterWriteApproval
        );
        row["decision"] = json!(if deny { "deny" } else { "accept" });
        let decision = host
            .decide_approval(
                "skills-c-logical".into(),
                case.id.clone(),
                pending.invocation_id.clone(),
                pending.approval_id.clone(),
                if deny {
                    ApprovalDecision::Deny
                } else {
                    ApprovalDecision::Accept
                },
            )
            .await;
        row["decision_error"] = json!(decision.as_ref().err().map(ToString::to_string));
        if !matches!(
            case.mutation,
            Mutation::RevokeAfterCompletedLoadBeforeNextStream
        ) {
            decision.map_err(|e| e.to_string())?;
        }
    }
    row["final"] = json!(
        host.query("skills-c-logical".into(), case.id.clone())
            .await
            .map_err(|e| e.to_string())?
    );
    if let Some(mut backend) = backend {
        let finish = backend.finish();
        row["http"] = json!(backend.requests());
        row["http_error"] = json!(finish.as_ref().err());
        finish?;
    }
    if !matches!(
        case.mutation,
        Mutation::RevokeAfterModelLoadCallBeforePrepare
            | Mutation::RevokeAfterCompletedLoadBeforeNextStream
    ) {
        start.map_err(|e| e.to_string())?;
    }
    Ok(())
}
pub(super) fn revoke(
    catalog: &kolyan_agent::SkillCatalog,
    selected: &RegisteredSkill,
) -> Result<(), String> {
    catalog
        .revoke(
            &selected.metadata().descriptor().key,
            selected.reference(),
            "c-revoke-r2",
            "Deterministic host revocation fixture",
        )
        .map(|_| ())
        .map_err(|e| e.to_string())
}
fn facts(root: &Path, task: &str, catalog: Option<&str>) -> Result<Vec<FactRecord>, String> {
    let journal =
        SqliteFactJournal::open(root.join("state/facts.sqlite")).map_err(|e| e.to_string())?;
    let mut result = BTreeMap::new();
    let mut todo = vec![task.to_owned()];
    if let Some(c) = catalog {
        todo.push(c.into());
    }
    let mut streams = BTreeSet::new();
    while let Some(stream) = todo.pop() {
        if !streams.insert(stream.clone()) {
            continue;
        }
        let mut cursor = 0;
        loop {
            let records = journal
                .read(&stream, cursor, 1024)
                .map_err(|e| e.to_string())?;
            if records.is_empty() {
                break;
            }
            for record in records {
                if record.stream_id != stream || record.position != cursor + 1 {
                    return Err("foreign/noncontiguous fact".into());
                }
                cursor = record.position;
                if record.draft.kind == "agent.context.bound" {
                    // Diagnostic lookup of the actual saved instance namespace;
                    // no owner is reserved, no model identity is trusted, and no
                    // second namespace hash/publisher implementation is created.
                    let identity = record.draft.payload["snapshot"]["identity"]["instance_id"]
                        .as_str()
                        .ok_or("saved instance identity missing")?;
                    let mut parts = identity.split(':');
                    if parts.next() != Some("agent-instance") {
                        return Err("unexpected instance protocol".into());
                    }
                    let namespace = parts.next().ok_or("instance namespace missing")?;
                    if namespace.len() != 64
                        || !namespace.bytes().all(|b| b.is_ascii_hexdigit())
                        || parts.next().and_then(|p| p.parse::<u64>().ok()).is_none()
                        || parts.next().is_some()
                    {
                        return Err("invalid instance coordinates".into());
                    }
                    todo.push(format!("agent-instances:{namespace}"));
                }
                for cause in &record.draft.causes {
                    todo.push(cause.stream_id.clone());
                }
                result.insert((stream.clone(), cursor), record);
                if result.len() > 16384 {
                    return Err("fact bound exceeded".into());
                }
            }
        }
    }
    Ok(result.into_values().collect())
}
// Host diagnostics happen after execution. These audit reads do not represent
// Skill tool reads, and never put unselected knowledge into a model request.
fn artifacts(root: &Path) -> Result<Vec<Value>, String> {
    let directory = root.join("state/artifacts");
    let store = kolyan_trace::ArtifactStore::new(&directory, 16 * 1024 * 1024)
        .map_err(|e| e.to_string())?;
    let mut names = Vec::new();
    for entry in std::fs::read_dir(&directory).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let name = entry
            .file_name()
            .into_string()
            .map_err(|_| "non UTF8 artifact name")?;
        if name.len() == 64 && name.bytes().all(|b| b.is_ascii_hexdigit()) {
            names.push(name);
        }
    }
    names.sort();
    if names.len() > 256 {
        return Err("artifact cardinality bound".into());
    }
    let mut bytes = 0u64;
    let mut result = Vec::new();
    for name in names {
        let metadata =
            std::fs::symlink_metadata(directory.join(&name)).map_err(|e| e.to_string())?;
        if !metadata.is_file() || metadata.file_type().is_symlink() {
            return Err("invalid artifact leaf".into());
        }
        bytes = bytes
            .checked_add(metadata.len())
            .ok_or("artifact byte overflow")?;
        if bytes > 64 * 1024 * 1024 {
            return Err("artifact audit byte bound".into());
        }
        let required = directory.join(format!("{name}.required")).exists();
        let reference = kolyan_trace::ArtifactRef {
            digest: name,
            byte_length: metadata.len(),
            retention: if required {
                kolyan_trace::Retention::Required
            } else {
                kolyan_trace::Retention::Optional
            },
        };
        let bytes = store
            .read(&reference, 16 * 1024 * 1024)
            .map_err(|e| e.to_string())?;
        result.push(json!({"reference":reference,"bytes":bytes,"json":serde_json::from_slice::<Value>(&bytes).ok(),"meaning":"bounded_host_audit_after_execution_not_skill_read"}));
    }
    Ok(result)
}
pub(super) fn save(path: &Path, rows: &[Value]) -> Result<Vec<Value>, String> {
    let mut file = std::fs::File::create(path).map_err(|e| e.to_string())?;
    for row in rows {
        writeln!(file, "{row}").map_err(|e| e.to_string())?;
    }
    file.flush().map_err(|e| e.to_string())?;
    file.sync_all().map_err(|e| e.to_string())?;
    drop(file);
    std::fs::read_to_string(path)
        .map_err(|e| e.to_string())?
        .lines()
        .map(|s| serde_json::from_str(s).map_err(|e| e.to_string()))
        .collect()
}
