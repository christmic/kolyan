//! Actual Runner/Server/Runtime/native hook consumers; only model frames are scripted.
#![cfg(target_os = "macos")]

use futures_util::stream;
use kolyan_agent::context::{BudgetMode, ContextPolicy, SerializedByteEstimator};
use kolyan_agent::hooks::{
    AgentEffectHookBridge, HookAccessPolicy, HookAccessRule, HookCatalog, HookKey, HookManifest,
    HookPhase, HookScope, NativeHookHost,
};
use kolyan_agent::provider::{
    ContextPreparingProvider, ContextRecord, ContextRecordError, ContextRecorder,
};
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBindingStore,
    AgentPermissions, AgentRunner, AgentSelector, AgentSnapshot, EnvironmentTool,
    EnvironmentToolFactory, ProviderFactory, RootApprovalResumeRequest,
    RootInputPreparationRequest, RootRunRequest, RunnerError, RunnerToolSet,
};
use kolyan_core::{TurnConfig, TurnRequest};
use kolyan_ledger::{
    FactDraft, FactError, FactJournal, FactRecord, LedgerStore, SqliteFactJournal, SqliteLedger,
};
use kolyan_model::{
    ContentBlock, Message, MessageRole, ModelDescriptor, ModelEvent, ModelEventStream,
    ModelProvider, ModelRef, ModelRequest, ModelResponse, ProviderFuture, ToolChoice,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_runtime::effect_hooks::{
    CommittedEffectReceipt, EffectHookContext, EffectHookDecision, EffectHookFuture, EffectHookPort,
};
use kolyan_sandbox::{SandboxProcessEvent, sandbox_process_observation_channel};
use kolyan_server::{
    CancellationPolicy, ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService,
    SessionService, TaskCoordinator, TaskExecutionService, TaskLimits,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_tools::{IsolatedShellConfig, IsolatedShellTool};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::{BTreeSet, VecDeque},
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    path::{Path, PathBuf},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    schema_version: u32,
    hook_timeout_ms: u64,
    cleanup_wait_ms: u64,
    model: ModelRef,
    input: String,
    frames: Vec<ModelResponse>,
    cases: Vec<Case>,
}
#[derive(Clone, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    before: String,
    after: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Expected {
    id: String,
    completed: bool,
    receipt: usize,
    spawned: usize,
    verify: Option<String>,
}

struct Recorder(Mutex<fs::File>);
impl Recorder {
    fn append(&self, value: Value) {
        let mut file = self.0.lock().unwrap();
        writeln!(file, "{value}").unwrap();
        file.flush().unwrap();
        file.sync_all().unwrap();
    }
}
impl ContextRecorder for Recorder {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError> {
        let row = match record {
            ContextRecord::Prepared { source, prepared } => {
                json!({"event":"context_record","source":source,"prepared":prepared})
            }
            ContextRecord::Rejected {
                source,
                failure,
                preparation,
            } => {
                json!({"event":"context_rejected","source":source,"error":failure.to_string(),"preparation":preparation})
            }
        };
        self.append(row);
        Ok(())
    }
}
struct Scripted {
    frames: Mutex<VecDeque<ModelResponse>>,
    record: Arc<Recorder>,
}
impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.record
                .append(json!({"event":"request","request":request}));
            let response = self
                .frames
                .lock()
                .unwrap()
                .pop_front()
                .expect("data-owned offline frames");
            let event = ModelEvent::Completed(response);
            self.record
                .append(json!({"event":"model_event","content":event}));
            Ok(Box::pin(stream::iter(vec![Ok(event)])) as ModelEventStream)
        })
    }
}
struct Providers {
    plan: Plan,
    offset: usize,
    record: Arc<Recorder>,
}
impl ProviderFactory for Providers {
    type Provider = Scripted;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Self::Provider>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Scripted {
                frames: Mutex::new(self.plan.frames[self.offset..].to_vec().into()),
                record: self.record.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(32768),
                max_output_tokens: Some(256),
                features: Default::default(),
            },
            ContextPolicy {
                id: "hook-test-inspect".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 1024 * 1024,
                max_messages: 128,
                max_content_blocks: 256,
                context_limit_tokens: None,
                output_reserve_tokens: 256,
            },
            Arc::new(SerializedByteEstimator),
            self.record.clone(),
        ))
    }
}
struct Tools {
    workspace: PathBuf,
    approval: bool,
}
impl EnvironmentToolFactory for Tools {
    type Executor = IsolatedShellTool;
    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<Self::Executor>, RunnerError> {
        let executor = IsolatedShellTool::new(IsolatedShellConfig {
            workspace: self.workspace.clone(),
            protected_roots: vec![],
            max_command_bytes: 4096,
            max_output_bytes: 65536,
            timeout: Duration::from_secs(30),
        })
        .map_err(|e| RunnerError::Host(e.to_string()))?;
        let mut policy = PolicyEngine::default();
        policy.register(ToolManifest {
            tool_name: "shell".into(),
            capabilities: [
                Capability::FilesystemRead,
                Capability::FilesystemWrite,
                Capability::ProcessExecute,
            ]
            .into(),
            effects: [
                Effect::Read,
                Effect::Create,
                Effect::Update,
                Effect::Delete,
                Effect::Execute,
            ]
            .into(),
            path_scopes: vec![PathScope::new(self.workspace.to_string_lossy())],
            idempotency: Idempotency::NonIdempotent,
            approval: if self.approval {
                ApprovalMode::Always
            } else {
                ApprovalMode::Never
            },
        });
        policy.restrict_workspace(self.workspace.to_string_lossy());
        Ok(RunnerToolSet {
            executor,
            definitions: vec![IsolatedShellTool::tool_definition()],
            policy: Arc::new(policy),
        })
    }
}

#[derive(Clone)]
struct Journal {
    inner: SqliteFactJournal,
    streams: Arc<Mutex<BTreeSet<String>>>,
    fail_after: Arc<AtomicBool>,
}
impl Journal {
    fn records(&self) -> Vec<FactRecord> {
        self.streams
            .lock()
            .unwrap()
            .iter()
            .flat_map(|s| self.inner.read(s, 0, 1024).unwrap())
            .collect()
    }
}
impl FactJournal for Journal {
    fn read(&self, s: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(s.into());
        self.inner.read(s, after, limit)
    }
    fn append(
        &self,
        s: &str,
        head: u64,
        drafts: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(s.into());
        if self.fail_after.load(Ordering::SeqCst)
            && drafts
                .iter()
                .any(|d| d.kind == "agent.hook.effect.completed")
        {
            let saved = self.inner.read(s, 0, 3)?;
            if saved
                .first()
                .is_some_and(|r| r.draft.payload["event"]["payload"]["phase"] == "after_tool")
            {
                return Err(FactError::Storage(
                    "explicit observer publication fault".into(),
                ));
            }
        }
        self.inner.append(s, head, drafts)
    }
}
type Bridge = AgentEffectHookBridge<SqliteLedger, Journal>;
struct Probe {
    bridge: Bridge,
    before: Mutex<Option<EffectHookContext>>,
    receipt: Mutex<Option<(EffectHookContext, CommittedEffectReceipt)>>,
}
impl EffectHookPort for Probe {
    fn before_effect(&self, c: EffectHookContext) -> EffectHookFuture<'_, EffectHookDecision> {
        *self.before.lock().unwrap() = Some(c.clone());
        self.bridge.before_effect(c)
    }
    fn after_receipt(
        &self,
        c: EffectHookContext,
        r: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        *self.receipt.lock().unwrap() = Some((c.clone(), r.clone()));
        self.bridge.after_receipt(c, r)
    }
    fn verify_observation(
        &self,
        c: EffectHookContext,
        r: CommittedEffectReceipt,
    ) -> EffectHookFuture<'_, ()> {
        self.bridge.verify_observation(c, r)
    }
}
type Runner = AgentRunner<Journal, SqliteLedger, NoopTraceSink, FileSessionStore, Providers, Tools>;
type HostStores = (Arc<Recorder>, Journal, SqliteLedger, Arc<ArtifactStore>);

fn runner(
    root: &Path,
    plan: &Plan,
    case: &Case,
    probe: Arc<Probe>,
    stores: HostStores,
    offset: usize,
) -> Arc<Runner> {
    let (record, journal, ledger, artifacts) = stores;
    let sessions = FileSessionStore::new(root.join("sessions")).unwrap();
    let service = Arc::new(
        TaskExecutionService::new(
            TaskCoordinator::new(journal.clone()),
            SessionExecutionService::new(
                ExecutionService::new(ledger, NoopTraceSink).with_effect_hooks(probe),
                SessionService::new(sessions),
            )
            .with_context_policy(SessionContextPolicy::FullTrajectory),
        )
        .with_artifacts(ArtifactStore::new(root.join("artifacts"), 4 * 1024 * 1024).unwrap()),
    );
    let permission = AgentPermissions {
        tools: [EnvironmentTool::Shell].into(),
        ..Default::default()
    };
    Arc::new(
        AgentRunner::new(
            service,
            InstanceRegistry::new(Arc::new(journal.clone()), "hook-host", 16).unwrap(),
            AgentInvocationBindingStore::new(Arc::new(journal)),
            AgentCatalog::new(4).unwrap(),
            permission,
            (
                Providers {
                    plan: plan.clone(),
                    offset,
                    record,
                },
                Tools {
                    workspace: root.join("workspace"),
                    approval: case.mode == "approval",
                },
            ),
            artifacts,
        )
        .unwrap(),
    )
}

fn manifest(phase: HookPhase, timeout: u64) -> HookManifest {
    HookManifest {
        key: HookKey {
            id: if phase == HookPhase::BeforeTool {
                "before"
            } else {
                "after"
            }
            .into(),
            revision: "1".into(),
        },
        phases: [phase].into(),
        tool_names: ["shell".into()].into(),
        capabilities: [Capability::ProcessExecute, Capability::FilesystemRead].into(),
        effects: [Effect::Execute, Effect::Read].into(),
        timeout_ms: timeout,
        max_input_bytes: 65536,
        max_output_bytes: 65536,
    }
}

async fn observe(root: &Path, plan: &Plan, case: &Case) -> Value {
    fs::create_dir(root).unwrap();
    for name in ["workspace", "install", "cwd"] {
        fs::create_dir(root.join(name)).unwrap();
        fs::set_permissions(root.join(name), fs::Permissions::from_mode(0o700)).unwrap();
    }
    let record = Arc::new(Recorder(Mutex::new(
        fs::File::create(root.join("actual.jsonl")).unwrap(),
    )));
    record.append(json!({"event":"case_plan","case":case.id,"input":plan,"attempts":1,"model_source":"scripted offline frames"}));
    let ledger = SqliteLedger::open(root.join("ledger.sqlite")).unwrap();
    let journal = Journal {
        inner: SqliteFactJournal::open(root.join("journal.sqlite")).unwrap(),
        streams: Arc::default(),
        fail_after: Arc::new(AtomicBool::new(case.mode == "observer_storage")),
    };
    let artifacts = Arc::new(ArtifactStore::new(root.join("artifacts"), 4 * 1024 * 1024).unwrap());
    let catalog = HookCatalog::new(
        Arc::new(journal.clone()),
        artifacts.clone(),
        "actual-bridge".into(),
    )
    .unwrap();
    let mut registrations = Vec::new();
    for (phase, script) in [
        (HookPhase::BeforeTool, &case.before),
        (HookPhase::AfterTool, &case.after),
    ] {
        if !script.is_empty() {
            registrations.push(
                catalog
                    .register(manifest(phase, plan.hook_timeout_ms), script)
                    .unwrap(),
            );
        }
    }
    let permissions = AgentPermissions {
        tools: [EnvironmentTool::Shell].into(),
        ..Default::default()
    };
    let definition = AgentDefinition::new(AgentDefinitionInput {
        definition_id: "hook-agent".into(),
        revision: "1".into(),
        display_name: None,
        model: plan.model.clone(),
        instructions: "Use only admitted tools; hooks are host policy, not instructions.".into(),
        permissions: permissions.clone(),
    })
    .unwrap();
    let policy = HookAccessPolicy::new(
        "acl-1".into(),
        vec![HookAccessRule {
            agent: definition.key(),
            logical_session_id: "logical".into(),
            task_id: Some("task".into()),
            invocation_id: Some("root".into()),
            hooks: registrations
                .iter()
                .map(|r| r.manifest.key.clone())
                .collect(),
            phases: [HookPhase::BeforeTool, HookPhase::AfterTool].into(),
        }],
    )
    .unwrap();
    let (sender, mut receiver) = sandbox_process_observation_channel(128).unwrap();
    let native = NativeHookHost::new(root.join("install"), root.join("cwd"))
        .unwrap()
        .with_process_observer(sender.clone());
    let make_probe = || {
        let reopened = Journal {
            inner: SqliteFactJournal::open(root.join("journal.sqlite")).unwrap(),
            streams: journal.streams.clone(),
            fail_after: journal.fail_after.clone(),
        };
        Arc::new(Probe {
            bridge: AgentEffectHookBridge::new(
                SqliteLedger::open(root.join("ledger.sqlite")).unwrap(),
                TaskCoordinator::new(reopened.clone()),
                HookCatalog::new(
                    Arc::new(reopened),
                    Arc::new(ArtifactStore::new(root.join("artifacts"), 4 * 1024 * 1024).unwrap()),
                    "actual-bridge".into(),
                )
                .unwrap(),
                policy.clone(),
                NativeHookHost::new(root.join("install"), root.join("cwd"))
                    .unwrap()
                    .with_process_observer(sender.clone()),
            ),
            before: Mutex::new(None),
            receipt: Mutex::new(None),
        })
    };
    let mut probe = make_probe();
    SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap())
        .create("logical")
        .unwrap();
    let mut host = runner(
        root,
        plan,
        case,
        probe.clone(),
        (
            record.clone(),
            journal.clone(),
            ledger.clone(),
            artifacts.clone(),
        ),
        0,
    );
    let execution = ExecutionRef {
        session_id: "logical".into(),
        execution_id: "execution".into(),
        turn_id: "turn".into(),
    };
    let request = ModelRequest {
        request_id: "original".into(),
        model: plan.model.clone(),
        system: vec![],
        messages: vec![Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: plan.input.clone(),
            }],
        }],
        tools: vec![],
        tool_choice: ToolChoice::Auto,
        output_format: None,
        prompt_cache: None,
        reasoning: None,
        max_output_tokens: Some(256),
        extensions: Value::Null,
    };
    let prepared = host
        .prepare_root_input(RootInputPreparationRequest {
            task_id: "task".into(),
            invocation_id: "root".into(),
            execution: execution.clone(),
            selector: AgentSelector::Inline(definition.clone()),
            requested_permissions: permissions.clone(),
            model_request: request.clone(),
        })
        .await
        .unwrap();
    let scope = HookScope {
        logical_session_id: "logical".into(),
        task_id: "task".into(),
        invocation_id: "root".into(),
        private_session_id: "logical".into(),
        execution_id: execution.execution_id.clone(),
        turn_id: execution.turn_id.clone(),
        agent_snapshot_digest: prepared.snapshot.digest().into(),
    };
    if case.mode != "unbound" {
        let binding = catalog
            .bind(
                scope,
                prepared.ownership,
                registrations
                    .iter()
                    .map(|r| r.manifest.key.clone())
                    .collect(),
                &policy,
                native.digest().into(),
            )
            .unwrap();
        record.append(json!({"event":"hook_binding","binding":binding}));
    }
    if case.mode == "revoked" {
        let r = &registrations[0];
        catalog
            .revoke(
                &r.manifest.key,
                &r.reference,
                "revoke-before".into(),
                "host revoked before effect".into(),
            )
            .unwrap();
    }
    let mut life = Vec::new();
    let mut trigger_error = None;
    let mut suspended = None;
    let mut result = None;
    let mut dropped = false;
    {
        let mut future = Box::pin(host.start(RootRunRequest {
            goals: vec![],
            task_id: "task".into(),
            invocation_id: "root".into(),
            attempt_id: "attempt-1".into(),
            execution: execution.clone(),
            selector: AgentSelector::Inline(definition),
            requested_permissions: permissions,
            objective: plan.input.clone(),
            limits: TaskLimits {
                max_depth: 1,
                max_invocations: 1,
                max_attempts: 1,
                max_tokens: None,
                max_steps_per_turn: 4,
            },
            cancellation_policy: CancellationPolicy::AllInvocations,
            turn: TurnRequest {
                turn_id: "turn".into(),
                config: TurnConfig {
                    max_steps: 4,
                    max_tool_calls: Some(4),
                    deadline: None,
                },
                model_request: request,
            },
        }));
        if case.mode == "cancel" || case.mode == "drop" {
            let trigger=tokio::time::timeout(Duration::from_millis(plan.cleanup_wait_ms),async {
                loop {
                    tokio::select! {
                        value=&mut future=>break Err(value),
                        observation=receiver.recv()=>{
                            let Some(observation)=observation else {break Ok(false);};
                            let spawned=matches!(observation.event,SandboxProcessEvent::Spawned);
                            life.push(observation);
                            if spawned {break Ok(true);}
                        }
                    }
                }
            }).await;
            match trigger {
                Ok(Ok(true)) => {
                    if case.mode == "cancel" {
                        probe
                            .before
                            .lock()
                            .unwrap()
                            .as_ref()
                            .unwrap()
                            .control()
                            .cancel();
                        result = Some(future.await);
                    } else {
                        drop(future);
                        dropped = true;
                    }
                }
                Ok(Err(value)) => {
                    result = Some(value);
                    trigger_error = Some("execution returned before native Spawn".to_owned());
                }
                _ => {
                    drop(future);
                    trigger_error = Some("native Spawn trigger absent".to_owned());
                }
            }
        } else {
            result = Some(future.await);
        }
    }
    if case.mode == "approval"
        && let Some(Ok(value)) = &result
        && let kolyan_runtime::DurableTurnResult::Suspended { suspension, .. } = &value.execution
    {
        suspended = Some(
            json!({"suspension":suspension,"file_exists":root.join("workspace/proof.txt").exists(),
                    "native_facts":journal.records().iter().filter(|r|r.draft.kind=="agent.hook.started").count()}),
        );
        let approval = suspension
            .waiting
            .approvals
            .first()
            .unwrap()
            .approval_id
            .clone();
        // Discard Runner, service, Provider and bridge, then reopen durable stores.
        drop(host);
        probe = make_probe();
        host = runner(
            root,
            plan,
            case,
            probe.clone(),
            (
                record.clone(),
                Journal {
                    inner: SqliteFactJournal::open(root.join("journal.sqlite")).unwrap(),
                    streams: journal.streams.clone(),
                    fail_after: journal.fail_after.clone(),
                },
                SqliteLedger::open(root.join("ledger.sqlite")).unwrap(),
                Arc::new(ArtifactStore::new(root.join("artifacts"), 4 * 1024 * 1024).unwrap()),
            ),
            1,
        );
        result = Some(
            host.resume_approval(RootApprovalResumeRequest {
                task_id: "task".into(),
                invocation_id: "root".into(),
                logical_session_id: "logical".into(),
                attempt_id: "attempt-1".into(),
                approval_id: approval,
            })
            .await,
        );
    }
    let captured = probe.receipt.lock().unwrap().clone();
    let mut verify = None;
    let mut verification_error = None;
    let mut native_before_verify = 0;
    let mut native_after_verify = 0;
    if let Some((context, receipt)) = captured {
        if case.mode == "pass" {
            let r = &registrations[0];
            catalog
                .revoke(
                    &r.manifest.key,
                    &r.reference,
                    "revoke-after".into(),
                    "historical proof must not authorize new dispatch".into(),
                )
                .unwrap();
        }
        native_before_verify = journal
            .records()
            .iter()
            .filter(|r| r.draft.kind == "agent.hook.started")
            .count();
        let rebuilt = make_probe();
        let checked = rebuilt.verify_observation(context, receipt).await;
        verify = Some(if checked.is_ok() { "ok" } else { "error" });
        verification_error = checked.err().map(|e| e.to_string());
        native_after_verify = journal
            .records()
            .iter()
            .filter(|r| r.draft.kind == "agent.hook.started")
            .count();
    }
    let launches = journal
        .records()
        .iter()
        .filter(|r| r.draft.kind == "agent.hook.started")
        .count();
    let cleanup_error = tokio::time::timeout(Duration::from_millis(plan.cleanup_wait_ms), async {
        while life
            .iter()
            .filter(|o| matches!(o.event, SandboxProcessEvent::CaptureCompleted { .. }))
            .count()
            < launches * 2
            || life
                .iter()
                .filter(|o| matches!(o.event, SandboxProcessEvent::Reaped { .. }))
                .count()
                < launches
        {
            let Some(observation) = receiver.recv().await else {
                break;
            };
            life.push(observation);
        }
    })
    .await
    .err()
    .map(|e| e.to_string());
    while let Ok(o) = receiver.try_recv() {
        life.push(o);
    }
    let events = ledger.execution_events_after("execution", 0).unwrap();
    let facts = journal.records();
    let documents:Vec<_>=facts.iter().filter_map(|f|f.draft.payload.get("input").or_else(||f.draft.payload.get("output"))).map(|value|{
        let reference:kolyan_trace::ArtifactRef=serde_json::from_value(value.clone()).unwrap();
        let bytes=artifacts.read(&reference,1024*1024).unwrap();json!({"reference":reference,"content":serde_json::from_slice::<Value>(&bytes).unwrap()})
    }).collect();
    let completed = result.as_ref().is_some_and(|r| {
        r.as_ref().is_ok_and(|r| {
            matches!(
                &r.execution,
                kolyan_runtime::DurableTurnResult::Completed(turn, _)
                    if matches!(turn.result.outcome, kolyan_core::TurnOutcome::FinalAnswer { .. })
            )
        })
    });
    let error = result
        .as_ref()
        .and_then(|r| r.as_ref().err())
        .map(ToString::to_string);
    let value = json!({"event":"case_result","id":case.id,"completed":completed,"error":error,"dropped":dropped,"suspended":suspended,
        "snapshot":prepared.snapshot,"selected_input":prepared.selected_input,"ledger":events,"journal":facts,"documents":documents,
        "physical":physical_file(&root.join("workspace/proof.txt")),"verify":verify,"verification_error":verification_error,
        "native_before_verify":native_before_verify,"native_after_verify":native_after_verify,"lifecycle":life,
        "trigger_error":trigger_error,"cleanup_error":cleanup_error,"lifecycle_dropped":sender.lifecycle_dropped(),"attempts":1});
    record.append(value.clone());
    value
}

fn physical_file(path: &Path) -> Option<String> {
    match fs::read_to_string(path) {
        Ok(content) => Some(content),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
        Err(error) => panic!("physical file read failed at {}: {error}", path.display()),
    }
}

#[tokio::test]
async fn actual_native_tool_hooks_and_approval_reconstruction() {
    let input: Value =
        serde_json::from_str(include_str!("../fixtures/agent/hooks_bridge.json")).unwrap();
    let expected_input: Value =
        serde_json::from_str(include_str!("../expected/agent/hooks_bridge.json")).unwrap();
    let plan: Plan = serde_json::from_value(input.clone()).unwrap();
    let expected: Vec<Expected> = serde_json::from_value(expected_input.clone()).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-native-bridge-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let path = root.join("actual.jsonl");
    let mut output = fs::File::create(&path).unwrap();
    writeln!(
        output,
        "{}",
        json!({"event":"plan","input":input,"expected":expected_input,"attempts":1})
    )
    .unwrap();
    output.sync_all().unwrap();
    for case in &plan.cases {
        let row = observe(&root.join(&case.id), &plan, case).await;
        writeln!(output, "{row}").unwrap();
        output.flush().unwrap();
        output.sync_all().unwrap();
    }
    drop(output);
    println!("HOOK_BRIDGE_NATIVE={}", path.display());
    let rows: Vec<Value> = fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(plan.schema_version, 1);
    assert_eq!(rows.len(), plan.cases.len() + 1);
    assert_eq!(expected.len(), plan.cases.len());
    for expected in expected {
        let row = rows.iter().find(|r| r["id"] == expected.id).unwrap();
        assert_eq!(
            row["completed"],
            expected.completed,
            "{} at {}",
            expected.id,
            path.display()
        );
        let receipts: Vec<_> = row["ledger"]
            .as_array()
            .unwrap()
            .iter()
            .filter(|e| e["kind"] == "effect_receipt")
            .collect();
        assert_eq!(receipts.len(), expected.receipt, "{}", expected.id);
        assert_eq!(row["verify"], json!(expected.verify), "{}", expected.id);
        assert_eq!(
            row["native_before_verify"], row["native_after_verify"],
            "verification must not execute scripts"
        );
        let life = row["lifecycle"].as_array().unwrap();
        for (kind, count) in [
            ("spawned", expected.spawned),
            ("reaped", expected.spawned),
            ("capture_completed", expected.spawned * 2),
        ] {
            assert_eq!(
                life.iter().filter(|e| e["event"]["kind"] == kind).count(),
                count,
                "{}/{}",
                expected.id,
                kind
            );
        }
        assert_eq!(
            life.iter()
                .filter(|e| e["event"]["kind"] == "group_cleanup" && e["event"]["error"].is_null())
                .count(),
            expected.spawned
        );
        assert!(
            row["trigger_error"].is_null() && row["cleanup_error"].is_null(),
            "{}",
            expected.id
        );
        assert_eq!(row["lifecycle_dropped"], 0);
        if expected.receipt == 1 {
            assert_eq!(row["physical"], "HOOK-TARGET-PROOF");
        } else {
            assert!(row["physical"].is_null());
        }
        if expected.id == "approval-reconstruction" {
            assert_eq!(row["suspended"]["file_exists"], false);
            assert_eq!(row["suspended"]["native_facts"], 0);
        }
        assert_eq!(row["attempts"], 1);
    }
}
