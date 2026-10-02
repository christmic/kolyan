//! Actual governed Runtime producers; scripted ports, not native file/Provider acceptance.
use super::super::*;
use kolyan_core::{
    ToolError, ToolErrorPolicy, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome,
    ToolPreparationFuture, TurnConfig, TurnExecutor, TurnRequest,
};
use kolyan_ledger::*;
use kolyan_model::*;
use kolyan_policy::*;
use kolyan_server::*;
use kolyan_storage::{FileSessionStore, SessionStore};
use kolyan_tools::{
    ExactDirectoryBinding, ExactFileBinding, ExactFileIdentity, FileOperationResult,
};
use kolyan_trace::NoopTraceSink;
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

pub(super) const REVISION: &str = "host-exact-fixture-adapter-v1";
pub(super) fn checker() -> FileWriteCommittedChecker {
    FileWriteCommittedChecker::new([REVISION.into()].into()).unwrap()
}
pub(super) fn hash(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
pub(super) fn binding() -> ExactFileBinding {
    let workspace = ExactDirectoryBinding {
        physical_path: "/historical/workspace".into(),
        identity_chain: (1..=3)
            .map(|ino| ExactFileIdentity { dev: 1, ino })
            .collect(),
    };
    ExactFileBinding {
        parent: workspace.clone(),
        workspace,
        leaf: "result.txt".into(),
        target_identity: None,
        protected_roots: vec![],
    }
}
pub(super) fn predicate() -> FileWriteCommittedPredicateV1 {
    let binding = binding();
    FileWriteCommittedPredicateV1 {
        schema_version: 1,
        tool_revision: REVISION.into(),
        workspace: binding.workspace,
        parent: binding.parent,
        leaf: binding.leaf,
        expected_bytes: 5,
        expected_sha256: hash(b"goal\n"),
    }
}
pub(super) fn criterion(predicate: Value) -> GoalCriterion {
    GoalCriterion::new(
        "goal".into(),
        "root".into(),
        checker().key().clone(),
        predicate,
    )
    .unwrap()
}
#[derive(Clone)]
pub(super) struct Ledger(pub Arc<dyn LedgerStore>);
#[derive(Clone)]
pub(super) struct Journal(pub Arc<dyn FactJournal>);
impl LedgerStore for Ledger {
    fn append(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.0.append(e)
    }
    fn append_unless_cancelled(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.0.append_unless_cancelled(e)
    }
    fn claim(&self, key: &str) -> Result<bool, LedgerError> {
        self.0.claim(key)
    }
    fn events_after(&self, c: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.0.events_after(c)
    }
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.0.query(q)
    }
}
impl FactJournal for Journal {
    fn read(&self, s: &str, a: u64, l: usize) -> Result<Vec<FactRecord>, FactError> {
        self.0.read(s, a, l)
    }
    fn append(&self, s: &str, p: u64, b: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        self.0.append(s, p, b)
    }
}
pub(super) fn stores(sqlite: bool, path: &std::path::Path) -> (Ledger, Journal) {
    if sqlite {
        (
            Ledger(Arc::new(
                SqliteLedger::open(path.join("ledger.sqlite")).unwrap(),
            )),
            Journal(Arc::new(
                SqliteFactJournal::open(path.join("facts.sqlite")).unwrap(),
            )),
        )
    } else {
        (
            Ledger(Arc::new(InMemoryLedger::default())),
            Journal(Arc::new(MemoryFactJournal::default())),
        )
    }
}
#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub id: String,
    pub content: String,
    pub display_path: String,
    pub tool: String,
    pub adapter_revision: String,
    pub writes: usize,
    pub result_mode: String,
    pub binding_mode: String,
    pub claim_mode: String,
    pub predicate_mode: String,
    pub source_mode: String,
    pub expected: String,
}
pub(super) struct Harness {
    pub root: tempfile::TempDir,
    pub ledger: Ledger,
    pub journal: Journal,
    pub prefix: TaskSnapshot,
    pub attempt: AttemptBinding,
    pub requests: Arc<Mutex<Vec<ModelRequest>>>,
    pub effects: Arc<AtomicUsize>,
}
pub(super) async fn produce(case: &Case, sqlite: bool) -> Harness {
    let root = tempfile::tempdir().unwrap();
    let store = FileSessionStore::new(root.path()).unwrap();
    store.create("session").unwrap();
    let (ledger, journal) = stores(sqlite, root.path());
    let coordinator = TaskCoordinator::new(journal.clone());
    let agent = AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "fixture".into(),
    };
    coordinator
        .register_task(
            "register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "historical file goal".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "execution-goal".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                limits: TaskLimits {
                    max_depth: 1,
                    max_invocations: 1,
                    max_attempts: 1,
                    max_tokens: None,
                    max_steps_per_turn: 4,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    let source = coordinator
        .publish_invocation_input_source(
            InvocationInputEnvelope {
                kind: InvocationInputKind::Standalone,
                scope: InvocationInputScope {
                    task_id: "task".into(),
                    invocation_id: "root".into(),
                    agent: agent.clone(),
                    constraints_digest: "a".repeat(64),
                },
                body: json!({"fixture":case.id}),
            },
            vec![FactRef {
                stream_id: "task".into(),
                position: 1,
                fact_id: journal.read("task", 0, 1).unwrap()[0].draft.fact_id.clone(),
            }],
        )
        .unwrap();
    let input_source = InvocationInputSource::Standalone {
        fact: source.reference,
    };
    coordinator
        .admit_invocation(
            "task",
            "admit",
            InvocationDefinition {
                invocation_id: "root".into(),
                agent: agent.clone(),
                constraints_digest: "a".repeat(64),
                role: InvocationRole::Root,
                parent_invocation_id: None,
                dependencies: vec![],
                input_source: input_source.clone(),
            },
        )
        .unwrap();
    let attempt = AttemptBinding {
        attempt_id: "attempt".into(),
        invocation_id: "root".into(),
        agent,
        constraints_digest: "a".repeat(64),
        input_source,
        execution: ExecutionRef {
            session_id: "session".into(),
            turn_id: "turn".into(),
            execution_id: "execution".into(),
        },
    };
    let service = TaskExecutionService::new(
        coordinator,
        SessionExecutionService::new(
            ExecutionService::new(ledger.clone(), NoopTraceSink),
            SessionService::new(store),
        ),
    );
    let requests = Arc::new(Mutex::new(vec![]));
    let effects = Arc::new(AtomicUsize::new(0));
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: case.tool.clone(),
        capabilities: [Capability::FilesystemWrite].into(),
        effects: [Effect::Create, Effect::Update].into(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    let executor = TurnExecutor::with_tools(
        Provider {
            case: case.clone(),
            requests: requests.clone(),
        },
        Tools {
            case: case.clone(),
            effects: effects.clone(),
        },
    )
    .with_policy_engine(Arc::new(policy))
    .with_agent_snapshot_digest("a".repeat(64))
    .with_tool_dispatch_policy(kolyan_core::ToolDispatchPolicy {
        on_error: ToolErrorPolicy::ContinueBatch,
        ..Default::default()
    });
    service
        .run(
            "task",
            attempt.clone(),
            executor,
            TurnRequest {
                turn_id: "turn".into(),
                model_request: ModelRequest {
                    request_id: "request".into(),
                    model: ModelRef::new("fixture", "model"),
                    system: vec![],
                    messages: vec![],
                    tools: vec![],
                    tool_choice: ToolChoice::Auto,
                    output_format: None,
                    prompt_cache: None,
                    reasoning: None,
                    max_output_tokens: Some(64),
                    extensions: Value::Null,
                },
                config: TurnConfig {
                    max_steps: 4,
                    ..Default::default()
                },
            },
        )
        .await
        .unwrap();
    let prefix = service.coordinator().snapshot("task").unwrap();
    drop(service);
    Harness {
        root,
        ledger,
        journal,
        prefix,
        attempt,
        requests,
        effects,
    }
}
struct Provider {
    case: Case,
    requests: Arc<Mutex<Vec<ModelRequest>>>,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut requests = self.requests.lock().unwrap();
        let calls = requests.is_empty() && self.case.writes > 0;
        requests.push(request.clone());
        let response = ModelResponse {
            id: "response".into(),
            model: request.model,
            content: if calls {
                (0..self.case.writes)
                    .map(|i| ContentBlock::ToolCall {
                        call: ToolCall {
                            id: format!("call-{}", self.case.writes - 1 - i),
                            name: self.case.tool.clone(),
                            arguments: if self.case.tool == "file.read" {
                                json!({"path":self.case.display_path})
                            } else {
                                json!({"path":self.case.display_path,"content":self.case.content})
                            },
                        },
                    })
                    .collect()
            } else {
                vec![ContentBlock::Text {
                    text: "done".into(),
                }]
            },
            structured_output: None,
            stop_reason: if calls {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage::default(),
            metadata: Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
struct Tools {
    case: Case,
    effects: Arc<AtomicUsize>,
}
impl ToolExecutor for Tools {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let mut b = binding();
        match self.case.binding_mode.as_str() {
            "old_inode" => b.target_identity = Some(ExactFileIdentity { dev: 1, ino: 900 }),
            "different_parent" => {
                b.parent.physical_path.push("other");
                b.parent
                    .identity_chain
                    .push(ExactFileIdentity { dev: 1, ino: 9 });
            }
            "broken_chain" => {
                b.parent.identity_chain.pop();
            }
            "unknown" => {}
            "none" => {}
            _ => panic!("unknown binding mode"),
        }
        let mut value = json!(b);
        if self.case.binding_mode == "unknown" {
            value["extra"] = json!(true);
        }
        let path = b
            .parent
            .physical_path
            .join(&b.leaf)
            .to_str()
            .unwrap()
            .to_owned();
        let case = self.case.clone();
        Box::pin(async move {
            PreparedCall::new(
                call.clone(),
                case.adapter_revision.clone(),
                InvocationClaim {
                    tool_name: call.name,
                    capabilities: [Capability::FilesystemWrite].into(),
                    effects: if case.claim_mode == "missing_create" {
                        [Effect::Update].into()
                    } else {
                        [Effect::Create, Effect::Update].into()
                    },
                    resource: ResourceClaim {
                        path: Some(if case.claim_mode == "wrong_resource" {
                            "/foreign/result.txt".into()
                        } else {
                            path
                        }),
                    },
                    idempotency: Idempotency::NonIdempotent,
                },
                ToolRequirements {
                    process_sandbox: case.claim_mode != "no_sandbox",
                    max_output_bytes: 4096,
                    timeout_ms: 1000,
                },
            )
            .and_then(|p| p.with_execution_binding(value))
            .map_err(|e| ToolError::Failed {
                message: e.to_string(),
            })
        })
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        self.effects.fetch_add(1, Ordering::SeqCst);
        let case = self.case.clone();
        Box::pin(async move {
            if case.result_mode == "failed" {
                return Err(ToolError::Failed {
                    message: "definitive fixture failure".into(),
                });
            }
            let mut result = json!(FileOperationResult {
                path: case.display_path.clone(),
                bytes: case.content.len(),
                sha256: hash(case.content.as_bytes()),
                content: None
            });
            match case.result_mode.as_str() {
                "wrong_hash" => result["sha256"] = json!("b".repeat(64)),
                "wrong_bytes" => result["bytes"] = json!(900),
                "wrong_path" => result["path"] = json!("other"),
                "content" => result["content"] = json!("unexpected"),
                "unknown" => result["extra"] = json!(true),
                _ => {}
            }
            let mut content = result.to_string();
            if case.result_mode == "duplicate" {
                content = content.replacen("{", "{\"path\":\"duplicated\",", 1);
            }
            Ok(ToolOutcome::Completed(ToolResult {
                call_id: invocation.prepared.call().id.clone(),
                content,
                is_error: case.result_mode == "is_error",
            }))
        })
    }
}
