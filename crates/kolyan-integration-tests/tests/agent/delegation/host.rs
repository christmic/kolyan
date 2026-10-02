//! Actual durable host assembly; offline fault injection is explicit and typed.

use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
};

use futures_util::{StreamExt, stream};
use kolyan_agent::{
    AgentCatalog, AgentChildWaitVerifier, AgentDefinition, AgentDefinitionInput,
    AgentDelegationConfig, AgentExecutionBudget, AgentPermissions, AgentRunner, AgentSelector,
    AgentSnapshot, EnvironmentTool, InvokePrepareLimits, ProviderFactory, RunnerError,
    binding::AgentInvocationBindingStore,
    context::{BudgetMode, ContextPolicy},
    provider::ContextPreparingProvider,
};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelDescriptor, ModelEvent, ModelEventStream, ModelProvider, ModelRef,
    ModelRequest, ModelResponse, ProviderError, ProviderErrorKind, ProviderErrorPhase,
    ProviderFuture, StopReason,
};
use kolyan_policy::ApprovalMode;
use kolyan_server::{
    ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService, SessionService,
    TaskCoordinator, TaskExecutionService,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy, SessionStore};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::{Value, json};

use super::super::{data, evidence::Evidence, providers::UnsupportedCounter, tools::Tools};
use super::overlap::ProviderOverlap;
use super::{Case, dataset};

pub(super) struct ChildPlan {
    pub tools: Vec<EnvironmentTool>,
    pub instructions: String,
    pub parent_frames: Vec<Value>,
    pub child_frames: Vec<data::Frame>,
    pub overlap: Arc<ProviderOverlap>,
    pub recursive_frames: Option<Vec<data::Frame>>,
    pub approval_tools: Vec<String>,
}

type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;
type Runner =
    AgentRunner<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore, Providers, Tools>;

pub(super) struct Host {
    pub runner: Arc<Runner>,
    pub ledger: SqliteLedger,
    pub journal: SqliteFactJournal,
    pub child: AgentDefinition,
    pub permissions: AgentPermissions,
    parent: AgentDefinition,
    sessions: FileSessionStore,
    tool_error_policy: kolyan_core::ToolErrorPolicy,
}

impl Host {
    pub fn open_plan(
        root: &Path,
        case: &Case,
        model: &ModelRef,
        live: Option<Arc<dyn ModelProvider>>,
        evidence: Arc<Evidence>,
        plan: Option<ChildPlan>,
    ) -> Self {
        Self::open_plan_with_policy(
            root,
            case,
            model,
            live,
            evidence,
            plan,
            kolyan_core::ToolErrorPolicy::FailTurn,
        )
    }

    pub fn open_plan_with_policy(
        root: &Path,
        case: &Case,
        model: &ModelRef,
        live: Option<Arc<dyn ModelProvider>>,
        evidence: Arc<Evidence>,
        plan: Option<ChildPlan>,
        tool_error_policy: kolyan_core::ToolErrorPolicy,
    ) -> Self {
        let input = dataset();
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
        let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
        let sessions = FileSessionStore::new(root.join("state/sessions")).unwrap();
        match sessions.load("logical-session") {
            Ok(_) => {}
            Err(kolyan_storage::StorageError::NotFound(_)) => {
                SessionService::new(sessions.clone())
                    .create("logical-session")
                    .unwrap();
            }
            Err(error) => panic!("cannot reopen logical Session: {error}"),
        }
        let child_permissions = AgentPermissions {
            tools: plan
                .as_ref()
                .map(|plan| plan.tools.iter().copied().collect())
                .unwrap_or_else(|| [EnvironmentTool::Read].into()),
            ..Default::default()
        };
        let child=AgentDefinition::new(AgentDefinitionInput{definition_id:"child-definition".into(),revision:"r1".into(),display_name:None,model:model.clone(),instructions:plan.as_ref().map(|plan|plan.instructions.clone()).unwrap_or_else(||"Read the explicit private input using file.read, then finish with the actual observed content. Do not delegate.".into()),permissions:child_permissions.clone()}).unwrap();
        let mut permissions = child_permissions.clone();
        permissions.delegation.allow_inline = true;
        permissions.delegation.allow_self = true;
        permissions.delegation.named_targets.insert(child.key());
        let parent = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "parent-definition".into(),
            revision: "r1".into(),
            display_name: Some("Delegating parent".into()),
            model: model.clone(),
            instructions: input.instructions.clone(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let target = match case.target.as_str() {
            "named" => json!({"kind":"named","value":child.key()}),
            "inline" => json!({"kind":"inline","value":child}),
            "self" => json!({"kind":"self_call"}),
            other => panic!("unknown target {other}"),
        };
        let mut parent_frames = plan
            .as_ref()
            .map(|plan| plan.parent_frames.clone())
            .unwrap_or(input.parent_frames);
        for frame in &mut parent_frames {
            substitute(frame, &target);
        }
        let parent_frames = parent_frames
            .into_iter()
            .map(|value| serde_json::from_value(value).unwrap())
            .collect();
        let verifier = Arc::new(AgentChildWaitVerifier::default());
        let execution = ExecutionService::new(ledger.clone(), NoopTraceSink)
            .with_external_wait_verifier(verifier.clone());
        let service = Arc::new(
            Service::new(
                TaskCoordinator::new(journal.clone()),
                SessionExecutionService::new(
                    execution.clone(),
                    SessionService::new(sessions.clone()),
                )
                .with_context_policy(SessionContextPolicy::FullTrajectory),
            )
            .with_artifacts(
                ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap(),
            ),
        );
        let mut catalog = AgentCatalog::new(8).unwrap();
        catalog.register(child.clone()).unwrap();
        catalog.register(parent.clone()).unwrap();
        let journal_port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let mut tool_dataset = data::dataset();
        if let Some(plan) = &plan {
            for manifest in &mut tool_dataset.policy {
                if plan.approval_tools.contains(&manifest.tool_name) {
                    manifest.approval = ApprovalMode::Always;
                }
            }
        }
        let mut runner = AgentRunner::new(
            service,
            InstanceRegistry::new(journal_port.clone(), "agent-delegation-host", 128).unwrap(),
            AgentInvocationBindingStore::new(journal_port),
            catalog,
            permissions.clone(),
            (
                Providers {
                    live,
                    evidence: evidence.clone(),
                    ledger: ledger.clone(),
                    execution,
                    parent_frames,
                    child_frames: plan
                        .as_ref()
                        .map(|plan| plan.child_frames.clone())
                        .unwrap_or(input.child_frames),
                    overlap: plan.as_ref().map(|plan| plan.overlap.clone()),
                    recursive_frames: plan.as_ref().and_then(|plan| plan.recursive_frames.clone()),
                    fault: case.fault.clone(),
                },
                Tools {
                    root: root.into(),
                    dataset: tool_dataset,
                    evidence,
                },
            ),
            Arc::new(ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap()),
        )
        .unwrap()
        .with_tool_error_policy(tool_error_policy)
        .with_delegation(AgentDelegationConfig {
            approval: ApprovalMode::Never,
            limits: InvokePrepareLimits {
                max_children: if plan.is_some() { 2 } else { 1 },
                max_parallel: if plan.is_some() { 2 } else { 1 },
                max_child_input_bytes: 4096,
                max_output_bytes: 1024 * 1024,
                admission_timeout_ms: 10000,
            },
        })
        .unwrap();
        if plan.is_some() {
            runner = runner.with_execution_budget(AgentExecutionBudget::new(2).unwrap());
        }
        let runner = Arc::new(runner);
        verifier.attach(&runner).unwrap();
        Self {
            runner,
            ledger,
            journal,
            child,
            permissions,
            parent,
            sessions,
            tool_error_policy,
        }
    }
    pub fn selector(&self) -> AgentSelector {
        AgentSelector::Named(self.parent.key())
    }
    pub fn export(&self, task: &str, evidence: &Evidence) {
        let state = kolyan_server::TaskCoordinator::new(self.journal.clone())
            .snapshot(task)
            .unwrap();
        for attempt in state.attempts.values() {
            let bindings = AgentInvocationBindingStore::new(Arc::new(self.journal.clone()));
            let (binding, reference) = bindings
                .load_with_reference(task, &attempt.binding.invocation_id, "logical-session")
                .unwrap()
                .expect("every admitted Agent attempt has an immutable binding");
            evidence
                .append(json!({"event":"agent_binding","reference":reference,"binding":binding}))
                .unwrap();
            let context = self
                .sessions
                .load(&attempt.binding.execution.session_id)
                .unwrap();
            evidence.append(json!({"event":"private_session","invocation_id":attempt.binding.invocation_id,"value":context})).unwrap();
            let events = self
                .ledger
                .execution_events_after(&attempt.binding.execution.execution_id, 0)
                .unwrap();
            let failures = events
                .iter()
                .filter(|event| event.kind == kolyan_ledger::LedgerEventKind::ToolExecutionFailed)
                .count();
            let feedback = events
                .iter()
                .filter(|event| {
                    event.kind == kolyan_ledger::LedgerEventKind::ToolExecutionCompleted
                        && event.payload["is_error"] == true
                })
                .count();
            evidence.append(json!({"event":"tool_error_policy_observation","execution":attempt.binding.execution,
                "policy":self.tool_error_policy,"failed_calls":failures,"error_feedback_results":feedback,
                "error_feedback_observed":feedback>0,"meaning":"feedback_count_does_not_prove_later_success"})).unwrap();
            for event in events {
                evidence
                    .append(json!({"event":"ledger","value":event}))
                    .unwrap();
            }
        }
        for fact in self.journal.read(task, 0, 512).unwrap() {
            evidence
                .append(json!({"event":"journal","value":fact}))
                .unwrap();
        }
        evidence
            .append(json!({"event":"task_snapshot","value":state}))
            .unwrap();
    }
}

fn substitute(value: &mut Value, target: &Value) {
    if *value == Value::String("$TARGET".into()) {
        *value = target.clone();
        return;
    }
    match value {
        Value::Array(items) => {
            for item in items {
                substitute(item, target)
            }
        }
        Value::Object(items) => {
            for item in items.values_mut() {
                substitute(item, target)
            }
        }
        _ => {}
    }
}

pub(super) struct Providers {
    live: Option<Arc<dyn ModelProvider>>,
    evidence: Arc<Evidence>,
    ledger: SqliteLedger,
    execution: ExecutionService<SqliteLedger, NoopTraceSink>,
    parent_frames: Vec<data::Frame>,
    child_frames: Vec<data::Frame>,
    fault: String,
    overlap: Option<Arc<ProviderOverlap>>,
    recursive_frames: Option<Vec<data::Frame>>,
}

impl ProviderFactory for Providers {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        let parent = execution.session_id == "logical-session";
        let count = self
            .ledger
            .execution_events_after(&execution.execution_id, 0)
            .map_err(|error| RunnerError::Host(error.to_string()))?
            .iter()
            .filter(|event| event.kind == kolyan_ledger::LedgerEventKind::ModelRequested)
            .count();
        let mut frames = if parent {
            self.parent_frames.clone()
        } else if snapshot.permissions().delegation.allow_self {
            self.recursive_frames
                .clone()
                .unwrap_or_else(|| self.child_frames.clone())
        } else {
            self.child_frames.clone()
        };
        if self.live.is_none() {
            assert!(count <= frames.len());
            frames.drain(..count);
        }
        Ok(ContextPreparingProvider::new(
            Provider {
                live: self.live.clone(),
                frames: Mutex::new(frames.into()),
                evidence: self.evidence.clone(),
                fault: if parent {
                    "none".into()
                } else {
                    self.fault.clone()
                },
                execution: self.execution.clone(),
                key: execution.clone(),
                overlap: if parent { None } else { self.overlap.clone() },
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(32768),
                max_output_tokens: None,
                features: Default::default(),
            },
            ContextPolicy {
                id: "delegation-inspect".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 4 * 1024 * 1024,
                max_messages: 2048,
                max_content_blocks: 8192,
                context_limit_tokens: None,
                output_reserve_tokens: 8192,
            },
            Arc::new(UnsupportedCounter),
            self.evidence.clone(),
        ))
    }
}

pub(super) struct Provider {
    live: Option<Arc<dyn ModelProvider>>,
    frames: Mutex<VecDeque<data::Frame>>,
    evidence: Arc<Evidence>,
    fault: String,
    execution: ExecutionService<SqliteLedger, NoopTraceSink>,
    key: ExecutionRef,
    overlap: Option<Arc<ProviderOverlap>>,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.evidence
                .append(json!({"event":"request","execution":self.key,"request":request}))
                .unwrap();
            let span = match &self.overlap {
                Some(overlap) => Some(overlap.enter(&self.key).await?),
                None => None,
            };
            if self.fault == "provider_open_failure" {
                self.evidence.append(json!({"event":"injected_host_fault","fault":self.fault,"execution":self.key})).unwrap();
                return Err(ProviderError::new(
                    ProviderErrorKind::InvalidRequest,
                    ProviderErrorPhase::Open,
                    "data-declared offline child open failure",
                ));
            }
            if self.fault == "cancel_on_model_open" {
                self.execution.cancel(&self.key).map_err(|error| {
                    ProviderError::new(
                        ProviderErrorKind::Other,
                        ProviderErrorPhase::Open,
                        error.to_string(),
                    )
                })?;
                self.evidence.append(json!({"event":"injected_host_fault","fault":self.fault,"execution":self.key})).unwrap();
            }
            let events = if let Some(live) = &self.live {
                live.stream(request).await?
            } else {
                let frame = self
                    .frames
                    .lock()
                    .unwrap()
                    .pop_front()
                    .expect("data script exhausted");
                let tools = frame
                    .content
                    .iter()
                    .any(|block| matches!(block, ContentBlock::ToolCall { .. }));
                let mut content = vec![ContentBlock::Reasoning {
                    text: frame.reasoning.clone(),
                    opaque: None,
                }];
                content.extend(frame.content);
                Box::pin(stream::iter([
                    Ok(ModelEvent::Started),
                    Ok(ModelEvent::ReasoningDelta(frame.reasoning)),
                    Ok(ModelEvent::Completed(ModelResponse {
                        id: request.request_id,
                        model: request.model,
                        content,
                        structured_output: None,
                        stop_reason: if tools {
                            StopReason::ToolUse
                        } else {
                            StopReason::EndTurn
                        },
                        usage: frame.usage,
                        metadata: json!({"source":"delegation-offline-fixture"}),
                    })),
                ])) as ModelEventStream
            };
            let evidence = self.evidence.clone();
            let key = self.key.clone();
            let observed = Box::pin(events.map(move |event|{evidence.append(json!({"event":"model_event","execution":key,"content":event.as_ref().ok(),"error":event.as_ref().err().map(ToString::to_string)})).unwrap();event})) as ModelEventStream;
            Ok(super::overlap::observe_stream(observed, span))
        })
    }
}
