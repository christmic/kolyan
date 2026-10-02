//! Production Runner assembly with test-owned fault ports and real OS adapters.

use std::{
    collections::VecDeque,
    path::Path,
    sync::{Arc, Mutex},
};

use futures_util::{StreamExt, stream};
use kolyan_agent::{
    AgentCatalog, AgentChildWaitVerifier, AgentDefinition, AgentDefinitionInput,
    AgentDelegationConfig, AgentPermissions, AgentRunner, AgentSnapshot, EnvironmentTool,
    InvokePrepareLimits, ProviderFactory, RunnerError,
    binding::AgentInvocationBindingStore,
    context::{BudgetMode, ContextPolicy},
    provider::ContextPreparingProvider,
};
use kolyan_ledger::{FactJournal, LedgerEventKind, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelDescriptor, ModelEvent, ModelEventStream, ModelProvider, ModelRequest,
    ModelResponse, ProviderFuture, StopReason,
};
use kolyan_policy::ApprovalMode;
use kolyan_server::{
    ExecutionRef, ExecutionService, InstanceRegistry, SessionExecutionService, SessionService,
    TaskCoordinator, TaskExecutionService,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::json;

use super::super::{data, evidence::Evidence, providers::UnsupportedCounter, tools::Tools};
use super::{
    Dataset, Source,
    ports::{Factory, FaultLedger, State},
};

pub(super) type Service =
    TaskExecutionService<SqliteFactJournal, FaultLedger, NoopTraceSink, FileSessionStore>;
pub(super) type Runner = AgentRunner<
    SqliteFactJournal,
    FaultLedger,
    NoopTraceSink,
    FileSessionStore,
    Providers,
    Factory,
>;

pub(super) struct Host {
    pub runner: Arc<Runner>,
    pub service: Arc<Service>,
    pub ledger: SqliteLedger,
    pub journal: SqliteFactJournal,
    pub root_definition: AgentDefinition,
    pub permissions: AgentPermissions,
    pub child_key: kolyan_agent::AgentKey,
    pub child_input: String,
}

impl Host {
    pub fn open(
        root: &Path,
        dataset: &Dataset,
        state: Arc<State>,
        source: &Source,
    ) -> Result<Self, String> {
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite"))
            .map_err(|error| error.to_string())?;
        let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite"))
            .map_err(|error| error.to_string())?;
        let sessions = FileSessionStore::new(root.join("state/sessions"))
            .map_err(|error| error.to_string())?;
        let mut permissions = AgentPermissions {
            tools: [EnvironmentTool::Write].into(),
            ..Default::default()
        };
        let model = source.model.clone();
        let child = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "native-child".into(),
            revision: "r1".into(),
            display_name: None,
            model: model.clone(),
            instructions: dataset.input.clone(),
            permissions: permissions.clone(),
        })
        .map_err(|error| error.to_string())?;
        permissions.delegation.named_targets.insert(child.key());
        let child_key = child.key();
        let root_definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "native-root".into(),
            revision: "r1".into(),
            display_name: None,
            model,
            instructions: "Follow the private input and actual tool feedback. Do not repeat calls."
                .into(),
            permissions: permissions.clone(),
        })
        .map_err(|error| error.to_string())?;
        let verifier = Arc::new(AgentChildWaitVerifier::default());
        let service = Arc::new(
            Service::new(
                TaskCoordinator::new(journal.clone()),
                SessionExecutionService::new(
                    ExecutionService::new(
                        FaultLedger {
                            inner: ledger.clone(),
                            state: state.clone(),
                        },
                        NoopTraceSink,
                    )
                    .with_external_wait_verifier(verifier.clone()),
                    SessionService::new(sessions),
                )
                .with_context_policy(SessionContextPolicy::FullTrajectory),
            )
            .with_artifacts(
                ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
                    .map_err(|error| error.to_string())?,
            ),
        );
        let mut catalog = AgentCatalog::new(8).map_err(|error| error.to_string())?;
        catalog.register(child).map_err(|error| error.to_string())?;
        catalog
            .register(root_definition.clone())
            .map_err(|error| error.to_string())?;
        let journal_port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let runner = Arc::new(
            AgentRunner::new(
                service.clone(),
                InstanceRegistry::new(journal_port.clone(), "native-effects-host", 128)
                    .map_err(|error| error.to_string())?,
                AgentInvocationBindingStore::new(journal_port),
                catalog,
                permissions.clone(),
                (
                    Providers {
                        live: source.live.clone(),
                        dataset: dataset.clone(),
                        state: state.clone(),
                        ledger: ledger.clone(),
                    },
                    Factory {
                        inner: Tools {
                            root: root.into(),
                            dataset: data::dataset(),
                            evidence: state.evidence.clone(),
                        },
                        state,
                    },
                ),
                Arc::new(
                    ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024)
                        .map_err(|error| error.to_string())?,
                ),
            )
            .map_err(|error| error.to_string())?
            .with_delegation(AgentDelegationConfig {
                approval: ApprovalMode::Never,
                limits: InvokePrepareLimits {
                    max_children: 1,
                    max_parallel: 1,
                    max_child_input_bytes: 4096,
                    max_output_bytes: 1024 * 1024,
                    admission_timeout_ms: 10000,
                },
            })
            .map_err(|error| error.to_string())?,
        );
        verifier
            .attach(&runner)
            .map_err(|error| error.to_string())?;
        Ok(Self {
            runner,
            service,
            ledger,
            journal,
            root_definition,
            permissions,
            child_key,
            child_input: dataset.input.clone(),
        })
    }
    pub fn export(&self, task: &str, evidence: &Evidence) -> Result<(), String> {
        for event in self
            .ledger
            .events_after(0)
            .map_err(|error| error.to_string())?
        {
            evidence.append(json!({"event":"ledger","value":event}))?;
        }
        for fact in self
            .journal
            .read(task, 0, 512)
            .map_err(|error| error.to_string())?
        {
            evidence.append(json!({"event":"journal","value":fact}))?;
        }
        let snapshot = self
            .service
            .coordinator()
            .snapshot(task)
            .map_err(|error| error.to_string())?;
        let bindings = AgentInvocationBindingStore::new(Arc::new(self.journal.clone()));
        for invocation in snapshot.invocations.values() {
            let binding = bindings
                .load_with_reference(
                    task,
                    &invocation.definition.invocation_id,
                    "logical-session",
                )
                .map_err(|error| error.to_string())?;
            evidence.append(json!({"event":"native_binding","value":binding}))?;
            let source = invocation.definition.input_source.fact();
            for fact in self
                .journal
                .read(&source.stream_id, source.position.saturating_sub(1), 1)
                .map_err(|error| error.to_string())?
            {
                evidence.append(
                    json!({"event":"native_input_source","reference":source,"value":fact}),
                )?;
            }
        }
        evidence.append(json!({"event":"task_snapshot","value":snapshot}))
    }
}

pub(super) struct Providers {
    pub(super) live: Option<Arc<dyn ModelProvider>>,
    pub(super) dataset: Dataset,
    pub(super) state: Arc<State>,
    pub(super) ledger: SqliteLedger,
}
pub(super) struct Provider {
    live: Option<Arc<dyn ModelProvider>>,
    frames: Mutex<VecDeque<data::Frame>>,
    evidence: Arc<Evidence>,
}

impl ProviderFactory for Providers {
    type Provider = Provider;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<Provider>, RunnerError> {
        let mut frames = if self.live.is_some() {
            Vec::new()
        } else {
            if self.state.fault == "late_child" && execution.session_id == "logical-session" {
                self.dataset.parent_frames.clone()
            } else {
                self.dataset.frames.clone()
            }
        };
        let count = self
            .ledger
            .execution_events_after(&execution.execution_id, 0)
            .map_err(|error| RunnerError::Host(error.to_string()))?
            .iter()
            .filter(|event| event.kind == LedgerEventKind::ModelRequested)
            .count();
        if self.live.is_none() && count > frames.len() {
            return Err(RunnerError::Host(
                "recorded requests exceed native fixture frames".into(),
            ));
        }
        if self.live.is_none() {
            frames.drain(..count);
        }
        Ok(ContextPreparingProvider::new(
            Provider {
                live: self.live.clone(),
                frames: Mutex::new(frames.into()),
                evidence: self.state.evidence.clone(),
            },
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(self.dataset.inspection_window_assumption_tokens),
                max_output_tokens: None,
                features: Default::default(),
            },
            ContextPolicy {
                id: "native-effects-inspect".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 4 * 1024 * 1024,
                max_messages: 2048,
                max_content_blocks: 8192,
                context_limit_tokens: None,
                output_reserve_tokens: 8192,
            },
            Arc::new(UnsupportedCounter),
            self.state.evidence.clone(),
        ))
    }
}

impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        Box::pin(async move {
            self.evidence
                .append(json!({"event":"request","request":request}))
                .unwrap();
            if let Some(provider) = &self.live {
                let stream = match provider.stream(request).await {
                    Ok(stream) => stream,
                    Err(error) => {
                        self.evidence
                            .append(json!({"event":"provider_error","error":error.to_string()}))
                            .unwrap();
                        return Err(error);
                    }
                };
                let evidence = self.evidence.clone();
                return Ok(Box::pin(stream.map(move |event| {
                    evidence
                        .append(match &event {
                            Ok(content) => json!({"event":"model_event","content":content}),
                            Err(error) => {
                                json!({"event":"model_stream_error","error":error.to_string()})
                            }
                        })
                        .unwrap();
                    event
                })) as ModelEventStream);
            }
            let frame = self
                .frames
                .lock()
                .unwrap()
                .pop_front()
                .expect("native fixture script exhausted");
            let tools = frame
                .content
                .iter()
                .any(|block| matches!(block, ContentBlock::ToolCall { .. }));
            let mut content = vec![ContentBlock::Reasoning {
                text: frame.reasoning.clone(),
                opaque: None,
            }];
            content.extend(frame.content);
            let response = ModelResponse {
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
                metadata: json!({"source":"offline-native-fault-fixture"}),
            };
            let events = vec![
                ModelEvent::Started,
                ModelEvent::ReasoningDelta(frame.reasoning),
                ModelEvent::Completed(response),
            ];
            for event in &events {
                self.evidence
                    .append(json!({"event":"model_event","content":event}))
                    .unwrap();
            }
            Ok(Box::pin(stream::iter(events.into_iter().map(Ok))) as ModelEventStream)
        })
    }
}
