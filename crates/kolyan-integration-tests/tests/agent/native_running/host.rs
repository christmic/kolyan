//! Persistent production Runner assembly; only the offline Provider is scripted.

use super::super::{data, evidence::Evidence, providers::Providers};
use super::{
    Case,
    model::{BoundedModel, Selection},
    ports::{Factory, State},
};
use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentDefinitionInput, AgentInvocationBindingStore,
    AgentPermissions, AgentRunner, EnvironmentTool,
};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_sandbox::SandboxProcessObservationSender;
use kolyan_server::{
    ExecutionService, InstanceRegistry, SessionExecutionService, SessionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::json;
use std::{path::Path, sync::Arc};

pub(super) type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;
pub(super) type Runner = AgentRunner<
    SqliteFactJournal,
    SqliteLedger,
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
    pub definition: AgentDefinition,
    pub permissions: AgentPermissions,
    pub dataset: data::Dataset,
}
impl Host {
    pub fn open(
        root: &Path,
        case: &Case,
        state: Arc<State>,
        observer: SandboxProcessObservationSender,
        selection: &Selection,
        model_timeout_ms: u64,
    ) -> Result<Self, String> {
        let ledger =
            SqliteLedger::open(root.join("state/ledger.sqlite")).map_err(|e| e.to_string())?;
        let journal =
            SqliteFactJournal::open(root.join("state/ledger.sqlite")).map_err(|e| e.to_string())?;
        let sessions =
            FileSessionStore::new(root.join("state/sessions")).map_err(|e| e.to_string())?;
        let service = Arc::new(
            Service::new(
                TaskCoordinator::new(journal.clone()),
                SessionExecutionService::new(
                    ExecutionService::new(ledger.clone(), NoopTraceSink),
                    SessionService::new(sessions),
                )
                .with_context_policy(SessionContextPolicy::FullTrajectory),
            )
            .with_artifacts(
                ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024)
                    .map_err(|e| e.to_string())?,
            ),
        );
        let permissions = AgentPermissions {
            tools: [EnvironmentTool::Shell].into(),
            ..Default::default()
        };
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "native-running-root".into(),
            revision: "r1".into(),
            display_name: None,
            model: selection.model.clone(),
            instructions: case.input.clone(),
            permissions: permissions.clone(),
        })
        .map_err(|e| e.to_string())?;
        let mut dataset = data::dataset();
        let mut turn = dataset.turns.remove(0);
        turn.id = case.id.clone();
        turn.input = case.input.clone();
        turn.script = case.frames.clone();
        dataset.turns = vec![turn];
        let mut catalog = AgentCatalog::new(8).map_err(|e| e.to_string())?;
        catalog
            .register(definition.clone())
            .map_err(|e| e.to_string())?;
        let port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let runner = Arc::new(
            AgentRunner::new(
                service.clone(),
                InstanceRegistry::new(port.clone(), "native-running-host", 32)
                    .map_err(|e| e.to_string())?,
                AgentInvocationBindingStore::new(port),
                catalog,
                permissions.clone(),
                (
                    Providers {
                        live: selection.live.as_ref().map(|inner| {
                            Arc::new(BoundedModel {
                                inner: inner.clone(),
                                wait: std::time::Duration::from_millis(model_timeout_ms),
                                evidence: state.evidence.clone(),
                            }) as Arc<dyn kolyan_model::ModelProvider>
                        }),
                        dataset: dataset.clone(),
                        evidence: state.evidence.clone(),
                    },
                    Factory {
                        root: root.into(),
                        state,
                        observer,
                    },
                ),
                Arc::new(
                    ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024)
                        .map_err(|e| e.to_string())?,
                ),
            )
            .map_err(|e| e.to_string())?,
        );
        Ok(Self {
            runner,
            service,
            ledger,
            journal,
            definition,
            permissions,
            dataset,
        })
    }
    pub fn export(&self, root: &Path, task: &str, evidence: &Evidence) -> Result<(), String> {
        export_facts(&self.ledger, &self.journal, root, task, evidence)
    }
}

/// Inspect existing stores after an early failure, without rebuilding a Provider.
pub(super) fn export_stored(root: &Path, task: &str, evidence: &Evidence) -> Result<(), String> {
    let path = root.join("state/ledger.sqlite");
    if !path.is_file() {
        return Err("no existing execution store to export".into());
    }
    let ledger = SqliteLedger::open(&path).map_err(|e| e.to_string())?;
    let journal = SqliteFactJournal::open(&path).map_err(|e| e.to_string())?;
    export_facts(&ledger, &journal, root, task, evidence)
}

fn export_facts(
    ledger: &SqliteLedger,
    journal: &SqliteFactJournal,
    root: &Path,
    task: &str,
    evidence: &Evidence,
) -> Result<(), String> {
    for event in ledger.events_after(0).map_err(|e| e.to_string())? {
        evidence.append(json!({"event":"ledger","value":event}))?;
    }
    for fact in journal.read(task, 0, 1024).map_err(|e| e.to_string())? {
        evidence.append(json!({"event":"journal","value":fact}))?;
    }
    let task = TaskCoordinator::new(journal.clone())
        .snapshot(task)
        .map_err(|e| e.to_string())?;
    evidence.append(json!({"event":"task_snapshot","value":task}))?;
    for invocation in task.invocations.values() {
        let reference = invocation.definition.input_source.fact();
        for fact in journal
            .read(
                &reference.stream_id,
                reference.position.saturating_sub(1),
                1,
            )
            .map_err(|e| e.to_string())?
        {
            evidence.append(json!({"event":"input_source","value":fact}))?;
            let artifact: kolyan_trace::ArtifactRef =
                serde_json::from_value(fact.draft.payload["body"]["artifact"].clone())
                    .map_err(|e| e.to_string())?;
            let bytes = ArtifactStore::new(root.join("state/artifacts"), 16 * 1024 * 1024)
                .map_err(|e| e.to_string())?
                .read(&artifact, 16 * 1024 * 1024)
                .map_err(|e| e.to_string())?;
            evidence.append(json!({"event":"input_archive","reference":artifact,"value":serde_json::from_slice::<serde_json::Value>(&bytes).map_err(|e|e.to_string())?}))?;
        }
    }
    Ok(())
}
