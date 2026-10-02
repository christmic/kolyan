//! Actual Agent factories and durable services reconstructed at every stage.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentPermissions, AgentRunner, ContinuationProjectionConfig,
    binding::AgentInvocationBindingStore,
};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{ModelProvider, ModelRef};
use kolyan_server::{
    ExecutionService, InstanceRegistry, SessionExecutionService, SessionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::json;

use super::super::{
    data,
    evidence::Evidence,
    providers::{Providers, UnsupportedCounter},
};
use super::{Plan, tools::Factory};

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
}
impl Host {
    pub fn open(
        plan: &Plan,
        control: &Path,
        definition: &AgentDefinition,
        permissions: &AgentPermissions,
        live: Arc<dyn ModelProvider>,
        evidence: Arc<Evidence>,
    ) -> Self {
        let ledger = SqliteLedger::open(control.join("state/ledger.sqlite")).unwrap();
        let journal = SqliteFactJournal::open(control.join("state/ledger.sqlite")).unwrap();
        let sessions = FileSessionStore::new(control.join("state/sessions")).unwrap();
        let service = Arc::new(Service::new(
            TaskCoordinator::new(journal.clone()),
            SessionExecutionService::new(
                ExecutionService::new(ledger.clone(), NoopTraceSink),
                SessionService::new(sessions),
            )
            .with_context_policy(SessionContextPolicy::FullTrajectory),
        ));
        let mut catalog = AgentCatalog::new(8).unwrap();
        catalog.register(definition.clone()).unwrap();
        let port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let mut dataset = data::dataset();
        dataset.inspection_window_assumption_tokens = plan.inspection_window_assumption_tokens;
        dataset.output_reserve_tokens = plan.output_reserve_tokens;
        let runner = AgentRunner::new(
            service.clone(),
            InstanceRegistry::new(port.clone(), "self-iteration-host", 128).unwrap(),
            AgentInvocationBindingStore::new(port),
            catalog,
            permissions.clone(),
            (
                Providers {
                    live: Some(live),
                    dataset,
                    evidence: evidence.clone(),
                },
                Factory {
                    plan: plan.clone(),
                    control: PathBuf::from(control),
                    evidence,
                },
            ),
            Arc::new(
                ArtifactStore::new(control.join("state/artifacts"), 16 * 1024 * 1024).unwrap(),
            ),
        )
        .unwrap()
        .with_continuation_projection(ContinuationProjectionConfig {
            counter: Arc::new(UnsupportedCounter),
        });
        Self {
            runner: Arc::new(runner),
            service,
            ledger,
            journal,
        }
    }
    pub fn export(&self, task: &str, evidence: &Evidence) -> Result<(), String> {
        for event in self.ledger.events_after(0).map_err(|e| e.to_string())? {
            evidence.append(json!({"event":"ledger","value":event}))?;
        }
        for fact in self
            .journal
            .read(task, 0, 1024)
            .map_err(|e| e.to_string())?
        {
            evidence.append(json!({"event":"journal","value":fact}))?;
        }
        evidence.append(json!({"event":"self_iteration_task","value":self.service.coordinator().snapshot(task).map_err(|e|e.to_string())?}))
    }
}
pub(super) fn model(plan: &Plan) -> ModelRef {
    ModelRef::new(&plan.family, &plan.model)
}
