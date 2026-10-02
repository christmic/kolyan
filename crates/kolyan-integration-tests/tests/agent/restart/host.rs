//! Reopen every host store and reconstruct adapters; restored catalogs are empty.

use std::{
    path::{Path, PathBuf},
    sync::Arc,
};

use kolyan_agent::{
    AgentCatalog, AgentDefinition, AgentPermissions, AgentRunner,
    binding::AgentInvocationBindingStore,
};
use kolyan_ledger::{FactJournal, LedgerEvent, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::ModelProvider;
use kolyan_server::{
    ExecutionService, InstanceRegistry, SessionExecutionService, SessionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_storage::{FileSessionStore, SessionContextPolicy};
use kolyan_trace::{ArtifactStore, NoopTraceSink};
use serde_json::json;

use super::super::{data::Dataset, evidence::Evidence, providers::Providers, tools::Tools};

type Runner =
    AgentRunner<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore, Providers, Tools>;

pub struct Host {
    pub runner: Arc<Runner>,
    pub ledger: SqliteLedger,
    pub journal: SqliteFactJournal,
}

impl Host {
    pub fn open(
        root: &Path,
        dataset: &Dataset,
        definition: Option<&AgentDefinition>,
        permissions: &AgentPermissions,
        live: Option<Arc<dyn ModelProvider>>,
        evidence: Arc<Evidence>,
        consumed: Option<(&str, usize)>,
    ) -> Self {
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).unwrap();
        let journal = SqliteFactJournal::open(root.join("state/ledger.sqlite")).unwrap();
        let sessions = FileSessionStore::new(root.join("state/sessions")).unwrap();
        let service = Arc::new(
            TaskExecutionService::new(
                TaskCoordinator::new(journal.clone()),
                SessionExecutionService::new(
                    ExecutionService::new(ledger.clone(), NoopTraceSink),
                    SessionService::new(sessions),
                )
                .with_context_policy(SessionContextPolicy::FullTrajectory),
            )
            .with_artifacts(
                ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap(),
            ),
        );
        let mut catalog = AgentCatalog::new(8).unwrap();
        if let Some(definition) = definition {
            catalog.register(definition.clone()).unwrap();
        }
        let mut script = dataset.clone();
        // Only the offline model script needs an offset. It comes from persisted
        // ModelRequested facts, never from retained Provider/Runner memory.
        if live.is_none()
            && let Some((turn_id, count)) = consumed
        {
            let turn = script
                .turns
                .iter_mut()
                .find(|turn| turn.id == turn_id)
                .unwrap();
            assert!(count <= turn.script.len());
            turn.script.drain(..count);
        }
        let journal_port: Arc<dyn FactJournal> = Arc::new(journal.clone());
        let runner = Arc::new(
            AgentRunner::new(
                service,
                InstanceRegistry::new(journal_port.clone(), "agent-restart-host", 128).unwrap(),
                AgentInvocationBindingStore::new(journal_port),
                catalog,
                permissions.clone(),
                (
                    Providers {
                        live,
                        dataset: script,
                        evidence: evidence.clone(),
                    },
                    Tools {
                        root: PathBuf::from(root),
                        dataset: dataset.clone(),
                        evidence,
                    },
                ),
                Arc::new(
                    ArtifactStore::new(root.join("state/artifacts"), 4 * 1024 * 1024).unwrap(),
                ),
            )
            .unwrap(),
        );
        Self {
            runner,
            ledger,
            journal,
        }
    }

    pub fn export(&self, execution: &str, task: &str, evidence: &Evidence) -> Vec<LedgerEvent> {
        let events = self.ledger.execution_events_after(execution, 0).unwrap();
        for event in &events {
            evidence
                .append(json!({"event":"ledger","value":event}))
                .unwrap();
        }
        let mut after = 0;
        loop {
            let page = self.journal.read(task, after, 512).unwrap();
            if page.is_empty() {
                break;
            }
            after = page.last().unwrap().position;
            for fact in page {
                evidence
                    .append(json!({"event":"journal","value":fact}))
                    .unwrap();
            }
        }
        events
    }
}
