//! Trusted Skills assembly shares the Host journal and Required artifact store.

use std::sync::Arc;

use kolyan_agent::{RunnerError, SkillAccessPolicy, SkillCatalog, SkillLimits, SkillRuntime};
use kolyan_ledger::SqliteFactJournal;
use kolyan_trace::ArtifactStore;

/// Current host ACL is explicit on every rebuild; catalog history is durable.
pub struct HostSkillsConfig {
    pub namespace: String,
    pub limits: SkillLimits,
    pub policy: SkillAccessPolicy,
}

pub(crate) fn assemble(
    config: HostSkillsConfig,
    journal: &SqliteFactJournal,
    artifacts: Arc<ArtifactStore>,
) -> Result<(SkillCatalog, Arc<SkillRuntime>), RunnerError> {
    let catalog = SkillCatalog::new(
        Arc::new(journal.clone()),
        artifacts,
        config.namespace,
        config.limits,
    )
    .map_err(super::host::host)?;
    let runtime = Arc::new(SkillRuntime::new(catalog.clone(), config.policy));
    Ok((catalog, runtime))
}

#[cfg(test)]
mod tests;
