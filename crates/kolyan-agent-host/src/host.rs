//! Durable production assembly. No HTTP protocol, CLI loop or automatic correction.

use std::{collections::BTreeSet, path::PathBuf, sync::Arc};

use kolyan_agent::{
    AgentCatalog, AgentChildWaitVerifier, AgentDefinition, AgentDelegationConfig,
    AgentExecutionBudget, AgentInvocationBindingStore, AgentPermissions, AgentRunner,
    FileWriteCommittedChecker, RunnerError,
    context::ContextPolicy,
    provider::{ContextRecord, ContextRecordError, ContextRecorder},
};
use kolyan_core::ToolErrorPolicy;
use kolyan_ledger::{SqliteFactJournal, SqliteLedger};
use kolyan_policy::{PolicyEngine, ToolManifest};
use kolyan_server::{
    ExecutionService, GoalCheckerRegistry, GoalSourceLimits, InstanceRegistry,
    LedgerTaskGoalVerifier, SessionExecutionService, SessionService, TaskCoordinator,
    TaskExecutionService,
};
use kolyan_storage::FileSessionStore;
use kolyan_tools::{IsolatedToolSet, IsolatedToolSetConfig};
use kolyan_trace::{ArtifactStore, NoopTraceSink, Retention};
use serde_json::json;

use super::{HostDeployment, HostProviderFactory, environment::HostEnvironment};

pub(crate) type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;
pub(crate) type Runner = AgentRunner<
    SqliteFactJournal,
    SqliteLedger,
    NoopTraceSink,
    FileSessionStore,
    HostProviderFactory,
    HostEnvironment,
>;

/// Explicit trusted configuration. Policy manifests are host declarations, not
/// model input; adapters and Runner still validate the actual prepared claims.
pub struct AgentHostConfig {
    pub state_root: PathBuf,
    pub host_id: String,
    pub deployment: HostDeployment,
    pub context: ContextPolicy,
    pub catalog: Vec<AgentDefinition>,
    pub permissions: AgentPermissions,
    pub environment: IsolatedToolSetConfig,
    pub tool_manifests: Vec<ToolManifest>,
    pub tool_scope: String,
    pub allow_shell: bool,
    pub delegation: AgentDelegationConfig,
    pub tool_error_policy: ToolErrorPolicy,
    pub max_parallel: usize,
    pub template: kolyan_model::ModelRequest,
    pub turn_config: kolyan_core::TurnConfig,
    pub task_limits: kolyan_server::TaskLimits,
    pub cancellation_policy: kolyan_server::CancellationPolicy,
}

/// A rebuilt host uses the same control paths and host ID. The catalog is only
/// consulted for new admissions; saved execution restores immutable bindings.
pub struct AgentHost {
    pub(crate) runner: Arc<Runner>,
    pub(crate) service: Arc<Service>,
    pub(crate) bindings: AgentInvocationBindingStore,
    pub(crate) permissions: AgentPermissions,
    pub(crate) tools: IsolatedToolSet,
    pub(crate) checker: Arc<FileWriteCommittedChecker>,
    pub(crate) workspace: PathBuf,
    pub(crate) protected: Vec<PathBuf>,
    pub(crate) template: kolyan_model::ModelRequest,
    pub(crate) turn_config: kolyan_core::TurnConfig,
    pub(crate) task_limits: kolyan_server::TaskLimits,
    pub(crate) cancellation_policy: kolyan_server::CancellationPolicy,
    pub(crate) max_write_bytes: usize,
    pub(crate) skill_catalog: Option<kolyan_agent::SkillCatalog>,
}

impl AgentHost {
    /// Blocking filesystem/client assembly only; never sends a model request.
    /// Async transports must call on a blocking worker. Control state and private
    /// staging must be physically disjoint from the selected workspace.
    pub fn open(config: AgentHostConfig) -> Result<Self, RunnerError> {
        Self::open_assembled(config, None)
    }

    /// Assemble Skills using the same durable control stores as the Runner.
    /// Rebuild with the same namespace and an explicitly current host ACL.
    pub fn open_with_skills(
        config: AgentHostConfig,
        skills: super::HostSkillsConfig,
    ) -> Result<Self, RunnerError> {
        Self::open_assembled(config, Some(skills))
    }

    /// Trusted administrative catalog access, never a model authorization port.
    pub fn skill_catalog(&self) -> Option<&kolyan_agent::SkillCatalog> {
        self.skill_catalog.as_ref()
    }

    fn open_assembled(
        mut config: AgentHostConfig,
        skills: Option<super::HostSkillsConfig>,
    ) -> Result<Self, RunnerError> {
        config.permissions.validate()?;
        if config.template.model != config.deployment.descriptor.reference
            || !config.template.tools.is_empty()
            || config.template.max_output_tokens != Some(config.context.output_reserve_tokens)
            || config.context.output_reserve_tokens == 0
            || config.turn_config.max_steps == 0
            || config
                .turn_config
                .max_tool_calls
                .is_none_or(|limit| limit == 0)
        {
            return Err(host(
                "template model, inventory or explicit output/Turn limits are invalid",
            ));
        }
        for definition in &config.catalog {
            if definition.model() != &config.template.model {
                return Err(host("catalog model differs from the configured deployment"));
            }
        }
        let workspace = config
            .environment
            .files
            .workspace
            .canonicalize()
            .map_err(host)?;
        let state = config.state_root.canonicalize().map_err(host)?;
        if !state.is_dir()
            || state.parent().is_none()
            || state.starts_with(&workspace)
            || workspace.starts_with(&state)
        {
            return Err(host(
                "control state must be an existing directory disjoint from workspace",
            ));
        }
        let staging = config
            .environment
            .files
            .staging_root
            .canonicalize()
            .map_err(host)?;
        if staging.starts_with(&state) || state.starts_with(&staging) {
            return Err(host("staging and control state must be disjoint"));
        }
        for name in ["sessions", "artifacts"] {
            control_directory(&state.join(name))?;
        }
        for name in ["executions.sqlite", "facts.sqlite"] {
            if std::fs::symlink_metadata(state.join(name)).is_ok_and(|m| m.file_type().is_symlink())
            {
                return Err(host("persistent database leaf must not be a symlink"));
            }
        }
        let worker = config
            .environment
            .files
            .worker
            .canonicalize()
            .map_err(host)?;
        config
            .environment
            .files
            .protected_roots
            .push(worker.clone());
        config.environment.shell.protected_roots.push(worker);
        let scope = scope(&workspace, &config.tool_scope)?;
        config.environment.files.protected_roots.push(state.clone());
        config.environment.shell.protected_roots.push(state.clone());
        let mut protected = config.environment.files.protected_roots.clone();
        protected.extend(config.environment.shell.protected_roots.clone());
        protected.push(staging);
        let max_write_bytes = config.environment.files.file_limits.max_write_bytes;
        let tools = IsolatedToolSet::new(config.environment).map_err(host)?;
        let revision = tools.file_adapter_revision().map_err(host)?;
        let checker = Arc::new(FileWriteCommittedChecker::new(BTreeSet::from([revision]))?);
        let mut policy = PolicyEngine::default();
        let mut names = BTreeSet::new();
        for manifest in config.tool_manifests {
            if !matches!(
                manifest.tool_name.as_str(),
                "file.read" | "file.write" | "file.edit" | "shell"
            ) || !names.insert(manifest.tool_name.clone())
            {
                return Err(host("environment manifest name is unknown or duplicated"));
            }
            policy.register(manifest);
        }
        if names.len() != 4 {
            return Err(host(
                "all four environment manifests must be explicitly configured",
            ));
        }
        if !config.allow_shell || scope != workspace {
            policy.deny_tool("shell");
        }
        policy.restrict_workspace(scope.to_str().ok_or_else(|| host("scope must be UTF-8"))?);
        let ledger = SqliteLedger::open(state.join("executions.sqlite")).map_err(host)?;
        let journal = SqliteFactJournal::open(state.join("facts.sqlite")).map_err(host)?;
        let artifacts =
            Arc::new(ArtifactStore::new(state.join("artifacts"), 16 * 1024 * 1024).map_err(host)?);
        let recorder = Arc::new(ArtifactRecorder(artifacts.clone()));
        let skills = skills
            .map(|config| super::skills::assemble(config, &journal, artifacts.clone()))
            .transpose()?;
        let providers = HostProviderFactory::open(config.deployment, config.context, recorder)?;
        let registry = GoalCheckerRegistry::new(vec![checker.clone()])?;
        let verifier =
            LedgerTaskGoalVerifier::new(ledger.clone(), registry, GoalSourceLimits::default())?;
        let tasks = TaskCoordinator::new(journal.clone()).with_goal_verifier(Arc::new(verifier));
        let bindings = AgentInvocationBindingStore::new(Arc::new(journal.clone()));
        let instances = InstanceRegistry::new(Arc::new(journal), config.host_id, 65536)?;
        let wait_verifier = Arc::new(AgentChildWaitVerifier::default());
        let execution = ExecutionService::new(ledger, NoopTraceSink)
            .with_external_wait_verifier(wait_verifier.clone());
        let sessions = SessionExecutionService::new(
            execution,
            SessionService::new(FileSessionStore::new(state.join("sessions")).map_err(host)?),
        )
        .with_context_policy(kolyan_storage::SessionContextPolicy::FullTrajectory);
        let service = Arc::new(TaskExecutionService::new(tasks, sessions).with_artifacts(
            ArtifactStore::new(state.join("artifacts"), 16 * 1024 * 1024).map_err(host)?,
        ));
        let mut catalog = AgentCatalog::new(4096)?;
        for definition in config.catalog {
            catalog.register(definition)?;
        }
        let mut runner = AgentRunner::new(
            service.clone(),
            instances,
            bindings.clone(),
            catalog,
            config.permissions.clone(),
            (
                providers,
                HostEnvironment {
                    tools: tools.clone(),
                    policy: Arc::new(policy),
                },
            ),
            artifacts,
        )?
        .with_delegation(config.delegation)?
        .with_execution_budget(AgentExecutionBudget::new(config.max_parallel)?)
        .with_tool_error_policy(config.tool_error_policy);
        let skill_catalog = if let Some((catalog, runtime)) = skills {
            runner = runner.with_skills(runtime);
            Some(catalog)
        } else {
            None
        };
        let runner = Arc::new(runner);
        wait_verifier.attach(&runner).map_err(host)?;
        Ok(Self {
            runner,
            service,
            bindings,
            permissions: config.permissions,
            tools,
            checker,
            workspace,
            protected,
            template: config.template,
            turn_config: config.turn_config,
            task_limits: config.task_limits,
            cancellation_policy: config.cancellation_policy,
            max_write_bytes,
            skill_catalog,
        })
    }
}

struct ArtifactRecorder(Arc<ArtifactStore>);

impl ContextRecorder for ArtifactRecorder {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError> {
        let value = match record {
            ContextRecord::Prepared { source, prepared } => {
                json!({"kind":"prepared","source":source,"prepared":prepared})
            }
            ContextRecord::Rejected {
                source,
                failure,
                preparation,
            } => {
                json!({"kind":"rejected","source":source,"failure":failure.to_string(),"preparation":preparation})
            }
        };
        let bytes = serde_json::to_vec(&value).map_err(record_error)?;
        if bytes.len() > 16 * 1024 * 1024 {
            return Err(record_error(
                "context evidence exceeds the host artifact bound",
            ));
        }
        self.0
            .put(&bytes, Retention::Required)
            .map_err(record_error)?;
        Ok(())
    }
}

fn record_error(error: impl std::fmt::Display) -> ContextRecordError {
    ContextRecordError {
        message: error.to_string(),
    }
}
pub(crate) fn host(error: impl std::fmt::Display) -> RunnerError {
    RunnerError::Host(error.to_string())
}
fn scope(workspace: &std::path::Path, relative: &str) -> Result<PathBuf, RunnerError> {
    use std::path::{Component, Path};
    let path = Path::new(relative);
    if relative.is_empty()
        || relative.len() > 4096
        || path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(host("tool scope must be bounded and workspace-relative"));
    }
    let physical = workspace.join(path).canonicalize().map_err(host)?;
    if !physical.is_dir() || !physical.starts_with(workspace) {
        return Err(host("tool scope escapes workspace or is not a directory"));
    }
    Ok(physical)
}

fn control_directory(path: &std::path::Path) -> Result<(), RunnerError> {
    if let Ok(metadata) = std::fs::symlink_metadata(path) {
        if !metadata.is_dir() || metadata.file_type().is_symlink() {
            return Err(host(
                "control directory must be a physical nonsymlink directory",
            ));
        }
    } else {
        #[cfg(unix)]
        {
            use std::os::unix::fs::DirBuilderExt;
            std::fs::DirBuilder::new()
                .mode(0o700)
                .create(path)
                .map_err(host)?;
        }
        #[cfg(not(unix))]
        return Err(host("private native control directories are unavailable"));
    }
    Ok(())
}

#[cfg(test)]
mod tests;
