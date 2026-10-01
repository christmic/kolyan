//! Shared dependency assembly for stdio and HTTP; no transport-specific policy.

use crate::config;
use kolyan_core::{TurnConfig, TurnExecutor};
use kolyan_ledger::SqliteLedger;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_server::{ExecutionRpc, ExecutionService, SessionExecutionService, SessionService};
use kolyan_storage::FileSessionStore;
use kolyan_tools::{
    FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig, IsolatedToolSet,
    IsolatedToolSetConfig, PolicyEnforcingTool,
};
use kolyan_trace::NoopTraceSink;
use std::{
    path::{Component, Path, PathBuf},
    sync::Arc,
    time::Duration,
};

pub type Service = SessionExecutionService<SqliteLedger, NoopTraceSink, FileSessionStore>;
pub type Executor =
    TurnExecutor<config::Provider, PolicyEnforcingTool<IsolatedToolSet, PolicyEngine>>;

#[derive(Clone)]
pub struct App {
    pub service: Service,
    pub template: kolyan_model::ModelRequest,
    pub budget: TurnConfig,
    provider: config::Provider,
    policy: Arc<PolicyEngine>,
    tools: IsolatedToolSet,
}

impl App {
    pub fn new(
        mut config: config::Config,
        config_path: &Path,
    ) -> Result<Self, Box<dyn std::error::Error>> {
        if config.max_steps == 0 || config.max_tool_calls == 0 || config.timeout_secs == 0 {
            return Err("execution budgets must be positive".into());
        }
        let (tools, policy) = environment(&mut config, config_path)?;
        let provider = config.provider()?;
        let policy = Arc::new(policy);
        let ledger = SqliteLedger::open(&config.ledger_path)?;
        let sessions = FileSessionStore::new(&config.session_root)?;
        let service = SessionExecutionService::new(
            ExecutionService::new(ledger, NoopTraceSink),
            SessionService::new(sessions),
        )
        .with_context_policy(kolyan_storage::SessionContextPolicy::FullTrajectory);
        let definitions = advertised_tools(&config)?;
        let mut template = config.request;
        template.tools = definitions;
        Ok(Self {
            service,
            template,
            provider,
            policy,
            tools,
            budget: TurnConfig {
                max_steps: config.max_steps,
                max_tool_calls: Some(config.max_tool_calls),
                deadline: None,
            },
        })
    }

    pub fn executor(&self) -> Executor {
        TurnExecutor::with_tools(
            self.provider.clone(),
            PolicyEnforcingTool::new(self.tools.clone(), self.policy.clone()),
        )
        .with_policy_engine(self.policy.clone())
    }

    pub fn rpc(
        self,
    ) -> ExecutionRpc<
        SqliteLedger,
        NoopTraceSink,
        FileSessionStore,
        config::Provider,
        PolicyEnforcingTool<IsolatedToolSet, PolicyEngine>,
    > {
        let app = self.clone();
        ExecutionRpc::new(
            self.service,
            move || app.executor(),
            self.template,
            self.budget,
        )
    }
}

/// Advertising is a projection of the trusted ceiling, never authorization.
/// Keep runtime denial even for tools omitted from model feedback.
fn advertised_tools(
    config: &config::Config,
) -> Result<Vec<kolyan_model::ToolDefinition>, Box<dyn std::error::Error>> {
    let shell_available = config.allow_shell
        && physical_scope(&config.workspace, &config.tool_scope)? == config.workspace;
    let mut definitions = IsolatedToolSet::tool_definitions();
    if !shell_available {
        definitions.retain(|tool| tool.name != "shell");
    }
    Ok(definitions)
}

/// Freeze physical resources before opening persistence or obtaining credentials.
/// Tool preparation and policy consequently refer to the same absolute namespace.
fn environment(
    config: &mut config::Config,
    config_path: &Path,
) -> Result<(IsolatedToolSet, PolicyEngine), Box<dyn std::error::Error>> {
    config.workspace = directory(&config.workspace)?;
    config.ledger_path = physical_leaf(&config.ledger_path)?;
    let state_root = directory(
        config
            .ledger_path
            .parent()
            .ok_or("ledger requires a state directory")?,
    )?;
    if state_root.starts_with(&config.workspace) || config.workspace.starts_with(&state_root) {
        return Err("ledger state directory and workspace must be disjoint".into());
    }
    config.session_root = physical_leaf(&config.session_root)?;
    if !config.session_root.starts_with(&state_root) || config.session_root == state_root {
        return Err("sessions must be below the ledger state directory".into());
    }
    config.staging_root = private_staging(&config.staging_root, &config.workspace)?;
    if config.staging_root.starts_with(&state_root) || state_root.starts_with(&config.staging_root)
    {
        return Err("staging and persistent control roots must be disjoint".into());
    }
    config.worker_path = config.worker_path.canonicalize()?;
    if !config.worker_path.is_file() || config.worker_path.starts_with(&state_root) {
        return Err("worker must be a trusted executable outside control state".into());
    }
    let config_path = config_path.canonicalize()?;
    if !config_path.is_file() || config_path.starts_with(&config.staging_root) {
        return Err("loaded configuration must be an existing file outside staging".into());
    }
    let scope = physical_scope(&config.workspace, &config.tool_scope)?;
    if scope
        .components()
        .any(|part| matches!(part, Component::Normal(name) if name == ".git" || name == ".kolyan"))
    {
        return Err("tool scope selects protected control paths".into());
    }
    let scope = scope.to_str().ok_or("scope must be UTF-8")?;
    let protected = vec![state_root, config_path];
    let tools = IsolatedToolSet::new(IsolatedToolSetConfig {
        files: IsolatedFileConfig {
            workspace: config.workspace.clone(),
            worker: config.worker_path.clone(),
            staging_root: config.staging_root.clone(),
            protected_roots: protected.clone(),
            file_limits: FileOperationLimits {
                max_read_bytes: 1024 * 1024,
                max_write_bytes: 1024 * 1024,
            },
            max_output_bytes: 1024 * 1024,
            timeout: Duration::from_secs(30),
        },
        shell: IsolatedShellConfig {
            workspace: config.workspace.clone(),
            protected_roots: protected,
            max_command_bytes: 64 * 1024,
            max_output_bytes: 1024 * 1024,
            timeout: Duration::from_secs(30),
        },
    })?;
    let mut policy = PolicyEngine::default();
    for (name, capabilities, effects, approval, idempotency) in [
        (
            "file.read",
            vec![Capability::FilesystemRead],
            vec![Effect::Read],
            ApprovalMode::Never,
            Idempotency::Idempotent,
        ),
        (
            "file.write",
            vec![Capability::FilesystemWrite],
            vec![Effect::Create, Effect::Update],
            ApprovalMode::Always,
            Idempotency::NonIdempotent,
        ),
        (
            "file.edit",
            vec![Capability::FilesystemRead, Capability::FilesystemWrite],
            vec![Effect::Read, Effect::Update],
            ApprovalMode::Always,
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
            ApprovalMode::Always,
            Idempotency::NonIdempotent,
        ),
    ] {
        policy.register(ToolManifest {
            tool_name: name.into(),
            capabilities: capabilities.into_iter().collect(),
            effects: effects.into_iter().collect(),
            path_scopes: vec![PathScope::new(scope)],
            approval,
            idempotency,
        });
    }
    if !config.allow_shell {
        policy.deny_tool("shell");
    }
    policy.restrict_workspace(scope);
    Ok((tools, policy.with_progress_policy(config.progress.clone())?))
}

fn directory(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let physical = path.canonicalize()?;
    if !physical.is_dir() || physical.parent().is_none() {
        return Err("host directory is invalid or filesystem root".into());
    }
    Ok(physical)
}

fn physical_leaf(path: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    if std::fs::symlink_metadata(path).is_ok_and(|metadata| metadata.file_type().is_symlink()) {
        return Err("persistent control leaf must not be a symlink".into());
    }
    if path.exists() {
        return Ok(path.canonicalize()?);
    }
    let parent = path
        .parent()
        .ok_or("host resource requires a parent")?
        .canonicalize()?;
    let leaf = path.file_name().ok_or("host resource requires a leaf")?;
    Ok(parent.join(leaf))
}

fn physical_scope(
    workspace: &Path,
    configured: &str,
) -> Result<PathBuf, Box<dyn std::error::Error>> {
    let path = Path::new(configured);
    if configured.trim().is_empty()
        || configured.len() > 4096
        || configured.chars().any(char::is_control)
        || path.is_absolute()
        || path.components().any(|part| {
            matches!(
                part,
                Component::ParentDir | Component::RootDir | Component::Prefix(_)
            )
        })
    {
        return Err(
            "tool_scope must be a bounded workspace-relative directory without parent traversal"
                .into(),
        );
    }
    let physical = directory(&workspace.join(path))?;
    if !physical.starts_with(workspace) {
        return Err("tool_scope escapes physical workspace".into());
    }
    Ok(physical)
}

#[cfg(unix)]
fn private_staging(path: &Path, workspace: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
    let physical = physical_leaf(path)?;
    if physical.starts_with(workspace) || workspace.starts_with(&physical) {
        return Err("staging must be outside and disjoint from workspace".into());
    }
    if !physical.exists() {
        std::fs::DirBuilder::new().mode(0o700).create(&physical)?;
    }
    let metadata = std::fs::symlink_metadata(&physical)?;
    if !metadata.is_dir()
        || metadata.permissions().mode() & 0o7777 != 0o700
        || metadata.dev() != std::fs::metadata(workspace)?.dev()
    {
        return Err("staging must be a private 0700 directory on the workspace filesystem".into());
    }
    Ok(physical)
}

#[cfg(not(unix))]
fn private_staging(_: &Path, _: &Path) -> Result<PathBuf, Box<dyn std::error::Error>> {
    Err("private staging and macOS sandbox are unavailable".into())
}

#[cfg(test)]
mod tests;
