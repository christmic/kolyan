//! Shared dependency assembly for stdio and HTTP; no transport-specific policy.

use crate::config;
use kolyan_core::{TurnConfig, TurnExecutor};
use kolyan_ledger::SqliteLedger;
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, PathScope, PolicyEngine, ToolManifest,
};
use kolyan_server::{ExecutionRpc, ExecutionService, SessionExecutionService, SessionService};
use kolyan_storage::FileSessionStore;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::NoopTraceSink;
use std::sync::Arc;

pub type Service = SessionExecutionService<SqliteLedger, NoopTraceSink, FileSessionStore>;
pub type Executor =
    TurnExecutor<config::Provider, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>>;

#[derive(Clone)]
pub struct App {
    pub service: Service,
    pub template: kolyan_model::ModelRequest,
    pub budget: TurnConfig,
    provider: config::Provider,
    policy: Arc<PolicyEngine>,
    workspace: std::path::PathBuf,
}

impl App {
    pub fn new(config: config::Config) -> Result<Self, Box<dyn std::error::Error>> {
        if config.max_steps == 0 || config.max_tool_calls == 0 || config.timeout_secs == 0 {
            return Err("execution budgets must be positive".into());
        }
        std::fs::create_dir_all(&config.workspace)?;
        let provider = config.provider()?;
        let mut policy = PolicyEngine::default();
        for (name, capability, effect, approval, idempotency) in [
            (
                "file.read",
                Capability::FilesystemRead,
                Effect::Read,
                ApprovalMode::Never,
                Idempotency::Idempotent,
            ),
            (
                "file.write",
                Capability::FilesystemWrite,
                Effect::Update,
                ApprovalMode::Always,
                Idempotency::NonIdempotent,
            ),
        ] {
            policy.register(ToolManifest {
                tool_name: name.into(),
                capabilities: [capability].into_iter().collect(),
                effects: [effect].into_iter().collect(),
                path_scopes: vec![PathScope::new(&config.tool_scope)],
                idempotency,
                approval,
            });
        }
        policy.restrict_workspace(&config.tool_scope);
        let policy = policy.with_progress_policy(config.progress)?;
        let policy = Arc::new(policy);
        let ledger = SqliteLedger::open(&config.ledger_path)?;
        let sessions = FileSessionStore::new(&config.session_root)?;
        let service = SessionExecutionService::new(
            ExecutionService::new(ledger, NoopTraceSink),
            SessionService::new(sessions),
        )
        .with_context_policy(kolyan_storage::SessionContextPolicy::FullTrajectory);
        let mut template = config.request;
        template.tools = RestrictedFileTool::tool_definitions();
        Ok(Self {
            service,
            template,
            provider,
            policy,
            workspace: config.workspace,
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
            PolicyEnforcingTool::new(
                RestrictedFileTool::new(&self.workspace),
                self.policy.clone(),
            ),
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
        PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>,
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
