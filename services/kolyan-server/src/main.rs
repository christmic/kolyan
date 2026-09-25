//! Stdio service assembly. Model requests run independently of control queries.
mod config;

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
use tokio::io::{AsyncBufReadExt, AsyncWriteExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let path =
        std::env::var_os("KOLYAN_SERVER_CONFIG").ok_or("KOLYAN_SERVER_CONFIG is required")?;
    let config: config::Config = serde_json::from_slice(&std::fs::read(path)?)?;
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
    let host = Arc::new(ExecutionRpc::new(
        service,
        move || {
            TurnExecutor::with_tools(
                provider.clone(),
                PolicyEnforcingTool::new(
                    RestrictedFileTool::new(&config.workspace),
                    policy.clone(),
                ),
            )
            .with_policy_engine(policy.clone())
        },
        template,
        TurnConfig {
            max_steps: config.max_steps,
            max_tool_calls: Some(config.max_tool_calls),
            deadline: None,
        },
    ));
    let (sender, mut receiver) = tokio::sync::mpsc::channel::<String>(64);
    let writer = tokio::spawn(async move {
        let mut stdout = tokio::io::stdout();
        while let Some(response) = receiver.recv().await {
            stdout.write_all(response.as_bytes()).await?;
            stdout.write_all(b"\n").await?;
            stdout.flush().await?;
        }
        Ok::<_, std::io::Error>(())
    });
    let mut lines = tokio::io::BufReader::new(tokio::io::stdin()).lines();
    let mut tasks = tokio::task::JoinSet::new();
    while let Some(line) = lines.next_line().await? {
        let host = host.clone();
        let sender = sender.clone();
        tasks.spawn(async move {
            let response = host.handle_json(&line).await;
            // Reader disconnect never rolls back durable execution.
            let _ = sender.send(response).await;
        });
        while let Some(result) = tasks.try_join_next() {
            result?;
        }
    }
    drop(sender);
    while let Some(result) = tasks.join_next().await {
        result?;
    }
    writer.await??;
    Ok(())
}
