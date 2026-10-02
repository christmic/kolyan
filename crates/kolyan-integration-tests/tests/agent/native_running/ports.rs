//! Forward original invocations unchanged; observation never returns authorization.

use super::super::{data, evidence::Evidence, tools};
use kolyan_agent::{AgentSnapshot, EnvironmentToolFactory, RunnerError, RunnerToolSet};
use kolyan_core::{ToolExecutor, ToolFuture, ToolInvocation, ToolPreparationFuture, TurnControl};
use kolyan_model::ToolCall;
use kolyan_policy::{PathScope, PolicyEngine, PreparedCall, ToolExecutionScope};
use kolyan_sandbox::SandboxProcessObservationSender;
use kolyan_server::ExecutionRef;
use kolyan_tools::{
    FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig, IsolatedToolSet,
    IsolatedToolSetConfig,
};
use serde_json::json;
use std::path::PathBuf;
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;

pub(super) struct Captured {
    pub scope: ToolExecutionScope,
    pub prepared: PreparedCall,
    pub control: TurnControl,
}
pub(super) struct State {
    pub captured: Mutex<Option<Captured>>,
    pub executions: AtomicUsize,
    pub evidence: Arc<Evidence>,
    pub capture_time: tokio::sync::watch::Sender<Option<tokio::time::Instant>>,
}
impl State {
    pub fn new(evidence: Arc<Evidence>) -> Arc<Self> {
        Arc::new(Self {
            captured: Mutex::new(None),
            executions: AtomicUsize::new(0),
            evidence,
            capture_time: tokio::sync::watch::channel(None).0,
        })
    }
}
pub(super) struct Factory {
    pub root: PathBuf,
    pub state: Arc<State>,
    pub observer: SandboxProcessObservationSender,
}
pub(super) struct Executor {
    inner: IsolatedToolSet,
    state: Arc<State>,
}
impl EnvironmentToolFactory for Factory {
    type Executor = Executor;
    fn build(
        &self,
        _: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<Executor>, RunnerError> {
        let worker = tools::worker::verified_worker(&self.root, &self.state.evidence)
            .map_err(RunnerError::Host)?;
        let workspace = self.root.join("workspace");
        let protected = vec![
            self.root.join("state"),
            self.root.join("trusted-worker"),
            worker
                .parent()
                .ok_or_else(|| RunnerError::Host("worker parent absent".into()))?
                .into(),
        ];
        let inner = IsolatedToolSet::new(IsolatedToolSetConfig {
            files: IsolatedFileConfig {
                workspace: workspace.clone(),
                staging_root: self.root.join("staging"),
                worker,
                protected_roots: protected.clone(),
                file_limits: FileOperationLimits {
                    max_read_bytes: 65536,
                    max_write_bytes: 65536,
                },
                max_output_bytes: 1024 * 1024,
                timeout: Duration::from_secs(30),
            },
            shell: IsolatedShellConfig {
                workspace: workspace.clone(),
                protected_roots: protected,
                max_command_bytes: 65536,
                max_output_bytes: 1024 * 1024,
                timeout: Duration::from_secs(30),
            },
        })
        .map_err(|error| RunnerError::Host(error.to_string()))?
        .with_process_observer(self.observer.clone());
        let workspace = workspace
            .canonicalize()
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let mut policy = PolicyEngine::default();
        for mut manifest in data::dataset().policy {
            let scope = if manifest.tool_name == "shell" {
                workspace.clone()
            } else {
                workspace.join("safe")
            };
            manifest.path_scopes = vec![PathScope::new(scope.to_string_lossy())];
            policy.register(manifest);
        }
        policy.restrict_workspace(workspace.to_string_lossy());
        Ok(RunnerToolSet {
            executor: Executor {
                inner,
                state: self.state.clone(),
            },
            definitions: IsolatedToolSet::tool_definitions(),
            policy: Arc::new(policy),
        })
    }
}
impl ToolExecutor for Executor {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        self.inner.prepare(call)
    }
    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            self.state.evidence.append(json!({"event":"native_invocation_forwarded","prepared":invocation.prepared,"scope":invocation.scope,"grant":invocation.grant,"policy_revision":invocation.policy_revision})).expect("capture original invocation");
            {
                let mut captured = self.state.captured.lock().unwrap();
                if captured.is_none() {
                    *captured = Some(Captured {
                        scope: invocation.scope.clone(),
                        prepared: invocation.prepared.clone(),
                        control: invocation.control.clone(),
                    });
                    self.state
                        .capture_time
                        .send_replace(Some(tokio::time::Instant::now()));
                }
            }
            self.state.executions.fetch_add(1, Ordering::SeqCst);
            self.inner.execute_invocation(invocation).await
        })
    }
}
