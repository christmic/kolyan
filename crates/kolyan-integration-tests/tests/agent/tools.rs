//! The fixture is the trusted host; effects use production isolated workers.

mod observations;
mod readonly;
pub(crate) mod worker;

pub use worker::initialize_worker;

use std::{
    path::PathBuf,
    sync::Arc,
    time::{Duration, Instant},
};

use kolyan_agent::{
    AgentSnapshot, EnvironmentTool, EnvironmentToolFactory, RunnerError, RunnerToolSet,
};
use kolyan_core::{
    ToolError, ToolExecutor, ToolFuture, ToolInvocation, ToolOutcome, ToolPreparationFuture,
    TurnControl,
};
use kolyan_model::ToolCall;
use kolyan_policy::{PathScope, PolicyEngine};
use kolyan_server::ExecutionRef;
use kolyan_tools::{
    FileOperationLimits, IsolatedFileConfig, IsolatedShellConfig, IsolatedToolSet,
    IsolatedToolSetConfig,
};
use serde_json::{Value, json};

use super::{data::Dataset, evidence::Evidence};

pub struct Tools {
    pub root: PathBuf,
    pub dataset: Dataset,
    pub evidence: Arc<Evidence>,
}
impl EnvironmentToolFactory for Tools {
    type Executor = ObservedTools<readonly::AuthorityTools>;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<RunnerToolSet<Self::Executor>, RunnerError> {
        let workspace = self.root.join("workspace");
        let worker =
            worker::verified_worker(&self.root, &self.evidence).map_err(RunnerError::Host)?;
        let protected = vec![
            self.root.join("state"),
            self.root.join("trusted-worker"),
            worker
                .parent()
                .ok_or_else(|| RunnerError::Host("installation root missing".into()))?
                .to_path_buf(),
        ];
        let executor = IsolatedToolSet::new(IsolatedToolSetConfig {
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
        .map_err(|error| RunnerError::Host(error.to_string()))?;
        let scope = workspace
            .join(&self.dataset.policy_scope)
            .canonicalize()
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let shell_scope = workspace
            .join(&self.dataset.shell_policy_scope)
            .canonicalize()
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let workspace_scope = workspace
            .canonicalize()
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let mut policy = PolicyEngine::default();
        for mut manifest in self.dataset.policy.clone() {
            // Shell preparation claims the entire enforced workspace, not its cwd.
            // File manifests remain independently confined to the fixture's safe tree.
            let tool_scope = if manifest.tool_name == "shell" {
                &shell_scope
            } else {
                &scope
            };
            manifest.path_scopes = vec![PathScope::new(tool_scope.to_string_lossy())];
            policy.register(manifest);
        }
        policy.restrict_workspace(workspace_scope.to_string_lossy());
        Ok(RunnerToolSet {
            executor: ObservedTools {
                inner: readonly::AuthorityTools::new(executor, read_only(snapshot)),
                evidence: self.evidence.clone(),
            },
            definitions: IsolatedToolSet::tool_definitions(),
            policy: Arc::new(policy),
        })
    }

    fn enforces_read_only_parallel(&self, snapshot: &AgentSnapshot) -> bool {
        // This factory constructs AuthorityTools, not an arbitrary host adapter.
        // Its read-only branch checks actual prepared claims and exact grants;
        // IsolatedFileTools additionally launches Read with zero writable leaves.
        read_only(snapshot)
    }
}

fn read_only(snapshot: &AgentSnapshot) -> bool {
    snapshot.permissions().tools == [EnvironmentTool::Read].into()
}

/// Test-only observation at the actual tool adapter boundary. It cannot expose
/// an internal native worker phase or identify a journal stall by itself.
pub struct ObservedTools<T> {
    pub inner: T,
    pub evidence: Arc<Evidence>,
}

impl<T: ToolExecutor> ToolExecutor for ObservedTools<T> {
    fn prepare(&self, call: ToolCall) -> ToolPreparationFuture<'_> {
        let span = Observation::new(self.evidence.clone(), "prepare", json!({"call":call}), None);
        let future = self.inner.prepare(call);
        Box::pin(async move {
            let mut span = span;
            span.polled();
            let result = future.await;
            span.returned(match &result {
                Ok(prepared) => json!({"status":"prepared","prepared":prepared}),
                Err(error) => json!({"status":"error","error":error.to_string(),"error_kind":format!("{error:?}")}),
            });
            result
        })
    }

    fn execute_invocation(&self, invocation: ToolInvocation) -> ToolFuture<'_> {
        let issued = invocation.prepared.clone();
        let span = Observation::new(
            self.evidence.clone(),
            "execute",
            json!({"prepared":invocation.prepared,"grant":invocation.grant,"scope":invocation.scope,
                "policy_revision":invocation.policy_revision}),
            Some(invocation.control.clone()),
        );
        let future = self.inner.execute_invocation(invocation);
        Box::pin(async move {
            let mut span = span;
            span.polled();
            let result = future.await;
            span.returned(match &result {
                Ok(ToolOutcome::Completed(result)) => json!({"status":"completed","result":result}),
                Ok(ToolOutcome::AwaitingExternal(wait)) => json!({"status":"awaiting_external","wait":wait}),
                Err(error) => json!({"status":"error","error":error.to_string(),"error_kind":format!("{error:?}")}),
            });
            if matches!(&result, Err(ToolError::PolicyDenied { message }) if message.contains("binding mismatch"))
            {
                // Diagnostic only: this is NOT the adapter's private re-preparation
                // at failure time. No grant is issued and no execution is retried.
                let fresh = self.inner.prepare(issued.call().clone()).await;
                let record = match fresh {
                    Ok(current) => json!({"current":current,
                        "digest_changed":current.digest()!=issued.digest(),
                        "tool_revision_changed":current.tool_revision()!=issued.tool_revision(),
                        "binding_changed":current.execution_binding()!=issued.execution_binding()}),
                    Err(error) => {
                        json!({"error":error.to_string(),"error_kind":format!("{error:?}")})
                    }
                };
                self.evidence
                    .append(json!({"event":"post_failure_fresh_preparation",
                    "meaning":"later_observation_not_exact_failure_repreparation",
                    "issued":issued,"observation":record}))
                    .expect("preparation diagnostic write");
            }
            result
        })
    }
}

struct Observation {
    evidence: Arc<Evidence>,
    id: u64,
    phase: &'static str,
    operation: Value,
    started: Instant,
    polled: bool,
    returned: bool,
    control: Option<TurnControl>,
}

impl Observation {
    fn new(
        evidence: Arc<Evidence>,
        phase: &'static str,
        operation: Value,
        control: Option<TurnControl>,
    ) -> Self {
        let observation = Self {
            id: evidence.next_observation_id(),
            evidence,
            phase,
            operation,
            started: Instant::now(),
            polled: false,
            returned: false,
            control,
        };
        observation
            .evidence
            .append(observation.record("created", Value::Null))
            .expect("tool observation write");
        observation
    }

    fn polled(&mut self) {
        self.polled = true;
        self.evidence
            .append(self.record("polled", Value::Null))
            .expect("tool observation write");
    }

    fn returned(&mut self, outcome: Value) {
        self.returned = true;
        self.evidence
            .append(self.record("returned", outcome))
            .expect("tool observation write");
    }

    fn record(&self, stage: &str, outcome: Value) -> Value {
        json!({"event":"tool_adapter","observation_id":self.id,"phase":self.phase,"stage":stage,
            "elapsed_ns":self.started.elapsed().as_nanos(),"operation":self.operation,"outcome":outcome,
            "polled":self.polled,"inner_returned":self.returned,
            "control_cancelled":self.control.as_ref().map(TurnControl::is_cancelled)})
    }
}

impl Drop for Observation {
    fn drop(&mut self) {
        let completion = if self.returned {
            "returned"
        } else if self.polled {
            "dropped_while_pending"
        } else {
            "dropped_before_poll"
        };
        self.evidence
            .append_from_drop(self.record("dropped", json!({"completion":completion})));
    }
}
