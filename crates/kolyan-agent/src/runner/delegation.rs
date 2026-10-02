//! Actual authorized child admission, private initialization and durable wait.
//! Admission never launches a child; readonly recovery never recreates missing work.

mod admission;
mod consumption;
mod driver;
mod pump;
mod resume;
pub(super) mod scheduling;
mod verifier;

pub use driver::AgentChildDriveResult;
pub use pump::AgentChildrenPumpResult;
pub use resume::ChildApprovalResumeRequest;
pub use verifier::AgentChildWaitVerifier;

use std::sync::Arc;

use kolyan_core::{ExternalWait, IssuedToolAuthority, ToolError, ToolInvocation};
use kolyan_ledger::{FactJournal, FactRef, LedgerStore};
use kolyan_policy::{PolicyEngine, ToolExecutionScope};
use kolyan_server::{AttemptBinding, PrivateContextOwner};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;
use serde::{Deserialize, Serialize};

use super::{AgentRunner, EnvironmentToolFactory, ProviderFactory};
use crate::InvokePrepareLimits;

const WAIT_KIND: &str = "agent.children";
const ADMISSION_KIND: &str = "agent.children.admitted";
const MAX_ADMISSION_BYTES: usize = 128 * 1024;

/// Independent trusted host coordinates, not fields reconstructed from a grant.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DelegationOwner {
    pub task_id: String,
    pub logical_session_id: String,
    pub parent: AttemptBinding,
    pub scope: ToolExecutionScope,
}

/// Child definitions remain in BindingStore. This receipt binds actual ownership,
/// private initialization and the exact future attempt without another snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AdmittedAgentChild {
    pub binding_fact: FactRef,
    pub instance_fact: FactRef,
    pub attempt: AttemptBinding,
    pub context_owner: PrivateContextOwner,
    pub initialization_digest: String,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Admission {
    owner: DelegationOwner,
    parent_binding_fact: FactRef,
    issued: IssuedToolAuthority,
    children: Vec<AdmittedAgentChild>,
    parallel_requested: bool,
    // False only for explicitly attested read-only work without a token ceiling.
    serialized: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct WaitBinding {
    admission: FactRef,
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    /// Enforce fresh scoped authority, reserve host identities, admit children and
    /// initialize selected private inputs before committing one durable wait fact.
    /// The caller must supply authenticated host scope and current policy. No grant
    /// is issued here. Partial cross-store admission is uncertain, not replay-safe.
    pub async fn admit_agent_children(
        self: &Arc<Self>,
        owner: DelegationOwner,
        invocation: ToolInvocation,
        limits: InvokePrepareLimits,
        policy: Arc<PolicyEngine>,
    ) -> Result<ExternalWait, ToolError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            runner.admit_children_blocking(owner, invocation, limits, policy)
        })
        .await
        .map_err(|error| uncertain(format!("child admission worker failed: {error}")))?
    }

    /// Read only an already committed exact admission; absence is not permission
    /// to rerun an entered effect. This does not use current catalog content or
    /// grant another attempt. Runtime must verify the returned wait before publish.
    pub async fn recover_agent_child_wait(
        self: &Arc<Self>,
        owner: DelegationOwner,
        issued: IssuedToolAuthority,
    ) -> Result<Option<ExternalWait>, ToolError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            issued.validate(&owner.scope).map_err(denied)?;
            let coordinate = admission_coordinate(&owner, &issued)?;
            let Some(admission) = runner.load_child_admission(&coordinate)? else {
                return Ok(None);
            };
            runner.verify_child_admission(&owner, &issued, &admission)?;
            wait(coordinate).map(Some)
        })
        .await
        .map_err(|error| uncertain(format!("child recovery worker failed: {error}")))?
    }

    /// Inspect only historical ownership and exact admitted wait. This read-only
    /// evidence port never authorizes child execution or result consumption.
    pub async fn inspect_agent_child_wait(
        self: &Arc<Self>,
        owner: DelegationOwner,
        issued: IssuedToolAuthority,
        supplied: ExternalWait,
    ) -> Result<Vec<AdmittedAgentChild>, ToolError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            let coordinate = admission_coordinate(&owner, &issued)?;
            if supplied != wait(coordinate.clone())? {
                return Err(denied("external wait differs from exact host admission"));
            }
            let admission = runner
                .load_child_admission(&coordinate)?
                .ok_or_else(|| denied("child admission is absent"))?;
            runner.inspect_historical_child_admission(&owner, &issued, &admission)?;
            Ok(admission.children)
        })
        .await
        .map_err(uncertain)?
    }

    /// Resolve the exact wait fact and revalidate ownership/initialization without
    /// changing journals or starting any children. Unknown schemas fail closed.
    pub async fn verify_agent_child_wait(
        self: &Arc<Self>,
        owner: DelegationOwner,
        issued: IssuedToolAuthority,
        supplied: ExternalWait,
    ) -> Result<Vec<AdmittedAgentChild>, ToolError> {
        let runner = self.clone();
        tokio::task::spawn_blocking(move || {
            issued.validate(&owner.scope).map_err(denied)?;
            let coordinate = admission_coordinate(&owner, &issued)?;
            if supplied != wait(coordinate.clone())? {
                return Err(denied("external wait differs from exact host admission"));
            }
            let admission = runner
                .load_child_admission(&coordinate)?
                .ok_or_else(|| denied("child admission is absent"))?;
            runner.verify_child_admission(&owner, &issued, &admission)?;
            Ok(admission.children)
        })
        .await
        .map_err(|error| uncertain(format!("child verification worker failed: {error}")))?
    }
}

fn admission_coordinate(
    owner: &DelegationOwner,
    issued: &IssuedToolAuthority,
) -> Result<FactRef, ToolError> {
    let identity = crate::digest(&(
        "kolyan.agent.children.admission.v1",
        &owner.task_id,
        &owner.parent,
        &owner.scope,
        &issued.prepared.call().id,
    ))
    .map_err(denied)?;
    Ok(FactRef {
        stream_id: format!("agent.children.{identity}"),
        position: 1,
        fact_id: format!("agent.children.{identity}"),
    })
}

fn wait(coordinate: FactRef) -> Result<ExternalWait, ToolError> {
    let wait = ExternalWait {
        wait_id: coordinate.fact_id.clone(),
        kind: WAIT_KIND.into(),
        schema_version: 1,
        binding: serde_json::to_value(WaitBinding {
            admission: coordinate,
        })
        .map_err(denied)?,
    };
    wait.validate().map_err(denied)?;
    Ok(wait)
}

pub(super) fn denied(message: impl std::fmt::Display) -> ToolError {
    ToolError::PolicyDenied {
        message: message.to_string(),
    }
}

fn uncertain(message: impl std::fmt::Display) -> ToolError {
    ToolError::Uncertain {
        message: message.to_string(),
    }
}

#[cfg(test)]
mod tests;
