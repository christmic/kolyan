//! Host-owned inputs and ownership-checked Task projections; no caller authority.

use std::{
    path::{Component, Path},
    sync::Arc,
};

use kolyan_agent::{
    AgentPermissions, AgentSelector, FileWriteCommittedPredicateV1, RootRunRequest, RunnerError,
};
use kolyan_core::TurnRequest;
use kolyan_model::Message;
use kolyan_server::{ExecutionRef, GoalChecker, GoalCriterion, InvocationRole, TaskSnapshot};
use kolyan_storage::StorageError;
use kolyan_tools::ExactFileBinding;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::{AgentHost, host::host};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileGoalInput {
    pub id: String,
    pub path: String,
    pub expected_content: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostStartRequest {
    pub task_id: String,
    pub invocation_id: String,
    pub attempt_id: String,
    pub turn_id: String,
    pub execution_id: String,
    pub selector: AgentSelector,
    pub requested_permissions: AgentPermissions,
    pub objective: String,
    pub messages: Vec<Message>,
    pub goals: Vec<FileGoalInput>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingApproval {
    pub invocation_id: String,
    pub attempt_id: String,
    pub approval_id: String,
}

/// Snapshot includes physical attempt states and independent goal assessments.
/// Waiting/RecoveryRequired is not reported as successful business completion.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostTaskView {
    pub task: TaskSnapshot,
    pub approvals: Vec<PendingApproval>,
    pub physical: Vec<HostAttemptView>,
}

/// Read-only Runtime disposition, separate from Task's last recorded observation.
/// This does not reconcile effects or assert that every process is quiescent.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HostAttemptView {
    pub binding: kolyan_server::AttemptBinding,
    pub execution_state: kolyan_server::ExecutionState,
}

impl AgentHost {
    /// Start one real Root. IDs are caller correlation coordinates, never grants.
    /// A repeated admitted attempt is rejected by the durable Runner boundary.
    pub async fn start(
        self: &Arc<Self>,
        session_id: String,
        request: HostStartRequest,
    ) -> Result<HostTaskView, RunnerError> {
        let this = self.clone();
        let logical = session_id.clone();
        let task_id = request.task_id.clone();
        let input = tokio::task::spawn_blocking(move || this.prepare_start(&logical, request))
            .await
            .map_err(host)??;
        self.runner.start(input).await?;
        self.advance_task(&session_id, &task_id).await?;
        self.query(session_id, task_id).await
    }

    /// Reconstruct from durable Task, ownership and exact current suspensions.
    /// No model, tool, reconciliation write or permission is issued by query.
    pub async fn query(
        self: &Arc<Self>,
        session_id: String,
        task_id: String,
    ) -> Result<HostTaskView, RunnerError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || this.read_view(&session_id, &task_id))
            .await
            .map_err(host)?
    }

    /// Deliver the configured cancellation policy. This reports durable intent,
    /// not an assertion that every running process or uncertain effect stopped.
    pub async fn cancel(
        self: &Arc<Self>,
        session_id: String,
        task_id: String,
    ) -> Result<HostTaskView, RunnerError> {
        let this = self.clone();
        tokio::task::spawn_blocking(move || {
            this.read_view(&session_id, &task_id)?;
            let digest = format!("{:x}", Sha256::digest(task_id.as_bytes()));
            this.service.cancel(
                &task_id,
                &format!("host.cancel.{digest}"),
                "cancelled by host user",
            )?;
            this.read_view(&session_id, &task_id)
        })
        .await
        .map_err(host)?
    }

    fn prepare_start(
        &self,
        session_id: &str,
        request: HostStartRequest,
    ) -> Result<RootRunRequest, RunnerError> {
        request
            .requested_permissions
            .require_subset_of(&self.permissions)?;
        if let AgentSelector::Inline(definition) = &request.selector
            && definition.model() != &self.template.model
        {
            return Err(host("inline model differs from the configured deployment"));
        }
        if request.goals.len() > 127 {
            return Err(host("request permits at most 127 explicit file goals"));
        }
        let mut goals = Vec::with_capacity(request.goals.len());
        for goal in request.goals {
            let revision = self.tools.file_adapter_revision().map_err(host)?;
            if goal.expected_content.len() > self.max_write_bytes {
                return Err(host(
                    "expected file content exceeds the configured write bound",
                ));
            }
            let path = Path::new(&goal.path);
            if goal.path.is_empty()
                || goal.path.len() > 4096
                || path.is_absolute()
                || path.components().any(|part| {
                    matches!(
                        part,
                        Component::ParentDir | Component::RootDir | Component::Prefix(_)
                    )
                })
            {
                return Err(host(
                    "file goal path must be bounded and workspace-relative",
                ));
            }
            let target = self.workspace.join(path);
            let parent = target
                .parent()
                .ok_or_else(|| host("goal requires a parent"))?
                .canonicalize()
                .map_err(host)?;
            let target = parent.join(
                target
                    .file_name()
                    .ok_or_else(|| host("goal requires a leaf"))?,
            );
            let binding = ExactFileBinding::prepare(&self.workspace, &target, &self.protected)
                .map_err(host)?;
            let predicate = FileWriteCommittedPredicateV1 {
                schema_version: 1,
                tool_revision: revision,
                workspace: binding.workspace,
                parent: binding.parent,
                leaf: binding.leaf,
                expected_bytes: goal.expected_content.len() as u64,
                expected_sha256: format!("{:x}", Sha256::digest(goal.expected_content.as_bytes())),
            };
            goals.push(GoalCriterion::new(
                goal.id,
                request.invocation_id.clone(),
                self.checker.key().clone(),
                serde_json::to_value(predicate).map_err(host)?,
            )?);
        }
        match self.service.sessions().sessions().load(session_id) {
            Ok(_) => {}
            Err(StorageError::NotFound(_)) => {
                self.service
                    .sessions()
                    .sessions()
                    .create(session_id)
                    .map_err(host)?;
            }
            Err(error) => return Err(host(error)),
        }
        let mut model_request = self.template.clone();
        model_request.request_id = request.execution_id.clone();
        model_request.messages = request.messages;
        Ok(RootRunRequest {
            goals,
            task_id: request.task_id,
            invocation_id: request.invocation_id,
            attempt_id: request.attempt_id,
            execution: ExecutionRef {
                session_id: session_id.into(),
                turn_id: request.turn_id.clone(),
                execution_id: request.execution_id,
            },
            selector: request.selector,
            requested_permissions: request.requested_permissions,
            objective: request.objective,
            limits: self.task_limits.clone(),
            cancellation_policy: self.cancellation_policy,
            turn: TurnRequest {
                turn_id: request.turn_id,
                model_request,
                config: self.turn_config,
            },
        })
    }

    pub(crate) fn read_view(
        &self,
        session_id: &str,
        task_id: &str,
    ) -> Result<HostTaskView, RunnerError> {
        let task = self.service.coordinator().snapshot(task_id)?;
        let mut roots = 0;
        let mut owners = std::collections::BTreeMap::new();
        for (id, invocation) in &task.invocations {
            let saved = self
                .bindings
                .load(task_id, id, session_id)?
                .ok_or_else(|| host("Task invocation has no saved logical owner"))?;
            if saved.snapshot.identity() != &invocation.definition.agent
                || saved.snapshot.digest() != invocation.definition.constraints_digest
                || saved.invocation_id != *id
                || invocation.definition.invocation_id != *id
                || saved.task_id != task.definition.task_id
                || saved.logical_session_id != session_id
            {
                return Err(host("Task invocation and saved Agent ownership differ"));
            }
            if invocation.definition.role == InvocationRole::Root {
                if saved.context_kind != kolyan_agent::BindingContextKind::Root
                    || saved.private_session_id != session_id
                    || task.definition.agent != *saved.snapshot.identity()
                    || task.definition.constraints_digest != saved.snapshot.digest()
                {
                    return Err(host("Task root and saved logical Session ownership differ"));
                }
                roots += 1;
            } else if saved.context_kind != kolyan_agent::BindingContextKind::Child {
                return Err(host("nonroot invocation cannot borrow root context"));
            }
            owners.insert(id, saved);
        }
        if roots != 1 {
            return Err(host("Task must have exactly one saved root owner"));
        }
        let mut approvals = Vec::new();
        let mut physical = Vec::new();
        for (attempt_id, attempt) in &task.attempts {
            let binding = &attempt.binding;
            let saved = owners
                .get(&binding.invocation_id)
                .ok_or_else(|| host("attempt invocation has no verified saved owner"))?;
            let invocation = &task.invocations[&binding.invocation_id];
            if binding.attempt_id != *attempt_id
                || binding.agent != *saved.snapshot.identity()
                || binding.constraints_digest != saved.snapshot.digest()
                || binding.execution.session_id != saved.private_session_id
                || binding.input_source != invocation.definition.input_source
            {
                return Err(host("Task attempt and saved execution ownership differ"));
            }
            let execution_state = self
                .service
                .sessions()
                .execution()
                .state(&binding.execution.execution_id)
                .map_err(host)?;
            physical.push(HostAttemptView {
                binding: binding.clone(),
                execution_state,
            });
            if attempt.state != kolyan_server::InvocationState::Suspended
                || execution_state != kolyan_server::ExecutionState::Suspended
            {
                continue;
            }
            let suspension = self
                .service
                .sessions()
                .execution()
                .load_current_suspension(&attempt.binding.execution.execution_id)
                .map_err(host)?;
            let scope = &suspension.checkpoint.scope;
            if scope.execution.session_id != binding.execution.session_id
                || scope.execution.turn_id != binding.execution.turn_id
                || scope.execution.execution_id != binding.execution.execution_id
                || scope.agent_snapshot_digest.as_deref() != Some(saved.snapshot.digest())
            {
                return Err(host(
                    "approval checkpoint differs from the admitted Agent attempt",
                ));
            }
            for pending in &suspension.checkpoint.approvals {
                if pending.evidence_id.is_none() {
                    approvals.push(PendingApproval {
                        invocation_id: attempt.binding.invocation_id.clone(),
                        attempt_id: attempt.binding.attempt_id.clone(),
                        approval_id: pending.approval_id.clone(),
                    });
                }
            }
        }
        Ok(HostTaskView {
            task,
            approvals,
            physical,
        })
    }
}
