//! Root source preparation precedes Task admission and never enters execution.

use std::sync::Arc;

use kolyan_ledger::{FactJournal, FactRef, LedgerStore};
use kolyan_model::{ModelRequest, SystemInstruction, ToolChoice};
use kolyan_server::{ExecutionRef, InstanceOwner, VerifiedInvocationInputSource};
use kolyan_storage::SessionStore;
use kolyan_trace::TraceSink;

use super::{AgentRunner, EnvironmentToolFactory, ProviderFactory, RunnerError, RunnerToolSet};
use crate::{
    AgentInvocationBinding, AgentPermissions, AgentSelector, AgentSnapshot, BindingContextKind,
};

/// Host input before Task admission. The logical Session must already exist;
/// tools must be empty and the output ceiling explicitly nonzero.
pub struct RootInputPreparationRequest {
    pub task_id: String,
    pub invocation_id: String,
    pub execution: ExecutionRef,
    pub selector: AgentSelector,
    pub requested_permissions: AgentPermissions,
    pub model_request: ModelRequest,
}

/// Exact retained preparation. This is input evidence, not an execution grant.
pub struct PreparedRootInput {
    pub snapshot: AgentSnapshot,
    pub ownership: FactRef,
    pub selected_input: ModelRequest,
    pub input_source: VerifiedInvocationInputSource,
}

pub(super) struct AssembledRoot<E> {
    pub request: RootInputPreparationRequest,
    pub saved: AgentInvocationBinding,
    original_input: ModelRequest,
    tool_set: RunnerToolSet<E>,
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
    /// Reserve immutable identity and retain owner/full input/source. This builds
    /// trusted inventory but never constructs a Provider, executes a tool, issues
    /// a grant or registers/admits/starts a Task. Exact retries preserve sources;
    /// changed content refuses publication. Dropping this future does not roll
    /// back its blocking publisher; recovery must inspect exact durable facts.
    pub async fn prepare_root_input(
        self: &Arc<Self>,
        request: RootInputPreparationRequest,
    ) -> Result<PreparedRootInput, RunnerError> {
        let runner = Arc::clone(self);
        tokio::task::spawn_blocking(move || {
            let assembled = runner.assemble_root_input(request)?;
            runner
                .retain_root_input(assembled)
                .map(|(input, _, _)| input)
        })
        .await
        .map_err(|error| RunnerError::Host(format!("root preparation worker failed: {error}")))?
    }

    pub(super) fn assemble_root_input(
        self: &Arc<Self>,
        mut request: RootInputPreparationRequest,
    ) -> Result<AssembledRoot<T::Executor>, RunnerError> {
        let original_input = request.model_request.clone();
        for id in [
            &request.task_id,
            &request.invocation_id,
            &request.execution.session_id,
            &request.execution.turn_id,
            &request.execution.execution_id,
        ] {
            crate::identity(id)?;
        }
        if !request.model_request.tools.is_empty()
            || request
                .model_request
                .max_output_tokens
                .is_none_or(|limit| limit == 0)
        {
            return Err(RunnerError::Host(
                "Turn identity, trusted inventory or explicit output limit is invalid".into(),
            ));
        }
        self.service
            .sessions()
            .sessions()
            .load(&request.execution.session_id)
            .map_err(|error| RunnerError::Host(format!("logical Session unavailable: {error}")))?;
        let reservation = self.instances.reserve(InstanceOwner {
            logical_session_id: request.execution.session_id.clone(),
            task_id: request.task_id.clone(),
            invocation_id: request.invocation_id.clone(),
        })?;
        let snapshot = self.catalog.resolve(
            &request.selector,
            reservation.instance_id,
            &self.host,
            &request.requested_permissions,
        )?;
        let tool_set = self.routed_tool_set(&snapshot, &request.execution)?;
        request.model_request.model = snapshot.definition().model().clone();
        request.model_request.system.push(SystemInstruction {
            text: snapshot.definition().instructions().into(),
            cache: false,
        });
        request.model_request.tools = tool_set
            .definitions
            .iter()
            .filter(|definition| {
                definition.name == crate::AGENT_INVOKE_NAME
                    || super::tools::permits(&snapshot, &definition.name)
            })
            .cloned()
            .collect();
        match &request.model_request.tool_choice {
            ToolChoice::Required if request.model_request.tools.is_empty() => {
                return Err(RunnerError::Host(
                    "required tools conflict with an empty Agent ceiling".into(),
                ));
            }
            ToolChoice::Tool(name)
                if !request
                    .model_request
                    .tools
                    .iter()
                    .any(|tool| &tool.name == name) =>
            {
                return Err(RunnerError::Host(
                    "named tool exceeds the Agent ceiling".into(),
                ));
            }
            _ => {}
        }
        let saved = AgentInvocationBinding {
            task_id: request.task_id.clone(),
            invocation_id: request.invocation_id.clone(),
            logical_session_id: request.execution.session_id.clone(),
            private_session_id: request.execution.session_id.clone(),
            context_kind: BindingContextKind::Root,
            snapshot: snapshot.clone(),
        };
        Ok(AssembledRoot {
            request,
            saved,
            original_input,
            tool_set,
        })
    }

    pub(super) fn retain_root_input(
        &self,
        assembled: AssembledRoot<T::Executor>,
    ) -> Result<
        (
            PreparedRootInput,
            RunnerToolSet<T::Executor>,
            AgentInvocationBinding,
        ),
        RunnerError,
    > {
        let AssembledRoot {
            request,
            saved,
            original_input,
            tool_set,
        } = assembled;
        let inventory = tool_set.definitions.clone();
        let snapshot = saved.snapshot.clone();
        let ownership = self.bindings.save(&saved)?;
        let input = self.publish_input_document(
            &saved,
            kolyan_server::InvocationInputKind::Standalone,
            &super::input::RootInput {
                ownership: ownership.clone(),
                original_input,
                execution: request.execution.clone(),
                requested_permissions: request.requested_permissions.clone(),
                selected_input: request.model_request.clone(),
                inventory,
            },
            vec![ownership.clone()],
        )?;
        Ok((
            PreparedRootInput {
                snapshot,
                ownership,
                selected_input: request.model_request,
                input_source: input,
            },
            tool_set,
            saved,
        ))
    }
}

#[cfg(test)]
mod tests;
