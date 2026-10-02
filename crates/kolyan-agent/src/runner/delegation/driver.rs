//! Real serial or host-attested bounded read-only child driving.
//! Started work is never implicitly replayed, including after host restart.

use kolyan_core::{TurnExecutor, TurnRequest};
use kolyan_model::{SystemInstruction, ToolChoice};
use kolyan_server::{InvocationState, VerifiedTaskResult};

use super::*;
use crate::runner::routing::RoutedTools;
use crate::runner::tools::{SnapshotTools, permits};

/// A real stopped child or a durable approval/external suspension. A failure or
/// cancellation remains the Server's typed terminal result, never Completed text.
pub enum AgentChildDriveResult {
    Terminal {
        result: Box<VerifiedTaskResult>,
        dispatch_error: Option<String>,
    },
    Waiting {
        child: Box<AdmittedAgentChild>,
        execution: Box<kolyan_runtime::DurableTurnResult>,
    },
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
    /// Drive admitted children under conservative serial/read-only fanout policy.
    /// The trusted request template must
    /// contain no messages or schemas: private initialization supplies selected
    /// input exactly once and the factory supplies the environment inventory.
    /// Existing terminal attempts are read, not rerun. Suspended/uncertain attempts
    /// require their respective explicit resume/recovery operation.
    /// Actual shared usage is recorded by TaskExecutionService; this does not claim
    /// concurrent token reservations or safe writable resource isolation.
    pub async fn drive_agent_children(
        self: &Arc<Self>,
        owner: DelegationOwner,
        issued: IssuedToolAuthority,
        supplied: ExternalWait,
        template: TurnRequest,
    ) -> Result<Vec<AgentChildDriveResult>, ToolError> {
        if !template.model_request.messages.is_empty()
            || !template.model_request.tools.is_empty()
            || template
                .model_request
                .max_output_tokens
                .is_none_or(|n| n == 0)
        {
            return Err(denied(
                "child template must have explicit output limit and no messages/schemas",
            ));
        }
        let children = self
            .verify_agent_child_wait(owner.clone(), issued.clone(), supplied)
            .await?;
        let runner = self.clone();
        let scheduling_owner = owner.clone();
        let scheduling_children = children.clone();
        let (parallel, advance_readonly_waits) = tokio::task::spawn_blocking(move || {
            let coordinate = admission_coordinate(&scheduling_owner, &issued)?;
            let admission = runner
                .load_child_admission(&coordinate)?
                .ok_or_else(|| denied("admission disappeared"))?;
            let parallel = runner.parallel_eligible(
                &scheduling_owner,
                &scheduling_children,
                !admission.serialized,
            )?;
            let advance = admission.parallel_requested
                && runner.read_only_children(&scheduling_owner, &scheduling_children)?;
            Ok::<_, ToolError>((parallel, advance))
        })
        .await
        .map_err(uncertain)??;
        if parallel {
            return self
                .drive_parallel_children(owner, children, template)
                .await;
        }
        let mut results = Vec::with_capacity(children.len());
        for child in children {
            let result = self
                .drive_one_child(owner.clone(), child, template.clone())
                .await?;
            let waiting = matches!(result, AgentChildDriveResult::Waiting { .. });
            results.push(result);
            if waiting && !advance_readonly_waits {
                break;
            }
        }
        Ok(results)
    }

    async fn drive_parallel_children(
        self: &Arc<Self>,
        owner: DelegationOwner,
        children: Vec<AdmittedAgentChild>,
        template: TurnRequest,
    ) -> Result<Vec<AgentChildDriveResult>, ToolError> {
        let mut jobs = tokio::task::JoinSet::new();
        for (index, child) in children.into_iter().enumerate() {
            let runner = self.clone();
            let owner = owner.clone();
            let template = template.clone();
            jobs.spawn(async move {
                let result = runner.drive_one_child(owner, child, template).await;
                Ok::<_, ToolError>((index, result))
            });
        }
        let mut completed = Vec::new();
        let mut error = None;
        while let Some(joined) = jobs.join_next().await {
            match joined {
                Ok(Ok((index, Ok(result)))) => completed.push((index, result)),
                Ok(Ok((_, Err(failure)))) | Ok(Err(failure)) => {
                    error.get_or_insert(failure);
                }
                Err(failure) => {
                    error.get_or_insert_with(|| uncertain(failure));
                }
            }
        }
        // Drain all entered work before returning an error; dropping a sibling
        // future is not evidence that its effect did not start.
        if let Some(error) = error {
            return Err(error);
        }
        completed.sort_by_key(|(index, _)| *index);
        Ok(completed.into_iter().map(|(_, result)| result).collect())
    }

    async fn drive_one_child(
        self: &Arc<Self>,
        owner: DelegationOwner,
        child: AdmittedAgentChild,
        template: TurnRequest,
    ) -> Result<AgentChildDriveResult, ToolError> {
        let runner = self.clone();
        let current_owner = owner.clone();
        let current_child = child.clone();
        let input = template.clone();
        let prepared = tokio::task::spawn_blocking(move || {
            runner.validate_child_owner(&current_owner)?;
            let state = runner
                .service
                .coordinator()
                .snapshot(&current_owner.task_id)
                .map_err(denied)?;
            if let Some(attempt) = state.attempts.get(&current_child.attempt.attempt_id) {
                if attempt.binding != current_child.attempt {
                    return Err(denied("existing child attempt differs"));
                }
                if matches!(
                    attempt.state,
                    InvocationState::Completed
                        | InvocationState::Failed
                        | InvocationState::Cancelled
                ) {
                    let result = runner
                        .service
                        .load_verified_result(
                            &current_owner.task_id,
                            &current_child.attempt,
                            1024 * 1024,
                        )
                        .map_err(denied)?;
                    return Ok(Err(AgentChildDriveResult::Terminal {
                        result: Box::new(result),
                        dispatch_error: None,
                    }));
                }
                if attempt.state == InvocationState::Suspended && !attempt.cancellation_requested {
                    let execution = runner.service.sessions().execution();
                    let suspension = execution
                        .load_current_suspension(&current_child.attempt.execution.execution_id)
                        .map_err(denied)?;
                    let key = &suspension.checkpoint.scope.execution;
                    if key.session_id != current_child.attempt.execution.session_id
                        || key.turn_id != current_child.attempt.execution.turn_id
                        || key.execution_id != current_child.attempt.execution.execution_id
                        || suspension.checkpoint.scope.agent_snapshot_digest.as_deref()
                            != Some(&current_child.attempt.constraints_digest)
                    {
                        return Err(denied("restored child checkpoint ownership differs"));
                    }
                    // The suspension is physically verified by Runtime. Rebuild
                    // observability from durable events, not from a held future;
                    // transient trace failures from an old process are not replayed.
                    let events = execution
                        .server()
                        .coordinator()
                        .ledger()
                        .execution_events_after(&current_child.attempt.execution.execution_id, 0)
                        .map_err(denied)?;
                    let trajectory = kolyan_runtime::Trajectory {
                        turn_id: current_child.attempt.execution.turn_id.clone(),
                        execution_id: current_child.attempt.execution.execution_id.clone(),
                        records: events
                            .into_iter()
                            .map(|event| kolyan_runtime::TrajectoryRecord {
                                sequence: event.cursor,
                                kind: event.kind,
                                payload: event.payload,
                            })
                            .collect(),
                        trace_errors: Vec::new(),
                    };
                    return Ok(Err(AgentChildDriveResult::Waiting {
                        child: Box::new(current_child),
                        execution: Box::new(kolyan_runtime::DurableTurnResult::Suspended {
                            suspension: Box::new(suspension),
                            trajectory,
                        }),
                    }));
                }
                return Err(uncertain(
                    "existing nonterminal child needs explicit resume/recovery",
                ));
            }
            let (saved, reference) = runner
                .bindings
                .load_with_reference(
                    &current_owner.task_id,
                    &current_child.attempt.invocation_id,
                    &current_owner.logical_session_id,
                )
                .map_err(denied)?
                .ok_or_else(|| denied("child binding disappeared"))?;
            if reference != current_child.binding_fact {
                return Err(denied("child binding reference differs"));
            }
            let ceiling = runner
                .host
                .intersection(saved.snapshot.definition().permissions())
                .map_err(denied)?;
            saved
                .snapshot
                .permissions()
                .require_subset_of(&ceiling)
                .map_err(denied)?;
            let skill_binding = runner
                .restored_skills(&saved, &current_child.attempt.input_source)
                .map_err(denied)?;
            let provider = runner
                .routed_provider(
                    &saved.snapshot,
                    &current_child.attempt.execution,
                    skill_binding.as_ref(),
                )
                .map_err(denied)?;
            let set = runner
                .routed_tool_set(
                    &saved.snapshot,
                    &current_child.attempt.execution,
                    skill_binding.as_ref(),
                )
                .map_err(denied)?;
            let mut turn = input;
            turn.turn_id = current_child.attempt.execution.turn_id.clone();
            turn.model_request.request_id =
                format!("{}/request", current_child.attempt.execution.turn_id);
            turn.model_request.model = saved.snapshot.definition().model().clone();
            turn.model_request.system.push(SystemInstruction {
                text: saved.snapshot.definition().instructions().into(),
                cache: false,
            });
            turn.model_request.tools = set
                .definitions
                .into_iter()
                .filter(|definition| {
                    definition.name == crate::AGENT_INVOKE_NAME
                        || definition.name == crate::skills::SKILL_LOAD_NAME
                        || permits(&saved.snapshot, &definition.name)
                })
                .collect();
            match &turn.model_request.tool_choice {
                ToolChoice::Required if turn.model_request.tools.is_empty() => {
                    return Err(denied("required child tool has empty ceiling"));
                }
                ToolChoice::Tool(name)
                    if !turn
                        .model_request
                        .tools
                        .iter()
                        .any(|tool| &tool.name == name) =>
                {
                    return Err(denied("named child tool exceeds ceiling"));
                }
                _ => {}
            }
            let executor = TurnExecutor::with_tools(
                provider,
                RoutedTools {
                    skill: runner
                        .skill_executor(
                            skill_binding.as_ref(),
                            &current_child.attempt.execution,
                            set.policy.clone(),
                        )
                        .map_err(denied)?,
                    runner: runner.clone(),
                    saved: saved.clone(),
                    parent: current_child.attempt.clone(),
                    environment: SnapshotTools {
                        inner: set.executor,
                        snapshot: saved.snapshot.clone(),
                        execution: current_child.attempt.execution.clone(),
                        policy: set.policy.clone(),
                    },
                },
            )
            .with_tool_dispatch_policy(runner.tool_dispatch_policy())
            .with_policy_engine(set.policy)
            .with_agent_snapshot_digest(saved.snapshot.digest().into());
            Ok(Ok((turn, executor)))
        })
        .await
        .map_err(uncertain)??;
        let (turn, executor) = match prepared {
            Err(result) => return Ok(result),
            Ok(prepared) => prepared,
        };
        let permits = self
            .enter_execution(&owner.task_id, &owner.logical_session_id, &child.attempt)
            .await?;
        let stopped =
            Box::pin(
                self.service
                    .run(&owner.task_id, child.attempt.clone(), executor, turn),
            )
            .await;
        drop(permits);
        if let Ok((_, execution @ kolyan_runtime::DurableTurnResult::Suspended { .. })) = stopped {
            return Ok(AgentChildDriveResult::Waiting {
                child: Box::new(child),
                execution: Box::new(execution),
            });
        }
        let dispatch_error = stopped.err().map(|error| error.to_string());
        let service = self.service.clone();
        let task_id = owner.task_id.clone();
        let binding = child.attempt;
        let result = tokio::task::spawn_blocking(move || {
            // Publish a terminal from this already-admitted run, including late
            // observations after Task cancellation. This grants no new work.
            service.load_verified_historical_result(&task_id, &binding, 1024 * 1024)
        })
        .await
        .map_err(uncertain)?
        .map_err(|error| {
            uncertain(format!(
                "child terminal proof unavailable: {error}; dispatch={dispatch_error:?}"
            ))
        })?;
        Ok(AgentChildDriveResult::Terminal {
            result: Box::new(result),
            dispatch_error,
        })
    }
}
