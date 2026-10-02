//! One bounded hook dispatch. No model/tool loop, grants, parameter rewriting or retries.
//! Drop may leave a durable intent without completion; rebuilding never reruns it.

use std::time::{Duration, Instant};

use serde::Serialize;

use kolyan_core::TurnControl;
use kolyan_ledger::{FactDraft, FactRef, FactSubject};
use kolyan_trace::Retention;

use super::{
    HookAccessPolicy, HookCatalog, HookDecision, HookError, HookEvent, HookReply, NativeHookHost,
    VerifiedHookBinding, encode, hash, id, native::NativeOutcome, reference,
};

/// Supplied by the actual host binding port, never inferred from ModelRequest.
/// The monotonic window is anchored at construction and cannot renew in dispatch.
pub struct HookExecutionWindow {
    deadline: Instant,
    control: TurnControl,
}
impl HookExecutionWindow {
    pub fn new(control: TurnControl, remaining: Duration) -> Result<Self, HookError> {
        if remaining.is_zero() {
            return Err(HookError::Expired);
        }
        let deadline = Instant::now()
            .checked_add(remaining.min(Duration::from_secs(10)))
            .ok_or(HookError::Capacity)?;
        Ok(Self { deadline, control })
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "decision", rename_all = "snake_case")]
pub enum HookDispatchResult {
    Continued {
        completed: Vec<FactRef>,
    },
    Denied {
        reason: String,
        completed: Vec<FactRef>,
    },
}

#[derive(Clone)]
pub struct HookRuntime {
    catalog: HookCatalog,
    policy: HookAccessPolicy,
    native: NativeHookHost,
}
impl HookRuntime {
    pub fn new(catalog: HookCatalog, policy: HookAccessPolicy, native: NativeHookHost) -> Self {
        Self {
            catalog,
            policy,
            native,
        }
    }

    /// Requires a Tokio runtime with timers enabled, supplied by the host.
    /// Dispatch once under an explicit operation identity. All hook failures are
    /// host errors, separate from a valid typed Denied decision. Persisted intent
    /// cannot be retried after cancellation/drop or host reconstruction.
    pub async fn dispatch(
        &self,
        binding: &VerifiedHookBinding,
        event: HookEvent,
        operation_id: String,
        window: HookExecutionWindow,
    ) -> Result<HookDispatchResult, HookError> {
        id(&operation_id)?;
        tokio::runtime::Handle::try_current().map_err(|_| {
            HookError::Invalid("hook dispatch requires a Tokio timer runtime".into())
        })?;
        event.validate()?;
        if &event.scope != binding.scope() {
            return Err(HookError::Integrity("event differs from binding".into()));
        }
        if window.control.is_cancelled() {
            return Err(HookError::Cancelled);
        }
        let deadline = tokio::time::Instant::from_std(window.deadline);
        let execution = self.run(binding.clone(), event, operation_id, &window);
        tokio::time::timeout_at(deadline, execution)
            .await
            .map_err(|_| HookError::Expired)?
    }

    async fn run(
        &self,
        binding: VerifiedHookBinding,
        event: HookEvent,
        operation_id: String,
        window: &HookExecutionWindow,
    ) -> Result<HookDispatchResult, HookError> {
        let run = hash(&("kolyan.hooks.run.v1", binding.reference(), &operation_id))?;
        let stream = format!("agent.hooks.run.{run}");
        let runtime = self.clone();
        let saved = binding.clone();
        let stream_check = stream.clone();
        blocking(move || {
            runtime
                .catalog
                .validate_current(&saved, &runtime.policy, runtime.native.digest())?;
            let existing = runtime.catalog.journal.read(&stream_check, 0, 18)?;
            if !existing.is_empty() {
                return Err(
                    if existing
                        .last()
                        .is_some_and(|r| r.draft.kind == "agent.hook.started")
                    {
                        HookError::Interrupted
                    } else {
                        HookError::Conflict
                    },
                );
            }
            Ok(())
        })
        .await?;
        let hooks = binding
            .saved
            .hooks
            .iter()
            .filter(|h| h.manifest.matches(event.phase(), event.tool_name()));
        let mut completed = Vec::new();
        let mut position = 0;
        for hook in hooks {
            if window.control.is_cancelled() {
                return Err(HookError::Cancelled);
            }
            let runtime = self.clone();
            let binding = binding.clone();
            let hook = hook.clone();
            let event_prepare = event.clone();
            let stream_prepare = stream.clone();
            let run_id = format!("{run}.{}", completed.len());
            let deadline = window.deadline;
            let (installed, started) = blocking(move || {
                runtime.catalog.validate_current(&binding, &runtime.policy, runtime.native.digest())?;
                let input = encode(&event_prepare)?;
                if input.len() > hook.manifest.max_input_bytes { return Err(HookError::Capacity); }
                let body = runtime.catalog.body(&hook)?;
                let remaining = deadline.checked_duration_since(Instant::now()).ok_or(HookError::Expired)?;
                let installed = runtime.native.prepare(&hook, &body, input.clone(), remaining.min(Duration::from_millis(hook.manifest.timeout_ms)), &run_id)?;
                let input = runtime.catalog.artifacts.put(&input, Retention::Required)?;
                let rows = runtime.catalog.journal.append(&stream_prepare, position, vec![FactDraft {
                    fact_id: format!("agent.hook.started.{run_id}"), subject: FactSubject { kind: "agent.hook.run".into(), id: run_id },
                    kind: "agent.hook.started".into(), schema_version: 1, critical: true,
                    causes: vec![binding.reference.clone(), hook.reference.clone()],
                    payload: serde_json::json!({"binding":binding.reference,"registration":hook.reference,"input":input,"native_digest":runtime.native.digest()}),
                }])?;
                Ok((installed, reference(&rows[0])))
            }).await?;
            position += 1;
            if window.control.is_cancelled() {
                return Err(HookError::Cancelled);
            }
            let output = installed.execute(&window.control).await;
            let reply = match &output {
                NativeOutcome::Exited {
                    exit_code: Some(0),
                    stdout,
                    ..
                } => HookReply::parse(stdout, event.phase()),
                NativeOutcome::Exited { exit_code, .. } => {
                    Err(HookError::Native(format!("script exit {exit_code:?}")))
                }
                NativeOutcome::Failed {
                    kind: "cancelled", ..
                } => Err(HookError::Cancelled),
                NativeOutcome::Failed {
                    kind: "timeout", ..
                } => Err(HookError::Expired),
                NativeOutcome::Failed { message, .. } => Err(HookError::Native(message.clone())),
            };
            let runtime = self.clone();
            let stream_commit = stream.clone();
            let reason = reply.as_ref().err().map(ToString::to_string);
            let success = reply.as_ref().ok().cloned();
            let denial = success
                .as_ref()
                .filter(|r| r.decision == HookDecision::Deny)
                .map(|r| r.reason.clone());
            let fact = blocking(move || {
                // Byte vectors in JSON can be larger than the raw output ceiling.
                let bytes = serde_json::to_vec(&output).map_err(|e| HookError::Protocol(e.to_string()))?;
                let artifact = runtime.catalog.artifacts.put(&bytes, Retention::Required)?;
                let rows = runtime.catalog.journal.append(&stream_commit, position, vec![FactDraft {
                    fact_id: format!("{}.completed", started.fact_id), subject: FactSubject { kind: "agent.hook.run".into(), id: started.fact_id.clone() },
                    kind: "agent.hook.completed".into(), schema_version: 1, critical: true, causes: vec![started],
                    payload: serde_json::json!({"output":artifact,"reply":success,"host_failure":reason}),
                }])?;
                Ok(reference(&rows[0]))
            }).await?;
            position += 1;
            completed.push(fact);
            reply?;
            if let Some(reason) = denial {
                return Ok(HookDispatchResult::Denied { reason, completed });
            }
        }
        Ok(HookDispatchResult::Continued { completed })
    }
}

async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T, HookError> + Send + 'static,
) -> Result<T, HookError> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| HookError::Native(format!("host worker failed: {e}")))?
}
