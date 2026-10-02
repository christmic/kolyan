use super::super::*;
use crate::input_fixture::SourceFixtureAdmission;
use crate::{
    AgentIdentity, CancellationPolicy, ExecutionRef, InvocationDefinition, InvocationRole,
    TaskDefinition, TaskLimits,
};
use kolyan_core::{
    ExternalWait, IssuedToolAuthority, ToolError, ToolFuture, ToolInvocation, ToolOutcome,
    ToolPreparationFuture,
};
use kolyan_ledger::{
    FactDraft, FactSubject, InMemoryLedger, LedgerError, LedgerQuery, MemoryFactJournal,
};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelRef, ModelRequest, ModelResponse,
    ProviderFuture, StopReason, TokenUsage, ToolCall, ToolChoice, ToolResult,
};
use kolyan_policy::{
    ApprovalMode, Capability, Effect, Idempotency, InvocationClaim, PolicyEngine, PreparedCall,
    ResourceClaim, ToolManifest, ToolRequirements,
};
use kolyan_runtime::{ExternalVerificationFuture, ExternalWaitContext, ExternalWaitVerifier};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

#[derive(Clone, Default)]
pub(super) struct FaultLedger {
    pub inner: InMemoryLedger,
    pub failure: Arc<Mutex<Option<LedgerEventKind>>>,
}
impl FaultLedger {
    fn check_failure(&self, kind: &LedgerEventKind) -> Result<(), LedgerError> {
        let mut failure = self.failure.lock().unwrap();
        if failure.as_ref() == Some(kind) {
            *failure = None;
            return Err(LedgerError::Storage("injected publication crash".into()));
        }
        Ok(())
    }
}
impl LedgerStore for FaultLedger {
    fn query(&self, q: &LedgerQuery) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.query(q)
    }
    fn events_after(&self, c: u64) -> Result<Vec<LedgerEvent>, LedgerError> {
        self.inner.events_after(c)
    }
    fn claim(&self, k: &str) -> Result<bool, LedgerError> {
        self.inner.claim(k)
    }
    fn append(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.check_failure(&e.kind)?;
        self.inner.append(e)
    }
    fn append_unless_cancelled(&self, e: LedgerEvent) -> Result<LedgerEvent, LedgerError> {
        self.check_failure(&e.kind)?;
        self.inner.append_unless_cancelled(e)
    }
}

pub(super) fn agent() -> AgentIdentity {
    AgentIdentity {
        definition_id: "fixture".into(),
        revision: "r1".into(),
        instance_id: "instance".into(),
    }
}
pub(super) fn binding(inv: &str) -> AttemptBinding {
    AttemptBinding {
        input_source: crate::input_fixture::fixture_source(
            "task",
            inv,
            if (inv) == "root" {
                crate::InvocationInputKind::Standalone
            } else {
                crate::InvocationInputKind::Derived
            },
        ),
        attempt_id: format!("attempt-{inv}"),
        invocation_id: inv.into(),
        execution: ExecutionRef {
            session_id: format!("session-{inv}"),
            turn_id: format!("turn-{inv}"),
            execution_id: format!("execution-{inv}"),
        },
        agent: agent(),
        constraints_digest: "c".repeat(64),
    }
}
pub(super) fn invocation(id: &str, parent: Option<&str>) -> InvocationDefinition {
    InvocationDefinition {
        input_source: crate::input_fixture::fixture_source(
            "task",
            id,
            if (if parent.is_some() {
                InvocationRole::Delegation
            } else {
                InvocationRole::Root
            }) == crate::InvocationRole::Root
            {
                crate::InvocationInputKind::Standalone
            } else {
                crate::InvocationInputKind::Derived
            },
        ),
        invocation_id: id.into(),
        agent: agent(),
        constraints_digest: "c".repeat(64),
        role: if parent.is_some() {
            InvocationRole::Delegation
        } else {
            InvocationRole::Root
        },
        parent_invocation_id: parent.map(str::to_owned),
        dependencies: vec![],
    }
}
pub(super) fn coordinator() -> TaskCoordinator<MemoryFactJournal> {
    let value = TaskCoordinator::new(MemoryFactJournal::default());
    value
        .register_task(
            "task-register",
            TaskDefinition {
                task_id: "task".into(),
                objective: "verified parent result".into(),
                criteria: vec![CompletionCriterion::ExecutionCompleted {
                    id: "done".into(),
                    invocation_id: "root".into(),
                }],
                agent: agent(),
                constraints_digest: "c".repeat(64),
                limits: TaskLimits {
                    max_depth: 3,
                    max_invocations: 8,
                    max_attempts: 8,
                    max_tokens: None,
                    max_steps_per_turn: 3,
                },
                cancellation_policy: CancellationPolicy::AllInvocations,
            },
        )
        .unwrap();
    value
        .admit_fixture("task", "root-admit", invocation("root", None))
        .unwrap();
    value
}
pub(super) fn wait() -> ExternalWait {
    ExternalWait {
        wait_id: "children-wait".into(),
        kind: "fixture.children".into(),
        schema_version: 1,
        binding: json!({"admission":"fixture.admission","position":1}),
    }
}
pub(super) fn call() -> ToolCall {
    ToolCall {
        id: "delegate-call".into(),
        name: "fixture.delegate".into(),
        arguments: json!({"input":"explicit child input"}),
    }
}
pub(super) fn prepared() -> PreparedCall {
    PreparedCall::new(
        call(),
        "fixture-adapter-v1".into(),
        InvocationClaim {
            tool_name: call().name,
            capabilities: [Capability::AgentDelegate].into_iter().collect(),
            effects: [Effect::Delegate].into_iter().collect(),
            resource: ResourceClaim { path: None },
            idempotency: Idempotency::NonIdempotent,
        },
        ToolRequirements {
            process_sandbox: false,
            max_output_bytes: 65536,
            timeout_ms: 1000,
        },
    )
    .unwrap()
}
pub(super) fn policy() -> Arc<PolicyEngine> {
    let mut policy = PolicyEngine::default();
    policy.register(ToolManifest {
        tool_name: call().name,
        capabilities: [Capability::AgentDelegate].into_iter().collect(),
        effects: [Effect::Delegate].into_iter().collect(),
        path_scopes: vec![],
        idempotency: Idempotency::NonIdempotent,
        approval: ApprovalMode::Never,
    });
    Arc::new(policy)
}
pub(super) fn request(binding: &AttemptBinding) -> TurnRequest {
    TurnRequest {
        turn_id: binding.execution.turn_id.clone(),
        model_request: ModelRequest {
            request_id: "request".into(),
            model: ModelRef::new("fixture", "model"),
            system: vec![],
            messages: vec![],
            tools: vec![],
            tool_choice: ToolChoice::Auto,
            output_format: None,
            prompt_cache: None,
            reasoning: None,
            max_output_tokens: Some(64),
            extensions: serde_json::Value::Null,
        },
        config: kolyan_core::TurnConfig {
            max_steps: 3,
            ..Default::default()
        },
    }
}

pub(super) struct Provider {
    pub delegate: bool,
    pub calls: Arc<AtomicUsize>,
}
impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let index = self.calls.fetch_add(1, Ordering::SeqCst);
        let delegated = self.delegate && index == 0;
        let response = ModelResponse {
            id: format!("response-{index}"),
            model: request.model,
            content: if delegated {
                vec![ContentBlock::ToolCall { call: call() }]
            } else {
                vec![ContentBlock::Text {
                    text: "verified final response".into(),
                }]
            },
            structured_output: None,
            stop_reason: if delegated {
                StopReason::ToolUse
            } else {
                StopReason::EndTurn
            },
            usage: TokenUsage {
                input_tokens: Some(1),
                output_tokens: Some(1),
                ..Default::default()
            },
            metadata: serde_json::Value::Null,
        };
        Box::pin(async move {
            Ok(Box::pin(futures_util::stream::iter(vec![
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}
pub(super) struct Delegate {
    pub coordinator: TaskCoordinator<MemoryFactJournal>,
    pub calls: Arc<AtomicUsize>,
}
impl ToolExecutor for Delegate {
    fn prepare(&self, c: ToolCall) -> ToolPreparationFuture<'_> {
        Box::pin(async move {
            assert_eq!(c, call());
            Ok(prepared())
        })
    }
    fn execute_invocation(&self, inv: ToolInvocation) -> ToolFuture<'_> {
        Box::pin(async move {
            self.calls.fetch_add(1, Ordering::SeqCst);
            self.coordinator
                .admit_fixture("task", "child-admit", invocation("child", Some("root")))
                .unwrap();
            let issued = IssuedToolAuthority {
                prepared: inv.prepared,
                grant: inv.grant,
                scope: inv.scope,
                policy_revision: inv.policy_revision,
            };
            self.coordinator.journal().append("fixture.admission",0,vec![FactDraft {fact_id:"fixture.admission".into(),subject:FactSubject {kind:"fixture.children".into(),id:"fixture.admission".into()},kind:"fixture.children.admitted".into(),schema_version:1,critical:true,causes:vec![],payload:json!({"issued":issued,"wait":wait(),"parent":binding("root"),"child":binding("child")})}]).unwrap();
            Ok(ToolOutcome::AwaitingExternal(wait()))
        })
    }
}

#[derive(Clone)]
pub(super) struct Host {
    pub coordinator: TaskCoordinator<MemoryFactJournal>,
    pub ledger: FaultLedger,
}
impl Host {
    pub fn proof(&self) -> Result<VerifiedConsumedResult, ToolError> {
        result::load_consumed(
            &self.coordinator,
            &self.ledger,
            "task",
            &binding("root"),
            &binding("child"),
            65536,
        )
        .map_err(denied)?
        .ok_or_else(|| denied("missing committed consumption"))
    }
    pub fn result(&self) -> Result<ToolResult, ToolError> {
        let proof = self.proof()?;
        Ok(ToolResult {
            call_id: call().id,
            content: serde_json::to_string(&proof).map_err(denied)?,
            is_error: !matches!(proof.child.outcome, VerifiedTaskOutcome::Completed { .. }),
        })
    }
    fn admission(&self, context: &ExternalWaitContext) -> Result<(), ToolError> {
        let rows = self
            .coordinator
            .journal()
            .read("fixture.admission", 0, 2)
            .map_err(denied)?;
        if rows.len() != 1
            || rows[0].draft.kind != "fixture.children.admitted"
            || rows[0].draft.schema_version != 1
            || !rows[0].draft.critical
            || context.wait != wait()
            || rows[0].draft.payload
                != json!({"issued":context.issued,"wait":wait(),"parent":binding("root"),"child":binding("child")})
        {
            return Err(denied("unknown or foreign admission"));
        }
        let task = self.coordinator.snapshot("task").map_err(denied)?;
        if task.state.is_terminal() || task.invocations["root"].cancellation_requested {
            return Err(denied("cancelled root"));
        }
        context.validate(&context.issued.scope)
    }
}
impl ExternalWaitVerifier for Host {
    fn verify_wait(&self, context: ExternalWaitContext) -> ExternalVerificationFuture<'_> {
        Box::pin(async move { self.admission(&context) })
    }
    fn verify_result(
        &self,
        context: ExternalWaitContext,
        result: ToolResult,
    ) -> ExternalVerificationFuture<'_> {
        Box::pin(async move {
            self.admission(&context)?;
            if result != self.result()? {
                return Err(denied(
                    "result differs from exact physical/consumption proofs",
                ));
            }
            Ok(())
        })
    }
}
fn denied(value: impl std::fmt::Display) -> ToolError {
    ToolError::PolicyDenied {
        message: value.to_string(),
    }
}
