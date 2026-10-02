//! Record complete observations before the parent compares any case.

use std::sync::{Arc, Mutex};

use kolyan_core::ToolErrorPolicy;
use kolyan_ledger::{FactJournal, LedgerStore};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::super::super::*;
use super::super::support::{Harness, execution};
use super::{Case, adapters::Factories};

pub(super) async fn run(case: &Case) -> Value {
    run_configured(case, false).await
}

pub(super) async fn run_delegation(case: &Case) -> Value {
    run_configured(case, true).await
}

async fn run_configured(case: &Case, self_delegation: bool) -> Value {
    let harness = Harness::new();
    let mut input = harness.request(&case.id, false);
    let mut host = harness.runner.host.clone();
    if self_delegation {
        host.delegation.allow_self = true;
        input.requested_permissions = host.clone();
        let AgentSelector::Inline(definition) = &input.selector else {
            unreachable!("inline Harness request")
        };
        input.selector = AgentSelector::Inline(
            crate::AgentDefinition::new(crate::AgentDefinitionInput {
                definition_id: definition.key().definition_id,
                revision: definition.key().revision,
                display_name: definition.display_name().map(str::to_owned),
                model: definition.model().clone(),
                instructions: definition.instructions().into(),
                permissions: host.clone(),
            })
            .unwrap(),
        );
    }
    input.limits.max_tokens = None;
    input.limits.max_steps_per_turn = u32::try_from(case.max_steps).unwrap();
    input.turn.config.max_steps = case.max_steps;
    input.turn.config.max_tool_calls = Some(case.max_tool_calls);
    input.turn.model_request.max_output_tokens = Some(50);
    let factory = Factories {
        case: case.clone(),
        observations: harness.observations.clone(),
        dispatched: Arc::new(Mutex::new(Vec::new())),
    };
    let make_runner = |continued| {
        let runner = AgentRunner::new(
            harness.service.clone(),
            harness.runner.instances.clone(),
            harness.bindings.clone(),
            AgentCatalog::new(8).unwrap(),
            host.clone(),
            (factory.clone(), factory.clone()),
            harness.runner.input_artifacts.clone(),
        )
        .unwrap();
        let runner = if self_delegation {
            runner
                .with_delegation(AgentDelegationConfig {
                    limits: crate::InvokePrepareLimits {
                        max_children: 1,
                        max_parallel: 1,
                        max_child_input_bytes: 4096,
                        max_output_bytes: 65536,
                        admission_timeout_ms: 1000,
                    },
                    approval: kolyan_policy::ApprovalMode::Never,
                })
                .unwrap()
        } else {
            runner
        };
        Arc::new(if continued {
            runner.with_tool_error_policy(ToolErrorPolicy::ContinueBatch)
        } else {
            runner
        })
    };
    let runner = make_runner(case.continue_batch);
    let mut result = runner.start(input).await;
    let mut saved = Value::Null;
    let mut effects_before = Value::Null;
    let mut requests_before = Value::Null;
    let mut resume_error = Value::Null;
    let mut restored_policy = Value::Null;
    if case.approval
        && let Ok(started) = &result
    {
        if let DurableTurnResult::Suspended { suspension, .. } = &started.execution {
            saved = json!(suspension);
            effects_before = json!(harness.observations.effects.lock().unwrap().len());
            requests_before = json!(harness.observations.requests.lock().unwrap().len());
            let approval = suspension
                .waiting
                .approvals
                .first()
                .map(|a| a.approval_id.clone());
            if let Some(approval_id) = approval {
                let restored = make_runner(false);
                restored_policy = json!(restored.tool_error_policy);
                result = restored
                    .resume_approval(RootApprovalResumeRequest {
                        task_id: case.id.clone(),
                        invocation_id: "root".into(),
                        logical_session_id: "session".into(),
                        attempt_id: "attempt".into(),
                        approval_id,
                    })
                    .await;
            } else {
                resume_error = json!("no approval in saved suspension");
            }
        } else {
            resume_error = json!("expected approval suspension absent");
        }
    }
    let outcome = result.as_ref().ok().map(|r| match &r.execution {
        DurableTurnResult::Completed(done, _) => format!("{:?}", done.result.outcome),
        DurableTurnResult::Suspended { .. } => "Suspended".into(),
    });
    let ledger = harness
        .service
        .sessions()
        .execution()
        .server()
        .coordinator()
        .ledger()
        .execution_events_after(&execution(&case.id).execution_id, 0)
        .unwrap();
    let task = harness.service.coordinator().snapshot(&case.id);
    let journal = harness
        .service
        .coordinator()
        .journal()
        .read(&case.id, 0, 1024)
        .unwrap();
    let instance_stream = format!("agent-instances:{:x}", Sha256::digest(b"unit-host"));
    let instances = harness
        .service
        .coordinator()
        .journal()
        .read(&instance_stream, 0, 1024)
        .unwrap();
    let session = harness.service.sessions().sessions().load("session");
    let context_records: Vec<_> = harness.observations.records.lock().unwrap().iter().map(|r| match r {
        crate::provider::ContextRecord::Prepared {source,prepared} => json!({"kind":"prepared","source":source,"prepared":prepared}),
        crate::provider::ContextRecord::Rejected {source,failure,preparation} => json!({"kind":"rejected","source":source,"failure":failure.to_string(),"preparation":preparation}),
    }).collect();
    json!({"case":case.id,"host_permissions":host,"model_source":"synthetic_module_provider_not_actual_llm","tool_source":"synthetic_fault_wrapper_existing_unit_adapter","requests":*harness.observations.requests.lock().unwrap(),"context_records":context_records,"effects":*harness.observations.effects.lock().unwrap(),"dispatched_grants":*factory.dispatched.lock().unwrap(),"ledger":ledger,"journal":journal,"instance_facts":instances,"session":session.as_ref().map_err(ToString::to_string),"task":task.as_ref().ok(),"task_error":task.as_ref().err().map(ToString::to_string),"error":result.as_ref().err().map(ToString::to_string),"error_debug":result.as_ref().err().map(|e|format!("{e:?}")),"outcome":outcome,"saved_suspension":saved,"restored_host_policy":restored_policy,"effects_before_resume":effects_before,"requests_before_resume":requests_before,"resume_observation_error":resume_error})
}
