//! Audit the Runtime's exact prepared-effect protocol, not an authority issuer.

use kolyan_ledger::{LedgerEvent, LedgerEventKind};
use kolyan_policy::{PreparedCall, PreparedGrant, ToolExecutionScope};
use kolyan_runtime::{EffectGrant, EffectReceipt, EffectRequest, ReceiptStatus};
use serde_json::{Value, json};

pub(super) fn verify(
    events: &[LedgerEvent],
    receipt: &LedgerEvent,
    prepared: &PreparedCall,
    grant: &PreparedGrant,
    scope: &ToolExecutionScope,
) -> Result<(), String> {
    let effect = format!("{}/{}", scope.step_id, prepared.call().id);
    let prefix = format!("{}/effect/{effect}", scope.execution.execution_id);
    let find = |suffix: &str, kind| -> Result<&LedgerEvent, String> {
        let matching = events
            .iter()
            .filter(|e| {
                e.event_id == format!("{prefix}/{suffix}")
                    && e.idempotency_key == e.event_id
                    && e.execution_id == scope.execution.execution_id
                    && e.turn_id == scope.execution.turn_id
                    && e.kind == kind
            })
            .collect::<Vec<_>>();
        if matching.len() != 1 {
            return Err(format!("missing/nonunique exact effect {suffix}"));
        }
        Ok(matching[0])
    };
    let entered = find("prepared", LedgerEventKind::EffectPrepared)?;
    let authorized = find("authorized", LedgerEventKind::EffectAuthorized)?;
    let started = find("started", LedgerEventKind::EffectStarted)?;
    let recorded = find("receipt", LedgerEventKind::EffectReceipt)?;
    let completed = find("completed", LedgerEventKind::EffectCompleted)?;
    if recorded != receipt {
        return Err("receipt is not the exact admitted effect event".into());
    }
    let request: EffectRequest =
        serde_json::from_value(entered.payload.clone()).map_err(|e| e.to_string())?;
    grant
        .validate(prepared, &request.policy_revision, scope)
        .map_err(|e| e.to_string())?;
    let input_digest = digest(
        "kolyan.runtime.prepared-input.v1",
        json!({"prepared_digest":prepared.digest(),"scope":scope}),
    )?;
    let expected_request = EffectRequest {
        effect_id: effect.clone(),
        operation_kind: prepared.call().name.clone(),
        input_digest: input_digest.clone(),
        requirements: vec![
            serde_json::to_string(&canonical(json!(prepared.requirements())))
                .map_err(|e| e.to_string())?,
        ],
        policy_revision: request.policy_revision.clone(),
    };
    let mut prepared_payload = json!(expected_request);
    prepared_payload["binding_kind"] = json!("prepared_tool_v1");
    prepared_payload["input"] = json!({"prepared":prepared,"scope":scope});
    let authorization = EffectGrant {
        authorization_id: format!("{prefix}/authorized"),
        effect_id: effect.clone(),
        input_digest: input_digest.clone(),
        constraints_digest: digest("kolyan.runtime.prepared-grant.v1", json!(grant))?,
        authority_revision: request.policy_revision,
    };
    let mut authorized_payload = json!(authorization);
    authorized_payload["prepared_grant"] = json!(grant);
    let expected_receipt = EffectReceipt {
        receipt_id: format!("{prefix}/receipt"),
        effect_id: effect.clone(),
        authorization_id: authorization.authorization_id.clone(),
        input_digest: input_digest.clone(),
        executor_id: format!("tool/{}", prepared.call().name),
        executor_revision: prepared.tool_revision().into(),
        result_digest: digest(
            "kolyan.runtime.prepared-result.v1",
            json!({"output":receipt.payload["output"]}),
        )?,
        status: ReceiptStatus::Completed,
    };
    let receipt_payload = json!({"effect_id":effect,"input":{"prepared":prepared,"scope":scope},"authorization":authorization,"prepared_grant":grant,"receipt":expected_receipt,"output":receipt.payload["output"]});
    let model = events
        .iter()
        .find(|e| {
            e.kind == LedgerEventKind::StepCompleted
                && e.execution_id == scope.execution.execution_id
                && e.turn_id == scope.execution.turn_id
                && super::super::super::driver::exact_step_call(
                    &e.payload["step"],
                    &scope.step_id,
                    prepared.call(),
                )
        })
        .ok_or("exact model call/step missing")?;
    if entered.payload != prepared_payload
        || authorized.payload != authorized_payload
        || started.payload != json!({"effect_id":effect,"input_digest":input_digest,"scope":scope})
        || receipt.payload != receipt_payload
        || completed.payload != receipt.payload
        || !(model.cursor < entered.cursor
            && entered.cursor < authorized.cursor
            && authorized.cursor < started.cursor
            && started.cursor < receipt.cursor
            && receipt.cursor < completed.cursor)
    {
        return Err("prepared effect input/authority/result/causal order differs".into());
    }
    Ok(())
}

fn digest(domain: &str, value: Value) -> Result<String, String> {
    let bytes =
        serde_json::to_vec(&canonical(json!([domain, value]))).map_err(|e| e.to_string())?;
    Ok(super::super::baseline::digest(&bytes))
}

fn canonical(value: Value) -> Value {
    match value {
        Value::Object(entries) => Value::Object(
            entries
                .into_iter()
                .collect::<std::collections::BTreeMap<_, _>>()
                .into_iter()
                .map(|(k, v)| (k, canonical(v)))
                .collect(),
        ),
        Value::Array(values) => Value::Array(values.into_iter().map(canonical).collect()),
        value => value,
    }
}
