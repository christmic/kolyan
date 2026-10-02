//! Manually constructed normative trajectories, not a production writer substitute.

use kolyan_ledger::{FactDraft, LedgerEvent, LedgerEventKind as Kind};
use serde::Deserialize;
use serde_json::{Value, json};

use super::super::*;

#[derive(Debug, Deserialize)]
pub(super) struct Mutation {
    target: String,
    pointer: String,
    value: Option<Value>,
    #[serde(default)]
    remove: bool,
}

#[derive(Debug, Deserialize)]
pub(super) struct Case {
    pub id: String,
    pub expected: String,
    #[serde(default)]
    pub mutations: Vec<Mutation>,
    #[serde(default)]
    pub drop: Vec<String>,
    #[serde(default)]
    pub mode: String,
    #[serde(default)]
    pub padding: usize,
}

pub(super) struct Fixture {
    pub request: ModelOpeningInspectionRequest,
    pub events: Vec<LedgerEvent>,
    pub drafts: Vec<FactDraft>,
}

pub(super) fn fixture(case: &Case) -> Fixture {
    let base: Value = serde_json::from_str(include_str!("base.json")).unwrap();
    let requested = json!({"event_id":"execution/request","cursor":4 + case.padding});
    let opening_ref =
        json!({"event_id":"execution/model-opening/turn-step-0","cursor":5 + case.padding});
    let model_request: kolyan_model::ModelRequest =
        serde_json::from_value(base["request"].clone()).unwrap();
    let digest = kolyan_model::digest_json(&model_request).unwrap();
    let mut values = json!({
        "identity": base["execution"],
        "admission": {"schema_version":1,"opening_protocol":1,"key":base["execution"],
            "model_request":base["request"],"max_steps":2,"max_tool_calls":null,"deadline_at_ms":null,
            "tool_timeout_ms":null,"dispatch":{"mode":"Serial","on_error":"FailTurn"},"agent_snapshot_digest":null},
        "start":{"step_id":"turn-step-0"},
        "request":{"request":base["request"]},
        "opening":{"schema_version":1,"opening_protocol":1,"execution":base["execution"],
            "step_id":"turn-step-0","model_requested":requested,
            "preparation":{"stream_id":"opening-fixtures","position":1,"fact_id":"prepared"},
            "neutral_digest":digest,"generation_wire_digest":base["generation_wire_digest"],
            "generation_wire_bytes":base["generation_wire_bytes"],"count_profile_digest":base["count_profile_digest"],
            "deadline_at_ms":null},
        "fact":{"fact_id":"prepared","subject":{"kind":"runtime.execution","id":"execution"},
            "kind":"model.context_prepared","schema_version":1,"critical":true,"causes":[],
            "payload":{"opening_protocol":1,"execution":base["execution"],"step_id":"turn-step-0",
                "model_requested":requested,"neutral_digest":digest,"mapping_identity":base["mapping_identity"],
                "count_profile_digest":base["count_profile_digest"],"generation_wire_digest":base["generation_wire_digest"],
                "generation_wire_bytes":base["generation_wire_bytes"],"count_input_digest":base["count_input_digest"],
                "unsupported_count_fields":[],"accounting":base["accounting"]}},
        "completed":{"step_id":"turn-step-0","outcome":"FinalAnswer",
            "step":{"step_id":"turn-step-0","response":base["response"],"outcome":"FinalAnswer"},
            "opening_protocol":1,"model_requested":requested,"model_opening":opening_ref},
        "terminal":{"reason":"fixture stop"},
        "through":{"event_id":"execution/terminal","cursor":7 + case.padding},
        "limits":{"max_events":2048,"max_total_bytes":16777216,"max_payload_bytes":1048576}
    });
    for mutation in &case.mutations {
        let root = if mutation.target == "prepared" {
            &mut values["fact"]["payload"]
        } else {
            &mut values[&mutation.target]
        };
        mutate(root, mutation);
    }
    let mut rows = vec![
        (
            "identity",
            "execution/execution-started",
            Kind::ExecutionStarted,
        ),
        (
            "admission",
            "execution/input-admitted",
            Kind::ExecutionInputAdmitted,
        ),
        ("start", "execution/step", Kind::StepStarted),
        ("request", "execution/request", Kind::ModelRequested),
        (
            "opening",
            "execution/model-opening/turn-step-0",
            Kind::ModelOpeningAdmitted,
        ),
        ("completed", "execution/complete", Kind::StepCompleted),
        (
            "terminal",
            "execution/terminal",
            match case.mode.as_str() {
                "timeout" => Kind::TurnTimedOut,
                "cancelled" => Kind::TurnCancelled,
                _ => Kind::TurnFailed,
            },
        ),
    ];
    if case.mode == "opening_after_terminal" {
        rows.swap(4, 6);
    }
    if case.mode == "terminal_before_admission" {
        rows.swap(1, 6);
    }
    if case.mode == "duplicate_request" {
        rows.insert(
            4,
            ("request", "execution/request-copy", Kind::ModelRequested),
        );
    }
    if case.mode == "duplicate_opening" {
        rows.insert(
            5,
            (
                "opening",
                "execution/opening-copy",
                Kind::ModelOpeningAdmitted,
            ),
        );
    }
    if case.mode == "cancel_before_opening" {
        rows.insert(
            4,
            (
                "terminal",
                "execution/execution-cancelled",
                Kind::ExecutionCancelled,
            ),
        );
    }
    if case.mode == "stream_without_opening" {
        rows.insert(4, ("start", "execution/stream", Kind::ModelStreamEvent));
    }
    let mut events = Vec::new();
    let mut drafts = Vec::new();
    if !case.drop.iter().any(|drop| drop == "fact") {
        drafts.push(serde_json::from_value(values["fact"].clone()).unwrap());
    }
    for (target, id, kind) in rows {
        if case.drop.iter().any(|drop| drop == target) {
            continue;
        }
        if target == "start" {
            for index in 0..case.padding {
                let mut padding = event(
                    &format!("execution/padding-{index}"),
                    Kind::ToolCallRequested,
                    json!({"fixture_padding":index}),
                );
                if case.mode == "foreign_padding" {
                    padding.execution_id = "other-execution".into();
                    padding.turn_id = "other-turn".into();
                }
                events.push(padding);
            }
        }
        events.push(event(id, kind, values[target].clone()));
        if target == "completed"
            && matches!(
                case.mode.as_str(),
                "two_steps" | "second_reuses_request" | "step_after_final"
            )
        {
            second_step(&mut events, &mut drafts, &values, &case.mode);
        }
        if target == "opening" && case.mode == "tool_not_committed" {
            events.push(event(
                "execution/effect/manual/started",
                Kind::EffectStarted,
                json!({"effect_id":"manual"}),
            ));
            events.push(event("execution/effect/manual/reconciled",Kind::EffectReconciled,
                json!({"effect_id":"manual","resolution":{"NotCommitted":{"evidence":"manual protocol fixture, not tool proof validation"}}})));
        }
    }
    if case.mode == "empty" {
        events.clear();
    }
    let mut through: ModelOpeningEventRef =
        serde_json::from_value(values["through"].clone()).unwrap();
    // Drop/reorder cases inspect their complete actually stored prefix, unless a
    // case explicitly corrupts the through coordinate.
    if !case
        .mutations
        .iter()
        .any(|mutation| mutation.target == "through")
    {
        through.cursor = events.len() as u64;
        if let Some(last) = events.last() {
            through.event_id = last.event_id.clone();
        }
    }
    Fixture {
        request: ModelOpeningInspectionRequest {
            execution: serde_json::from_value(base["execution"].clone()).unwrap(),
            through,
            limits: ModelOpeningInspectionLimits {
                max_events: values["limits"]["max_events"].as_u64().unwrap() as usize,
                max_total_bytes: values["limits"]["max_total_bytes"].as_u64().unwrap() as usize,
                max_payload_bytes: values["limits"]["max_payload_bytes"].as_u64().unwrap() as usize,
            },
        },
        events,
        drafts,
    }
}

fn second_step(
    events: &mut Vec<LedgerEvent>,
    drafts: &mut Vec<FactDraft>,
    values: &Value,
    mode: &str,
) {
    let step_id = "turn-step-1";
    let request_ref = json!({"event_id":"execution/request-1","cursor":events.len() + 2});
    let opening_ref =
        json!({"event_id":"execution/model-opening/turn-step-1","cursor":events.len() + 3});
    let mut request = values["request"]["request"].clone();
    request["request_id"] = json!(step_id);
    request["messages"].as_array_mut().unwrap().push(
        json!({"role":"assistant","content":values["completed"]["step"]["response"]["content"]}),
    );
    let typed: kolyan_model::ModelRequest = serde_json::from_value(request.clone()).unwrap();
    let digest = kolyan_model::digest_json(&typed).unwrap();
    let mut draft = values["fact"].clone();
    draft["fact_id"] = json!("prepared-step-1");
    draft["payload"]["step_id"] = json!(step_id);
    draft["payload"]["model_requested"] = request_ref.clone();
    draft["payload"]["neutral_digest"] = json!(digest);
    drafts.push(serde_json::from_value(draft).unwrap());
    let mut opening = values["opening"].clone();
    opening["step_id"] = json!(step_id);
    opening["neutral_digest"] = json!(digest);
    if mode != "second_reuses_request" {
        opening["model_requested"] = request_ref.clone();
    }
    opening["preparation"]["fact_id"] = json!("prepared-step-1");
    opening["preparation"]["position"] = json!(2);
    let mut completed = values["completed"].clone();
    completed["step_id"] = json!(step_id);
    completed["step"]["step_id"] = json!(step_id);
    completed["outcome"] = json!("FinalAnswer");
    completed["step"]["outcome"] = json!("FinalAnswer");
    completed["step"]["response"]["stop_reason"] = json!({"reason":"end_turn"});
    completed["model_requested"] = request_ref;
    completed["model_opening"] = opening_ref;
    events.extend([
        event(
            "execution/step-1",
            Kind::StepStarted,
            json!({"step_id":step_id}),
        ),
        event(
            "execution/request-1",
            Kind::ModelRequested,
            json!({"request":request}),
        ),
        event(
            "execution/model-opening/turn-step-1",
            Kind::ModelOpeningAdmitted,
            opening,
        ),
        event("execution/complete-1", Kind::StepCompleted, completed),
    ]);
}

fn event(id: &str, kind: Kind, payload: Value) -> LedgerEvent {
    LedgerEvent {
        event_id: id.into(),
        idempotency_key: id.into(),
        cursor: 0,
        execution_id: "execution".into(),
        turn_id: "turn".into(),
        kind,
        payload,
    }
}

fn mutate(root: &mut Value, mutation: &Mutation) {
    let (parent, key) = mutation.pointer.rsplit_once('/').unwrap();
    let target = if parent.is_empty() {
        root
    } else {
        root.pointer_mut(parent).unwrap()
    };
    if mutation.remove {
        target.as_object_mut().unwrap().remove(key);
    } else {
        target[key] = mutation.value.clone().unwrap();
    }
}
