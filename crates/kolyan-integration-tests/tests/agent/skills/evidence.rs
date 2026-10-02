//! Diagnostic consistency checks, never authorization or a receipt/Goal substitute.
//! The Host matrix obtains receipts, binding/Goal facts and native files; pure
//! diagnostic inputs below are explicitly not execution evidence.
use super::data::{Dataset, Mutation};
use kolyan_ledger::FactRef;
use kolyan_model::{ContentBlock, Message, MessageRole, ModelRequest, ToolResult};
use kolyan_trace::{ArtifactRef, Retention};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::io::{self, Write};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct BodyObservation {
    pub key: kolyan_agent::SkillKey,
    pub content_digest: String,
    pub registration: FactRef,
    pub binding: FactRef,
    pub artifact: ArtifactRef,
    pub body: String,
}
pub(super) fn check_next_result(
    messages: &[Message],
    result: &ToolResult,
    expected: &BodyObservation,
) -> Result<(), String> {
    serde_json::to_writer(&mut BoundedCount(0), result).map_err(|e| e.to_string())?;
    if result.is_error || result.content.len() > 512 * 1024 {
        return Err("failed or oversized load result".into());
    }
    let value: serde_json::Value =
        serde_json::from_str(&result.content).map_err(|e| e.to_string())?;
    let observed: BodyObservation =
        serde_json::from_value(value.clone()).map_err(|e| e.to_string())?;
    if serde_json::to_value(&observed).map_err(|e| e.to_string())? != value {
        return Err("unknown nested output field".into());
    }
    if &observed != expected {
        return Err("load differs from selected registration/binding/body".into());
    }
    if observed.body.len() > 32768
        || observed.artifact.retention != Retention::Required
        || observed.artifact.byte_length != observed.body.len() as u64
        || observed.artifact.digest != format!("{:x}", Sha256::digest(observed.body.as_bytes()))
    {
        return Err("load body artifact consistency".into());
    }
    if !messages.iter().any(|m| {
        m.role == MessageRole::User
            && m.content
                .iter()
                .any(|b| matches!(b,ContentBlock::ToolResult{result:actual} if actual==result))
    }) {
        return Err("next model input lacks exact original ToolResult".into());
    }
    Ok(())
}

// Account the complete outer JSON and escaping without allocating a second envelope.
struct BoundedCount(usize);
/// Compare only physically re-read observations, including all dataset expectations.
pub(super) fn compare(dataset: &Dataset, row: &Value) -> Result<(), String> {
    let check = |ok: bool, message: &str| {
        if ok {
            Ok(())
        } else {
            Err(format!(
                "{} {:?}: {message}",
                row["case_id"], row["protocol"]
            ))
        }
    };
    check(
        row["error"].is_null()
            && row["ledger_error"].is_null()
            && row["facts_error"].is_null()
            && row["artifact_error"].is_null(),
        &format!(
            "run failure {} {} {}",
            row["error"], row["ledger_error"], row["facts_error"]
        ),
    )?;
    let id = row["case_id"].as_str().ok_or("case missing")?;
    let case = dataset
        .cases
        .iter()
        .find(|c| c.id == id)
        .ok_or("unknown case")?;
    let events = row["ledger"].as_array().ok_or("ledger missing")?;
    let facts = row["facts"].as_array().ok_or("facts missing")?;
    let instances: Vec<_> = facts
        .iter()
        .filter(|f| f["draft"]["kind"] == "agent.instance_reserved")
        .collect();
    check(
        instances.len() == 1
            && instances[0]["draft"]["payload"]["owner"]["invocation_id"] == "root",
        "unexpected or orphan child instance reservation",
    )?;
    let requests: Vec<ModelRequest> = events
        .iter()
        .filter(|e| e["kind"] == "model_requested")
        .map(|e| serde_json::from_value(e["payload"]["request"].clone()).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    check(!requests.is_empty(), "no real model request")?;
    let initial = serde_json::to_string(&requests[0]).map_err(|e| e.to_string())?;
    let body = dataset.selected()?.body.clone();
    check(
        !initial.contains(&dataset.artifact.source_only_marker)
            && !initial.contains(&serde_json::to_string(&body).map_err(|e| e.to_string())?),
        "body/payload leaked before load",
    )?;
    let skill_inventory: Vec<_> = requests[0]
        .tools
        .iter()
        .filter(|t| t.name == "skill.load")
        .collect();
    check(skill_inventory.len() == 1, "exact skill inventory missing")?;
    check(
        !serde_json::to_string(&skill_inventory)
            .map_err(|e| e.to_string())?
            .contains(&dataset.artifact.source_only_marker),
        "tool metadata leaks payload",
    )?;
    if row["scripted_localhost"] == true {
        let http = row["http"].as_array().ok_or("actual HTTP missing")?;
        check(!http.is_empty(), "no actual SDK request")?;
        let initial_wire = serde_json::to_string(&http[0]["body"]).map_err(|e| e.to_string())?;
        check(
            !initial_wire.contains(&dataset.artifact.source_only_marker),
            "actual initial HTTP leaks knowledge payload",
        )?;
        check(
            http.iter().all(|r| {
                r["request_line"]
                    .as_str()
                    .is_some_and(|l| l.contains("/responses ") || l.contains("/messages "))
            }),
            "unexpected count/other endpoint",
        )?;
    }
    let receipts: Vec<_> = events
        .iter()
        .filter(|e| e["kind"] == "effect_receipt")
        .collect();
    let count = |name: &str| {
        receipts
            .iter()
            .filter(|e| e["payload"]["input"]["prepared"]["call"]["name"] == name)
            .count()
    };
    // Every actual receipt must carry a valid prepared grant, exact result and its
    // corresponding entered effect; forbidden calls may only be error feedback.
    for receipt in &receipts {
        let p = &receipt["payload"];
        let prepared: kolyan_policy::PreparedCall =
            serde_json::from_value(p["input"]["prepared"].clone()).map_err(|e| e.to_string())?;
        let scope: kolyan_policy::ToolExecutionScope =
            serde_json::from_value(p["input"]["scope"].clone()).map_err(|e| e.to_string())?;
        let grant: kolyan_policy::PreparedGrant =
            serde_json::from_value(p["prepared_grant"].clone()).map_err(|e| e.to_string())?;
        grant
            .validate(
                &prepared,
                p["authorization"]["authority_revision"]
                    .as_str()
                    .ok_or("revision missing")?,
                &scope,
            )
            .map_err(|e| e.to_string())?;
        check(
            events.iter().any(|e| {
                e["kind"] == "effect_started"
                    && e["payload"]["effect_id"] == p["effect_id"]
                    && e["cursor"].as_u64() < receipt["cursor"].as_u64()
            }),
            "receipt lacks entered effect",
        )?;
        check(
            prepared.call().name != "shell" && prepared.call().name != "agent.invoke",
            "unauthorized effect",
        )?;
    }
    let loads: Vec<_> = receipts
        .iter()
        .filter(|e| e["payload"]["input"]["prepared"]["call"]["name"] == "skill.load")
        .collect();
    let mut body_next = false;
    for load in &loads {
        let result: kolyan_model::ToolResult =
            serde_json::from_value(load["payload"]["output"].clone()).map_err(|e| e.to_string())?;
        let observed: super::evidence::BodyObservation =
            serde_json::from_str(&result.content).map_err(|e| e.to_string())?;
        let mut expected_body = body.clone();
        if matches!(
            case.mutation,
            Mutation::MaliciousBodyFixtureAndExplicitForbiddenCalls
        ) {
            expected_body.push_str(&dataset.malicious_fixture_appendix);
        }
        check(
            observed.body == expected_body && observed.key.revision == "r2",
            "wrong skill body/version",
        )?;
        check(
            facts.iter().any(|f| {
                f["draft"]["kind"] == "agent.skill.bound"
                    && f["draft"]["fact_id"] == observed.binding.fact_id
                    && f["stream_id"] == observed.binding.stream_id
                    && f["position"] == observed.binding.position
            }),
            "actual binding proof missing",
        )?;
        check(
            facts.iter().any(|f| {
                f["draft"]["kind"] == "agent.skill.registered"
                    && f["draft"]["fact_id"] == observed.registration.fact_id
                    && f["draft"]["payload"]["metadata"]["content_digest"]
                        == observed.content_digest
            }),
            "registration proof missing",
        )?;
        body_next = requests
            .iter()
            .skip(1)
            .any(|r| super::evidence::check_next_result(&r.messages, &result, &observed).is_ok());
    }
    let satisfied = row["final"]["task"]["goal_assessments"]
        .as_array()
        .ok_or("goal assessments missing")?
        .iter()
        .any(|g| g["assessment"]["verdict"] == "Satisfied");
    let successful = matches!(
        case.mutation,
        Mutation::None
            | Mutation::RebuildAfterWriteApproval
            | Mutation::MaliciousBodyFixtureAndExplicitForbiddenCalls
    );
    if successful {
        check(
            row["file_bytes"] == json!(dataset.artifact.expected_utf8.as_bytes()),
            "physical bytes mismatch",
        )?;
        check(
            row["file_sha256"]
                == format!(
                    "{:x}",
                    Sha256::digest(dataset.artifact.expected_utf8.as_bytes())
                ),
            "physical SHA mismatch",
        )?;
        check(
            satisfied && row["final"]["task"]["state"] == "Completed",
            "no actual Goal/Task completion",
        )?;
        check(
            body_next,
            "next Step does not contain exact original load result",
        )?;
        // Both native results must have their exact original envelopes in a
        // later immutable ModelRequested, not only textual claims in an answer.
        for receipt in receipts.iter().filter(|e| {
            matches!(
                e["payload"]["input"]["prepared"]["call"]["name"].as_str(),
                Some("file.write" | "file.read")
            )
        }) {
            let result: kolyan_model::ToolResult =
                serde_json::from_value(receipt["payload"]["output"].clone())
                    .map_err(|e| e.to_string())?;
            let native: kolyan_tools::FileOperationResult =
                serde_json::from_str(&result.content).map_err(|e| e.to_string())?;
            check(
                !result.is_error
                    && native.path == dataset.artifact.path
                    && native.bytes == dataset.artifact.expected_utf8.len()
                    && native.sha256
                        == format!(
                            "{:x}",
                            Sha256::digest(dataset.artifact.expected_utf8.as_bytes())
                        ),
                "native result digest/byte count mismatch",
            )?;
            if receipt["payload"]["input"]["prepared"]["call"]["name"] == "file.read" {
                check(
                    native.content.as_deref() == Some(dataset.artifact.expected_utf8.as_str()),
                    "native read bytes changed",
                )?;
            }
            check(requests.iter().any(|r|r.messages.iter().any(|m|m.content.iter().any(|b|matches!(b,kolyan_model::ContentBlock::ToolResult{result:r} if r==&result)))),"native ToolResult lost before final Step")?;
        }
    } else {
        check(
            row["file_bytes"].is_null()
                && !satisfied
                && row["final"]["task"]["state"] != "Completed",
            "failed/revoked task produced successful artifact",
        )?;
    }
    check(
        row["final"]["task"]["invocations"]
            .as_object()
            .ok_or("invocations missing")?
            .len()
            == 1,
        "unexpected child instance",
    )?;
    if row.get("rebuilt").is_some() {
        check(
            row["ledger_after_rebuild"] == row["ledger_before"],
            "rebuild changed saved checkpoint or emitted execution facts",
        )?;
        check(
            row["before"]["physical"] == row["rebuilt"]["physical"]
                && row["before"]["approvals"] == row["rebuilt"]["approvals"],
            "rebuild changed attempt/instance/approval",
        )?;
        let before = row["ledger_before"]
            .as_array()
            .ok_or("before ledger missing")?;
        check(
            before
                .iter()
                .filter(|e| {
                    e["kind"] == "effect_receipt"
                        && e["payload"]["input"]["prepared"]["call"]["name"] == "file.write"
                })
                .count()
                == 0,
            "write before approval",
        )?;
    }
    let deny = matches!(
        case.mutation,
        Mutation::RevokeSavedR2AfterWriteApproval | Mutation::RemoveAclAfterWriteApproval
    );
    // ModelRequested records intent before the Skill provider guard. For local
    // fixtures the actual HTTP observer, not intent count, proves zero late GEN.
    let late_openings = if deny
        || matches!(
            case.mutation,
            Mutation::RevokeAfterCompletedLoadBeforeNextStream
        ) {
        let before = row["ledger_before"]
            .as_array()
            .ok_or("before ledger missing")?;
        if row["scripted_localhost"] == true {
            row["http"]
                .as_array()
                .ok_or("HTTP observations missing")?
                .len()
                - usize::try_from(
                    row["http_before_decision"]
                        .as_u64()
                        .ok_or("before HTTP count missing")?,
                )
                .map_err(|e| e.to_string())?
        } else {
            requests.len()
                - before
                    .iter()
                    .filter(|e| e["kind"] == "model_requested")
                    .count()
        }
    } else if matches!(
        case.mutation,
        Mutation::RevokeAfterModelLoadCallBeforePrepare
    ) {
        row["http"]
            .as_array()
            .ok_or("HTTP observations missing")?
            .len()
            - 1
    } else {
        0
    };
    let errors: Vec<_> = events
        .iter()
        .filter(|e| e["kind"] == "tool_execution_completed" && e["payload"]["is_error"] == true)
        .collect();
    let feedback=errors.len()==2 && errors.iter().all(|e|{let result=serde_json::from_value::<kolyan_model::ToolResult>(e["payload"]["result"].clone());result.is_ok_and(|result|requests.iter().any(|r|r.messages.iter().any(|m|m.content.iter().any(|b|matches!(b,kolyan_model::ContentBlock::ToolResult{result:r} if r==&result)))))});
    let summary = json!({"selected_revision":"r2","skill_receipts":count("skill.load"),"write_receipts":count("file.write"),"read_receipts":count("file.read"),"goal_verdict":if satisfied{"Satisfied"}else{"NotSatisfied"},"task_status":row["final"]["task"]["state"],
        "payload_leaked_before_load":false,"load_body_exact_next_request":body_next,"minimum_model_steps":events.iter().filter(|e|e["kind"]=="step_completed").count(),"write_before_decision":0,"new_instance_on_rebuild":false,
        "same_attempt_and_checkpoint":row["before"]["physical"]==row["rebuilt"]["physical"] && row["before"]["approvals"]==row["rebuilt"]["approvals"],
        "new_model_openings_after_decision":late_openings,"new_skill_reads_after_decision":count("skill.load")-row.get("ledger_before").and_then(Value::as_array).map_or(0,|es|es.iter().filter(|e|e["kind"]=="effect_receipt" && e["payload"]["input"]["prepared"]["call"]["name"]=="skill.load").count()),
        "decision":row["decision"],"approval_closed":row["final"]["approvals"].as_array().is_some_and(Vec::is_empty),"task_cancel_command":events.iter().any(|e|e["kind"]=="execution_cancelled"),
        "shell_receipts":count("shell"),"child_instances":instances.len()-1,"unauthorized_grants":events.iter().filter(|e|e["kind"]=="effect_prepared" && matches!(e["payload"]["operation_kind"].as_str(),Some("shell"|"agent.invoke"))).count(),
        "failed_calls_feedback_exact_next_request":feedback,"valid_write_receipts":count("file.write"),"new_model_openings_after_revocation":late_openings,"authority_expanded":false,"version_reselected":loads.iter().any(|e|e["payload"]["input"]["prepared"]["call"]["arguments"]["revision"]!="r2"),
        "task_success_fact":row["final"]["task"]["state"]=="Completed","prior_load_receipt_preserved":count("skill.load")==1});
    let expectations: Vec<Value> = include_str!("../../expected/agent/skills.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).map_err(|e| e.to_string()))
        .collect::<Result<_, _>>()?;
    let expected = expectations
        .iter()
        .find(|e| e["id"] == id)
        .ok_or("missing expected row")?;
    for (key, value) in expected.as_object().ok_or("expected object")? {
        if key == "id" {
            continue;
        }
        if key == "minimum_model_steps" {
            check(
                summary[key].as_u64() >= value.as_u64(),
                "minimum Steps not met",
            )?;
        } else {
            check(
                summary[key] == *value,
                &format!("{key}: actual={} expected={value}", summary[key]),
            )?;
        }
    }
    Ok(())
}
impl Write for BoundedCount {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        let next = self
            .0
            .checked_add(bytes.len())
            .ok_or_else(|| io::Error::other("ToolResult size overflow"))?;
        if next > 512 * 1024 {
            return Err(io::Error::other("complete ToolResult exceeds bound"));
        }
        self.0 = next;
        Ok(bytes.len())
    }
    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}
