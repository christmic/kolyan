//! Native byte fidelity without a model, Provider, candidate or newline repair.
//! Generic ExecutionRuntime evidence is distinct from Turn prepared_tool_v1 facts.

mod bridge;

use std::{fs, os::unix::fs::DirBuilderExt, sync::Arc, time::Duration};

use kolyan_core::{ToolExecutor, ToolInvocation, TurnControl};
use kolyan_ledger::{LedgerStore, SqliteLedger};
use kolyan_model::ToolCall;
use kolyan_policy::{
    ApprovalEvidence, PathScope, PolicyContext, PolicyEngine, PreparedGrant, ToolExecutionScope,
};
use kolyan_runtime::{ExecutionKey, ExecutionRuntime};
use kolyan_tools::{FileOperationLimits, IsolatedFileConfig, IsolatedFileTools};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::{evidence::Evidence, tools};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    operation: Operation,
    seed: Option<String>,
    content: String,
    expected: String,
}

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Operation {
    Write,
    Edit,
}

#[tokio::test]
async fn native_write_edit_eof_byte_fidelity_exports_all_before_comparing() {
    let cases: Vec<Case> =
        serde_json::from_str(include_str!("../fixtures/agent/file_byte_fidelity.json")).unwrap();
    let installation = tools::worker::WorkerRun::prepare().await;
    let mut observations = Vec::new();
    for case in &cases {
        observations.push(observe(case, &installation).await);
    }
    // All cases are durable before the first semantic comparison.
    for (case, actual) in cases.iter().zip(observations) {
        compare(case, &actual);
    }
}

async fn observe(case: &Case, installation: &tools::worker::WorkerRun) -> Value {
    let root = tempfile::Builder::new()
        .prefix("kolyan-file-byte-fidelity-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    fs::create_dir_all(root.join("workspace/safe")).unwrap();
    fs::create_dir(root.join("state")).unwrap();
    fs::DirBuilder::new()
        .mode(0o700)
        .create(root.join("staging"))
        .unwrap();
    let evidence = Arc::new(Evidence::new(&root.join("actual.jsonl")));
    println!(
        "BYTE_FIDELITY_TRACE={}",
        root.join("actual.jsonl").display()
    );
    evidence.append(json!({"event":"plan","case":case.id,"model_requests":0,"receipt_port":"production_generic_execution_runtime","content_bytes":case.content.as_bytes(),"expected_bytes":case.expected.as_bytes()})).unwrap();
    let result = async {
        tools::initialize_worker(&root, &evidence, installation)?;
        let worker = tools::worker::verified_worker(&root, &evidence)?;
        let tools = Arc::new(IsolatedFileTools::new(IsolatedFileConfig {workspace:root.join("workspace"),staging_root:root.join("staging"),protected_roots:vec![root.join("state"),root.join("trusted-worker"),worker.parent().ok_or("worker parent missing")?.to_path_buf()],worker,file_limits:FileOperationLimits {max_read_bytes:65536,max_write_bytes:65536},max_output_bytes:1024*1024,timeout:Duration::from_secs(30)}).map_err(|e|e.to_string())?);
        let ledger = SqliteLedger::open(root.join("state/ledger.sqlite")).map_err(|e|e.to_string())?;
        let mut policy = PolicyEngine::default();
        for mut manifest in super::data::dataset().policy {
            manifest.path_scopes = vec![PathScope::new(root.join("workspace/safe").to_string_lossy())];
            policy.register(manifest);
        }
        policy.restrict_workspace(root.join("workspace").to_string_lossy());
        let key = ExecutionKey {session_id:"byte-session".into(),turn_id:"byte-turn".into(),execution_id:"byte-execution".into()};
        let mut calls = Vec::new();
        if let Some(seed) = &case.seed {calls.push(ToolCall {id:"seed".into(),name:"file.write".into(),arguments:json!({"path":"safe/proof.txt","content":seed})});}
        calls.push(ToolCall {id:"target".into(),name:match case.operation {Operation::Write=>"file.write",Operation::Edit=>"file.edit"}.into(),arguments:match case.operation {Operation::Write=>json!({"path":"safe/proof.txt","content":case.content}),Operation::Edit=>json!({"path":"safe/proof.txt","old_text":"old","new_text":case.content})}});
        calls.push(ToolCall {id:"read".into(),name:"file.read".into(),arguments:json!({"path":"safe/proof.txt"})});
        for call in calls {
            let before = fs::read(root.join("workspace/safe/proof.txt"));
            evidence.append(json!({"event":"operation_input","call":call,"physical_before":before.as_ref().ok(),"physical_before_error":before.as_ref().err().map(ToString::to_string)}))?;
            let prepared = ToolExecutor::prepare(tools.as_ref(), call.clone()).await.map_err(|e|e.to_string())?;
            let decision = policy.decide_prepared(&prepared, &PolicyContext::default());
            let revision = decision.policy_version.clone();
            let scope = ToolExecutionScope {execution:key.clone(),step_id:format!("byte-{}",call.id),agent_snapshot_digest:None};
            let grant = PreparedGrant::issue(&prepared,decision.clone(),ApprovalEvidence::NotConfirmed,scope.clone()).map_err(|e|e.to_string())?;
            evidence.append(json!({"event":"prepared_authority","call":call,"prepared":prepared,"decision":decision,"grant":grant,"scope":scope,"policy_revision":revision}))?;
            let invocation = ToolInvocation {prepared,grant,scope,policy_revision:revision,control:TurnControl::default(),window:kolyan_core::ToolExecutionWindow::at_deadline(std::time::Instant::now()+std::time::Duration::from_secs(30))};
            let port = bridge::Bridge::new(tools.clone(), invocation, evidence.clone())?;
            let request = port.request.clone();
            let key = key.clone();
            let ledger = ledger.clone();
            let disposition = tokio::task::spawn_blocking(move || {
                let runtime = ExecutionRuntime::new(ledger,port.clone(),port);
                runtime.start(&key).map_err(|e|e.to_string())?;
                runtime.apply_effect(&key,&request).map_err(|e|e.to_string())
            }).await.map_err(|e|e.to_string())??;
            let after = fs::read(root.join("workspace/safe/proof.txt")).map_err(|e|e.to_string())?;
            evidence.append(json!({"event":"operation_observed","call_id":call.id,"disposition":format!("{disposition:?}"),"physical_after":after,"physical_sha256":digest(&after)}))?;
        }
        Ok::<(),String>(())
    }.await;
    let ledger =
        SqliteLedger::open(root.join("state/ledger.sqlite")).and_then(|l| l.events_after(0));
    evidence.append(json!({"event":"ledger_export","events":ledger.as_ref().ok(),"error":ledger.as_ref().err().map(ToString::to_string)})).unwrap();
    let physical = fs::read(root.join("workspace/safe/proof.txt"));
    let rows = evidence.rows();
    let actual = json!({"case":case.id,"evidence":root.join("actual.jsonl"),"error":result.err(),"ledger":ledger.as_ref().ok(),"physical":physical.as_ref().ok(),"physical_sha256":physical.as_ref().ok().map(|b|digest(b)),"rows":rows});
    evidence
        .append(json!({"event":"actual","value":actual}))
        .unwrap();
    actual
}

fn compare(case: &Case, actual: &Value) {
    let label = format!("{} {}", case.id, actual["evidence"]);
    assert!(actual["error"].is_null(), "{label}: {actual}");
    assert_eq!(
        actual["physical"],
        json!(case.expected.as_bytes()),
        "{label}"
    );
    assert_eq!(
        actual["physical_sha256"],
        digest(case.expected.as_bytes()),
        "{label}"
    );
    let ledger = actual["ledger"].as_array().unwrap();
    let count = if case.seed.is_some() { 3 } else { 2 };
    for kind in ["effect_authorized", "effect_started", "effect_receipt"] {
        assert_eq!(
            ledger.iter().filter(|e| e["kind"] == kind).count(),
            count,
            "{label}"
        );
    }
    assert!(
        !ledger.iter().any(|e| e["kind"] == "model_requested"),
        "{label}"
    );
    for call_id in ["target", "read"] {
        let receipt = ledger
            .iter()
            .find(|e| {
                e["kind"] == "effect_receipt" && e["payload"]["receipt"]["effect_id"] == call_id
            })
            .unwrap();
        let returned = &receipt["payload"]["output"];
        assert_eq!(returned["call_id"], call_id, "{label}");
        assert_eq!(returned["is_error"], false, "{label}");
        let operation: Value = serde_json::from_str(returned["content"].as_str().unwrap()).unwrap();
        assert_eq!(operation["bytes"], case.expected.len(), "{label}");
        assert_eq!(
            operation["sha256"],
            digest(case.expected.as_bytes()),
            "{label}"
        );
        if call_id == "read" {
            assert_eq!(operation["content"], case.expected, "{label}");
        }
        let rows = actual["rows"].as_array().unwrap();
        let input = rows
            .iter()
            .find(|r| r["event"] == "operation_input" && r["call"]["id"] == call_id)
            .unwrap();
        let authority = rows
            .iter()
            .find(|r| r["event"] == "prepared_authority" && r["call"]["id"] == call_id)
            .unwrap();
        let admission = rows
            .iter()
            .find(|r| r["event"] == "runtime_authorization" && r["request"]["effect_id"] == call_id)
            .unwrap();
        let authorized = ledger
            .iter()
            .find(|e| e["kind"] == "effect_authorized" && e["payload"]["effect_id"] == call_id)
            .unwrap();
        let started = ledger
            .iter()
            .find(|e| e["kind"] == "effect_started" && e["payload"]["effect_id"] == call_id)
            .unwrap();
        assert_eq!(input["call"], authority["prepared"]["call"], "{label}");
        if call_id == "target" {
            let field = match case.operation {
                Operation::Write => "content",
                Operation::Edit => "new_text",
            };
            assert_eq!(input["call"]["arguments"][field], case.content, "{label}");
        }
        assert_eq!(
            authority["grant"]["prepared_digest"], authority["prepared"]["digest"],
            "{label}"
        );
        assert_eq!(authority["grant"]["scope"], authority["scope"], "{label}");
        assert_eq!(authority["scope"]["execution"], admission["key"], "{label}");
        assert_eq!(
            authority["grant"]["policy_revision"], authority["policy_revision"],
            "{label}"
        );
        assert_eq!(
            admission["request"]["input_digest"], authority["prepared"]["digest"],
            "{label}"
        );
        assert_eq!(
            admission["authorization"]["constraints_digest"],
            digest(&serde_json::to_vec(&authority["grant"]["constraints"]).unwrap()),
            "{label}"
        );
        assert_eq!(authorized["payload"], admission["authorization"], "{label}");
        assert_eq!(
            receipt["payload"]["receipt"]["authorization_id"],
            authorized["payload"]["authorization_id"],
            "{label}"
        );
        assert_eq!(
            receipt["payload"]["receipt"]["input_digest"], authority["prepared"]["digest"],
            "{label}"
        );
        assert_eq!(
            receipt["payload"]["receipt"]["executor_revision"],
            authority["prepared"]["tool_revision"],
            "{label}"
        );
        assert!(
            authorized["cursor"].as_u64().unwrap() < started["cursor"].as_u64().unwrap(),
            "{label}"
        );
        assert!(
            started["cursor"].as_u64().unwrap() < receipt["cursor"].as_u64().unwrap(),
            "{label}"
        );
        let observed = rows
            .iter()
            .find(|r| r["event"] == "operation_observed" && r["call_id"] == call_id)
            .unwrap();
        assert_eq!(
            observed["physical_after"],
            json!(case.expected.as_bytes()),
            "{label}"
        );
        let adapter = rows
            .iter()
            .find(|r| r["event"] == "adapter_returned" && r["result"]["call_id"] == call_id)
            .unwrap();
        assert_eq!(adapter["result"], *returned, "{label}");
        assert_eq!(
            receipt["payload"]["receipt"]["result_digest"],
            digest(&serde_json::to_vec(returned).unwrap()),
            "{label}"
        );
    }
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}
