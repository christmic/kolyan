mod config;
mod wire;

use std::{collections::BTreeMap, io::Write, sync::Arc};

use kolyan_agent_host::{AgentHost, ApprovalDecision, HostStartRequest, HostTaskView};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    protocols: Vec<String>,
    replies: BTreeMap<String, Value>,
    cases: Vec<Case>,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    named: bool,
    approval: bool,
    replies: Vec<String>,
    goals: Vec<kolyan_agent_host::FileGoalInput>,
    actions: Vec<String>,
    input: String,
    expected_state: String,
    expected_files: BTreeMap<String, Option<String>>,
    expected_requests: usize,
    expected_initial_approvals: usize,
    expected_refusals: usize,
    #[serde(default)]
    expected_child_writes: Vec<String>,
}

#[test]
fn production_host_localhost_and_native_matrix() {
    let evidence = tempfile::Builder::new()
        .prefix("kolyan-agent-host-native-")
        .tempdir()
        .unwrap()
        .keep();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "agent_host::production_host_child",
            "--ignored",
            "--nocapture",
        ])
        .env("KOLYAN_HOST_EVIDENCE", &evidence)
        .env(
            "KOLYAN_HOST_LOOPBACK_KEY",
            "localhost-placeholder-not-a-real-secret",
        )
        .status()
        .unwrap();
    let path = evidence.join("actual.jsonl");
    eprintln!("AGENT_HOST_NATIVE_EVIDENCE={}", path.display());
    let dataset: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let actual: Vec<Value> = std::fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actual.len(), dataset.protocols.len() * dataset.cases.len());
    for row in actual {
        let case = dataset
            .cases
            .iter()
            .find(|case| case.id == row["case_id"])
            .unwrap();
        assert!(row["error"].is_null(), "{row}");
        assert!(row["ledger_error"].is_null(), "{row}");
        assert!(row["task_facts_error"].is_null(), "{row}");
        assert!(
            !row["ledger"]
                .as_array()
                .expect("actual ledger events are required")
                .is_empty(),
            "{row}"
        );
        assert!(
            !row["task_facts"]
                .as_array()
                .expect("actual Task facts are required")
                .is_empty(),
            "{row}"
        );
        assert_eq!(
            row["initial_approvals"], case.expected_initial_approvals,
            "{row}"
        );
        assert_eq!(row["final"]["task"]["state"], case.expected_state, "{row}");
        assert_eq!(
            row["requests"].as_array().unwrap().len(),
            case.expected_requests,
            "{row}"
        );
        assert_eq!(row["refusals"], case.expected_refusals, "{row}");
        for (name, content) in &case.expected_files {
            assert_eq!(row["files"][name], json!(content), "{row}");
        }
        if case.approval {
            for name in case.expected_files.keys() {
                assert!(row["initial_files"][name].is_null(), "{row}");
            }
        }
        if !case.expected_child_writes.is_empty() {
            let attempts = row["final"]["task"]["attempts"].as_object().unwrap();
            let mut paths = Vec::new();
            for attempt in attempts
                .values()
                .filter(|a| a["binding"]["invocation_id"] != "root")
            {
                let execution = &attempt["binding"]["execution"]["execution_id"];
                let writes: Vec<_> = row["ledger"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|event| {
                        event["execution_id"] == *execution && event["kind"] == "step_completed"
                    })
                    .flat_map(|event| {
                        event["payload"]["step"]["response"]["content"]
                            .as_array()
                            .unwrap()
                    })
                    .filter(|block| block["call"]["name"] == "file.write")
                    .collect();
                assert_eq!(writes.len(), 1, "each actual child must write once: {row}");
                paths.push(
                    writes[0]["call"]["arguments"]["path"]
                        .as_str()
                        .unwrap()
                        .to_owned(),
                );
            }
            paths.sort();
            let mut expected = case.expected_child_writes.clone();
            expected.sort();
            assert_eq!(paths, expected, "{row}");
        }
    }
    assert!(
        status.success(),
        "fixture process failed after exporting actual evidence"
    );
}

#[tokio::test]
#[ignore = "helper subprocess: invoked by the local matrix with isolated environment"]
async fn production_host_child() {
    let evidence = std::path::PathBuf::from(std::env::var("KOLYAN_HOST_EVIDENCE").unwrap());
    let dataset: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let mut export = std::fs::File::create(evidence.join("actual.jsonl")).unwrap();
    for protocol in &dataset.protocols {
        for case in &dataset.cases {
            let row = scenario(&dataset, protocol, case, &evidence).await;
            writeln!(export, "{row}").unwrap();
            export.flush().unwrap();
        }
    }
    export.sync_all().unwrap();
    drop(export);
}

async fn scenario(
    dataset: &Dataset,
    protocol: &str,
    case: &Case,
    evidence: &std::path::Path,
) -> Value {
    let root = evidence.join(format!("{}-{protocol}", case.id));
    let mut row = json!({"case_id":case.id,"protocol":protocol,"error":null,"steps":[],"requests":[],"files":{},"refusals":0});
    let script = case
        .replies
        .iter()
        .map(|key| dataset.replies[key].clone())
        .collect();
    let mut backend = wire::Backend::start(protocol, script).unwrap();
    let result = run(case, protocol, &root, &backend.endpoint, &mut row).await;
    if let Err(error) = result {
        row["error"] = json!(error);
    }
    match backend.finish() {
        Ok(()) => {}
        Err(error) => {
            row["backend_error"] = json!(error);
            row["error"] = json!(format!("backend: {error}; run={}", row["error"]));
        }
    }
    row["requests"] = json!(backend.requests());
    for name in case.expected_files.keys() {
        row["files"][name] = match std::fs::read(root.join("workspace").join(name)) {
            Ok(bytes) => {
                row["file_bytes"][name] = json!(bytes);
                json!(String::from_utf8(bytes).unwrap())
            }
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Value::Null,
            Err(error) => json!({"error":error.to_string()}),
        };
    }
    match SqliteLedger::open(root.join("state/executions.sqlite")) {
        Ok(ledger) => match ledger.events_after(0) {
            Ok(events) => row["ledger"] = json!(events),
            Err(error) => row["ledger_error"] = json!(error.to_string()),
        },
        Err(error) => row["ledger_error"] = json!(error.to_string()),
    }
    match SqliteFactJournal::open(root.join("state/facts.sqlite")) {
        Ok(journal) => match task_facts(&journal, &case.id) {
            Ok(facts) => row["task_facts"] = json!(facts),
            Err(error) => row["task_facts_error"] = json!(error),
        },
        Err(error) => row["task_facts_error"] = json!(error.to_string()),
    }
    row
}

fn task_facts(
    journal: &SqliteFactJournal,
    task: &str,
) -> Result<Vec<kolyan_ledger::FactRecord>, String> {
    let mut facts = Vec::new();
    let mut cursor = 0;
    loop {
        let page = journal
            .read(task, cursor, 1024)
            .map_err(|error| error.to_string())?;
        if page.is_empty() {
            return Ok(facts);
        }
        for fact in page {
            if fact.stream_id != task || fact.position != cursor + 1 {
                return Err("noncontiguous or foreign Task evidence".into());
            }
            cursor = fact.position;
            facts.push(fact);
            if facts.len() > 16384 {
                return Err("Task evidence fixture bound exceeded".into());
            }
        }
    }
}

async fn run(
    case: &Case,
    protocol: &str,
    root: &std::path::Path,
    endpoint: &str,
    row: &mut Value,
) -> Result<(), String> {
    config::directories(root).map_err(|e| e.to_string())?;
    let mut host = Arc::new(
        AgentHost::open(config::configuration(
            root, protocol, endpoint, case, false, false,
        )?)
        .map_err(|e| e.to_string())?,
    );
    let request: HostStartRequest = config::request(case)?;
    row["start_input"] = json!(request);
    let mut view = match host.start("logical".into(), request).await {
        Ok(view) => view,
        Err(error) => {
            match host.query("logical".into(), case.id.clone()).await {
                Ok(view) => row["final"] = json!(view),
                Err(query_error) => row["query_error"] = json!(query_error.to_string()),
            }
            return Err(error.to_string());
        }
    };
    row["initial_approvals"] = json!(view.approvals.len());
    row["initial"] = json!(view);
    for name in case.expected_files.keys() {
        row["initial_files"][name] = match std::fs::read(root.join("workspace").join(name)) {
            Ok(bytes) => json!(bytes),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => Value::Null,
            Err(error) => return Err(error.to_string()),
        };
    }
    let first = view.approvals.first().cloned();
    for action in &case.actions {
        let result: Result<HostTaskView, String> = match action.as_str() {
            "rebuild" | "revoke" | "different_model" | "missing_credential" => {
                drop(host);
                let mut configuration = config::configuration(
                    root,
                    protocol,
                    endpoint,
                    case,
                    action == "revoke",
                    action == "different_model",
                )?;
                if action == "missing_credential" {
                    configuration.deployment.api_key_env =
                        "KOLYAN_HOST_TEST_ABSENT_CREDENTIAL_87FA3".into();
                }
                host = Arc::new(AgentHost::open(configuration).map_err(|e| e.to_string())?);
                host.query("logical".into(), case.id.clone())
                    .await
                    .map_err(|e| e.to_string())
            }
            "accept" | "deny" => {
                let pending = view
                    .approvals
                    .first()
                    .ok_or("fixture requested decision without pending approval")?;
                host.decide_approval(
                    "logical".into(),
                    case.id.clone(),
                    pending.invocation_id.clone(),
                    pending.approval_id.clone(),
                    if action == "accept" {
                        ApprovalDecision::Accept
                    } else {
                        ApprovalDecision::Deny
                    },
                )
                .await
                .map_err(|e| e.to_string())
            }
            "repeat_accept" => {
                let pending = first.as_ref().ok_or("fixture has no original approval")?;
                host.decide_approval(
                    "logical".into(),
                    case.id.clone(),
                    pending.invocation_id.clone(),
                    pending.approval_id.clone(),
                    ApprovalDecision::Accept,
                )
                .await
                .map_err(|e| e.to_string())
            }
            "foreign_query" => host
                .query("foreign".into(), case.id.clone())
                .await
                .map_err(|e| e.to_string()),
            "cancel" => host
                .cancel("logical".into(), case.id.clone())
                .await
                .map_err(|e| e.to_string()),
            _ => return Err(format!("unknown fixture action {action}")),
        };
        let record = match result {
            Ok(next) => {
                view = next;
                json!({"action":action,"view":view,"error":null})
            }
            Err(error) => {
                row["refusals"] = json!(row["refusals"].as_u64().unwrap() + 1);
                json!({"action":action,"error":error})
            }
        };
        row["steps"].as_array_mut().unwrap().push(record);
    }
    row["final"] = json!(
        host.query("logical".into(), case.id.clone())
            .await
            .map_err(|e| e.to_string())?
    );
    Ok(())
}
