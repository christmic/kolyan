//! L1 isolation across real HTTP Sessions, process restarts and shared stores.

use super::http_live::matrix;

use std::{fs, io::Write, path::Path};

use kolyan_ledger::{LedgerEvent, LedgerEventKind, LedgerStore, SqliteLedger};
use kolyan_storage::{FileSessionStore, SessionStore};
use reqwest::Method;
use serde_json::{Value, json};

use super::{Process, common, setup};

fn fixture() -> Value {
    serde_json::from_str(include_str!("../../fixtures/server_ledger_live.json")).unwrap()
}

fn configured_entries() -> Vec<(
    &'static str,
    &'static str,
    String,
    String,
    common::ModelMatrixEntry,
)> {
    let config = common::load_config();
    let mut entries = Vec::new();
    for (family, cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        for model in cfg.model_matrix {
            entries.push((
                family,
                "openai_responses",
                cfg.base_url.clone(),
                cfg.api_key_env.clone(),
                model,
            ));
        }
    }
    for (family, cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        for model in cfg.model_matrix {
            entries.push((
                family,
                "anthropic_messages",
                cfg.base_url.clone(),
                cfg.api_key_env.clone(),
                model,
            ));
        }
    }
    entries
}

#[tokio::test]
#[ignore = "real HTTP processes and all configured Provider credentials"]
async fn http_process_ledger_live_matrix() {
    let data = fixture();
    let entries = configured_entries();
    assert_eq!(entries.len() as u64, data["configured_combinations"]);
    let planned = entries
        .into_iter()
        .flat_map(|entry| {
            data["groups"]
                .as_array()
                .unwrap()
                .iter()
                .cloned()
                .map(move |group| (entry.clone(), group))
        })
        .collect::<Vec<_>>();
    let mut report = matrix::Matrix::new(planned.iter().map(
        |((family, protocol, _, _, model), group)| {
            format!(
                "{family}/{protocol}/{}/{}",
                model.model,
                group["name"].as_str().unwrap()
            )
        },
    ));
    fs::copy(
        env!("CARGO_BIN_EXE_kolyan-server"),
        report.directory.join("server.bin"),
    )
    .unwrap();
    for (index, ((family, protocol, url, key_env, model), group)) in planned.into_iter().enumerate()
    {
        let directory = report.directory.join(index.to_string());
        fs::create_dir_all(&directory).unwrap();
        report
            .run(index, async {
                assert!(
                    std::env::var(&key_env).is_ok_and(|key| !key.trim().is_empty()),
                    "missing credential variable {key_env}"
                );
                setup(&directory, &url);
                let path = directory.join("server.json");
                let mut config: Value = serde_json::from_slice(&fs::read(&path).unwrap()).unwrap();
                config["protocol"] = json!(protocol);
                config["api_key_env"] = json!(key_env);
                config["parameter_table"] = common::parameter_table(family, protocol, &model.model);
                config["request"]["model"] = json!({"provider":family,"model":model.model});
                config["request"]["system"] = group["system"].clone();
                config["request"]["max_output_tokens"] =
                    json!(model.max_output_tokens.or(Some(80960)));
                fs::write(path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
                live_isolation(&directory, group["cases"].as_array().unwrap()).await;
            })
            .await;
    }
    assert!(
        report.complete(),
        "L1 HTTP matrix failures: {}",
        report.directory.display()
    );
}

async fn live_isolation(directory: &Path, cases: &[Value]) {
    let mut process = Process::start(directory);
    let mut saved = Vec::new();
    for case in cases {
        let histories = saved
            .iter()
            .map(|(prior, _)| history(directory, prior))
            .collect::<Vec<_>>();
        let session = case["session_id"].as_str().unwrap();
        let turn = case["turn_id"].as_str().unwrap();
        let session_path = format!("/v1/sessions/{session}");
        let resource = format!("{session_path}/turns/{turn}");
        if case["create"] == true {
            process
                .call(
                    Method::POST,
                    "/v1/sessions",
                    Some(json!({"session_id":session})),
                    201,
                )
                .await;
        }
        let mut result = process
            .call(
                Method::POST,
                &format!("{session_path}/turns"),
                Some(json!({"turn_id":turn,"input":case["input"]})),
                200,
            )
            .await;
        if case["approve"] == true {
            assert_eq!(result["state"], "suspended", "{result}");
            assert_eq!(result["execution_stopped"], true);
            assert_eq!(
                result["pending_approval"]["tool_name"],
                case["expected"]["tool"]
            );
            assert_eq!(
                result["pending_approval"]["arguments"],
                json!({"path":case["path"],"content":case["marker"]})
            );
            assert!(
                !directory
                    .join("workspace")
                    .join(case["path"].as_str().unwrap())
                    .exists()
            );
            assert!(
                !execution_events(directory, case)
                    .iter()
                    .any(|event| matches!(
                        event.kind,
                        LedgerEventKind::EffectStarted | LedgerEventKind::EffectReceipt
                    ))
            );
            let approval = result["pending_approval"]["approval_id"]
                .as_str()
                .unwrap()
                .to_owned();
            if case["restart_pending"] == true {
                drop(process);
                process = Process::start(directory);
                assert_eq!(
                    process.call(Method::GET, &resource, None, 200).await,
                    result
                );
            }
            result = process
                .call(
                    Method::POST,
                    &format!("{resource}/approvals/{approval}/decision"),
                    Some(json!({"decision":"approve"})),
                    200,
                )
                .await;
        }
        verify_execution(directory, case, &result);
        for old in &histories {
            let current = history(directory, &json!({"session_id":old["session_id"]}));
            if old["session_id"] != case["session_id"] {
                assert_eq!(&current, old, "another Session's Turn changed this history");
            } else {
                for field in ["messages", "context_messages", "turns"] {
                    assert!(
                        current[field]
                            .as_array()
                            .unwrap()
                            .starts_with(old[field].as_array().unwrap()),
                        "lost {field} history"
                    );
                }
                for field in ["inputs", "commits", "context_commits"] {
                    for (key, value) in old[field].as_object().unwrap() {
                        assert_eq!(&current[field][key], value, "rewritten {field}/{key}");
                    }
                }
            }
        }
        saved.push((case.clone(), result));
        if case["restart_completed"] == true {
            let ledger_before = SqliteLedger::open(directory.join("ledger.sqlite"))
                .unwrap()
                .events_after(0)
                .unwrap();
            let histories_before = saved
                .iter()
                .map(|(case, _)| history(directory, case))
                .collect::<Vec<_>>();
            drop(process);
            process = Process::start(directory);
            for (prior, expected) in &saved {
                let path = format!(
                    "/v1/sessions/{}/turns/{}",
                    prior["session_id"].as_str().unwrap(),
                    prior["turn_id"].as_str().unwrap()
                );
                let restored = process.call(Method::GET, &path, None, 200).await;
                assert_eq!(&restored, expected);
                verify_execution(directory, prior, &restored);
                process
                    .call(
                        Method::GET,
                        &format!("/v1/sessions/{}", prior["session_id"].as_str().unwrap()),
                        None,
                        200,
                    )
                    .await;
            }
            assert_eq!(
                histories_before,
                saved
                    .iter()
                    .map(|(case, _)| history(directory, case))
                    .collect::<Vec<_>>()
            );
            assert_eq!(
                ledger_before,
                SqliteLedger::open(directory.join("ledger.sqlite"))
                    .unwrap()
                    .events_after(0)
                    .unwrap(),
                "status reads must not execute or append facts"
            );
        }
        for (prior, _) in &saved {
            assert_eq!(
                fs::read_to_string(
                    directory
                        .join("workspace")
                        .join(prior["path"].as_str().unwrap())
                )
                .unwrap(),
                prior["marker"]
            );
            let record = history(directory, prior);
            let context = record["context_messages"].to_string();
            assert!(
                context.contains(prior["marker"].as_str().unwrap())
                    && context.contains("tool_result")
            );
            assert!(
                !record
                    .to_string()
                    .contains(prior["forbidden_marker"].as_str().unwrap()),
                "cross-Session history: {record}"
            );
        }
    }
}

fn history(directory: &Path, case: &Value) -> Value {
    json!(
        FileSessionStore::new(directory.join("sessions"))
            .unwrap()
            .load(case["session_id"].as_str().unwrap())
            .unwrap()
    )
}

fn execution_events(directory: &Path, case: &Value) -> Vec<LedgerEvent> {
    let session = case["session_id"].as_str().unwrap();
    let turn = case["turn_id"].as_str().unwrap();
    let execution = format!("http-{}-{session}-{turn}", session.len());
    SqliteLedger::open(directory.join("ledger.sqlite"))
        .unwrap()
        .execution_events_after(&execution, 0)
        .unwrap()
}

fn verify_execution(directory: &Path, case: &Value, result: &Value) {
    let expected = &case["expected"];
    assert_eq!(result["session_id"], case["session_id"]);
    assert_eq!(result["turn_id"], case["turn_id"]);
    assert_eq!(result["state"], expected["state"], "{result}");
    assert_eq!(result["end_reason"], expected["end_reason"]);
    assert_eq!(result["execution_stopped"], true);
    assert_eq!(result["recovery_required"], false);
    assert!(result["pending_approval"].is_null());
    let steps = result["steps"].as_array().unwrap();
    assert!(steps.len() as u64 >= expected["minimum_steps"].as_u64().unwrap());
    let text = steps.last().unwrap()["content"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|block| block["type"] == "text")
        .map(|block| block["text"].as_str().unwrap())
        .collect::<String>();
    assert!(text.contains(case["marker"].as_str().unwrap()), "{result}");
    assert!(
        !result
            .to_string()
            .contains(case["forbidden_marker"].as_str().unwrap()),
        "cross-execution HTTP result: {result}"
    );
    let events = execution_events(directory, case);
    let mut trace =
        fs::File::create(directory.join(format!("{}.jsonl", case["name"].as_str().unwrap())))
            .unwrap();
    for event in &events {
        writeln!(trace, "{}", serde_json::to_string(event).unwrap()).unwrap();
        assert!(
            !event
                .payload
                .to_string()
                .contains(case["forbidden_marker"].as_str().unwrap()),
            "cross-execution trajectory: {event:?}"
        );
        assert_eq!(event.turn_id, case["turn_id"].as_str().unwrap());
        let session = case["session_id"].as_str().unwrap();
        assert_eq!(
            event.execution_id,
            format!(
                "http-{}-{session}-{}",
                session.len(),
                case["turn_id"].as_str().unwrap()
            )
        );
    }
    let calls = events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::ToolCallRequested)
        .collect::<Vec<_>>();
    let receipts = events
        .iter()
        .filter(|event| event.kind == LedgerEventKind::EffectReceipt)
        .collect::<Vec<_>>();
    assert_eq!(calls.len() as u64, expected["calls"]);
    assert_eq!(receipts.len() as u64, expected["receipts"]);
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == LedgerEventKind::EffectStarted)
            .count(),
        receipts.len()
    );
    assert_eq!(
        result["tool_results"].as_array().unwrap().len(),
        receipts.len()
    );
    for call in &calls {
        assert_eq!(call.payload["name"], expected["tool"]);
        assert_eq!(call.payload["arguments"]["path"], case["path"]);
        if case["approve"] == true {
            assert_eq!(call.payload["arguments"]["content"], case["marker"]);
        }
        // The receipt must come from this execution's actual model-generated call.
        assert!(
            steps.iter().any(
                |step| step["content"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|block| block["type"] == "tool_call"
                        && block["id"] == call.payload["call_id"]
                        && block["name"] == call.payload["name"]
                        && block["arguments"] == call.payload["arguments"])
            )
        );
        let receipt = receipts
            .iter()
            .find(|event| event.payload["output"]["call_id"] == call.payload["call_id"])
            .unwrap();
        assert_eq!(
            receipt.payload["input"],
            json!({"name":call.payload["name"],"arguments":call.payload["arguments"]})
        );
        assert_eq!(receipt.payload["receipt"]["status"], "Completed");
        assert_eq!(receipt.payload["output"]["is_error"], false);
        assert!(
            result["tool_results"]
                .as_array()
                .unwrap()
                .contains(&receipt.payload["output"])
        );
        if case["verify_context"] == true {
            assert!(
                receipt.payload["output"]
                    .to_string()
                    .contains(case["marker"].as_str().unwrap())
            );
        }
        assert!(
            events
                .iter()
                .any(|event| event.kind == LedgerEventKind::ModelRequested
                    && event.cursor > receipt.cursor
                    && event.payload["request"]["messages"]
                        .to_string()
                        .contains(&receipt.payload["output"].to_string())),
            "fresh result must reach a subsequent model request"
        );
    }
    if case["verify_context"] == true {
        let request = events
            .iter()
            .find(|event| event.kind == LedgerEventKind::ModelRequested)
            .unwrap();
        let messages = request.payload["request"]["messages"].to_string();
        assert!(
            messages.contains(case["marker"].as_str().unwrap())
                && messages.contains("tool_call")
                && messages.contains("tool_result"),
            "missing first Session history: {messages}"
        );
    }
}

#[test]
fn ledger_live_fixture_plans_every_configured_combination() {
    let data = fixture();
    assert_eq!(
        configured_entries().len() as u64,
        data["configured_combinations"]
    );
    let cases = data["groups"][0]["cases"].as_array().unwrap();
    assert_eq!(cases.len(), 3);
    assert_eq!(cases[0]["session_id"], cases[2]["session_id"]);
    assert_ne!(cases[0]["session_id"], cases[1]["session_id"]);
    assert_eq!(cases[2]["expected"]["tool"], "file.read");
    for group in data["groups"].as_array().unwrap() {
        let system = group["system"].to_string();
        for case in group["cases"].as_array().unwrap() {
            assert!(
                !system.contains(case["forbidden_marker"].as_str().unwrap()),
                "shared system contains a foreign marker for {}",
                case["name"]
            );
        }
    }
}
