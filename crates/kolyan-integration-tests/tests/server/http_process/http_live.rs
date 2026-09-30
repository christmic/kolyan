//! Real HTTP Provider matrix, driven by scenario fixtures.
use super::*;

#[path = "../../common/matrix.rs"]
#[allow(dead_code)]
pub(super) mod matrix;

#[tokio::test]
#[ignore = "actual HTTP processes and all configured Provider credentials"]
async fn http_process_live_matrix() {
    let configs = common::load_config();
    let mut entries = Vec::new();
    for (family, cfg) in [
        ("minimax", configs.minimax_openai),
        ("qwen", configs.qwen_openai),
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
        ("minimax", configs.minimax_anthropic),
        ("qwen", configs.qwen_anthropic),
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
    let fixture: Value =
        serde_json::from_str(include_str!("../../fixtures/server_http_live.json")).unwrap();
    let groups = fixture["groups"].as_array().unwrap();
    let planned = entries
        .into_iter()
        .flat_map(|entry| {
            groups
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
                    std::env::var(&key_env).is_ok_and(|key| !key.is_empty()),
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
                config["max_steps"] = json!(24);
                config["max_tool_calls"] = json!(24);
                config["progress"]["repeat_limit"] = json!(4);
                fs::write(&path, serde_json::to_vec_pretty(&config).unwrap()).unwrap();
                // Each case has real model-generated calls; no scripted actions.
                live_scenarios(&directory, group["cases"].as_array().unwrap()).await;
            })
            .await;
    }
    assert!(
        report.complete(),
        "HTTP live matrix failures: {}",
        report.directory.display()
    );
}

async fn live_scenarios(directory: &Path, cases: &[Value]) {
    let mut process = Process::start(directory);
    for case in cases {
        let session = case["session_id"].as_str().unwrap();
        let turn = case["turn_id"].as_str().unwrap();
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
        let resource = format!("/v1/sessions/{session}/turns/{turn}");
        let mut result = process
            .call(
                Method::POST,
                &format!("/v1/sessions/{session}/turns"),
                Some(json!({"turn_id":turn,"input":case["input"]})),
                200,
            )
            .await;
        if case["action"] != "none" {
            assert_eq!(result["state"], "suspended", "actual result: {result}");
            assert_eq!(result["pending_approval"]["tool_name"], "file.write");
            assert_eq!(
                result["pending_approval"]["arguments"]["path"],
                case["path"]
            );
            let approval = result["pending_approval"]["approval_id"]
                .as_str()
                .unwrap()
                .to_owned();
            assert!(
                !directory
                    .join("workspace")
                    .join(case["path"].as_str().unwrap())
                    .exists()
            );
            if case["restart"] == true {
                drop(process);
                process = Process::start(directory);
                assert_eq!(
                    process.call(Method::GET, &resource, None, 200).await,
                    result
                );
            }
            let action = case["action"].as_str().unwrap();
            let path = if action == "cancel" {
                format!("{resource}/cancel")
            } else {
                format!("{resource}/approvals/{approval}/decision")
            };
            result = process
                .call(
                    Method::POST,
                    &path,
                    Some(if action == "cancel" {
                        json!({})
                    } else {
                        json!({"decision":action})
                    }),
                    200,
                )
                .await;
        }
        assert_eq!(
            result["state"], case["expected_state"],
            "actual result: {result}"
        );
        assert_eq!(
            result["end_reason"], case["end_reason"],
            "actual result: {result}"
        );
        assert!(
            result["steps"].as_array().unwrap().len() as u64
                >= case["minimum_steps"].as_u64().unwrap()
        );
        assert_eq!(result["execution_stopped"], true);
        assert_eq!(
            process.call(Method::GET, &resource, None, 200).await,
            result
        );
        if let Some(text) = case["contains"].as_str() {
            let final_text = result["steps"].as_array().unwrap().last().unwrap()["content"]
                .as_array()
                .unwrap()
                .iter()
                .filter(|block| block["type"] == "text")
                .map(|block| block["text"].as_str().unwrap())
                .collect::<String>();
            assert!(final_text.contains(text), "actual result: {result}");
        }
        if let Some(path) = case["path"].as_str() {
            let target = directory.join("workspace").join(path);
            if let Some(content) = case["file_content"].as_str() {
                assert_eq!(fs::read_to_string(target).unwrap(), content);
            } else {
                assert!(!target.exists());
            }
        }
        let execution = format!("http-{}-{session}-{turn}", session.len());
        let events = SqliteLedger::open(directory.join("ledger.sqlite"))
            .unwrap()
            .events_after(0)
            .unwrap()
            .into_iter()
            .filter(|event| event.execution_id == execution)
            .collect::<Vec<_>>();
        let mut trace =
            fs::File::create(directory.join(format!("{}.jsonl", case["name"].as_str().unwrap())))
                .unwrap();
        for event in &events {
            writeln!(trace, "{}", serde_json::to_string(event).unwrap()).unwrap();
        }
        let receipts = events
            .iter()
            .filter(|event| event.kind == kolyan_ledger::LedgerEventKind::EffectReceipt)
            .count() as u64;
        assert!(
            receipts >= case["minimum_effects"].as_u64().unwrap()
                && receipts <= case["maximum_effects"].as_u64().unwrap(),
            "unexpected receipt count {receipts}"
        );
        let calls = events
            .iter()
            .filter(|event| event.kind == kolyan_ledger::LedgerEventKind::ToolCallRequested)
            .collect::<Vec<_>>();
        let allowed = case["allowed_tools"].as_array().unwrap();
        for call in &calls {
            assert!(
                allowed.contains(&call.payload["name"]),
                "unapproved tool class: {}",
                call.payload
            );
        }
        assert_eq!(
            calls
                .iter()
                .filter(|call| call.payload["name"] == "file.write")
                .count() as u64,
            case["write_calls"].as_u64().unwrap()
        );
        if receipts == 0 {
            assert!(
                !events
                    .iter()
                    .any(|event| event.kind == kolyan_ledger::LedgerEventKind::EffectStarted)
            );
        }
        if case["verify_context"] == true {
            let request = events
                .iter()
                .find(|event| event.kind == kolyan_ledger::LedgerEventKind::ModelRequested)
                .unwrap();
            let context = request.payload["request"]["messages"].to_string();
            assert!(
                context.contains("http-proof") && context.contains("tool_result"),
                "{context}"
            );
        }
    }
}
