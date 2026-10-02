//! Deterministic SQLite service governance, not live Provider evidence.
mod task_input_source;

use std::{
    fs,
    io::Write,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

use kolyan_core::{TurnConfig, TurnExecutor, TurnRequest};
use kolyan_ledger::{FactJournal, LedgerStore, SqliteFactJournal, SqliteLedger};
use kolyan_model::{
    ContentBlock, ModelEvent, ModelEventStream, ModelProvider, ModelRequest, ModelResponse,
    ProviderFuture,
};
use kolyan_policy::{ApprovalMode, PathScope, PolicyEngine};
use kolyan_runtime::DurableTurnResult;
use kolyan_server::*;
use kolyan_storage::FileSessionStore;
use kolyan_tools::{PolicyEnforcingTool, RestrictedFileTool};
use kolyan_trace::{ArtifactStore, NoopTraceSink, Retention};
use serde_json::{Value, json};

type Service =
    TaskExecutionService<SqliteFactJournal, SqliteLedger, NoopTraceSink, FileSessionStore>;

#[derive(Clone)]
struct Scripted {
    case: Value,
    response: ModelResponse,
    calls: Arc<AtomicUsize>,
}

impl ModelProvider for Scripted {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let observed = request
            .messages
            .iter()
            .flat_map(|m| &m.content)
            .any(|block| matches!(block, ContentBlock::ToolResult { .. }));
        let mut response = self.response.clone();
        if !self.case["calls"].as_array().unwrap().is_empty() && !observed {
            response.content = serde_json::from_value(Value::Array(
                self.case["calls"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .map(|call| json!({"type":"tool_call","call":call}))
                    .collect(),
            ))
            .unwrap();
            response.stop_reason = kolyan_model::StopReason::ToolUse;
        }
        Box::pin(async move {
            Ok(
                Box::pin(futures_util::stream::iter(vec![Ok(ModelEvent::Completed(
                    response,
                ))])) as ModelEventStream,
            )
        })
    }
}

fn service(root: &Path, data: &Value) -> Service {
    TaskExecutionService::new(
        TaskCoordinator::new(SqliteFactJournal::open(root.join("ledger.sqlite")).unwrap()),
        SessionExecutionService::new(
            ExecutionService::new(
                SqliteLedger::open(root.join("ledger.sqlite")).unwrap(),
                NoopTraceSink,
            ),
            SessionService::new(FileSessionStore::new(root.join("sessions")).unwrap()),
        ),
    )
    .with_artifacts(
        ArtifactStore::new(
            root.join("artifacts"),
            data["artifact_max_bytes"].as_u64().unwrap(),
        )
        .unwrap(),
    )
}

fn executor(
    provider: Scripted,
    root: &Path,
    data: &Value,
) -> TurnExecutor<Scripted, PolicyEnforcingTool<RestrictedFileTool, PolicyEngine>> {
    let scope = root
        .join("workspace")
        .join(data["workspace_scope"].as_str().unwrap())
        .to_string_lossy()
        .into_owned();
    let mut policy = PolicyEngine::default();
    for mut manifest in RestrictedFileTool::tool_manifests() {
        manifest.path_scopes = vec![PathScope::new(&scope)];
        if manifest.tool_name == "file.write" {
            manifest.approval = ApprovalMode::Always;
        }
        policy.register(manifest);
    }
    policy.restrict_workspace(scope);
    let policy = Arc::new(policy);
    TurnExecutor::with_tools(
        provider,
        PolicyEnforcingTool::new(
            RestrictedFileTool::new(root.join("workspace")),
            policy.clone(),
        ),
    )
    .with_policy_engine(policy)
}

fn binding(root: &Path, data: &Value, inv: &Value) -> AttemptBinding {
    let input_source = service(root, data)
        .coordinator()
        .snapshot(task(data))
        .unwrap()
        .invocations[inv["definition"]["invocation_id"].as_str().unwrap()]
    .definition
    .input_source
    .clone();
    serde_json::from_value(json!({
        "attempt_id":inv["attempt_id"],"invocation_id":inv["definition"]["invocation_id"],
        "execution":inv["execution"],"agent":data["task"]["agent"],"constraints_digest":data["task"]["constraints_digest"],
        "input_source":input_source
    })).unwrap()
}

fn provider(data: &Value, inv: &Value, usage: &Value, calls: Arc<AtomicUsize>) -> Scripted {
    let mut response = data["response"].clone();
    response["usage"] = usage.clone();
    Scripted {
        case: inv.clone(),
        response: serde_json::from_value(response).unwrap(),
        calls,
    }
}

fn request(data: &Value, inv: &Value) -> TurnRequest {
    let mut model_request = data["request"].clone();
    model_request["request_id"] = inv["execution"]["turn_id"].clone();
    model_request["tools"] = serde_json::to_value(RestrictedFileTool::tool_definitions()).unwrap();
    TurnRequest {
        turn_id: inv["execution"]["turn_id"].as_str().unwrap().into(),
        model_request: serde_json::from_value(model_request).unwrap(),
        config: TurnConfig {
            max_steps: data["turn_max_steps"].as_u64().unwrap() as usize,
            ..Default::default()
        },
    }
}

fn setup(root: &Path, data: &Value, case: &Value) -> Service {
    fs::create_dir_all(
        root.join("workspace")
            .join(data["workspace_scope"].as_str().unwrap()),
    )
    .unwrap();
    let current = service(root, data);
    let mut definition = data["task"].clone();
    definition["cancellation_policy"] = case["policy"].clone();
    if !case["max_tokens"].is_null() {
        definition["limits"]["max_tokens"] = case["max_tokens"].clone();
    }
    current
        .coordinator()
        .register_task(
            data["register_fact"].as_str().unwrap(),
            serde_json::from_value(definition).unwrap(),
        )
        .unwrap();
    for id in case["invocations"].as_array().unwrap() {
        let inv = &data["invocations"][id.as_str().unwrap()];
        let mut definition = inv["definition"].clone();
        definition["agent"] = data["task"]["agent"].clone();
        definition["constraints_digest"] = data["task"]["constraints_digest"].clone();
        definition["input_source"] = serde_json::to_value(task_input_source::publish(
            current.coordinator(),
            task(data),
            &definition,
            request(data, inv).model_request,
        ))
        .unwrap();
        current
            .coordinator()
            .admit_invocation(
                task(data),
                inv["admit_fact"].as_str().unwrap(),
                serde_json::from_value(definition).unwrap(),
            )
            .unwrap();
        current
            .sessions()
            .sessions()
            .create(inv["execution"]["session_id"].as_str().unwrap())
            .unwrap();
    }
    current
}

fn task(data: &Value) -> &str {
    data["task"]["task_id"].as_str().unwrap()
}

fn evidence(root: &Path, data: &Value, label: &str) -> (Value, Value) {
    let journal = SqliteFactJournal::open(root.join("ledger.sqlite")).unwrap();
    task_input_source::export(
        &TaskCoordinator::new(journal.clone()),
        task(data),
        &root.join(format!("{label}-input-sources.jsonl")),
    )
    .unwrap();
    let facts = journal.read(task(data), 0, 1024).unwrap();
    let ledger = SqliteLedger::open(root.join("ledger.sqlite")).unwrap();
    let events = ledger.events_after(0).unwrap();
    for (suffix, records) in [
        ("journal", serde_json::to_value(&facts).unwrap()),
        ("execution", serde_json::to_value(&events).unwrap()),
    ] {
        let mut file = fs::File::create(root.join(format!("{label}-{suffix}.jsonl"))).unwrap();
        for record in records.as_array().unwrap() {
            writeln!(file, "{}", serde_json::to_string(record).unwrap()).unwrap();
        }
        file.sync_all().unwrap();
    }
    let snapshot = service(root, data)
        .coordinator()
        .snapshot(task(data))
        .unwrap();
    fs::write(
        root.join(format!("{label}-state.json")),
        serde_json::to_vec_pretty(&snapshot).unwrap(),
    )
    .unwrap();
    (
        serde_json::to_value(facts).unwrap(),
        serde_json::to_value(events).unwrap(),
    )
}

#[tokio::test]
async fn deterministic_task_service_governance_from_fixture() {
    let data: Value =
        serde_json::from_str(include_str!("../fixtures/task_governance.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-task-governance-")
        .tempdir()
        .unwrap()
        .keep();
    eprintln!("Deterministic governance evidence: {}", root.display());
    for case in data["cases"].as_array().unwrap() {
        let name = case["name"].as_str().unwrap();
        assert!(!name.is_empty() && name.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_'));
        let directory = root.join(name);
        fs::create_dir_all(&directory).unwrap();
        match case["scenario"].as_str().unwrap() {
            "approval" => approvals(&directory, &data, case).await,
            "artifact" => artifacts(&directory, &data, case).await,
            "quota" => quotas(&directory, &data, case).await,
            other => panic!("unknown governance scenario {other}"),
        }
    }
}

async fn approvals(root: &Path, data: &Value, case: &Value) {
    let current = setup(root, data, case);
    let mut approvals = std::collections::BTreeMap::new();
    for id in case["invocations"].as_array().unwrap() {
        let id = id.as_str().unwrap();
        let inv = &data["invocations"][id];
        let result = current
            .run(
                task(data),
                binding(root, data, inv),
                executor(
                    provider(data, inv, &data["response"]["usage"], Arc::default()),
                    root,
                    data,
                ),
                request(data, inv),
            )
            .await;
        evidence(root, data, &format!("suspend-{id}"));
        let (_, result) = result.unwrap();
        let DurableTurnResult::Suspended { suspension, .. } = result else {
            panic!("expected approval");
        };
        let approval = suspension.waiting.approvals.first().unwrap();
        assert!(
            !root
                .join("workspace")
                .join(inv["path"].as_str().unwrap())
                .exists()
        );
        approvals.insert(id.to_owned(), approval.approval_id.clone());
    }
    let saved = current.coordinator().snapshot(task(data)).unwrap();
    drop(current);
    let rebuilt = service(root, data);
    evidence(root, data, "reconstructed");
    assert_eq!(rebuilt.coordinator().snapshot(task(data)).unwrap(), saved);
    for guard in case["guards"].as_array().unwrap() {
        let id = guard["invocation"].as_str().unwrap();
        let mut bound =
            serde_json::to_value(binding(root, data, &data["invocations"][id])).unwrap();
        for (key, value) in guard["patch"].as_object().unwrap() {
            bound[key] = value.clone();
        }
        let before = evidence(
            root,
            data,
            &format!("before-{}", guard["name"].as_str().unwrap()),
        );
        let result = rebuilt
            .resume_approval(
                task(data),
                serde_json::from_value(bound).unwrap(),
                &approvals[id],
                executor(
                    provider(
                        data,
                        &data["invocations"][id],
                        &data["response"]["usage"],
                        Arc::default(),
                    ),
                    root,
                    data,
                ),
            )
            .await;
        let after = evidence(
            root,
            data,
            &format!("after-{}", guard["name"].as_str().unwrap()),
        );
        assert!(result.is_err());
        assert_eq!(before, after);
        assert_eq!(rebuilt.coordinator().snapshot(task(data)).unwrap(), saved);
        assert!(
            !root
                .join("workspace")
                .join(data["invocations"][id]["path"].as_str().unwrap())
                .exists()
        );
    }
    if case["cancel"] == true {
        let result = rebuilt.cancel(
            task(data),
            case["cancel_fact"].as_str().unwrap(),
            case["cancel_reason"].as_str().unwrap(),
        );
        evidence(root, data, "cancelled");
        result.unwrap();
    }
    drop(rebuilt);
    for resume in case["resume"].as_array().unwrap() {
        let id = resume["invocation"].as_str().unwrap();
        let inv = &data["invocations"][id];
        let current = service(root, data);
        let before = evidence(root, data, &format!("before-resume-{id}"));
        let result = current
            .resume_approval(
                task(data),
                binding(root, data, inv),
                &approvals[id],
                executor(
                    provider(data, inv, &data["response"]["usage"], Arc::default()),
                    root,
                    data,
                ),
            )
            .await;
        let after = evidence(root, data, &format!("after-resume-{id}"));
        assert_eq!(
            result.is_ok(),
            resume["allowed"].as_bool().unwrap(),
            "{} {id}",
            case["name"]
        );
        match result {
            Err(_) => {
                assert_eq!(before, after);
                assert!(
                    !root
                        .join("workspace")
                        .join(inv["path"].as_str().unwrap())
                        .exists()
                );
            }
            Ok((snapshot, result)) => {
                assert!(matches!(result, DurableTurnResult::Completed(..)));
                assert_eq!(snapshot.invocations[id].state, InvocationState::Completed);
                assert_eq!(
                    fs::read_to_string(root.join("workspace").join(inv["path"].as_str().unwrap()))
                        .unwrap(),
                    inv["calls"][0]["arguments"]["content"].as_str().unwrap()
                );
                if let Some(parent) = resume["consume_by"].as_str() {
                    let completed = &snapshot.invocations[id];
                    let AttemptOutcome::Completed { evidence: proofs } = &snapshot.attempts
                        [inv["attempt_id"].as_str().unwrap()]
                    .observation
                    .as_ref()
                    .unwrap()
                    .outcome
                    else {
                        panic!("missing completion proof");
                    };
                    let result = current.coordinator().consume_child_result(
                        task(data),
                        resume["consume_fact"].as_str().unwrap(),
                        parent,
                        ConsumedResult {
                            child_invocation_id: id.into(),
                            completion_fact: completed.completion_fact.clone().unwrap(),
                            evidence: proofs.clone(),
                        },
                    );
                    evidence(root, data, &format!("consumed-{id}"));
                    result.unwrap();
                }
            }
        }
    }
    let snapshot = service(root, data)
        .coordinator()
        .snapshot(task(data))
        .unwrap();
    evidence(root, data, "final");
    assert_eq!(
        serde_json::to_value(snapshot.state).unwrap(),
        case["expected_state"]
    );
}

async fn artifacts(root: &Path, data: &Value, case: &Value) {
    let inv = &data["invocations"][case["invocation"].as_str().unwrap()];
    let scripted = provider(data, inv, &data["response"]["usage"], Arc::default());
    let bytes = serde_json::to_vec(&scripted.response).unwrap();
    let store = ArtifactStore::new(
        root.join("artifacts"),
        data["artifact_max_bytes"].as_u64().unwrap(),
    )
    .unwrap();
    let reference = store.put(&bytes, Retention::Required).unwrap();
    let mut specialized = data.clone();
    specialized["task"]["criteria"] = json!([{"ArtifactDigest":{"id":case["criterion_id"],"invocation_id":inv["definition"]["invocation_id"],"sha256":reference.digest}}]);
    let current = setup(root, &specialized, case);
    let result = current
        .run(
            task(data),
            binding(root, data, inv),
            executor(scripted, root, data),
            request(data, inv),
        )
        .await;
    evidence(root, data, "bound-response");
    let (snapshot, _) = result.unwrap();
    let observation = snapshot.attempts[inv["attempt_id"].as_str().unwrap()]
        .observation
        .as_ref()
        .unwrap();
    let AttemptOutcome::Completed { evidence: proofs } = &observation.outcome else {
        panic!("actual execution did not complete");
    };
    assert!(
        matches!(&proofs[0], CompletionEvidence::VerifiedArtifact { source, sha256, byte_len, .. } if source.execution == binding(root, data, inv).execution && sha256 == &reference.digest && *byte_len == bytes.len() as u64)
    );
    let path = root.join("artifacts").join(&reference.digest);
    match case["mutation"].as_str().unwrap() {
        "missing" => fs::rename(&path, root.join("saved-artifact")).unwrap(),
        "corrupt" => {
            let mut corrupted = bytes.clone();
            corrupted[0] ^= case["corrupt_mask"].as_u64().unwrap() as u8;
            fs::write(&path, corrupted).unwrap();
        }
        "intact" => {}
        other => panic!("unknown artifact mutation {other}"),
    }
    drop(current);
    let before = evidence(root, data, "before-complete");
    let result = service(root, data).complete(task(data), case["complete_fact"].as_str().unwrap());
    let after = evidence(root, data, "after-complete");
    assert_eq!(result.is_ok(), case["allowed"].as_bool().unwrap());
    if result.is_err() {
        assert_eq!(before, after);
    }
    let snapshot = service(root, data)
        .coordinator()
        .snapshot(task(data))
        .unwrap();
    assert_eq!(
        serde_json::to_value(snapshot.state).unwrap(),
        case["expected_state"]
    );
}

async fn quotas(root: &Path, data: &Value, case: &Value) {
    let current = setup(root, data, case);
    let first = &data["invocations"][case["first"].as_str().unwrap()];
    let calls = Arc::new(AtomicUsize::new(0));
    let result = current
        .run(
            task(data),
            binding(root, data, first),
            executor(
                provider(data, first, &case["usage"], calls.clone()),
                root,
                data,
            ),
            request(data, first),
        )
        .await;
    evidence(root, data, "quota-consumed");
    result.unwrap();
    drop(current);
    let blocked = &data["invocations"][case["blocked"].as_str().unwrap()];
    let before = evidence(root, data, "before-quota-guard");
    let result = service(root, data)
        .run(
            task(data),
            binding(root, data, blocked),
            executor(
                provider(data, blocked, &case["usage"], calls.clone()),
                root,
                data,
            ),
            request(data, blocked),
        )
        .await;
    let after = evidence(root, data, "after-quota-guard");
    assert!(
        result
            .unwrap_err()
            .to_string()
            .contains(case["expected_error"].as_str().unwrap())
    );
    assert_eq!(before, after);
    assert_eq!(
        calls.load(Ordering::SeqCst) as u64,
        case["expected_model_calls"].as_u64().unwrap()
    );
    let snapshot = service(root, data)
        .coordinator()
        .snapshot(task(data))
        .unwrap();
    assert_eq!(
        serde_json::to_value(snapshot.state).unwrap(),
        case["expected_state"]
    );
    assert_eq!(
        snapshot.usage.unreported_steps,
        case["expected_unreported_steps"].as_u64().unwrap()
    );
}
