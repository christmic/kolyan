//! Additive approval/reconstruction network gate; original scenarios remain unchanged.
use super::*;
use std::collections::BTreeSet;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    family: String,
    surfaces: Vec<String>,
    local_backends: Vec<String>,
    network_backends: Vec<String>,
    evidence_scope: String,
    cases: Vec<Expectation>,
}

#[derive(Deserialize, serde::Serialize)]
#[serde(deny_unknown_fields)]
struct Expectation {
    case_id: String,
    named: bool,
    approval: bool,
    existing_target: bool,
    status: String,
    verdict: String,
    receipts: usize,
}

#[derive(Clone, Copy)]
enum Mode {
    LocalScript,
    ActualProviders,
}

#[tokio::test]
async fn approval_live_dataset_with_actual_local_workers() {
    run(Mode::LocalScript).await;
}

#[tokio::test]
#[ignore = "actual MiniMax both protocols: approval/rebuilt host, existing-target writes and goal closure"]
async fn actual_minimax_root_approval_and_replacement_goals() {
    run(Mode::ActualProviders).await;
}

async fn run(mode: Mode) {
    let plan: Plan = serde_json::from_str(include_str!("live_approvals.json")).unwrap();
    let base: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let deployments: Vec<_> = match mode {
        Mode::LocalScript => vec![],
        Mode::ActualProviders => super::super::deployments()
            .into_iter()
            .filter(|d| d.family == plan.family)
            .collect(),
    };
    let selected: Vec<Option<&super::super::Deployment>> = match mode {
        Mode::LocalScript => vec![None],
        Mode::ActualProviders => deployments.iter().map(Some).collect(),
    };
    let backends = match mode {
        Mode::LocalScript => &plan.local_backends,
        Mode::ActualProviders => &plan.network_backends,
    };
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for deployment in &selected {
        for backend in backends {
            for expected in &plan.cases {
                let mut row = json!({"case":expected.case_id,"backend":backend,"expectation":expected,"evidence_scope":plan.evidence_scope,
                    "deployment":deployment.map(|d|json!({"family":d.family,"surface":d.surface,"model":d.model})),
                    "provider_source":if deployment.is_some(){"actual_network"}else{"local_script"}});
                let result = match base.cases.iter().find(|c| c.id == expected.case_id) {
                    Some(case) => {
                        row["fixture"] = case_to_value(case);
                        scenario(case, backend, &installation, &mut row, *deployment).await
                    }
                    None => Err("referenced original scenario does not exist".into()),
                };
                row["scenario_error"] = json!(result.err());
                writeln!(export, "{row}").unwrap();
            }
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("RUNNER_APPROVAL_GOALS_TRACE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(
        rows.len(),
        selected.len() * backends.len() * plan.cases.len()
    );
    if let Mode::ActualProviders = mode {
        let actual: BTreeSet<_> = deployments.iter().map(|d| d.surface).collect();
        let expected: BTreeSet<_> = plan.surfaces.iter().map(String::as_str).collect();
        assert_eq!(actual, expected);
    }
    for row in &rows {
        let expected = plan
            .cases
            .iter()
            .find(|c| c.case_id == row["case"])
            .unwrap();
        assert!(row["scenario_error"].is_null(), "{row}");
        assert_eq!(row["fixture"]["named"], expected.named);
        assert_eq!(row["fixture"]["approval"], expected.approval);
        assert_eq!(
            !row["fixture"]["old_content"].is_null(),
            expected.existing_target
        );
        assert_eq!(row["final"]["status"], expected.status, "{row}");
        let assessments = row["final"]["task"]["goal_assessments"].as_array().unwrap();
        assert_eq!(assessments.len(), 1);
        assert_eq!(assessments[0]["assessment"]["verdict"], expected.verdict);
        assert_eq!(row["bytes"], json!(b"goal\n".as_slice()));
        assert_eq!(row["repeat"], row["final"]);
        assert_eq!(row["facts"], row["before_finalize_facts"]);
        assert_eq!(row["events"], row["before_finalize_events"]);
        assert_eq!(row["requests"], row["before_finalize_requests"]);
        let events = row["events"].as_array().unwrap();
        assert_eq!(
            events
                .iter()
                .filter(|e| e["kind"] == "effect_receipt")
                .count(),
            expected.receipts
        );
        let proof = &assessments[0]["assessment"]["proof"];
        let coordinates = proof["coordinates"].as_array().unwrap();
        assert_eq!(coordinates.len(), 6);
        for coordinate in coordinates {
            assert!(
                events
                    .iter()
                    .any(|e| e["event_id"] == coordinate["event_id"]
                        && e["cursor"] == coordinate["cursor"])
            );
        }
        assert!(
            coordinates
                .windows(2)
                .all(|pair| pair[0]["cursor"].as_u64().unwrap()
                    < pair[1]["cursor"].as_u64().unwrap())
        );
        for field in ["prepared_digest", "tool_result_digest"] {
            assert_eq!(proof[field].as_str().unwrap().len(), 64);
        }
        if expected.approval {
            assert_eq!(row["initial"]["task"]["state"], "Waiting");
            assert_eq!(
                row["initial"]["task"]["invocations"]["root"]["state"],
                "Suspended"
            );
            assert!(row["before_resume_bytes"].is_null());
            assert!(
                !row["before_resume_events"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|e| matches!(
                        e["kind"].as_str(),
                        Some("effect_started" | "effect_receipt")
                    ))
            );
            assert_eq!(row["resumed"]["task"]["state"], expected.status);
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e["kind"] == "approval_resolved")
                    .count(),
                1
            );
        }
    }
}
