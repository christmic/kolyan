//! Explicit network entry; local OS acceptance never substitutes for this gate.
use super::*;

#[tokio::test]
#[ignore = "actual MiniMax two-protocol named/inline physical write goals; requires credentials"]
async fn actual_minimax_root_write_goals() {
    let data: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let deployments: Vec<_> = super::super::deployments()
        .into_iter()
        .filter(|d| d.family == data.network_plan["family"].as_str().unwrap())
        .collect();
    let cases: Vec<_> = data
        .cases
        .iter()
        .filter(|c| c.id == "named_committed" || c.id == "inline_committed")
        .collect();
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    let mut export = std::fs::File::create(&path).unwrap();
    for deployment in &deployments {
        for case in &cases {
            let mut row = json!({"family":deployment.family,"surface":deployment.surface,"model":deployment.model,"case":case.id,"fixture":case_to_value(case),"network_plan":data.network_plan});
            let result = scenario(case, "sqlite", &installation, &mut row, Some(deployment)).await;
            row["scenario_error"] = json!(result.err());
            writeln!(export, "{row}").unwrap();
        }
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    println!("RUNNER_NATIVE_GOALS_LIVE_TRACE={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    let actual_surfaces: std::collections::BTreeSet<_> =
        deployments.iter().map(|d| d.surface).collect();
    let expected_surfaces: std::collections::BTreeSet<_> = data.network_plan["surfaces"]
        .as_array()
        .unwrap()
        .iter()
        .map(|v| v.as_str().unwrap())
        .collect();
    assert_eq!(actual_surfaces, expected_surfaces);
    assert_eq!(cases.len(), 2);
    assert_eq!(rows.len(), deployments.len() * cases.len());
    for row in &rows {
        assert!(row["scenario_error"].is_null(), "{row}");
        assert_eq!(row["final"]["status"], "Completed", "{row}");
        assert_eq!(
            row["final"]["task"]["goal_assessments"][0]["assessment"]["verdict"], "Satisfied",
            "{row}"
        );
        assert_eq!(row["bytes"], json!(b"goal\n".as_slice()));
        assert_eq!(row["repeat"], row["final"]);
        assert_eq!(row["facts"], row["before_finalize_facts"]);
        assert_eq!(row["events"], row["before_finalize_events"]);
        assert_eq!(row["requests"], row["before_finalize_requests"]);
    }
}
