use super::*;

#[test]
fn explicit_contract_data_guard_exports_then_rejects_non_task_changes() {
    let cases = prepared_cases();
    let data = contract();
    let (spec, _) = &cases[0];
    let (bytes, full) = baseline(&spec.baseline_scope);
    let original = full["cases"]
        .as_array()
        .unwrap()
        .iter()
        .find(|case| case["id"] == spec.baseline_case_id)
        .unwrap();
    let rows: Vec<_> = data
        .guard_cases
        .iter()
        .map(|guard| {
            let mut candidate = overlay(spec, original);
            if !guard.pointer.is_empty() {
                candidate.as_object_mut().unwrap().insert(
                    guard.pointer.strip_prefix('/').unwrap().into(),
                    guard.value.clone(),
                );
            }
            json!({"case":guard.id,"baseline":original,"candidate":candidate,
            "result":validate_overlay(spec,bytes,original,&candidate),"expected":guard.accepted})
        })
        .collect();
    let root = tempfile::Builder::new()
        .prefix("kolyan-explicit-contract-guards-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&rows).unwrap()).unwrap();
    println!("EXPLICIT_CONTRACT_GUARDS={}", path.display());
    let actual: Vec<Value> = serde_json::from_slice(&std::fs::read(path).unwrap()).unwrap();
    for row in actual {
        assert_eq!(
            row["result"].get("Ok").is_some(),
            row["expected"].as_bool().unwrap(),
            "{row}"
        );
    }
    let selected = crate::minimax_live::selected(
        "delegation",
        dataset()
            .cases
            .into_iter()
            .filter(|case| case.live)
            .map(|case| case.id)
            .collect(),
    );
    let mut inventory = Vec::new();
    for deployment in &selected {
        for (spec, _) in &cases {
            inventory.push(json!({"scope":spec.baseline_scope,"case":spec.case_id,"surface":deployment.surface,"model":deployment.model,"family":deployment.family}));
        }
    }
    let labels = live_labels(&selected, &cases);
    let path = root.join("live-inventory.json");
    std::fs::write(&path, serde_json::to_vec_pretty(&inventory).unwrap()).unwrap();
    let actual: Vec<Value> = serde_json::from_slice(&std::fs::read(&path).unwrap()).unwrap();
    assert_eq!(actual.len(), 16);
    assert_eq!(
        actual
            .iter()
            .map(|row| format!(
                "minimax/explicit-contract-v1/{}/{}/{}",
                row["surface"].as_str().unwrap(),
                row["model"].as_str().unwrap(),
                row["case"].as_str().unwrap()
            ))
            .collect::<Vec<_>>(),
        labels
    );
    assert_eq!(
        actual
            .iter()
            .map(|row| row["surface"].as_str().unwrap())
            .collect::<BTreeSet<_>>(),
        ["openai_compat", "anthropic_compat"].into()
    );
}

#[tokio::test]
async fn explicit_contract_offline_actual_runner_and_workers() {
    let cases = prepared_cases();
    let mut report = matrix::Matrix::new(cases.iter().map(|(spec, _)| spec.case_id.clone()));
    let installation = crate::tools::worker::WorkerRun::prepare().await;
    for (index, (spec, candidate)) in cases.iter().enumerate() {
        report
            .run(
                index,
                run_case(
                    spec,
                    candidate.clone(),
                    ModelRef::new("fixture", "explicit-contract"),
                    None,
                    &installation,
                ),
            )
            .await;
    }
    assert!(
        report.complete(),
        "Explicit contract OS evidence: {}",
        report.directory.display()
    );
}
