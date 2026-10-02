//! Dataset/entrypoint inventory only. Never claims model or native cancellation execution.

use std::collections::BTreeSet;

use serde_json::{Value, json};

use super::super::super::{data, evidence::Evidence};

fn fixture(text: &str) -> Value {
    serde_json::from_str(text).unwrap()
}

#[test]
fn fresh_matrix_inventory_matches_declared_entrypoints_and_full_long_graph_axes() {
    let plan = fixture(include_str!(
        "../../../fixtures/agent/affected_matrix_plan.json"
    ));
    let deployments = super::super::super::deployments();
    let root = data::dataset();
    let long = fixture(include_str!("../../../fixtures/agent/long_task.json"));
    let finalization = fixture(include_str!(
        "../../../fixtures/agent/long_task_runner_finalization.json"
    ));
    let expected =
        fixture(include_str!("../../../expected/agent/long_task_runner_finalization.jsonl").trim());
    let delegation = fixture(include_str!("../../../fixtures/agent/delegation.json"));
    let parallel = fixture(include_str!(
        "../../../fixtures/agent/parallel_runtime.json"
    ));
    let topology = fixture(include_str!("../../../fixtures/agent/topology.json"));
    let native = fixture(include_str!("../../../fixtures/agent/native_effects.json"));
    let directory = tempfile::Builder::new()
        .prefix("kolyan-offline-coverage-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&directory.join("actual.jsonl"));
    println!(
        "COVERAGE_AUDIT_TRACE={}",
        directory.join("actual.jsonl").display()
    );
    let mut observed = Vec::new();
    for entry in plan["matrices"].as_array().unwrap() {
        let filter = entry["filter"].as_str().unwrap();
        let (source, axes, prefix) = match filter {
            "actual_model_root_matrix" => (
                include_str!("../../../agent/root.rs"),
                root.selectors.clone(),
                "root",
            ),
            "native_effects::actual_model_native_effects_matrix" => (
                include_str!("../../../agent/native_effects.rs"),
                ids(&fixture(include_str!("../../../fixtures/agent/native_effects.json"))["cases"]),
                "native-effects",
            ),
            "actual_model_root_approval_restart_matrix" => (
                include_str!("../../../agent/root.rs"),
                root.selectors.clone(),
                "restart",
            ),
            "delegation::actual_model_delegation_matrix" => (
                include_str!("../../delegation.rs"),
                delegation["cases"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .filter(|case| case["live"] == true)
                    .map(|case| case["id"].as_str().unwrap().into())
                    .collect(),
                "delegation",
            ),
            "delegation::parallel::actual_model_parallel_and_writable_serial_matrix" => (
                include_str!("../parallel.rs"),
                ids(&parallel["cases"]),
                "scheduling",
            ),
            "delegation::topology::actual_model_recursive_self_and_multiple_child_approvals_matrix" => {
                (include_str!("../topology.rs"), ids(&topology), "topology")
            }
            "actual_model_long_task_matrix" => (
                include_str!("../../../agent/long_task.rs"),
                strings(&long["selectors"]),
                "long",
            ),
            "continuation::actual_model_long_task_continuation_matrix" => (
                include_str!("../../../agent/long_task/continuation.rs"),
                strings(&long["selectors"]),
                "continuation",
            ),
            "continuation::projection::positive_projection_actual_model_matrix" => (
                include_str!("../../../agent/long_task/continuation/projection.rs"),
                strings(&long["selectors"]),
                "projection",
            ),
            "continuation::finalization::runner_finalized_long_graph_actual_model_matrices" => {
                let axes = finalization["selections"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .flat_map(|selection| {
                        let projection = match selection.as_str().unwrap() {
                            "full" => false,
                            "explicit_projection" => true,
                            other => panic!("unknown graph selection {other}"),
                        };
                        strings(&finalization["selectors"])
                            .into_iter()
                            .map(move |selector| format!("{projection}/{selector}"))
                    })
                    .collect();
                (
                    include_str!("../../../agent/long_task/continuation/finalization.rs"),
                    axes,
                    "runner-finalized",
                )
            }
            other => panic!("unmapped real entrypoint {other}"),
        };
        let entry_exists = source.contains(&format!(
            "async fn {}(",
            filter.rsplit("::").next().unwrap()
        ));
        let label_literal = match prefix {
            "root" => "agent/root/live/{}/{}/{selector}",
            "restart" => "agent/root/restart/live/{}/{}/{}/{selector}",
            "delegation" => "agent/delegation/{}/{}/{}/{}",
            "native-effects" => "agent/native-effects/{}/{}/{}/{}",
            "scheduling" => "agent/scheduling/{}/{}/{}/{}",
            "topology" => "agent/topology/{}/{}/{}/{}",
            "long" => "long/live/{}/{selector}",
            "continuation" => "continuation/{}/named",
            "projection" => "projection/{}/named",
            "runner-finalized" => "runner-finalized/{projection}/{}/named",
            other => panic!("unknown entrypoint label scope {other}"),
        };
        let label_source_matches = source.contains(label_literal);
        let mut labels = Vec::new();
        for deployment in &deployments {
            let protocol = match deployment.surface {
                "openai_compat" => "openai",
                "anthropic_compat" => "anthropic",
                other => panic!("unknown surface {other}"),
            };
            let identity = format!(
                "{}/{}/{}",
                deployment.family, deployment.surface, deployment.model
            );
            let long_identity = format!("{}/{protocol}/{}", deployment.family, deployment.model);
            for axis in &axes {
                labels.push(match prefix {
                    "root" => format!(
                        "agent/root/live/{}/{}/{axis}/{}",
                        deployment.family, deployment.surface, deployment.model
                    ),
                    "restart" => format!("agent/root/restart/live/{identity}/{axis}"),
                    "delegation" | "scheduling" | "topology" | "native-effects" => {
                        format!("agent/{prefix}/{identity}/{axis}")
                    }
                    "long" => format!("long/live/{long_identity}/{axis}"),
                    "runner-finalized" => {
                        let (projection, selector) = axis.split_once('/').unwrap();
                        format!("runner-finalized/{projection}/{long_identity}/{selector}")
                    }
                    _ => format!("{prefix}/{long_identity}/{axis}"),
                });
            }
        }
        let turn_ids: Vec<Value> = match prefix {
            "root" | "restart" => root.turns.iter().map(|turn| json!(turn.id)).collect(),
            "long" | "continuation" | "projection" | "runner-finalized" => long["turns"]
                .as_array()
                .unwrap()
                .iter()
                .map(|turn| turn["id"].clone())
                .collect(),
            _ => Vec::new(),
        };
        evidence.append(json!({"event":"matrix_offline_inventory","entry":entry,"declared_entry_exists":entry_exists,"label_source_matches":label_source_matches,"actual_label_format":labels,"axes":axes,"turn_ids":turn_ids,"projection_axis_mapping":{"false":"full","true":"explicit_projection"},"coverage":"declaration_alignment_only","network_executed":false})).unwrap();
        observed.push((
            entry.clone(),
            entry_exists,
            label_source_matches,
            axes.len(),
            labels,
        ));
    }
    for (entry, exists, label_matches, axes, labels) in observed {
        if entry["filter"] == "native_effects::actual_model_native_effects_matrix" {
            assert_eq!(entry["case_ids"], json!(ids(&native["cases"])));
            assert_eq!(
                entry["total_rows"],
                deployments.len() * native["cases"].as_array().unwrap().len()
            );
            assert_eq!(entry["attempts_per_row"], 1);
        }
        assert!(exists, "missing entrypoint {}", entry["filter"]);
        assert!(label_matches, "label drift in {}", entry["filter"]);
        assert_eq!(axes as u64, entry["rows_per_deployment"].as_u64().unwrap());
        assert_eq!(labels.len(), deployments.len() * axes);
        assert_eq!(labels.iter().collect::<BTreeSet<_>>().len(), labels.len());
    }
    assert_eq!(
        deployments.len() as u64,
        plan["configured_deployments"].as_u64().unwrap()
    );
    assert_eq!(root.selectors, ["named", "inline"]);
    assert_eq!(long["selectors"], json!(["named", "inline"]));
    assert_eq!(long["turns"].as_array().unwrap().len(), 10);
    assert_eq!(
        finalization["selections"],
        json!(["full", "explicit_projection"])
    );
    assert_eq!(expected["invocations"], 10);
    assert_eq!(expected["continuations"], 9);
    assert_eq!(plan["fresh_post_migration_topology_required"], true);
    assert_eq!(plan["old_topology_snapshot_restart_allowed"], false);
    let complete_axes = plan["matrices"]
        .as_array()
        .unwrap()
        .iter()
        .map(|entry| entry["rows_per_deployment"].as_u64().unwrap())
        .sum::<u64>();
    assert_eq!(complete_axes, 25);
    assert_eq!(
        plan["fresh_base_allocation"]["complete_inventory_rows_with_native_addition"],
        complete_axes * deployments.len() as u64
    );
    assert_eq!(
        plan["fresh_base_allocation"]["main_requested_rows_with_native_addition"],
        plan["fresh_base_allocation"]["main_requested_rows"]
            .as_u64()
            .unwrap()
            + 57
    );
    assert_eq!(
        plan["fresh_base_allocation"]["main_scenarios_per_deployment_with_native_addition"],
        23
    );
}

fn strings(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item.as_str().unwrap().into())
        .collect()
}
fn ids(value: &Value) -> Vec<String> {
    value
        .as_array()
        .unwrap()
        .iter()
        .map(|item| item["id"].as_str().unwrap().into())
        .collect()
}

#[test]
fn native_cancel_resume_inventory_distinguishes_scope_and_unexecuted_gaps() {
    let dataset = fixture(include_str!(
        "../../../fixtures/agent/native_cancel_resume_coverage.json"
    ));
    let directory = tempfile::Builder::new()
        .prefix("kolyan-native-coverage-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&directory.join("actual.jsonl"));
    println!(
        "NATIVE_COVERAGE_TRACE={}",
        directory.join("actual.jsonl").display()
    );
    let mut observations = Vec::new();
    for case in dataset["cases"].as_array().unwrap() {
        let declared = match case["declaration"].as_str() {
            Some("tests/agent/restart/worker_pin.rs") => {
                Some(include_str!("../../../agent/restart/worker_pin.rs"))
            }
            Some("tests/agent/root.rs") => Some(include_str!("../../../agent/root.rs")),
            Some("tests/agent/native_effects.rs") => {
                Some(include_str!("../../../agent/native_effects.rs"))
            }
            Some("../kolyan-sandbox/src/tests.rs") => {
                Some(include_str!("../../../../../kolyan-sandbox/src/tests.rs"))
            }
            Some("tests/server/task_cancellation.rs") => {
                Some(include_str!("../../../server/task_cancellation.rs"))
            }
            None => None,
            other => panic!("unmapped native coverage reference {other:?}"),
        };
        let found = declared.is_some_and(|text| text.contains(case["anchor"].as_str().unwrap()));
        evidence.append(json!({"event":"native_coverage_inventory","case":case,"declaration_found":found,"executed_by_this_audit":false,"actual_model_acceptance":false})).unwrap();
        observations.push((case.clone(), found));
    }
    for (case, found) in observations {
        assert_eq!(found, case["coverage"] != "required_gap", "{}", case["id"]);
    }
}
