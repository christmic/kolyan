//! Guard projections are synthetic, never receipts or model-authored candidates.

use std::collections::BTreeMap;

use serde_json::{Value, json};

use super::*;
use crate::evidence::Evidence;

#[test]
fn correction_guard_dataset_exports_all_before_comparison() {
    let p = plan(super::super::tests::unit_plan().run.clone()).unwrap();
    let paths = p.allowlist.clone();
    let inventory=Value::Object(paths.iter().map(|s|(s.clone(),json!({"kind":"file","mode":420,"sha256":"a".repeat(64),"link_target_sha256":null}))).collect());
    let failed = json!({"event":"self_iteration_rejection_closure","task_id":"prior-failed-task","result":{"Ok":{"fact_id":"prior/host/rejected","outcome":"failed","state":"Failed"}},"execution_outcomes_synthesized":false});
    let checks=["server-full","server-focused","format","server-strict","server-build","oracle-compile","oracle-run"].map(|id|json!({"event":"self_iteration_host_validation","value":{"id":id,"exit_code":0,"timed_out":false,"output_overflow":false,"stdout":"synthetic bounded observation","stderr":""}}));
    let mut rows = vec![failed.clone()];
    rows.extend(checks);
    let config = Config {
        original_run_config: "/offline/original.local.json".into(),
        expected_current_inventory: inventory.clone(),
        candidate_sha256: paths
            .iter()
            .map(|p| (p.clone(), "a".repeat(64)))
            .collect::<BTreeMap<_, _>>(),
        prior_trace: "/offline/prior.jsonl".into(),
        prior_trace_sha256: "b".repeat(64),
        prior_task_id: "prior-failed-task".into(),
        prior_failed_fact: failed,
    };
    let dir = tempfile::tempdir().unwrap().keep();
    let path = dir.join("actual.jsonl");
    let evidence = Evidence::new(&path);
    for case in dataset().guard_cases {
        let mut c: Config = serde_json::from_value(serde_json::to_value(&config).unwrap()).unwrap();
        let mut source = rows.clone();
        let mut actual = inventory.clone();
        match case.mutation.as_str() {
            "valid" => {}
            "inventory" => {
                actual[&paths[0]]["mode"] = json!(493);
            }
            "candidate" => {
                c.candidate_sha256.insert(paths[0].clone(), "c".repeat(64));
            }
            "prior_failed_fact" => {
                c.prior_failed_fact["result"]["Ok"]["state"] = json!("Ready");
            }
            "prior_task" => {
                c.prior_task_id = p.task_id.clone();
            }
            "fixed_check" => {
                source[1]["value"]["exit_code"] = json!(1);
            }
            _ => panic!("unknown guard mutation"),
        }
        let actual = pins::verify(&c, &p, &actual, &source);
        evidence.append(json!({"event":"correction_guard","projection":"synthetic negative only","case":case,"config":c,"source":source,"actual":actual})).unwrap();
    }
    let stage = &p.stages[0];
    let mut request = super::super::super::driver::request(&p, &stage.id, stage.input.clone());
    let configured = super::super::input::configure(&p, stage, &mut request);
    evidence.append(json!({"event":"correction_request_contract","request":request,"configured":configured,"max_steps":p.max_steps,"max_calls":p.max_tool_calls,"stages":p.stages.len(),"new_task":p.task_id,"new_session":p.logical_session_id,"model_calls":0})).unwrap();
    drop(evidence);
    let read = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str::<Value>(l).unwrap())
        .collect::<Vec<_>>();
    println!("CORRECTION_GUARD_TRACE={}", path.display());
    assert_eq!(read.len(), dataset().guard_cases.len() + 1);
    for r in &read[..read.len() - 1] {
        assert_eq!(
            r["actual"].get("Ok").is_some(),
            r["case"]["expected_valid"].as_bool().unwrap()
        );
    }
    let r = read.last().unwrap();
    assert_eq!(r["stages"], 1);
    assert_eq!(r["max_steps"], 16);
    assert_eq!(r["max_calls"], 40);
    assert!(r["request"]["output_format"].is_null());
    assert!(
        !r["request"]["system"]
            .to_string()
            .contains("LOCAL review contract")
    );
    assert!(p.instructions.contains("BEFORE ANY comparison"));
}
