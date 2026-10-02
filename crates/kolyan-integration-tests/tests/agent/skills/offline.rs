//! Separate pure diagnostic and actual production Host/SDK/native matrices.
use super::{data::*, evidence::*};
use kolyan_ledger::FactRef;
use kolyan_model::{ContentBlock, Message, MessageRole, ToolResult};
use kolyan_trace::{ArtifactRef, Retention};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    fs::File,
    io::{BufRead, BufReader, Write},
};

#[derive(Debug, Serialize, Deserialize)]
#[serde(tag = "id", rename_all = "snake_case", deny_unknown_fields)]
enum Expected {
    SelectedR2NativeGoal {
        selected_revision: String,
        skill_receipts: usize,
        write_receipts: usize,
        read_receipts: usize,
        goal_verdict: String,
        task_status: String,
        payload_leaked_before_load: bool,
        load_body_exact_next_request: bool,
        minimum_model_steps: usize,
    },
    ApprovalRebuildAccept {
        skill_receipts: usize,
        write_before_decision: usize,
        write_receipts: usize,
        read_receipts: usize,
        new_instance_on_rebuild: bool,
        same_attempt_and_checkpoint: bool,
        goal_verdict: String,
        task_status: String,
    },
    ApprovalRebuildRevokedDeny {
        skill_receipts: usize,
        write_before_decision: usize,
        write_receipts: usize,
        new_model_openings_after_decision: usize,
        new_skill_reads_after_decision: usize,
        decision: String,
        approval_closed: bool,
        task_cancel_command: bool,
    },
    ApprovalRebuildAclChangedDeny {
        skill_receipts: usize,
        write_before_decision: usize,
        write_receipts: usize,
        new_model_openings_after_decision: usize,
        new_skill_reads_after_decision: usize,
        decision: String,
        approval_closed: bool,
        task_cancel_command: bool,
    },
    MaliciousKnowledgeNoAuthority {
        skill_receipts: usize,
        shell_receipts: usize,
        child_instances: usize,
        unauthorized_grants: usize,
        failed_calls_feedback_exact_next_request: bool,
        valid_write_receipts: usize,
        goal_verdict: String,
    },
    RevokedBeforeLoad {
        skill_receipts: usize,
        write_receipts: usize,
        new_model_openings_after_revocation: usize,
        authority_expanded: bool,
        version_reselected: bool,
        task_success_fact: bool,
    },
    RevokedBeforeNextOpening {
        skill_receipts: usize,
        write_receipts: usize,
        new_model_openings_after_revocation: usize,
        prior_load_receipt_preserved: bool,
        version_reselected: bool,
        task_success_fact: bool,
    },
}

#[test]
fn production_host_skills_localhost_native_all_eight_rows() {
    let dataset = dataset().unwrap();
    dataset.validate().unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-c-host-")
        .tempdir()
        .unwrap()
        .keep();
    let status = std::process::Command::new(std::env::current_exe().unwrap())
        .args([
            "--exact",
            "skills::offline::production_host_skills_fixture_process",
            "--ignored",
            "--nocapture",
        ])
        .env("KOLYAN_SKILLS_C_EVIDENCE", &root)
        .env(
            "KOLYAN_SKILLS_LOCAL_KEY",
            "localhost-fixture-no-real-credential",
        )
        .status()
        .unwrap();
    let path = root.join("actual.jsonl");
    println!("SKILLS_C_HOST_ACTUAL={}", path.display());
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    assert_eq!(rows.len(), dataset.offline.planned_rows);
    let failures: Vec<_> = rows
        .iter()
        .filter_map(|r| super::evidence::compare(&dataset, r).err())
        .collect();
    assert!(failures.is_empty(), "{}: {failures:#?}", path.display());
    assert!(status.success(), "fixture process exited {status}");
}

#[tokio::test]
#[ignore = "Local subprocess fixture only, isolated credential environment; not a real provider"]
async fn production_host_skills_fixture_process() {
    let dataset = dataset().unwrap();
    let root = std::path::PathBuf::from(std::env::var("KOLYAN_SKILLS_C_EVIDENCE").unwrap());
    let installation = super::super::tools::worker::WorkerRun::prepare().await;
    let mut output = File::create(root.join("actual.jsonl")).unwrap();
    for (protocol, id) in &dataset.offline.rows {
        let case = dataset.cases.iter().find(|c| &c.id == id).unwrap();
        let row = super::driver::scenario(
            &dataset,
            case,
            *protocol,
            Selector::Named,
            &root,
            &installation,
            None,
        )
        .await;
        writeln!(output, "{row}").unwrap();
        output.flush().unwrap();
        output.sync_data().unwrap();
    }
    output.sync_all().unwrap();
    drop(output);
}

#[test]
fn typed_skill_cases_and_diagnostic_witness_data_matrix() {
    let parsed = dataset().unwrap();
    let expectations: Vec<Expected> = include_str!("../../expected/agent/skills.jsonl")
        .lines()
        .map(|l| serde_json::from_str(l).unwrap())
        .collect();
    let root = tempfile::Builder::new()
        .prefix("kolyan-skills-c-framework-")
        .tempdir()
        .unwrap()
        .keep();
    let path = root.join("actual.jsonl");
    let mut output = File::create(&path).unwrap();
    writeln!(output,"{}",json!({"kind":"data_contract_not_host_execution","dataset":parsed,"expectations":expectations,
        "validation":parsed.validate(),"network_executed":false,"host_execution_rows":0})).unwrap();
    let mut unknown = serde_json::to_value(&parsed).unwrap();
    unknown["network"]["unknown"] = json!(true);
    let unknown_error = serde_json::from_value::<Dataset>(unknown.clone())
        .err()
        .map(|e| e.to_string());
    writeln!(
        output,
        "{}",
        json!({"kind":"unknown_dataset_field_not_execution","input":unknown,"error":unknown_error})
    )
    .unwrap();
    let skill = parsed.selected().unwrap();
    let known = BodyObservation {
        key: kolyan_agent::SkillKey::new(skill.id.clone(), skill.revision.clone()).unwrap(),
        content_digest: "0".repeat(64),
        registration: FactRef {
            stream_id: "synthetic.diagnostic.registration".into(),
            position: 1,
            fact_id: "synthetic.registration.1".into(),
        },
        binding: FactRef {
            stream_id: "synthetic.diagnostic.binding".into(),
            position: 1,
            fact_id: "synthetic.binding.1".into(),
        },
        artifact: ArtifactRef {
            digest: format!("{:x}", Sha256::digest(skill.body.as_bytes())),
            byte_length: skill.body.len() as u64,
            retention: Retention::Required,
        },
        body: skill.body.clone(),
    };
    for case in &parsed.evidence_cases {
        let original = ToolResult {
            call_id: "diagnostic-call".into(),
            content: serde_json::to_string(&known).unwrap(),
            is_error: false,
        };
        let mut result = original.clone();
        let mut payload = serde_json::to_value(&known).unwrap();
        match case.mutation {
            EvidenceMutation::Tail => {
                payload["body"] = json!(format!("{}\n", known.body));
            }
            EvidenceMutation::Error => result.is_error = true,
            EvidenceMutation::Digest => payload["artifact"]["digest"] = json!("f".repeat(64)),
            EvidenceMutation::Revision => payload["key"]["revision"] = json!("r1"),
            EvidenceMutation::Unknown => payload["unexpected"] = json!(true),
            EvidenceMutation::Retention => payload["artifact"]["retention"] = json!("optional"),
            EvidenceMutation::Outer => result.call_id = "x".repeat(512 * 1024),
            _ => {}
        }
        result.content = serde_json::to_string(&payload).unwrap();
        let mut next = result.clone();
        if matches!(case.mutation, EvidenceMutation::Call) {
            next.call_id = "foreign-call".into();
        }
        let messages = if matches!(case.mutation, EvidenceMutation::Absent) {
            vec![]
        } else {
            vec![Message {
                role: MessageRole::User,
                content: vec![ContentBlock::ToolResult { result: next }],
            }]
        };
        let actual = check_next_result(&messages, &result, &known);
        writeln!(output,"{}",json!({"kind":"synthetic_diagnostic_inputs_not_execution","fixture":case,"known":known,"original":original,"result":result,
            "next_messages":messages,"accepted":actual.is_ok(),"error":actual.err(),"authority_proven":false})).unwrap();
    }
    output.flush().unwrap();
    output.sync_all().unwrap();
    drop(output);
    println!("SKILLS_C_FRAMEWORK_ACTUAL={}", path.display());
    let rows: Vec<Value> = BufReader::new(File::open(path).unwrap())
        .lines()
        .map(|l| serde_json::from_str(&l.unwrap()).unwrap())
        .collect();
    assert_eq!(rows.len(), parsed.evidence_cases.len() + 2);
    assert_eq!(rows[0]["validation"], json!({"Ok":null}));
    assert_eq!(rows[0]["host_execution_rows"], 0);
    assert_eq!(rows[0]["network_executed"], false);
    let read_dataset: Dataset = serde_json::from_value(rows[0]["dataset"].clone()).unwrap();
    let read_expected: Vec<Expected> =
        serde_json::from_value(rows[0]["expectations"].clone()).unwrap();
    let expected_ids: BTreeSet<_> = rows[0]["expectations"]
        .as_array()
        .unwrap()
        .iter()
        .map(|e| e["id"].as_str().unwrap())
        .collect();
    assert_eq!(
        expected_ids,
        read_dataset.cases.iter().map(|c| c.id.as_str()).collect()
    );
    assert_eq!(read_expected.len(), read_dataset.cases.len());
    for expected in read_expected {
        if let Expected::SelectedR2NativeGoal {
            minimum_model_steps,
            ..
        } = expected
        {
            assert_eq!(minimum_model_steps, 3);
        }
    }
    assert!(rows[1]["error"].is_string());
    for (actual, case) in rows[2..].iter().zip(&read_dataset.evidence_cases) {
        assert_eq!(actual["fixture"]["id"], case.id);
        assert_eq!(actual["accepted"], case.accepted, "{}: {actual}", case.id);
        assert_eq!(actual["authority_proven"], false);
    }
}
