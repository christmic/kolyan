//! Synthetic artifact/fact adversaries, not evidence of native execution.

use super::super::*;
use kolyan_core::TurnControl;
use kolyan_ledger::{FactDraft, FactJournal, FactSubject, MemoryFactJournal};
use kolyan_trace::{ArtifactStore, Retention};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{
    fs,
    io::Write,
    os::unix::fs::PermissionsExt,
    sync::Arc,
    time::{Duration, Instant},
};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Plan {
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: String,
    expected: String,
}

fn category(result: &Result<(), HookError>) -> &'static str {
    match result {
        Ok(()) => "ok",
        Err(HookError::Expired) => "expired",
        Err(HookError::Cancelled) => "cancelled",
        Err(HookError::Interrupted) => "interrupted",
        Err(HookError::Protocol(_)) => "protocol",
        _ => "integrity",
    }
}

#[test]
fn native_bridge_proof_and_cutoff_dataset() {
    let input: Value = serde_json::from_str(include_str!("bridge.json")).unwrap();
    let plan: Plan = serde_json::from_value(input.clone()).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-bridge-proof-")
        .tempdir()
        .unwrap()
        .keep()
        .canonicalize()
        .unwrap();
    let path = root.join("actual.jsonl");
    let mut out = fs::File::create(&path).unwrap();
    writeln!(out,"{}",json!({"event":"plan","input":input,"scope":"synthetic proof negatives; no native process","attempts":1})).unwrap();
    out.sync_all().unwrap();
    for case in &plan.cases {
        let dir = root.join(&case.id);
        fs::create_dir(&dir).unwrap();
        for name in ["install", "cwd"] {
            fs::create_dir(dir.join(name)).unwrap();
            fs::set_permissions(dir.join(name), fs::Permissions::from_mode(0o700)).unwrap();
        }
        let native = NativeHookHost::new(dir.join("install"), dir.join("cwd")).unwrap();
        let journal = Arc::new(MemoryFactJournal::default());
        let artifacts = Arc::new(ArtifactStore::new(dir.join("artifacts"), 1024 * 1024).unwrap());
        let (scope, owner, policy) = super::foundation::setup(journal.clone());
        let catalog =
            HookCatalog::new(journal.clone(), artifacts.clone(), "bridge-unit".into()).unwrap();
        let registration = catalog
            .register(
                super::foundation::manifest(),
                "synthetic artifact proof only",
            )
            .unwrap();
        let binding = catalog
            .bind(
                scope.clone(),
                owner,
                if case.mode == "empty" {
                    vec![]
                } else {
                    vec![registration.manifest.key.clone()]
                },
                &policy,
                native.digest().into(),
            )
            .unwrap();
        let event = HookEvent {
            schema_version: 1,
            scope: scope.clone(),
            payload: HookPayload::BeforeModel {
                opening: 1,
                source_digest: "a".repeat(64),
            },
        };
        let runtime = HookRuntime::new(catalog.clone(), policy, native);
        let control = TurnControl::default();
        let cutoff = if case.mode == "expired" {
            Instant::now() - Duration::from_secs(1)
        } else {
            Instant::now() + Duration::from_secs(1)
        };
        if case.mode == "cancelled" {
            control.cancel();
        }
        let mut recorded = Vec::new();
        let mut deadline = None;
        let result = if ["window", "expired", "cancelled"].contains(&case.mode.as_str()) {
            HookExecutionWindow::at_deadline(control,cutoff).map(|window|{deadline=Some(json!({"actual":format!("{:?}",window.deadline()),"original":format!("{cutoff:?}")}));})
        } else if case.mode == "unbound" {
            let mut foreign = scope.clone();
            foreign.execution_id = "missing-execution".into();
            catalog.restore_for_scope(&foreign).map(|_| ())
        } else {
            let run = hash(&("kolyan.hooks.run.v1", binding.reference(), "operation")).unwrap();
            let stream = format!("agent.hooks.run.{run}");
            if case.mode != "empty" {
                let mutated = HookEvent {
                    schema_version: 1,
                    scope: scope.clone(),
                    payload: HookPayload::BeforeModel {
                        opening: 2,
                        source_digest: "b".repeat(64),
                    },
                };
                let input = artifacts
                    .put(
                        &encode(if case.mode == "input" {
                            &mutated
                        } else {
                            &event
                        })
                        .unwrap(),
                        Retention::Required,
                    )
                    .unwrap();
                let id = format!("{run}.0");
                let started = journal.append(&stream,0,vec![FactDraft {
                    fact_id:format!("agent.hook.started.{id}"),subject:FactSubject{kind:"agent.hook.run".into(),id},
                    kind:"agent.hook.started".into(),schema_version:1,critical:true,causes:vec![binding.reference().clone(),registration.reference.clone()],
                    payload:json!({"binding":binding.reference(),"registration":registration.reference,"input":input,"native_digest":binding.saved.native_digest})
                }]).unwrap().remove(0);
                recorded.push(started.clone());
                if case.mode != "missing" {
                    let stdout = if case.mode == "reply" {
                        b"prose".to_vec()
                    } else {
                        br#"{"schema_version":1,"decision":"continue","reason":""}"#.to_vec()
                    };
                    let output = artifacts.put(&serde_json::to_vec(&json!({"status":"exited","exit_code":if case.mode=="failed" {1} else {0},"stdout":stdout,"stderr":[]})).unwrap(),Retention::Required).unwrap();
                    let ended = journal.append(&stream,1,vec![FactDraft {
                        fact_id:format!("{}.completed",started.draft.fact_id),subject:FactSubject{kind:"agent.hook.run".into(),id:started.draft.fact_id.clone()},
                        kind:"agent.hook.completed".into(),schema_version:1,critical:true,
                        causes:vec![if case.mode=="cause" {registration.reference.clone()} else {reference(&started)}],
                        payload:json!({"output":output,"reply":{"schema_version":1,"decision":"continue","reason":""},"host_failure":null})
                    }]).unwrap().remove(0);
                    recorded.push(ended);
                }
                if case.mode == "extra" {
                    recorded.extend(
                        journal
                            .append(
                                &stream,
                                2,
                                vec![FactDraft {
                                    fact_id: "extra-proof".into(),
                                    subject: FactSubject {
                                        kind: "fixture.proof".into(),
                                        id: "extra".into(),
                                    },
                                    kind: "fixture.extra".into(),
                                    schema_version: 1,
                                    critical: true,
                                    causes: vec![],
                                    payload: json!({}),
                                }],
                            )
                            .unwrap(),
                    );
                }
            }
            if case.mode == "revoked" {
                catalog
                    .revoke(
                        &registration.manifest.key,
                        &registration.reference,
                        "revoke".into(),
                        "history remains readable".into(),
                    )
                    .unwrap();
            }
            runtime
                .verify_dispatch(&binding, &event, "operation")
                .map(|_| ())
        };
        writeln!(out,"{}",json!({"event":"actual","id":case.id,"result":category(&result),"error":result.as_ref().err().map(ToString::to_string),"deadline":deadline,"event_input":event,"binding":binding,"facts":recorded,"synthetic":true})).unwrap();
        out.flush().unwrap();
        out.sync_all().unwrap();
    }
    drop(out);
    eprintln!("HOOK_BRIDGE_PROOF={}", path.display());
    let rows: Vec<Value> = fs::read_to_string(path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(rows.len(), plan.cases.len() + 1);
    for (row, case) in rows.iter().skip(1).zip(plan.cases) {
        assert_eq!(row["result"], case.expected, "{}", case.id);
        if case.mode == "window" {
            assert_eq!(row["deadline"]["actual"], row["deadline"]["original"]);
        }
    }
}
