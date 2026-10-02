//! Historical initialization identity is independent from later history.
use super::*;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Mutation {
    Read,
    Critical,
    Schema,
    Subject,
    Cause,
    Digest,
    Foreign,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutation: Mutation,
    max_bytes: usize,
    expected: Expected,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(rename_all = "snake_case")]
enum Expected {
    ExactInitialization,
    Refused,
}

struct ReadOnly {
    inner: Arc<dyn FactJournal>,
    mode: Mutation,
}
impl FactJournal for ReadOnly {
    fn read(
        &self,
        stream: &str,
        after: u64,
        limit: usize,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        let mut records = self.inner.read(stream, after, limit)?;
        for record in &mut records {
            if record.draft.kind == "server.private-context.initialized" {
                match self.mode {
                    Mutation::Critical => record.draft.critical = false,
                    Mutation::Schema => record.draft.schema_version = 2,
                    Mutation::Subject => record.draft.subject.id = "foreign".into(),
                    Mutation::Cause => record.draft.causes.clear(),
                    Mutation::Digest => {
                        record.draft.payload["binding_digest"] = json!("b".repeat(64))
                    }
                    Mutation::Read | Mutation::Foreign => {}
                }
            }
        }
        Ok(records)
    }
    fn append(
        &self,
        _: &str,
        _: u64,
        _: Vec<FactDraft>,
    ) -> Result<Vec<kolyan_ledger::FactRecord>, kolyan_ledger::FactError> {
        panic!("verification must not initialize")
    }
}
#[test]
fn verified_initialization_data_preserves_future_history_and_refuses_foreign_proofs() {
    let mut all_observations = Vec::new();
    for sqlite in [false, true] {
        let root = tempfile::Builder::new()
            .prefix("kolyan-verified-initialization-")
            .tempdir()
            .unwrap()
            .keep();
        let journal: Arc<dyn FactJournal> = if sqlite {
            Arc::new(SqliteFactJournal::open(root.join("facts.sqlite")).unwrap())
        } else {
            Arc::new(MemoryFactJournal::default())
        };
        let store = FileSessionStore::new(root.join("sessions")).unwrap();
        let owner = owner();
        let reference = source(journal.as_ref(), &owner);
        let service = PrivateContextService::new(
            SessionService::new(store.clone()),
            journal.clone(),
            Arc::new(ExactOwner(journal.clone())),
        );
        let initial = service
            .initialize(&owner, &reference, messages("original selected projection"))
            .unwrap()
            .initialization
            .unwrap();
        store
            .append_turn(
                "private",
                kolyan_storage::SessionTurn {
                    turn_id: "future".into(),
                    execution_id: "future-execution".into(),
                    status: kolyan_storage::SessionTurnStatus::Completed,
                },
                messages("future history retained"),
            )
            .unwrap();
        let before = store.load("private").unwrap();
        let facts_before = journal.read("owner", 0, 2).unwrap();
        let coordinate = digest(&json!([
            "kolyan.server.private-context.owner.v1",
            owner.private_session_id
        ]))
        .unwrap();
        let initialization_stream = format!("server.private-context.{coordinate}");
        let initialization_before = journal.read(&initialization_stream, 0, 2).unwrap();
        let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
        use std::io::Write;
        println!(
            "VERIFIED_INITIALIZATION_TRACE={}",
            root.join("actual.jsonl").display()
        );
        let cases: Vec<Case> = serde_json::from_str(include_str!("verified_cases.json")).unwrap();
        let mut observations = Vec::new();
        for case in cases {
            let read = PrivateContextService::new(
                SessionService::new(store.clone()),
                Arc::new(ReadOnly {
                    inner: journal.clone(),
                    mode: case.mutation,
                }),
                Arc::new(ExactOwner(journal.clone())),
            );
            let mut proof = reference.clone();
            if matches!(case.mutation, Mutation::Foreign) {
                proof.fact_id = "foreign".into();
            }
            let result = read.load_verified_initialization(&owner, &proof, case.max_bytes);
            let after = store.load("private").unwrap();
            let facts_after = journal.read("owner", 0, 2).unwrap();
            let initialization_after = journal.read(&initialization_stream, 0, 2).unwrap();
            writeln!(output,"{}",json!({"backend":if sqlite{"sqlite"}else{"memory"},"case":case,"before":{"session":before,"ownership_journal":facts_before,"initialization_journal":initialization_before},"ownership":reference,"result":result.as_ref().map_err(ToString::to_string),"after":{"session":after,"ownership_journal":facts_after,"initialization_journal":initialization_after}})).unwrap();
            observations.push((case, result, after, facts_after, initialization_after));
        }
        output.sync_all().unwrap();
        all_observations.push((
            initial,
            before,
            facts_before,
            initialization_before,
            observations,
        ));
    }
    // Both backends' complete evidence must exist before any comparison can panic.
    for (initial, before, facts_before, initialization_before, observations) in all_observations {
        for (case, result, after, facts_after, initialization_after) in observations {
            if matches!(case.expected, Expected::ExactInitialization) {
                assert_eq!(result.unwrap().initialization, initial);
            } else {
                assert!(result.is_err(), "{}", case.id);
            }
            assert_eq!(after, before, "{}", case.id);
            assert_eq!(facts_after, facts_before, "{}", case.id);
            assert_eq!(initialization_after, initialization_before, "{}", case.id);
        }
    }
}
