//! Explicit DFS bounds against real journals, with all observed reads exported.
use super::*;
use std::sync::{Arc, Mutex};

#[derive(Debug, Deserialize, Serialize)]
enum Shape {
    Chain,
    SharedDag,
    Cycle,
}
#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct GraphCase {
    id: String,
    shape: Shape,
    nodes: usize,
    body_bytes: usize,
    accepted: bool,
    unique_reads: Option<usize>,
}
struct Recording<J> {
    journal: J,
    reads: Arc<Mutex<Vec<FactRecord>>>,
    cycle: Option<FactRef>,
}
impl<J: FactJournal> FactJournal for Recording<J> {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        let mut rows = self.journal.read(stream, after, limit)?;
        if stream == "node-0"
            && let Some(cycle) = &self.cycle
        {
            for row in &mut rows {
                row.draft.causes = vec![cycle.clone()];
            }
        }
        self.reads.lock().unwrap().extend(rows.clone());
        Ok(rows)
    }
    fn append(&self, _: &str, _: u64, _: Vec<FactDraft>) -> Result<Vec<FactRecord>, FactError> {
        panic!("causal verifier writes")
    }
}
fn run<J: FactJournal>(
    journal: J,
    case: &GraphCase,
    output: &mut std::fs::File,
    backend: &str,
) -> (bool, usize) {
    let mut records = Vec::new();
    let mut refs: Vec<FactRef> = Vec::new();
    for index in 0..case.nodes {
        let id = format!("node-{index}");
        let causes = if index == 0 {
            vec![]
        } else if matches!(case.shape, Shape::SharedDag) {
            vec![refs[0].clone()]
        } else {
            vec![refs[index - 1].clone()]
        };
        records.extend(
            journal
                .append(
                    &id,
                    0,
                    vec![FactDraft {
                        fact_id: id.clone(),
                        subject: FactSubject {
                            kind: "fixture.causal".into(),
                            id: id.clone(),
                        },
                        kind: "fixture.causal.node".into(),
                        schema_version: 1,
                        critical: true,
                        causes,
                        payload: json!({"bytes":"x".repeat(case.body_bytes)}),
                    }],
                )
                .unwrap(),
        );
        refs.push(FactRef {
            stream_id: id.clone(),
            position: 1,
            fact_id: id,
        });
    }
    let causes = if matches!(case.shape, Shape::SharedDag) {
        refs[1..].to_vec()
    } else {
        vec![refs.last().unwrap().clone()]
    };
    let reads = Arc::new(Mutex::new(Vec::new()));
    let readonly = Recording {
        journal,
        reads: reads.clone(),
        cycle: matches!(case.shape, Shape::Cycle).then(|| refs.last().unwrap().clone()),
    };
    let result = validate_causes(&readonly, &causes, "prospective-source");
    let observed = reads.lock().unwrap().clone();
    writeln!(output,"{}",json!({"backend":backend,"case":case,"source_causes":causes,"before":records,"observed_reads":observed,"result":result.as_ref().map_err(ToString::to_string),"scan_limits":{"unique_records":MAX_CAUSAL_RECORDS,"bytes":MAX_BYTES},"writes":0})).unwrap();
    (result.is_ok(), observed.len())
}
#[test]
fn invocation_input_source_causal_deep_shared_cycle_and_limits_data() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-input-causal-")
        .tempdir()
        .unwrap()
        .keep();
    let mut output = std::fs::File::create(root.join("actual.jsonl")).unwrap();
    println!("INPUT_CAUSAL_TRACE={}", root.join("actual.jsonl").display());
    let cases: Vec<GraphCase> = serde_json::from_str(include_str!("causal_cases.json")).unwrap();
    let mut observations = Vec::new();
    for case in cases {
        let memory = run(MemoryFactJournal::default(), &case, &mut output, "memory");
        let sqlite = run(
            SqliteFactJournal::open(root.join(format!("{}.sqlite", case.id))).unwrap(),
            &case,
            &mut output,
            "sqlite",
        );
        observations.push((case, memory, sqlite));
    }
    output.sync_all().unwrap();
    for (case, memory, sqlite) in observations {
        for (accepted, count) in [memory, sqlite] {
            assert_eq!(accepted, case.accepted, "{}", case.id);
            if let Some(expected) = case.unique_reads {
                assert_eq!(count, expected, "{}", case.id);
            }
        }
    }
}
