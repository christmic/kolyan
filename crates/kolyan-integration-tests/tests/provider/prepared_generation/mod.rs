//! One data-driven framework; the SDK owns preparation, counting and generation.

mod wire;

use std::{io::Write, time::Duration};

use futures_util::StreamExt;
use kolyan_model::{
    CountProfile, ModelRequest, PreparedModelGeneration, PreparedModelProvider, ProviderError,
    digest_json,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::net::TcpListener;

#[derive(Clone, Copy, Deserialize)]
#[serde(rename_all = "snake_case")]
enum Mode {
    Default,
    Registered,
    UnchangedClone,
    ForeignOwner,
    ProfileChanged,
    CallerChanged,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mode: Mode,
    registered: bool,
    count_requests: usize,
    generation_requests: usize,
    input_tokens: Option<u64>,
    generation_error: bool,
}

pub async fn run<P: PreparedModelProvider + Clone>(
    fixture: &str,
    count_fixture: &str,
    generation_path: &'static str,
    count_path: &'static str,
    factory: impl Fn(&str) -> P,
    configure: super::ProfileAdapter<P>,
) {
    let fixture: Value = serde_json::from_str(fixture).unwrap();
    let count_fixture: Value = serde_json::from_str(count_fixture).unwrap();
    let count_response = count_fixture["rows"]
        .as_array()
        .unwrap()
        .iter()
        .find(|row| row["id"] == "full_fields")
        .unwrap()["response"]
        .as_str()
        .unwrap();
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-prepared-port-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for case in &cases {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let base = format!("http://{}", listener.local_addr().unwrap());
        let mut request: ModelRequest = serde_json::from_value(fixture["request"].clone()).unwrap();
        let mut owner = factory(&base);
        let identity = owner
            .prepare_generation(&request)
            .unwrap()
            .wire()
            .identity()
            .clone();
        let profile = if case.registered {
            CountProfile::registered(identity.clone(), "fixture-port-v1".into()).unwrap()
        } else {
            CountProfile::default()
        };
        owner = configure(owner, profile.clone());
        let prepared = owner.prepare_generation(&request).unwrap();
        let wire = prepared.wire();
        let evidence = json!({"identity":wire.identity(),"neutral_digest":wire.neutral_digest(),
            "generation_body":wire.generation_body(),"generation_digest":wire.generation_wire_digest(),
            "generation_bytes":wire.generation_wire_bytes(),"count_body":wire.count_body(),
            "count_digest":wire.count_input_digest(),"profile_digest":wire.count_profile_digest().unwrap()});
        let consumer_profile = if matches!(case.mode, Mode::ProfileChanged) {
            CountProfile::registered(identity, "fixture-port-v2".into()).unwrap()
        } else {
            profile.clone()
        };
        let consumer = match case.mode {
            Mode::ForeignOwner => configure(factory(&base), profile.clone()),
            Mode::ProfileChanged => configure(owner.clone(), consumer_profile.clone()),
            _ => owner.clone(),
        };
        if matches!(case.mode, Mode::CallerChanged) {
            request.messages.clear();
        }
        let server = tokio::spawn(wire::serve(
            listener,
            generation_path,
            count_path,
            fixture["stream_response"].as_str().unwrap().into(),
            count_response.into(),
        ));
        let counted = consumer
            .count_prepared(&prepared, Duration::from_secs(2))
            .await;
        let count = match counted {
            Ok(value) => json!({"report":value}),
            Err(error) => json!({"error":error_json(error)}),
        };
        let events: Vec<Value> = match consumer.stream_prepared(prepared).await {
            Ok(stream) => {
                stream
                    .map(|event| match event {
                        Ok(value) => json!({"event":value}),
                        Err(error) => json!({"error":error_json(error)}),
                    })
                    .collect()
                    .await
            }
            Err(error) => vec![json!({"error":error_json(error)})],
        };
        writeln!(export, "{}", json!({"id":case.id,"caller_input":request,"prepared":evidence,
            "prepared_profile":profile,"consumer_profile":consumer_profile,"count":count,"events":events,
            "physical":server.await.unwrap()})).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let actual: Vec<Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    eprintln!("prepared port evidence: {}", dir.display());
    assert_eq!(actual.len(), cases.len());
    for (case, row) in cases.iter().zip(&actual) {
        assert_eq!(row["id"], case.id);
        let prepared = &row["prepared"];
        assert_eq!(
            prepared["profile_digest"],
            digest_json(&row["prepared_profile"]).unwrap()
        );
        if matches!(case.mode, Mode::ProfileChanged) {
            assert_ne!(
                prepared["profile_digest"],
                digest_json(&row["consumer_profile"]).unwrap()
            );
        }
        assert_eq!(
            prepared["profile_digest"] == digest_json(&CountProfile::default()).unwrap(),
            !case.registered
        );
        let physical = row["physical"].as_array().unwrap();
        let counts: Vec<_> = physical
            .iter()
            .filter(|v| v["path"] == count_path)
            .collect();
        let generations: Vec<_> = physical
            .iter()
            .filter(|v| v["path"] == generation_path)
            .collect();
        assert_eq!(
            physical.len(),
            case.count_requests + case.generation_requests,
            "{}",
            case.id
        );
        assert_eq!(counts.len(), case.count_requests, "{}", case.id);
        assert_eq!(generations.len(), case.generation_requests, "{}", case.id);
        assert_eq!(
            row["count"]["report"]["input_tokens"].as_u64(),
            case.input_tokens
        );
        if case.input_tokens.is_some() {
            for field in ["identity", "neutral_digest"] {
                assert_eq!(row["count"]["report"][field], prepared[field]);
            }
            assert_eq!(
                row["count"]["report"]["generation_wire_digest"],
                prepared["generation_digest"]
            );
            assert_eq!(
                row["count"]["report"]["count_input_digest"],
                prepared["count_digest"]
            );
        } else {
            assert!(row["count"].get("error").is_some());
        }
        for count in counts {
            assert_eq!(count["body"], prepared["count_body"]);
            assert_eq!(count["body_digest"], prepared["count_digest"]);
            // Count uses the existing typed DTO serializer: key ordering may
            // differ, but the entire decoded projection and digest must match.
            let raw: Value = serde_json::from_str(count["raw_body"].as_str().unwrap()).unwrap();
            assert_eq!(raw, prepared["count_body"]);
        }
        for generation in generations {
            assert_eq!(generation["body"], prepared["generation_body"]);
            assert_eq!(generation["body_digest"], prepared["generation_digest"]);
            assert_eq!(
                generation["raw_body"],
                serde_json::to_string(&prepared["generation_body"]).unwrap()
            );
            assert_eq!(
                generation["raw_body"].as_str().unwrap().len() as u64,
                prepared["generation_bytes"].as_u64().unwrap()
            );
        }
        let events = row["events"].as_array().unwrap();
        assert_eq!(
            events.iter().any(|v| v.get("error").is_some()),
            case.generation_error,
            "{}",
            case.id
        );
        if !case.generation_error {
            assert!(events.iter().any(|v| v["event"].get("Completed").is_some()));
        }
        if matches!(case.mode, Mode::CallerChanged) {
            assert_ne!(
                digest_json(&row["caller_input"]).unwrap(),
                prepared["neutral_digest"]
            );
        }
    }
}

fn error_json(error: ProviderError) -> Value {
    json!({"kind":error.kind,"phase":error.phase,"message":error.to_string()})
}
