//! Full R1 matrix with per-row failure isolation and test-owned evidence files.

#[path = "../common/mod.rs"]
mod common;
#[path = "../common/matrix.rs"]
mod matrix;

use common::*;
use futures_util::StreamExt;
use kolyan_core::{StepEvent, StepExecutor, StepRequest};
use kolyan_model::{ModelProvider, ModelRequest, ProviderFuture};
use matrix::{Matrix, Status};
use serde_json::json;
use std::{fs, io::Write, path::Path};

#[derive(Clone)]
enum Provider {
    OpenAi(kolyan_provider_openai::OpenAiProvider),
    Anthropic(kolyan_provider_anthropic::AnthropicProvider),
}

impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        match self {
            Self::OpenAi(p) => p.stream(request),
            Self::Anthropic(p) => p.stream(request),
        }
    }
}

#[derive(Clone)]
struct Case {
    label: String,
    family: String,
    protocol: String,
    fixture: String,
    entry: ModelMatrixEntry,
    provider: Option<Provider>,
}

fn plan() -> Vec<Case> {
    let config = load_config();
    let dataset: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/r1_matrix.json")).unwrap();
    let fixtures = dataset["cases"].as_array().unwrap();
    let mut cases = vec![];
    for (family, cfg) in [
        ("minimax", config.minimax_openai),
        ("qwen", config.qwen_openai),
    ] {
        let provider = has_api_key(&cfg)
            .then(|| Provider::OpenAi(build_openai_provider(&cfg, &require_api_key(&cfg))));
        for entry in cfg.model_matrix {
            for fixture in fixtures {
                let fixture = fixture.as_str().unwrap();
                cases.push(Case {
                    label: format!("{family}/openai/{}/{fixture}", entry.model),
                    family: family.into(),
                    protocol: "openai_responses".into(),
                    fixture: fixture.into(),
                    entry: entry.clone(),
                    provider: provider.clone(),
                });
            }
        }
    }
    for (family, cfg) in [
        ("minimax", config.minimax_anthropic),
        ("qwen", config.qwen_anthropic),
    ] {
        let provider = has_api_key_anthropic(&cfg).then(|| {
            Provider::Anthropic(build_anthropic_provider(
                &cfg,
                &require_api_key_anthropic(&cfg),
            ))
        });
        for entry in cfg.model_matrix {
            for fixture in fixtures {
                let fixture = fixture.as_str().unwrap();
                cases.push(Case {
                    label: format!("{family}/anthropic/{}/{fixture}", entry.model),
                    family: family.into(),
                    protocol: "anthropic_messages".into(),
                    fixture: fixture.into(),
                    entry: entry.clone(),
                    provider: provider.clone(),
                });
            }
        }
    }
    cases
}

#[tokio::test]
#[ignore = "real providers; all configured protocol/model/case combinations; writes complete matrix evidence"]
async fn all_r1_provider_step_contracts() {
    let cases = plan();
    let dataset: serde_json::Value =
        serde_json::from_str(include_str!("../fixtures/r1_matrix.json")).unwrap();
    let mut matrix = Matrix::new(cases.iter().map(|case| case.label.clone()));
    let revision = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .output()
        .unwrap();
    fs::write(matrix.directory.join("revision.txt"), revision.stdout).unwrap();
    fs::write(
        matrix.directory.join("config.toml"),
        include_str!("../config/live-tests.toml"),
    )
    .unwrap();
    for (index, case) in cases.into_iter().enumerate() {
        let fixture = load_fixture(&case.fixture);
        if should_skip(&fixture, &case.entry.capabilities, &case.label) {
            matrix.record(
                index,
                Status::Skipped,
                "declared unsupported fixture capability",
            );
            continue;
        }
        if case.fixture == "structured_output"
            && !common::supports_feature(
                &case.family,
                &case.protocol,
                &case.entry.model,
                kolyan_model::ModelFeature::StructuredOutput,
            )
        {
            matrix.record(
                index,
                Status::Skipped,
                "feature table declares structured output unsupported",
            );
            continue;
        }
        let Some(provider) = case.provider.clone() else {
            matrix.record(index, Status::NotRun, "missing provider credential");
            continue;
        };
        let directory = matrix.directory.join(index.to_string());
        fs::create_dir(&directory).unwrap();
        if let Some(sampling) = dataset["sampling"].get(&case.fixture) {
            let samples = sampling["samples"].as_u64().unwrap() as usize;
            let minimum = sampling["minimum_passes"].as_u64().unwrap() as usize;
            matrix
                .run_samples(index, samples, minimum, |sample| {
                    let case = case.clone();
                    let directory = directory.clone();
                    async move {
                        let provider = case.provider.clone().unwrap();
                        let fixture = load_fixture(&case.fixture);
                        run_step(&provider, &case, &fixture, &directory, sample).await;
                    }
                })
                .await;
        } else {
            matrix
                .run(index, async {
                    let first = run_step(&provider, &case, &fixture, &directory, 0).await;
                    if let Some(expected) = &fixture.expectations.prompt_cache {
                        let second = run_step(&provider, &case, &fixture, &directory, 1).await;
                        assert_prompt_cache_diff(&first, &second, expected, &case.label);
                    }
                })
                .await;
        }
    }
    assert!(
        matrix.complete(),
        "matrix contains failed/unexecuted rows: {}",
        matrix.directory.display()
    );
}

async fn run_step(
    provider: &Provider,
    case: &Case,
    fixture: &Fixture,
    directory: &Path,
    attempt: usize,
) -> kolyan_model::ModelResponse {
    let request = build_request(
        &case.family,
        &case.entry.model,
        fixture,
        format!("r1-{attempt}"),
        case.entry.max_output_tokens,
    );
    fs::write(
        directory.join(format!("request-{attempt}.json")),
        serde_json::to_vec_pretty(&request).unwrap(),
    )
    .unwrap();
    let executor = StepExecutor::new(provider.clone());
    let mut events = executor
        .execute_stream(StepRequest {
            step_id: format!("step-{attempt}"),
            model_request: request,
            options: Default::default(),
        })
        .await
        .unwrap();
    let mut file = fs::File::create(directory.join(format!("events-{attempt}.jsonl"))).unwrap();
    let mut completed = None;
    let mut count = 0;
    while let Some(event) = events.next().await {
        // Keep every actual event, including text/reasoning and errors, before assertions.
        writeln!(
            file,
            "{}",
            json!({"sequence":count,"event_debug":format!("{event:?}")})
        )
        .unwrap();
        file.flush().unwrap();
        count += 1;
        if let StepEvent::Completed(result) = event.unwrap() {
            assert!(completed.is_none(), "duplicate completion");
            completed = Some(result.response);
        } else {
            assert!(completed.is_none(), "event after completion");
        }
    }
    let response = completed.expect("missing completion");
    assert_expectations(&response, &fixture.expectations, &case.label);
    assert_usage(&response.usage, &case.entry.capabilities, &case.label);
    response
}

#[tokio::test]
async fn matrix_failure_does_not_prevent_later_rows() {
    let mut matrix = Matrix::new(["first".into(), "second".into(), "third".into()]);
    matrix
        .run(0, async { panic!("intentional regression fixture") })
        .await;
    matrix.run(1, async {}).await;
    assert_eq!(matrix.rows[0].status, Status::Failed);
    assert_eq!(matrix.rows[1].status, Status::Passed);
    assert_eq!(matrix.rows[2].status, Status::NotRun);
    assert!(!matrix.complete());
    assert!(matrix.directory.join("report.json").exists());
}

#[tokio::test]
async fn matrix_retry_preserves_failed_attempt_evidence() {
    let mut matrix = Matrix::new(["model".into()]);
    matrix
        .run_attempts(0, 2, |attempt| async move {
            assert_eq!(attempt, 2, "transient model failure");
        })
        .await;
    assert_eq!(matrix.rows[0].status, Status::Passed);
    assert_eq!(matrix.rows[0].attempts, 2);
    assert_eq!(matrix.rows[0].passed_attempts, 1);
    assert_eq!(matrix.rows[0].attempt_failures.len(), 1);
    assert!(matrix.rows[0].attempt_failures[0].contains("transient model failure"));
}

#[tokio::test]
async fn matrix_sampling_runs_every_sample_and_applies_threshold() {
    let mut matrix = Matrix::new(["structured".into()]);
    matrix
        .run_samples(0, 3, 2, |sample| async move {
            assert_ne!(sample, 1, "first sample failed schema validation");
        })
        .await;
    assert_eq!(matrix.rows[0].status, Status::Passed);
    assert_eq!(matrix.rows[0].attempts, 3);
    assert_eq!(matrix.rows[0].passed_attempts, 2);
    assert_eq!(matrix.rows[0].attempt_failures.len(), 1);
}

#[tokio::test]
#[ignore = "real providers; tool identity regression over every configured model/protocol"]
async fn tool_identity_streams_across_all_models() {
    let cases = plan()
        .into_iter()
        .filter(|case| case.fixture == "tool_call")
        .collect::<Vec<_>>();
    assert!(
        !cases.is_empty(),
        "tool_call must remain in the data-driven matrix"
    );
    let mut matrix = Matrix::new(cases.iter().map(|case| case.label.clone()));
    for (index, case) in cases.into_iter().enumerate() {
        let Some(provider) = case.provider.clone() else {
            matrix.record(index, Status::NotRun, "missing provider credential");
            continue;
        };
        let directory = matrix.directory.join(index.to_string());
        fs::create_dir(&directory).unwrap();
        matrix
            .run(index, async {
                run_step(
                    &provider,
                    &case,
                    &load_fixture(&case.fixture),
                    &directory,
                    0,
                )
                .await;
            })
            .await;
    }
    assert!(
        matrix.complete(),
        "tool identity matrix incomplete: {}",
        matrix.directory.display()
    );
    println!("PASS tool identity matrix: {}", matrix.directory.display());
}
