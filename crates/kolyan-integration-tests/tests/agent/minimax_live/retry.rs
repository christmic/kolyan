//! Project configuration and pure HTTP-opening decisions, not a network retry run.

use kolyan_protocol_http::{RetryDecision, RetryHeaders, RetryInput};
use serde::Deserialize;
use serde_json::json;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    providers: Vec<Provider>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Provider {
    surface: String,
    profile: String,
    max_retries: u32,
    max_elapsed_ms: u64,
    max_server_delay_ms: u64,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    status: u16,
    code: Option<String>,
    retry: bool,
}

#[test]
fn minimax_project_retry_configuration_is_bounded_and_not_quota_or_stream_replay() {
    let data: Dataset =
        serde_json::from_str(include_str!("../../fixtures/agent/minimax_http_retry.json")).unwrap();
    let config = crate::common::load_config();
    let mut rows = Vec::new();
    for provider in &data.providers {
        let policy = match provider.surface.as_str() {
            "openai_compat" => &config.minimax_openai.http_retry,
            "anthropic_compat" => &config.minimax_anthropic.http_retry,
            other => panic!("unknown fixture surface {other}"),
        };
        for case in &data.cases {
            let decision = policy.decide(RetryInput {
                retries_used: 0,
                elapsed_ms: 0,
                now_unix_ms: 0,
                jitter: 0.75,
                status: case.status,
                error_code: case.code.as_deref(),
                headers: RetryHeaders::default(),
            });
            rows.push(json!({"surface":provider.surface,"case":case.id,
                "source":"offline_project_policy_not_actual_network",
                "policy":policy,"profile":format!("{:?}",policy.profile()),
                "decision":decision,"retry":matches!(decision,RetryDecision::Retry{..})}));
        }
    }
    let directory = tempfile::Builder::new()
        .prefix("kolyan-minimax-project-retry-")
        .tempdir()
        .unwrap()
        .keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in &rows {
        writeln!(output, "{row}").unwrap();
    }
    output.sync_all().unwrap();
    println!("MINIMAX_PROJECT_RETRY_TRACE={}", path.display());
    for (row, (provider, case)) in rows.iter().zip(
        data.providers
            .iter()
            .flat_map(|provider| data.cases.iter().map(move |case| (provider, case))),
    ) {
        assert_eq!(row["profile"], provider.profile);
        assert_eq!(row["policy"]["max_retries"], provider.max_retries);
        assert_eq!(row["policy"]["max_elapsed_ms"], provider.max_elapsed_ms);
        assert_eq!(
            row["policy"]["max_server_delay_ms"],
            provider.max_server_delay_ms
        );
        assert_eq!(row["retry"], case.retry, "{}: {row}", case.id);
    }
}
