//! Live regression tests for **every** Anthropic-compatible provider
//! family registered in `tests/live-tests.toml`.
//!
//! This file contains no test-case logic — every assertion lives in the
//! fixture's `expectations` block. To add a new case, drop a JSON file
//! under `tests/fixtures/` and register its name in `common::load_fixture`.
//! To add a new provider, add a section to `tests/live-tests.toml` only.
//!
//! Every test is `#[ignore]` so `cargo test --workspace` (CI) compiles
//! and skips them. Run explicitly with
//! `cargo test -p kolyan-provider-tests -- --ignored`. See `README.md` for
//! how to set the per-family API-key env vars.

mod common;

use common::{
    AnthropicProviderConfig, Fixture, ModelMatrixEntry, assert_expectations,
    assert_prompt_cache_diff, assert_usage, build_anthropic_provider, build_request,
    has_api_key_anthropic, load_config, load_fixture, require_api_key_anthropic, run_scenario,
    run_scenario_labeled, should_skip,
};
use kolyan_model::ModelProvider;
use std::collections::BTreeMap;

const MAX_ENTRIES_PER_TEST: usize = 64;

const FIXTURE_NAMES: &[&str] = &["text", "tool_call", "structured_output", "prompt_cache"];

fn for_each_anthropic_matrix_entry<F>(mut visit: F)
where
    F: FnMut(&str, &AnthropicProviderConfig, &ModelMatrixEntry),
{
    let cfg = load_config();
    for (family, provider_cfg) in [
        (common::family::MINIMAX, &cfg.minimax_anthropic),
        (common::family::QWEN, &cfg.qwen_anthropic),
    ] {
        for entry in &provider_cfg.model_matrix {
            visit(family, provider_cfg, entry);
        }
    }
}

fn label_for(family: &str, model: &str, fixture_name: &str) -> String {
    format!("{family}/anthropic_compat/{model}/{fixture_name}")
}

fn sanitize(model: &str) -> String {
    model
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

fn plan() -> Vec<(
    String,
    AnthropicProviderConfig,
    ModelMatrixEntry,
    String,
    String,
)> {
    let mut out = Vec::new();
    for_each_anthropic_matrix_entry(|family, cfg, entry| {
        for name in FIXTURE_NAMES {
            out.push((
                family.to_string(),
                cfg.clone(),
                entry.clone(),
                (*name).to_string(),
                label_for(family, &entry.model, name),
            ));
        }
    });
    out
}

#[tokio::test]
#[ignore = "live test: requires KOLYAN_{MINIMAX,QWEN}_API_KEY; run with `cargo test -- --ignored`. Iterates over [provider.*.anthropic_compat.model_matrix] × every fixture with no prompt_cache field."]
async fn anthropic_single_shot_matrix() {
    let plan = plan();
    let plan: Vec<_> = plan
        .into_iter()
        .filter(|(_, _, _, name, _)| name != "prompt_cache")
        .collect();
    assert!(!plan.is_empty());
    assert!(plan.len() <= MAX_ENTRIES_PER_TEST);

    let mut fixtures_by_name: BTreeMap<String, Fixture> = BTreeMap::new();
    for name in FIXTURE_NAMES {
        if *name == "prompt_cache" {
            continue;
        }
        fixtures_by_name.insert((*name).to_string(), load_fixture(name));
    }

    for (family, cfg, entry, fixture_name, label) in plan {
        let fixture = fixtures_by_name
            .get(&fixture_name)
            .expect("fixture preloaded");
        if should_skip(fixture, &entry.capabilities, &label) {
            continue;
        }
        if !has_api_key_anthropic(&cfg) {
            eprintln!("[SKIP {label}] env var {} not set", cfg.api_key_env);
            continue;
        }
        let key = require_api_key_anthropic(&cfg);
        let provider = build_anthropic_provider(&cfg, &key);
        let request = build_request(
            &family,
            &entry.model,
            fixture,
            format!("anthropic-{}-{}", fixture_name, sanitize(&entry.model)),
            entry.max_output_tokens,
        );
        let response = run_scenario_labeled(&provider, request, &label).await;
        eprintln!("[{label}] ok stop_reason={:?}", response.stop_reason);
        assert_expectations(&response, &fixture.expectations, &label);
        assert_usage(&response.usage, &entry.capabilities, &label);
    }
}

#[tokio::test]
#[ignore = "live test: requires KOLYAN_{MINIMAX,QWEN}_API_KEY; run with `cargo test -- --ignored`"]
async fn anthropic_prompt_cache_matrix() {
    let plan = plan();
    let plan: Vec<_> = plan
        .into_iter()
        .filter(|(_, _, _, name, _)| name == "prompt_cache")
        .collect();
    assert!(plan.len() <= MAX_ENTRIES_PER_TEST);
    assert!(
        !plan.is_empty(),
        "prompt_cache fixture is missing from FIXTURE_NAMES"
    );

    let fixture = load_fixture("prompt_cache");

    for (family, cfg, entry, _name, label) in plan {
        if should_skip(&fixture, &entry.capabilities, &label) {
            continue;
        }
        if !has_api_key_anthropic(&cfg) {
            eprintln!("[SKIP {label}] env var {} not set", cfg.api_key_env);
            continue;
        }
        let key = require_api_key_anthropic(&cfg);
        let provider = build_anthropic_provider(&cfg, &key);
        let prefix = format!("anthropic-cache-{}", sanitize(&entry.model));
        let request_first = build_request(
            &family,
            &entry.model,
            &fixture,
            format!("{prefix}-first"),
            entry.max_output_tokens,
        );
        let request_second = build_request(
            &family,
            &entry.model,
            &fixture,
            format!("{prefix}-second"),
            entry.max_output_tokens,
        );
        let first = run_scenario(&provider, request_first).await;
        let second = run_scenario(&provider, request_second).await;
        assert_expectations(&first, &fixture.expectations, &format!("{label}/first"));
        assert_expectations(&second, &fixture.expectations, &format!("{label}/second"));
        if let Some(pc_exp) = &fixture.expectations.prompt_cache {
            assert_prompt_cache_diff(&first, &second, pc_exp, &label);
        }
    }
}

// --------------------------------------------------------------------------
// Diagnostic dumps — see exactly what each event looks like on the wire
// for the first matrix entry of each surface.
// --------------------------------------------------------------------------

#[tokio::test]
#[ignore = "diagnostic dump; run with `--ignored --nocapture`"]
async fn anthropic_dump_first_text() {
    use futures_util::StreamExt;

    let mut entries = Vec::new();
    for_each_anthropic_matrix_entry(|family, cfg, entry| {
        entries.push((family.to_string(), cfg.clone(), entry.clone()))
    });
    let (family, cfg, entry) = match entries.into_iter().next() {
        Some(t) => t,
        None => return,
    };
    if !has_api_key_anthropic(&cfg) {
        eprintln!("[SKIP] env var {} not set", cfg.api_key_env);
        return;
    }
    let key = require_api_key_anthropic(&cfg);
    let provider = build_anthropic_provider(&cfg, &key);
    let fixture = load_fixture("text");
    let request = build_request(
        &family,
        &entry.model,
        &fixture,
        format!("anthropic-dump-{}", sanitize(&entry.model)),
        entry.max_output_tokens,
    );

    eprintln!(
        "\n=== anthropic_dump_first_text BEGIN ({}/anthropic_compat/{}) ===",
        family, entry.model
    );
    let mut stream = provider
        .stream(request)
        .await
        .expect("provider.stream should produce a ModelEventStream");
    let mut event_count: usize = 0;
    while let Some(event) = stream.next().await {
        event_count += 1;
        match event {
            Ok(kolyan_model::ModelEvent::Completed(response)) => {
                eprintln!(
                    "[{event_count:03}] Completed stop_reason={:?} usage={:?}",
                    response.stop_reason, response.usage
                );
                if let Some(s) = &response.structured_output {
                    eprintln!("           structured_output = {s:?}");
                } else {
                    eprintln!("           structured_output = <None>");
                }
            }
            Ok(kolyan_model::ModelEvent::TextDelta(text)) => {
                eprintln!(
                    "[{event_count:03}] TextDelta({} chars) {:?}",
                    text.chars().count(),
                    text
                )
            }
            Ok(other) => eprintln!("[{event_count:03}] {other:?}"),
            Err(e) => eprintln!(
                "[{event_count:03}] ERR kind={:?} phase={:?} message={:?}",
                e.kind, e.phase, e.message
            ),
        }
    }
    eprintln!("=== anthropic_dump_first_text END ({event_count} events) ===\n");
}

#[tokio::test]
#[ignore = "diagnostic dump; run with `--ignored --nocapture`"]
async fn anthropic_dump_first_structured_output() {
    use futures_util::StreamExt;

    let mut entries = Vec::new();
    for_each_anthropic_matrix_entry(|family, cfg, entry| {
        entries.push((family.to_string(), cfg.clone(), entry.clone()))
    });
    let (family, cfg, entry) = match entries.into_iter().next() {
        Some(t) => t,
        None => return,
    };
    if !has_api_key_anthropic(&cfg) {
        eprintln!("[SKIP] env var {} not set", cfg.api_key_env);
        return;
    }
    let key = require_api_key_anthropic(&cfg);
    let provider = build_anthropic_provider(&cfg, &key);
    let fixture = load_fixture("structured_output");
    let request = build_request(
        &family,
        &entry.model,
        &fixture,
        format!("anthropic-so-dump-{}", sanitize(&entry.model)),
        entry.max_output_tokens,
    );

    eprintln!(
        "\n=== anthropic_dump_first_structured_output BEGIN ({}/anthropic_compat/{}) ===",
        family, entry.model
    );
    let mut stream = provider
        .stream(request)
        .await
        .expect("provider.stream should produce a ModelEventStream");
    let mut event_count: usize = 0;
    while let Some(event) = stream.next().await {
        event_count += 1;
        match event {
            Ok(kolyan_model::ModelEvent::Completed(response)) => {
                eprintln!(
                    "[{event_count:03}] Completed stop_reason={:?} usage={:?}",
                    response.stop_reason, response.usage
                );
                if let Some(s) = &response.structured_output {
                    eprintln!("           structured_output = {s:?}");
                } else {
                    eprintln!("           structured_output = <None>");
                }
            }
            Ok(kolyan_model::ModelEvent::TextDelta(text)) => {
                eprintln!(
                    "[{event_count:03}] TextDelta({} chars) {:?}",
                    text.chars().count(),
                    text
                )
            }
            Ok(other) => eprintln!("[{event_count:03}] {other:?}"),
            Err(e) => eprintln!(
                "[{event_count:03}] ERR kind={:?} phase={:?} message={:?}",
                e.kind, e.phase, e.message
            ),
        }
    }
    eprintln!("=== anthropic_dump_first_structured_output END ({event_count} events) ===\n");
}
