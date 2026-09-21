//! Live regression tests for **every** OpenAI-compatible provider family
//! registered in `tests/config/live-tests.toml`.
//!
//! This file contains no test-case logic — every assertion lives in the
//! fixture's `expectations` block. To add a new case, drop a JSON file
//! under `tests/fixtures/` and register its name in `common::load_fixture`.
//! To add a new provider, add a section to `tests/config/live-tests.toml` only.
//!
//! Every test is `#[ignore]` so `cargo test --workspace` (CI) compiles and
//! skips them. Run explicitly with
//! `cargo test -p kolyan-integration-tests -- --ignored`. See `README.md` for
//! how to set the per-family API-key env vars.

#[path = "../common/mod.rs"]
mod common;

use common::{
    Fixture, ModelMatrixEntry, ProviderConfig, assert_expectations, assert_prompt_cache_diff,
    assert_usage, build_openai_provider, build_request, has_api_key, load_config, load_fixture,
    require_api_key, run_scenario, run_scenario_labeled, should_skip,
};
use kolyan_model::{ModelEvent, ModelProvider};
use std::collections::BTreeMap;

/// Cap on how many matrix entries a single test will exercise. The matrix
/// currently has ~11 models × 2 families; this leaves headroom without
/// dragging CI runs.
const MAX_ENTRIES_PER_TEST: usize = 64;

/// Names of fixtures exercised by `openai_<case>_fixture_matrix`. Edit
/// this list (or register a new fixture name in
/// `common::load_fixture`) to add a new test case.
const FIXTURE_NAMES: &[&str] = &["text", "tool_call", "structured_output", "prompt_cache"];

fn for_each_openai_matrix_entry<F>(mut visit: F)
where
    F: FnMut(&str, &ProviderConfig, &ModelMatrixEntry),
{
    let cfg = load_config();
    for (family, provider_cfg) in [
        (common::family::MINIMAX, &cfg.minimax_openai),
        (common::family::QWEN, &cfg.qwen_openai),
    ] {
        for entry in &provider_cfg.model_matrix {
            visit(family, provider_cfg, entry);
        }
    }
}

fn label_for(family: &str, model: &str, fixture_name: &str) -> String {
    format!("{family}/openai_compat/{model}/{fixture_name}")
}

fn sanitize(model: &str) -> String {
    model
        .chars()
        .map(|c| if c.is_ascii_alphanumeric() { c } else { '_' })
        .collect()
}

/// Plan: (family, cfg, entry, fixture_name, label) for every matrix
/// entry × every fixture. Pre-computed so the test bodies stay linear.
fn plan() -> Vec<(String, ProviderConfig, ModelMatrixEntry, String, String)> {
    let mut out = Vec::new();
    for_each_openai_matrix_entry(|family, cfg, entry| {
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

// --------------------------------------------------------------------------
// Text + tool_call + structured_output — single-shot matrix.
// --------------------------------------------------------------------------

#[tokio::test]
#[ignore = "live test: requires KOLYAN_{MINIMAX,QWEN}_API_KEY; run with `cargo test -- --ignored`. Iterates over [provider.*.openai_compat.model_matrix] × every fixture with no prompt_cache field."]
async fn openai_single_shot_matrix() {
    let plan = plan();
    let plan: Vec<_> = plan
        .into_iter()
        .filter(|(_, _, _, name, _)| name != "prompt_cache")
        .collect();
    assert!(
        !plan.is_empty(),
        "openai_compat.matrix × single-shot fixtures is empty; nothing to test"
    );
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
        if !has_api_key(&cfg) {
            eprintln!("[SKIP {label}] env var {} not set", cfg.api_key_env);
            continue;
        }
        let key = require_api_key(&cfg);
        let provider = build_openai_provider(&cfg, &key);
        let request = build_request(
            &family,
            &entry.model,
            fixture,
            format!("openai-{}-{}", fixture_name, sanitize(&entry.model)),
            entry.max_output_tokens,
        );
        let response = run_scenario_labeled(&provider, request, &label).await;
        eprintln!("[{label}] ok stop_reason={:?}", response.stop_reason);
        assert_expectations(&response, &fixture.expectations, &label);
        assert_usage(&response.usage, &entry.capabilities, &label);
    }
}

// --------------------------------------------------------------------------
// prompt_cache — two-shot matrix (skip rows whose fixture has no
// prompt_cache expectation, and skip models whose capability flag is
// false).
// --------------------------------------------------------------------------

#[tokio::test]
#[ignore = "live test: requires KOLYAN_{MINIMAX,QWEN}_API_KEY; run with `cargo test -- --ignored`"]
async fn openai_prompt_cache_matrix() {
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
        if !has_api_key(&cfg) {
            eprintln!("[SKIP {label}] env var {} not set", cfg.api_key_env);
            continue;
        }
        let key = require_api_key(&cfg);
        let provider = build_openai_provider(&cfg, &key);
        let prefix = format!("openai-cache-{}", sanitize(&entry.model));
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
// Diagnostic dump — exercises the first matrix entry across all families
// using the `text` fixture.
// --------------------------------------------------------------------------

#[tokio::test]
#[ignore = "diagnostic dump; run with `--ignored --nocapture`"]
async fn openai_dump_first_text() {
    use futures_util::StreamExt;

    let mut entries = Vec::new();
    for_each_openai_matrix_entry(|family, cfg, entry| {
        entries.push((family.to_string(), cfg.clone(), entry.clone()))
    });
    let (family, cfg, entry) = match entries.into_iter().next() {
        Some(t) => t,
        None => return,
    };
    let key = require_api_key(&cfg);
    let provider = build_openai_provider(&cfg, &key);
    let fixture = load_fixture("text");
    let request = build_request(
        &family,
        &entry.model,
        &fixture,
        format!("openai-dump-{}", sanitize(&entry.model)),
        entry.max_output_tokens,
    );

    eprintln!(
        "\n=== openai_dump_first_text BEGIN ({}/openai_compat/{}) ===",
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
            Ok(ModelEvent::Completed(response)) => {
                eprintln!(
                    "[{event_count:03}] Completed stop_reason={:?} usage={:?}",
                    response.stop_reason, response.usage
                );
            }
            Ok(ModelEvent::TextDelta(text)) => {
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
    eprintln!("=== openai_dump_first_text END ({event_count} events) ===\n");
}

/// Diagnostic: dump every event for a `structured_output` request on the
/// first matrix entry, printing both `Completed.structured_output` and
/// the raw `response.metadata` payload so we can see where the JSON
/// actually lives on the wire (vs. what the provider extracted).
#[tokio::test]
#[ignore = "diagnostic dump; run with `--ignored --nocapture`"]
async fn openai_dump_first_structured_output() {
    use futures_util::StreamExt;

    let mut entries = Vec::new();
    for_each_openai_matrix_entry(|family, cfg, entry| {
        entries.push((family.to_string(), cfg.clone(), entry.clone()))
    });
    let (family, cfg, entry) = match entries.into_iter().next() {
        Some(t) => t,
        None => return,
    };
    let key = require_api_key(&cfg);
    let provider = build_openai_provider(&cfg, &key);
    let fixture = load_fixture("structured_output");
    let request = build_request(
        &family,
        &entry.model,
        &fixture,
        format!("openai-so-dump-{}", sanitize(&entry.model)),
        entry.max_output_tokens,
    );

    eprintln!(
        "\n=== openai_dump_first_structured_output BEGIN ({}/openai_compat/{}) ===",
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
            Ok(ModelEvent::Completed(response)) => {
                eprintln!(
                    "[{event_count:03}] Completed stop_reason={:?} usage={:?}",
                    response.stop_reason, response.usage
                );
                if let Some(s) = &response.structured_output {
                    eprintln!("           structured_output = {s:?}");
                } else {
                    eprintln!("           structured_output = <None>");
                }
                if let Some(output) = response.metadata.get("output").and_then(|v| v.as_array()) {
                    eprintln!("           output[] ({} items):", output.len());
                    for (i, item) in output.iter().enumerate() {
                        let t = item.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                        eprintln!("             [{i}] type={t}");
                        if let Some(content) = item.get("content").and_then(|v| v.as_array()) {
                            for (j, block) in content.iter().enumerate() {
                                let bt = block.get("type").and_then(|v| v.as_str()).unwrap_or("?");
                                let txt = block
                                    .get("text")
                                    .and_then(|v| v.as_str())
                                    .map(|s| format!("({} chars) {:?}", s.chars().count(), s));
                                eprintln!(
                                    "                  .content[{j}] type={bt} text={}",
                                    txt.unwrap_or_else(|| "<no text>".into())
                                );
                            }
                        }
                        if let Some(parsed) = item.get("parsed") {
                            eprintln!("             [{i}].parsed = {parsed:?}");
                        }
                    }
                }
                if let Some(output_text) = response.metadata.get("output_text") {
                    eprintln!("           metadata.output_text = {output_text:?}");
                }
                if let Some(parsed) = response.metadata.get("parsed") {
                    eprintln!("           metadata.parsed = {parsed:?}");
                }
                if let Some(text) = response.metadata.get("text") {
                    eprintln!("           metadata.text = {text:?}");
                }
                if let Some(status) = response.metadata.get("status") {
                    eprintln!("           metadata.status = {status:?}");
                }
                if let Some(incomplete) = response.metadata.get("incomplete_details") {
                    eprintln!("           metadata.incomplete_details = {incomplete:?}");
                }
            }
            Ok(ModelEvent::TextDelta(text)) => {
                eprintln!(
                    "[{event_count:03}] TextDelta({} chars) {:?}",
                    text.chars().count(),
                    text
                )
            }
            Ok(ModelEvent::Provider(meta)) => {
                // Probe: if the raw fields contain a `response` key with status,
                // emit it on its own line so we can see whether the upstream SSE
                // event kind was `response.incomplete`, `response.failed`, or
                // something else entirely.
                if let Some(raw) = &meta.raw
                    && let Some(response) = raw.get("response")
                {
                    eprintln!(
                        "[{event_count:03}] Provider(response-shaped) status={:?}",
                        response.get("status")
                    );
                    if let Some(incomplete) = response.get("incomplete_details") {
                        eprintln!("           incomplete_details = {incomplete:?}");
                    }
                    eprintln!("           raw={raw}");
                } else {
                    eprintln!("[{event_count:03}] Provider raw={:?}", meta.raw);
                }
            }
            Ok(other) => eprintln!("[{event_count:03}] {other:?}"),
            Err(e) => eprintln!(
                "[{event_count:03}] ERR kind={:?} phase={:?} message={:?}",
                e.kind, e.phase, e.message
            ),
        }
    }
    eprintln!("=== openai_dump_first_structured_output END ({event_count} events) ===\n");
}
