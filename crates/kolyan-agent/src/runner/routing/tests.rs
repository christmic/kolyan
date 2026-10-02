//! Actual root request inventory, exported before data-driven comparison.

mod limits;

use super::*;
use crate::runner::tests::support::*;
use crate::{AgentDefinition, AgentDefinitionInput, AgentSelector};
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    configured: bool,
    admitted: bool,
    advertised: bool,
}

#[tokio::test]
async fn root_advertises_only_explicit_admitted_delegation() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/inventory.json")).unwrap();
    let mut rows = Vec::new();
    for case in &cases {
        let harness = Harness::new();
        let mut permissions = harness.runner.host.clone();
        permissions.delegation.allow_self = case.admitted;
        let definition = AgentDefinition::new(AgentDefinitionInput {
            definition_id: "routing-root".into(),
            revision: "1".into(),
            display_name: None,
            model: kolyan_model::ModelRef::new("test-provider", "selected-model"),
            instructions: "Routing fixture".into(),
            permissions: permissions.clone(),
        })
        .unwrap();
        let mut runner = AgentRunner::new(
            harness.service.clone(),
            harness.runner.instances.clone(),
            harness.bindings.clone(),
            harness.runner.catalog.clone(),
            permissions.clone(),
            (
                Providers {
                    observations: harness.observations.clone(),
                    fail: false,
                    reject_context: false,
                    call_tool: false,
                },
                Tools(harness.observations.clone(), false),
            ),
            harness.runner.input_artifacts.clone(),
        )
        .unwrap();
        if case.configured {
            runner = runner
                .with_delegation(AgentDelegationConfig {
                    limits: InvokePrepareLimits {
                        max_children: 2,
                        max_parallel: 2,
                        max_child_input_bytes: 1024,
                        max_output_bytes: 65536,
                        admission_timeout_ms: 1000,
                    },
                    approval: ApprovalMode::Never,
                })
                .unwrap();
        }
        let mut request = harness.request(&case.id, false);
        request.selector = AgentSelector::Inline(definition);
        request.requested_permissions = permissions;
        let result = Arc::new(runner).start(request).await;
        rows.push(serde_json::json!({ "id":case.id,
            "error":result.err().map(|error|error.to_string()),
            "requests":harness.observations.requests.lock().unwrap().clone(),
            "effects":harness.observations.effects.lock().unwrap().clone() }));
    }
    let directory = tempfile::tempdir().unwrap().keep();
    let path = directory.join("actual.jsonl");
    use std::io::Write;
    let mut output = std::fs::File::create(&path).unwrap();
    for row in &rows {
        writeln!(output, "{}", serde_json::to_string(row).unwrap()).unwrap();
    }
    output.sync_all().unwrap();
    eprintln!("Agent routing inventory: {}", path.display());
    let actual = std::fs::read_to_string(&path).unwrap();
    for (line, case) in actual.lines().zip(&cases) {
        let row: serde_json::Value = serde_json::from_str(line).unwrap();
        assert!(row["error"].is_null(), "{}: {row}", case.id);
        let requests = row["requests"].as_array().unwrap();
        assert_eq!(requests.len(), 1);
        let tools = requests[0]["tools"].as_array().unwrap();
        assert_eq!(
            tools.iter().any(|tool| tool["name"] == AGENT_INVOKE_NAME),
            case.advertised
        );
        assert!(row["effects"].as_array().unwrap().is_empty());
    }
}
