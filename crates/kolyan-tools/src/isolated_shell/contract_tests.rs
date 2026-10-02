//! Advertised constraints and actual preparation; never execute model commands.

use super::*;

use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    schema_version: u32,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    arguments: Value,
    prepared: bool,
}

#[cfg(target_os = "macos")]
#[test]
fn declared_directory_constraints_export_then_compare_actual_preparation() {
    let data: Dataset = serde_json::from_str(include_str!("contract_cases.json")).unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-shell-declared-contract-")
        .tempdir()
        .unwrap()
        .keep();
    std::fs::create_dir(root.join("sub")).unwrap();
    let tool = IsolatedShellTool::new(IsolatedShellConfig {
        workspace: root.clone(),
        protected_roots: vec![],
        max_command_bytes: 8192,
        max_output_bytes: 8192,
        timeout: Duration::from_secs(5),
    })
    .unwrap();
    let definition = IsolatedShellTool::tool_definition();
    let observations = data
        .cases
        .iter()
        .map(|case| {
            let prepared = tool.prepare(ToolCall {
                id: case.id.clone(),
                name: "shell".into(),
                arguments: case.arguments.clone(),
            });
            json!({"id":case.id,"arguments":case.arguments,
                "prepared":prepared.is_ok(),"error":prepared.as_ref().err().map(ToString::to_string),
                "scope":"preparation only; no execution/grant/process/receipt","network":false})
        })
        .collect::<Vec<_>>();
    let exported = json!({"schema_version":data.schema_version,
        "advertised_tool":definition,"observations":observations});
    let file = root.join("actual.json");
    std::fs::write(&file, serde_json::to_vec_pretty(&exported).unwrap()).unwrap();
    println!("SHELL_DECLARED_CONTRACT_TRACE={}", file.display());

    // Assertions only consume the fully exported observation collection.
    assert_eq!(data.schema_version, 1);
    assert_eq!(observations.len(), data.cases.len());
    for (observed, case) in observations.iter().zip(&data.cases) {
        assert_eq!(observed["id"], case.id);
        assert_eq!(observed["prepared"], case.prepared, "{}", case.id);
    }
    let schema = &definition.input_schema;
    assert_eq!(schema["additionalProperties"], false);
    assert_eq!(schema["required"], json!(["command"]));
    assert_eq!(schema["properties"]["command"]["minLength"], 1);
    assert_eq!(
        schema["properties"]["path"]["type"],
        json!(["string", "null"])
    );
    assert_eq!(schema["properties"]["path"]["minLength"], 1);
    assert_eq!(schema["properties"]["path"]["maxLength"], 4096);
    assert!(
        schema["properties"]["path"]["description"]
            .as_str()
            .unwrap()
            .contains("Empty strings")
    );
}
