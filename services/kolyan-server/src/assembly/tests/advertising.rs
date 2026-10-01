use super::*;

#[test]
fn service_advertising_filters_without_changing_base_schemas_or_inventory() {
    let mut fixture = fixture();
    fixture.config.workspace = fixture.config.workspace.canonicalize().unwrap();
    let base = IsolatedToolSet::tool_definitions();
    assert_eq!(base.len(), 4);
    for (allow_shell, scope, count) in [
        (false, "safe", 3),
        (false, ".", 3),
        (true, "safe", 3),
        (true, ".", 4),
    ] {
        fixture.config.allow_shell = allow_shell;
        fixture.config.tool_scope = scope.into();
        let definitions = advertised_tools(&fixture.config).unwrap();
        assert_eq!(
            definitions.len(),
            count,
            "allow_shell={allow_shell}, scope={scope}"
        );
        for definition in &definitions {
            assert_eq!(
                definition,
                base.iter()
                    .find(|tool| tool.name == definition.name)
                    .unwrap()
            );
            assert_eq!(definition.input_schema["additionalProperties"], false);
        }
        assert_eq!(
            definitions.iter().any(|tool| tool.name == "shell"),
            count == 4
        );
    }
    assert_eq!(IsolatedToolSet::tool_definitions(), base);
}
