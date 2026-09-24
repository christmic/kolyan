use super::*;
use serde_json::json;

fn table() -> ParameterTable {
    serde_json::from_value(json!({
        "provider":"vendor", "protocol":"fixture",
        "features":["text_input","text_output","streaming","tool_use"],
        "defaults":{
            "max_output_tokens":{"support":"supported","schema":{"type":"integer","minimum":1,"maximum":8192},"default":1024},
            "reasoning.effort":{"support":"unsupported","default":"high"},
            "extensions.vendor.temperature":{"support":"supported","wire_name":"temperature","schema":{"type":"number","minimum":0,"maximum":1},"default":0.5}
        },
        "models":{"a":{},"b":{"parameters":{"max_output_tokens":{"support":"supported","schema":{"type":"integer"},"default":2048}}}}
    })).unwrap()
}

fn request() -> ModelRequest {
    serde_json::from_value(
        json!({"request_id":"test","model":{"provider":"vendor","model":"a"},
        "system":[],"messages":[],"tools":[],"tool_choice":"auto", "extensions":null}),
    )
    .unwrap()
}

fn planner() -> RequestPlanner {
    RequestPlanner::new(table(), "fixture", &["model", "stream"]).unwrap()
}

#[test]
fn model_overrides_defaults_and_preserves_original_request() {
    for (model, expected) in [("a", 1024), ("b", 2048)] {
        let mut input = request();
        input.model.model = model.into();
        input.reasoning = Some(ReasoningConfig {
            effort: Some("high".into()),
            budget_tokens: Some(9999),
        });
        let original = input.clone();
        let plan = planner().plan(&input).unwrap();
        assert_eq!(input, original);
        assert_eq!(plan.request.max_output_tokens, Some(expected));
        assert!(plan.request.reasoning.is_none());
        assert_eq!(plan.wire_extensions["temperature"], 0.5);
        assert!(plan.omitted("reasoning.effort"));
        assert!(plan.omitted("reasoning.budget_tokens"));
    }
}

#[test]
fn user_value_wins_and_bad_value_fails_without_echoing_it() {
    let mut input = request();
    input.max_output_tokens = Some(321);
    assert_eq!(
        planner().plan(&input).unwrap().request.max_output_tokens,
        Some(321)
    );
    input.max_output_tokens = Some(987654);
    let error = planner().plan(&input).unwrap_err();
    assert_eq!(error.kind, ProviderErrorKind::InvalidRequest);
    assert!(!error.message.contains("987654"));
    input.max_output_tokens = None;
    input.extensions = json!({"vendor.temperature":"secret-value"});
    assert!(
        !planner()
            .plan(&input)
            .unwrap_err()
            .message
            .contains("secret-value")
    );
}

#[test]
fn unsupported_and_unknown_defaults_are_never_injected() {
    for support in [ParameterSupport::Unsupported, ParameterSupport::Unknown] {
        let mut table = table();
        table
            .defaults
            .get_mut("extensions.vendor.temperature")
            .unwrap()
            .support = support;
        let plan = RequestPlanner::new(table, "fixture", &[])
            .unwrap()
            .plan(&request())
            .unwrap();
        assert!(plan.wire_extensions.is_empty());
        assert!(plan.request.reasoning.is_none());
    }
}

#[test]
fn exact_provider_protocol_and_model_identity_are_required() {
    let mut input = request();
    input.model.model = "unknown".into();
    assert!(planner().plan(&input).is_err());
    input.model.model = "a".into();
    input.model.provider = "other-endpoint".into();
    assert!(planner().plan(&input).is_err());
    assert!(RequestPlanner::new(table(), "other-protocol", &[]).is_err());
}

#[test]
fn required_capabilities_and_explicit_tool_choice_cannot_be_removed() {
    let mut input = request();
    input.output_format = Some(crate::OutputFormat {
        name: "test".into(),
        schema: json!({}),
        strict: true,
    });
    assert_eq!(
        planner().plan(&input).unwrap_err().kind,
        ProviderErrorKind::Unsupported
    );
    input.output_format = None;
    for choice in [
        ToolChoice::Required,
        ToolChoice::Tool("read".into()),
        ToolChoice::None,
    ] {
        input.tool_choice = choice;
        assert!(planner().plan(&input).is_err());
    }
    input.tool_choice = ToolChoice::Auto;
    let mut table = table();
    table.models.get_mut("a").unwrap().features = Some(ModelFeatures::new());
    assert!(
        RequestPlanner::new(table, "fixture", &[])
            .unwrap()
            .plan(&input)
            .is_err()
    );
}

#[test]
fn unregistered_extensions_reserved_bindings_and_collisions_fail() {
    let mut input = request();
    input.extensions = json!({"vendor.unregistered":true});
    assert!(planner().plan(&input).is_err());
    for wire in ["model", "stream", "", "a.b"] {
        let mut table = table();
        table
            .defaults
            .get_mut("extensions.vendor.temperature")
            .unwrap()
            .wire_name = Some(wire.into());
        assert!(RequestPlanner::new(table, "fixture", &["model", "stream"]).is_err());
    }
    let mut table = table();
    let rule = table.defaults["extensions.vendor.temperature"].clone();
    table
        .defaults
        .insert("extensions.other.temperature".into(), rule);
    assert!(RequestPlanner::new(table, "fixture", &[]).is_err());
}

#[test]
fn invalid_schemas_unknown_keys_and_bad_defaults_fail_at_configuration() {
    for schema in [
        json!({"type":"bad"}),
        json!({"$ref":"https://invalid.example/schema"}),
    ] {
        let mut table = table();
        table.defaults.get_mut("max_output_tokens").unwrap().schema = schema;
        assert!(RequestPlanner::new(table, "fixture", &[]).is_err());
    }
    let mut invalid_default = table();
    invalid_default
        .defaults
        .get_mut("max_output_tokens")
        .unwrap()
        .default = Some(json!(0));
    assert!(RequestPlanner::new(invalid_default, "fixture", &[]).is_err());
    let mut typo = table();
    typo.defaults.insert(
        "reasoning.effrot".into(),
        typo.defaults["reasoning.effort"].clone(),
    );
    assert!(RequestPlanner::new(typo, "fixture", &[]).is_err());
}

#[test]
fn disabled_cache_children_remove_the_container_and_audit_has_no_values() {
    let mut input = request();
    input.prompt_cache = Some(PromptCacheConfig {
        key: Some("private-cache-key".into()),
        retention: None,
        breakpoints: vec![crate::CacheBreakpoint::Messages],
    });
    let plan = planner().plan(&input).unwrap();
    assert!(plan.request.prompt_cache.is_none());
    assert!(
        !serde_json::to_string(&plan.decisions)
            .unwrap()
            .contains("private-cache-key")
    );
}

#[test]
fn non_omittable_parameter_fails_and_unconfigured_extensions_are_not_ignored() {
    let mut table = table();
    table
        .defaults
        .get_mut("reasoning.effort")
        .unwrap()
        .omittable = false;
    let mut input = request();
    input.reasoning = Some(ReasoningConfig {
        effort: Some("high".into()),
        budget_tokens: None,
    });
    assert!(
        RequestPlanner::new(table, "fixture", &[])
            .unwrap()
            .plan(&input)
            .is_err()
    );
    input.extensions = json!({"vendor.temperature":0.2});
    assert!(PlannedRequest::unconfigured(input).is_err());
}

#[test]
fn explicit_nullable_extension_is_sent_instead_of_replaced_by_a_default() {
    let mut table = table();
    table
        .defaults
        .get_mut("extensions.vendor.temperature")
        .unwrap()
        .schema = json!({"type":["number","null"]});
    let mut input = request();
    input.extensions = json!({"vendor.temperature":null});
    let plan = RequestPlanner::new(table, "fixture", &[])
        .unwrap()
        .plan(&input)
        .unwrap();
    assert_eq!(plan.wire_extensions.get("temperature"), Some(&Value::Null));
    assert!(plan.decisions.iter().any(
        |d| d.parameter == "extensions.vendor.temperature" && d.action == ParameterAction::Sent
    ));
}

#[test]
fn inactive_alias_does_not_conflict_with_an_active_binding() {
    let mut table = table();
    let mut inactive = table.defaults["extensions.vendor.temperature"].clone();
    inactive.support = ParameterSupport::Unsupported;
    table
        .defaults
        .insert("extensions.other.temperature".into(), inactive);
    assert!(RequestPlanner::new(table, "fixture", &[]).is_ok());
}

#[test]
fn optional_parameter_support_cannot_contradict_feature_declarations() {
    let mut table = table();
    table.defaults.get_mut("reasoning.effort").unwrap().support = ParameterSupport::Supported;
    assert!(RequestPlanner::new(table.clone(), "fixture", &[]).is_err());
    table.features.insert(ModelFeature::Reasoning);
    assert!(RequestPlanner::new(table, "fixture", &[]).is_ok());
}

#[test]
fn required_image_document_and_tool_history_are_not_discarded() {
    let source = crate::ImageSource::Url {
        url: "https://example.invalid/input".into(),
    };
    for block in [
        ContentBlock::Image {
            source: source.clone(),
        },
        ContentBlock::Document {
            source,
            title: None,
        },
    ] {
        let mut input = request();
        input.messages.push(crate::Message {
            role: crate::MessageRole::User,
            content: vec![block],
        });
        assert_eq!(
            planner().plan(&input).unwrap_err().kind,
            ProviderErrorKind::Unsupported
        );
    }
    let mut input = request();
    input.messages.push(crate::Message {
        role: crate::MessageRole::Assistant,
        content: vec![ContentBlock::ToolCall {
            call: crate::ToolCall {
                id: "call".into(),
                name: "read".into(),
                arguments: json!({}),
            },
        }],
    });
    let mut table = table();
    table.features.remove(&ModelFeature::ToolUse);
    assert!(
        RequestPlanner::new(table, "fixture", &[])
            .unwrap()
            .plan(&input)
            .is_err()
    );
}

#[test]
fn registered_required_features_preserve_the_exact_tools_and_schema() {
    let mut table = table();
    table.features.insert(ModelFeature::StructuredOutput);
    let mut input = request();
    input.tools.push(crate::ToolDefinition {
        name: "read".into(),
        description: None,
        input_schema: json!({"type":"object"}),
    });
    input.output_format = Some(crate::OutputFormat {
        name: "answer".into(),
        schema: json!({"type":"object","required":["ok"]}),
        strict: true,
    });
    let planned = RequestPlanner::new(table, "fixture", &[])
        .unwrap()
        .plan(&input)
        .unwrap()
        .request;
    assert_eq!(planned.tools, input.tools);
    assert_eq!(planned.output_format, input.output_format);
}

#[test]
fn public_config_round_trip_validates_against_schema_and_runtime_contract() {
    let schema: Value = serde_json::from_str(include_str!(
        "../../../../schemas/parameter-table.schema.json"
    ))
    .unwrap();
    let mut config = table();
    config.protocol = "openai_responses".into();
    let value = serde_json::to_value(config).unwrap();
    assert!(jsonschema::validator_for(&schema).unwrap().is_valid(&value));
    RequestPlanner::new(
        serde_json::from_value(value).unwrap(),
        "openai_responses",
        &[],
    )
    .unwrap();
}
