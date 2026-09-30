//! Validate the response-schema vocabulary used by the checked-in OpenAPI.
//! This is not an OpenAPI document validator or a general JSON Schema engine.
use super::*;

pub(super) fn response(value: &Value, status: u16, path: &str) {
    let document: Value = serde_json::from_str(include_str!(
        "../../../../../schemas/server-http.openapi.json"
    ))
    .unwrap();
    let name = if status >= 400 {
        "Problem"
    } else if path.contains("/turns") {
        "TurnView"
    } else {
        "SessionView"
    };
    assert!(
        matches(value, &document["components"]["schemas"][name], &document),
        "{name} schema mismatch: {value}"
    );
}
fn kind(value: &Value, name: &str) -> bool {
    match name {
        "object" => value.is_object(),
        "array" => value.is_array(),
        "string" => value.is_string(),
        "integer" => value.as_u64().is_some(),
        "boolean" => value.is_boolean(),
        "null" => value.is_null(),
        other => panic!("unhandled contract type {other}"),
    }
}
fn matches(value: &Value, schema: &Value, document: &Value) -> bool {
    if let Some(reference) = schema["$ref"].as_str() {
        return matches(
            value,
            document
                .pointer(reference.strip_prefix('#').unwrap())
                .unwrap(),
            document,
        );
    }
    if let Some(variants) = schema["oneOf"].as_array() {
        return variants
            .iter()
            .filter(|variant| matches(value, variant, document))
            .count()
            == 1;
    }
    if let Some(variants) = schema["anyOf"].as_array() {
        return variants
            .iter()
            .any(|variant| matches(value, variant, document));
    }
    if let Some(constant) = schema.get("const")
        && value != constant
    {
        return false;
    }
    if let Some(variants) = schema["enum"].as_array()
        && !variants.contains(value)
    {
        return false;
    }
    if let Some(name) = schema["type"].as_str()
        && !kind(value, name)
    {
        return false;
    }
    if let Some(types) = schema["type"].as_array()
        && !types.iter().any(|name| kind(value, name.as_str().unwrap()))
    {
        return false;
    }
    if let Some(object) = value.as_object() {
        if let Some(required) = schema["required"].as_array()
            && required
                .iter()
                .any(|key| !object.contains_key(key.as_str().unwrap()))
        {
            return false;
        }
        if let Some(properties) = schema["properties"].as_object() {
            if schema["additionalProperties"] == false
                && object.keys().any(|name| !properties.contains_key(name))
            {
                return false;
            }
            for (name, property) in properties {
                if let Some(child) = object.get(name)
                    && !matches(child, property, document)
                {
                    return false;
                }
            }
        }
    }
    if let Some(array) = value.as_array()
        && let Some(item) = schema.get("items")
        && !array.iter().all(|element| matches(element, item, document))
    {
        return false;
    }
    if let Some(text) = value.as_str() {
        if let Some(minimum) = schema["minLength"].as_u64()
            && (text.chars().count() as u64) < minimum
        {
            return false;
        }
        if let Some(maximum) = schema["maxLength"].as_u64()
            && (text.chars().count() as u64) > maximum
        {
            return false;
        }
        if let Some(pattern) = schema["pattern"].as_str() {
            assert_eq!(pattern, "^[A-Za-z0-9_-]+$", "unhandled contract pattern");
            if !text
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_' || byte == b'-')
            {
                return false;
            }
        }
    }
    if let (Some(number), Some(minimum)) = (value.as_f64(), schema["minimum"].as_f64())
        && number < minimum
    {
        return false;
    }
    true
}
