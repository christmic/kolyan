use super::*;

#[test]
fn adapter_plan_is_bound_without_changing_call_or_implementation() {
    let original = prepared("edit-v1", "a");
    let bound = original
        .clone()
        .with_execution_binding(json!({
            "schema_version":1,"parent":{"dev":1,"ino":2},"leaf":"a.txt"
        }))
        .unwrap();
    assert_eq!(bound.call(), original.call());
    assert_eq!(bound.tool_revision(), original.tool_revision());
    assert_ne!(bound.digest(), original.digest());
    bound.validate().unwrap();
    let decision = policy(ApprovalMode::Never).decide_prepared(&bound, &PolicyContext::default());
    let revision = decision.policy_version.clone();
    let grant =
        PreparedGrant::issue(&bound, decision, ApprovalEvidence::NotConfirmed, scope()).unwrap();
    grant.validate(&bound, &revision, &scope()).unwrap();
    let changed = bound
        .with_execution_binding(json!({
            "schema_version":1,"parent":{"dev":1,"ino":3},"leaf":"a.txt"
        }))
        .unwrap();
    assert_eq!(
        grant.validate(&changed, &revision, &scope()),
        Err(PreparedError::BindingMismatch)
    );
}

#[test]
fn plan_tampering_missing_binding_and_invalid_shapes_fail_closed() {
    let bound = prepared("edit-v1", "a")
        .with_execution_binding(json!({"root":"physical"}))
        .unwrap();
    let mut value = serde_json::to_value(&bound).unwrap();
    value["execution_binding"]["root"] = json!("redirected");
    let corrupt: PreparedCall = serde_json::from_value(value.clone()).unwrap();
    assert!(corrupt.validate().is_err());
    value.as_object_mut().unwrap().remove("execution_binding");
    assert!(serde_json::from_value::<PreparedCall>(value).is_err());
    for invalid in [json!(false), json!("plan"), json!([1, 2])] {
        assert!(bound.clone().with_execution_binding(invalid).is_err());
    }
    assert!(
        bound
            .with_execution_binding(json!({"large":"x".repeat(1024 * 1024)}))
            .is_err()
    );
}

#[test]
fn plan_canonicalization_sorts_objects_but_preserves_array_order() {
    let base = prepared("edit-v1", "a");
    let first = base
        .clone()
        .with_execution_binding(serde_json::from_str(r#"{"b":{"y":2,"x":1},"a":[1,2]}"#).unwrap())
        .unwrap();
    let reordered = base
        .clone()
        .with_execution_binding(serde_json::from_str(r#"{"a":[1,2],"b":{"x":1,"y":2}}"#).unwrap())
        .unwrap();
    assert_eq!(first.digest(), reordered.digest());
    let changed = base
        .with_execution_binding(json!({"a":[2,1],"b":{"x":1,"y":2}}))
        .unwrap();
    assert_ne!(first.digest(), changed.digest());
}
