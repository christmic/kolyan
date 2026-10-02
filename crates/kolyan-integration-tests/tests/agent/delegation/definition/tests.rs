//! The declared wire schema permits omission of the optional display name only.

use serde::Deserialize;
use serde_json::Value;

use super::validated;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    definition: Value,
    cases: Vec<Case>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    field: String,
    remove: bool,
    value: Value,
    valid: bool,
    equal: bool,
}

#[test]
fn inline_wire_optional_name_parity_preserves_all_required_semantics() {
    let dataset: Dataset =
        serde_json::from_str(include_str!("../../../fixtures/agent/inline_wire.json")).unwrap();
    let expected = validated(dataset.definition.clone()).unwrap();
    for case in dataset.cases {
        let mut source = dataset.definition.clone();
        if case.remove {
            source.as_object_mut().unwrap().remove(&case.field);
        } else {
            source[&case.field] = case.value;
        }
        let original = source.clone();
        let actual = validated(source.clone());
        assert_eq!(actual.is_ok(), case.valid, "{}", case.field);
        assert_eq!(
            actual
                .as_ref()
                .is_ok_and(|definition| definition == &expected),
            case.equal,
            "{}",
            case.field
        );
        assert_eq!(
            source, original,
            "comparison must not mutate actual wire evidence"
        );
    }
}
