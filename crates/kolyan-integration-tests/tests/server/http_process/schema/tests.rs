//! Data-driven wire-shape regression using the canonical OpenAPI, not a shadow DTO.

use serde::Deserialize;

use super::*;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Dataset {
    base: Value,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    mutations: Vec<Mutation>,
    accepted: bool,
}

#[derive(Deserialize)]
#[serde(tag = "op", rename_all = "snake_case", deny_unknown_fields)]
enum Mutation {
    Set {
        parent: String,
        key: String,
        value: Value,
    },
    Remove {
        parent: String,
        key: String,
    },
}

#[test]
fn compound_turn_view_schema_exports_all_positive_and_negative_rows() {
    let dataset: Dataset = serde_json::from_str(include_str!(
        "../../../fixtures/server_http_turn_view_schema.json"
    ))
    .unwrap();
    let document: Value = serde_json::from_str(include_str!(
        "../../../../../../schemas/server-http.openapi.json"
    ))
    .unwrap();
    let root = tempfile::Builder::new()
        .prefix("kolyan-http-turn-view-schema-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = fs::File::create(root.join("actual.jsonl")).unwrap();
    let mut observations = Vec::new();
    for case in dataset.cases {
        let mut value = dataset.base.clone();
        for mutation in case.mutations {
            match mutation {
                Mutation::Set {
                    parent,
                    key,
                    value: child,
                } => {
                    value
                        .pointer_mut(&parent)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .insert(key, child);
                }
                Mutation::Remove { parent, key } => {
                    value
                        .pointer_mut(&parent)
                        .unwrap()
                        .as_object_mut()
                        .unwrap()
                        .remove(&key)
                        .unwrap();
                }
            }
        }
        let accepted = matches(
            &value,
            &document["components"]["schemas"]["TurnView"],
            &document,
        );
        writeln!(
            export,
            "{}",
            json!({"case":case.id,"value":value,"expected":case.accepted,"accepted":accepted})
        )
        .unwrap();
        observations.push((case.id, accepted, case.accepted));
    }
    export.sync_all().unwrap();
    eprintln!("TurnView schema evidence: {}", root.display());
    for (id, accepted, expected) in observations {
        assert_eq!(accepted, expected, "{id}");
    }
}
