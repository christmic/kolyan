use serde::Deserialize;
use serde_json::{Value, json};

use super::super::StateCase;
use super::{contract, validate_candidate_observations};

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
    mutation: String,
    error: Option<String>,
}

#[test]
fn candidate_stdout_observations_are_strict_and_exported_before_comparison() {
    let data: Dataset = serde_json::from_str(include_str!("cases.json")).unwrap();
    let plan: Value = serde_json::from_str(include_str!(
        "../../../../fixtures/agent/self_iteration.json"
    ))
    .unwrap();
    let expected: Vec<StateCase> = serde_json::from_value(plan["expected_states"].clone()).unwrap();
    let requirements = contract(&expected);
    let mut exported = Vec::new();
    for case in &data.cases {
        let mut expected = expected.clone();
        let mut lines: Vec<String> = expected.iter().map(|e| json!({
            "event":"candidate_invocation_state_v1", "schema_version":1,
            "state":e.state,"before":e.state,"after":e.state,"results":[e.terminal,e.terminal,e.terminal]
        }).to_string()).collect();
        let mut first: Value = serde_json::from_str(&lines[0]).unwrap();
        match case.mutation.as_str() {
            "noise" => lines.insert(
                0,
                "Compiling candidate\nrunning 1 test\n{\"event\":\"unrelated\"}".into(),
            ),
            "reverse" => lines.reverse(),
            "escape_marker" => lines[0] = lines[0].replace("candidate_", "candidate\\u005f"),
            "empty" => lines.clear(),
            "remove" => {
                lines.pop();
            }
            "duplicate" => lines.push(lines[0].clone()),
            "foreign" => first["state"] = json!("foreign"),
            "version" => first["schema_version"] = json!(2),
            "missing_version" => {
                first.as_object_mut().unwrap().remove("schema_version");
            }
            "unknown" => first["extra"] = json!(true),
            "duplicate_field" => lines[0] = format!("{{\"state\":\"duplicate\",{}", &lines[0][1..]),
            "duplicate_event" => lines[0] = format!("{{\"event\":\"other\",{}", &lines[0][1..]),
            "escaped_duplicate_event" => lines.push(
                "{\"event\":\"candidate\\u005finvocation_state_v1\",\"event\":\"other\"}".into(),
            ),
            "escaped_malformed" => {
                lines.push("{\"event\":\"candidate\\u005finvocation_state_v1\",".into())
            }
            "missing_before" | "missing_after" | "missing_results" => {
                first
                    .as_object_mut()
                    .unwrap()
                    .remove(case.mutation.strip_prefix("missing_").unwrap());
            }
            "malformed" => lines.push("{\"event\":\"candidate_invocation_state_v1\"".into()),
            "before" => first["before"] = json!("foreign"),
            "after" => first["after"] = json!("foreign"),
            "result" => first["results"][1] = json!(!expected[0].terminal),
            "two_results" => first["results"] = json!([true, true]),
            "four_results" => first["results"] = json!([true, true, true, true]),
            "string_result" => first["results"][0] = json!("false"),
            "expected_duplicate" => expected[1].state = expected[0].state.clone(),
            "expected_remove" => {
                expected.pop();
            }
            "expected_invert" => expected[0].terminal = !expected[0].terminal,
            other => panic!("unknown dataset mutation {other}"),
        }
        if matches!(
            case.mutation.as_str(),
            "foreign"
                | "version"
                | "missing_version"
                | "unknown"
                | "before"
                | "after"
                | "result"
                | "two_results"
                | "four_results"
                | "string_result"
                | "missing_before"
                | "missing_after"
                | "missing_results"
        ) {
            lines[0] = first.to_string();
        }
        let stdout = lines.join("\n");
        let result = validate_candidate_observations(&stdout, &expected);
        exported.push(json!({"id":case.id,"stdout":stdout,"expected":expected,"actual":result}));
    }
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("actual.json");
    std::fs::write(
        &path,
        serde_json::to_vec_pretty(&json!({"contract":requirements,"cases":exported})).unwrap(),
    )
    .unwrap();
    let path = directory.keep().join("actual.json");
    println!("CANDIDATE_OBSERVATIONS_TRACE={}", path.display());
    assert_eq!(data.schema_version, 1);
    assert_eq!(
        requirements["expected"],
        serde_json::to_value(&expected).unwrap()
    );
    assert_eq!(requirements["sourceAuditMandatory"], true);
    assert_eq!(requirements["export_all_rows_before_any_comparison"], true);
    assert_eq!(
        requirements["row_schema"]["required"]
            .as_array()
            .unwrap()
            .len(),
        6
    );
    assert_eq!(requirements["row_schema"]["additionalProperties"], false);
    for (case, row) in data.cases.iter().zip(&exported) {
        match &case.error {
            None => {
                assert!(row["actual"]["Ok"].is_object(), "{}: {}", case.id, row);
                assert_eq!(row["actual"]["Ok"]["rows"].as_array().unwrap().len(), 7);
            }
            Some(code) => assert!(
                row["actual"]["Err"]["issues"]
                    .as_array()
                    .unwrap()
                    .iter()
                    .any(|i| i["code"] == *code),
                "{}: {}",
                case.id,
                row
            ),
        }
    }
}
