use super::super::*;
use serde::Deserialize;
use serde_json::json;
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Row {
    id: String,
    target: String,
    delta: Option<usize>,
    mutation: Option<String>,
    limit: Option<usize>,
    expected: String,
    error_contains: Option<String>,
}

#[test]
fn complete_representation_and_identity_bounds_export_before_compare() {
    let rows: Vec<Row> = serde_json::from_str(include_str!("bounds.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-accounting-bounds-")
        .tempdir()
        .unwrap()
        .keep();
    let mut export = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for row in &rows {
        let mut request: ModelRequest = serde_json::from_value(
            json!({"request_id":"fixture","model":{"provider":"fixture","model":"m"},
            "system":[],"messages":[],"tools":[],"tool_choice":"auto","extensions":null}),
        )
        .unwrap();
        let mut generation = json!({"model":"m","input":""});
        let mut count = generation.clone();
        let mut model = request.model.clone();
        let mut mapping = "mapping-v1".to_owned();
        let mut coverage = "coverage-v1".to_owned();
        let mut counter = "counter-v1".to_owned();
        let planned_bytes = row
            .delta
            .map(|delta| MAX_CONTEXT_JSON_BYTES as usize + delta);
        if let Some(target_bytes) = planned_bytes {
            match row.target.as_str() {
                "neutral" => {
                    request.request_id.clear();
                    let base = serde_json::to_vec(&request).unwrap().len();
                    request.request_id = "x".repeat(target_bytes - base);
                }
                "generation" => {
                    let base = serde_json::to_vec(&generation).unwrap().len();
                    generation["input"] = json!("x".repeat(target_bytes - base));
                }
                "count" => {
                    let base = serde_json::to_vec(&count).unwrap().len();
                    count["input"] = json!("x".repeat(target_bytes - base));
                }
                _ => panic!("unknown budget target"),
            }
        } else {
            let limit = row.limit.unwrap();
            let text = match row.mutation.as_deref().unwrap() {
                "exact" => "x".repeat(limit),
                "over" => "x".repeat(limit + 1),
                "control" => "bad\\n".replace("\\n", "\n"),
                "blank" => " ".into(),
                _ => panic!("unknown identity mutation"),
            };
            match row.target.as_str() {
                "provider" => model.provider = text,
                "model" => model.model = text,
                "mapping_revision" => mapping = text,
                "coverage_revision" => coverage = text,
                "counter_revision" => counter = text,
                _ => panic!("unknown identity target"),
            }
        }
        // Full inputs live in durable sidecars, including refused/over-limit rows.
        let mut inputs = serde_json::Map::new();
        for (name, value) in [
            ("neutral", serde_json::to_value(&request).unwrap()),
            ("generation", generation.clone()),
            ("count", count.clone()),
        ] {
            let path = dir.join(format!("{}-{name}.json", row.id));
            let mut file = std::fs::File::create(&path).unwrap();
            serde_json::to_writer(&mut file, &value).unwrap();
            file.flush().unwrap();
            file.sync_all().unwrap();
            drop(file);
            inputs.insert(
                name.into(),
                json!({"path":path,"bytes":std::fs::metadata(&path).unwrap().len()}),
            );
        }
        let input_identity = json!({"model":model,"mapping_revision":mapping,"coverage_revision":coverage,"counter_revision":counter});
        let result=MappingIdentity::new(endpoint_identity("http://localhost:1","responses/v1"),
            ContextProtocol::OpenAiResponses,model,mapping,coverage)
            .and_then(|identity| {
                let profile=CountProfile::registered(identity.clone(),counter)?;
                if planned_bytes.is_none() {return Ok(None);}
                PreparedContextWire::new(std::sync::Arc::new(()),identity,profile,&request,generation,count,CountCoverage::new(vec![]))
                    .map(|prepared|Some(json!({"neutral_digest":prepared.neutral_digest(),
                        "wire_digest":prepared.generation_wire_digest(),"wire_bytes":prepared.generation_wire_bytes(),
                        "count_digest":prepared.count_input_digest()})))
            });
        let mut actual = match result {
            Ok(prepared) => {
                json!({"case":row.id,"class":"success","identity_input":input_identity,"planned_bytes":planned_bytes,"prepared":prepared})
            }
            Err(error) => {
                json!({"case":row.id,"class":"refused","identity_input":input_identity,"planned_bytes":planned_bytes,"error":error.to_string()})
            }
        };
        actual["inputs"] = serde_json::Value::Object(inputs);
        actual["owner"] = json!({"same":true,"note":"constructor-only; no foreign verification"});
        actual["profile_input"] = json!({"identity":input_identity,"counter_revision":input_identity["counter_revision"]});
        writeln!(export, "{}", actual).unwrap();
    }
    export.flush().unwrap();
    export.sync_all().unwrap();
    drop(export);
    let actuals: Vec<serde_json::Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    assert_eq!(actuals.len(), rows.len());
    eprintln!("accounting bounds evidence: {}", dir.display());
    for (row, actual) in rows.iter().zip(&actuals) {
        assert_eq!(actual["case"], row.id);
        for name in ["neutral", "generation", "count"] {
            let path = actual["inputs"][name]["path"].as_str().unwrap();
            let file = std::fs::File::open(path).unwrap();
            let _: serde_json::Value = serde_json::from_reader(file).unwrap();
            assert_eq!(
                std::fs::metadata(path).unwrap().len(),
                actual["inputs"][name]["bytes"].as_u64().unwrap()
            );
        }
        assert_eq!(
            actual["class"],
            row.expected,
            "{} {}",
            row.id,
            dir.display()
        );
        if let Some(needle) = &row.error_contains {
            assert!(
                actual["error"].as_str().unwrap().contains(needle),
                "{}",
                row.id
            );
        }
        if row.target == "generation" && row.expected == "success" {
            assert_eq!(actual["prepared"]["wire_bytes"], MAX_CONTEXT_JSON_BYTES);
        }
    }
}

#[test]
fn json_measure_counts_escaping_not_scalar_length() {
    let input = json!("\u{0}中文🦀");
    assert_eq!(
        context_json_bytes(&input).unwrap(),
        serde_json::to_vec(&input).unwrap().len() as u64
    );
}
