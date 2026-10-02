//! Wire contract regression, distinct from the real OS/model scene.
use super::*;

#[test]
fn positive_projection_wire_rejects_unknown_missing_and_wrong_types() {
    let root = tempfile::Builder::new()
        .prefix("kolyan-projection-wire-")
        .tempdir()
        .unwrap()
        .keep();
    let evidence = Evidence::new(&root.join("actual.jsonl"));
    println!(
        "PROJECTION_WIRE_TRACE={}",
        root.join("actual.jsonl").display()
    );
    for (schema, original) in [
        ("case", serde_json::to_value(fixture()).unwrap()),
        (
            "expected",
            serde_json::from_str(
                include_str!("../../../../expected/agent/long_task_projection.jsonl").trim(),
            )
            .unwrap(),
        ),
    ] {
        let keys = original
            .as_object()
            .unwrap()
            .keys()
            .cloned()
            .collect::<Vec<_>>();
        let mut variants = vec![];
        let mut unknown = original.clone();
        unknown["unknown"] = json!(true);
        variants.push(("unknown".to_string(), unknown));
        for key in keys {
            let mut missing = original.clone();
            missing.as_object_mut().unwrap().remove(&key);
            variants.push((format!("missing/{key}"), missing));
            let mut wrong = original.clone();
            wrong[&key] = json!([]);
            variants.push((format!("type/{key}"), wrong));
        }
        for (mutation, value) in variants {
            let result = if schema == "case" {
                serde_json::from_value::<ProjectionCase>(value.clone()).map(|_| ())
            } else {
                serde_json::from_value::<ProjectionExpected>(value.clone()).map(|_| ())
            };
            evidence.append(json!({"event":"wire_rejected","schema":schema,"mutation":mutation,"input":value,"error":result.as_ref().err().map(ToString::to_string)})).unwrap();
            assert!(result.is_err());
        }
    }
}
