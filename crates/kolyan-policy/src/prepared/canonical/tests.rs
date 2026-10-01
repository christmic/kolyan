use super::*;
use serde_json::json;
use sha2::{Digest, Sha256};

#[test]
fn canonical_bytes_preserve_order_escaping_numbers_and_exact_limit() {
    let cases = [
        (
            json!({"z":[],"a":{"z":false,"a":null}}),
            r#"{"a":{"a":null,"z":false},"z":[]}"#,
        ),
        (
            json!([{"z":2,"a":1},true,null,-3,1.5]),
            r#"[{"a":1,"z":2},true,null,-3,1.5]"#,
        ),
        (json!({"中文":"🦀\n\"\\"}), "{\"中文\":\"🦀\\n\\\"\\\\\"}"),
        (json!({}), "{}"),
    ];
    for (source, expected) in cases {
        let actual = bytes(&source, expected.len()).unwrap();
        assert_eq!(actual, expected.as_bytes());
        assert_eq!(Sha256::digest(&actual), Sha256::digest(expected.as_bytes()));
        assert!(
            matches!(bytes(&source, expected.len() - 1), Err(PreparedError::Invalid(message)) if message.contains("byte limit"))
        );
    }
}

#[test]
fn deeply_nested_canonicalization_uses_explicit_stack() {
    let mut source = Value::Null;
    for _ in 0..96 {
        source = json!({"z":0,"a":source});
    }
    let expected = format!("{}null{}", "{\"a\":".repeat(96), ",\"z\":0}".repeat(96));
    // Borrow the tree: its recursive Drop is outside the small-stack worker.
    let actual = std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("canonical-small-stack".into())
            .stack_size(128 * 1024)
            .spawn_scoped(scope, || bytes(&source, expected.len()))
            .unwrap()
            .join()
            .unwrap()
            .unwrap()
    });
    assert_eq!(actual, expected.as_bytes());
}

#[test]
fn large_string_and_many_entries_refuse_bounded_output() {
    for source in [json!("x".repeat(1024)), json!({"a":[1,2,3,4,5]})] {
        assert!(
            matches!(bytes(&source, 8), Err(PreparedError::Invalid(message)) if message.contains("byte limit"))
        );
    }
}

#[test]
fn preparation_schema_two_digest_matches_fixed_canonical_payload() {
    let prepared: super::super::PreparedCall = serde_json::from_value(json!({
        "call":{"id":"c","name":"file.read","arguments":{"path":"safe/a"}},
        "tool_revision":"r1",
        "claim":{"tool_name":"file.read","capabilities":["filesystem_read"],
                 "effects":["read"],"resource":{"path":"safe/a"},"idempotency":"idempotent"},
        "requirements":{"process_sandbox":true,"max_output_bytes":4096,"timeout_ms":1000},
        "execution_binding":{"z":[2,1],"a":{"b":null}},
        "digest":"not_part_of_the_payload"
    }))
    .unwrap();
    let expected = concat!(
        r#"{"call":{"arguments":{"path":"safe/a"},"id":"c","name":"file.read"},"#,
        r#""claim":{"capabilities":["filesystem_read"],"effects":["read"],"idempotency":"idempotent","resource":{"path":"safe/a"},"tool_name":"file.read"},"#,
        r#""execution_binding":{"a":{"b":null},"z":[2,1]},"#,
        r#""requirements":{"max_output_bytes":4096,"process_sandbox":true,"timeout_ms":1000},"#,
        r#""schema_version":2,"tool_revision":"r1"}"#,
    );
    assert_eq!(
        prepared.compute_digest().unwrap(),
        format!("{:x}", Sha256::digest(expected.as_bytes()))
    );
}
