//! Generation binding remains strict without promoting unsupported count profiles.
use super::*;
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct GenerationRow {
    mode: String,
    expected_generation: bool,
    expected_count: bool,
}
#[test]
fn generation_and_count_binding_are_separate_and_bounded() {
    let rows: Vec<GenerationRow> =
        serde_json::from_str(include_str!("generation_cases.json")).unwrap();
    let dir = tempfile::Builder::new()
        .prefix("kolyan-generation-binding-")
        .tempdir()
        .unwrap()
        .keep();
    let mut file = std::fs::File::create(dir.join("actual.jsonl")).unwrap();
    for row in &rows {
        let request: ModelRequest = serde_json::from_value(json!({"request_id":"fixture",
            "model":{"provider":"fixture","model":"m"},"system":[],"messages":[],
            "tools":[],"tool_choice":"auto","extensions":null}))
        .unwrap();
        let owner = Arc::new(());
        let identity = MappingIdentity::new(
            endpoint_identity("localhost", "v1"),
            ContextProtocol::OpenAiResponses,
            request.model.clone(),
            "mapper-v1".into(),
            "coverage-v1".into(),
        )
        .unwrap();
        let profile = if row.mode == "unsupported" || row.mode == "incomplete_coverage" {
            CountProfile::default()
        } else {
            CountProfile::registered(identity.clone(), "counter-v1".into()).unwrap()
        };
        let coverage = CountCoverage::new(if row.mode == "incomplete_coverage" {
            vec!["future".into()]
        } else {
            vec![]
        });
        let mut wire = PreparedContextWire::new(
            owner.clone(),
            identity.clone(),
            profile.clone(),
            &request,
            json!({"model":"m","input":"中文🦀","stream":true}),
            json!({"model":"m","input":"中文🦀"}),
            coverage,
        )
        .unwrap();
        let mut target_owner = owner.clone();
        let mut target_identity = identity.clone();
        let mut target_profile = profile.clone();
        match row.mode.as_str() {
            "foreign_owner" => target_owner = Arc::new(()),
            "profile_changed" => {
                target_profile =
                    CountProfile::registered(identity.clone(), "counter-v2".into()).unwrap()
            }
            "identity_changed" => target_identity.mapping_revision = "changed".into(),
            "wire_digest" => wire.generation_wire_digest = "wrong".into(),
            "count_digest" => wire.count_input_digest = "wrong".into(),
            "wire_bytes" => wire.generation_wire_bytes += 1,
            _ => {}
        }
        let generation =
            wire.verify_generation_for(&target_owner, &target_identity, &target_profile);
        let count = wire.verify_for(&target_owner, &target_identity, &target_profile);
        writeln!(file,"{}",json!({"mode":row.mode,"input":request,"identity":identity,
            "profile":profile,"target_identity":target_identity,"target_profile":target_profile,
            "owner":{"same":Arc::ptr_eq(&owner,&target_owner)},"body":wire.generation_body(),
            "count_body":wire.count_body(),"wire_digest":wire.generation_wire_digest(),
            "count_digest":wire.count_input_digest(),"generation_bytes":wire.generation_wire_bytes(),
            "generation_ok":generation.is_ok(),"count_ok":count.is_ok(),
            "generation_error":generation.err().map(|e|e.to_string()),
            "count_error":count.err().map(|e|e.to_string())})).unwrap();
    }
    file.flush().unwrap();
    file.sync_all().unwrap();
    drop(file);
    let actual: Vec<Value> = std::fs::read_to_string(dir.join("actual.jsonl"))
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    eprintln!("generation binding evidence: {}", dir.display());
    assert_eq!(actual.len(), rows.len());
    for (row, actual) in rows.iter().zip(actual) {
        assert_eq!(actual["mode"], row.mode);
        assert_eq!(
            actual["generation_ok"], row.expected_generation,
            "{}",
            row.mode
        );
        assert_eq!(actual["count_ok"], row.expected_count, "{}", row.mode);
    }
}
