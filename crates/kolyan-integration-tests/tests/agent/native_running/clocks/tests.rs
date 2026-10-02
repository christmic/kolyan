//! Clock arithmetic is deterministic and does not start real network requests.

use super::*;
use serde::Deserialize;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    captured_after_ms: u64,
    cleanup_after_ms: u64,
    expected_phase_ms: u64,
    expected_cleanup_ms: u64,
}

#[test]
fn model_phase_and_cleanup_have_independent_origins() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("cases.json")).unwrap();
    let mut dataset: Dataset = serde_json::from_str(include_str!("../data/cases.json")).unwrap();
    dataset.model_timeout_ms = 100;
    dataset.gate_timeout_ms = 10;
    dataset.cleanup_timeout_ms = 20;
    let origin = Instant::now();
    let clocks = Clocks::new(&dataset, origin);
    assert_eq!(clocks.model_deadline, origin + Duration::from_millis(100));
    for case in cases {
        let phase = clocks.phase_deadline(origin + Duration::from_millis(case.captured_after_ms));
        let cleanup =
            clocks.cleanup_deadline(origin + Duration::from_millis(case.cleanup_after_ms));
        assert_eq!(
            phase,
            origin + Duration::from_millis(case.expected_phase_ms),
            "{}",
            case.id
        );
        assert_eq!(
            cleanup,
            origin + Duration::from_millis(case.expected_cleanup_ms),
            "{}",
            case.id
        );
    }
}
