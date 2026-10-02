//! Pure injected clock samples; no changes to real system clocks.

use super::*;
use serde::Deserialize;
use serde_json::{Value, json};
use std::io::Write;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Case {
    id: String,
    duration_ns: Option<u64>,
    unix_seconds: u64,
    unix_nanos: u32,
    span_ns: u64,
    ceiling_ms: Option<u64>,
    epoch_error: bool,
    instant_error: bool,
    expected_ms: Option<u64>,
    error: Option<String>,
}

#[test]
fn injected_clock_matrix_exports_before_physical_comparison() {
    let cases: Vec<Case> = serde_json::from_str(include_str!("tests/cases.json")).unwrap();
    let root = std::env::temp_dir().join(format!(
        "kolyan-deadline-clock-{}-{}",
        std::process::id(),
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir(&root).unwrap();
    let path = root.join("actual.jsonl");
    let mut file = std::fs::File::create(&path).unwrap();
    for case in &cases {
        let before = Instant::now();
        let duration = case.duration_ns.map(Duration::from_nanos);
        let sample = ClockSample::from_reading(
            before,
            before + Duration::from_nanos(case.span_ns),
            if case.epoch_error {
                Err(TurnDeadlineError::ClockBeforeUnixEpoch)
            } else {
                Ok(Duration::new(case.unix_seconds, case.unix_nanos))
            },
        );
        let result = sample.and_then(|sample| {
            TurnDeadline::from_sample(duration, case.ceiling_ms, sample, |instant, duration| {
                if case.instant_error {
                    None
                } else {
                    instant.checked_add(duration)
                }
            })
        });
        let mut row = match result {
            Ok(deadline) => {
                let original_instant = deadline.instant;
                let original_unix = deadline.unix_ms;
                let staged = TurnDeadline::from_sample(
                    duration,
                    None,
                    deadline.sample.clone().unwrap(),
                    |instant, duration| instant.checked_add(duration),
                )
                .unwrap()
                .tighten_absolute(case.ceiling_ms)
                .unwrap();
                let tightened = deadline
                    .clone()
                    .tighten_absolute(original_unix.map(|value| value.saturating_sub(1)))
                    .unwrap();
                let repeated = tightened
                    .clone()
                    .tighten_absolute(tightened.unix_ms)
                    .unwrap();
                let mono_delta = deadline
                    .instant
                    .map(|instant| instant.duration_since(before));
                let earliest_live = mono_delta.map(|delta| {
                    Duration::new(case.unix_seconds, case.unix_nanos)
                        .saturating_sub(Duration::from_nanos(case.span_ns))
                        .checked_add(delta)
                        .unwrap()
                        .as_nanos()
                });
                let deadline = deadline
                    .tighten_absolute(None)
                    .unwrap()
                    .tighten_absolute(original_unix.map(|value| value.saturating_add(1)))
                    .unwrap();
                json!({"id":case.id,"duration_ns":case.duration_ns,"unix_seconds":case.unix_seconds,
                    "unix_nanos":case.unix_nanos,"span_ns":case.span_ns,"ceiling_ms":case.ceiling_ms,
                    "deadline_ms":deadline.unix_ms,"error":null,
                    "mono_ns":original_instant.map(|value| value.duration_since(before).as_nanos()),
                    "staged_capture_matches":staged.instant == original_instant && staged.unix_ms == original_unix,
                    "earliest_live_unix_ns":earliest_live,
                    "later_tighten_unix_ms":tightened.unix_ms,
                    "later_tighten_mono_ns":tightened.instant.map(|value| value.duration_since(before).as_nanos()),
                    "repeated_tighten_unchanged":repeated.unix_ms == tightened.unix_ms && repeated.instant == tightened.instant,
                    "tightening_unchanged":deadline.unix_ms == original_unix,
                    "looser_tighten_mono_ns":deadline.instant.map(|value| value.duration_since(before).as_nanos()),
                    "duration_matches":deadline.validate_duration(duration).is_ok(),
                    "duration_mismatch":matches!(deadline.validate_duration(Some(Duration::from_secs(23))), Err(TurnDeadlineError::DurationMismatch))})
            }
            Err(error) => json!({"id":case.id,"deadline_ms":null,"error":format!("{error:?}")}),
        };
        row["samples"] = json!({"duration_ns":case.duration_ns,"unix_seconds":case.unix_seconds,
            "unix_nanos":case.unix_nanos,"span_ns":case.span_ns,"ceiling_ms":case.ceiling_ms,
            "epoch_error":case.epoch_error,"instant_error":case.instant_error});
        writeln!(file, "{row}").unwrap();
    }
    file.sync_all().unwrap();
    drop(file);
    let rows: Vec<Value> = std::fs::read_to_string(&path)
        .unwrap()
        .lines()
        .map(|line| serde_json::from_str(line).unwrap())
        .collect();
    println!(
        "deadline clock actual {} rows: {}",
        rows.len(),
        path.display()
    );
    assert_eq!(rows.len(), cases.len());
    for (case, row) in cases.iter().zip(rows) {
        assert_eq!(row["id"], case.id);
        assert_eq!(row["deadline_ms"], json!(case.expected_ms), "{}", case.id);
        assert_eq!(row["error"], json!(case.error), "{}", case.id);
        if case.error.is_none() {
            assert_eq!(row["tightening_unchanged"], true);
            assert_eq!(row["duration_matches"], true);
            assert_eq!(row["duration_mismatch"], true);
            assert_eq!(row["staged_capture_matches"], true, "{}", case.id);
            assert!(row["looser_tighten_mono_ns"].as_u64() <= row["mono_ns"].as_u64());
            if let Some(duration) = case.duration_ns {
                assert!(row["mono_ns"].as_u64().unwrap() <= duration);
            }
            if let Some(unix_ms) = case.expected_ms {
                if case.ceiling_ms.is_none() {
                    assert!(
                        (unix_ms as u128) * 1_000_000
                            <= row["earliest_live_unix_ns"].as_u64().unwrap() as u128,
                        "{} relative serialization must not expand the live window",
                        case.id
                    );
                }
                if let Some(ceiling) = case.ceiling_ms {
                    let bound = Duration::from_millis(ceiling)
                        .saturating_sub(Duration::new(case.unix_seconds, case.unix_nanos));
                    assert!(row["mono_ns"].as_u64().unwrap() as u128 <= bound.as_nanos());
                    if case.duration_ns.is_none() {
                        assert_eq!(unix_ms, ceiling, "{} external identity", case.id);
                    }
                }
                assert_eq!(row["later_tighten_unix_ms"], unix_ms.saturating_sub(1));
                assert_eq!(row["repeated_tighten_unchanged"], true);
                let tightened_bound = Duration::from_millis(unix_ms.saturating_sub(1))
                    .saturating_sub(Duration::new(case.unix_seconds, case.unix_nanos));
                assert!(
                    row["later_tighten_mono_ns"].as_u64().unwrap() as u128
                        <= tightened_bound.as_nanos()
                );
                assert!(
                    row["later_tighten_mono_ns"].as_u64().unwrap()
                        <= row["mono_ns"].as_u64().unwrap()
                );
            }
        }
    }
    let unlimited = TurnDeadline::capture(None, None).unwrap();
    assert_eq!(unlimited.remaining(), None);
    assert_eq!(unlimited.deadline_at_ms(), None);
    assert_eq!(
        TurnDeadline::restore(Some(0)).unwrap().remaining(),
        Some(Duration::ZERO)
    );
}
