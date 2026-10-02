//! Strict candidate stdout evidence; this validates observations, not source code.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use serde::{Deserialize, Serialize};

const EVENT: &str = "candidate_invocation_state_v1";
const REQUIRED_ROWS: usize = 7;

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(super) struct CandidateObservation {
    pub event: String,
    pub schema_version: u32,
    pub state: String,
    pub before: String,
    pub after: String,
    pub results: [bool; 3],
}

#[derive(Debug, Serialize)]
pub(super) struct ObservedRow {
    pub line: usize,
    pub observation: CandidateObservation,
}

#[derive(Debug, Serialize)]
pub(super) struct ValidatedObservations {
    pub rows: Vec<ObservedRow>,
}

#[derive(Debug, Serialize)]
pub(super) struct ObservationIssue {
    pub code: String,
    pub line: Option<usize>,
    pub detail: String,
}

#[derive(Debug, Serialize)]
pub(super) struct ObservationDiagnostic {
    pub rows: Vec<ObservedRow>,
    pub issues: Vec<ObservationIssue>,
}

/// A single presentation of observation requirements for the model input.
pub(super) fn contract(expected: &[super::StateCase]) -> serde_json::Value {
    serde_json::json!({
        "row_schema": {
            "type": "object", "additionalProperties": false,
            "required": ["event", "schema_version", "state", "before", "after", "results"],
            "properties": {
                "event": {"type": "string", "const": EVENT},
                "schema_version": {"type": "integer", "const": 1},
                "state": {"type": "string", "enum": expected.iter().map(|c| &c.state).collect::<Vec<_>>()},
                "before": {"type": "string"}, "after": {"type": "string"},
                "results": {"type": "array", "items": {"type": "boolean"}, "minItems": 3, "maxItems": 3}
            }
        },
        "expected": expected, "required_rows": REQUIRED_ROWS,
        "unique_complete_states": true, "duplicate_json_fields_rejected": true,
        "before_after_equal_state": true, "each_result_matches_expected_terminal": true,
        "export_all_rows_before_any_comparison": true, "malformed_marker_lines_fail": true,
        "sourceAuditMandatory": true, "compiled_public_oracle_required": true,
        "scope": "Runtime observations do not prove source assertion ordering, strict DTOs, version checks or preservation of old tests."
    })
}

/// Ignore unrelated Cargo output, but never discard malformed marker-bearing lines.
/// The caller must export either result before comparing or rejecting acceptance.
pub(super) fn validate_candidate_observations(
    stdout: &str,
    expected: &[super::StateCase],
) -> Result<ValidatedObservations, ObservationDiagnostic> {
    let mut diagnostic = ObservationDiagnostic {
        rows: Vec::new(),
        issues: Vec::new(),
    };
    let expected_map: BTreeMap<_, _> = expected
        .iter()
        .map(|c| (c.state.as_str(), c.terminal))
        .collect();
    if expected.len() != REQUIRED_ROWS
        || expected_map.len() != REQUIRED_ROWS
        || expected_map.keys().any(|state| state.is_empty())
    {
        issue(
            &mut diagnostic,
            "invalid_expected",
            None,
            "expected data must contain seven unique nonempty states".into(),
        );
    }
    for (index, line) in stdout.lines().enumerate() {
        if !contains_marker(line) {
            continue;
        }
        match serde_json::from_str::<CandidateObservation>(line) {
            Ok(observation) => diagnostic.rows.push(ObservedRow {
                line: index + 1,
                observation,
            }),
            Err(error) => issue(
                &mut diagnostic,
                "invalid_row",
                Some(index + 1),
                error.to_string(),
            ),
        }
    }
    let mut seen = BTreeSet::new();
    let mut row_issues = Vec::new();
    for row in &diagnostic.rows {
        let o = &row.observation;
        let mut reject = |code: &str, detail: String| {
            row_issues.push(ObservationIssue {
                code: code.into(),
                line: Some(row.line),
                detail,
            })
        };
        if o.event != EVENT {
            reject("wrong_event", o.event.clone());
        }
        if o.schema_version != 1 {
            reject("wrong_version", o.schema_version.to_string());
        }
        if !seen.insert(o.state.as_str()) {
            reject("duplicate_state", o.state.clone());
        }
        match expected_map.get(o.state.as_str()) {
            None => reject("foreign_state", o.state.clone()),
            Some(terminal) => {
                if o.before != o.state || o.after != o.state {
                    reject(
                        "copy_changed",
                        format!("{}: before={}, after={}", o.state, o.before, o.after),
                    );
                }
                if o.results != [*terminal; 3] {
                    reject(
                        "results_mismatch",
                        format!("{}: observed={:?}, expected={terminal}", o.state, o.results),
                    );
                }
            }
        }
    }
    for state in expected_map.keys() {
        if !seen.contains(state) {
            row_issues.push(ObservationIssue {
                code: "missing_state".into(),
                line: None,
                detail: (*state).into(),
            });
        }
    }
    diagnostic.issues.extend(row_issues);
    if diagnostic.rows.len() != REQUIRED_ROWS {
        let count = diagnostic.rows.len();
        issue(
            &mut diagnostic,
            "row_count",
            None,
            format!("expected {REQUIRED_ROWS} rows, observed {count}"),
        );
    }
    if diagnostic.issues.is_empty() {
        Ok(ValidatedObservations {
            rows: diagnostic.rows,
        })
    } else {
        Err(diagnostic)
    }
}

fn issue(diagnostic: &mut ObservationDiagnostic, code: &str, line: Option<usize>, detail: String) {
    diagnostic.issues.push(ObservationIssue {
        code: code.into(),
        line,
        detail,
    });
}

fn contains_marker(line: &str) -> bool {
    if line.contains(EVENT) {
        return true;
    }
    // Do not collapse duplicate event keys into a Value: an escaped target marker
    // followed by another event value must still select a strictly rejected row.
    struct Probe<'a>(&'a Cell<bool>);
    impl<'de> serde::de::Visitor<'de> for Probe<'_> {
        type Value = ();
        fn expecting(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            f.write_str("an observation object")
        }
        fn visit_map<A: serde::de::MapAccess<'de>>(self, mut map: A) -> Result<(), A::Error> {
            while let Some((key, value)) = map.next_entry::<String, serde_json::Value>()? {
                if key == "event" && value.as_str() == Some(EVENT) {
                    self.0.set(true);
                }
            }
            Ok(())
        }
    }
    let selected = Cell::new(false);
    let mut decoder = serde_json::Deserializer::from_str(line);
    // This probe selects only; malformed selected input is diagnosed by the DTO.
    let _ = serde::Deserializer::deserialize_map(&mut decoder, Probe(&selected));
    selected.get()
}

#[cfg(test)]
#[path = "observations/tests.rs"]
mod tests;
