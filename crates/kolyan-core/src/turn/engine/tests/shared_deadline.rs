//! Restore the same sample, never remap a saved cutoff inside later phases.

use super::*;

#[test]
fn restore_uses_the_supplied_exact_monotonic_anchor() {
    for cutoff in [None, Some(1), Some(now_ms().saturating_add(5_000))] {
        let mut state = state();
        state.deadline_at_ms = cutoff;
        let saved = checkpoint(&state).checkpoint;
        let deadline = TurnDeadline::restore(cutoff).unwrap();
        let exact = deadline.instant();
        let restored = RunState::restore_with_deadline(&saved, &saved.scope, deadline).unwrap();
        assert_eq!(restored.deadline, exact);
        assert_eq!(restored.deadline_at_ms, cutoff);
        assert_eq!(restored.model_request, saved.model_request);
        assert_eq!(restored.pending, Some(saved));
    }
}

#[test]
fn mismatched_restored_cutoffs_and_relative_windows_are_refused() {
    let saved = checkpoint(&state()).checkpoint;
    for cutoff in [None, Some(1), saved.budget.deadline_at_ms.map(|n| n + 1)] {
        let result = RunState::restore_with_deadline(
            &saved,
            &saved.scope,
            TurnDeadline::restore(cutoff).unwrap(),
        );
        assert!(matches!(result, Err(TurnError::InvalidRequest { .. })));
    }
    let relative = TurnDeadline::capture(Some(Duration::from_secs(5)), None).unwrap();
    assert!(matches!(
        RunState::restore_with_deadline(&saved, &saved.scope, relative),
        Err(TurnError::Deadline(TurnDeadlineError::DurationMismatch))
    ));
}
