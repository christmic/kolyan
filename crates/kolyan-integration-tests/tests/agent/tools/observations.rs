//! Test observation classification, not native-worker timeout attribution.

use std::{
    future::{Future, pending},
    task::{Context, Waker},
};

use super::{Evidence, Observation};
use serde_json::{Value, json};

fn evidence() -> (tempfile::TempDir, std::sync::Arc<Evidence>) {
    let directory = tempfile::tempdir().unwrap();
    let evidence = std::sync::Arc::new(Evidence::new(&directory.path().join("actual.jsonl")));
    (directory, evidence)
}

#[test]
fn dropped_unpolled_and_pending_futures_have_distinct_observations() {
    let (_directory, evidence) = evidence();
    let span = Observation::new(
        evidence.clone(),
        "prepare",
        json!({"call_id":"unpolled"}),
        None,
    );
    let future = async move {
        let mut span = span;
        span.polled();
        pending::<()>().await;
    };
    drop(future);
    let span = Observation::new(
        evidence.clone(),
        "execute",
        json!({"call_id":"pending"}),
        None,
    );
    let mut future = Box::pin(async move {
        let mut span = span;
        span.polled();
        pending::<()>().await;
    });
    assert!(
        future
            .as_mut()
            .poll(&mut Context::from_waker(Waker::noop()))
            .is_pending()
    );
    drop(future);
    let rows = evidence.rows();
    let dropped: Vec<&Value> = rows
        .iter()
        .filter(|row| row["stage"] == "dropped")
        .collect();
    assert_eq!(dropped.len(), 2);
    assert_eq!(dropped[0]["outcome"]["completion"], "dropped_before_poll");
    assert_eq!(dropped[0]["polled"], false);
    assert_eq!(dropped[1]["outcome"]["completion"], "dropped_while_pending");
    assert_eq!(dropped[1]["polled"], true);
    assert_ne!(dropped[0]["observation_id"], dropped[1]["observation_id"]);
}

#[test]
fn returned_timeout_is_not_labeled_outer_future_drop_and_control_is_observed() {
    let (_directory, evidence) = evidence();
    let control = kolyan_core::TurnControl::default();
    let mut span = Observation::new(
        evidence.clone(),
        "execute",
        json!({"call_id":"timeout"}),
        Some(control.clone()),
    );
    span.polled();
    control.cancel();
    span.returned(json!({"status":"error","error_kind":"TimedOut"}));
    drop(span);
    let rows = evidence.rows();
    assert_eq!(
        rows.iter()
            .map(|row| row["stage"].as_str().unwrap())
            .collect::<Vec<_>>(),
        ["created", "polled", "returned", "dropped"]
    );
    assert_eq!(rows[2]["outcome"]["error_kind"], "TimedOut");
    assert_eq!(rows[3]["outcome"]["completion"], "returned");
    assert_eq!(rows[3]["inner_returned"], true);
    assert_eq!(rows[3]["control_cancelled"], true);
    assert!(
        rows.windows(2)
            .all(|rows| rows[0]["elapsed_ns"].as_u64().unwrap()
                <= rows[1]["elapsed_ns"].as_u64().unwrap())
    );
    assert!(
        rows.iter()
            .all(|row| row["test_timing"]["meaning"] == "test_observation_not_ledger_commit")
    );
}
