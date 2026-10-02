//! Host rejection closes Task admission without inventing execution outcomes.

mod tests;

use std::path::Path;

use kolyan_ledger::{FactJournal, SqliteFactJournal};
use kolyan_server::{TaskCoordinator, TaskError, TaskState};
use serde::Serialize;
use serde_json::json;

use crate::evidence::Evidence;

#[derive(Debug, Serialize)]
#[serde(tag = "outcome", rename_all = "snake_case")]
enum Closure {
    NotAdmitted,
    TerminalPreserved { state: TaskState },
    Failed { fact_id: String, state: TaskState },
}

pub(super) fn record(
    control: &Path,
    task_id: &str,
    original_error: &str,
    evidence: &Evidence,
) -> Result<(), String> {
    let path = control.join("state/ledger.sqlite");
    let result = if path.try_exists().map_err(|error| error.to_string())? {
        SqliteFactJournal::open(path)
            .map_err(|error| error.to_string())
            .and_then(|journal| close(&TaskCoordinator::new(journal), task_id, original_error))
    } else {
        // Baseline rejection is not Task admission; do not create empty stores.
        Ok(Closure::NotAdmitted)
    };
    evidence.append(
        json!({"event":"self_iteration_rejection_closure", "task_id":task_id,
        "original_error":original_error, "result":result,
        "execution_outcomes_synthesized":false}),
    )?;
    result.map(|_| ())
}

fn close<J: FactJournal>(
    coordinator: &TaskCoordinator<J>,
    task_id: &str,
    reason: &str,
) -> Result<Closure, String> {
    let before = match coordinator.snapshot(task_id) {
        Ok(snapshot) => snapshot,
        Err(TaskError::NotFound(_)) => return Ok(Closure::NotAdmitted),
        Err(error) => return Err(error.to_string()),
    };
    if before.state.is_terminal() {
        return Ok(Closure::TerminalPreserved {
            state: before.state,
        });
    }
    let fact_id = format!("{task_id}/self-iteration-host/rejected");
    let committed = coordinator
        .fail_task(task_id, &fact_id, reason)
        .map_err(|error| error.to_string())?;
    let observed = coordinator
        .snapshot(task_id)
        .map_err(|error| error.to_string())?;
    if observed.state != TaskState::Failed
        || observed != committed
        || observed.invocations != before.invocations
        || observed.attempts != before.attempts
    {
        return Err("durable rejection changed execution outcomes or did not close Task".into());
    }
    Ok(Closure::Failed {
        fact_id,
        state: observed.state,
    })
}
