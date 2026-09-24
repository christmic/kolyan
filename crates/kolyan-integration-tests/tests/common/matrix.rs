//! Record the entire plan before running it; isolate failures without hiding them.

use std::{future::Future, panic::AssertUnwindSafe, path::PathBuf, time::Instant};

use futures_util::FutureExt;
use serde::Serialize;

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub enum Status {
    Passed,
    Failed,
    Skipped,
    NotRun,
}

#[derive(Debug, Serialize)]
pub struct Row {
    pub label: String,
    pub status: Status,
    pub detail: String,
    pub elapsed_ms: u128,
    pub attempts: usize,
}

pub struct Matrix {
    pub rows: Vec<Row>,
    pub directory: PathBuf,
}

impl Matrix {
    pub fn new(labels: impl IntoIterator<Item = String>) -> Self {
        let directory = tempfile::Builder::new()
            .prefix("kolyan-r1-matrix-")
            .tempdir()
            .unwrap()
            .keep();
        let rows = labels
            .into_iter()
            .map(|label| Row {
                label,
                status: Status::NotRun,
                detail: "not started".into(),
                elapsed_ms: 0,
                attempts: 0,
            })
            .collect();
        let matrix = Self { rows, directory };
        matrix.save();
        eprintln!("matrix evidence: {}", matrix.directory.display());
        matrix
    }

    pub fn record(&mut self, index: usize, status: Status, detail: &str) {
        self.rows[index].status = status;
        self.rows[index].detail = detail.into();
        self.save();
    }

    pub async fn run(&mut self, index: usize, future: impl Future<Output = ()>) {
        let started = Instant::now();
        self.rows[index].attempts = 1;
        self.rows[index].detail = "running".into();
        self.save();
        let result = AssertUnwindSafe(future).catch_unwind().await;
        self.rows[index].elapsed_ms = started.elapsed().as_millis();
        match result {
            Ok(()) => self.record(index, Status::Passed, "all assertions passed"),
            Err(error) => {
                let detail = error
                    .downcast_ref::<String>()
                    .map(String::as_str)
                    .or_else(|| error.downcast_ref::<&str>().copied())
                    .unwrap_or("non-string panic");
                self.record(index, Status::Failed, detail);
            }
        }
        eprintln!("[{:?}] {}", self.rows[index].status, self.rows[index].label);
    }

    pub fn complete(&self) -> bool {
        self.rows.iter().any(|row| row.status == Status::Passed)
            && self
                .rows
                .iter()
                .all(|row| matches!(row.status, Status::Passed | Status::Skipped))
    }

    fn save(&self) {
        std::fs::write(
            self.directory.join("report.json"),
            serde_json::to_vec_pretty(&self.rows).unwrap(),
        )
        .unwrap();
    }
}
