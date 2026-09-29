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
    pub passed_attempts: usize,
    pub attempt_failures: Vec<String>,
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
                passed_attempts: 0,
                attempt_failures: vec![],
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
            Ok(()) => {
                self.rows[index].passed_attempts = 1;
                self.record(index, Status::Passed, "all assertions passed");
            }
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

    /// Retry only the test scenario, with a caller-provided fresh environment.
    /// Every failed model attempt remains visible in the matrix report.
    pub async fn run_attempts<F, Fut>(&mut self, index: usize, max_attempts: usize, mut make: F)
    where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = ()>,
    {
        assert!(max_attempts > 0);
        let started = Instant::now();
        self.rows[index].detail = "running".into();
        self.save();
        for attempt in 1..=max_attempts {
            self.rows[index].attempts = attempt;
            self.save();
            match AssertUnwindSafe(make(attempt)).catch_unwind().await {
                Ok(()) => {
                    self.rows[index].passed_attempts = 1;
                    self.rows[index].elapsed_ms = started.elapsed().as_millis();
                    self.record(
                        index,
                        Status::Passed,
                        if attempt == 1 {
                            "all assertions passed"
                        } else {
                            "passed after a recorded model retry"
                        },
                    );
                    eprintln!("[{:?}] {}", self.rows[index].status, self.rows[index].label);
                    return;
                }
                Err(error) => {
                    let detail = panic_detail(&error);
                    self.rows[index]
                        .attempt_failures
                        .push(format!("attempt {attempt}: {detail}"));
                    self.save();
                }
            }
        }
        self.rows[index].elapsed_ms = started.elapsed().as_millis();
        let detail = self.rows[index].attempt_failures.last().unwrap().clone();
        self.record(index, Status::Failed, &detail);
        eprintln!("[{:?}] {}", self.rows[index].status, self.rows[index].label);
    }

    /// Run every independent model sample and accept only the configured threshold.
    pub async fn run_samples<F, Fut>(
        &mut self,
        index: usize,
        samples: usize,
        minimum_passes: usize,
        mut make: F,
    ) where
        F: FnMut(usize) -> Fut,
        Fut: Future<Output = ()>,
    {
        assert!(samples > 0 && minimum_passes > 0 && minimum_passes <= samples);
        let started = Instant::now();
        self.rows[index].detail = "sampling".into();
        self.save();
        for sample in 1..=samples {
            self.rows[index].attempts = sample;
            match AssertUnwindSafe(make(sample)).catch_unwind().await {
                Ok(()) => self.rows[index].passed_attempts += 1,
                Err(error) => self.rows[index]
                    .attempt_failures
                    .push(format!("sample {sample}: {}", panic_detail(&error))),
            }
            self.save();
        }
        self.rows[index].elapsed_ms = started.elapsed().as_millis();
        let passed = self.rows[index].passed_attempts;
        let detail = format!("{passed}/{samples} samples passed; threshold {minimum_passes}");
        self.record(
            index,
            if passed >= minimum_passes {
                Status::Passed
            } else {
                Status::Failed
            },
            &detail,
        );
        eprintln!(
            "[{:?}] {} ({detail})",
            self.rows[index].status, self.rows[index].label
        );
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
            self.directory.join("report.next.json"),
            serde_json::to_vec_pretty(&self.rows).unwrap(),
        )
        .unwrap();
        std::fs::rename(
            self.directory.join("report.next.json"),
            self.directory.join("report.json"),
        )
        .unwrap();
    }
}

fn panic_detail(error: &Box<dyn std::any::Any + Send>) -> &str {
    error
        .downcast_ref::<String>()
        .map(String::as_str)
        .or_else(|| error.downcast_ref::<&str>().copied())
        .unwrap_or("non-string panic")
}
