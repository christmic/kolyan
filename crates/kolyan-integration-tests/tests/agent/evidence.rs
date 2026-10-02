//! Append complete evidence before comparing expectations, including on failures.

use std::{
    fs::File,
    io::Write,
    path::Path,
    sync::{
        Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::{Instant, SystemTime, UNIX_EPOCH},
};

use kolyan_agent::provider::{ContextRecord, ContextRecordError, ContextRecorder};
use serde_json::{Value, json};

pub struct Evidence {
    file: Mutex<File>,
    rows: Mutex<Vec<Value>>,
    origin: Instant,
    observations: AtomicU64,
    deferred_errors: Mutex<Vec<String>>,
}

impl Evidence {
    pub fn new(path: &Path) -> Self {
        Self {
            file: Mutex::new(File::create(path).unwrap()),
            rows: Mutex::new(vec![]),
            origin: Instant::now(),
            observations: AtomicU64::new(1),
            deferred_errors: Mutex::new(vec![]),
        }
    }

    pub fn append(&self, mut row: Value) -> Result<(), String> {
        let errors = self
            .deferred_errors
            .lock()
            .map_err(|error| error.to_string())?;
        if !errors.is_empty() {
            return Err(format!("prior Drop observation failed: {errors:?}"));
        }
        drop(errors);
        let mut file = self.file.lock().map_err(|error| error.to_string())?;
        // This timestamps observation/export, not the original ledger commit.
        let timing = json!({"elapsed_ns":self.origin.elapsed().as_nanos(),
            "unix_ms":SystemTime::now().duration_since(UNIX_EPOCH).map_err(|error| error.to_string())?.as_millis(),
            "meaning":"test_observation_not_ledger_commit"});
        row.as_object_mut()
            .ok_or("evidence row must be an object")?
            .insert("test_timing".into(), timing);
        writeln!(
            file,
            "{}",
            serde_json::to_string(&row).map_err(|error| error.to_string())?
        )
        .map_err(|error| error.to_string())?;
        file.sync_data().map_err(|error| error.to_string())?;
        self.rows
            .lock()
            .map_err(|error| error.to_string())?
            .push(row);
        Ok(())
    }

    pub fn rows(&self) -> Vec<Value> {
        assert!(
            self.deferred_errors.lock().unwrap().is_empty(),
            "Drop observation failed"
        );
        self.rows.lock().unwrap().clone()
    }

    pub fn next_observation_id(&self) -> u64 {
        self.observations.fetch_add(1, Ordering::Relaxed)
    }

    /// Never panic during future unwinding. A failed Drop write explicitly poisons
    /// later evidence operations, so a broken recorder cannot yield a passing case.
    pub fn append_from_drop(&self, row: Value) {
        if let Err(error) = self.append(row) {
            self.deferred_errors
                .lock()
                .expect("deferred evidence error lock")
                .push(error);
        }
    }
}

impl ContextRecorder for Evidence {
    fn record(&self, record: &ContextRecord) -> Result<(), ContextRecordError> {
        let row = match record {
            ContextRecord::Prepared { source, prepared } => {
                json!({"event":"context","source":source,"prepared":prepared})
            }
            ContextRecord::Rejected {
                source,
                failure,
                preparation,
            } => {
                json!({"event":"context_rejected","source":source,"error":failure.to_string(),"preparation":preparation})
            }
        };
        self.append(row)
            .map_err(|message| ContextRecordError { message })
    }
}

pub fn compare_order(rows: &[Value]) {
    let mut offset = 0;
    for line in include_str!("../expected/agent/root.jsonl").lines() {
        let expected: Value = serde_json::from_str(line).unwrap();
        let found = rows[offset..]
            .iter()
            .position(|row| row["event"] == expected["event"])
            .unwrap_or_else(|| panic!("missing ordered evidence {expected}"));
        offset += found + 1;
    }
}
