use kolyan_core::ApprovalRequest;
use kolyan_model::Message;
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("stored approval is invalid: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("approval checkpoint not found: {0}")]
    NotFound(String),
    #[error("storage record conflict: {0}")]
    Conflict(String),
    #[error("invalid approval id")]
    InvalidId,
}

pub trait ApprovalStore: Send + Sync {
    fn save(&self, request: &ApprovalRequest) -> Result<(), StorageError>;
    fn load(&self, approval_id: &str) -> Result<ApprovalRequest, StorageError>;
    fn claim(&self, approval_id: &str) -> Result<ApprovalRequest, StorageError>;
    fn delete(&self, approval_id: &str) -> Result<(), StorageError>;
}

#[derive(Debug, Clone)]
pub struct FileApprovalStore {
    root: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub version: u64,
    pub turns: Vec<SessionTurn>,
    pub messages: Vec<Message>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTurn {
    pub turn_id: String,
    pub execution_id: String,
    pub status: SessionTurnStatus,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionTurnStatus {
    Running,
    Suspended,
    Completed,
    Cancelled,
    Failed,
}

pub trait SessionStore: Send + Sync {
    fn create(&self, session_id: &str) -> Result<SessionRecord, StorageError>;
    fn load(&self, session_id: &str) -> Result<SessionRecord, StorageError>;
    fn begin_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
    ) -> Result<SessionRecord, StorageError>;
    fn update_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError>;
    fn append_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError>;
}

#[derive(Debug, Clone)]
pub struct FileSessionStore {
    root: PathBuf,
    lock: Arc<Mutex<()>>,
}

impl FileSessionStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self {
            root,
            lock: Arc::new(Mutex::new(())),
        })
    }

    fn path(&self, session_id: &str) -> Result<PathBuf, StorageError> {
        if session_id.is_empty()
            || session_id == "."
            || session_id == ".."
            || session_id.contains('/')
            || session_id.contains('\\')
        {
            return Err(StorageError::InvalidId);
        }
        Ok(self.root.join(format!("{session_id}.json")))
    }

    fn read(path: &std::path::Path, session_id: &str) -> Result<SessionRecord, StorageError> {
        let payload = fs::read(path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(session_id.to_owned())
            } else {
                StorageError::Io(error)
            }
        })?;
        Ok(serde_json::from_slice(&payload)?)
    }

    fn write(path: &std::path::Path, record: &SessionRecord) -> Result<(), StorageError> {
        let temp = path.with_extension("json.tmp");
        fs::write(&temp, serde_json::to_vec_pretty(record)?)?;
        fs::rename(temp, path)?;
        Ok(())
    }
}

impl SessionStore for FileSessionStore {
    fn create(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let path = self.path(session_id)?;
        if path.exists() {
            return Err(StorageError::Conflict(session_id.to_owned()));
        }
        let record = SessionRecord {
            session_id: session_id.to_owned(),
            version: 0,
            turns: Vec::new(),
            messages: Vec::new(),
        };
        Self::write(&path, &record)?;
        Ok(record)
    }

    fn load(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        Self::read(&self.path(session_id)?, session_id)
    }

    fn begin_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
    ) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        if record
            .turns
            .iter()
            .any(|item| item.turn_id == turn.turn_id || item.execution_id == turn.execution_id)
        {
            return Err(StorageError::Conflict(turn.turn_id));
        }
        record.turns.push(turn);
        record.version += 1;
        Self::write(&path, &record)?;
        Ok(record)
    }

    fn update_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        let turn = record
            .turns
            .iter_mut()
            .find(|item| item.turn_id == turn_id)
            .ok_or_else(|| StorageError::NotFound(turn_id.to_owned()))?;
        turn.status = status;
        record.messages.extend(messages);
        record.version += 1;
        Self::write(&path, &record)?;
        Ok(record)
    }

    fn append_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        if record
            .turns
            .iter()
            .any(|item| item.turn_id == turn.turn_id || item.execution_id == turn.execution_id)
        {
            return Err(StorageError::Conflict(turn.turn_id));
        }
        record.turns.push(turn);
        record.messages.extend(messages);
        record.version += 1;
        Self::write(&path, &record)?;
        Ok(record)
    }
}

impl FileApprovalStore {
    pub fn new(root: impl Into<PathBuf>) -> Result<Self, StorageError> {
        let root = root.into();
        fs::create_dir_all(&root)?;
        Ok(Self { root })
    }

    fn path(&self, approval_id: &str) -> Result<PathBuf, StorageError> {
        if approval_id.is_empty()
            || approval_id == "."
            || approval_id == ".."
            || approval_id.contains('/')
            || approval_id.contains('\\')
        {
            return Err(StorageError::InvalidId);
        }
        Ok(self.root.join(format!("{}.json", approval_id)))
    }

    fn claimed_path(&self, approval_id: &str) -> Result<PathBuf, StorageError> {
        Ok(self.path(approval_id)?.with_extension("claimed.json"))
    }
}

impl ApprovalStore for FileApprovalStore {
    fn save(&self, request: &ApprovalRequest) -> Result<(), StorageError> {
        let path = self.path(&request.approval_id)?;
        let temp = path.with_extension("json.tmp");
        let payload = serde_json::to_vec_pretty(request)?;
        fs::write(&temp, payload)?;
        fs::rename(temp, path)?;
        Ok(())
    }

    fn load(&self, approval_id: &str) -> Result<ApprovalRequest, StorageError> {
        let path = self.path(approval_id)?;
        let payload = fs::read(&path).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(approval_id.to_owned())
            } else {
                StorageError::Io(error)
            }
        })?;
        Ok(serde_json::from_slice(&payload)?)
    }

    fn claim(&self, approval_id: &str) -> Result<ApprovalRequest, StorageError> {
        let pending = self.path(approval_id)?;
        let claimed = self.claimed_path(approval_id)?;
        fs::rename(&pending, &claimed).map_err(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                StorageError::NotFound(approval_id.to_owned())
            } else {
                StorageError::Io(error)
            }
        })?;
        let payload = fs::read(claimed)?;
        Ok(serde_json::from_slice(&payload)?)
    }

    fn delete(&self, approval_id: &str) -> Result<(), StorageError> {
        let path = self.path(approval_id)?;
        let claimed = self.claimed_path(approval_id)?;
        let mut removed = false;
        for candidate in [path, claimed] {
            match fs::remove_file(candidate) {
                Ok(()) => removed = true,
                Err(error) if error.kind() == std::io::ErrorKind::NotFound => {}
                Err(error) => return Err(StorageError::Io(error)),
            }
        }
        if !removed {
            return Err(StorageError::NotFound(approval_id.to_owned()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests;
