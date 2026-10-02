mod initialization;
mod session;

pub use initialization::SessionInitialization;

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
    /// Immutable host-bound initial projection, separate from later history.
    #[serde(deserialize_with = "initialization::required_option")]
    pub initialization: Option<SessionInitialization>,
    pub version: u64,
    pub turns: Vec<SessionTurn>,
    pub messages: Vec<Message>,
    /// Full model context including tool calls, results and signed thinking.
    pub context_messages: Vec<Message>,
    pub inputs: std::collections::BTreeMap<String, SessionTurnInput>,
    pub commits: std::collections::BTreeMap<String, Vec<Message>>,
    pub context_commits: std::collections::BTreeMap<String, Vec<Message>>,
}

/// Immutable input boundary, captured by the same write that registers a Turn.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionTurnInput {
    pub base_version: u64,
    pub history_len: usize,
    pub messages: Vec<Message>,
    pub context_policy: SessionContextPolicy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionContextPolicy {
    ConversationOnly,
    FullTrajectory,
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
    /// Atomically create the full initial context or verify an identical retry.
    /// Persistence does not authenticate the host's ownership or permission proof.
    fn initialize(
        &self,
        session_id: &str,
        initialization: &SessionInitialization,
    ) -> Result<SessionRecord, StorageError>;
    fn begin_turn_with_projection(
        &self,
        _session_id: &str,
        _turn: SessionTurn,
        _expected_version: u64,
        _messages: Vec<Message>,
        _policy: SessionContextPolicy,
    ) -> Result<SessionRecord, StorageError> {
        Err(StorageError::Conflict(
            "context projection is unsupported".into(),
        ))
    }
    fn update_turn_with_context(
        &self,
        _session_id: &str,
        _turn_id: &str,
        _status: SessionTurnStatus,
        _messages: Vec<Message>,
        _context: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        Err(StorageError::Conflict(
            "atomic context commit is unsupported".into(),
        ))
    }
    fn begin_turn_with_input(
        &self,
        _session_id: &str,
        _turn: SessionTurn,
        _expected_version: u64,
        _messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        Err(StorageError::Conflict(
            "versioned Turn input is unsupported".into(),
        ))
    }
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
