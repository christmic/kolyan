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
mod tests {
    use super::*;
    use kolyan_core::{StepOutcome, StepResult};
    use kolyan_model::{
        ContentBlock, Message, MessageRole, ModelRef, ModelResponse, StopReason, TokenUsage,
    };
    use serde_json::json;

    fn request() -> ApprovalRequest {
        ApprovalRequest {
            approval_id: "approval-1".into(),
            turn_id: "turn-1".into(),
            call_id: "call-1".into(),
            tool_name: "file.write".into(),
            reason: "requires approval".into(),
            state: kolyan_core::ApprovalState::Pending,
            expires_at_ms: None,
            continuation: kolyan_core::TurnContinuation {
                continuation_id: "continuation-1".into(),
                approval_id: "approval-1".into(),
                turn_id: "turn-1".into(),
                model_request: serde_json::from_value(json!({
                    "request_id":"r", "model":{"provider":"p","model":"m"},
                    "system":[], "messages":[], "tools":[], "tool_choice":"auto",
                    "output_format":null, "prompt_cache":null, "reasoning":null,
                    "max_output_tokens":null, "extensions":{}
                }))
                .unwrap(),
                assistant_content: vec![],
                pending_calls: vec![],
                steps: vec![StepResult {
                    step_id: "s".into(),
                    response: ModelResponse {
                        id: "r".into(),
                        model: ModelRef {
                            provider: "p".into(),
                            model: "m".into(),
                        },
                        content: vec![],
                        structured_output: None,
                        stop_reason: StopReason::ToolUse,
                        usage: TokenUsage::default(),
                        metadata: json!({}),
                    },
                    outcome: StepOutcome::ToolCalls,
                }],
                max_steps: 2,
                next_step_index: 1,
                call_id: "call-1".into(),
                tool_name: "file.write".into(),
                args_fingerprint: "fp".into(),
                policy_version: "v1".into(),
                approved_call_ids: Vec::new(),
                max_tool_calls: None,
                tool_calls_used: 0,
                deadline_at_ms: None,
                tool_dispatch: Default::default(),
                tool_timeout_ms: None,
            },
        }
    }

    #[test]
    fn file_store_round_trips_and_deletes() {
        let root = std::env::temp_dir().join(format!("kolyan-storage-{}", std::process::id()));
        let store = FileApprovalStore::new(&root).unwrap();
        let value = request();
        store.save(&value).unwrap();
        assert_eq!(store.load("approval-1").unwrap(), value);
        assert_eq!(store.claim("approval-1").unwrap(), value);
        assert!(matches!(
            store.claim("approval-1"),
            Err(StorageError::NotFound(_))
        ));
        store.delete("approval-1").unwrap();
        assert!(matches!(
            store.load("approval-1"),
            Err(StorageError::NotFound(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn file_store_rejects_path_traversal() {
        let store =
            FileApprovalStore::new(std::env::temp_dir().join("kolyan-storage-invalid")).unwrap();
        assert!(matches!(
            store.load("../escape"),
            Err(StorageError::InvalidId)
        ));
    }

    #[test]
    fn file_store_rejects_corrupt_checkpoint() {
        let root =
            std::env::temp_dir().join(format!("kolyan-storage-corrupt-{}", std::process::id()));
        let store = FileApprovalStore::new(&root).unwrap();
        fs::write(root.join("approval-corrupt.json"), b"{not-json")
            .expect("corrupt fixture should be written");
        assert!(matches!(
            store.load("approval-corrupt"),
            Err(StorageError::Serialization(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }

    #[test]
    fn session_store_reopens_and_preserves_ordered_turns_and_messages() {
        let root = std::env::temp_dir().join(format!("kolyan-session-{}", std::process::id()));
        let first = FileSessionStore::new(&root).unwrap();
        assert_eq!(first.create("session-1").unwrap().version, 0);
        let message_one = Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "first turn".into(),
            }],
        };
        let message_two = Message {
            role: MessageRole::Assistant,
            content: vec![ContentBlock::Text {
                text: "first answer".into(),
            }],
        };
        let after_first = first
            .append_turn(
                "session-1",
                SessionTurn {
                    turn_id: "turn-1".into(),
                    execution_id: "execution-1".into(),
                    status: SessionTurnStatus::Completed,
                },
                vec![message_one.clone(), message_two.clone()],
            )
            .unwrap();
        assert_eq!(after_first.version, 1);
        let message_three = Message {
            role: MessageRole::User,
            content: vec![ContentBlock::Text {
                text: "second turn".into(),
            }],
        };
        first
            .append_turn(
                "session-1",
                SessionTurn {
                    turn_id: "turn-2".into(),
                    execution_id: "execution-2".into(),
                    status: SessionTurnStatus::Running,
                },
                vec![message_three.clone()],
            )
            .unwrap();
        drop(first);

        let reopened = FileSessionStore::new(&root).unwrap();
        let record = reopened.load("session-1").unwrap();
        assert_eq!(record.version, 2);
        assert_eq!(
            record
                .turns
                .iter()
                .map(|turn| turn.turn_id.as_str())
                .collect::<Vec<_>>(),
            vec!["turn-1", "turn-2"]
        );
        assert_eq!(
            record.messages,
            vec![message_one, message_two, message_three]
        );
        assert!(matches!(
            reopened.append_turn(
                "session-1",
                SessionTurn {
                    turn_id: "turn-1".into(),
                    execution_id: "execution-duplicate".into(),
                    status: SessionTurnStatus::Completed,
                },
                Vec::new(),
            ),
            Err(StorageError::Conflict(_))
        ));
        fs::remove_dir_all(root).unwrap();
    }
}
