use kolyan_core::ApprovalRequest;
use std::fs;
use std::path::PathBuf;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("storage I/O failed: {0}")]
    Io(#[from] std::io::Error),
    #[error("stored approval is invalid: {0}")]
    Serialization(#[from] serde_json::Error),
    #[error("approval checkpoint not found: {0}")]
    NotFound(String),
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
    use kolyan_model::{ModelRef, ModelResponse, StopReason, TokenUsage};
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
}
