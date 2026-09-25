//! Atomic Session snapshots and versioned Turn input boundaries.

use super::*;

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
        use std::io::Write;
        let mut file = fs::File::create(&temp)?;
        file.write_all(&serde_json::to_vec_pretty(record)?)?;
        file.sync_all()?;
        fs::rename(temp, path)?;
        if let Some(parent) = path.parent() {
            fs::File::open(parent)?.sync_all()?;
        }
        Ok(())
    }

    fn file_lock(&self) -> Result<fs::File, StorageError> {
        let file = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .open(self.root.join(".sessions.lock"))?;
        file.lock()?;
        Ok(file)
    }

    fn begin(
        &self,
        session_id: &str,
        turn: SessionTurn,
        input: Option<(u64, Vec<Message>, SessionContextPolicy)>,
    ) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let _file_lock = self.file_lock()?;
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        if record.turns.iter().any(|item| {
            item.turn_id == turn.turn_id
                || item.execution_id == turn.execution_id
                || matches!(
                    item.status,
                    SessionTurnStatus::Running | SessionTurnStatus::Suspended
                )
        }) {
            return Err(StorageError::Conflict(
                "session has an active or duplicate Turn".into(),
            ));
        }
        if let Some((version, messages, context_policy)) = input {
            if record.version != version {
                return Err(StorageError::Conflict(
                    "session context version changed".into(),
                ));
            }
            record.inputs.insert(
                turn.turn_id.clone(),
                SessionTurnInput {
                    base_version: version,
                    history_len: match context_policy {
                        SessionContextPolicy::ConversationOnly => record.messages.len(),
                        SessionContextPolicy::FullTrajectory => record.context_messages.len(),
                    },
                    messages,
                    context_policy,
                },
            );
        }
        record.turns.push(turn);
        record.version += 1;
        Self::write(&path, &record)?;
        Ok(record)
    }
}

impl SessionStore for FileSessionStore {
    fn create(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let _file_lock = self.file_lock()?;
        let path = self.path(session_id)?;
        if path.exists() {
            return Err(StorageError::Conflict(session_id.to_owned()));
        }
        let record = SessionRecord {
            session_id: session_id.to_owned(),
            version: 0,
            turns: Vec::new(),
            messages: Vec::new(),
            context_messages: Vec::new(),
            inputs: Default::default(),
            commits: Default::default(),
            context_commits: Default::default(),
        };
        Self::write(&path, &record)?;
        Ok(record)
    }

    fn load(&self, session_id: &str) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let _file_lock = self.file_lock()?;
        Self::read(&self.path(session_id)?, session_id)
    }

    fn begin_turn(
        &self,
        session_id: &str,
        turn: SessionTurn,
    ) -> Result<SessionRecord, StorageError> {
        self.begin(session_id, turn, None)
    }

    fn begin_turn_with_input(
        &self,
        session_id: &str,
        turn: SessionTurn,
        expected_version: u64,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.begin_turn_with_projection(
            session_id,
            turn,
            expected_version,
            messages,
            SessionContextPolicy::FullTrajectory,
        )
    }

    fn begin_turn_with_projection(
        &self,
        session_id: &str,
        turn: SessionTurn,
        expected_version: u64,
        messages: Vec<Message>,
        policy: SessionContextPolicy,
    ) -> Result<SessionRecord, StorageError> {
        self.begin(session_id, turn, Some((expected_version, messages, policy)))
    }

    fn update_turn(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        self.update_turn_with_context(session_id, turn_id, status, messages.clone(), messages)
    }

    fn update_turn_with_context(
        &self,
        session_id: &str,
        turn_id: &str,
        status: SessionTurnStatus,
        messages: Vec<Message>,
        context: Vec<Message>,
    ) -> Result<SessionRecord, StorageError> {
        let _guard = self.lock.lock().expect("session lock must not be poisoned");
        let _file_lock = self.file_lock()?;
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        let turn = record
            .turns
            .iter_mut()
            .find(|item| item.turn_id == turn_id)
            .ok_or_else(|| StorageError::NotFound(turn_id.to_owned()))?;
        if turn.status == status
            && record.commits.get(turn_id) == Some(&messages)
            && record.context_commits.get(turn_id) == Some(&context)
        {
            return Ok(record);
        }
        if matches!(
            turn.status,
            SessionTurnStatus::Completed | SessionTurnStatus::Cancelled | SessionTurnStatus::Failed
        ) {
            return Err(StorageError::Conflict(
                "terminal Turn commit cannot change".into(),
            ));
        }
        turn.status = status;
        record.commits.insert(turn_id.into(), messages.clone());
        record
            .context_commits
            .insert(turn_id.into(), context.clone());
        record.context_messages.extend(context);
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
        let _file_lock = self.file_lock()?;
        let path = self.path(session_id)?;
        let mut record = Self::read(&path, session_id)?;
        if record.turns.iter().any(|item| {
            item.turn_id == turn.turn_id
                || item.execution_id == turn.execution_id
                || matches!(
                    item.status,
                    SessionTurnStatus::Running | SessionTurnStatus::Suspended
                )
        }) {
            return Err(StorageError::Conflict(turn.turn_id));
        }
        record.turns.push(turn);
        record.context_messages.extend(messages.clone());
        record.messages.extend(messages);
        record.version += 1;
        Self::write(&path, &record)?;
        Ok(record)
    }
}
