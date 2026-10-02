//! Create one fully bound context snapshot. Storage compares the host's opaque
//! digest and exact projection; the host authenticates ownership and permissions.

use super::*;

const MAX_INITIALIZATION_BYTES: usize = 16 * 1024 * 1024;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SessionInitialization {
    pub binding_digest: String,
    pub messages: Vec<Message>,
}

impl SessionInitialization {
    pub fn validate(&self) -> Result<(), StorageError> {
        if self.binding_digest.len() != 64
            || !self
                .binding_digest
                .bytes()
                .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        {
            return Err(StorageError::Conflict(
                "invalid context binding digest".into(),
            ));
        }
        if serde_json::to_vec(self)?.len() > MAX_INITIALIZATION_BYTES {
            return Err(StorageError::Conflict(
                "initial context exceeds 16 MiB".into(),
            ));
        }
        Ok(())
    }
}

impl FileSessionStore {
    pub(super) fn initialize_context(
        &self,
        session_id: &str,
        initialization: &SessionInitialization,
    ) -> Result<SessionRecord, StorageError> {
        initialization.validate()?;
        let _guard = self
            .lock
            .lock()
            .map_err(|_| StorageError::Conflict("session lock poisoned".into()))?;
        let _file_lock = self.file_lock()?;
        let path = self.path(session_id)?;
        match Self::read(&path, session_id) {
            Ok(record) => {
                if record.session_id != session_id
                    || record.initialization.as_ref() != Some(initialization)
                {
                    return Err(StorageError::Conflict(
                        "context initialization differs from saved binding".into(),
                    ));
                }
                // Never replace subsequently committed messages or running Turns.
                return Ok(record);
            }
            Err(StorageError::NotFound(_)) => {}
            Err(error) => return Err(error),
        }
        let record = SessionRecord {
            session_id: session_id.into(),
            initialization: Some(initialization.clone()),
            version: 0,
            turns: vec![],
            messages: initialization.messages.clone(),
            context_messages: initialization.messages.clone(),
            inputs: Default::default(),
            commits: Default::default(),
            context_commits: Default::default(),
        };
        Self::write(&path, &record)?;
        Ok(record)
    }
}

pub(super) fn required_option<'de, D, T>(deserializer: D) -> Result<Option<T>, D::Error>
where
    D: serde::Deserializer<'de>,
    T: Deserialize<'de>,
{
    Option::<T>::deserialize(deserializer)
}

#[cfg(test)]
mod tests;
