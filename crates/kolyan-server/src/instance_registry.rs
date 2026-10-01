//! Host-wide durable instance reservations, not an Agent runner or a lease.
//! The journal is authoritative; no model-selected instance or volatile counter.

use std::collections::BTreeMap;
use std::sync::Arc;

use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRef, FactSubject};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;

const MAX_RESERVATIONS: usize = 65_536;
const MAX_CAS_ATTEMPTS: usize = 32;
const PAGE_SIZE: usize = 256;
const KIND: &str = "agent.instance_reserved";

/// Trusted admission coordinates, not raw model arguments. New children must
/// receive new invocation coordinates even when their definition is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct InstanceOwner {
    pub logical_session_id: String,
    pub task_id: String,
    pub invocation_id: String,
}

/// Durable evidence returned only after persistence or exact verified replay.
/// The instance string can populate the existing `AgentIdentity.instance_id`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct InstanceReservation {
    pub instance_id: String,
    pub fact: FactRef,
}

#[derive(Debug, Error)]
pub enum InstanceRegistryError {
    #[error("instance registry journal: {0}")]
    Journal(#[from] FactError),
    #[error("invalid instance registry admission: {0}")]
    Invalid(String),
    #[error("unknown instance reservation schema: {0}")]
    UnknownSchema(u32),
    #[error("corrupt instance reservation: {0}")]
    Corrupt(String),
    #[error("instance is bound to conflicting owners: {0}")]
    ConflictingOwner(String),
    #[error("instance registry capacity exhausted")]
    Capacity,
    #[error("instance registry CAS contention exceeded its retry bound")]
    Contention,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Reserved {
    host_namespace: String,
    owner: InstanceOwner,
    instance_id: String,
}

struct Snapshot {
    head: u64,
    owners: BTreeMap<InstanceOwner, InstanceReservation>,
}

/// One permanent reservation stream per host namespace in the supplied journal.
/// All admission paths for a host must share this registry namespace/journal.
/// Methods perform blocking journal I/O; async hosts must use a blocking worker.
#[derive(Clone)]
pub struct InstanceRegistry {
    journal: Arc<dyn FactJournal>,
    host_namespace: String,
    namespace_digest: String,
    stream: String,
    capacity: usize,
}

impl InstanceRegistry {
    /// `host_namespace` must come from persistent trusted host configuration,
    /// never process randomness or a model. Changing it creates a new domain;
    /// replacing/erasing the journal loses reservation history. Capacity is a
    /// trusted ceiling (1..=65,536), not permission to reuse retired identities.
    pub fn new(
        journal: Arc<dyn FactJournal>,
        host_namespace: impl Into<String>,
        capacity: usize,
    ) -> Result<Self, InstanceRegistryError> {
        let host_namespace = host_namespace.into();
        identity(&host_namespace)?;
        if !(1..=MAX_RESERVATIONS).contains(&capacity) {
            return Err(InstanceRegistryError::Invalid(
                "capacity must be 1..=65536".into(),
            ));
        }
        let namespace_digest = digest(host_namespace.as_bytes());
        Ok(Self {
            journal,
            stream: format!("agent-instances:{namespace_digest}"),
            host_namespace,
            namespace_digest,
            capacity,
        })
    }

    /// Persist an instance before invocation admission. Identical owner retries
    /// return the original identity AND FactRef, including after reconstruction
    /// or at capacity. A different owner gets a new identity, never the old one.
    /// No release/reuse, grant creation or invocation execution occurs here.
    pub fn reserve(
        &self,
        owner: InstanceOwner,
    ) -> Result<InstanceReservation, InstanceRegistryError> {
        validate_owner(&owner)?;
        for _ in 0..MAX_CAS_ATTEMPTS {
            let snapshot = self.load()?;
            if let Some(original) = snapshot.owners.get(&owner) {
                return Ok(original.clone());
            }
            if snapshot.owners.len() >= self.capacity {
                return Err(InstanceRegistryError::Capacity);
            }
            let instance_id = self.instance_id(snapshot.head + 1);
            let draft = FactDraft {
                fact_id: self.fact_id(&owner)?,
                subject: FactSubject {
                    kind: "agent.instance".into(),
                    id: instance_id.clone(),
                },
                kind: KIND.into(),
                schema_version: 1,
                critical: true,
                causes: vec![],
                payload: serde_json::to_value(Reserved {
                    host_namespace: self.host_namespace.clone(),
                    owner: owner.clone(),
                    instance_id,
                })
                .map_err(|error| InstanceRegistryError::Invalid(error.to_string()))?,
            };
            match self
                .journal
                .append(&self.stream, snapshot.head, vec![draft])
            {
                Ok(_) => {
                    // Read/validate the authoritative record, not an unverified
                    // append response. Also detects corrupt later facts before
                    // returning an admission proof.
                    return self.load()?.owners.get(&owner).cloned().ok_or_else(|| {
                        InstanceRegistryError::Corrupt("append did not persist reservation".into())
                    });
                }
                Err(error @ (FactError::StalePosition { .. } | FactError::Conflict(_))) => {
                    // FactJournal reports a changed batch at an occupied head
                    // as Conflict, not StalePosition. Retry only real progress;
                    // a foreign fact identity at the same head is not contention.
                    if self.load()?.head <= snapshot.head {
                        return Err(error.into());
                    }
                }
                Err(error) => return Err(error.into()),
            }
        }
        Err(InstanceRegistryError::Contention)
    }

    fn load(&self) -> Result<Snapshot, InstanceRegistryError> {
        let mut snapshot = Snapshot {
            head: 0,
            owners: BTreeMap::new(),
        };
        let mut instances = BTreeMap::new();
        loop {
            let limit = PAGE_SIZE.min(self.capacity + 1 - snapshot.owners.len());
            let records = self.journal.read(&self.stream, snapshot.head, limit)?;
            if records.len() > limit {
                return Err(InstanceRegistryError::Corrupt(
                    "oversized journal page".into(),
                ));
            }
            if records.is_empty() {
                return Ok(snapshot);
            }
            for record in records {
                if snapshot.owners.len() >= self.capacity {
                    return Err(InstanceRegistryError::Capacity);
                }
                if record.stream_id != self.stream || record.position != snapshot.head + 1 {
                    return Err(InstanceRegistryError::Corrupt(
                        "foreign stream or noncontiguous position".into(),
                    ));
                }
                let draft = record.draft;
                if draft.schema_version != 1 {
                    return Err(InstanceRegistryError::UnknownSchema(draft.schema_version));
                }
                if draft.kind != KIND || !draft.critical || !draft.causes.is_empty() {
                    return Err(InstanceRegistryError::Corrupt(
                        "invalid reservation envelope".into(),
                    ));
                }
                let reserved: Reserved = serde_json::from_value(draft.payload)
                    .map_err(|error| InstanceRegistryError::Corrupt(error.to_string()))?;
                validate_owner(&reserved.owner)
                    .map_err(|error| InstanceRegistryError::Corrupt(error.to_string()))?;
                if let Some(original) = instances.get(&reserved.instance_id)
                    && original != &reserved.owner
                {
                    return Err(InstanceRegistryError::ConflictingOwner(
                        reserved.instance_id,
                    ));
                }
                if reserved.host_namespace != self.host_namespace
                    || reserved.instance_id != self.instance_id(record.position)
                    || draft.subject.kind != "agent.instance"
                    || draft.subject.id != reserved.instance_id
                    || draft.fact_id != self.fact_id(&reserved.owner)?
                {
                    return Err(InstanceRegistryError::Corrupt(
                        "reservation binding mismatch".into(),
                    ));
                }
                let proof = InstanceReservation {
                    instance_id: reserved.instance_id.clone(),
                    fact: FactRef {
                        stream_id: record.stream_id,
                        position: record.position,
                        fact_id: draft.fact_id,
                    },
                };
                instances.insert(reserved.instance_id, reserved.owner.clone());
                if snapshot.owners.insert(reserved.owner, proof).is_some() {
                    return Err(InstanceRegistryError::Corrupt(
                        "duplicate owner reservation".into(),
                    ));
                }
                snapshot.head = record.position;
            }
        }
    }

    fn instance_id(&self, position: u64) -> String {
        format!("agent-instance:{}:{position}", self.namespace_digest)
    }

    fn fact_id(&self, owner: &InstanceOwner) -> Result<String, InstanceRegistryError> {
        let bytes = serde_json::to_vec(owner)
            .map_err(|error| InstanceRegistryError::Invalid(error.to_string()))?;
        Ok(format!(
            "instance-reservation:{}:{}",
            self.namespace_digest,
            digest(&bytes)
        ))
    }
}

fn validate_owner(owner: &InstanceOwner) -> Result<(), InstanceRegistryError> {
    for value in [
        &owner.logical_session_id,
        &owner.task_id,
        &owner.invocation_id,
    ] {
        identity(value)?;
    }
    Ok(())
}

fn identity(value: &str) -> Result<(), InstanceRegistryError> {
    if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(InstanceRegistryError::Invalid(
            "identity must be nonempty, at most 256 bytes and control-free".into(),
        ));
    }
    Ok(())
}

fn digest(bytes: &[u8]) -> String {
    format!("{:x}", Sha256::digest(bytes))
}

#[cfg(test)]
mod tests;
