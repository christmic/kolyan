//! Bounded catalogs with exact revision resolution and delegation admission.

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{AgentDefinition, AgentError, AgentKey, AgentPermissions, AgentSnapshot};

/// Inline definitions use the same immutable contract as registered definitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "kind",
    content = "value",
    rename_all = "snake_case",
    deny_unknown_fields
)]
pub enum AgentSelector {
    Named(AgentKey),
    Inline(AgentDefinition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Registration {
    Inserted,
    Unchanged,
}

/// An in-memory definition catalog, not a registry of live workers or instances.
#[derive(Debug, Clone)]
pub struct AgentCatalog {
    definitions: BTreeMap<AgentKey, AgentDefinition>,
    capacity: usize,
}

impl AgentCatalog {
    /// Capacity must be 1..4096. Existing identical registrations do not consume it.
    pub fn new(capacity: usize) -> Result<Self, AgentError> {
        if !(1..=4096).contains(&capacity) {
            return Err(AgentError::Invalid(
                "catalog capacity must be 1..4096".into(),
            ));
        }
        Ok(Self {
            definitions: BTreeMap::new(),
            capacity,
        })
    }

    pub fn register(&mut self, definition: AgentDefinition) -> Result<Registration, AgentError> {
        let key = definition.key();
        if let Some(existing) = self.definitions.get(&key) {
            return if existing == &definition {
                Ok(Registration::Unchanged)
            } else {
                Err(AgentError::Conflict)
            };
        }
        if self.definitions.len() == self.capacity {
            return Err(AgentError::Capacity);
        }
        self.definitions.insert(key, definition);
        Ok(Registration::Inserted)
    }

    /// Exact discoverable targets, narrowed without creating instances or grants.
    /// Missing revisions are not advertised; registered self identities retain
    /// the separate self authority and saved-definition equality requirement.
    pub(crate) fn admitted_named_definitions(
        &self,
        parent: &AgentSnapshot,
        host: &AgentPermissions,
    ) -> Result<Vec<AgentDefinition>, AgentError> {
        let authority = parent.permissions().intersection(host)?;
        let mut definitions = Vec::new();
        for key in &authority.delegation.named_targets {
            if !self.definitions.contains_key(key) {
                continue;
            }
            match self.child_definition_ceiling(parent, &AgentSelector::Named(key.clone()), host) {
                Ok((definition, _)) => definitions.push(definition),
                Err(AgentError::PermissionDenied) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(definitions)
    }

    /// Resolve exact content and intersect host, definition and requested ceilings.
    /// Requested authority outside either ceiling is an error, not a silent trim.
    pub fn resolve(
        &self,
        selector: &AgentSelector,
        instance_id: impl Into<String>,
        host: &AgentPermissions,
        requested: &AgentPermissions,
    ) -> Result<AgentSnapshot, AgentError> {
        let definition = self.definition(selector)?;
        let ceiling = host.intersection(definition.permissions())?;
        requested.require_subset_of(&ceiling)?;
        AgentSnapshot::new(definition, instance_id.into(), requested.clone())
    }

    /// Admit a child before any model call. All authority is narrowed by the
    /// parent's effective ceiling and the current host allowance. Instance IDs
    /// remain host-issued; this catalog does not assert global uniqueness.
    pub fn resolve_child(
        &self,
        parent: &AgentSnapshot,
        selector: &AgentSelector,
        instance_id: impl Into<String>,
        host: &AgentPermissions,
        requested: &AgentPermissions,
    ) -> Result<AgentSnapshot, AgentError> {
        let (definition, ceiling) = self.child_definition_ceiling(parent, selector, host)?;
        let instance_id = instance_id.into();
        if instance_id == parent.identity().instance_id {
            return Err(AgentError::Invalid(
                "child must have a distinct instance identity".into(),
            ));
        }
        requested.require_subset_of(&ceiling)?;
        AgentSnapshot::new(definition, instance_id, requested.clone())
    }

    /// Resolve immutable intent without allocating or fabricating an instance.
    pub(crate) fn resolve_child_definition(
        &self,
        parent: &AgentSnapshot,
        selector: &AgentSelector,
        host: &AgentPermissions,
        requested: &AgentPermissions,
    ) -> Result<AgentDefinition, AgentError> {
        let (definition, ceiling) = self.child_definition_ceiling(parent, selector, host)?;
        requested.require_subset_of(&ceiling)?;
        Ok(definition)
    }

    fn child_definition_ceiling(
        &self,
        parent: &AgentSnapshot,
        selector: &AgentSelector,
        host: &AgentPermissions,
    ) -> Result<(AgentDefinition, AgentPermissions), AgentError> {
        let definition = self.definition(selector)?;
        let authority = parent.permissions().intersection(host)?;
        let is_self = definition.key() == parent.definition().key();
        let allowed = if is_self {
            authority.delegation.allow_self && definition == *parent.definition()
        } else {
            match selector {
                AgentSelector::Named(key) => authority.delegation.named_targets.contains(key),
                AgentSelector::Inline(_) => authority.delegation.allow_inline,
            }
        };
        if !allowed {
            return Err(AgentError::PermissionDenied);
        }
        let ceiling = authority.intersection(definition.permissions())?;
        Ok((definition, ceiling))
    }

    fn definition(&self, selector: &AgentSelector) -> Result<AgentDefinition, AgentError> {
        match selector {
            AgentSelector::Named(key) => {
                key.validate()?;
                self.definitions
                    .get(key)
                    .cloned()
                    .ok_or(AgentError::NotFound)
            }
            AgentSelector::Inline(definition) => {
                if let Some(existing) = self.definitions.get(&definition.key())
                    && existing != definition
                {
                    return Err(AgentError::Conflict);
                }
                Ok(definition.clone())
            }
        }
    }
}

/// Admit self-recursion from the authenticated saved snapshot, without consulting
/// a catalog. The current host, parent and original definition all bound the
/// request. A fresh host-issued instance is required; this function neither
/// allocates globally unique identities nor runs or schedules the child.
pub fn resolve_self(
    parent: &AgentSnapshot,
    instance_id: impl Into<String>,
    host: &AgentPermissions,
    requested: &AgentPermissions,
) -> Result<AgentSnapshot, AgentError> {
    let (definition, ceiling) = self_definition_ceiling(parent, host)?;
    let instance_id = instance_id.into();
    if instance_id == parent.identity().instance_id {
        return Err(AgentError::Invalid(
            "child must have a distinct instance identity".into(),
        ));
    }
    requested.require_subset_of(&ceiling)?;
    AgentSnapshot::new(definition, instance_id, requested.clone())
}

pub(crate) fn resolve_self_definition(
    parent: &AgentSnapshot,
    host: &AgentPermissions,
    requested: &AgentPermissions,
) -> Result<AgentDefinition, AgentError> {
    let (definition, ceiling) = self_definition_ceiling(parent, host)?;
    requested.require_subset_of(&ceiling)?;
    Ok(definition)
}

fn self_definition_ceiling(
    parent: &AgentSnapshot,
    host: &AgentPermissions,
) -> Result<(AgentDefinition, AgentPermissions), AgentError> {
    let definition = parent.definition();
    let ceiling = parent
        .permissions()
        .intersection(host)?
        .intersection(definition.permissions())?;
    if !ceiling.delegation.allow_self {
        return Err(AgentError::PermissionDenied);
    }
    Ok((definition.clone(), ceiling))
}

#[cfg(test)]
mod tests;
