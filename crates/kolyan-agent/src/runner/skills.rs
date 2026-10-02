//! One source-bound Skill assembly path for initial and restored executions.

use super::*;
use crate::{AgentInvocationBinding, SkillScope, VerifiedSkillBinding};
use kolyan_ledger::FactRef;
use kolyan_server::InvocationInputSource;
use serde::{Deserialize, Deserializer};

/// Custom deserializer makes nullable required: a missing field is an error.
pub(super) fn required_binding<'de, D: Deserializer<'de>>(
    deserializer: D,
) -> Result<Option<FactRef>, D::Error> {
    let value = serde_json::Value::deserialize(deserializer)?;
    if value.is_null() {
        return Ok(None);
    }
    let reference: FactRef =
        serde_json::from_value(value.clone()).map_err(serde::de::Error::custom)?;
    if serde_json::to_value(&reference).map_err(serde::de::Error::custom)? != value {
        return Err(serde::de::Error::custom("unknown Skill reference fields"));
    }
    Ok(Some(reference))
}

pub(super) fn causes(mut original: Vec<FactRef>, binding: Option<&FactRef>) -> Vec<FactRef> {
    if let Some(reference) = binding {
        original.push(reference.clone());
    }
    original
}

impl<J, L, S, SS, P, T> AgentRunner<J, L, S, SS, P, T>
where
    J: FactJournal + 'static,
    L: LedgerStore + Clone + 'static,
    S: TraceSink + Clone + 'static,
    SS: SessionStore + Clone + 'static,
    P: ProviderFactory,
    T: EnvironmentToolFactory,
{
    pub(super) fn prepare_skills(
        &self,
        saved: &AgentInvocationBinding,
        ownership: &FactRef,
    ) -> Result<Option<VerifiedSkillBinding>, RunnerError> {
        self.skills
            .as_ref()
            .map(|runtime| {
                let scope = SkillScope::from_binding(saved).map_err(skill_error)?;
                let advertisement = runtime
                    .discover(&saved.snapshot, &scope)
                    .map_err(skill_error)?;
                runtime.bind(&advertisement, ownership).map_err(skill_error)
            })
            .transpose()
    }

    pub(super) fn verify_skill_origin(
        &self,
        saved: &AgentInvocationBinding,
        reference: Option<&FactRef>,
        source_causes: &[FactRef],
    ) -> Result<Option<VerifiedSkillBinding>, RunnerError> {
        let Some(reference) = reference else {
            return Ok(None);
        };
        if source_causes
            .iter()
            .filter(|cause| *cause == reference)
            .count()
            != 1
        {
            return Err(RunnerError::Host(
                "Skill binding is not an exact unique source cause".into(),
            ));
        }
        let runtime = self.skills.as_ref().ok_or_else(|| {
            RunnerError::Host("saved Skill source requires host configuration".into())
        })?;
        runtime
            .restore_binding(
                reference,
                &SkillScope::from_binding(saved).map_err(skill_error)?,
            )
            .map(Some)
            .map_err(skill_error)
    }

    pub(super) fn restored_skills(
        &self,
        saved: &AgentInvocationBinding,
        source: &InvocationInputSource,
    ) -> Result<Option<VerifiedSkillBinding>, RunnerError> {
        // The role-specific readers retain ownership of the full source schema.
        // This shared seam only reads its mandatory nullable Skill reference.
        let (proof, document): (_, serde_json::Value) = self.load_input_document(saved, source)?;
        let value = document
            .get("skill_binding")
            .ok_or_else(|| RunnerError::Host("source is missing mandatory skill_binding".into()))?;
        let reference = required_binding(value.clone())
            .map_err(|error| RunnerError::Host(error.to_string()))?;
        let binding = self.verify_skill_origin(saved, reference.as_ref(), &proof.causes)?;
        Ok(binding)
    }

    pub(super) fn skill_executor(
        &self,
        binding: Option<&VerifiedSkillBinding>,
        execution: &ExecutionRef,
        policy: Arc<PolicyEngine>,
    ) -> Result<Option<crate::skills::SkillExecutor>, RunnerError> {
        binding
            .map(|binding| {
                crate::skills::SkillExecutor::new(
                    self.skills
                        .clone()
                        .ok_or_else(|| RunnerError::Host("Skill runtime missing".into()))?,
                    binding.clone(),
                    execution.clone(),
                    policy,
                )
                .map_err(skill_error)
            })
            .transpose()
    }
}

fn skill_error(error: crate::SkillError) -> RunnerError {
    RunnerError::Host(error.to_string())
}
