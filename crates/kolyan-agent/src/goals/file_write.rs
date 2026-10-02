//! Deterministic commit witness selection from independently verified sources.
use std::collections::BTreeSet;
use std::path::Path;

use kolyan_policy::{Capability, Effect, Idempotency};
use kolyan_runtime::{ReceiptStatus, VerifiedEffectProof};
use kolyan_server::{
    ComputedGoalDecision, GoalChecker, GoalCheckerKey, GoalCriterion, GoalSourceCoverage,
    GoalVerdict, TaskError, VerifiedGoalSource,
};
use kolyan_tools::{ExactFileBinding, FileOperation, FileOperationResult};
use serde_json::{Value, json};
use sha2::{Digest, Sha256};

use super::FileWriteCommittedPredicateV1;
use super::predicate::{bad, directories, leaf, physical, revision, sha256};

pub struct FileWriteCommittedChecker {
    key: GoalCheckerKey,
    trusted_tool_revisions: BTreeSet<String>,
}

impl FileWriteCommittedChecker {
    /// Trust comes from exact host-assembled adapters, not names or revision prefixes.
    pub fn new(trusted_tool_revisions: BTreeSet<String>) -> Result<Self, TaskError> {
        if trusted_tool_revisions.is_empty() || trusted_tool_revisions.len() > 128 {
            return Err(bad("trusted file revision count must be 1..128"));
        }
        for value in &trusted_tool_revisions {
            revision(value)?;
        }
        let contract = json!({"schema_version":1,"implementation":"file_write_committed/v1",
            "trusted_tool_revisions":trusted_tool_revisions});
        let mut digest = Sha256::new();
        digest.update(b"kolyan.agent.goal-checker.file-write-committed\0");
        digest.update(serde_json::to_vec(&contract).map_err(|e| bad(&e.to_string()))?);
        Ok(Self {
            key: GoalCheckerKey {
                kind: "file_write_committed".into(),
                revision: format!("{:x}", digest.finalize()),
            },
            trusted_tool_revisions,
        })
    }

    fn predicate(
        &self,
        criterion: &GoalCriterion,
    ) -> Result<FileWriteCommittedPredicateV1, TaskError> {
        criterion.validate()?;
        if criterion.checker != self.key {
            return Err(bad("file checker key differs"));
        }
        let predicate: FileWriteCommittedPredicateV1 =
            serde_json::from_value(criterion.predicate.clone()).map_err(|e| bad(&e.to_string()))?;
        predicate.validate()?;
        if !self
            .trusted_tool_revisions
            .contains(&predicate.tool_revision)
        {
            return Err(bad("file goal adapter revision is not trusted"));
        }
        Ok(predicate)
    }
}

impl GoalChecker for FileWriteCommittedChecker {
    fn key(&self) -> &GoalCheckerKey {
        &self.key
    }
    fn validate_predicate(&self, criterion: &GoalCriterion) -> Result<(), TaskError> {
        self.predicate(criterion).map(|_| ())
    }
    fn assess(
        &self,
        criterion: &GoalCriterion,
        source: &VerifiedGoalSource,
    ) -> Result<ComputedGoalDecision, TaskError> {
        let predicate = self.predicate(criterion)?;
        if criterion.invocation_id != source.binding().invocation_id {
            return Err(bad("goal source invocation differs"));
        }
        let mut witnesses = Vec::new();
        let (mut failed, mut unrelated, mut mismatched) = (0, 0, 0);
        for effect in source.effects() {
            let scope = effect.scope();
            let binding = source.binding();
            if scope.execution.session_id != binding.execution.session_id
                || scope.execution.turn_id != binding.execution.turn_id
                || scope.execution.execution_id != binding.execution.execution_id
                || scope.agent_snapshot_digest.as_deref()
                    != Some(binding.constraints_digest.as_str())
            {
                return Err(bad("file witness scope differs from source binding"));
            }
            let prepared = effect.prepared();
            if prepared.call().name != "file.write"
                || prepared.tool_revision() != predicate.tool_revision
            {
                unrelated += 1;
                continue;
            }
            let matches = match_write(&predicate, effect)?;
            if effect.receipt().status != ReceiptStatus::Completed
                || effect.result().as_ref().map_or(true, |r| r.is_error)
            {
                failed += 1;
            } else if matches {
                witnesses.push(effect);
            } else {
                mismatched += 1;
            }
        }
        // Incomplete coverage never proves either success or absence, even if an
        // inspected prefix happens to contain a matching receipt.
        if source.coverage() != GoalSourceCoverage::Complete {
            return Ok(decision(
                GoalVerdict::Indeterminate,
                "historical source coverage is incomplete",
                json!({"schema_version":1,"through":source.through(),"coverage":source.coverage(),"checked":source.effects().len()}),
            ));
        }
        witnesses.sort_by(|a, b| {
            (a.sources().receipt.cursor, &a.sources().receipt.event_id)
                .cmp(&(b.sources().receipt.cursor, &b.sources().receipt.event_id))
        });
        if let Some(witness) = witnesses.first() {
            let result = witness.result().as_ref().map_err(|e| bad(&e.to_string()))?;
            let s = witness.sources();
            let coords = [
                &s.admission,
                &s.prepared,
                &s.authorized,
                &s.started,
                &s.receipt,
                &s.terminal,
            ]
            .map(|c| json!({"event_id":c.event_id,"cursor":c.cursor}));
            return Ok(decision(
                GoalVerdict::Satisfied,
                "verified historical governed write committed",
                json!({"schema_version":1,"coordinates":coords,"prepared_digest":witness.prepared().digest(),
                    "tool_result_digest":format!("{:x}",Sha256::digest(serde_json::to_vec(result).map_err(|e| bad(&e.to_string()))?)),
                    "bytes":predicate.expected_bytes,"sha256":predicate.expected_sha256}),
            ));
        }
        Ok(decision(
            GoalVerdict::Unsatisfied,
            "complete historical source has no matching committed write",
            json!({"schema_version":1,"through":source.through(),"checked":source.effects().len(),
                "failed":failed,"unrelated":unrelated,"mismatched":mismatched}),
        ))
    }
}

fn match_write(
    predicate: &FileWriteCommittedPredicateV1,
    effect: &VerifiedEffectProof,
) -> Result<bool, TaskError> {
    let p = effect.prepared();
    let operation: FileOperation =
        serde_json::from_value(json!({"name":p.call().name,"arguments":p.call().arguments}))
            .map_err(|e| bad(&e.to_string()))?;
    let FileOperation::Write(args) = operation else {
        return Err(bad("write witness operation differs"));
    };
    let binding: ExactFileBinding =
        serde_json::from_value(p.execution_binding().clone()).map_err(|e| bad(&e.to_string()))?;
    directories(&binding.workspace, &binding.parent)?;
    leaf(&binding.leaf)?;
    let target = binding.parent.physical_path.join(&binding.leaf);
    for protected in &binding.protected_roots {
        physical(protected)?;
        if target.starts_with(protected) || binding.workspace.physical_path.starts_with(protected) {
            return Err(bad("historical file binding overlaps protected root"));
        }
    }
    let claim = p.claim();
    if claim.capabilities != BTreeSet::from([Capability::FilesystemWrite])
        || claim.effects != BTreeSet::from([Effect::Create, Effect::Update])
        || claim.idempotency != Idempotency::NonIdempotent
        || claim.resource.path.as_deref() != target.to_str()
        || !p.requirements().process_sandbox
    {
        return Err(bad("historical write claim/resource/sandbox differs"));
    }
    let path = Path::new(&args.path);
    if path.is_absolute()
        || args.path.contains('\0')
        || path.components().next().is_none()
        || path
            .components()
            .any(|c| !matches!(c, std::path::Component::Normal(_)))
    {
        return Err(bad("historical write model path is not workspace-relative"));
    }
    let content_hash = format!("{:x}", Sha256::digest(args.content.as_bytes()));
    let target_matches = predicate.workspace == binding.workspace
        && predicate.parent == binding.parent
        && predicate.leaf == binding.leaf
        && predicate.expected_bytes == args.content.len() as u64
        && predicate.expected_sha256 == content_hash;
    let Ok(result) = effect.result() else {
        return Ok(false);
    };
    if result.call_id != p.call().id {
        return Err(bad("write result call ID differs"));
    }
    if result.is_error {
        return Ok(false);
    }
    // Parse the original string, not a Value: duplicate/unknown keys are corruption.
    let receipt: FileOperationResult =
        serde_json::from_str(&result.content).map_err(|e| bad(&e.to_string()))?;
    if receipt.path != args.path
        || receipt.content.is_some()
        || !sha256(&receipt.sha256)
        || receipt.bytes != args.content.len()
        || receipt.sha256 != content_hash
    {
        return Err(bad(
            "historical write result does not match prepared content",
        ));
    }
    Ok(target_matches)
}

fn decision(verdict: GoalVerdict, reason: &str, proof: Value) -> ComputedGoalDecision {
    ComputedGoalDecision {
        verdict,
        reason: reason.into(),
        proof,
    }
}
