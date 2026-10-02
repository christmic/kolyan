//! Strict test inputs; no Host configuration, authority or execution DTO shadow.
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
pub(super) enum Protocol {
    OpenAI,
    Anthropic,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Selector {
    Named,
    Inline,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Dataset {
    pub schema_version: u32,
    pub case_revision: String,
    pub initial_input: String,
    pub skills: Vec<Skill>,
    pub artifact: Artifact,
    pub cases: Vec<Case>,
    pub malicious_fixture_appendix: String,
    pub fixture_boundaries: Boundaries,
    pub network: Network,
    pub offline: Offline,
    pub evidence_cases: Vec<EvidenceCase>,
    pub host_layout: HostLayout,
    pub frames: std::collections::BTreeMap<String, Frame>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Frame {
    pub text: String,
    pub calls: Vec<Call>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Call {
    pub id: String,
    pub name: String,
    pub arguments: serde_json::Value,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct HostLayout {
    pub execution_ledger: String,
    pub fact_journal: String,
    pub sessions: String,
    pub artifacts: String,
    pub separate_skill_store: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Skill {
    pub id: String,
    pub revision: String,
    pub title: String,
    pub description: String,
    pub body: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Artifact {
    pub path: String,
    pub expected_utf8: String,
    pub source_only_marker: String,
    pub goal_checker: String,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Case {
    pub id: String,
    pub mutation: Mutation,
    pub approval: Approval,
    pub tool_error_policy: ErrorPolicy,
    pub expected: String,
    pub frames: Vec<String>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Mutation {
    None,
    RebuildAfterWriteApproval,
    RevokeSavedR2AfterWriteApproval,
    RemoveAclAfterWriteApproval,
    MaliciousBodyFixtureAndExplicitForbiddenCalls,
    RevokeAfterModelLoadCallBeforePrepare,
    RevokeAfterCompletedLoadBeforeNextStream,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum Approval {
    Never,
    WriteAlways,
    ReadAlways,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum ErrorPolicy {
    FailTurn,
    ContinueBatch,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Boundaries {
    pub strict_typed_denies_unknown_fields: bool,
    pub all_fields_drive_execution_or_explicit_validation: bool,
    pub no_knowledge_body_in_initial_model_request: bool,
    pub no_expected_payload_in_initial_model_request_or_tool_metadata: bool,
    pub no_missing_skill_binding_compatibility: bool,
    pub no_raw_grant_or_snapshot_from_model: bool,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Network {
    pub family: String,
    pub model: String,
    pub protocols: Vec<Protocol>,
    pub selectors: Vec<Selector>,
    pub operation_case: String,
    pub planned_rows: usize,
    pub attempts_per_row: usize,
    pub authorized_to_launch: bool,
    pub scripted_responses: bool,
    pub count_endpoint_calls: usize,
    pub accounting: String,
    pub model_step_limit: usize,
    pub output_tokens: u32,
    pub row_deadline_ms: u64,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct Offline {
    pub production_host: bool,
    pub actual_sdk_localhost_protocol_fixtures: bool,
    pub actual_llm: bool,
    pub sqlite: bool,
    pub native_macos_worker: bool,
    pub planned_rows: usize,
    pub rows: Vec<(Protocol, String)>,
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct EvidenceCase {
    pub id: String,
    pub mutation: EvidenceMutation,
    pub accepted: bool,
}
#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub(super) enum EvidenceMutation {
    None,
    Tail,
    Call,
    Error,
    Absent,
    Digest,
    Revision,
    Unknown,
    Retention,
    Outer,
}

pub(super) fn dataset() -> Result<Dataset, String> {
    serde_json::from_str(include_str!("../../fixtures/agent/skills_cases.json"))
        .map_err(|e| e.to_string())
}
impl Dataset {
    pub fn validate(&self) -> Result<(), String> {
        let n = &self.network;
        let o = &self.offline;
        let b = &self.fixture_boundaries;
        let h = &self.host_layout;
        let check = |condition: bool, message: &str| {
            if condition {
                Ok(())
            } else {
                Err(message.to_owned())
            }
        };
        check(
            self.schema_version == 1 && self.case_revision == "skills-c-v1",
            "dataset identity",
        )?;
        check(
            h.execution_ledger == "executions.sqlite"
                && h.fact_journal == "facts.sqlite"
                && h.sessions == "sessions"
                && h.artifacts == "artifacts"
                && !h.separate_skill_store,
            "shared actual Host stores",
        )?;
        check(
            n.family == "minimax" && n.model == "MiniMax-M3",
            "exact real deployment",
        )?;
        check(
            n.protocols.iter().copied().collect::<BTreeSet<_>>()
                == [Protocol::OpenAI, Protocol::Anthropic].into()
                && n.protocols.len() == 2,
            "protocol cardinality",
        )?;
        check(
            n.selectors.iter().copied().collect::<BTreeSet<_>>()
                == [Selector::Named, Selector::Inline].into()
                && n.selectors.len() == 2,
            "selector cardinality",
        )?;
        check(
            n.planned_rows == n.protocols.len() * n.selectors.len() && n.attempts_per_row == 1,
            "once-only network cardinality",
        )?;
        check(
            !n.authorized_to_launch && !n.scripted_responses && n.count_endpoint_calls == 0,
            "network remains held; unsupported counting",
        )?;
        check(
            n.accounting == "Unsupported count, explicit WireBytesUnknown assurance; never Trusted",
            "count assurance",
        )?;
        check(
            n.model_step_limit == 6 && n.output_tokens == 2048 && n.row_deadline_ms == 180000,
            "explicit candidate budgets",
        )?;
        check(
            o.production_host
                && o.actual_sdk_localhost_protocol_fixtures
                && !o.actual_llm
                && o.sqlite
                && o.native_macos_worker,
            "offline identity",
        )?;
        check(
            o.rows.len() == o.planned_rows && o.planned_rows == 8,
            "offline cardinality",
        )?;
        check(
            b.strict_typed_denies_unknown_fields
                && b.all_fields_drive_execution_or_explicit_validation
                && b.no_knowledge_body_in_initial_model_request
                && b.no_expected_payload_in_initial_model_request_or_tool_metadata
                && b.no_missing_skill_binding_compatibility
                && b.no_raw_grant_or_snapshot_from_model,
            "fixture boundaries",
        )?;
        let ids = self
            .cases
            .iter()
            .map(|c| c.id.as_str())
            .collect::<BTreeSet<_>>();
        check(
            ids.len() == self.cases.len() && ids.contains(n.operation_case.as_str()),
            "case identities",
        )?;
        check(
            o.rows.iter().all(|(_, id)| ids.contains(id.as_str())),
            "offline references",
        )?;
        check(
            self.cases.iter().all(|c| {
                !c.frames.is_empty() && c.frames.iter().all(|f| self.frames.contains_key(f))
            }),
            "script frame references",
        )?;
        check(
            o.rows.iter().collect::<BTreeSet<_>>().len() == o.rows.len(),
            "duplicate offline row",
        )?;
        check(self.skills.len() == 2, "two exact revisions")?;
        let keys = self
            .skills
            .iter()
            .map(|s| (&s.id, &s.revision))
            .collect::<BTreeSet<_>>();
        check(keys.len() == self.skills.len(), "duplicate Skill key")?;
        for skill in &self.skills {
            kolyan_agent::SkillKey::new(skill.id.clone(), skill.revision.clone())
                .map_err(|e| e.to_string())?;
            check(
                !skill.title.is_empty()
                    && !skill.description.is_empty()
                    && skill.body.len() <= 32768,
                "bounded Skill data",
            )?;
        }
        let selected = self.selected()?;
        check(
            selected.body.contains(
                &serde_json::to_string(&self.artifact.expected_utf8).map_err(|e| e.to_string())?,
            ),
            "exact knowledge payload recipe",
        )?;
        check(
            !self
                .initial_input
                .contains(&self.artifact.source_only_marker)
                && self.skills.iter().all(|s| {
                    !s.title.contains(&self.artifact.source_only_marker)
                        && !s.description.contains(&self.artifact.source_only_marker)
                }),
            "payload leaked before load",
        )?;
        check(
            self.artifact.path == "safe/result.txt"
                && !self.artifact.source_only_marker.is_empty()
                && self
                    .artifact
                    .expected_utf8
                    .starts_with(&self.artifact.source_only_marker)
                && self.artifact.expected_utf8.ends_with("\n\n"),
            "physical target and exact tail",
        )?;
        check(
            self.artifact.goal_checker
                == "actual FileWriteCommittedChecker with actual pinned file adapter revision",
            "real checker contract",
        )?;
        check(
            !self.malicious_fixture_appendix.is_empty(),
            "adversarial knowledge input",
        )?;
        check(
            self.evidence_cases
                .iter()
                .map(|c| &c.id)
                .collect::<BTreeSet<_>>()
                .len()
                == self.evidence_cases.len(),
            "validator case identities",
        )?;
        Ok(())
    }
    pub fn selected(&self) -> Result<&Skill, String> {
        self.skills
            .iter()
            .find(|s| s.id == "artifact-recipe" && s.revision == "r2")
            .ok_or_else(|| "selected exact r2 missing".into())
    }
}
