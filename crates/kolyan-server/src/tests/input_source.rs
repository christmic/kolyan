//! Generic Server fixture preparation, not Agent request/body authorization.
use crate::{
    AgentIdentity, InvocationDefinition, InvocationInputEnvelope, InvocationInputKind,
    InvocationInputScope, InvocationInputSource, InvocationRole, TaskCoordinator, TaskError,
    TaskSnapshot,
};
use kolyan_ledger::{FactJournal, MemoryFactJournal};
use serde_json::json;

fn typed(kind: InvocationInputKind, fact: kolyan_ledger::FactRef) -> InvocationInputSource {
    match kind {
        InvocationInputKind::Standalone => InvocationInputSource::Standalone { fact },
        InvocationInputKind::Derived => InvocationInputSource::Derived { fact },
    }
}
/// Construct an exact reference through the Server publisher SSOT. It becomes
/// usable only after the real fixture journal receives its matching source.
pub(crate) fn fixture_source(
    task_id: &str,
    invocation_id: impl AsRef<str>,
    kind: InvocationInputKind,
) -> InvocationInputSource {
    let coordinator = TaskCoordinator::new(MemoryFactJournal::default());
    let journal = coordinator.journal();
    journal
        .append(
            "fixture.owner",
            0,
            vec![kolyan_ledger::FactDraft {
                fact_id: "fixture.owner".into(),
                subject: kolyan_ledger::FactSubject {
                    kind: "fixture.owner".into(),
                    id: "owner".into(),
                },
                kind: "fixture.owner.created".into(),
                schema_version: 1,
                critical: true,
                causes: vec![],
                payload: json!(null),
            }],
        )
        .unwrap();
    let result = coordinator
        .publish_invocation_input_source(
            InvocationInputEnvelope {
                kind,
                scope: InvocationInputScope {
                    task_id: task_id.into(),
                    invocation_id: invocation_id.as_ref().into(),
                    agent: AgentIdentity {
                        definition_id: "fixture".into(),
                        revision: "r1".into(),
                        instance_id: "fixture".into(),
                    },
                    constraints_digest: "a".repeat(64),
                },
                body: json!({"meaning":"reference preparation only, not Agent authorization"}),
            },
            vec![kolyan_ledger::FactRef {
                stream_id: "fixture.owner".into(),
                position: 1,
                fact_id: "fixture.owner".into(),
            }],
        )
        .unwrap();
    typed(kind, result.reference)
}

pub(crate) trait SourceFixtureAdmission {
    fn admit_fixture(
        &self,
        task: &str,
        command: &str,
        definition: InvocationDefinition,
    ) -> Result<TaskSnapshot, TaskError>;
}
impl<J: FactJournal> SourceFixtureAdmission for TaskCoordinator<J> {
    fn admit_fixture(
        &self,
        task: &str,
        command: &str,
        mut definition: InvocationDefinition,
    ) -> Result<TaskSnapshot, TaskError> {
        let records = self.records(task)?;
        let registered = records
            .first()
            .ok_or_else(|| TaskError::Invalid("missing fixture Task registration".into()))?;
        let mut causes = vec![kolyan_ledger::FactRef {
            stream_id: registered.stream_id.clone(),
            position: registered.position,
            fact_id: registered.draft.fact_id.clone(),
        }];
        if definition.role == InvocationRole::Continuation {
            if definition
                .parent_invocation_id
                .as_ref()
                .is_none_or(|parent| !definition.dependencies.contains(parent))
            {
                return self.admit_invocation(task, command, definition);
            }
            let state = self.snapshot(task)?;
            let completed = definition
                .parent_invocation_id
                .as_ref()
                .and_then(|id| state.invocations.get(id))
                .and_then(|inv| inv.completion_fact.clone());
            let Some(completed) = completed else {
                return self.admit_invocation(task, command, definition);
            };
            causes.push(completed);
        }
        let kind = definition.input_source.kind();
        let result = self.publish_invocation_input_source(InvocationInputEnvelope { kind, scope: InvocationInputScope { task_id: task.into(), invocation_id: definition.invocation_id.clone(), agent: definition.agent.clone(), constraints_digest: definition.constraints_digest.clone() }, body: json!({"meaning":"generic Server typed source fixture; Agent validation is outside this test"}) },causes)?;
        definition.input_source = typed(kind, result.reference);
        self.admit_invocation(task, command, definition)
    }
}
