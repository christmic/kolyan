//! Durable lookup hints; only a complete verified admission authorizes execution.

use super::*;
use kolyan_ledger::{FactDraft, FactSubject};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ExecutionLink {
    admission: FactRef,
    child: AdmittedAgentChild,
}

fn coordinate(task: &str, invocation: &str) -> Result<String, ToolError> {
    crate::digest(&("kolyan.agent.execution-link.v1", task, invocation))
        .map(|digest| format!("agent.execution-link.{digest}"))
        .map_err(denied)
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
    pub(in crate::runner::delegation) fn save_execution_link(
        &self,
        admission: &FactRef,
        child: &AdmittedAgentChild,
    ) -> Result<(), ToolError> {
        let id = coordinate(&child.context_owner.task_id, &child.attempt.invocation_id)?;
        self.service
            .coordinator()
            .journal()
            .append(
                &id,
                0,
                vec![FactDraft {
                    fact_id: id.clone(),
                    subject: FactSubject {
                        kind: "agent.execution-link".into(),
                        id: id.clone(),
                    },
                    kind: "agent.execution-link".into(),
                    schema_version: 1,
                    critical: true,
                    causes: vec![child.binding_fact.clone(), child.instance_fact.clone()],
                    payload: serde_json::to_value(ExecutionLink {
                        admission: admission.clone(),
                        child: child.clone(),
                    })
                    .map_err(uncertain)?,
                }],
            )
            .map_err(uncertain)?;
        Ok(())
    }

    pub(super) fn execution_link(
        &self,
        task_id: &str,
        binding: &kolyan_server::AttemptBinding,
    ) -> Result<(FactRef, Admission), ToolError> {
        let id = coordinate(task_id, &binding.invocation_id)?;
        let rows = self
            .service
            .coordinator()
            .journal()
            .read(&id, 0, 2)
            .map_err(denied)?;
        let [record] = rows.as_slice() else {
            return Err(denied("missing or ambiguous child execution link"));
        };
        if record.stream_id != id
            || record.position != 1
            || record.draft.fact_id != id
            || record.draft.subject
                != (FactSubject {
                    kind: "agent.execution-link".into(),
                    id,
                })
            || record.draft.kind != "agent.execution-link"
            || record.draft.schema_version != 1
            || !record.draft.critical
        {
            return Err(denied("invalid child execution link"));
        }
        let link: ExecutionLink =
            serde_json::from_value(record.draft.payload.clone()).map_err(denied)?;
        if link.child.attempt != *binding
            || link.child.context_owner.task_id != task_id
            || !record.draft.causes.contains(&link.child.binding_fact)
            || !record.draft.causes.contains(&link.child.instance_fact)
        {
            return Err(denied("child execution link ownership differs"));
        }
        let admission = self
            .load_child_admission(&link.admission)?
            .ok_or_else(|| uncertain("execution link has no committed admission"))?;
        if admission_coordinate(&admission.owner, &admission.issued)? != link.admission
            || !admission.children.contains(&link.child)
        {
            return Err(denied("execution link differs from exact admission"));
        }
        self.inspect_child_admission(&admission.owner, &admission.issued, &admission)?;
        Ok((link.admission, admission))
    }
}
