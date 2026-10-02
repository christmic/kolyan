//! Synthetic model only; Skill artifacts, journal and execution ports are real.

use std::sync::{Arc, Mutex};

use futures_util::stream;
use kolyan_ledger::{FactDraft, FactError, FactJournal, FactRecord};
use kolyan_model::*;

use super::{Case, Mode};
use crate::runner::tests::support::Observations;
use crate::{
    AgentSnapshot, EnvironmentToolFactory, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, SerializedByteEstimator},
    provider::ContextPreparingProvider,
};

#[derive(Clone)]
pub(super) struct Journal {
    pub inner: Arc<dyn FactJournal>,
    pub streams: Arc<Mutex<std::collections::BTreeSet<String>>>,
}

impl FactJournal for Journal {
    fn read(&self, stream: &str, after: u64, limit: usize) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        self.inner.read(stream, after, limit)
    }
    fn append(
        &self,
        stream: &str,
        expected: u64,
        batch: Vec<FactDraft>,
    ) -> Result<Vec<FactRecord>, FactError> {
        self.streams.lock().unwrap().insert(stream.into());
        self.inner.append(stream, expected, batch)
    }
}

impl Journal {
    pub fn records(&self) -> Vec<FactRecord> {
        self.streams
            .lock()
            .unwrap()
            .iter()
            .flat_map(|s| self.inner.read(s, 0, 1024).unwrap())
            .collect()
    }
}

#[derive(Clone)]
pub(super) struct Factory {
    pub observations: Arc<Observations>,
    pub case: Case,
}

pub(super) struct Script(Factory, kolyan_server::ExecutionRef);
impl ModelProvider for Script {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        let mut requests = self.0.observations.requests.lock().unwrap();
        let prefix = format!("{}-step-", self.1.turn_id);
        let step = requests
            .iter()
            .filter(|r| r.request_id.starts_with(&prefix))
            .count();
        requests.push(request.clone());
        let call = if step == 0
            && matches!(self.0.case.mode, Mode::Child)
            && self.1.session_id == "session"
        {
            Some(ToolCall {
                id: "delegate-call".into(),
                name: crate::AGENT_INVOKE_NAME.into(),
                arguments: serde_json::json!({
                "parallel":false,"children":[{"target":{"kind":"named","value":{"definition_id":"knowledge-child","revision":"1"}},
                    "input":"Load your exact Skill and answer.","permissions":{"tools":[],"delegation":{"named_targets":[],"allow_inline":false,"allow_self":false}}}]}),
            })
        } else if step == 0 && !matches!(self.0.case.mode, Mode::Answer) {
            let properties = &request
                .tools
                .iter()
                .find(|d| d.name == crate::skills::SKILL_LOAD_NAME)
                .expect("fixture requires frozen Skill schema")
                .input_schema["oneOf"][0]["properties"];
            let mut arguments = serde_json::json!({"skill_id":properties["skill_id"]["const"],
                "revision":properties["revision"]["const"],"content_digest":properties["content_digest"]["const"]});
            if matches!(self.0.case.mode, Mode::WrongDigest) {
                arguments["content_digest"] = serde_json::json!("0".repeat(64));
            }
            Some(ToolCall {
                id: "skill-call".into(),
                name: crate::skills::SKILL_LOAD_NAME.into(),
                arguments,
            })
        } else if step == 1 && matches!(self.0.case.mode, Mode::Malicious) {
            Some(ToolCall {
                id: "forbidden-shell".into(),
                name: "shell".into(),
                arguments: serde_json::json!({"command":"echo unauthorized"}),
            })
        } else {
            None
        };
        Box::pin(async move {
            let response = ModelResponse {
                id: request.request_id,
                model: request.model,
                content: call
                    .iter()
                    .cloned()
                    .map(|call| ContentBlock::ToolCall { call })
                    .chain(call.is_none().then_some(ContentBlock::Text {
                        text: "synthetic final answer".into(),
                    }))
                    .collect(),
                structured_output: None,
                stop_reason: if call.is_some() {
                    StopReason::ToolUse
                } else {
                    StopReason::EndTurn
                },
                usage: TokenUsage {
                    input_tokens: Some(1),
                    output_tokens: Some(2),
                    ..Default::default()
                },
                metadata: serde_json::json!({"kind":"synthetic_module_provider_not_actual_llm"}),
            };
            Ok(Box::pin(stream::iter([
                Ok(ModelEvent::Started),
                Ok(ModelEvent::Completed(response)),
            ])) as ModelEventStream)
        })
    }
}

impl ProviderFactory for Factory {
    type Provider = Script;
    fn build(
        &self,
        snapshot: &AgentSnapshot,
        execution: &kolyan_server::ExecutionRef,
    ) -> Result<ContextPreparingProvider<Script>, RunnerError> {
        Ok(ContextPreparingProvider::new(
            Script(self.clone(), execution.clone()),
            ModelDescriptor {
                reference: snapshot.definition().model().clone(),
                context_window: Some(100000),
                max_output_tokens: Some(100),
                features: Default::default(),
            },
            ContextPolicy {
                id: "skills.fixture".into(),
                revision: "1".into(),
                mode: BudgetMode::Inspect,
                max_serialized_bytes: 65536,
                max_messages: 100,
                max_content_blocks: 200,
                context_limit_tokens: None,
                output_reserve_tokens: 50,
            },
            Arc::new(SerializedByteEstimator),
            self.observations.clone(),
        ))
    }
}

// The existing environment fixture remains unchanged. None of its effects may
// run in this dataset; knowledge is read by the actual production Skill adapter.
pub(super) fn environment(observations: Arc<Observations>) -> impl EnvironmentToolFactory {
    crate::runner::tests::support::Tools(observations, false)
}
