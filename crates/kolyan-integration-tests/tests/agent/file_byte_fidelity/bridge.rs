//! Test-only bridge between public generic Runtime and actual async OS adapter.
//! No model, simulated result, synthetic file implementation or fabricated grant.

use std::sync::{Arc, Mutex};

use kolyan_core::{ToolExecutor, ToolInvocation, ToolOutcome};
use kolyan_runtime::{
    AdmissionDecision, AdmissionPort, EffectExecutor, EffectGrant, EffectOutcome, EffectReceipt,
    EffectRequest, ExecutionKey, ReceiptStatus, RuntimeExecutionError,
};
use kolyan_tools::IsolatedFileTools;
use serde_json::json;

use super::{Evidence, digest};

#[derive(Clone)]
pub(super) struct Bridge {
    pub request: EffectRequest,
    key: ExecutionKey,
    authorization: EffectGrant,
    invocation: Arc<Mutex<Option<ToolInvocation>>>,
    tools: Arc<IsolatedFileTools>,
    runtime: tokio::runtime::Handle,
    revision: String,
    evidence: Arc<Evidence>,
}

impl Bridge {
    pub fn new(
        tools: Arc<IsolatedFileTools>,
        invocation: ToolInvocation,
        evidence: Arc<Evidence>,
    ) -> Result<Self, String> {
        invocation
            .grant
            .validate(
                &invocation.prepared,
                &invocation.policy_revision,
                &invocation.scope,
            )
            .map_err(|e| e.to_string())?;
        let request = EffectRequest {
            effect_id: invocation.prepared.call().id.clone(),
            operation_kind: invocation.prepared.call().name.clone(),
            input_digest: invocation.prepared.digest().into(),
            requirements: vec!["process_sandbox".into()],
            policy_revision: invocation.policy_revision.clone(),
        };
        let authorization = EffectGrant {
            authorization_id: format!(
                "{}:{}:authorized",
                invocation.scope.execution.execution_id, request.effect_id
            ),
            effect_id: request.effect_id.clone(),
            input_digest: request.input_digest.clone(),
            constraints_digest: digest(
                &serde_json::to_vec(invocation.grant.constraints()).map_err(|e| e.to_string())?,
            ),
            authority_revision: request.policy_revision.clone(),
        };
        Ok(Self {
            key: invocation.scope.execution.clone(),
            revision: invocation.prepared.tool_revision().into(),
            request,
            authorization,
            invocation: Arc::new(Mutex::new(Some(invocation))),
            tools,
            runtime: tokio::runtime::Handle::current(),
            evidence,
        })
    }

    fn validate(
        &self,
        key: &ExecutionKey,
        request: &EffectRequest,
    ) -> Result<(), RuntimeExecutionError> {
        if key != &self.key || request != &self.request {
            return Err(RuntimeExecutionError::Invalid(
                "bridge execution/request differs from exact issued tool authority".into(),
            ));
        }
        Ok(())
    }
}

impl AdmissionPort for Bridge {
    fn decide(
        &self,
        key: &ExecutionKey,
        request: &EffectRequest,
    ) -> Result<AdmissionDecision, RuntimeExecutionError> {
        self.validate(key, request)?;
        self.evidence.append(json!({"event":"runtime_authorization","key":key,"request":request,"authorization":self.authorization})).map_err(RuntimeExecutionError::Admission)?;
        Ok(AdmissionDecision::Grant(self.authorization.clone()))
    }
}

impl EffectExecutor for Bridge {
    fn execute(
        &self,
        key: &ExecutionKey,
        request: &EffectRequest,
        grant: &EffectGrant,
    ) -> Result<EffectOutcome, RuntimeExecutionError> {
        self.validate(key, request)?;
        if grant != &self.authorization {
            return Err(RuntimeExecutionError::Invalid(
                "generic grant differs from exact PreparedGrant-derived authorization".into(),
            ));
        }
        let invocation = self
            .invocation
            .lock()
            .map_err(|e| RuntimeExecutionError::Executor(e.to_string()))?
            .take()
            .ok_or_else(|| {
                RuntimeExecutionError::Executor(
                    "already dispatched invocation cannot repeat".into(),
                )
            })?;
        let result = self
            .runtime
            .block_on(self.tools.execute_invocation(invocation));
        let output = match result {
            Ok(ToolOutcome::Completed(output)) => output,
            other => {
                self.evidence
                    .append(json!({"event":"adapter_error","result":format!("{other:?}")}))
                    .map_err(RuntimeExecutionError::Executor)?;
                return Err(RuntimeExecutionError::Executor(format!(
                    "native adapter did not complete: {other:?}"
                )));
            }
        };
        self.evidence
            .append(json!({"event":"adapter_returned","result":output}))
            .map_err(RuntimeExecutionError::Executor)?;
        let output = serde_json::to_value(output)
            .map_err(|e| RuntimeExecutionError::Executor(e.to_string()))?;
        let result_digest = digest(
            &serde_json::to_vec(&output)
                .map_err(|e| RuntimeExecutionError::Executor(e.to_string()))?,
        );
        Ok(EffectOutcome::Completed {
            receipt: EffectReceipt {
                receipt_id: format!("{}/effect/{}/receipt", key.execution_id, request.effect_id),
                effect_id: request.effect_id.clone(),
                authorization_id: grant.authorization_id.clone(),
                input_digest: request.input_digest.clone(),
                executor_id: "native-isolated-file-public-adapter".into(),
                executor_revision: self.revision.clone(),
                result_digest,
                status: ReceiptStatus::Completed,
            },
            output,
        })
    }
}
