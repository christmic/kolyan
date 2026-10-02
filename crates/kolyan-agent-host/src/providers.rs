//! Exact single-deployment adapters; wire planning and retries remain protocol-owned.

mod prepared;
pub use prepared::HostPreparedGeneration;

use std::{sync::Arc, time::Duration};

use kolyan_agent::{
    AgentSnapshot, ProviderFactory, RunnerError,
    context::{BudgetMode, ContextPolicy, SerializedByteEstimator},
    provider::{ContextPreparingProvider, ContextRecorder},
};
use kolyan_model::{ModelDescriptor, ModelProvider, ModelRequest, ParameterTable, ProviderFuture};
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_http::HttpRetryPolicy;
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use kolyan_provider_anthropic::AnthropicProvider;
use kolyan_provider_openai::OpenAiProvider;
use kolyan_server::ExecutionRef;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum DeploymentProtocol {
    OpenaiResponses,
    AnthropicMessages,
}

/// Trusted configuration contains an environment reference, never an API secret.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HostDeployment {
    pub protocol: DeploymentProtocol,
    pub descriptor: ModelDescriptor,
    pub base_url: String,
    pub api_key_env: String,
    pub timeout_secs: u64,
    pub http_retry: HttpRetryPolicy,
    pub parameter_table: ParameterTable,
}

#[derive(Clone)]
pub enum HostProvider {
    Openai(OpenAiProvider),
    Anthropic(AnthropicProvider),
}

impl ModelProvider for HostProvider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        match self {
            Self::Openai(provider) => provider.stream(request),
            Self::Anthropic(provider) => provider.stream(request),
        }
    }
}

/// Exact immutable deployment and explicit Inspect preparation. No model-name
/// heuristics or trusted-token assertions are inferred from the byte estimator.
pub struct HostProviderFactory {
    deployment: HostDeployment,
    descriptor: ModelDescriptor,
    context: ContextPolicy,
    recorder: Arc<dyn ContextRecorder>,
}

impl HostProviderFactory {
    /// Validate immutable configuration only. Credentials and HTTP clients are
    /// resolved for execution, never for query, cancellation or approval denial.
    pub fn open(
        deployment: HostDeployment,
        context: ContextPolicy,
        recorder: Arc<dyn ContextRecorder>,
    ) -> Result<Self, RunnerError> {
        if context.mode != BudgetMode::Inspect
            || deployment.timeout_secs == 0
            || deployment
                .descriptor
                .context_window
                .is_none_or(|limit| limit == 0)
            || context.output_reserve_tokens == 0
            || context.output_reserve_tokens >= deployment.descriptor.context_window.unwrap_or(0)
        {
            return Err(host(
                "host requires explicit Inspect context and positive bounds",
            ));
        }
        if deployment.api_key_env.is_empty()
            || !deployment
                .api_key_env
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b == b'_')
        {
            return Err(host("invalid credential environment reference"));
        }
        Ok(Self {
            descriptor: deployment.descriptor.clone(),
            deployment,
            context,
            recorder,
        })
    }

    fn executable_provider(&self) -> Result<HostProvider, RunnerError> {
        let deployment = &self.deployment;
        let key = std::env::var(&deployment.api_key_env)
            .map_err(|_| host("configured credential environment variable is absent"))?;
        if key.trim().is_empty() {
            return Err(host("configured credential is empty"));
        }
        let timeout = Duration::from_secs(deployment.timeout_secs);
        let provider = match deployment.protocol {
            DeploymentProtocol::OpenaiResponses => {
                let mut config = OpenAiConfig::new(key);
                config.base_url = deployment.base_url.clone();
                config.timeout = timeout;
                config.http_retry = deployment.http_retry.clone();
                HostProvider::Openai(
                    OpenAiProvider::new(OpenAiClient::new(config).map_err(host)?)
                        .with_parameter_table(deployment.parameter_table.clone())
                        .map_err(host)?,
                )
            }
            DeploymentProtocol::AnthropicMessages => {
                let mut config = AnthropicConfig::new(key);
                config.base_url = deployment.base_url.clone();
                config.timeout = timeout;
                config.http_retry = deployment.http_retry.clone();
                HostProvider::Anthropic(
                    AnthropicProvider::new(AnthropicClient::new(config).map_err(host)?)
                        .with_parameter_table(deployment.parameter_table.clone())
                        .map_err(host)?,
                )
            }
        };
        Ok(provider)
    }
}

impl ProviderFactory for HostProviderFactory {
    type Provider = HostProvider;

    fn build(
        &self,
        snapshot: &AgentSnapshot,
        _: &ExecutionRef,
    ) -> Result<ContextPreparingProvider<HostProvider>, RunnerError> {
        if snapshot.definition().model() != &self.descriptor.reference {
            return Err(host("Agent model differs from the configured deployment"));
        }
        Ok(ContextPreparingProvider::new(
            self.executable_provider()?,
            self.descriptor.clone(),
            self.context.clone(),
            Arc::new(SerializedByteEstimator),
            self.recorder.clone(),
        ))
    }
}

fn host(error: impl std::fmt::Display) -> RunnerError {
    RunnerError::Host(error.to_string())
}
