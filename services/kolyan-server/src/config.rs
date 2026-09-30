//! Local configuration keeps credentials in the environment, never in RPC.

use kolyan_model::{ModelProvider, ModelRequest, ParameterTable, ProviderFuture};
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use kolyan_provider_anthropic::AnthropicProvider;
use kolyan_provider_openai::OpenAiProvider;
use serde::Deserialize;
use std::{path::PathBuf, time::Duration};

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Config {
    #[serde(default)]
    pub http: Option<HttpConfig>,
    pub ledger_path: PathBuf,
    pub session_root: PathBuf,
    pub workspace: PathBuf,
    pub tool_scope: String,
    pub protocol: String,
    pub base_url: String,
    pub api_key_env: String,
    pub timeout_secs: u64,
    pub parameter_table: ParameterTable,
    pub request: ModelRequest,
    pub max_steps: usize,
    pub max_tool_calls: usize,
    pub progress: kolyan_policy::ProgressPolicy,
}

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HttpConfig {
    pub listen: std::net::SocketAddr,
    pub api_token_env: String,
    pub max_active_turns: usize,
}

#[derive(Clone)]
pub enum Provider {
    OpenAi(OpenAiProvider),
    Anthropic(AnthropicProvider),
}

impl Config {
    pub fn provider(&self) -> Result<Provider, Box<dyn std::error::Error>> {
        let key = std::env::var(&self.api_key_env)
            .map_err(|_| format!("missing credential variable {}", self.api_key_env))?;
        if key.is_empty() {
            return Err("empty API credential".into());
        }
        let timeout = Duration::from_secs(self.timeout_secs);
        match self.protocol.as_str() {
            "openai_responses" => {
                let mut config = OpenAiConfig::new(key);
                config.base_url = self.base_url.clone();
                config.timeout = timeout;
                Ok(Provider::OpenAi(
                    OpenAiProvider::new(OpenAiClient::new(config)?)
                        .with_parameter_table(self.parameter_table.clone())?,
                ))
            }
            "anthropic_messages" => {
                let mut config = AnthropicConfig::new(key);
                config.base_url = self.base_url.clone();
                config.timeout = timeout;
                Ok(Provider::Anthropic(
                    AnthropicProvider::new(AnthropicClient::new(config)?)
                        .with_parameter_table(self.parameter_table.clone())?,
                ))
            }
            _ => Err("unsupported server protocol".into()),
        }
    }
}

impl ModelProvider for Provider {
    fn stream(&self, request: ModelRequest) -> ProviderFuture<'_> {
        match self {
            Self::OpenAi(provider) => provider.stream(request),
            Self::Anthropic(provider) => provider.stream(request),
        }
    }
}
