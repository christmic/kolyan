//! Local HTTP integration of the generic preparation port through both real SDKs.

#[path = "prepared_generation/mod.rs"]
mod prepared_generation;

use kolyan_model::CountProfile;
use kolyan_protocol_anthropic::{AnthropicClient, AnthropicConfig};
use kolyan_protocol_openai::{OpenAiClient, OpenAiConfig};
use kolyan_provider_anthropic::AnthropicProvider;
use kolyan_provider_openai::OpenAiProvider;

// Registration is explicit; neither protocol name implies count support.
type ProfileAdapter<P> = fn(P, CountProfile) -> P;

#[tokio::test]
async fn openai_prepared_port_preserves_wire_and_owner() {
    prepared_generation::run(
        include_str!("../../../kolyan-provider-openai/src/generation/cases.json"),
        include_str!("../../../kolyan-protocol-openai/src/client/counting/cases.json"),
        "/v1/responses",
        "/v1/responses/input_tokens",
        |base| {
            let mut config = OpenAiConfig::new("localhost-prepared-port-only");
            config.base_url = base.into();
            OpenAiProvider::new(OpenAiClient::new(config).unwrap())
        },
        OpenAiProvider::with_count_profile,
    )
    .await;
}

#[tokio::test]
async fn anthropic_prepared_port_preserves_wire_and_owner() {
    prepared_generation::run(
        include_str!("../../../kolyan-provider-anthropic/src/generation/cases.json"),
        include_str!("../../../kolyan-protocol-anthropic/src/client/counting/cases.json"),
        "/v1/messages",
        "/v1/messages/count_tokens",
        |base| {
            let mut config = AnthropicConfig::new("localhost-prepared-port-only");
            config.base_url = base.into();
            AnthropicProvider::new(AnthropicClient::new(config).unwrap())
        },
        AnthropicProvider::with_count_profile,
    )
    .await;
}
