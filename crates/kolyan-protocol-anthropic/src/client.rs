use crate::{AnthropicConfig, AnthropicError, Message, MessageCreateRequest, MessageStream};
use reqwest::{Client, RequestBuilder, Response as HttpResponse};

#[derive(Clone)]
pub struct AnthropicClient {
    http: Client,
    config: AnthropicConfig,
}

impl AnthropicClient {
    pub fn new(config: AnthropicConfig) -> Result<Self, AnthropicError> {
        let http = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { http, config })
    }

    pub async fn create_message(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<Message, AnthropicError> {
        dump_request("anthropic", request);
        let response = self.request("/v1/messages").json(request).send().await?;
        decode_json(response).await
    }

    pub async fn stream_message(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<MessageStream, AnthropicError> {
        let mut attempts = 0;
        let response = loop {
            dump_request("anthropic", request);
            match self.request("/v1/messages").json(request).send().await {
                Ok(response) => {
                    break response.error_for_status().map_err(|source| {
                        AnthropicError::Transport {
                            source,
                            diagnostics: None,
                        }
                    })?;
                }
                Err(source)
                    if attempts < self.config.transport_retries
                        && (source.is_timeout() || source.is_connect() || source.is_request()) =>
                {
                    attempts += 1;
                }
                Err(source) => {
                    return Err(AnthropicError::Transport {
                        source,
                        diagnostics: None,
                    });
                }
            }
        };
        Ok(MessageStream::new(response, self.config.diagnostics))
    }

    fn request(&self, path: &str) -> RequestBuilder {
        self.http
            .post(format!("{}{}", self.config.base_url, path))
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", &self.config.version)
            .header("content-type", "application/json")
    }
}

fn dump_request(label: &str, request: &MessageCreateRequest) {
    if std::env::var("KOLYAN_DUMP_MODEL_REQUESTS")
        .map(|value| value == "1" || value.eq_ignore_ascii_case("true"))
        .unwrap_or(false)
    {
        eprintln!(
            "[{label} request] {}",
            serde_json::to_string(request).expect("model request must serialize")
        );
    }
}

async fn decode_json(response: HttpResponse) -> Result<Message, AnthropicError> {
    let status = response.status();
    if !status.is_success() {
        return Err(AnthropicError::Http {
            status: status.as_u16(),
            body: response.text().await?,
        });
    }
    Ok(response.json().await?)
}
