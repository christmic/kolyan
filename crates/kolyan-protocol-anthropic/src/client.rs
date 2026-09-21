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
        let response = self.request("/v1/messages").json(request).send().await?;
        decode_json(response).await
    }

    pub async fn stream_message(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<MessageStream, AnthropicError> {
        Ok(MessageStream::new(
            self.request("/v1/messages")
                .json(request)
                .send()
                .await?
                .error_for_status()?,
        ))
    }

    fn request(&self, path: &str) -> RequestBuilder {
        self.http
            .post(format!("{}{}", self.config.base_url, path))
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", &self.config.version)
            .header("content-type", "application/json")
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
