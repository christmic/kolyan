use crate::{OpenAiConfig, OpenAiError, Response, ResponseCreateRequest, ResponseStream};
use reqwest::{Client, RequestBuilder, Response as HttpResponse};

#[derive(Clone)]
pub struct OpenAiClient {
    http: Client,
    config: OpenAiConfig,
}

impl OpenAiClient {
    pub fn new(config: OpenAiConfig) -> Result<Self, OpenAiError> {
        let http = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { http, config })
    }

    pub async fn create_response(
        &self,
        request: &ResponseCreateRequest,
    ) -> Result<Response, OpenAiError> {
        let response = self.request("/v1/responses").json(request).send().await?;
        decode_json(response).await
    }

    pub async fn stream_response(
        &self,
        request: &ResponseCreateRequest,
    ) -> Result<ResponseStream, OpenAiError> {
        let mut attempts = 0;
        loop {
            match self.request("/v1/responses").json(request).send().await {
                Ok(response) => return Ok(ResponseStream::new(response.error_for_status()?)),
                Err(error)
                    if attempts < self.config.transport_retries
                        && (error.is_timeout() || error.is_connect() || error.is_request()) =>
                {
                    attempts += 1;
                }
                Err(error) => return Err(error.into()),
            }
        }
    }

    fn request(&self, path: &str) -> RequestBuilder {
        self.http
            .post(format!("{}{}", self.config.base_url, path))
            .bearer_auth(&self.config.api_key)
            .header("content-type", "application/json")
    }
}

async fn decode_json(response: HttpResponse) -> Result<Response, OpenAiError> {
    let status = response.status();
    if !status.is_success() {
        return Err(OpenAiError::Http {
            status: status.as_u16(),
            body: response.text().await?,
        });
    }
    Ok(response.json().await?)
}
