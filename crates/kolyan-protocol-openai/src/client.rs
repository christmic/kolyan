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
        dump_request("openai", request);
        let response = self.request("/v1/responses").json(request).send().await?;
        decode_json(response).await
    }

    pub async fn stream_response(
        &self,
        request: &ResponseCreateRequest,
    ) -> Result<ResponseStream, OpenAiError> {
        self.stream_response_with_extensions(request, &serde_json::Map::new())
            .await
    }

    /// Explicit SDK-style extra body fields; the Provider validates its configured bindings.
    pub async fn stream_response_with_extensions(
        &self,
        request: &ResponseCreateRequest,
        extensions: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<ResponseStream, OpenAiError> {
        let mut body = serde_json::to_value(request)?;
        body.as_object_mut()
            .expect("typed request serializes as an object")
            .extend(extensions.clone());
        let mut attempts = 0;
        loop {
            dump_request("openai", &body);
            match self.request("/v1/responses").json(&body).send().await {
                Ok(response) => {
                    let response = check_status(response).await?;
                    return Ok(ResponseStream::new(response, self.config.diagnostics));
                }
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

fn dump_request(label: &str, request: &impl serde::Serialize) {
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

async fn decode_json(response: HttpResponse) -> Result<Response, OpenAiError> {
    let response = check_status(response).await?;
    Ok(response.json().await?)
}

async fn check_status(mut response: HttpResponse) -> Result<HttpResponse, OpenAiError> {
    let status = response.status().as_u16();
    if response.status().is_success() {
        return Ok(response);
    }
    const MAX_ERROR_BYTES: usize = 4096;
    let mut body = Vec::new();
    while body.len() < MAX_ERROR_BYTES {
        match response.chunk().await {
            Ok(Some(chunk)) => {
                body.extend_from_slice(&chunk[..chunk.len().min(MAX_ERROR_BYTES - body.len())])
            }
            Ok(None) => break,
            Err(error) => {
                return Err(OpenAiError::Http {
                    status,
                    body: format!("error body could not be read: {error}"),
                });
            }
        }
    }
    Err(OpenAiError::Http {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

#[cfg(test)]
mod tests;
