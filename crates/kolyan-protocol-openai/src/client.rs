use crate::{OpenAiConfig, OpenAiError, Response, ResponseCreateRequest, ResponseStream};
use kolyan_protocol_http::{
    ResponseWithRetryReport, RetryDecision, RetryHeaders, RetryInput, RetryObservation,
    RetryProfile, RetryReport, StopReason,
};
use reqwest::{Client, RequestBuilder, Response as HttpResponse};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct OpenAiClient {
    http: Client,
    config: OpenAiConfig,
}

impl OpenAiClient {
    pub fn new(config: OpenAiConfig) -> Result<Self, OpenAiError> {
        if config.transport_retries > 16
            || config.timeout.is_zero()
            || (config.http_retry.max_retries() > 0
                && config.http_retry.profile() != RetryProfile::OpenAi)
        {
            return Err(OpenAiError::Configuration(
                "invalid transport cap, timeout or retry profile".into(),
            ));
        }
        let http = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { http, config })
    }

    pub async fn create_response(
        &self,
        request: &ResponseCreateRequest,
    ) -> Result<Response, OpenAiError> {
        Ok(self.create_response_with_report(request).await?.response)
    }

    /// Opening proof is local metadata, never a field of the SDK response DTO.
    pub async fn create_response_with_report(
        &self,
        request: &ResponseCreateRequest,
    ) -> Result<ResponseWithRetryReport<Response>, OpenAiError> {
        dump_request("openai", request);
        let opened = self.open(serde_json::to_vec(request)?).await?;
        let response = decode_json(opened.response)
            .await
            .map_err(|error| error.with_retry_report(&opened.retry_report))?;
        Ok(ResponseWithRetryReport {
            response,
            retry_report: opened.retry_report,
        })
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
        dump_request("openai", &body);
        let opened = self.open(serde_json::to_vec(&body)?).await?;
        Ok(
            ResponseStream::new(opened.response, self.config.diagnostics)
                .with_retry_report(opened.retry_report),
        )
    }

    async fn open(
        &self,
        body: Vec<u8>,
    ) -> Result<ResponseWithRetryReport<HttpResponse>, OpenAiError> {
        let started = Instant::now();
        let bounded = self.config.http_retry.max_retries() > 0;
        let mut report = RetryReport::default();
        let mut retries_used = 0;
        let mut previous: Option<OpenAiError> = None;
        loop {
            let elapsed_ms = milliseconds(started.elapsed());
            let remaining = if bounded {
                let Some(remaining) = self.config.http_retry.remaining_ms(elapsed_ms) else {
                    report.finish(StopReason::ElapsedLimit);
                    return Err(previous
                        .unwrap_or(OpenAiError::OpeningBudgetExhausted)
                        .with_retry_report(&report));
                };
                Some(Duration::from_millis(remaining))
            } else {
                None
            };
            let send = self
                .request("/v1/responses")
                .body(body.clone())
                .timeout(self.config.timeout)
                .send();
            let sent = match remaining {
                Some(remaining) => match tokio::time::timeout(remaining, send).await {
                    Ok(sent) => sent,
                    Err(_) => {
                        record_budget_stop(&mut report, retries_used + 1, None, None);
                        return Err(OpenAiError::OpeningBudgetExhausted.with_retry_report(&report));
                    }
                },
                None => send.await,
            };
            let (error, status, code, request_id, decision) = match sent {
                Ok(response) if response.status().is_success() => {
                    let request_id = response
                        .headers()
                        .get("x-request-id")
                        .and_then(|v| v.to_str().ok());
                    report
                        .push(RetryObservation::new(
                            retries_used + 1,
                            Some(response.status().as_u16()),
                            None,
                            request_id,
                            None,
                        ))
                        .expect("validated cap bounds ordered sends");
                    return Ok(ResponseWithRetryReport {
                        response,
                        retry_report: report,
                    });
                }
                Ok(response) => {
                    let status = response.status().as_u16();
                    let headers = response.headers().clone();
                    let checked = if bounded {
                        let remaining = self
                            .config
                            .http_retry
                            .remaining_ms(milliseconds(started.elapsed()))
                            .unwrap_or(0);
                        match tokio::time::timeout(
                            Duration::from_millis(remaining),
                            check_status(response),
                        )
                        .await
                        {
                            Ok(checked) => checked,
                            Err(_) => {
                                record_budget_stop(
                                    &mut report,
                                    retries_used + 1,
                                    Some(status),
                                    headers.get("x-request-id").and_then(|v| v.to_str().ok()),
                                );
                                return Err(OpenAiError::Http {
                                    status,
                                    body: "error body read exceeded opening budget".into(),
                                }
                                .with_retry_report(&report));
                            }
                        }
                    } else {
                        check_status(response).await
                    };
                    let error = checked.expect_err("non-success response");
                    let code = match &error {
                        OpenAiError::Http { body, .. } => structured_code(body),
                        _ => None,
                    };
                    let value = |name| headers.get(name).and_then(|v| v.to_str().ok());
                    let decision = self.config.http_retry.decide(RetryInput {
                        retries_used,
                        elapsed_ms: milliseconds(started.elapsed()),
                        now_unix_ms: now_ms(),
                        jitter: jitter(),
                        status,
                        error_code: code.as_deref(),
                        headers: RetryHeaders {
                            should_retry: value("x-should-retry"),
                            retry_after_ms: value("retry-after-ms"),
                            retry_after: value("retry-after"),
                        },
                    });
                    (
                        error,
                        Some(status),
                        code,
                        value("x-request-id").map(str::to_owned),
                        decision,
                    )
                }
                Err(error) => {
                    let eligible = error.is_timeout() || error.is_connect() || error.is_request();
                    // Preserve the existing immediate transport retry semantics.
                    // The counter is shared with HTTP retries, not reset per cause.
                    let decision =
                        if eligible && retries_used < u32::from(self.config.transport_retries) {
                            RetryDecision::Retry {
                                delay_ms: 0,
                                basis: kolyan_protocol_http::WaitBasis::Backoff,
                            }
                        } else {
                            RetryDecision::Stop {
                                reason: if eligible {
                                    kolyan_protocol_http::StopReason::Exhausted
                                } else {
                                    kolyan_protocol_http::StopReason::Ineligible
                                },
                            }
                        };
                    (error.into(), None, None, None, decision)
                }
            };
            report
                .push(RetryObservation::new(
                    retries_used + 1,
                    status,
                    code.as_deref(),
                    request_id.as_deref(),
                    Some(decision),
                ))
                .expect("validated cap bounds ordered sends");
            match decision {
                RetryDecision::Stop { reason } => {
                    report.finish(reason);
                    return Err(error.with_retry_report(&report));
                }
                RetryDecision::Retry { delay_ms, .. } => {
                    previous = Some(error);
                    tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    retries_used += 1;
                }
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

fn record_budget_stop(
    report: &mut RetryReport,
    attempt: u32,
    status: Option<u16>,
    request_id: Option<&str>,
) {
    report.finish(StopReason::ElapsedLimit);
    report
        .push(RetryObservation::new(
            attempt,
            status,
            None,
            request_id,
            Some(RetryDecision::Stop {
                reason: kolyan_protocol_http::StopReason::ElapsedLimit,
            }),
        ))
        .expect("validated cap bounds ordered sends");
}

fn milliseconds(duration: Duration) -> u64 {
    u64::try_from(duration.as_millis()).unwrap_or(u64::MAX)
}

fn now_ms() -> u64 {
    milliseconds(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default(),
    )
}

fn jitter() -> f64 {
    1.0 - 0.25 * rand::random::<f64>()
}

fn structured_code(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value
        .get("error")
        .and_then(|error| error.get("code"))
        .or_else(|| value.get("code"))?
        .as_str()
        .map(str::to_owned)
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

#[cfg(test)]
#[path = "client/opening_tests.rs"]
mod opening_tests;

#[cfg(test)]
#[path = "client/diagnostic_tests.rs"]
mod diagnostic_tests;

#[cfg(test)]
#[path = "client/opening_diagnostic_http_tests.rs"]
mod opening_diagnostic_http_tests;
