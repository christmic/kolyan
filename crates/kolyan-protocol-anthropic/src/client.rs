use crate::{AnthropicConfig, AnthropicError, Message, MessageCreateRequest, MessageStream};
use kolyan_protocol_http::{
    ResponseWithRetryReport, RetryDecision, RetryHeaders, RetryInput, RetryObservation,
    RetryProfile, RetryReport, StopReason,
};
use reqwest::{Client, RequestBuilder, Response as HttpResponse};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

#[derive(Clone)]
pub struct AnthropicClient {
    http: Client,
    config: AnthropicConfig,
}

impl AnthropicClient {
    pub fn new(config: AnthropicConfig) -> Result<Self, AnthropicError> {
        if config.transport_retries > 16
            || (config.http_retry.max_retries() > 0
                && config.http_retry.profile() != RetryProfile::Anthropic)
        {
            return Err(AnthropicError::Configuration(
                "transport_retries must be <=16; enabled HTTP retries require Anthropic profile"
                    .into(),
            ));
        }
        let http = Client::builder().timeout(config.timeout).build()?;
        Ok(Self { http, config })
    }

    pub async fn create_message(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<Message, AnthropicError> {
        Ok(self.create_message_with_report(request).await?.response)
    }

    /// Opens once or within the explicit retry budget, then decodes exactly once.
    /// Dropping this future cancels sends/backoff; no detached task survives it.
    pub async fn create_message_with_report(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<ResponseWithRetryReport<Message>, AnthropicError> {
        dump_request("anthropic", request);
        let opened = self.open(serde_json::to_vec(request)?).await?;
        let response = decode_json(opened.response)
            .await
            .map_err(|error| retried(error, opened.retry_report.clone()))?;
        Ok(ResponseWithRetryReport {
            response,
            retry_report: opened.retry_report,
        })
    }

    pub async fn stream_message(
        &self,
        request: &MessageCreateRequest,
    ) -> Result<MessageStream, AnthropicError> {
        self.stream_message_with_extensions(request, &serde_json::Map::new())
            .await
    }

    /// Explicit SDK-style extra body fields; the Provider validates its configured bindings.
    pub async fn stream_message_with_extensions(
        &self,
        request: &MessageCreateRequest,
        extensions: &serde_json::Map<String, serde_json::Value>,
    ) -> Result<MessageStream, AnthropicError> {
        let mut body = serde_json::to_value(request)?;
        body.as_object_mut()
            .expect("typed request serializes as an object")
            .extend(extensions.clone());
        dump_request("anthropic", &body);
        let opened = self.open(serde_json::to_vec(&body)?).await?;
        Ok(MessageStream::new(opened.response, self.config.diagnostics)
            .with_retry_report(opened.retry_report))
    }

    async fn open(
        &self,
        body: Vec<u8>,
    ) -> Result<ResponseWithRetryReport<HttpResponse>, AnthropicError> {
        let started = Instant::now();
        let policy = &self.config.http_retry;
        let bounded = policy.max_retries() > 0;
        let mut report = RetryReport::default();
        let mut retries = 0;
        let mut previous_error = None;
        loop {
            let request = self.request("/v1/messages").body(body.clone());
            let sent = if bounded {
                let Some(remaining) = policy.remaining_ms(elapsed_ms(started)) else {
                    report.finish(StopReason::ElapsedLimit);
                    return Err(retried(
                        previous_error.unwrap_or(AnthropicError::OpeningBudgetExhausted),
                        report,
                    ));
                };
                // Reqwest keeps its original body timeout after accepted headers.
                // Only the opening future is constrained by the remaining host window.
                match tokio::time::timeout(Duration::from_millis(remaining), request.send()).await {
                    Ok(result) => result,
                    Err(_) => return Err(opening_expired(report, None, None)),
                }
            } else {
                request.send().await
            };
            let (error, status, code, request_id, decision) = match sent {
                Ok(response) => {
                    let status = response.status().as_u16();
                    let headers = response.headers().clone();
                    let request_id =
                        header(&headers, "request-id").or_else(|| header(&headers, "x-request-id"));
                    let checked = if bounded && !response.status().is_success() {
                        let Some(remaining) = policy.remaining_ms(elapsed_ms(started)) else {
                            return Err(opening_expired(report, Some(status), request_id));
                        };
                        match tokio::time::timeout(
                            Duration::from_millis(remaining),
                            check_status(response),
                        )
                        .await
                        {
                            Ok(result) => result,
                            Err(_) => {
                                return Err(opening_expired(report, Some(status), request_id));
                            }
                        }
                    } else {
                        check_status(response).await
                    };
                    match checked {
                        Ok(response) => {
                            record(&mut report, Some(status), None, request_id, None);
                            return Ok(ResponseWithRetryReport {
                                response,
                                retry_report: report,
                            });
                        }
                        Err(error) => {
                            let code = match &error {
                                AnthropicError::Http { body, .. } => error_code(body),
                                _ => None,
                            };
                            let decision = policy.decide(RetryInput {
                                retries_used: retries,
                                elapsed_ms: elapsed_ms(started),
                                now_unix_ms: u64::try_from(
                                    SystemTime::now()
                                        .duration_since(UNIX_EPOCH)
                                        .unwrap_or_default()
                                        .as_millis(),
                                )
                                .unwrap_or(u64::MAX),
                                jitter: 0.75 + 0.25 * fastrand::f64(),
                                status,
                                error_code: code.as_deref(),
                                headers: RetryHeaders {
                                    should_retry: header(&headers, "x-should-retry"),
                                    retry_after_ms: header(&headers, "retry-after-ms"),
                                    retry_after: header(&headers, "retry-after"),
                                },
                            });
                            (
                                error,
                                Some(status),
                                code,
                                request_id.map(str::to_owned),
                                decision,
                            )
                        }
                    }
                }
                Err(source) => {
                    let eligible =
                        source.is_timeout() || source.is_connect() || source.is_request();
                    let decision = if eligible && retries < u32::from(self.config.transport_retries)
                    {
                        RetryDecision::Retry {
                            delay_ms: 0,
                            basis: kolyan_protocol_http::WaitBasis::Backoff,
                        }
                    } else {
                        RetryDecision::Stop {
                            reason: if eligible {
                                StopReason::Exhausted
                            } else {
                                StopReason::Ineligible
                            },
                        }
                    };
                    (
                        AnthropicError::Transport {
                            source,
                            diagnostics: None,
                        },
                        None,
                        None,
                        None,
                        decision,
                    )
                }
            };
            record(
                &mut report,
                status,
                code.as_deref(),
                request_id.as_deref(),
                Some(decision),
            );
            match decision {
                RetryDecision::Stop { reason } => {
                    report.finish(reason);
                    return Err(retried(error, report));
                }
                RetryDecision::Retry { delay_ms, .. } => {
                    if bounded
                        && policy
                            .remaining_ms(elapsed_ms(started))
                            .is_none_or(|remaining| delay_ms >= remaining)
                    {
                        report.finish(StopReason::ElapsedLimit);
                        return Err(retried(error, report));
                    }
                    if delay_ms > 0 {
                        tokio::time::sleep(Duration::from_millis(delay_ms)).await;
                    }
                    if bounded && policy.remaining_ms(elapsed_ms(started)).is_none() {
                        report.finish(StopReason::ElapsedLimit);
                        return Err(retried(error, report));
                    }
                    retries += 1;
                    previous_error = Some(error);
                }
            }
        }
    }

    fn request(&self, path: &str) -> RequestBuilder {
        self.http
            .post(format!("{}{}", self.config.base_url, path))
            .header("x-api-key", &self.config.api_key)
            .header("anthropic-version", &self.config.version)
            .header("content-type", "application/json")
    }
}

fn header<'a>(headers: &'a reqwest::header::HeaderMap, name: &str) -> Option<&'a str> {
    headers.get(name).and_then(|value| value.to_str().ok())
}

fn elapsed_ms(started: Instant) -> u64 {
    u64::try_from(started.elapsed().as_millis()).unwrap_or(u64::MAX)
}

fn error_code(body: &str) -> Option<String> {
    let value: serde_json::Value = serde_json::from_str(body).ok()?;
    value
        .pointer("/error/code")
        .or_else(|| value.pointer("/error/type"))
        .or_else(|| value.get("code"))?
        .as_str()
        .map(str::to_owned)
}

fn record(
    report: &mut RetryReport,
    status: Option<u16>,
    code: Option<&str>,
    request_id: Option<&str>,
    decision: Option<RetryDecision>,
) {
    report
        .push(RetryObservation::new(
            report.attempts() as u32 + 1,
            status,
            code,
            request_id,
            decision,
        ))
        .expect("validated retry caps bound reports to at most 17 sends");
}

fn retried(error: AnthropicError, report: RetryReport) -> AnthropicError {
    error.with_retry_report(&report)
}

fn opening_expired(
    mut report: RetryReport,
    status: Option<u16>,
    request_id: Option<&str>,
) -> AnthropicError {
    record(
        &mut report,
        status,
        None,
        request_id,
        Some(RetryDecision::Stop {
            reason: StopReason::ElapsedLimit,
        }),
    );
    report.finish(StopReason::ElapsedLimit);
    let error = match status {
        Some(status) => AnthropicError::Http {
            status,
            body: "error body read exceeded opening budget".into(),
        },
        None => AnthropicError::OpeningBudgetExhausted,
    };
    retried(error, report)
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

async fn decode_json(response: HttpResponse) -> Result<Message, AnthropicError> {
    let response = check_status(response).await?;
    Ok(response.json().await?)
}

async fn check_status(mut response: HttpResponse) -> Result<HttpResponse, AnthropicError> {
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
                return Err(AnthropicError::Http {
                    status,
                    body: format!("error body could not be read: {error}"),
                });
            }
        }
    }
    Err(AnthropicError::Http {
        status,
        body: String::from_utf8_lossy(&body).into_owned(),
    })
}

#[cfg(test)]
mod tests;

#[cfg(test)]
mod opening_tests;

#[cfg(test)]
mod opening_diagnostic_tests;
