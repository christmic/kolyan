use crate::{AnthropicError, MessageStreamEvent, ResponseDiagnostics, sse::Decoder};

use futures_core::Stream;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

pub struct MessageStream {
    body: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    decoder: Decoder,
    pending: VecDeque<(String, String)>,
    done: bool,
    diagnostics: ResponseDiagnostics,
    capture_tail: bool,
    tail: Vec<u8>,
    retry_report: kolyan_protocol_http::RetryReport,
}

impl MessageStream {
    pub(crate) fn new(response: reqwest::Response, capture_tail: bool) -> Self {
        let diagnostics = ResponseDiagnostics {
            status: response.status().as_u16(),
            content_type: response
                .headers()
                .get(reqwest::header::CONTENT_TYPE)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            content_encoding: response
                .headers()
                .get(reqwest::header::CONTENT_ENCODING)
                .and_then(|value| value.to_str().ok())
                .map(str::to_owned),
            received_bytes: 0,
            tail_preview: None,
        };
        Self {
            body: Box::pin(response.bytes_stream()),
            decoder: Decoder::default(),
            pending: VecDeque::new(),
            done: false,
            diagnostics,
            capture_tail,
            tail: Vec::new(),
            retry_report: Default::default(),
        }
    }

    pub(crate) fn with_retry_report(mut self, report: kolyan_protocol_http::RetryReport) -> Self {
        self.retry_report = report;
        self
    }

    /// Local opening observations, not Anthropic wire metadata. Stream errors never reopen.
    pub fn retry_report(&self) -> &kolyan_protocol_http::RetryReport {
        &self.retry_report
    }

    fn record_bytes(&mut self, bytes: &[u8]) {
        self.diagnostics.received_bytes += bytes.len();
        if !self.capture_tail {
            return;
        }
        const MAX_TAIL_BYTES: usize = 1024;
        self.tail.extend_from_slice(bytes);
        if self.tail.len() > MAX_TAIL_BYTES {
            let start = self.tail.len() - MAX_TAIL_BYTES;
            self.tail = self.tail[start..].to_vec();
        }
        self.diagnostics.tail_preview = Some(String::from_utf8_lossy(&self.tail).into_owned());
    }

    fn transport_error(&self, source: reqwest::Error) -> AnthropicError {
        AnthropicError::Transport {
            source,
            diagnostics: Some(self.diagnostics.clone()),
        }
        .with_retry_report(&self.retry_report)
    }
}

impl Stream for MessageStream {
    type Item = Result<MessageStreamEvent, AnthropicError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some((event, data)) = self.pending.pop_front() {
                if event == "error" {
                    self.done = true;
                    self.pending.clear();
                    return Poll::Ready(Some(Err(
                        AnthropicError::Api(data).with_retry_report(&self.retry_report)
                    )));
                }
                if !matches!(
                    event.as_str(),
                    "message_start"
                        | "message_delta"
                        | "message_stop"
                        | "content_block_start"
                        | "content_block_delta"
                        | "content_block_stop"
                        | "completion"
                        | "message"
                ) {
                    continue;
                }
                let result = serde_json::from_str::<serde_json::Value>(&data)
                    .map_err(AnthropicError::Decode)
                    .and_then(|mut value| {
                        if let Some(object) = value.as_object_mut() {
                            object
                                .entry("type")
                                .or_insert(serde_json::Value::String(event));
                        }
                        serde_json::from_value(value).map_err(AnthropicError::Decode)
                    });
                if result.is_err() {
                    self.done = true;
                    self.pending.clear();
                }
                return Poll::Ready(Some(
                    result.map_err(|error| error.with_retry_report(&self.retry_report)),
                ));
            }
            if self.done {
                return Poll::Ready(None);
            }
            if let Some(error) = self.decoder.take_error() {
                self.done = true;
                return Poll::Ready(Some(Err(
                    AnthropicError::Framing(error).with_retry_report(&self.retry_report)
                )));
            }
            match self.body.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    self.record_bytes(&bytes);
                    match self.decoder.push(&bytes) {
                        Ok(events) => self
                            .pending
                            .extend(events.into_iter().map(|event| (event.kind, event.data))),
                        Err(error) => {
                            self.done = true;
                            return Poll::Ready(Some(Err(AnthropicError::Framing(error)
                                .with_retry_report(&self.retry_report))));
                        }
                    }
                }
                Poll::Ready(Some(Err(error))) => {
                    self.done = true;
                    return Poll::Ready(Some(Err(self.transport_error(error))));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    if let Err(error) = self.decoder.finish() {
                        return Poll::Ready(Some(Err(
                            AnthropicError::Framing(error).with_retry_report(&self.retry_report)
                        )));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests;
