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
        }
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
    }
}

impl Stream for MessageStream {
    type Item = Result<MessageStreamEvent, AnthropicError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some((_event, data)) = self.pending.pop_front() {
                let result = serde_json::from_str(&data).map_err(AnthropicError::Decode);
                if result.is_err() {
                    self.done = true;
                    self.pending.clear();
                }
                return Poll::Ready(Some(result));
            }
            if self.done {
                return Poll::Ready(None);
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
                            return Poll::Ready(Some(Err(AnthropicError::Framing(error))));
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
                        return Poll::Ready(Some(Err(AnthropicError::Framing(error))));
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod tests;
