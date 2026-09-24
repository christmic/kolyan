use crate::{OpenAiError, ResponseDiagnostics, ResponseStreamEvent, sse::parse_sse_data};
use futures_core::Stream;
use std::{
    collections::VecDeque,
    pin::Pin,
    task::{Context, Poll},
};

pub struct ResponseStream {
    body: Pin<Box<dyn Stream<Item = Result<bytes::Bytes, reqwest::Error>> + Send>>,
    buffer: String,
    pending: VecDeque<String>,
    done: bool,
    diagnostics: ResponseDiagnostics,
    capture_tail: bool,
    tail: Vec<u8>,
}

impl ResponseStream {
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
            buffer: String::new(),
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

    fn transport_error(&self, source: reqwest::Error) -> OpenAiError {
        OpenAiError::Transport {
            source,
            diagnostics: Some(self.diagnostics.clone()),
        }
    }
}

impl Stream for ResponseStream {
    type Item = Result<ResponseStreamEvent, OpenAiError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            if let Some(data) = self.pending.pop_front() {
                if data == "[DONE]" {
                    self.done = true;
                    continue;
                }
                return Poll::Ready(Some(
                    serde_json::from_str(&data).map_err(OpenAiError::Decode),
                ));
            }
            if self.done {
                return Poll::Ready(None);
            }
            match self.body.as_mut().poll_next(cx) {
                Poll::Ready(Some(Ok(bytes))) => {
                    self.record_bytes(&bytes);
                    let events = parse_sse_data(&mut self.buffer, &bytes);
                    self.pending.extend(events);
                }
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(self.transport_error(error))));
                }
                Poll::Ready(None) => {
                    self.done = true;
                    if !self.buffer.is_empty() {
                        let events = parse_sse_data(&mut self.buffer, b"\n\n");
                        self.pending.extend(events);
                    }
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}
