use crate::{OpenAiError, ResponseStreamEvent, sse::parse_sse_data};
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
}

impl ResponseStream {
    pub(crate) fn new(response: reqwest::Response) -> Self {
        Self {
            body: Box::pin(response.bytes_stream()),
            buffer: String::new(),
            pending: VecDeque::new(),
            done: false,
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
                    let events = parse_sse_data(&mut self.buffer, &bytes);
                    self.pending.extend(events);
                }
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(OpenAiError::Transport(error))));
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
