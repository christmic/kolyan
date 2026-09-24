//! SDK-aligned SSE framing, independent of model completion and output validation.

use thiserror::Error;

#[derive(Debug, PartialEq, Eq)]
pub struct Event {
    pub kind: String,
    pub data: String,
}

#[derive(Debug, Error)]
pub enum DecodeError {
    #[error("invalid SSE UTF-8: {0}")]
    Utf8(#[from] std::str::Utf8Error),
}

/// Holds incomplete bytes until a full line can be decoded without loss.
#[derive(Default)]
pub struct Decoder {
    line: Vec<u8>,
    kind: String,
    data: Vec<String>,
    after_cr: bool,
    last_event_id: String,
    retry: Option<i64>,
    deferred_error: Option<DecodeError>,
}

impl Decoder {
    /// Return complete events; callers must terminate the stream on error.
    pub fn push(&mut self, bytes: &[u8]) -> Result<Vec<Event>, DecodeError> {
        let mut events = Vec::new();
        for &byte in bytes {
            if std::mem::take(&mut self.after_cr) && byte == b'\n' {
                continue;
            }
            if byte != b'\n' && byte != b'\r' {
                self.line.push(byte);
                continue;
            }
            self.after_cr = byte == b'\r';
            let line = std::mem::take(&mut self.line);
            let text = match std::str::from_utf8(&line) {
                Ok(text) => text,
                Err(error) if events.is_empty() => return Err(error.into()),
                Err(error) => {
                    self.deferred_error = Some(error.into());
                    return Ok(events);
                }
            };
            if text.is_empty() {
                if !self.data.is_empty()
                    || !self.kind.is_empty()
                    || !self.last_event_id.is_empty()
                    || self.retry.is_some()
                {
                    events.push(Event {
                        kind: std::mem::take(&mut self.kind),
                        data: self.data.join("\n"),
                    });
                }
                self.kind.clear();
                self.data.clear();
                self.retry = None;
            } else if !text.starts_with(':') {
                let (field, value) = text.split_once(':').unwrap_or((text, ""));
                let value = value.strip_prefix(' ').unwrap_or(value);
                match field {
                    "event" => self.kind = value.to_owned(),
                    "data" => self.data.push(value.to_owned()),
                    "id" if !value.contains('\0') => self.last_event_id = value.to_owned(),
                    "retry" => {
                        if let Ok(retry) = value.trim().parse() {
                            self.retry = Some(retry);
                        }
                    }
                    _ => {}
                }
            }
        }
        Ok(events)
    }

    /// Yield already-decoded events before reporting a later error in the same chunk.
    pub fn take_error(&mut self) -> Option<DecodeError> {
        self.deferred_error.take()
    }

    /// SDK framing discards an uncommitted EOF frame; completion is the caller's job.
    pub fn finish(&mut self) -> Result<(), DecodeError> {
        std::str::from_utf8(&self.line)?;
        if let Some(error) = self.take_error() {
            return Err(error);
        }
        *self = Self::default();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
