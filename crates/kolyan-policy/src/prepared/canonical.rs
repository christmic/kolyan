//! Compact canonical JSON with bounded output and no recursive traversal.

use std::io::{self, Write};

use serde_json::Value;

use super::PreparedError;

#[cfg(test)]
mod tests;

enum Token<'a> {
    Value(&'a Value),
    Key(&'a str),
    Byte(u8),
}

struct Output {
    bytes: Vec<u8>,
    limit: usize,
}

impl Write for Output {
    fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
        if self
            .bytes
            .len()
            .checked_add(bytes.len())
            .is_none_or(|n| n > self.limit)
        {
            return Err(io::Error::other("prepared input exceeds byte limit"));
        }
        self.bytes.extend_from_slice(bytes);
        Ok(bytes.len())
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

/// Object ordering is explicit even if another crate enables preserve_order.
/// Scalar escaping and number spelling remain serde_json's exact wire format.
pub(super) fn bytes(value: &Value, limit: usize) -> Result<Vec<u8>, PreparedError> {
    let mut output = Output {
        bytes: Vec::new(),
        limit,
    };
    let mut pending = vec![Token::Value(value)];
    while let Some(token) = pending.pop() {
        match token {
            Token::Value(Value::Object(entries)) => {
                output.write_all(b"{").map_err(invalid)?;
                pending.push(Token::Byte(b'}'));
                let mut entries: Vec<_> = entries.iter().collect();
                entries.sort_unstable_by(|left, right| left.0.cmp(right.0));
                for (index, (key, value)) in entries.into_iter().enumerate().rev() {
                    pending.push(Token::Value(value));
                    pending.push(Token::Byte(b':'));
                    pending.push(Token::Key(key));
                    if index > 0 {
                        pending.push(Token::Byte(b','));
                    }
                }
            }
            Token::Value(Value::Array(values)) => {
                output.write_all(b"[").map_err(invalid)?;
                pending.push(Token::Byte(b']'));
                for (index, value) in values.iter().enumerate().rev() {
                    pending.push(Token::Value(value));
                    if index > 0 {
                        pending.push(Token::Byte(b','));
                    }
                }
            }
            Token::Value(value) => serde_json::to_writer(&mut output, value).map_err(invalid)?,
            Token::Key(key) => serde_json::to_writer(&mut output, key).map_err(invalid)?,
            Token::Byte(byte) => output.write_all(&[byte]).map_err(invalid)?,
        }
    }
    Ok(output.bytes)
}

fn invalid(error: impl std::fmt::Display) -> PreparedError {
    PreparedError::Invalid(error.to_string())
}
