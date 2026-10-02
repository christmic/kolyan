//! Bounded new-Turn host preparation. No selector, counting, source publication,
//! policy authority or recovery execution; the host owns stronger provenance.

use std::io::{self, Write};
use std::time::{Duration, Instant};

use kolyan_core::TurnRequest;
use kolyan_model::ModelRequest;
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::{ExecutionRef, ServerError, TurnPreparationHook};

const MAX_PREPARATION_WAIT: Duration = Duration::from_secs(30);
const MAX_REQUEST_BYTES: usize = 16 * 1024 * 1024;
const MAX_MESSAGES: usize = 4096;
const MAX_BLOCKS: usize = 16384;

/// Preparation refusal is distinct from execution failure and Session conflict.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, Error)]
pub enum PreparationFailure {
    #[error("Turn deadline exhausted before admission")]
    DeadlineExpired,
    #[error("host preparation exceeded its bounded wait")]
    TimedOut,
    #[error("host changed a non-message request field")]
    FieldsChanged,
    #[error("selected history is not an original ordered subsequence")]
    MessagesNotSubsequence,
    #[error("host changed or omitted current input tail")]
    CurrentInputChanged,
    #[error("preparation request exceeds bounded bytes/messages/blocks")]
    BoundsExceeded,
    #[error("host preparation rejected: {reason}")]
    Rejected { reason: String },
}

pub(crate) async fn prepare_turn(
    hook: &dyn TurnPreparationHook,
    execution: &ExecutionRef,
    session_version: u64,
    request: &mut TurnRequest,
    current_message_count: usize,
    started: Instant,
) -> Result<(), ServerError> {
    remaining_deadline(request, started)?;
    bounded_request(&request.model_request)?;
    // Do not even construct the host future once the original window is gone.
    let remaining = remaining_deadline(request, started)?;
    let wait = remaining.map_or(MAX_PREPARATION_WAIT, |limit| {
        limit.min(MAX_PREPARATION_WAIT)
    });
    let waiting = Instant::now();
    let selected = tokio::time::timeout(wait, hook.prepare(execution, session_version, request))
        .await
        .map_err(|_| PreparationFailure::TimedOut)??;
    // A blocking/non-cooperative hook can return Ready after the timer expired.
    if waiting.elapsed() >= wait {
        return Err(PreparationFailure::TimedOut.into());
    }
    validate_selection(&request.model_request, &selected, current_message_count)?;
    let remaining = remaining_deadline(request, started)?;
    request.model_request = selected;
    request.config.deadline = remaining;
    Ok(())
}

fn remaining_deadline(
    request: &TurnRequest,
    started: Instant,
) -> Result<Option<Duration>, PreparationFailure> {
    request
        .config
        .deadline
        .map(|limit| {
            limit
                .checked_sub(started.elapsed())
                .filter(|remaining| !remaining.is_zero())
                .ok_or(PreparationFailure::DeadlineExpired)
        })
        .transpose()
}

pub(crate) fn validate_selection(
    source: &ModelRequest,
    selected: &ModelRequest,
    current_message_count: usize,
) -> Result<(), PreparationFailure> {
    bounded_request(source)?;
    bounded_request(selected)?;
    // Exhaustive destructuring makes additions to ModelRequest require review.
    let ModelRequest {
        request_id,
        model,
        system,
        messages: _,
        tools,
        tool_choice,
        output_format,
        prompt_cache,
        reasoning,
        max_output_tokens,
        extensions,
    } = source;
    if request_id != &selected.request_id
        || model != &selected.model
        || system != &selected.system
        || tools != &selected.tools
        || tool_choice != &selected.tool_choice
        || output_format != &selected.output_format
        || prompt_cache != &selected.prompt_cache
        || reasoning != &selected.reasoning
        || max_output_tokens != &selected.max_output_tokens
        || extensions != &selected.extensions
    {
        return Err(PreparationFailure::FieldsChanged);
    }
    let history_len = source
        .messages
        .len()
        .checked_sub(current_message_count)
        .ok_or(PreparationFailure::CurrentInputChanged)?;
    let selected_history_len = selected
        .messages
        .len()
        .checked_sub(current_message_count)
        .ok_or(PreparationFailure::CurrentInputChanged)?;
    if selected.messages[selected_history_len..] != source.messages[history_len..] {
        return Err(PreparationFailure::CurrentInputChanged);
    }
    let mut history = source.messages[..history_len].iter();
    for message in &selected.messages[..selected_history_len] {
        if !history.any(|original| original == message) {
            return Err(PreparationFailure::MessagesNotSubsequence);
        }
    }
    Ok(())
}

fn bounded_request(request: &ModelRequest) -> Result<(), PreparationFailure> {
    if request.messages.len() > MAX_MESSAGES {
        return Err(PreparationFailure::BoundsExceeded);
    }
    let mut blocks = 0usize;
    for message in &request.messages {
        blocks = blocks
            .checked_add(message.content.len())
            .filter(|count| *count <= MAX_BLOCKS)
            .ok_or(PreparationFailure::BoundsExceeded)?;
    }
    struct Counter(usize);
    impl Write for Counter {
        fn write(&mut self, bytes: &[u8]) -> io::Result<usize> {
            self.0 = self
                .0
                .checked_add(bytes.len())
                .filter(|count| *count <= MAX_REQUEST_BYTES)
                .ok_or_else(|| io::Error::other("preparation byte ceiling"))?;
            Ok(bytes.len())
        }
        fn flush(&mut self) -> io::Result<()> {
            Ok(())
        }
    }
    serde_json::to_writer(Counter(0), request).map_err(|_| PreparationFailure::BoundsExceeded)
}

#[cfg(test)]
mod tests;
