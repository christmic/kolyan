//! Low-level OpenAI Responses API protocol client.

mod client;
mod config;
mod error;
pub mod responses;
pub mod sse;

pub use client::OpenAiClient;
pub use config::OpenAiConfig;
pub use error::OpenAiError;
pub use responses::{
    FunctionTool, Response, ResponseCreateRequest, ResponseOutputItem, ResponseStream,
    ResponseStreamEvent, ResponseTextConfig,
};
