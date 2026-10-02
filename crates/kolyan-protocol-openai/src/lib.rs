//! Low-level OpenAI Responses API protocol client.

mod client;
mod config;
mod counting;
mod error;
pub mod responses;
pub mod sse;

pub use client::OpenAiClient;
pub use config::OpenAiConfig;
pub use counting::CountFailure;
pub use counting::{InputTokenCountObject, InputTokenCountRequest, InputTokenCountResponse};
pub use error::{OpenAiError, ResponseDiagnostics};
pub use kolyan_protocol_http::{
    HttpRetryPolicy, ResponseWithRetryReport, RetryProfile, RetryReport,
};
pub use responses::{
    FunctionTool, Response, ResponseCreateRequest, ResponseOutputItem, ResponseStream,
    ResponseStreamEvent, ResponseTextConfig,
};
