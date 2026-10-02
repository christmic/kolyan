# Shared HTTP opening policy

Pure eligibility/delay calculation and bounded local reports. This crate does not
send requests, sleep, own credentials, parse SDK wire DTOs, reopen accepted streams,
or retry tools/Turns. See requirement0030's bounded HTTP-opening section.

## Frozen client integration API

```rust,ignore
HttpRetryPolicy::new(max_retries: u32, max_elapsed_ms: u64,
    max_server_delay_ms: u64, profile: RetryProfile) -> Result<HttpRetryPolicy, PolicyError>
policy.decide(RetryInput {
    retries_used: u32, elapsed_ms: u64, now_unix_ms: u64, jitter: f64,
    status: u16, error_code: Option<&str>,
    headers: RetryHeaders { should_retry: Option<&str>,
        retry_after_ms: Option<&str>, retry_after: Option<&str> }
}) -> RetryDecision
// RetryDecision::Stop { reason: StopReason }
// RetryDecision::Retry { delay_ms: u64, basis: WaitBasis }
RetryObservation::new(attempt: u32, status: Option<u16>,
    error_code: Option<&str>, request_id: Option<&str>,
    decision: Option<RetryDecision>) -> RetryObservation
RetryReport::push(observation) -> Result<(), ReportError>
report.observations() -> &[RetryObservation]
report.attempts() -> usize
report.finish(reason: StopReason)
report.terminal_stop() -> Option<StopReason>
report.was_retried() -> bool
ResponseWithRetryReport<T> { response: T, retry_report: RetryReport }
```

Policy serde accepts exactly required `max_retries`, `max_elapsed_ms`,
`max_server_delay_ms`, `profile` (`OpenAi` or `Anthropic`), with validated private
fields. Default HTTP status retries = 0. Caps: 16 HTTP retries, 24-hour opening
window; enabled windows and server-delay caps must be positive, server cap <=
window. `max_retries()`, `max_elapsed_ms()`, `max_server_delay_ms()`, `profile()`
and `remaining_ms(elapsed_ms)` are read-only getters. Disabled HTTP policy does
not replace the client's existing transport policy.

The caller owns one retry counter shared by transport and status branches.
Each branch compares that counter with its own cap; all sends are bounded by
1 + max(transport cap, HTTP cap), never nested independent loops. Freeze the
serialized body/endpoint/options once. Use monotonic elapsed time for opening
bounds and wall-clock milliseconds only for HTTP dates. Supply an independently
random jitter multiplier in [.75, 1], not a clock-derived random value. Bound each
subsequent request timeout by remaining opening time. Cancel/drop must stop the
sleep/request inline; never spawn a detached retry task. After an accepted HTTP
response, decoding/framing/stream errors return directly without another send.

Exact known permanent codes: `Throttling.AllocationQuota`, `insufficient_quota`.
They stop even with should-retry true. Exact transient 429 allowlist:
`rate_limit_exceeded`, `rate_limit_error`, `Throttling.RateLimit`,
`Throttling.RateLimitExceeded`. Unknown 429 fails closed regardless of header.
Only pass a code parsed from an already bounded structured envelope; never pass
a message/preview/substring as a code. Other eligibility follows explicit
should-retry true/false then 408/409/5xx. Successful statuses must never be sent
to decide by the opening loop.

Positive finite hints prefer retry-after-ms, numeric retry-after seconds, then
HTTP date. Invalid/nonpositive hints fall back to jittered 500ms exponential
backoff capped at 8000ms. OpenAi refuses hints above 120000ms, including with
should-retry true; Anthropic retains longer hints subject to host cap. Finite
numeric hints whose millisecond conversion overflows are refused, not ignored.
Accepted hints exceeding the host cap stop; a wait reaching/exceeding remaining
opening time stops, never shortens. Server hints are not jittered.

Reports admit at most 64 actual-send observations, strictly increasing attempt
numbers from one. Clients must validate transport retry caps <=63 before use.
Code/request ID are UTF-8 prefixes bounded to 128/256 bytes with explicit
`fields_truncated`; reports contain no header maps, authentication or full body.
Decision contains selected wait basis/duration; success has decision None and
transport errors status None. Preserve the original terminal cause with the
report in SDK wrapper errors, and expose reports on accepted streams. Provider
successful-stream metadata uses a clearly local key only when attempts >1. Failure
diagnostics retain the report even for one send or zero sends. Protocol errors
expose the full report through `opening_report()`, while `retry_report()` describes
only multiple sends. Provider failure kinds distinguish `local_http_opening_report`
from `local_http_opening_retry`; successful-stream kinds remain unchanged. `finish(reason)`
records the serialized `terminal_stop` separately from each send's original
decision: an eligible Retry followed by elapsed-budget exhaustion stays Retry
in the observation, with terminal ElapsedLimit and no fabricated second send.
Successful opening leaves `terminal_stop` null. Do not modify remote
wire DTOs or masquerade as Stainless headers. This crate does not claim SDK client
wiring, localhost request retry tests, or real Provider acceptance are completed.

Local policy tests use explicit clock/jitter inputs and export all observations
before comparison. Run with the host-owned lock and private target:

```sh
cargo test --locked -p kolyan-protocol-http --target-dir /private/tmp/kolyan-error-feedback-target
cargo clippy --locked -p kolyan-protocol-http --all-targets --target-dir /private/tmp/kolyan-error-feedback-target -- -D warnings
```
