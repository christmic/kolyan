# Anthropic Messages protocol

Generation/SSE remain separate from explicit input counting.
`count_tokens(&MessageCountTokensRequest, Duration)`
sends once to `/v1/messages/count_tokens`.
The caller's positive timeout covers headers and the complete decoded body.
Success and error bodies are limited to 64 KiB; larger responses fail, never truncate
into a successful report. Dropping the future releases local waiting only.
No generation transport/status retry policy is used for counting.
Redirects and underlying HTTP retries are disabled for count operations.
Request JSON is encoded with a bounded writer (16 MiB, including escaping).
Oversize direct SDK input is rejected before any send.

Request DTOs preserve explicitly supplied SDK count fields, including nested settings.
Responses require nonnegative integer input_tokens and the documented response shape.
Reports are not a tokenizer guarantee or a trusted token budget.
Count tests use localhost protocol fixtures, not an actual vendor/model.

`count_endpoint_identity() -> String` returns a 64-character SHA-256 binding,
not a URL or an authentication/version header. It hashes the exact UTF-8 base URL
(including any userinfo/query) with a length prefix and the protocol version;
different credential-bearing URL configurations must not share prepared input.
This is an opaque configuration identity, not endpoint authentication or encryption;
low-entropy inputs may still be guessed. No count identity getter returns raw URL material.
