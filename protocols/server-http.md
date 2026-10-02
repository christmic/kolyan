# Minimal Server HTTP API v1

Implementation: [requirement 0028](../docs/requirements/0028-http-server-boundary.md).
Wire shapes: [OpenAPI](../schemas/server-http.openapi.json).

## Operations

| Method | Path | Success |
| --- | --- | --- |
| POST | /v1/sessions | Create client-named Session, 201 |
| GET | /v1/sessions/{session_id} | Session summary, 200 |
| POST | /v1/sessions/{session_id}/turns | Run to completion or suspension, 200 |
| GET | /v1/sessions/{session_id}/turns/{turn_id} | Status and available complete result, 200 |
| POST | /v1/sessions/{session_id}/turns/{turn_id}/approvals/{approval_id}/decision | Approve and run or deny, 200 |
| POST | /v1/sessions/{session_id}/turns/{turn_id}/cancel | Cancellation intent and current view, 200 |

No event stream or generic event/command API. Create takes session_id; start
takes turn_id and nonempty input; decision takes approve or deny; cancel takes
an empty object. Unknown fields fail. Duplicate create/start returns 409.
Clients query known IDs after disconnect instead of blindly resubmitting.
Turn paths enforce Session ownership; another Session's Turn returns 404.

## Result lifecycle

Start/approve await one execution attempt, not user confirmation. Suspension
returns a nullable checkpoint_id and required pending_approvals / external_waits
arrays; both arrays may be populated in a mixed wait. Approvals display approval_id,
turn_id, call_id, tool_name, reason, arguments and required nullable expires_at_ms.
External waits display call_id, wait_id, kind and schema_version only. These are
display coordinates, not restore authority: no checkpoint, grant, preparation,
continuation or external proof binding is exposed. Running and terminal views
have null checkpoint_id and empty waiting arrays. The old pending_approval field
is not accepted.
Later approval resumes the durable checkpoint after any length of wait or restart.
Deny terminates with ApprovalRejected without executing the pending call.

Turn views distinguish running, suspended, completed, cancelled and failed.
end_reason reports the actual terminal outcome, including limits/refusal.
steps contain completed Step display content, structured output and usage;
tool_results retain call identity, actual content and error status. Opaque fields stay
internal and unknown usage stays null. Querying after restart reconstructs the
same available result from durable facts, never from an in-memory response cache.

cancellation_requested is intent; execution_stopped is observation. Running
cancellation is observed at existing boundaries; suspended cancellation stops
without a worker. Disconnect is not cancel. A lost worker reports
recovery_required; status reads never invoke models or tools.

## Security and limits

Bind loopback only. Authorization Bearer uses a dedicated secret referenced by
environment variable, not a Provider key. Reject Origin; validate Host against
the listener. No CORS, credentials in URLs or automatic HTTP model retries.
Require application/json for mutations. Maximum body is 64 KiB; input is
nonempty and at most 32768 Unicode characters. IDs contain 1–128 ASCII letters,
digits, underscore or dash. Configuration bounds concurrent execution attempts.

Errors use application/problem+json: type, title, status, code, safe detail.
400 invalid_request; 401 unauthorized with WWW-Authenticate Bearer;
403 origin_forbidden; 404 not_found; 405 method_not_allowed with Allow;
409 conflict for duplicates/concurrent work/stale approval; 413 payload_too_large;
415 unsupported_media_type; 421 misdirected_request; 429 capacity_exceeded with
Retry-After; 500 internal_error. Never expose raw Provider or storage exceptions.
No idempotency header or durable asynchronous admission promise this phase.
