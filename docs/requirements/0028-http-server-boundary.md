# Minimal HTTP Server Boundary

## Scope and status

Implemented and verified on 2026-09-30. Add only a loopback HTTP boundary for the existing
configured Agent. Return complete results or pending approval without an event
stream. [Protocol](../../protocols/server-http.md) defines behavior and
[OpenAPI](../../schemas/server-http.openapi.json) defines wire shapes.

This supersedes the asynchronous command and SSE draft. No CommandStore,
idempotent receipts, subscription system, projection database, distributed
coordination or new execution loop is required. Keep stdio and its tests intact.

## Design

Reuse SessionExecutionService start, resume, cancel and reconciliation.
Session owns history; Runtime owns durable execution; service owns HTTP and task
supervision. Share Provider/tool/policy assembly between stdio and HTTP.
Clients supply Session and Turn IDs so they can query after a lost response.
Repeated creation/start is a conflict, not an automatic replay.

Start and approve wait for the attempt to terminate or suspend. Suspension returns
without a worker waiting for approval. Later approval loads the persisted
checkpoint, including after restart. Denial records approval rejection and never
executes the pending call. A disconnected request must not cancel execution.
Status/cancel remain responsive while a model runs. Bound active tasks and drain
them on graceful shutdown. Lost running work reports recovery_required and is
not blindly replayed.

Display DTOs expose identities, state, actual end reason, completed Step
content/structured output/usage, typed tool results and safe pending approval. Never expose raw SessionRecord,
ModelRequest, opaque reasoning signatures or approval continuations.
No HTTP-level model retries. Existing Session and Ledger remain authoritative.

Require loopback binding and a dedicated environment token. Reject Origin and
unexpected Host, disable CORS, bound bodies/text/identifiers and active attempts.
Use application/problem+json with sanitized errors.

## Implementation sequence

1. Replace over-scoped requirement/protocol/OpenAPI with six operations.
2. Add durable approval denial and independent service tests.
3. Extract shared assembly; implement HTTP routes, DTOs and bounded tasks.
4. Add independent data-driven HTTP process and real Provider tests.
5. Verify regression suites, contract and every configured live combination.

## Acceptance

Do not change existing tests. Unit tests live in separate module test files;
process tests live under kolyan-integration-tests/tests/server. Scenario inputs
and expected semantic traces are data fixtures. Only test code writes temporary
HTTP transcripts, Ledger JSONL and matrix reports.

Deterministic coverage: all operations, full result queries/restart, direct
answer, multi-Step tools, retained context across two Turns, approval pause and
restart/approve, denial without effects, running/suspended cancel, disconnect,
duplicate/concurrent starts, wrong ownership, stale approval, missing resource,
malformed/unknown fields, authentication, Host/Origin, size limits, capacity and
interrupted-worker status.

For every configured Provider/protocol/model run direct answer, multi-Step
read/write with approval, two Turns with context, approval restart/approve,
denial and cancellation. Compare steps, tool effects, receipts and prohibited
facts; retain actual variable text/reasoning instead of inventing exact strings.
Missing credentials and ordinary Provider failures cannot be silently skipped.

## Verification record

Six complete-result operations and shared stdio/HTTP assembly passed final
acceptance. Workspace tests, workspace strict Clippy and Rust formatting checks
exited successfully. Ordinary workspace tests leave explicitly ignored network
and SDK-oracle tests unexecuted; the HTTP live matrix was run separately.
Server unit tests passed 10 tests, HTTP module tests passed 7 tests, new
deterministic HTTP process tests passed 4 tests and unchanged stdio process
regression passed 5 tests.

| Acceptance area | Test evidence |
| --- | --- |
| All routes, multi-Step execution, two Turns, approval restart, denial and suspended cancellation | `http_process_complete_result_matrix`, driven by `server_http.json` |
| Authentication, Host/Origin, malformed inputs, method, size and ownership rejection | `http_process_error_matrix`, driven by `server_http_errors.json`; ownership also checked in execution cases |
| Responsive status/cancel, disconnected client, concurrent starts and capacity | `disconnected_execution_can_be_cancelled_without_blocking_status`, driven by `server_http_faults.json` |
| Lost worker without automatic replay | `interrupted_worker_is_not_replayed_on_restart`, driven by `server_http_faults.json` |
| Durable denial, projection repair without execution and concurrent identical commits | Separate Server denial and coordinator unit test files |
| Safe display mapping, actual end reason, usage, structured output, checkpoint identity and corrupt facts | Separate HTTP view unit test file |
| Public response shapes | Process responses validated against the checked-in OpenAPI component schemas, including live and fault responses |
| Every configured Provider/protocol/model | `http_process_live_matrix`, driven by `server_http_live.json` |

The finalized executable and fixture ran all 19 configured combinations across
four scenario groups: direct answer, approval/restart with two Turns, denial
and cancellation. All 76 rows passed with zero failures or skips in 328.04
seconds. The two-Turn group makes this 95 individual Turn cases. Model calls
produce the live tool proposals; test code does not fabricate those calls.
Assertions compare actual Step content, tool results, file effects, receipts,
prior context and prohibited facts. Variable text and reasoning remain in the
captured evidence rather than being replaced with fabricated expected strings.

The local evidence directory is
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-RSp01x`.
It contains `report.json`, HTTP JSONL, Ledger JSONL and actual model request
diagnostics. These are temporary test artifacts, not repository content.
Earlier diagnostic matrices exposed ambiguous prompts, legitimate optional
read-back calls and refusal of a benign write request. Fixtures were clarified
against captured requests; no Provider errors were converted to skips or hidden
by test retries.

Six unique OpenAPI operations and all 21 references were structurally checked.
The response validator covers the schema vocabulary used by this contract; it
is not a general OpenAPI conformance validator, which was not run.

## Deliberate limits

This phase does not add event streams, automatic replay, distributed admission,
remote binding, CORS or TLS. An interrupted running worker reports
`recovery_required`; this is not an automatic recovery endpoint. Approval
resumption after restart is supported. Passing this HTTP matrix is acceptance
for this boundary, not a claim that every historical ignored network or SDK
oracle test was rerun.
