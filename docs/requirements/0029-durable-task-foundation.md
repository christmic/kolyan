# Durable Task Foundation

## Scope and status

Design approved for incremental implementation by the user on 2026-09-30.
This requirement is the implementation and acceptance authority for strengthening
Ledger, Trajectory and long-task execution after the minimal HTTP boundary.
The source-backed rationale remains in [kernel evolution](../architecture/kernel-evolution.md).
No multi-agent capability is considered implemented merely because its identities
or relationships appear in this design. New-project development retains no legacy
format adapters, old-interface aliases or fallback reads hiding unsupported ports.
The user expanded the completion request to L1-L5. This document originally
defined only L1-L4; the fifth increment's substantive scope is awaiting explicit
confirmation. It must not be silently substituted with documentation cleanup.

## Source findings and decisions

| Inspected implementation | Observed mechanism | Decision for Kolyan |
| --- | --- | --- |
| DSH/Codewhale `4f6da02c2`, Fleet ledger and manager | Append-only coordination records, replay coordinates, bounded artifact references; notifications trigger another durable read | Durable coordinates, not in-memory wakes, determine observed progress |
| DSH delegated-work coordination ledger | Versioned decisions, scope contention and projection receipts | Coordination facts do not grant tool permission |
| Codex `5c5308fc9a`, Rollout recorder and Goal runtime | Recorded conversation history; persisted objective; next Turn admitted only if idle | Separate conversation reconstruction, execution recovery and task continuation |
| Garive `3d67ac6a`, Ledger state and governed reducer | Validated facts and transitions; recovery selected from execution position and executor evidence | Invalid facts fail closed; uncertain effects require evidence, not blind replay |

These are inspected source snapshots, not comparative performance benchmarks.
DSH's tree journal is still labeled a placeholder. A clean source update is not
test evidence. Never equate a saved working summary with restored authorization.

## Responsibility and data contracts

Ledger records committed execution and governance facts. Trajectory retains the
actual admitted model inputs, model outputs and tool observations, linked to
those facts. Trace records optional diagnostic observations and timing. These
are distinct semantics even when some records share a physical backend.
Required recovery writes fail execution when unavailable; optional diagnostics
must not invent completion or restore authority.

Server coordination owns persistent tasks, objectives, explicit success evidence,
waiting reasons and decisions to start another Turn. Runtime owns bounded
execution attempts and effect recovery. Turn and Step retain their current
execution roles; neither owns task scheduling, topology or Session storage.

Agent definition/revision, Agent instance, task, invocation and execution attempt
are separate identities. A self-call creates a new invocation even when it uses
the same definition. Relationships express call/delegation, dependency, result
consumption, continuation or supersession; a single parent pointer cannot encode
a join. Ancestry/causation cannot cycle. Iteration creates new identities, not a
rewrite of history. Topology has no implicit authorization, budget inheritance
or cancellation propagation. Those rules must be explicitly admitted policy.

Future extensible fact envelopes carry owning stream/position, typed subject and
causal references, namespaced kind, schema version and bounded payload/reference.
Registered validators check content, references and legal transitions. Unknown
critical semantics block recovery; unknown observational content cannot drive it.
No generic JSON/plugin registry is added before a concrete validated fact family.

## Delivery increments

### L1 Execution scoped Ledger reads

Implement this increment first, against the current fact shape, without claiming
the future envelope or task scheduler is present.

Introduce `LedgerQuery` with `execution_id: Option<String>`,
`event_id: Option<String>`, exclusive `after: u64`, optional inclusive
`through: Option<u64>` and `limit: usize`. Filters combine with AND. Limits are
1 through 1024; identities cannot be empty and `through` cannot precede `after`.
An empty valid range returns no rows. Results are strictly ascending by durable
cursor, limited after filtering. Missing events/executions return an empty result.
Queries perform no writes, claims, execution or authorization.

`LedgerStore::query(&LedgerQuery)` is mandatory, with no default full-scan
fallback. Provide shared `event_by_id` and paginated `execution_events_after`
operations through this required port. Keep `events_after` only as the existing
explicit global audit/export operation, not a production recovery fallback.
All three shipped stores implement query. SQLite uses execution/cursor indexes
and filters/LIMIT in SQL; FileLedger may scan its file but must return only the
requested bounded results. Do not claim indexed FileLedger performance.

Migrate production Runtime, Server and HTTP ownership/result reads to execution
queries or exact event lookup. Test Ledger wrappers implement the required query
port explicitly while preserving fault/race behavior. Existing test assertions
and scenario datasets must not be weakened or overwritten.

Pagination gives bounded individual reads, not a globally consistent snapshot
under concurrent appends. Callers requiring a frozen observation use `through`;
future cross-stream causality cannot be inferred from independent cursors.

### L2 Validated facts and recovery relationships

Implement versioned subject/reference contracts with first concrete invocation
and delegation facts. Validate identity separation, transitions and exact
idempotent semantics. Introduce atomic fact batches where correctness requires
them, rather than assuming a collection of individual appends is atomic.
Add explicit effect reconciliation evidence and outcomes. A receipt-backed
result may be reconstructed; Started without receipt is not permission to repeat.
No automatic restoration of approval after definition or constraint changes.

### L3 Linked Trajectory and Trace

Define queryable actual request/response/tool-observation records tied to
invocation/attempt/Step facts. Preserve source content when deriving compacted
context or UI views. Define integrity, access, redaction and retention for large
content references. Trace links aid diagnosis but do not replace recovery facts.

### L4 Minimal durable task coordination

Implement an objective, criteria/evidence, state, cumulative usage and waiting
contract above Runtime. Use bounded Turns and explicit continuation admission.
Separate approval resume, interrupted-attempt recovery, safe retry and next-Turn
continuation. Self-call/delegation and topology revisions retain identity,
budget, recursion and cancellation guards. No distributed lease infrastructure
or general workflow engine is required for the first useful task implementation.

## L2 through L4 implementation contract

Implementation authorized on 2026-09-30. These contracts supplement the increments
above, without expanding Step/Turn into task scheduling or adding historical
format adapters. Execution Ledger events continue to represent their existing
execution domain; the new versioned coordination journal represents task and
invocation facts. They are separate fact families, not alternative decoders for
the same stored format. Both may share the same SQLite database.

### Versioned coordination journal

`FactSubject { kind, id }`, `FactRef { stream_id, position, fact_id }`, and
`FactDraft { fact_id, subject, kind, schema_version, critical, causes, payload }`
form the append contract. `FactRecord { stream_id, position, draft }` is committed
evidence. Kinds are namespaced strings; identities are nonempty and bounded;
positions are one-based within the owning stream, not global execution cursors.
Payloads, batches and causal references are bounded. References must resolve to
an exact committed fact or an earlier member of the same transaction.

`FactJournal::read(stream_id, after, limit)` is an exclusive ascending query.
`append(stream_id, expected_position, batch)` atomically compares the stream
head and commits the entire batch. A stale head fails with no partial write.
Retrying an identical committed batch returns its original records; changing
content or a partly duplicated batch fails. Memory and SQLite adapters implement
the same contract. SQLite uses WAL/FULL durability and an immediate transaction.
No file adapter pretending that several JSONL appends are atomic is introduced.

Runtime effect reconciliation uses a trusted executor-owned inspection port, not
a model statement. It binds the saved prepared input, authorization and Started
fact to an exact execution/effect. A committed tool result and its reconciliation
evidence are persisted in one receipt append. Proven-not-committed and unknown
outcomes remain explicit evidence and never execute a retry or restore a grant.
Reconciliation request identities are idempotent; changed evidence under the same
identity is a conflict. Existing receipts are validated before reconstruction.

Task-family validation is Server-owned: append and replay both validate version,
subject, typed payload, references and transition. Unknown critical kinds or
versions block recovery. Unknown observational records remain queryable but
cannot change task state or grant authority. There is one concrete task family,
not an empty plugin registry. CAS protects validation against competing writers.

### Linked execution trajectory

A queryable `LinkedTrajectory` joins each actual execution record to its immutable
event identity and durable cursor. A caller supplies an explicit binding of task,
invocation, execution attempt, Session and Turn. Exact execution identity must
match; this never merges neighboring executions. Records retain actual requests,
responses, reasoning/text deltas and tool observations; Step identity is derived
only from recorded evidence, never invented. Repeated queries do not rewrite facts.

Content export is explicit and policy-controlled, with a redacted metadata-only
view available. Large content references carry digest and byte length; a local
content-addressed artifact store verifies bytes before returning them and enforces
bounded reads. Missing/corrupt retained content fails explicitly. Retention may
remove optional artifacts, but may not silently remove required recovery facts.
Trace gets durable event links through an additive linked-record contract; sink
failure remains diagnostic and cannot authorize rerun or replace Ledger evidence.

### Durable task coordinator

Server stores the objective, typed completion criteria, admitted agent definition
revision/instance, limits, waiting reason, cumulative usage and explicit success
evidence in a task stream. Invocation and attempt identities are distinct. A new
invocation is required for self-call, delegation or continuation; retries allocate
a new attempt only after an explicit safe-retry decision. No persisted grant is
inherited across a changed revision or changed constraints.

The coordinator validates creation, invocation admission, relation admission,
attempt start, attempt observation, result consumption, completion and cancellation.
Call/delegation ancestry is acyclic; dependency/result-consumption edges support
fan-out and join. Child completion alone does not commit the parent result.
Recursion, total invocations, bounded Turn attempts and cumulative token usage
are admitted limits. Cancellation propagation is explicit policy, not a property
of a parent pointer. Approval waiting consumes no worker and survives restart.

The Server driver invokes the existing SessionExecutionService with a fresh
executor/request supplied by the host, records authoritative stopped outcomes,
and reconstructs approval state from durable execution evidence. Interrupted
attempts fail closed to recovery-required; they are never automatically rerun.
Resume verifies the original binding, definition revision and constraints before
invoking the existing approval resume path. Completion criteria must be supported
by bound execution results or verified artifacts, not merely a model's claim.
This is a reusable Server service; new HTTP endpoints and distributed scheduling
are outside this increment. Tests call the service directly, including live models.

### Additional acceptance

New deterministic fixtures cover atomic rollback, concurrent stale writers,
identical/conflicting retries, dangling causal references, unknown critical and
observational facts, identity collision, revision change, self-call, fan-out/join,
unconsumed child results, recursion/shared budget limits, explicit cancel policy,
approval restart and effect uncertainty. Artifact tests cover hash mismatch,
missing content, bounds and path safety. Existing test fixtures remain intact.
New live scenarios drive the Server task service with real model calls and tools,
retain actual linked JSONL and compare fixture-owned semantic expectations across
every configured Provider/protocol/model row. Existing HTTP matrices are rerun.

## Acceptance and growth policy

Each increment adds module-local tests in separate source files and independent
data-driven integration fixtures. Existing cases remain regression evidence.
Test code alone writes temporary JSONL and comparison reports. New live cases
must exercise actual model-generated actions, not script fake live tool calls.
Every configured model/protocol/provider row is planned before execution; missing
credentials, ordinary Provider errors and assertion failures are failures, not
skips. Reports retain failures and actual reasoning/text/tool observations.

L1 gates: all-store filter parity, exclusive/inclusive cursor boundaries,
pagination, missing identity, invalid limits/ranges, interleaved executions,
restart reads, SQLite query-plan evidence and production scoped-only recovery.
Add a new live HTTP case: two independent Sessions share one ledger, approved
write and read-only follow-up survive process restart; unrelated execution facts
must not enter the target result/trajectory. Rerun the existing HTTP live matrix.

L2-L4 gates additionally cover self-call identity isolation, fan-out/join,
nested approval/restart, duplicate result observation, child terminal fact before
parent commit, changed revisions, policy-bound cancel propagation, recursion and
shared budgets, unknown critical kinds, uncertain-effect reconciliation and
evidence-backed completion. Deterministic tests prove crash and race invariants;
live tests prove model-facing integration. Neither substitutes for the other.

## Verification record

L1 accepted on 2026-09-30. L2-L4 production implementations and their initial
module/data-driven tests are now committed, but final expanded acceptance is in
progress. The L1 results below prove only scoped execution reads, not the later
fact envelope, topology execution, artifact retention or task coordination.

### Current L2-L4 verification checkpoint

- Atomic coordination journal, linked trajectories, integrity-checked artifacts,
  executor-owned effect reconciliation, task domain and host execution service
  are implemented in focused main-branch commits through `7abeaba`.
- The four-Turn task fixture passed all 19 configured model/protocol/provider
  rows in `kolyan-r1-matrix-XxVZbE`. It exercises two approved child writes,
  result consumption/join and a new Turn in the parent's retained Session.
  Revision, constraint and foreign-approval rejection checks run before resume.
- The fixture was subsequently expanded to five Turns with a third-level
  delegated write and approval reconstruction. Its deterministic run passed in
  `kolyan-task-offline-tOGWyq`; the expanded live matrix is running. Four-Turn
  evidence does not prove this additional branch.
- The scoped HTTP process matrix passed 19/19 in `kolyan-r1-matrix-9JGvaL`.
  Workspace default tests and strict all-target Clippy passed at this checkpoint;
  explicitly ignored historical network suites are not counted as executed.
- Initial task report `HUwwPH` retained nine Anthropic failures. Actual neutral
  inputs contained both tool results. Provider packaging was changed to one
  user message per adjacent same-role result batch, matching the official Tool
  Runner; three new mapping regressions passed. The next report `ag3nEa` passed
  all nine Anthropic rows but retained a MiniMax/OpenAI tool-availability refusal.
  Its wire body included tools. The new fixture clarifies that registered tools
  are executed by the host; no call-count/content assertions were removed.
- All evidence roots above are under
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/`.
  Actual sending-body logs are temporary `/tmp/kolyan-task-live-*-wire.log`
  files enabled by `KOLYAN_DUMP_MODEL_REQUESTS=1`; authentication headers are not
  logged. Test code exports journal/execution JSONL even during assertion unwind.
- Additional service recovery, nested governance, physical artifact completion
  checks and live cancellation-policy cases remain under development. No L4
  completion or L5 implementation is claimed by this checkpoint.

| L1 gate | Inspected evidence |
| --- | --- |
| All-store query parity, cursor bounds, AND filters, invalid queries | `kolyan-ledger/src/query/tests.rs`, all three stores |
| More than one page, interleaving, frozen through cursor, reopened stores | Same query suite; 1,025 target events among 2,050 facts |
| Native SQLite execution and exact-event seeks | Query suite runs `EXPLAIN QUERY PLAN` and verifies both indexes |
| Fail-closed adapters and storage/decode errors | Query helper and backend error tests; no audit fallback |
| Runtime cancellation, approval reconstruction, receipt replay and uncertainty | `kolyan-runtime/src/tests/scoped*` and `driver/tools/tests/scoped.rs`; global audit disabled |
| Server isolation and projection repair | `kolyan-server/src/tests/scoped.rs`; unrelated Session remains unchanged |
| Persistent cross-module recovery | `ledger_scoped_recovery`: both independent fixture cases pass after SQLite reopen |
| Real HTTP isolation, two Sessions, approval restart and subsequent Turn read | New live matrix: 19/19 rows, 57 Turns, 57 model-generated calls and matching effect receipts |
| Existing HTTP regression | Existing live matrix: 76/76 rows, 95 Turn cases |

Final checks passed: `cargo test --workspace --quiet`,
`cargo fmt --all --check`, `cargo clippy --workspace --all-targets -- -D warnings`,
`cargo check --workspace --all-targets`, and `git diff --check`.
Targeted Ledger/Runtime/Server suites passed 18/22/12 tests respectively.
Source-layout inspection found no inline tests in production files; the existing
denial test was moved under the standard tests subtree without changing its
scenario or assertions. Local documentation links were checked separately.
Historical ignored network and SDK-oracle suites were not all rerun; the two
HTTP matrices below were explicitly executed, not inferred from offline tests.

Run commands:

```sh
cargo test -p kolyan-integration-tests --test ledger_scoped_recovery -- --nocapture
cargo test -p kolyan-server-service --test server_http_process http_process_live_matrix -- --ignored --nocapture
cargo test -p kolyan-server-service --test server_http_process http_process_ledger_live_matrix -- --ignored --nocapture
```

Credentials were injected from the user's shell environment, never stored in
fixtures. Final matrices used every configured row, with one attempt per row,
zero failures, skips or unrun rows. Reports and actual request/output/reasoning,
tool results, receipt bindings, restart projections and JSONL remain outside Git:

- Existing HTTP matrix: 350.91 s,
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-GQMCNh/report.json`.
- New Ledger matrix: 250.37 s,
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-NZkRLN/report.json`.
- Deterministic recovery:
  `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-ledger-recovery-i2VaRS/`.

Earlier diagnostic failures remain failures, not retrospectively accepted passes.
The first new matrix passed 17/19; its two failures were successful writes followed
by an extra model-generated verification read, not repeated writes or missing
tool results. One model miscounted a 17-byte marker as 16 bytes. Actual successful
receipts were present in the subsequent model requests. The new fixture now states
each write's byte count and that its successful receipt is sufficient; exact-one-
call expectations remain unchanged. Evidence is retained in
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-fdwpOa/`.

An intermediate fixture mistakenly named both Session markers in shared system
text. Isolation assertions correctly rejected it; that run was stopped and is
not acceptance evidence (`kolyan-r1-matrix-Y5ncXP`). The system text now contains
neither marker, with an added offline guard. The approval fixture's mismatched
capability ceiling was also corrected with an explicit scoped `file.write`
manifest and grant check; production policy was not weakened. No test processes
remain active. L1 is committed on main in focused batches; no push was requested.
