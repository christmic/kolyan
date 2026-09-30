# Durable Task Foundation

## Scope and status

Design approved for incremental implementation by the user on 2026-09-30.
This requirement is the implementation and acceptance authority for strengthening
Ledger, Trajectory and long-task execution after the minimal HTTP boundary.
The source-backed rationale remains in [kernel evolution](../architecture/kernel-evolution.md).
No multi-agent capability is considered implemented merely because its identities
or relationships appear in this design. New-project development retains no legacy
format adapters, old-interface aliases or fallback reads hiding unsupported ports.

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

L1 accepted on 2026-09-30. L2-L4 remain specified future increments, not implemented
or accepted by these results. Scoped reads do not introduce the future fact
envelope, topology execution, artifact retention or durable task scheduling.

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
