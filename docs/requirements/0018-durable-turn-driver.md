# 0018 Durable Turn Driver

Status: implemented; extended by [0025 R2](0025-architecture-hardening.md).

## Goal

Connect the existing Core `TurnExecutor` to Runtime-owned execution identity and
Ledger boundaries. Runtime remains Agent-neutral: it stores `session_id`,
`turn_id`, `execution_id`, approvals and lifecycle facts, but does not interpret
Agent definitions or tool business meaning.

## Contract

`DurableTurnDriver` owns the Runtime attempt. It:

1. Creates an execution fact before starting Core;
2. injects `TurnBoundaryControl` backed by the Ledger;
3. persists Step, Tool, Approval and Terminal boundaries;
4. persists an approval checkpoint before returning `AwaitingApproval`;
5. loads the checkpoint by `execution_id + approval_id` on resume;
6. exposes cancellation by execution identity, not process identity;
7. records actual model requests and full Turn/Step/Tool facts incrementally;
8. wraps real tools with receipt-backed effect execution and projects Trace separately.

The Core executor is reconstructed for resume. The Runtime never calls the
model again to recreate the completed pre-approval Step.

## Current implementation boundary

The adapter uses the storage-neutral `LedgerStore` port; the service assembles
SQLite. Durable attempt claims prevent accidental replay, and tool outcomes share
the Effect receipt contract. Started without a receipt is uncertain. Cancellation
admission is atomic in supported backends. Runtime does not own distributed leases
and does not claim exactly-once external effects. Current guarantees, process-crash
tests and acceptance evidence are maintained in 0025 rather than duplicated here.

## Tests

- Runtime unit tests cover the existing event projection and Ledger facts;
- Runtime boundary integration tests use a real FileLedger and real Core
  `TurnExecutor` with a deterministic provider;
- the same tests verify durable cancellation and receipt replay after Runtime
  reconstruction.
