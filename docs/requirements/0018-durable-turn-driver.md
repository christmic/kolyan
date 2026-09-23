# 0018 Durable Turn Driver

Status: implemented as the first durable-driver slice.

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
7. records the completed Turn trajectory and Trace projection.

The Core executor is reconstructed for resume. The Runtime never calls the
model again to recreate the completed pre-approval Step.

## Current implementation boundary

The reference adapter uses the append-only FileLedger and a storage-neutral
`LedgerStore` port. It verifies close/reopen recovery and idempotent persisted
terminal effects. SQLite transactional append, cross-process lease/fencing,
uncertain-effect reconciliation and a full durable Tool executor remain the
next Runtime slice; this driver does not claim exactly-once external effects.

## Tests

- Runtime unit tests cover the existing event projection and Ledger facts;
- Runtime boundary integration tests use a real FileLedger and real Core
  `TurnExecutor` with a deterministic provider;
- the same tests verify durable cancellation and receipt replay after Runtime
  reconstruction.
