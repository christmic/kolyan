# 0020 Server and Execution Coordinator Boundary

Status: implemented as the first Server slice.

## Decision

`Server` is the external execution boundary. `ExecutionCoordinator` is its
internal orchestration component. Runtime executes one already-admitted
Execution and does not manage ownership, leases, scheduling, or transport.

```text
Client / API
    ↓
Server
    └── ExecutionCoordinator
            ↓
        Runtime
            ↓
        Turn → Step → Model / Tool
```

## Responsibilities

- **Server** accepts start, resume, approval, and cancellation commands and
  routes them by `execution_id`.
- **ExecutionCoordinator** derives the current lifecycle from the Ledger,
  prevents duplicate in-process execution, admits recovery, and tells the
  caller when an Execution is terminal.
- **Runtime** runs the admitted Execution and persists execution facts. It does
  not call a lease API or decide which worker should run an Execution.
- **Ledger** remains the source of truth for lifecycle facts; it is not a
  scheduler.

## Current lifecycle

```text
New → Running → Suspended → Running → Completed
                 ├───────────────→ Cancelled
                 └───────────────→ Failed
```

The first Server slice is single-process. The Coordinator keeps an in-memory
active set to prevent two local callers from running the same Execution. A
restart can explicitly call `recover`; it uses the Ledger's `Running` or
`Suspended` state as the recovery input.

## Non-goals

- No lease, fencing token, distributed lock, or worker registry is part of the
  Server v0 contract.
- No Session implementation is introduced.
- No model/provider/tool policy is interpreted by Server.

## Future extension

If Kolyan becomes multi-process, Server may add a Coordinator adapter for
worker assignment or lease management. That concern stays above Runtime;
Turn, Step, and Runtime APIs should remain unchanged. Runtime may later accept
an opaque execution admission context only at its boundary if a storage-backed
Coordinator needs fencing, without exposing lease semantics to Core.

## Acceptance

- start/resume/cancel are addressed by `execution_id`;
- duplicate local starts are rejected;
- terminal Executions cannot be started or resumed;
- restart recovery is explicit and derived from Ledger facts;
- unit and integration tests use a real FileLedger and do not require a model
  provider or API key.
