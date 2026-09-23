# 0023 Server JSON-RPC Entrypoint

Status: implemented as the first transport adapter.

## Decision

Server exposes a small JSON-RPC control-plane adapter over stdin/stdout. The
wire contract is transport-neutral and can later be mounted on HTTP or
WebSocket without changing Coordinator, Session, or Runtime.

```text
JSONL / HTTP / WebSocket
          ↓
      JSON-RPC codec
          ↓
      Server facade
          ↓
   Coordinator / Session / Runtime Service
```

## Methods

- `execution.start`: admit a new Execution;
- `execution.resume`: admit a suspended Execution;
- `execution.cancel`: persist cancellation;
- `execution.status`: read the Ledger-derived state.

The first transport is deliberately control-plane only. Model/provider
construction remains application-owned and uses `ExecutionService`; the RPC
adapter does not invent a provider from untrusted JSON.

## Wire rules

- JSON-RPC version is `2.0`;
- every request has an opaque `id`;
- successful results contain an explicit `state` or `admission`;
- malformed parameters return a structured error and do not mutate Ledger;
- one input line produces at most one output line;
- API keys and provider configuration never cross this control-plane contract.

## Verification

- codec tests cover valid start/status/cancel and malformed parameters;
- integration tests use a real FileLedger and replay the RPC request sequence;
- Server Runtime live tests remain the execution-level contract.
