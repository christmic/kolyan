# Kernel Assessment and Extension Boundaries

## Scope and conclusion

Kolyan has room to grow without replacing its Step, Turn, Runtime and Server
boundaries. Its current strengths are explicit recovery contracts, durable
effect accounting and model-neutral request planning. These are architectural
advantages for a reusable personal Agent kernel, not evidence of better task
success, latency or cost than other agents.

This assessment records the local source snapshots reviewed on 2026-09-30:
Kolyan `16a8e86`, Codex `5c5308fc9a`, Garive `3d67ac6a` and Grok Build
`4247f661`. Comparisons concern these implementations, not every version or
deployment. No comparative benchmark or new live acceptance run was performed.
Existing requirements remain the authority for implementation and test status.

## Responsibility boundaries

The Session, Turn and Step hierarchy describes conversation execution units;
Server and Runtime describe orchestration responsibilities, not extra
conversation units.

```text
Interactive client -> Server process -> Server coordination
                                         | Session input and commit
                                         v
One-shot CLI -------------------------> Runtime driver
                                         v
                                        Turn
                                         | Step -> ModelProvider -> adapter -> protocol
                                         | tool invocation -> ToolExecutor
                                         | policy, admission and event ports
                                         v
                                  result or suspension
```

Server owns Session coordination and execution ownership. Runtime owns durable
execution facts, cancellation admission and effect receipts. Turn owns the
bounded loop, tool-result feedback and continuation. Step owns one model
interaction and its event/result contract. Storage implementations own durable
data access; Trace observes execution and is not the authority for recovery.

Single-process execution does not require distributed leases. Any future
cross-worker ownership coordination belongs above Runtime in Server's
coordinator. It must not become a Step or Turn responsibility.

## Comparative findings

| Area | Kolyan evidence and practical consequence | Comparison and limitation |
| --- | --- | --- |
| Reusable execution boundary | [Module map](module-map.md) separates execution, protocols, storage and applications. Applications can share the same loop. | Well suited to embedding; fewer product responsibilities in the kernel is not proof of greater capability. Garive also separates execution ports. |
| Durable approval | [Continuation](../../crates/kolyan-core/src/turn.rs) and [restore validation](../../crates/kolyan-core/src/turn/engine.rs) retain pending calls and model context. Approval need not retain the original process stack. | More explicit durable continuation than the inspected Codex command-approval path, which stores an active-turn oneshot callback in `codex-rs/core/src/session/mod.rs`. This does not imply Codex lacks persisted history or other recovery paths. |
| Effect safety | [DurableTools](../../crates/kolyan-runtime/src/driver/tools.rs) reuses receipts and refuses blind replay of Started without a receipt. | A sound safety property, not a lead over Garive. Garive has committed governed results and explicit operator-reconciliation suspension. |
| Provider variation | [RequestPlanner](../../crates/kolyan-model/src/planning.rs) separates protocol validity from endpoint/model parameter support. | A strength for multi-provider use. Neutral types must still preserve protocol-specific semantics through explicit extensions. |
| Long context | Session selects conversation or full trajectory; no equivalent integrated token-budget/compaction port was found in the production execution path. | Codex has pre-sampling and intra-turn compaction; Grok has threshold triggering and reduction checks. More steps alone cannot solve context growth. |
| Tool preparation | [InvocationClaim](../../crates/kolyan-policy/src/lib.rs) currently infers semantics from built-in tool names and a `path` field. | Garive's `ToolPreparationPort` derives a validated Prepared Call independently of authorization. Kolyan still needs this boundary for heterogeneous tools. |
| Authorization binding | Grants and durable effects bind call identity, input and policy/constraints, but do not provide Garive's equivalent exact tool-revision binding. | An approval issued before a tool implementation changes needs an explicit validity rule. Garive binds Prepared Call digest, tool revision and execution requirements. |
| Isolation | [Workspace](../../crates/kolyan-tools/src/workspace.rs) uses a directory capability for file tools. | This is not a general process/network sandbox. Codex and Grok have executor-level isolation mechanisms. |
| Recovery scaling | Effect lookup and Session reconciliation read `events_after(0)`. | Correctness and scalability are separate. Long histories need bounded queries or materialized projections. |
| Progress detection | [ProgressPolicy](../../crates/kolyan-policy/src/progress.rs) detects consecutive identical completed invocation signatures. | Does not generally detect alternating cycles, changing arguments without progress, or semantic task completion. It is a heuristic guard. |

Reference implementations are located in sibling repositories, not vendored
dependencies: Garive `engine/core/src/governed_execution.rs`,
`engine/tools/src/governed_types.rs` and `governed_reducer.rs`; Codex
`codex-rs/core/src/session/turn.rs` and `tools/orchestrator.rs`; Grok Build
`crates/common/xai-grok-compaction/src/intra_compaction/trigger.rs` and
`crates/codegen/xai-grok-sandbox/src/lib.rs`. Recheck them before a future design
decision; these snapshot findings are not permanent feature inventories.

## Existing interfaces and remaining gaps

An existing trait is an extension point only when the execution path actually
uses it and its contract carries the information required by the extension.
A reserved directory or enum variant is not a finished extension mechanism.

| Concern | Existing integration | Status and required evolution |
| --- | --- | --- |
| Model invocation | `ModelProvider::stream`, protocol/provider separation, `ParameterTable` | Available. New adapters must preserve stream terminal/error semantics; the parameter registry can evolve without vendor branches in Turn. |
| Tool execution | `ToolExecutor::execute_invocation`, `map_tool_executor` | Available for wrappers and executor replacement. Default `execute_with_grant` delegates without enforcing a grant; governed hosts must supply an enforcing executor rather than assuming every implementation is safe. |
| Execution admission | `TurnBoundaryControl::admit` | Available for cancellation and fail-closed boundary checks. It does not itself guarantee exactly-once external effects. |
| Authorization | `PolicyResolver` exists, but `TurnExecutor` stores `Arc<PolicyEngine>` | Partial. The resolver trait alone is not a pluggable Turn policy boundary; batch planning, context and progress also need an explicit contract. |
| Session context | `SessionStore`, immutable Turn input and `SessionContextPolicy` | Available for persistence and coarse projection. The enum is not a token-budget or compaction strategy interface. |
| Durable execution | `LedgerStore`, `DurableTurnDriver`, effect receipts | Available for backend replacement. Indexed effect queries and external-effect reconciliation need additional contracts. |
| Observation | `StepEventRecorder`, `TurnEventRecorder`, `TraceSink` | Available. Optional observations must remain distinct from required recovery facts and must follow redaction/retention rules. |
| Cross-process clients | Server service and execution protocol | Available boundary. Future transport adapters should reuse services rather than copy Turn or approval logic. |

## Planned enhancement contracts

The names below describe proposed responsibilities, not APIs already present.
Do not add empty traits or a generic plugin framework just to claim extensibility.
Introduce a focused interface with its first real implementation and tests.

### Context preparation

Server prepares Session context for the first Step. A Turn-level preparation
port is needed before subsequent Steps to handle intra-turn growth. Runtime
supplies the implementation; Turn invokes it without depending on a database,
model vendor or summarization service.

Inputs should include the current message snapshot, model limits, output reserve
and a versioned context policy. Outputs should include the effective request,
budget accounting and provenance for any reduction. Preserve system constraints
and tool-call/result pairing. A summarizer failure must return an explicit
decision, not silently discard history. Store the admitted effective context
and its policy/provenance so approval recovery does not reconstruct a different
request from current Session history. Keep the source trajectory and Ledger
facts intact; compressed context is a derived view, not a replacement ledger.

### Prepared tool calls and policy resolution

Tool definitions and adapters validate arguments and derive exact resources,
effects, execution requirements and tool revision. Policy evaluates that
prepared description with dynamic context. It must not learn each tool's
parameter format. The executor enforces the granted constraints.

Bind preparation, authorization, suspension and receipt to the same invocation
and immutable input. Reject changed arguments or tool revisions on recovery,
or require a new preparation/authorization decision. Replace the concrete Turn
policy dependency with a port that covers batch decisions and progress, not
only the current single-call `PolicyResolver::decide` method.

### Effect reconciliation and bounded queries

Provide execution/effect-scoped lookup and incremental projections while
keeping the append-only facts authoritative. Backend transactions must preserve
atomic cancellation admission and duplicate handling.

External-effect reconciliation requires evidence that the operation completed,
failed before effect, or remains uncertain. Resuming must consume that evidence
without automatically rerunning a non-idempotent operation. Existing
`SessionExecutionService::reconcile` repairs Session commit projection; it is
not an external-effect reconciliation protocol. Idempotency identities are
effective only if the actual tool/service honors them.

### Isolated execution and progress assessment

Process/filesystem/network isolation belongs to tool executors or environment
adapters. Unsupported mandatory isolation must fail closed. Runtime records
the relevant enforcement result; Step and Turn do not implement OS sandboxes.

Progress assessment should support bounded cycle detection and explicit polling
policy. Keep heuristic no-progress detection separate from task-success
verification and hard execution budgets. Do not make another model mandatory
for every policy decision.

## Implementation order and acceptance

1. Context preparation and budget accounting.
2. Prepared calls, replaceable policy resolution and exact authorization binding.
3. Effect reconciliation and scoped Ledger queries/projections.
4. Isolated executor support and stronger progress assessment.

Each increment needs a requirement/design document before implementation,
module-local tests, new data-driven integration cases and live-model scenarios.
Do not overwrite existing cases to conceal a regression. Offline tests own
deterministic invariants; live tests verify the model-facing integration.

Required scenario coverage includes context overflow and compaction failure;
tool-result pairing after reduction; approval/restart with unchanged and changed
tool revisions; a side effect completed before receipt persistence; confirmed
reconciliation without duplicate execution; cancellation/admission races;
sandbox escape denial; alternating tool cycles and legitimate polling. Add
long-history query bounds and latency checks without claiming performance from
interface shape alone. Tests write artifacts and compare semantic trajectories;
normal Step/Turn execution does not write test files.

Provider support facts, request acceptance and empirical task reliability are
different dimensions. A few schema violations cannot by themselves establish
that an endpoint does not support native structured output. Record explicit
unsupported responses separately from probabilistic failures and transport
errors. Report skipped combinations and retained failures in acceptance results.

## Theoretical support

- [ReAct](https://arxiv.org/abs/2210.03629) supports interleaving reasoning,
  action and observation. It does not establish that naming units Step/Turn
  improves reasoning or that tool use guarantees correctness.
- [Durable execution and idempotency](https://docs.aws.amazon.com/durable-execution/patterns/best-practices/idempotency/)
  explain why interruption can leave an external effect uncertain. Receipts,
  idempotency and reconciliation are distinct safeguards, not a universal
  exactly-once guarantee.
- [Saltzer and Schroeder's protection principles](https://web.mit.edu/Saltzer/www/publications/protection/)
  motivate least privilege, fail-safe defaults and complete mediation. Their
  application here is a design inference: declarations, dynamic authorization
  and technical enforcement must remain separate and cooperate.
- [Lost in the Middle](https://arxiv.org/abs/2307.03172) identifies limitations
  in use of long contexts. It motivates evaluating context selection, but does
  not prove that any particular summarization strategy improves task success.
- [Codex sandbox documentation](https://learn.chatgpt.com/docs/sandboxing)
  explicitly separates approval policy from technical sandbox boundaries.

## Architectural decision

Retain the existing execution layers. Extend missing ports where concrete
capabilities require them; do not push Session storage, worker leases, OS
isolation or provider names into the loop. Module boundaries leave space, but
the context, preparation and policy gaps require local interface changes.
This is an evolution plan, not a claim that all future enhancements are already
plug-and-play or that live acceptance is complete.
