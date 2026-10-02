# Agent Framework Phase Review and Evolution Boundaries

## Phase conclusion and scope

As of 2026-10-02, Kolyan has a governed, recoverable execution kernel.
It is not yet an accepted, reliably delivered personal Agent product.
The existing layers should remain; the next gains depend on real task
completion, context engineering and operational integration, not additional
empty abstractions.

This is a phase summary, not final acceptance or authorization to implement
every recommendation. The implementation assessment includes the current
workspace, including uncommitted changes. Main HEAD at review was
`94adef0d`; that commit alone does not contain all capabilities described here.

This document owns architectural conclusions and the frozen phase snapshot.
[Requirement 0030](../requirements/0030-governed-agent-execution.md) owns current
implementation and acceptance evidence.
[Requirement 0029](../requirements/0029-durable-task-foundation.md) owns the
durable foundation contracts. Do not maintain a second rolling test log here.

## Assessment sources and limits

| Source | Local revision / scope |
| --- | --- |
| Kolyan | Current workspace; main HEAD `94adef0d`, with implementation changes not yet fully committed |
| Codex | `5c5308fc9a`; inspected execution, context, extensions, memory and subagent consumers |
| Garive | `3d67ac6ad6`; inspected governed execution, planning, evidence and capability preparation |
| Grok Build | `4247f66168`; inspected goals, memory, MCP, skills, hooks and background execution |
| DSH / Codewhale | Local `deepseek-tui`, `4f6da02c2`; inspected context reduction, fleet, tools and memory |
| Claude Code | Official checkout `7974a70773` exposes plugins and hooks, not the complete engine; the separate third-party checkout is documentation, not executable core |

These are local source comparisons, not comparative performance benchmarks.
No new live-model run was performed for this review. A mechanism in a library
does not prove production integration or operational reliability.

The local [AI Agent handbook](../AI-Agents-in-Depth-zh-CN.pdf), version 2.0,
informs the review: printed pages 18 (Harness), 43 (prompt cache), 57–59
(progressive Skill loading), 70 (context reduction), 73 (memory), 104–108
(tool/MCP boundaries), 163 (asynchronous coordination), 186–187 (repeatable
evaluation), and 265–270 (evolution and isolation).
These are design references, not evidence that Kolyan implements the
capabilities. Input coercion and silent fallback recipes are not adopted:
Kolyan retains strict protocol and authorization contracts.

## Current framework and responsibilities

Conversation units remain `Session -> Turn -> Step`. Agent, Server and Runtime
describe different orchestration responsibilities, not extra conversation units.

```text
Agent definition / catalog / scoped permissions
                       |
AgentRunner: root admission, delegation, recursion and bounded joins
                       |
Server: Task / Session / approval / execution ownership
                       |
Runtime: bounded durable execution, effects, receipts and recovery
                       |
Turn: multi-Step loop, tool batches, results and continuation
                       |
Step -> neutral model request/events -> provider mapping -> protocol
                       |
Prepared tool call -> policy decision -> enforcing isolated executor

Ledger: authoritative admitted facts and evidence
Trajectory: actual model/tool interaction records linked to those facts
Trace: diagnostic timing and observations, not recovery authority
```

AgentRunner exists as a library and in test hosts. It is not yet assembled into
a delivered production Agent host:
[the Agent application entry](../../apps/kolyan-agent/src/main.rs) is a
placeholder, and [the Server service](../../services/kolyan-server/Cargo.toml)
does not depend on `kolyan-agent`. Existing CLI/HTTP execution is not equivalent
to a production root/delegated Agent service.

Server owns long-lived coordination and execution ownership; Runtime executes
bounded attempts. Single-process operation needs no distributed lease.
Future cross-worker coordination belongs above Runtime in Server.
Approval waiting persists continuation, not the original process stack.
Storage supplies durable access; Trace cannot recreate an authorization.

## Implemented capabilities and remaining limits

“Implemented” below describes the inspected workspace, not full acceptance.

| Area | Implemented in this phase | Remaining limit |
| --- | --- | --- |
| Model / provider | OpenAI and Anthropic mapping, neutral requests/events, reasoning, structured output, cache and parameter support tables | Protocol support and request acceptance do not establish reliable task success; cross-model takeover is not delivered |
| Step / Turn | Streaming and aggregation, bounded multi-Step loop, tool batches/results, cancellation, budgets and continuation | Standalone Step deadline handling deserves a focused check; Turn already wraps model opening with a deadline |
| Session / Runtime | Durable context, approval reconstruction, receipts and reconciliation | No universal exactly-once guarantee for external effects |
| Ledger / Trajectory / Trace | Validated authoritative facts, linked interaction evidence and separate diagnostics | Long-history performance, operational querying, privacy and retention need further work |
| Tools / isolation | Read, Write, Edit, Shell; prepared grants and native macOS workers | General output artifacts, pagination, search, background jobs and cross-platform isolation are not delivered |
| Delegation | Named and inline definitions, self-call identities, private context, durable child waits/results and bounded joins | Complex live reliability remains incomplete; mailbox steering and parallel token reservations are absent |
| Long tasks | Cross-Turn continuation, context projection, approval reconstruction and idempotent finalization | Two live finalization cases failed; host-constructed progression is not autonomous planning |
| Context engineering | Source provenance, user anchors, tool-call/result pairing and Strict/Inspect budget modes | No production tokenizer, automatic summarization, semantic compaction or general relevance retrieval |
| Goals / planning | Task objectives, dependencies and criteria | Root admission currently uses execution completion, not a business-goal verifier or general planner/replanner |
| Memory | Durable Session facts and history | No complete extraction, retrieval, revision, conflict-resolution and forgetting pipeline |
| MCP / Skills / hooks | Concrete inventory and Turn preparation integration | Full MCP transport/discovery/auth lifecycle, Skill body loading and general hook lifecycle are not delivered |
| Evaluation / product | Data-driven tests, actual JSONL evidence and independent candidate review | Live matrices are not fully passing; no self-iteration candidate has been accepted and merged; production Agent host is missing |

Important source-level distinctions:

- [Root admission](../../crates/kolyan-agent/src/runner/admission.rs) uses
  `ExecutionCompleted`; a final refusal can end execution without achieving the
  user's business goal. [The Agent README](../../crates/kolyan-agent/README.md)
  records this distinction and the missing production context components.
- [Context preparation](../../crates/kolyan-agent/src/context.rs) and
  [projection](../../crates/kolyan-agent/src/context/projection.rs) preserve
  provenance and pairing. A diagnostic byte estimate is not a trusted tokenizer.
- [Delegation scheduling](../../crates/kolyan-agent/src/runner/delegation/scheduling.rs)
  disables parallel execution under a finite token ceiling rather than
  pretending concurrent reservations exist.
- [Step](../../crates/kolyan-core/src/step.rs) awaits stream opening before its
  controlled stream is established and checks deadlines during polling.
  This is a static standalone-Step concern, not a reproduced Turn hang:
  [Turn](../../crates/kolyan-core/src/turn/engine.rs) wraps opening in a timeout.
- [Invocation routing](../../crates/kolyan-agent/src/runner/routing.rs) maps
  preparation errors into denial; malformed input and authorization denial
  should eventually be distinguishable. This is a finding, not an applied fix.
- [Progress policy](../../crates/kolyan-policy/src/progress.rs) detects repeated
  completed signatures. It is not semantic progress assessment or goal proof.
- [Suspension recovery](../../crates/kolyan-runtime/src/driver/suspension.rs)
  includes scoped history reads from position zero. Measure long histories
  before asserting a performance defect or selecting a projection strategy.

## What the other implementations contribute

| Reference | Concrete mechanism worth learning from | Implication for Kolyan |
| --- | --- | --- |
| Codex | [Compaction](../../../codex/codex-rs/core/src/compact.rs), extension/Skill selection, tool search, process management and subagent delivery | Turn existing boundaries into usable context and extension consumers; do not claim superiority from interface shape |
| Garive | [Goal evidence](../../../Garive/runtime/replica/src/goal_evidence.rs), [plan proposal runtime](../../../Garive/runtime/replica/src/plan_proposal_runtime.rs), governed capability preparation | Separate admitted plans, goal evidence and execution receipts; add bounded correction without letting a plan grant authority |
| Grok Build | [Goal classification](../../../grok-build/crates/codegen/xai-grok-shell/src/session/goal_classifier.rs), memory search, Skill loading, MCP lifecycle and background tasks | Evaluate completion separately and make extension lifecycles operational; model verification is not deterministic proof |
| DSH / Codewhale | [Turn-loop compaction](../../../deepseek-tui/crates/tui/src/core/engine/turn_loop.rs), tool setup, fleet recovery and native memory | Preserve cancellation and atomic context replacement; distinguish recovery records from automatically resumed execution |
| Claude Code | [Official plugin surface](../../../claude-code/plugins/README.md) and public hook configuration | Learn from public integration contracts only; do not infer unavailable engine internals |

Kolyan's strengths are explicit recovery, authority/effect separation,
model-neutral mapping and topology identities. Its weaknesses are completion
semantics, context maturity, extension integration, operational tooling and
demonstrated reliability. None of these source observations establishes a
comparative lead in success rate, latency or cost.

## Frozen phase acceptance snapshot

The latest recorded terminal MiniMax runs at this review show:

| Matrix | Data cases | Result |
| --- | --- | --- |
| Host feedback / complex delegated execution | 16 | 7 passed, 9 failed |
| Long-task scenarios | 20 | 18 passed, 2 failed |
| Self-iteration candidate acceptance | Reviewed candidates r1–r8 | None accepted or merged |

The long-task failures were in finalization: 6/8 passed. Continuation,
projection and ordinary ten-Turn groups each passed 4/4.
These are data-case counts, not Rust test-function counts.

Retained failures cannot be attributed wholesale to the model or framework.
The long-task run used a frozen binary preceding later diagnostics; those
later diagnostics are not evidence about that earlier run.
Offline checks and passing simpler cases do not replace failed live acceptance.
See requirement 0030 for logs, trajectories, exact cases and current gates.

## Remaining work and proposed evolution

### First: close the three already-recorded acceptance items

The [remaining three-item checklist](../requirements/0030-governed-agent-execution.md#剩余目标三项验收清单)
remains the implementation authority:

1. Stabilize complex delegation, recursive calls and multiple-child approval
   scenarios using actual model trajectories and causal evidence.
2. Diagnose and close the two long-task finalization failures without weakening
   existing cases or treating a rerun as proof of a fix.
3. Obtain a genuinely model-authored worktree change; independently review it,
   run relevant regression and live validation, then decide whether to merge.

The following stages are architectural recommendations, not additional
implementation authorization under the current acceptance work.

### Next: verify goals and assemble the usable host

Introduce explicit postconditions and evidence consumption, distinguish
execution completion from goal satisfaction, and support bounded correction.
Then specify a production Agent host using the existing runner and services.
Do not silently expand current CLI/HTTP scope or replace Server/Runtime.

Expected result: a task can state what succeeded, what remains and why it
stopped; the same governed mechanism becomes usable outside test hosts.

### Next: context and long-running resources

Add trusted token accounting, selection and compaction with provenance.
Keep source facts intact, retain safety constraints and tool pairing, and
record admitted derived context for recovery. Evaluate stable prompt prefixes
and dynamic append behavior rather than assuming cache savings.

Add immutable large-output references and targeted reads; define background
execution, observation and cancellation with their first concrete tools.

Expected result: longer useful tasks without unbounded context, lost evidence
or synchronous waiting on every external operation.

### Next: governed extensions and memory

Skills need versioned metadata, selective body/reference loading and explicit
scopes. MCP needs actual transport, discovery, authentication refresh,
cancellation and ownership checks. Hooks need concrete consumers, ordering and
failure contracts. Neither Skill text nor MCP descriptions grant permission.

Memory should be selective, scoped and revisioned, with provenance, conflict
handling and forgetting. Recalled material is derived context, not a rewrite
of Ledger facts.

Expected result: reusable knowledge and external capabilities extend the Agent
without bypassing preparation, authorization or executor enforcement.

### Sustained work: coordination and evaluation

External wakeups, mailboxes, steering and concurrent budget reservations belong
to Server/Agent coordination; Runtime remains a bounded executor.
Evaluate resettable tasks with goal postconditions, retained failures, repeated
success, security checks, cost and holdouts. Execution logs alone are not proof
of improvement.

Voice, GUI, A2A interoperability and model training may follow real demand;
they are not mandatory completion criteria for this phase.

## Expected outcome and stable design rules

The target is a personal Agent that completes real tasks, distinguishes done
from unfinished, corrects within bounds, survives long approval waits without
the original process, and does not repeat confirmed effects. Extensions must
preserve authority, and independent regression must demonstrate improvement.
This does not mean infinite execution, universal exactly-once effects,
automatic approval or support for every workflow topology.

Keep these foundations stable:

- Separate definition/version, instance, invocation, attempt, task and effect
  identities. Named and inline Agents obey the same authority requirements.
- Delegation and dependency edges do not grant permission. Joins consume
  explicit results; one parent identifier cannot express every relationship.
- Causal ancestry must not cycle. Iteration creates new identities and facts,
  rather than rewriting history or creating a causal loop.
- Bind preparation, grant, implementation revision and receipt to the same
  immutable invocation. Recovery must not silently reauthorize changed input.
- Ledger is authoritative; Trajectory preserves interactions; Trace diagnoses.
  Summaries, memory and UI projections remain derived views.
- Validate registered authoritative extensions. Unknown recovery-critical
  facts fail closed; observational data must not acquire execution authority.
- Preserve strict contracts: no secret leakage, input coercion, compatibility
  baggage or silent fallback that conceals unsupported behavior.
- Specify each increment before implementing it. Keep module tests separate
  from source, add data-driven integration/live cases, and export actual
  trajectory rows before assertions. Normal execution does not write test files.
