# Agent Evolution Design and Acceptance Program

## Status and purpose

Authorized for phased design and implementation on 2026-10-02. This program
turns the [phase review](../architecture/kernel-evolution.md) into bounded
development increments. Baseline: main `3043095`. The baseline is committed,
but its remaining actual-model acceptance failures are not resolved.

The outcome is a usable personal Agent that verifies real task completion,
preserves authority and evidence across long waits, manages bounded context,
and integrates external capabilities without another execution loop.
This program does not promise universal autonomous planning or exactly-once
external effects. Voice, GUI, A2A and model training remain outside this scope.

## Requirements and ownership

The phase review owns the assessment. Requirement 0030 remains the sole
authority for its retained actual-model cases and run evidence. The user's
2026-10-02 clarification makes self-iteration an experimental feedback activity
after functional development, not a prerequisite for mainline delivery or an
obligation to immediately produce an accepted code change.
Functional implementation remains the primary delivery stream. MiniMax is the
model used to run Kolyan for post-feature feedback, not a separate framework to
develop. The coordinator diagnoses that feedback and implements verified fixes;
Kolyan does not have to author those fixes for an increment to be accepted.
This program owns stage dependencies and completion rules. Each increment owns
its precise contracts, implementation decisions, cases and validation receipts
in a separate numbered requirement, linked here before implementation.

Do not duplicate changing run status in the review. Do not replace retained
failed runs with a successful rerun. Record which code revision and configuration
each result actually validates.

## Stable architecture

Preserve Session, Turn and Step as conversation units. Agent coordinates goals,
definitions and delegation; Server owns long-lived task and execution ownership;
Runtime executes bounded durable attempts. Storage is an adapter, not a loop
dependency. Prepared calls, dynamic decisions and enforcing executors remain
separate. Ledger facts are authoritative; Trajectory preserves interaction
content; Trace is diagnostic. Derived context and memory cannot rewrite facts.

No topology edge, tool description, Skill body, memory record, plan or model
verdict grants permission. All execution passes the existing authority boundary.
Single-process scheduling adds no distributed lease to Runtime.

## Development stages

| Stage | Required outcome | Dependency and scope |
| --- | --- | --- |
| E0 Current acceptance | Close the functional 0030 gaps: complex delegation/approval and long-task finalization | Retain original cases and failures; self-iteration supplies experimental feedback, not a delivery prerequisite |
| E1 Execution correctness | Standalone Step liveness; typed preparation errors; explicit goal evidence and bounded correction | Step and error classification may develop independently of E0; business-goal contracts precede host wiring |
| E2 Context and resources | Authenticated model-aware accounting, governed reduction, large-output references and managed background execution | Preserve provenance and tool pairing; no diagnostic estimator promoted to trusted count |
| E3 Governed capabilities | Selective Skills loading, working MCP lifecycle, concrete hooks, scoped revisioned memory | Each first implementation must exercise a real execution consumer, not just expose a trait |
| E4 Coordination | External wakeups, mailboxes/steering and concurrent budget reservations | Above Runtime; admission, cancellation and reservation recovery have durable contracts |
| E5 Product and evaluation | Production Agent service/CLI assembly and repeatable independent evaluations | Reuse the runner; integrate completed contracts; no copied loop or permissive fallback |

Stage labels describe scope, not a demand to serialize all work. Disjoint
increments can run in parallel once their contracts and write sets are settled.
Cross-stage integration waits for the dependencies it actually needs.

## First parallel increment

The first mainline is goal verification and a usable Agent host, together with
context engineering. Small Step/error corrections are supporting slices, not
a replacement for the phase review's larger capability gaps. Existing 0030
functional failures remain acceptance dependencies, but do not prevent independent design
and development of these larger capabilities.

The coordinator owns this program, requirement 0030 evidence, the goal/host
specification, integration and final acceptance. Assigned subagents own
disjoint source/test slices:

1. Step liveness: establish cancellation/deadline behavior while opening a
   Provider stream and while an accepted stream is pending. Reproduce before
   changing behavior; retain Turn's existing independent deadline protection.
2. Preparation error classification: distinguish malformed input, unsupported
   preparation and authorization denial using typed causes. Preserve strict
   argument parsing and the original scope/grant checks. No type coercion.
3. Context design: inspect actual provider mappings and available counters;
   define what can be trusted for each supported protocol/model. Specify the
   first real implementation before selecting a tokenizer dependency or reducer.

Reserve 0032 for Step liveness, 0033 for preparation errors and 0034 for context.
These numbers are reservations until their specifications exist. The coordinator
reviews each contract before releasing its implementation. Broader goals,
resource tools, extensions, memory and coordination receive bounded specifications
as their prerequisites are established, not undocumented placeholder APIs.
Reserve 0035 for goal verification and production host assembly; its design and
implementation form the mainline rather than waiting for every minor finding.

Current mainline specifications:

- [Context accounting and governed reduction](0034-context-engineering.md).
- [Goal verification and usable Agent host](0035-goal-verification-and-agent-host.md).
- [Governed selective Skills loading](0036-governed-skills.md).
- [Governed MCP integration](0037-governed-mcp.md).
- [Governed native hooks](0039-governed-hooks.md).
- [Bounded goal correction and durable execution budgets](0038-bounded-goal-correction.md).
- [Governed Shell output artifacts and targeted reads](0040-governed-shell-output-artifacts.md).

Supporting specifications:

- [Standalone Step liveness](0032-step-liveness-boundary.md).
- [Typed Agent preparation errors](0033-agent-preparation-error-contract.md).

Their reviewed source and complete test observations have been integrated;
independent mainline verification is running. Isolated worktree receipts are
not substitutes for integrated or actual-model acceptance.

## Goal and host design constraints

ExecutionCompleted is physical execution evidence, not natural-language goal
satisfaction. Goal verification must consume explicit immutable evidence with
identity, provenance and verdict reasons. Deterministic postconditions should
be preferred where available; a model verifier must expose uncertainty and
cannot manufacture a durable tool receipt or authorize correction.

Bounded correction creates a new admitted attempt/continuation with explicit
limits and retained previous evidence. It does not rewrite a failed attempt or
retry an uncertain effect. Define cancellation, approval, missing evidence and
checker failure before integrating the production host.

The delivered host must select versioned Agent definitions, compose real
provider/tool adapters, and use the existing Task/Session/Runtime chain.
Interactive clients communicate through Server; a one-shot CLI may execute
locally without starting a service. Production assembly must not import tests
or silently choose permissive policies, fake counters or scripted responses.

## Required acceptance coverage

Every increment defines data-driven cases before claiming completion. Cases
carry stable IDs, setup, inputs, expected invariants and the evidence compared.
The framework executes cases, exports all actual rows before assertions, and
compares deterministic evidence separately from unconstrained model wording.
Temporary traces are test artifacts, not normal Step/Turn filesystem behavior.

The evolving integrated suite must cover:

- Normal, malformed, denied, unsupported and cancelled paths; no hidden skip
  of ordinary Provider or preparation failures.
- One and multiple Steps/Turns, named and inline Agents, recursion and joins.
- Long approval suspension followed by host reconstruction and exact resume;
  no effect before authorization and no duplicate confirmed effect afterward.
- Goal satisfied, final refusal, unmet postcondition, missing evidence,
  verifier error, bounded correction and exhausted limits.
- Context accounting/reduction boundaries, immutable source provenance,
  instruction anchors, tool pairing, unknown counts and reducer failures.
- Output artifacts and background work: ownership, bounds, continuation,
  cancellation, restart, late result and uncertain external effect.
- Skills/MCP/hooks: real discovery/loading/execution, revocation, changed
  revision, untrusted text, unavailable transport, cancellation and hook failure.
- Memory: selective recall, scope isolation, revision, conflict and forgetting;
  recalled instructions never replace current authority.
- Coordination: duplicate wakeups, mailbox order, cancellation races,
  concurrent budget ceilings, reservations and recovery without overspending.

After functional increments, run bounded self-iteration experiments in isolated
worktrees to collect actual-model feedback. Retain unsuccessful runs as findings;
an experiment may finish without a usable modification. No repeated correction
loop is required for mainline acceptance. If a candidate is proposed for merging,
require actual model-authored evidence, independent review, relevant regression
and an explicit merge decision; experimental status never relaxes these checks.

Offline tests prove deterministic invariants. Live tests use the authorized
MiniMax endpoints through both supported protocols and retain actual requests,
events, tool results, receipts and task postconditions. Keys remain environment
only; local private configuration and raw logs are not committed. Capability
tables still decide protocol fields; tests do not force unsupported parameters.

The live matrix grows as capabilities integrate. A row must state what it
actually exercises; a successful answer without the required interaction does
not count. Report rows, test functions, ignored cases and failures separately.

## Parallel integration and completion rules

Use small batches with explicit file ownership. Subagents do not commit,
push, alter each other's files or run uncontrolled duplicate live matrices.
The coordinator freezes revisions for full verification and serializes shared
contract changes. No source mutations while a baseline gate is being relied on.

For each increment: specification, implementation review, module tests, added
integration cases, relevant real scenarios, full regression, evidence receipt,
then normal-hook commit. Never weaken existing cases to make a gate pass.
Intermediate implementation is not acceptance. Failed scenarios stay open
until diagnosis and corresponding evidence establish the fix.

The program is complete only when the integrated host demonstrates the scoped
outcome, all required increments have verifiable acceptance receipts, and E0's
functional requirements are satisfied. Experimental self-iteration success is
not a completion condition. The document, parallel dispatch, offline
pass or a single successful model run does not constitute completion.

## Build resource discipline

The user additionally requires timely control of Rust build latency and disk
growth. Follow the single-source [build cache rules](../architecture/code-conventions.md#编译时间与缓存占用):
reuse bounded caches, serialize gates sharing a target, measure disk use, and
clean only confirmed inactive build artifacts. Keep source and experiment proof
outside disposable caches. This is part of the development workflow, not a
replacement for any E0–E5 implementation or actual-model acceptance requirement.

## Integrated regression and MiniMax checkpoint

On 2026-10-03, the joint integration tree passed the full offline workspace:
937 tests passed, zero failed and 69 were ignored. All-target warning-denied
Clippy, formatting and diff checks also passed. Ignored tests were not silently
counted as acceptance. Evidence is retained in
`/tmp/kolyan-opening-main-workspace-tests-v3.log` and
`/tmp/kolyan-opening-main-strict-v7.log`; failed earlier receipts remain intact.

The separately authorized MiniMax-M3 live gate then passed eight scenario rows:
OpenAI-compatible and Anthropic-compatible protocols, each with named and inline
roots, for ordinary execution and approval reconstruction. Each approval row
executed two independent Turns and four actual suspension/Host-rebuild/resume
cycles. The four physical approval traces were independently reread against the
ordered dataset and exact actual-request/Ledger pairing. Each scenario had one
attempt; no scenario retry replaced a failure. The process completed with two
test functions passed and zero failures in 203.05 seconds.
The live log is `/tmp/kolyan-opening-main-minimax-root-restart-v1.log`.

This checkpoint verifies the existing Agent Runner, SDK providers and native
tool/approval reconstruction paths in the acceptance harness. It does not prove
the pending CLI product, all configured model deployments, complete MCP/Hook
host integration, output publication/ACL, memory or E4 coordination increments.
Nor is it an experimental self-edit result. Those requirements retain their
own implementation and real-consumer gates; the full program remains open.
