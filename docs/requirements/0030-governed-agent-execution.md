# Governed Agent Execution

## Scope and acceptance status

This specification defines L5 after the accepted durable task foundation in
[0029](0029-durable-task-foundation.md). Implementation is authorized by the
user. Named registered and anonymous inline definitions are both required,
as confirmed on 2026-10-01. No capability below is accepted merely because a
type, relationship or test directory exists. This document owns the new scope;
0029 retains its historical acceptance evidence.

Deliver a reusable Agent abstraction, single Agent execution within a Session,
self-recursion, delegation and parallel joins, durable long tasks, four basic
environment tools, permissions and default macOS execution isolation. Preserve
the Step, Turn, Runtime and Server boundaries. Do not add distributed leases,
legacy adapters, a general workflow language or additional environment tools.

## Current evidence and design rationale

The current task service supports admitted graph identities, attempts, joins,
budgets, cancellation and durable approvals. A host still constructs and drives
that graph. It is not yet model-driven Agent delegation. Existing directory
capabilities restrict file access but do not isolate a shell process or network.
The current final response artifact is not proof of an arbitrary workspace file.

The inspected Garive preparation and governed execution ports distinguish
validated intent, authorization and committed results. Its invocation grant
binds a prepared digest and tool revision. The inspected Codex Seatbelt adapter
uses the fixed `/usr/bin/sandbox-exec` executable and separate filesystem and
network policies. DSH fleet and coordination journals motivate durable observed
coordinates and result consumption rather than reliance on notifications.
These source observations support the proposed boundaries, not comparative
performance or correctness claims. Sources are the sibling repositories:

- Garive `engine/core/src/governed_execution.rs` and
  `engine/tools/src/governed_types.rs`.
- Codex `codex-rs/sandboxing/src/seatbelt.rs` and its `.sbpl` policies.
- DSH `crates/tui/src/fleet/ledger.rs` and
  `crates/tui/src/tools/subagent/coord/ledger.rs`, as inspected for 0029.

The design applies least privilege, fail-safe defaults and complete mediation
as described by [Saltzer and Schroeder](https://web.mit.edu/Saltzer/www/publications/protection/).
Durable waits and effect receipts address distinct crash windows; they do not
promise exactly-once arbitrary shell side effects. Context reduction is a
derived projection, never a rewrite of source facts or authorization.

## Ownership and module boundaries

| Owner | Responsibility | Must not own |
| --- | --- | --- |
| Agent library | Definition resolution, effective snapshots, invocation composition and result envelopes | HTTP transport, SDK protocol mapping, OS sandbox internals |
| Server coordinator | Durable graph admission, private invocation context bindings, scheduling and join decisions | Model reasoning loop or implicit permission inheritance |
| Runtime | Bounded attempts, receipts, persisted suspension and recovery | Agent naming/catalog or topology authority |
| Turn and Step | Model loop, tool feedback and portable suspension contracts | Database, child scheduler, Session store or OS policy |
| Policy | Prepared claims, static ceilings, dynamic decisions and exact grants | Parsing arbitrary tool argument schemas or executing tools |
| Tools | Argument validation, preparation and four environment operations | Grant creation or Agent scheduling |
| Sandbox adapter | Enforced process/filesystem/network boundaries and process cleanup | Approving an invocation |

Introduce `crates/kolyan-agent` for reusable Agent composition and
`crates/kolyan-sandbox` for execution isolation. A thin trusted tool worker may
execute file operations under the same OS boundary; it is not a client app.
The worker lives in `crates/kolyan-tool-worker` as a one-shot adapter executable.
The Agent client package is `kolyan-agent-app`; its executable remains
`kolyan-agent`, distinct from the reusable library package.
The existing Server `AgentIdentity` remains the single identity contract until
an explicit shared-type migration is needed. Do not introduce duplicate identity
structs merely to avoid a dependency. Server must not depend on Agent if Agent
depends on Server; client/service assembly composes both.

## Agent definitions and identity

An immutable `AgentDefinition` contains a definition ID, revision, optional
display name, model reference, instructions, environment tool ceiling and
delegation ceiling. Resolution produces a digest-bound `AgentSnapshot`.
Registration of identical content is idempotent; the same ID and revision with
different content fails. Named selectors bind an exact revision, not a mutable
latest alias. Inline selectors carry a complete definition and obtain the same
validated snapshot contract; they are not an authority bypass.

Definition ID, revision, instance ID, invocation ID, attempt ID and Session ID
are distinct. An anonymous Agent lacks a display name, not durable identity.
Every self-call creates a new invocation and instance even when its definition
is unchanged. Changed definitions invalidate pending authority instead of
silently re-resolving a suspended invocation to new instructions.

Effective permission is the intersection of host allowance, Agent ceiling,
delegation ceiling and dynamic policy. A child cannot gain permission from its
name, instructions, parent pointer or an inline definition. Tool and delegation
ceilings are independent. A registered target outside the parent's delegation
scope is denied before child admission or any model call.

## Session contexts and Agent invocation

One logical user Session owns the task and root conversation. Each child has a
private persisted invocation context associated with that logical Session.
Concurrent children must not append into the same physical pending Session Turn.
The binding records logical Session, private context, invocation and snapshot.
Only an explicit selected context projection is passed into a child. Child
system instructions and unrelated history do not leak back into the parent.

`agent.invoke` is a reserved orchestration operation, not a fifth environment
tool. Its intent identifies a named or inline target, task input, selected context
and requested permission ceiling. The host validates it before creating facts.
Self-recursion uses the exact initiating snapshot. Independent children may run
concurrently under a bounded coordinator limit; dependency edges force order.
The parent receives a bounded result envelope with invocation identity, terminal
outcome, artifact references and durable evidence coordinates. Child completion
does not implicitly commit parent consumption or task success.

## Durable child waiting and recovery

Waiting for a child, an approval or external confirmation must release the
worker. Do not keep a suspended parent Rust future or tool stack alive, emit a
fake successful pending result, or poll children through repeated model calls.

Generalize the loop checkpoint to retain the admitted request, assistant tool
batch, completed results, pending calls, next Step and remaining limits. Keep
approval authority separate from loop state. A typed external wait carries an
opaque exact binding; Core does not interpret Agent or journal semantics.
Runtime commits the checkpoint and wait fact before returning suspension.
Server drives children and verifies committed results before resuming the parent.

Resume reconstructs the original snapshot and context, consumes results exactly
once, preserves tool-call/result pairing, and does not repeat the pre-wait model
request or child admission. Multiple children require explicit join evidence.
An explicit waiting fact differs from Started without a receipt: the latter
remains uncertain and requires trusted reconciliation. Unknown critical wait
semantics fail closed. Cancellation follows the admitted task policy and cannot
revive a cancelled root through a late child result.

## Tool preparation and permission enforcement

The environment inventory is `file.read`, `file.write`, `file.edit` and `shell`.
Trusted adapters validate arguments and derive canonical resources, effects,
implementation revision and mandatory isolation requirements. A prepared digest
binds exact arguments and requirements. Policy evaluates this prepared call with
dynamic execution context; it does not infer generic tool semantics from names.

Grants bind call identity, prepared digest, tool revision, policy revision and
constraints. The executor validates the complete binding before effects, including
on resume. Missing grants, changed inputs, revisions or unavailable mandatory
isolation are errors. Approval cannot expand static capability ceilings.

Read is bounded. Write uses an atomic replacement where supported. Edit performs
an exact unique replacement with optional expected content digest; missing,
ambiguous or stale content fails without partial modification. Do not claim
atomic compare-and-swap against arbitrary external writers. Shell runs a nonlogin
shell in an admitted working directory with bounded output and timeout. Arbitrary
shell commands are conservatively effectful; command string parsing is not a
security boundary. Receipt uncertainty never authorizes automatic shell rerun.

## Default macOS isolation

Use a replaceable execution adapter with a real Seatbelt implementation and
fixed `/usr/bin/sandbox-exec`. Parameterize trusted canonical paths rather than
interpolating model strings into policy syntax. Deny by default, deny network
including loopback, restrict reads and writes to granted roots plus explicitly
required platform loader resources, and protect control/ledger/credential paths.
Do not grant all filesystem reads for convenience. Clear inherited environment;
provider keys, user shell startup files and proxy settings never enter tools.

Cancellation, timeout, output overflow and dropped execution futures must stop
and reap the process group, including descendants. File helpers receive bounded
typed input and run inside isolation too. Directory capabilities are additional
defense, not a substitute for the claimed process sandbox. Unsupported platforms
or a missing backend fail explicitly; no unsandboxed fallback is allowed.

The installed macOS `sandbox-exec(1)` manual marks the utility deprecated.
This adapter is not Apple's signed application entitlement model. Backend
availability and enforcement require actual OS tests and remain replaceable.
Apple's [App Sandbox entitlement documentation](https://developer.apple.com/library/archive/documentation/Miscellaneous/Reference/EntitlementKeyReference/Chapters/EnablingAppSandbox.html)
describes entitlements incorporated into a target's code signature; it is not
the dynamic subprocess policy configuration used by this adapter.
Host-selected roots must not contain secrets or uncontrolled hard links/mounts;
document those limitations rather than claiming full hostile-host isolation.

## Long task execution

Keep each Turn bounded. Persist objective, completion evidence, invocation state,
waiting reason, cumulative usage and the decision to continue. Continuation is
not retry, approval resume or effect recovery. Enforce depth, invocation, attempt,
Step and shared usage ceilings. Observed token accounting is not a prepriced
reservation; unknown usage cannot satisfy a strict token ceiling.

Prepare context before both initial and subsequent model Steps using model
limits, output reserve and a versioned policy. Preserve governing instructions
and tool pairing. Reduction records provenance and retains full source
trajectory. Overflow and reduction failure are explicit outcomes. Completion
requires trusted execution or artifact evidence, not the model's declaration.

## Acceptance matrix

| Gate | Required cases and evidence |
| --- | --- |
| Definitions | Named and inline parity, exact revision, conflicting registration, invalid identities, distinct instances and denied delegation |
| Single Agent | All four tools, multiple Turns in one logical Session, exact context retention and no cross-Session leakage |
| Recursion | Model-generated bounded self-call, new identities, recursion limit, no permission expansion and restart before result consumption |
| Multiple Agents | Named and inline children, genuine bounded parallel fan-out, dependency ordering, durable join and duplicate result rejection |
| Approvals | Child approval releases workers, rebuild services, unchanged resume, changed definition/tool/grant rejection and multiple waiting children |
| Isolation | Real macOS outside-root read/write denial, symlink escape, network/loopback denial, secret environment absence, missing backend and descendant cleanup |
| Effects | Read bounds, atomic write/edit failure, stale/ambiguous edit, shell timeout/output cap and uncertain effect without blind replay |
| Long tasks | At least ten admitted Turns and twenty model Steps driven by fixture inputs, restart at waiting and continuation boundaries, context reduction and evidence-backed completion |
| Failures | Cancellation propagation, cancelled parent late child, budget exhaustion, missing/corrupt evidence and interrupted result commit |

Unit tests stay in module-owned separate files. New integration cases and semantic
expectations are data, independent of the execution framework. Preserve existing
scenarios and assertions. Test code exports actual requests, reasoning/text,
tool observations, waits, grants, facts and linked JSONL to temporary storage
before comparisons, including failure paths. Production loop code does not write
test files. Compare identities, pairing, outcomes and causal semantics, not exact
unpredictable prose. Long-task counts are asserted against actual recorded Steps,
not a host loop that invents model actions.

Every configured Provider/protocol/model combination runs the applicable real
Agent and long-task cases. Missing credentials and ordinary Provider errors are
failures, not silent skips. Deterministic providers prove crash windows but never
count as real-model evidence. Report all planned rows, failures and exclusions.

## Implementation sequence

1. Freeze this scope and implement definition resolution, sandbox execution and
   prepared four-tool operations in disjoint parallel work sets.
2. Wire preparation, dynamic policy and exact executor grant enforcement.
3. Generalize durable waits and implement single Agent, recursive delegation,
   private invocation contexts and bounded parallel joins using the task service.
4. Add context preparation and long-task continuation; complete data-driven and
   actual-model matrices, then rerun existing workspace and HTTP regressions.

Main integration owns Core/Runtime suspension and cross-module contracts.
Parallel workers own Agent definitions, sandbox adapter and prepared file
operations respectively. No worker claims full acceptance from module tests.
Implementation status and executed evidence will be appended here as verified;
the complete scope remains active until every gate above is proven.

## Implementation checkpoint on 2026 10 01

Definition/catalog resolution, permission intersection and immutable snapshots
are implemented. Agent module tests passed 13/13 at this checkpoint. This does
not implement model-driven delegation, durable child waits or an Agent runner.
Policy now accepts adapter-derived prepared claims and binds grants to input,
implementation and policy revisions. Five new tests passed with ten existing
policy tests. Approval evidence is typed and exact; its durable authority still
has to be verified by the invoking host. The existing Turn dispatch has not yet
been migrated to these preparation and grant contracts.

File operations implement strict typed read/write/edit, bounded content,
atomic replacement and stale/ambiguous edit rejection. Tools module tests passed
23/23, preserving nine original tests. The trusted helper has three passing
protocol tests. `IsolatedFileTools` reprepares the call, hashes the actual trusted
worker binary, checks the exact grant and launches the worker through Seatbelt.
No ambient fallback is present. Raw file operations remain explicitly separate
from authorization and isolation, for use inside the helper.

The macOS adapter has a replaceable executor port and real OS enforcement.
Its 17 module tests passed, including child probes used by the parent tests.
Executed gates cover outside-root and symlink reads/writes, loopback/external
network denial, clean environment, read-only roots, protected controls, bounded
output, timeout, cancellation, future drop, runtime shutdown and detached-session
escape denial. This evidence applies to the tested host, not every macOS release.
The adapter denies process-group/session detachment and cleans up normal child
groups; it is not a universal guarantee against an adversarial operating system.

The new `file_worker_process` integration target passed two tests: eight fixture
operations and refusal of changed arguments, policy revision and worker binary
before file effects. It compares explicit failure semantics and actual read
content after exporting JSONL. Cargo builds the exact production worker entrypoint
for this test target; no manual pre-build or duplicate worker source is required.
Final observed process artifacts at this checkpoint are:

- `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-file-worker-KfloKr/actual.jsonl`
- `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-file-bindings-GZY5O8/actual.jsonl`
- Sandbox output: `/tmp/kolyan-l5-sandbox-main.log`.

These are real local subprocess tests, not actual-model Agent acceptance. Shell
preparation, context preparation, loop integration, recursive/parallel Agent
execution, durable child waits and the full real-model/long-task matrix remain
in progress. L5 is not accepted.
