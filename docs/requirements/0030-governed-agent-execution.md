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

The prepared loop, exact four-tool adapters, macOS sandbox, durable instance
reservation and self-definition resolution are implemented and locally verified.
Default service assembly still needs correction after the latest HTTP subprocess
failure. Generic durable child waits, Agent Runner, recursive/parallel model calls
and the complete real-model long-task matrix are not accepted. Detailed gate
evidence and limitations are retained in the implementation checkpoints below.

## Current evidence and design rationale

The current task service supports admitted graph identities, attempts, joins,
budgets, cancellation and durable approvals. A host still constructs and drives
that graph. It is not yet model-driven Agent delegation. The original in-process
directory primitives restrict files but do not isolate a shell or network; the
new exact isolated tool set replaces them in default Agent/service assembly.
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

The host reserves instance identity durably before resolving or admitting an
invocation. A host-wide journal namespace binds logical Session, Task and
invocation to one instance; identical admission retries return the same identity,
while a new child gets a new identity. The reservation uses the journal's existing
compare-and-append contract, not an in-memory counter or a distributed lease.
Conflicting owner bindings, unknown versions, corrupt facts, exhausted capacity
and failed persistence block admission. Restart and approval resume reuse the
saved reservation. Models never choose their instance identity.

Runner admission must prepare the final request after Session history projection
and Task budget restriction. It must not prepend history twice or record a
different request from the actual Provider input. Child initialization binds an
explicit selected context projection and its digest to its private Session;
reopening verifies this initialization rather than overwriting it. Dynamic child
results require their own committed invocation result proof, independent of the
Task's predeclared success criteria. A verified bounded result loader checks the
exact invocation, attempt, execution, terminal state and artifact/result digest
before returning an envelope for parent consumption.

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

The implementation must distinguish `ToolOutcome::Completed` from
`ToolOutcome::AwaitingExternal`. An external wait carries a bounded host-generated
identity, kind, schema version and exact opaque binding. This is a planned contract,
not an existing capability. Runtime validates recognized kind/version and durable
admission evidence; Core retains the binding without interpreting child semantics.

Runtime requires an explicit trusted host verifier for external-wait kind/version,
admission references and resolved-result evidence. Its default is refusal, not an
accept-all verifier. This port keeps Agent topology and FactJournal interpretation
in Server orchestration while Runtime owns exact effect/checkpoint persistence.
The same saved PreparedCall, scope and issued grant bind both pending evidence and
the final receipt; resolution does not create a new grant. The complete serialized
ToolResult remains subject to the original output ceiling, including after a long
wait. Oversized results are explicit failures, never hidden truncation.

The general checkpoint retains original call order and mutually exclusive call
states: not executed, completed, or awaiting external resolution. It also retains
prepared authority, execution/snapshot identity, original message boundary, charged
usage and absolute limits. Serial dispatch stops at the first wait and preserves
the unexecuted tail. Parallel dispatch collects the current independent stage's
short-lived start results, then suspends before later conflict stages. Completed
effects are not reprepared or rerun. Only unexecuted calls require current policy
and preparation; the historical completed receipts require their saved authority.

Checkpoint decoding requires every authority and budget field explicitly, including
optional limits: explicit null may mean no ceiling, but an absent field must not
silently become unlimited. Reject unknown critical fields and unsupported schema
versions. Persist the original input-message boundary instead of guessing it from
response counts. Resume does not reset charged calls, elapsed deadlines or maximum
Steps. A verified merge is pure: it must be persistable before any resumed effect
or model request is dispatched.

Runtime must commit exact effect-wait evidence before returning a pending outcome,
then atomically commit the checkpoint and wait set as one suspension event through
`append_unless_cancelled`. An interruption between these commits must reconstruct
the batch from the completed Step, receipts and validated wait facts, without a
second model request or child admission. Started without either receipt or valid
wait evidence remains uncertain. Ledger and Server FactJournal are distinct
storage contracts; no cross-storage transaction is claimed. Deterministic admission
keys, exact fact references and recoverable commit steps bridge their boundaries.

Server validates child completion and join evidence, records the exact final tool
receipt, consumes the child result idempotently, and commits merged resume state
before continuing. Recovery retains the same attempt and budget. A permanent
one-shot resume claim is insufficient: a crash after claiming must not make the
checkpoint permanently unresumable. Local active-execution admission prevents
concurrent driving, while durable commit progress permits restart. Acceptance must
cover every commit gap, serial tails, partial parallel completion, out-of-order
children, duplicate consumption, foreign/corrupt waits and cancellation followed
by late results. These are required implementation gates, not completed tests.

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
The mandatory `ToolExecutionScope` binds the shared `ExecutionKey` (Session,
Execution and Turn), Step and Agent snapshot identity. A missing Agent snapshot
is permitted only for explicitly non-Agent execution. The expected scope must
come from host admission, never from the grant being checked or model arguments:
a model's raw call ID is not globally unique authority.
The integrated grant must reject reuse across another invocation or Session.
Policy revision must represent the effective trusted rule set, not a constant
label surviving changes to registered manifests, static ceilings or constraints.
Dynamic workspace restrictions intersect with host restrictions; they never
replace a narrower host scope. Approval evidence binds the same exact scope.
Standalone preparation/grant primitives are not acceptance of those integrated
scope and dynamic policy guarantees.

## Prepared Turn execution contract

Turn's tool port has two mandatory operations: asynchronous `prepare(call)` and
`execute_invocation(ToolInvocation)`. The latter carries the exact preparation,
scoped grant, independently admitted scope, current policy revision and cooperative
control. There is no raw-call execution entrypoint or optional-grant fallback on
this port. Adapters may retain internal synchronous operations for trusted workers,
but these are not the Turn tool interface.

The host supplies an exact `ExecutionKey` before effectful execution; Runtime uses
its admitted key and Agent assembly additionally supplies the immutable snapshot
digest. Turn derives its Step coordinate, rejects a mismatched Turn, prepares each
call, and plans conflicts from the resulting claims. Preparation is bounded by
the Turn deadline and cancellation. Preparation failures remain explicit tool
feedback under ReturnResult and terminate under FailTurn; invalid preparation
never receives a grant or starts an executor.

An approval checkpoint retains every admitted prepared call and its execution
scope. Resume re-prepares without effects, compares all saved preparations and
checks current policy before converting verified approval into scoped authority.
Changed targets, input, implementation, scope or policy invalidate approval.
Runtime records exact preparation and scoped authorization before Started, binds
receipts to the actual adapter revision and rejects foreign invocation scope.
Historical committed output is not authority to execute another effect.

## Exact file isolation and worker authority

File processes use an explicit exact-resource sandbox mode, distinct from Shell's
conservatively admitted workspace mode. No failed exact-file admission may fall
back to workspace-wide access. The exact mode admits content only for the selected
target and one host-owned staging file, with directory traversal metadata and
protected-control denial. Staging authority is infrastructure, not an additional
model-writable directory; it stays outside the model's writable workspace.

The trusted worker consumes a host-derived file execution plan, not a newly
resolved model alias. The plan retains the physical target, pinned parent/root
identity and bounded atomic staging authority. Directory traversal and the final
file open reject symlinks; model aliases may be resolved during preparation, but
must not be followed again during execution. Snapshot/implementation/plan binding
is checked before effects. Concurrent rebinding after validation must either use
the exact pinned resource or fail explicitly, never access another path.

The sandbox supports exact-file read/write permissions and necessary metadata
only. Its public host port does not confer authorization by itself. Worker
validation and OS isolation are both required: a profile string alone is not a
proof against pathname rebinding during policy compilation. Test both ordinary
per-path denial and deterministic rebinding at the execution boundary, including
an independently running sibling writer. Cancellation does not promise rollback
after the atomic rename commit point.

PreparedCall keeps an explicit opaque `execution_binding` in its versioned input
digest. Null means an adapter has no additional plan; otherwise the value must
be a bounded object interpreted and enforced by that adapter. File plans bind
the physical workspace, every opened parent component's device/inode identity,
the target leaf and its existing identity or absence. Keep these coordinates
separate from model arguments and implementation revision. Unknown or missing
serialized binding fields do not receive a legacy default.

The worker protocol accepts one exact plan envelope, not a raw FileOperation.
It opens each directory component with nofollow, validates handle metadata, and
uses a single target leaf relative to the pinned parent. Read and Edit open the
final file with nofollow and validate its identity before consuming content.
Successful Write/Edit rename one host-owned staging file across pinned parents;
the staging root is outside the workspace, denied to sibling shells, and on the
same filesystem as the target. A rename error, including EXDEV, is a failure;
there is no copy, truncate or workspace-wide fallback.

`IsolatedToolSet` composes exactly Read, Write, Edit and Shell under the same
physical workspace. It unions control paths from both adapters and always denies
the private staging root to sibling shells. Exact file execution permits only its
selected staging leaf; this infrastructure exception does not authorize access
to other files in the staging root. Unknown or legacy environment tool names do
not fall through to another executor. The Agent runner filters advertised tool
definitions against the effective snapshot permissions before model dispatch.

The process transport has an independent host-selected input byte ceiling, capped
at 64 MiB. Empty-only input may select zero; output remains a separate mandatory
grant constraint. Do not enlarge granted output merely to carry a large Write
request. File and Shell Turn ports bound the complete returned ToolResult envelope,
not only the subprocess pipes. Shell preserves readable UTF-8 text and represents
invalid UTF-8 bytes explicitly as hex; both stdout and stderr remain observable.

Device/inode checks identify observed objects, not immutable content versions or
an eternal generation across long suspensions. Hard links, mounts and non-sandboxed
host writers remain trusted-host constraints. Atomic replacement is not a
conditional compare-and-swap against external writers; path pinning and the
OS policy must both refuse redirected addressing.

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
are implemented. Agent module tests passed 50/50, including context preparation
and Memory/SQLite invocation ownership restoration. This does
not implement model-driven delegation, durable child waits or an Agent runner.
Policy now accepts adapter-derived prepared claims and binds grants to input,
implementation and policy revisions. Policy tests passed 24/24; a separate Core
regression verifies that changed rules invalidate old approval even when the
decision still requires approval. Revision hashes cover effective rules, and
prepared JSON canonicalization sorts nested objects without reordering arrays.
Approval evidence is typed and exact; its durable authority still
has to be verified by the invoking host. Turn dispatch now requires asynchronous
preparation and scoped execution; cross-module recovery acceptance is pending.

File operations implement strict typed read/write/edit, bounded content,
atomic replacement and stale/ambiguous edit rejection. Tools module tests passed
33/33, preserving nine original tests. This includes isolated shell argument,
grant, actual process output and refusal tests. The trusted helper has three passing
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

The new `file_worker_process` integration target passed four tests: eight fixture
operations and refusal of changed arguments, policy revision and worker binary
before file effects, canonical symlink resource authorization/rebinding, and
cross-scope refusal for both ordinary and confirmed-approval authority.
It compares explicit failure semantics and actual read
content after exporting JSONL. Cargo builds the exact production worker entrypoint
for this test target; no manual pre-build or duplicate worker source is required.
Final observed process artifacts at this checkpoint are:

- `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-file-worker-8z0pst/actual.jsonl`
- `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-file-bindings-S5hma0/actual.jsonl`
- Sandbox output: `/tmp/kolyan-l5-sandbox-main.log`.
- Scoped file/approval fixture: `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-file-execution-scopes-c3b6kF/actual.jsonl`.
- Scoped shell refusal: `/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-shell-execution-scope-xxZbyk/actual.jsonl`.

The scope integration fixture declares authority modes, exact inputs, mutations
and semantic expectations independently of the runner. Its final report has
20 rows and 20 unique fixture identities; every refusal precedes any effect.
The post-extraction process target passed 4/4. The current default workspace
regression and strict Clippy passed (`/tmp/kolyan-l5-scope-workspace-tests.log`
and `/tmp/kolyan-l5-scope-workspace-clippy-v2.log`). Explicitly ignored network
tests were not executed and are not included in actual-model acceptance.

The default workspace test run passed at its intermediate checkpoint, with
explicitly ignored network tests still unexecuted. A failed sandbox host-control
probe and a shell bootstrap permission failure were retained in diagnostic logs;
bounded host acceptance and the narrow root-owned shell selector read rule fixed
them without removing the security assertions. That historical run used sandbox
policy version v2.

These are real local subprocess tests, not actual-model Agent acceptance. Strict
model-specific token counting, context reduction, loop integration, recursive/parallel Agent
execution, durable child waits and the full real-model/long-task matrix remain
in progress. L5 is not accepted.

Every Provider opening can now be composed with an Agent-side preparation guard.
It records exact source/preparation before dispatch and blocks preparation or
recorder failures. It rejects changed requests at this boundary so the Core
trajectory cannot differ from actual dispatch. Initial request projection belongs
before Turn admission; Strict mode never falls back to diagnostic estimates.
Ten new wrapper tests verify repeated requests, refusal, stream/error/drop and
usage passthrough. Runner assembly and real-model acceptance remain pending.

File preparation now declares Create and Update for Write regardless of current
existence, and Read plus Update with both filesystem capabilities for Edit.
Integrated permission acceptance still requires file isolation to enforce the
admitted resource despite concurrent path rebinding after preparation. Current workspace
sandboxing and pre-execution re-preparation are not a proof of atomic per-path
mediation against an uncooperative sibling writer. Keep this limitation explicit;
do not describe the standalone adapters as the completed permission pipeline.

### Prepared loop integration checkpoint

Core tests passed 53/53 in `/tmp/kolyan-l5-prepared-core-tests-v3.log`.
New regressions cover missing/foreign execution ownership, preparation failures
without execution-start events, changed implementation/resource/Session/invocation
or snapshot at approval resume, and retained tool timeout during re-preparation.
Cancellation uses broadcast notification; 64 independently registered consumers
wake, and dropping one approval waiter does not erase another waiter's marker.
These controls remain ephemeral; durable cancellation and authority belong to
the admitted execution facts and checkpoints.

Runtime prepared dispatch tests passed 33/33 at their migration checkpoint.
The subsequent process-fault gate found generic effect recovery overwriting the
richer prepared-input fact. Shared historical receipt binding now fixes that
production defect, and the current Runtime gate passed 39/39
(`/tmp/kolyan-l5-exact-runtime-final.log`). Reconciliation validates saved authority
without issuing fresh grants; missing or corrupt authority fails closed. This
module result does not establish full workspace or Agent acceptance.

The explicit exact-file Seatbelt mode passed 24/24 sandbox module tests on
macOS 26.6.2, including child probes; policy revision is now v3. This mode rejects
Shell requests and grants literal content paths independently of workspace access.
Worker nofollow/identity pinning and assembly are still pending, so these tests do
not establish complete per-resource mediation. No actual-model Agent or long-task
matrix has been run for this integration checkpoint. L5 remains in progress.

### Exact resource execution checkpoint

Policy tests passed 27/27 (`/tmp/kolyan-l5-exact-binding-policy.log`). Mandatory
execution bindings are covered by prepared digest schema 2; missing, malformed or
tampered bindings are rejected. Sandbox tests passed 27/27 under revision v4
(`/tmp/kolyan-l5-sandbox-input-limits.log`), including independent input/output byte
limits. These supersede the respective module counts above, not full acceptance.

The exact trusted worker now opens directory chains without following symlinks,
checks saved physical identities, and uses a pinned external staging directory.
The new real sibling-shell boundary target passed its twelve data rows for
read/write/edit, normal execution, changed parent, changed leaf and final symlink.
The first passing report is
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-exact-file-boundary-85dIsn/actual.jsonl`
with exactly twelve rows (`/tmp/kolyan-l5-exact-file-boundary-v3.log`). The framework
exports actual request bindings, mutation subprocess output, worker bytes and file
state before comparison. It is a subprocess boundary test, not model acceptance.

The initial Tools run exposed a real missing private-permission setting when
creating staging directories: 56 passed, eight failed. Production creation and
fixtures now request 0700 at directory creation, not through a later chmod.
The post-correction Tools run passed 64/65: exact-file and isolated-file tests
passed, while a Shell overflow cleanup race returned EPERM instead of OutputLimit.
The cleanup fix now verifies the unreaped leader and every process-group member
before accepting Darwin's zombie-only EPERM; real denial and inspection failures
remain errors. Sandbox passed 31/31, including 64 short-process overflow iterations
(`/tmp/kolyan-l5-sandbox-cleanup-v5c.log`). Shell Turn passed 8/8 with the exact
OutputLimit assertion retained (`/tmp/kolyan-l5-shell-cleanup-v5.log`). No argument
or output truncation was introduced to pass these gates.

The full file-worker process target subsequently passed 6/6
(`/tmp/kolyan-file-worker-process-exact-v2.log`). Its data-driven reports preserve
8 operation rows, 20 scope rows, 7 permission rows and 6 worker-binding rows.
Path escape is explicitly refused during preparation, before any grant or worker
execution; the original refusal scenario was retained. The strengthened sibling
boundary comparison also passed, validating decoded result path/bytes/read content
and empty success stderr before accepting the twelve rows
(`/tmp/kolyan-l5-exact-file-boundary-v4.log`). Strict all-target Clippy passed at
the pre-cleanup checkpoint; the full workspace run failed at the then-unmigrated
escape fixture and must be rerun after both fixes. No failed gate is counted as
full acceptance.
Agent Runner, generic external waits, recursion/parallel invocation and the
actual-model long-task matrix remain unimplemented or unverified. L5 is not accepted.

The current complete Tools gate passed 65/65
(`/tmp/kolyan-l5-exact-tools-final.log`). The subsequent workspace rerun exposed
old hand-constructed recovery facts without the now-required PreparedCall and
scoped authority. The task-recovery integration fixture was migrated without
changing its uncertain-effect or no-replay assertions and passed
(`/tmp/kolyan-task-recovery-prepared-v3.log`). The three Server retry unit fixtures
were migrated to generate actual prepared crash-window facts through Runtime,
preserving receipt/no-replay assertions. Server passed 67/67 in the later workspace
run, which then failed the HTTP service assembly described below. Strict production
decoding is retained, not relaxed to accept incomplete historical fixtures.

### Durable identity and self resolution checkpoint

Host `InstanceRegistry` implements bounded permanent reservations using the
existing FactJournal compare-and-append contract. Its ten module tests passed
(`/tmp/kolyan-l5-instance-registry-tests.log`), including independent SQLite
connections, duplicate concurrent admission, restart, exact original fact replay,
capacity, corruption and unknown-schema refusal. A trusted stable host namespace
and journal are mandatory. These reservations are not leases, grants or executed
invocations, and Runner assembly has not yet consumed them.

`resolve_self` uses the saved authenticated parent definition directly, without
consulting a mutable catalog. Host, parent and original definition permissions
intersect; requests cannot expand them, and self invocation needs a distinct
host-issued instance. The complete Agent module passed 57/57
(`/tmp/kolyan-l5-self-resolution-main.log`). This proves self resolution and static
admission, not recursive model execution. Storage checkpoint regression passed
5/5 (`/tmp/kolyan-l5-prepared-storage.log`). Actual recursive/parallel Agent and
long-task acceptance still requires generic external waiting and Runner assembly.

### Service assembly correction

The full post-migration workspace run passed the library and recovery fixtures,
then failed the real HTTP subprocess approval scenario
(`/tmp/kolyan-l5-prepared-workspace-final.log`). Its actual Ledger records
`invocation exceeds the tool capability ceiling`: the service still assembled the
old two-tool adapter and declared Write Update without its required Create effect.
This is a production assembly defect, not a model failure. Approval assertions
remain unchanged. The actual failing evidence is under
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-http-offline-MJQBU1/`.

Both HTTP and stdio must share `IsolatedToolSet` assembly, with four exact schemas
and complete trusted manifests. Write declares Create/Update; Edit Read/Update.
Policy resource scopes are physical canonical paths derived from the configured
workspace-relative tool scope, not relative strings compared against absolute
prepared resources. Invalid or escaping scopes are refused. Shell's workspace
claim must not bypass a narrower configured subdirectory ceiling; advertising a
tool is not permission to expand filesystem access.

The host configuration explicitly supplies `worker_path` and `staging_root`.
`allow_shell` is an additional host ceiling and fails closed when omitted. The
base inventory remains four tools; effective request schemas omit a statically
denied Shell, while actual invocation still checks the ceiling. Enabling Shell
does not widen a narrower workspace scope or remove approval.
The service package builds the exact shared production worker entrypoint; there
is no guessed executable path or in-process fallback. Ledger and Session state
live in a protected state directory outside, and not containing, the workspace.
Protecting that directory covers SQLite sidecars and future state files. Staging
is independently protected, outside the workspace, and on the target filesystem;
its directories are created with explicit private permissions. The loaded config
file itself is protected without broadly denying its parent directory. Unsafe
overlapping layouts fail before serving requests. Existing process fixtures must
migrate only host layout/configuration, preserving their requests, expected
approval suspension, restart, cancellation, receipts and result comparisons.

Service assembly is corrected. Its seven admission/schema tests passed
(`/tmp/kolyan-l5-server-advertising-tests.log`); the existing HTTP process target
passed five with two explicitly ignored network tests, and stdio passed five with
one explicitly ignored network test
(`/tmp/kolyan-service-isolated-process-fixtures-v2.log`). Those scenarios retain
approval, restart, cancellation, exact receipts and parsed file-result/context
comparisons. A new data-driven four-tool end-to-end target is being added; final
workspace revalidation is pending. No actual-model Agent or long-task result is
inferred from these local subprocess fixtures.

### Boundary admission is not a committed suspension

The new four-tool HTTP dataset detected two `execution_suspended` facts per
approval: boundary admission and the subsequently saved approval checkpoint.
The failing export remains under `kolyan-http-four-tools-OjG8tq`. This is a
Runtime fact-classification defect, not justification to double the expected
suspension count. Before a checkpoint exists, consumers cannot safely resume.

Cancellation-checked entry into the approval boundary now records
`execution_boundary_admitted`. Only the durable checkpoint publication records
`execution_suspended`; it must follow the persisted approval request. Reducers
must not infer saved waiting state from admission. The same distinction applies
to future generic external waits. A dedicated Runtime regression checks that
boundary admission alone publishes neither a suspension nor an approval snapshot.
