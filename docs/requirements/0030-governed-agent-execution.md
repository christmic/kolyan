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
Default service assembly and the local HTTP four-tool matrix have been corrected
and verified. Generic durable child waits, Agent Runner, recursive/parallel model calls
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

`SessionStore::initialize(session_id, SessionInitialization)` atomically persists
the complete selected message projection and an immutable host binding digest.
The digest is computed and authenticated by Server from logical Session, Task,
invocation, snapshot and projection identities. Storage treats it as an opaque
comparison key, not authorization, and does not duplicate Agent definitions or
interpret topology. Initial messages populate both conversation and full-context
views before any child Turn is registered.

The FileSessionStore adapter serializes initialization across independent handles
with its filesystem lock, writes and synchronizes a complete snapshot, then
atomically publishes it. Identical retries verify the saved initialization and
return the current Session without resetting subsequent Turns or history. A
different digest or projection, a pre-existing ordinary Session, corrupt committed
data or an oversized initialization is rejected without overwriting it. The
initialization field is explicitly present and nullable for ordinary Sessions;
missing fields are not a legacy fallback. Server must separately verify owner
facts before creating or reopening a private context.

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
identity, kind, schema version and exact opaque binding. Core's unified contract
is implemented; end-to-end Agent acceptance is still pending. Runtime validates recognized kind/version and durable
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

Runtime persists an immutable `ExecutionInputAdmitted` fact before model dispatch.
It binds the original request and message boundary, execution and Agent snapshot,
dispatch policy, maximum Steps and calls, tool timeout and absolute deadline.
Recovered checkpoints must match this independent source and cannot widen limits.
Core's per-Step request ID is validated as a derived coordinate, not compared as
though it were an unauthorized change to model configuration. Suspension does not
restart the original deadline clock.

Core calls `TurnEventRecorder::record_checkpoint` after fresh preparation and
stage validation but before any effect admission in that stage. Runtime overrides
this port to save `TurnCheckpointPrepared` through cancellation-aware publication.
Persistence failure blocks effects. Core's default no-op means only that Core
does not own storage; it provides no durable guarantee. A prepared checkpoint is
an execution recovery barrier, not approval authority or a public Suspended state.
Recovery selects the latest prepared, merged or suspended checkpoint, validates
its Steps against committed model responses, and hydrates only historical entered
effects from exact authority and result evidence.

The host verifier exposes a read-only `recover_wait` operation for the window
between committed external admission and Runtime wait publication. It accepts
historical issued authority and may return only an already admitted exact wait;
Runtime verifies that wait before publication. It must not launch work, make a
model request, issue a grant or infer admission from an untrusted notification.
The default returns no proven wait. Missing or unverifiable entered-effect evidence
produces fatal `ToolError::Uncertain`, not ordinary model tool-error feedback.

A committed effect receipt repairs an older checkpoint without repeating
preparation, authorization or execution. Runtime verifies preparation,
authorization, entry, receipt, scope and complete output size, and retains charged
usage. A merged checkpoint is published before driving the next Ready call or
Step. Compound publications contain a required source ledger cursor and content
identity, so publishing unchanged waiting state after a new lifecycle transition
does not incorrectly reuse an earlier suspension event. This cursor identifies
causal publication; it is neither a lease nor a cross-storage transaction.

Server validates child completion and join evidence, records the exact final tool
receipt, consumes the child result idempotently, and commits merged resume state
before continuing. Recovery retains the same attempt and budget. A permanent
one-shot resume claim is insufficient: a crash after claiming must not make the
checkpoint permanently unresumable. Local active-execution admission prevents
concurrent driving, while durable commit progress permits restart. Acceptance must
cover every commit gap, serial tails, partial parallel completion, out-of-order
children, duplicate consumption, foreign/corrupt waits and cancellation followed
by late results. These are required implementation gates, not completed tests.

## Unified suspension and resume interfaces

The next breaking migration replaces approval-only continuations with
`ResumableTurn::Suspended(TurnSuspension)`. A suspension contains one
`TurnCheckpoint` and a derived `SuspensionSummary`, whose `approvals` and
`external_waits` vectors may both be nonempty. Mutually exclusive approval or
external reason variants cannot represent a mixed batch. The summary must match
the checkpoint exactly after decoding and after every partial merge; it is not
authority to approve work or accept results.

`ApprovalRequest` becomes display metadata: approval, Turn and call identities,
tool name, reason and explicitly present nullable expiration. Saved preparation,
scope, revision and confirmation evidence live only in `CheckpointApproval`;
its reason is required so the summary can be derived. Remove `TurnContinuation`
and approval-only restore formats rather than maintaining parallel snapshots or
historical fallbacks. All new envelopes reject unknown critical fields and
missing required fields, including optional ceilings.

`ResumeInput` carries either exact `ApprovalConfirmation` or a bounded vector of
`ExternalResolution`. Confirmation binds approval identity, prepared digest,
policy revision, execution scope and trusted evidence identity. The host verifies
durable proof before invoking Core. Model tool-call IDs and policy revisions
remain opaque payload values; encode durable host coordinates independently so
slashes, Unicode and long native IDs do not become ambiguous journal keys.

An approval resume boundary is not an `ApprovalResolved` decision. Runtime
requires an independently persisted affirmative decision matching checkpoint,
approval, prepared digest, policy revision, scope and evidence identity. The
canonical decision payload is shared with Server; encoding it alone grants no
authority. Approval display facts contain metadata, not executable continuation
state or a second checkpoint copy.

`merge_resume_with_control` performs pure validation and merging. It requests
neither preparation nor tools nor models. Runtime persists its result before
calling `resume_checkpoint_with_control`; the latter drives only Ready calls,
refreshing preparation and current policy. Historical Completed calls retain
their exact authority and results. Unresolved waits block later conflicting
stages. Pending summaries are derived again rather than copied from an earlier
attempt. The existing requested-tool-call budget semantics must be retained,
including denied/preparation-failed calls; recovery must neither reset usage nor
charge the same request batch twice.

The Runtime host port is `ExternalWaitVerifier`. `verify_wait` resolves recognized
admission evidence; `verify_result` resolves exact committed result evidence.
Both receive `ExternalWaitContext` with the historical issued authority and exact
wait binding. Runtime independently checks source scope, preparation, grant,
call identity and original complete-result ceiling. A verifier cannot authorize
new effects or bypass those checks. The default `RefuseExternalWaits` rejects both
operations. The port is now wired into Runtime dispatch and read-only recovery,
with local proof and replay tests passing. Full child admission, result consumption
and commit-gap acceptance remain pending; this does not establish autonomous Agent
execution or real-model acceptance.

## Tool preparation and permission enforcement

The environment inventory is `file.read`, `file.write`, `file.edit` and `shell`.
Delegation uses independent `Capability::AgentDelegate` and `Effect::Delegate`;
Shell process permission cannot authorize child admission. Unknown delegation
resource footprints conflict conservatively at the Turn batch layer. Explicit
independent children inside a validated invocation are scheduled by Server,
subject to child permission ceilings and its bounded concurrency policy.
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

The corrected Runtime module passed 40/40 and Ledger 28/28. The registered
`server_http_environment_tools` target then passed all eleven data rows
(`/tmp/kolyan-http-environment-tools-v3.log`). Its retained report is
`/private/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-http-four-tools-MaazbW/report.jsonl`.
Per-case HTTP, model request/output and complete Ledger JSONL are written by the
test before comparisons. The matrix checks actual four-tool effects, approval
restart, cross-Turn context, cross-Session isolation, changed preparation,
cancellation and control-path protection. Each persisted approval has exactly
one earlier boundary admission and one later committed suspension. The three
write/edit/shell approvals remain three, not six, suspension facts. This is real
local subprocess/OS/HTTP evidence with scripted Provider responses, not actual
model-network or autonomous recursive Agent acceptance.

Coordinator boundary regressions passed on Memory and SQLite: admission alone
remains Running and is not resumable; a saved checkpoint followed by suspension
is resumable; cancellation and foreign execution facts cannot resurrect it
(`/tmp/kolyan-l5-server-boundary-tests-v2.log`). The existing approval recovery
dataset previously expected suspension before the requested checkpoint. Its
sequence now explicitly requires boundary admission, then approval checkpoint,
then suspension, retaining approval resolution, tool execution and completion.
That existing reconstruction target passed unchanged scenario assertions after
this fact-contract migration. Full workspace revalidation is still in progress.

### Fixture transport failure and verified cause

The subsequent workspace regression passed the preceding targets, then failed
the new HTTP fixture. Its original `read_line` returned errno 35 (`WouldBlock`);
the fixture thread exited and the production request consequently observed
Connection refused. `Provider::drop` then panicked on the failed thread join and
aborted during cleanup, hiding the first failure. The failing HTTP and Ledger
evidence is retained under `kolyan-http-four-tools-M9neZo`, with the workspace log
at `/tmp/kolyan-l5-service-boundary-workspace-v2.log`. This is not a green gate.

A native macOS regression reproduced the same listener and accepted-stream
setup: despite a 30-second configured read timeout, the read returned WouldBlock
in 91 microseconds before any first byte was sent. Explicit blocking mode then
read the request successfully. The evidence is
`kolyan-http-fixture-transport-M6se2f/probe.jsonl`. Therefore the confirmed cause
is inherited nonblocking mode, not a 30-second timeout, failed model behavior or
production protocol decoding.

The fixture now bounds concurrent connection reads and waits for fragmented
headers and bodies before consuming a script response. Empty connections do not
consume scenarios; malformed complete requests fail explicitly. Partial headers,
read states and errors are exported. Explicit finish reports worker failure;
Drop records cleanup without a second panic. Completed corrupt JSONL lines fail
parsing rather than being silently ignored while polling a partial final line.
The eight-test transport/scenario target passed, retaining all eleven scenario
reports under `kolyan-http-four-tools-VeeRXH`. This local target result does not
replace final workspace or actual-model Agent acceptance.

### Agent root verification and cancellation barriers

The current workspace all-target compilation passed. The first Agent-root
integration run failed for both named and inline definitions: write, edit and
read completed, but the shell call was refused because its prepared workspace
claim exceeded the fixture's narrower `safe` authorization scope. This is a
host-fixture authorization mismatch, not a Provider failure. The original
failure, requests, tool outcomes and authoritative facts remain in
`/tmp/kolyan-l5-agent-root-offline-main.log` and its referenced `actual.jsonl`
exports. The subsequent workspace test encountered the same failure; compilation
alone does not establish workspace acceptance. Shell authorization must describe
its real workspace boundary, not imply that its working directory is a sandbox.

Two additional Runtime tests enforce the execution-specific cancellation
boundary before any tool effect: committed cancellation refuses checkpoint
publication with typed Cancelled and leaves the Ledger unchanged; cancellation
of another execution cannot block a valid checkpoint. Both tests passed along
with the two existing prepared-barrier tests in
`/tmp/kolyan-l5-cancel-barrier-tests.log`. These four local tests do not establish
actual-model delegation, recursive calls, parallel joins or long-task acceptance.

The corrected root dataset now declares separate file and shell authorization
scopes. File manifests retain the `safe` boundary; the host explicitly authorizes
Shell's real workspace claim. The named and inline offline cases both passed,
each executing four production isolated tools across two independent Turns in
one logical Session (`/tmp/kolyan-agent-root-offline-v2.log`). Existing assertions
were retained. The actual-model root matrix has been started separately for all
19 configured combinations and both selectors; its result remains pending and
must not be counted from the offline pass.

### Repeated recovery of a waiting checkpoint

A new regression recovered the same pending external wait three times and
exposed a production defect: the second attempt reused the fixed resume boundary
event identity, causing a Ledger conflict and an incorrect failed terminal.
The failing gate is `/tmp/kolyan-l5-cancel-barrier-tests-v2.log`; the assertion
remains in the suite.

Resume lifecycle admissions now include the committed Ledger cursor captured for
that attempt. This identifies an observation and admission attempt, not a lease
or execution authority. Step and tool-effect identities remain stable; completed
effects must never be entered again. Each admission still atomically checks
cancellation. A new attempt cannot widen the original budget, replace saved
authority or interpret a boundary admission as a published suspension.

The corrected Runtime module passed 57/57 in
`/tmp/kolyan-l5-runtime-repeat-resume-fixed.log`. The repeated-recovery regression
checks three suspensions, two entered effects, one receipt, one admitted external
wait, unchanged charges and conflict tail, and no model request. Cancellation
after a saved prepared checkpoint also refuses repeated recovery without polling
model/tool ports or appending facts.

Server commit-gap tests passed for consumption committed before either the
Runtime receipt or merged checkpoint publication, with exact proof rereading and
no second consumption (`/tmp/kolyan-l5-server-consumed-runtime-gap-v2.log`). Named
and inline root approval restarts passed through rebuilt production services and
isolated tools (`/tmp/kolyan-agent-root-restart-offline-v2.log`). These are local
domain/OS tests; neither establishes actual Agent delegation or live-model
approval acceptance.

### Cached input in actual model acceptance

The actual-model root matrix exposed a test expectation error on the
MiniMax Anthropic-compatible surface. The fixture summed only
`TokenUsage.input_tokens`, while Task accounting also includes reported cache
read and cache write inputs. One exported run reported 3562 ordinary input tokens
and 1664 cache-read tokens; its Task input total was correctly 5226. The original
assertion failure is retained in `/tmp/kolyan-l5-agent-root-live-main.log` and
`kolyan-agent-root-LXn117/actual.jsonl`.

Root and approval-restart acceptance must independently sum ordinary input,
cache-read input and cache-write input with overflow checks. Missing ordinary
input or output remains an unreported Step; optional cache omissions do not
invent cache measurements. Reasoning tokens are not added to output twice.
This corrects the expected measurement contract, not production accounting or
the assertion strength. The current matrix remains a failed run even after the
fixture source is corrected; only a new complete run can establish acceptance.

### Actual model diagnostics and remaining sandbox failure

The completed root matrix passed 20 of 38 rows; nine rows failed the old cache
assertion and nine independently failed with ToolTimedOut. The completed approval
restart matrix passed 14 of 38 rows; fourteen failed cache assertions and ten
failed with ToolTimedOut. Both failed reports remain authoritative for their
runs (`kolyan-r1-matrix-yzYmjM/report.json` and
`kolyan-r1-matrix-UjszSP/report.json`). Fixture corrections do not change them.

The recorded MiniMax cache fixture now preserves all five exact Step usage
objects from `kolyan-agent-root-LXn117/actual.jsonl`, rather than inventing a
distribution with the same aggregate. Deep equality against that export was
verified. All ten accounting cases passed with JSONL written before assertions
(`/tmp/kolyan-l5-agent-recorded-usage-tests.log`).

A separately registered diagnostic selects Qwen's Anthropic-compatible
`qwen3.7-plus` named root from the existing configuration. It uses the original
two-Turn dataset, real isolated tools and unchanged root assertions. It passed
(`/tmp/kolyan-l5-agent-tool-timeout-diagnostic.log`), exporting requests,
responses, tool timings and facts to `kolyan-agent-root-HiIOqE/actual.jsonl`.
This proves that selected run only, not either full matrix or recursion.

The workspace gate independently failed `exact_file_boundary` at its
`read_baseline` row with a sandbox timeout. This test has no model dependency;
its original five-second deadline and exact success assertion remain unchanged.
Evidence is `kolyan-exact-file-boundary-BmSKzm/actual.jsonl` and
`/tmp/kolyan-l5-workspace-tests-after-repeat-fix.log`. This establishes a
model-independent timeout, not its lower-level cause. Parent process samples
and test adapter timings do not by themselves prove a native worker startup,
stdin EOF or filesystem failure. Do not mask it with retries or larger deadlines.

An independent invocation reproduced the same baseline timeout
(`kolyan-exact-file-boundary-KW1yr7/actual.jsonl`). A subsequent invocation of
the same compiled test passed all twelve rows in 0.63 seconds
(`kolyan-exact-file-boundary-H4sNNy/actual.jsonl`). Neither replaces the failed
workspace gate nor identifies a repair. A sample of the new matrix test process
also captured `_dyld_start` before Rust test entry, followed by normal test
startup; this confirms that startup delay for that test process only, not the
cause of the unsampled file-worker timeout.

### Worker startup security evaluation

The system log now links the reproduced timeout to the actual worker PID 15446.
At 00:32:34.102 on 2026-10-02, AMFI inspected the trusted helper. At
00:32:39.097, AppleSystemPolicy recorded the process as disallowed. At
00:32:39.142, syspolicyd completed its evaluation for the same helper identifier
and recorded provenance tracking for PID 15446. That last event was about
45 milliseconds after the sandbox's five-second deadline. The exact evidence
is `/tmp/kolyan-l5-native-worker-system-diagnostics.log` and
`/tmp/kolyan-l5-worker-15446-security-evaluation.log`.

The helper has an ad-hoc linker signature, and `codesign --verify --strict`
succeeded. It has provenance metadata but no quarantine attribute. Consequently,
the AMFI message alone does not prove corrupt binary contents. The matched
startup evaluation timing supports cold trusted-execution assessment exhausting
the deadline; it does not prove that every earlier tool timeout has this cause.
There is still no worker stack proving a file-operation or stdin deadlock.
Apple's [trusted execution troubleshooting guidance](https://developer.apple.com/forums/thread/766466)
uses system policy logs to distinguish launch admission from application faults;
disk signature verification is not a claim that an execution policy will admit
every launch.

Independent data-driven stdin tests passed for 0, 1, 4096, 65536 and 1048576 bytes
with the original five-second deadline, alongside all three existing input-limit
tests (`/tmp/kolyan-l5-sandbox-stdin-eof-v2.log`, four tests). The large inputs
exercise pipe backpressure and require EOF before `wc` can complete. This rules
out a reproducible EOF failure in those cases, not every possible process fault.

Do not disable Gatekeeper, remove trust metadata or sign an active pinned worker
in place to make a test green. Executable packaging and host startup diagnostics
are separate from model retries, tool authority and uncertain-effect recovery.
No entered effect is automatically replayed because a startup fault is suspected.
Normal tool deadlines and fail-closed sandbox enforcement remain unchanged.

### Corrected complete actual model matrices

Both corrected matrices finished with every planned row executed. Root execution
passed 36 of 38 rows (`kolyan-r1-matrix-cPOpzj/report.json`); approval restart
passed 37 of 38 rows (`kolyan-r1-matrix-sZS84L/report.json`). Neither is a green
matrix. This run had no cache expectation failures or ToolTimedOut rows, but that
does not retroactively repair earlier runs or prove cold startup reliability.

The failed inline Qwen 3.8 Flash root first response claimed write and edit had
already completed, then requested read and shell. Its single observed request
contains the full ordered write/edit/read/shell instructions and no assistant or
tool-result history. Reading the nonexistent proof returned errno 2 in about
130 milliseconds (`kolyan-agent-root-jzirSh/actual.jsonl`). This is distinct from
the five-second startup timeout and remains a failed requested scenario.

The other failed rows are the named DeepSeek v4 Pro root and inline Qwen 3.8 Max
approval restart on the Anthropic-compatible surface. The transport reported
peer closure without TLS close_notify after 879 and 5709 response bytes,
respectively (`kolyan-agent-root-TBg7ia/actual.jsonl` and
`kolyan-agent-restart-dQtX7N/actual.jsonl`). Partial streaming observations are
not synthesized into a complete response or permission to replay tools.

An additive, data-driven diagnostic selects these three exact deployments and
flows. It preserves their requests and assertions but creates new stores and
workspaces for every new execution. All three diagnostic outcomes are reported,
including failures; this is not a hidden retry policy or full-matrix acceptance.
Its case file is `tests/fixtures/agent/observed_failures_diagnostic.json` under
`kolyan-integration-tests`. The complete Sandbox module passed 32 of 32 tests
after the additional EOF cases (`/tmp/kolyan-l5-sandbox-full-after-eof.log`).

The three fresh diagnostics subsequently all passed, with their independent
report at `kolyan-r1-matrix-D3j94H/report.json` and log at
`/tmp/kolyan-l5-observed-failures-fresh-diagnostic-v2.log`. A credential-free
selection test also passed, verifying nonempty unique case IDs and exactly one
configured deployment for each selector. These results show the three failures
are not reproduced on that fresh run; they do not convert either original
complete matrix into a pass. No production retry, altered prompt, changed
tool-error policy or looser assertion was introduced to obtain these passes.

### Child recovery and bounded parallel verification

The updated Agent module passed all 73 unit tests and its strict all-target
Clippy gate (`/tmp/kolyan-agent-hook-ready-tests-v2.log` and
`/tmp/kolyan-agent-hook-ready-clippy-v2.log`). These tests use scripted Provider
responses; they are not network-model acceptance. The routing schema includes
the complete named, inline and self-call target shapes and permission fields,
with 19 data scenarios checking schema, serde and preparation separately.

Checkpoint restoration accepts Provider-native call IDs containing slashes,
Unicode and more than 256 bytes; host definition-name constraints must not be
applied to these IDs. The child approval scenario rebuilds its Runner from saved
ownership, enters no effect before confirmation, enters one effect afterward,
and resumes the exact parent to Completed. Evidence is exported before comparison
in `kolyan-agent-pump-g3bSHY/actual.jsonl`.

Parallel scheduling requires both explicit preparation and a trusted factory's
read-only enforcement claim. A Runner-wide semaphore bounds active children;
the original prepared invocation bound may narrow it further. Tests observed
actual concurrent peaks of two and four, and a peak of two when preparation
narrowed the bound to two. They also verify zero active work afterward. Evidence
is `kolyan-agent-children-vEwSMB/actual.jsonl`.

Writable or shell-capable children, factories without the enforcement claim,
and Tasks with a finite shared token ceiling remain serial. Concurrent token
reservation and safe writable resource isolation are not implemented; unknown
usage must not be treated as zero or a configured ceiling removed to enable
fanout. This conservative fallback is an explicit current boundary, not evidence
that all parallel topologies are supported. Deep recursive, network delegation
and single-Task long-continuation acceptance remain separate pending gates.

The additional Sandbox EOF cases were committed as `a244c4f` with normal hooks.
The hook passed workspace formatting and strict all-target Clippy. This commit
does not establish a passing workspace test run or full Agent acceptance.

### Discoverable admitted targets

Agent invocation advertisement must expose the exact named definition keys
available under the saved parent snapshot and current host ceiling. A generic
string schema is not a registry discovery mechanism. Derive named-target choices
from the intersection of those ceilings and the current exact catalog entries;
do not advertise inaccessible definitions or substitute a display name for a
stable definition ID and revision. Inline and self-call branches are advertised
only when their respective capabilities permit them. An empty named set must
not leave an unrestricted named branch in the advertised schema.

This advertisement helps models construct valid inputs; it does not grant
authority. Preparation must independently resolve and validate exact definitions
and attenuated permissions. Unknown keys, invented versions and authority
expansion remain explicit failures. The advertisement must be derived from the
same saved snapshot used by dispatch, including after an approval or child-wait
restart; it must not use mutable display labels as identity.

The first actual-model delegation matrix remains an independent historical run.
Its MiniMax named case requested `child-definition/r1` but the actual response
selected `child/r1` and added that unregistered key to child delegation rights.
Preparation rejected it before child execution. The complete request, response
and failed preparation are preserved in
`kolyan-agent-delegation-dRlAFw/actual.jsonl`. This proves that specific invalid
selection, not a Provider decoder defect. Do not loosen registry validation or
rewrite model output to make the case pass.

Tests must compare advertisements for named-only, inline-only, self-only, no
delegation, missing catalog revisions and narrower host ceilings. Reconstructed
Runner advertisements must retain the saved identity constraints. Network cases
still require actual models to generate calls; adding discovery information
does not permit scripted insertion or retroactive changes to the running matrix.

### Explicit context projection before Turn admission

The trusted host may propose ordered, nonempty, disjoint half-open message ranges
from an immutable complete source request. The first implementation selects whole
messages; it does not invent a summary or rewrite retained contents. A versioned
projection policy and exact expected source digest bind this proposal. The host
must retain the first user input carrying non-tool-result content and the complete
tail beginning at the latest such user input. If either anchor is absent, refuse
projection rather than guess the objective or current input.

Validate the complete source under a separately bounded source policy before
selection. Validate the selected request again, including tool-call/result
pairing, under the target context policy and model descriptor. Governing system
instructions, tool schemas, model, extensions, cache configuration and explicit
output/reasoning limits are unchanged. Omitting only a call or only its result,
changing source identity, unordered or overlapping ranges, removing either
anchor and exceeding the target bound fail explicitly. The caller retains the
entire original trajectory; no source or checkpoint is modified.

Return the selected request with source and selected digests, policy identity,
retained and omitted ranges, source/selected byte sizes and normal context
budget evidence. A missing output limit is a separate preparation change and
must not be silently accepted by this projection operation. Unknown model token
counts remain unknown in Inspect mode and fail in Strict mode; smaller UTF-8
size is not proof that a token budget is satisfied.

This pure operation runs before Turn or continuation admission. The host must
persist its complete source and projection evidence before admitting the exact
selected neutral request. Core, its Ledger and the Provider observe that same
selected request. Provider-side preparation continues to reject invisible
request rewrites. Rebuilding the host from persisted source and the same proposal
must reproduce the same request and evidence, without reentering old effects.
Tests require positive reduction, unchanged/no-reduction, tool-pair failures,
anchor removal, digest mismatch, bounds and unknown-count behavior, with actual
JSONL written before comparison. This API alone is not long-task integration or
proof of an automatic summarizer.

### Parent completion after typed child failure

Turn completion and Task success are distinct. A parent may finish an honest
answer after consuming a verified Failed or Cancelled child result. Under the
current all-invocations-success completion policy, that does not satisfy Task
success. The host must derive a typed Task terminal from verified graph evidence
instead of attempting unconditional success solely because the root invocation
completed. Keep the child's failure/cancellation and original consumption proof
unchanged. A child-only cancellation must not masquerade as user cancellation of
the whole Task; the applicable Task policy determines its conclusion.

The new offline delegation matrix exposed the unconditional-success gap: four
normal cases passed, while typed Failed and Cancelled child cases consumed their
correct results but root finalization was rejected by the existing strict Task
success guard (`/tmp/kolyan-agent-delegation-offline-v4.log`). Preserve those
assertions and add durable finalization/reconstruction coverage; do not relax
the success guard, swallow the failure or synthesize successful child terminals.

### Immutable test executables across approval waits

A test host must fix its trusted tool executable before the first admission and
reuse that exact version across all Turns, child invocations and rebuilt hosts.
The mutable Cargo build output is a build input, not a long-lived execution
identity. Put a verified test-owned executable snapshot outside model-writable
workspace roots, record its path and digest, and reject missing or changed
snapshots on reconstruction. Do not recopy the current build output at resume,
alter an active pinned binary or weaken the production revision comparison.

The first long-task matrix exposed this fixture gap at Qwen 3.7 Max's inline
eighth Turn. `kolyan-agent-long-task-ZeP9S6/actual.jsonl` exports both actual
preparations for call `call_aefa9ca023334094aaf80b24`. Their complete scalar
comparison differs only in `tool_revision` and the derived preparation digest:
the worker hash changed from `7a01c3413f7a1e6bff6e6735a26b7c278208cd016602fd4b67c9c80f9a4fe128`
to `90e9319dd4b0b9da401182f6fc2f0733e61540b48391d266e9739defa24ff194`.
Arguments, target identity, directory identities and sandbox requirements match.
The fixture currently reloads `CARGO_BIN_EXE_kolyan-test-tool-worker` when
rebuilding tools, so a concurrent integration build can change the preparation
version. Approval correctly refused that mismatch; this is not evidence of a
model error or lost context.

New fixture tests must simulate replacing the build input while a task is
waiting, then rebuild against the original private executable and verify exact
preparation and one effect. Replacing the actual pinned executable must still
fail closed. Preserve the existing failed matrix and export its failure; do not
replace it with a retry or change the cases in an already running matrix.

### Explicit projection local verification

The projection API's two unit tests passed, including 18 data rows exported to
`kolyan-context-projection-ZTM9kD/actual.jsonl` before comparison. Positive
selection retains four of ten messages and removes only completed historical
messages. Original tool-pair membership is checked even when call IDs recur in
later Steps, avoiding accidental cross-Step pairing. Source/plan round trips
reproduce identical output and evidence; unknown token counts remain unverified.
The full Agent module passed 78 tests and strict all-target Clippy in
`/tmp/kolyan-l5-agent-with-context-projection-full.log` and
`/tmp/kolyan-l5-agent-with-context-projection-clippy.log`. These local gates do not
establish positive network long-task projection acceptance. The pure API and
its tests were committed as `456e631` with normal hooks.

### Recovery authority and execution admission

Every child-driving entry point, including approval resume and parent pump,
must verify the persisted definition and permission snapshot against the current
host ceiling before polling a model, executing a tool or consuming a child
result. Revocation fails closed; do not silently attenuate the saved definition
or rewrite an admitted request. Apply this check at the common owner-validation
boundary, not only when assembling the parent's resumed executor.

Actual child execution uses a host-owned, shared admission budget across initial
dispatch, approval resume and nested pump. Multiple Runner instances using that
host budget share its bound. The original invocation's prepared bound also
applies after suspension. These ephemeral permits are scheduling controls, not
persisted authority, distributed leases or Runtime ownership. Release them on
suspension, failure and completion. Do not hold a parent's execution permit
while waiting for descendants; a bound of one must not deadlock recursion.
Check durable cancellation while waiting and again before execution admission.
Writable or unattested executors remain serial. Shared finite Task budgets must
not become oversubscribed by independently admitted sibling groups.

Data-driven tests must cover revocation after child completion and before pump,
both root and recursive owners; simultaneous approval confirmations; nested
pump competing with fresh dispatch; and two Runner instances sharing a host
budget. Export complete requests, preparations, ledger facts and measured active
execution counts before assertions, including failure paths. Verify no new
model polls, effects or consumption facts after refused authority checks.

### Independently recoverable Task finalization

Provide a host entry point that finalizes a Task from already durable invocation
evidence without executing another Turn. Restore and verify exact scope,
bindings, root terminal and policy-required child terminals and consumption
proofs. A completed root with a missing Task terminal is a commit gap, not a
reason to rerun the root. Repeated finalization is idempotent and returns the
persisted Task verdict; it does not fabricate a new Turn result.

Root completion alone never establishes Task success. Verified child failures
and cancellations are evaluated under the existing completion policy. Root
failure and cancellation retain their original typed causes. Explicit Task
policy decides retry eligibility, recovery-required state or terminal failure;
there is no implicit model retry or conversion of failure into success. User
cancellation of the whole Task remains distinct from a child-only cancellation.
Missing or contradictory evidence refuses finalization without new effects.

Tests inject a gap after root terminal persistence and before Task finalization,
rebuild the host and finalize twice. Assert exactly one Task terminal commit,
unchanged invocation evidence and zero additional model/tool calls. Cover both
success and policy-derived failure, missing proofs and current host revocation.

### Remaining topology acceptance boundaries

Preflight recursion limits and authority before persisting a new child identity
or binding. Test exact-limit and over-limit recursion, nested attenuation,
deep failure propagation, concurrent admission conflicts and late child results
after parent cancellation. Cancellation must not resurrect the parent or permit
new result consumption. Dependency ordering requires explicit topology data and
durable dependency evidence; a serial loop is not proof of a dependency graph.
Multiple waiting children, mixed approval/external waits and partial-consumption
commit gaps require independent cases rather than a single happy-path case.

The recovery gaps above were identified by source audit; that audit is not a
passing regression test or a reproduced network failure. Implementation and
offline/network acceptance evidence must be recorded separately.

### Optional display-name comparison

The definition's optional display name may be omitted or explicitly null; both
deserialize to the same validated domain value. Integration comparisons use
strict validated AgentDefinition equality, retaining all identity, revision,
model, instruction and permission checks. Do not compare raw JSON spelling of
that optional field or normalize required fields. Preserve the earlier failed
network trace and add an omitted/null parity regression before a fresh run.

### New delegation timeout evidence

The fresh v3 matrix's `kolyan-agent-delegation-13k7qt/actual.jsonl` identifies
itself as `single-self`; trace paths printed before execution must not be
attributed to the preceding case's result line. For file.read call
`call_32bad7a9470d438c9d0a7e9f`, both preparation observations returned the same
digest `71a90808a4d0246537797e310bfddee4f25a43eb9247207dfe8e4c24d0f43f45`
in approximately 114 ms and 108 ms. The execution was polled, then dropped after
30,003,982,667 ns while its inner future had not returned and its control was
not cancelled. This establishes an outer execution timeout, not a returned
worker error or an approval revision mismatch. The trace alone does not locate
the stalled inner stage. Keep this distinction and the failed evidence while
investigating; do not increase the deadline or assign a model/SDK cause without
the missing execution-stage evidence.

The v3 delegation matrix completed all 76 rows in
`kolyan-r1-matrix-ILxIiA/report.json`: 59 Passed and 17 Failed, with one attempt
per row. Its test log ends in FAILED after 1,801.74 seconds. Failure details
comprise seven child tool timeouts, three permission-ceiling refusals, three
invalid sequence arguments, two optional-display-name JSON comparisons and two
read-receipt count mismatches. These are observed categories, not confirmed
root causes. In particular, the seven timeouts are not automatically the same
inner-stage defect. The original report remains failed after fixture corrections;
only a separately recorded fresh execution can supply new acceptance evidence.

### Repeated child read diagnosis

The Qwen 3.8 Flash OpenAI self-call trace
`kolyan-agent-delegation-Ovgubz/actual.jsonl` contains two successful child reads
with distinct call IDs, `call_2167878b32ac4993964e062c` and
`call_b9e4d947691444a29a1f1bbe`. Deduplicating the exported ledger by event ID
still leaves both requested calls and both successful receipts. This is not
replay of one admitted effect.

The child's actual neutral Step 1 request includes the original read-once input,
the first assistant call and its complete successful ToolResult containing
`CHILD-REAL-PROOF`, 16 bytes and the matching SHA-256. The following response
first recognizes that content, then incorrectly describes a tool syntax marker
and generates a new verification read. Step 2 includes both complete results.
The final model response acknowledges the inaccurate description and the extra
read. Thus this trace disproves missing ToolResult context in the recorded
neutral request as the cause of that extra call. It does not independently
capture the outgoing HTTP request body or explain other receipt failures.

Keep the read-once assertion and failed matrix row. Do not hide the extra call
with receipt filtering, argument-based effect deduplication or implicit retries.
Any future operation-count policy must be explicit host governance and tested
as such, not an undeclared patch to make this behavioral case pass.

### Continuation artifact verification before successor admission

The initial live continuation trace `kolyan-long-continuation-mmVhDt/actual.jsonl`
identifies Qwen 3.8 Flash with a named Agent. Its fifth invocation requests an
edit from TURN04 to TURN04 followed by TURN05, then a read. The observed tool
events contain only a read at that stage, returning the unchanged four-marker
file. The framework records the invocation as completed with 27 physical bytes
and admits invocation six, whose requested TURN05 replacement cannot match that
file. The worker correctly returns MissingMatch for the generated edit call
`call_16b2bd7089324c9f8c67d667`. This trace does not show a lost fifth write: no
fifth edit call was requested or receipted.

Model completion and invocation termination are not proof that a requested
artifact transition occurred. Extend data-driven acceptance with independently
verified per-invocation expected physical state and receipt milestones, recorded
before successor admission. Retain complete failure evidence and distinguish
the earliest unmet artifact obligation from a later correctly refused edit.
Do not replace final verification or weaken existing assertions. Production
hosts that require artifact completion must use an explicit trusted verifier;
the generic Runner must not infer objective success from natural-language claims
or hardcode this test's marker sequence.

### Stack bounded preparation digest serialization

The focused recovery gate aborted with stack overflow. The macOS crash report
`kolyan_agent-4c6b0c6ab6099703-2026-10-02-035308.ips` places the fault in the
recursive policy preparation canonicalizer while validating issued authority
inside actual child dispatch. The report contains the digest validation and
Runtime/Core/Agent pump call chain; it is not evidence of model recursion beyond
the configured topology depth.

Replace recursive canonicalization with an explicit traversal that emits the
same compact JSON bytes, sorting object keys and preserving array order and
scalar serialization. Keep the existing one-MiB prepared-input ceiling and
refuse overflow while writing. No digest schema change, permissive validation,
stack-limit increase or special recovery-test bypass is allowed. Add independent
golden-byte/digest, deep-input and small-stack regressions, then rerun the actual
recovery gate. A crash location alone does not prove this replacement fixes all
remaining stack pressure in the execution path.

Policy local verification passed all 32 tests, including four new independent
regressions, in `/tmp/kolyan-l5-policy-iterative-canonical-tests.log`. The 96-level
borrowed input serializes on a 128-KiB worker stack without changing the normal
test/runtime stack limit. The fixed schema-two payload produces its expected
digest; exact byte ceilings and overflow refusal are checked. Strict all-target
Policy Clippy passed in `/tmp/kolyan-l5-policy-iterative-canonical-clippy.log`.
These checks establish the serializer's local contract, not full Agent recovery
or network acceptance.

### Finalization evidence by verdict

Successful parent completion requires every policy-required child or dependency
result to have its exact durable consumption proof. That is distinct from a
failed or cancelled parent: such a parent must not be resumed merely to consume
a late result. Verify physical child terminals and preserve any existing
consumption proofs, but do not manufacture missing consumption or require a new
one to establish failure. Under the current all-invocations-success policy, a
new failure finalization requires all admitted executions to have verified
terminal evidence; unfinished work explicitly refuses that operation.

A whole-Task cancellation already recorded in the journal remains its verdict.
An idempotent finalization request may return that verified verdict after exact
owner/scope validation without pretending every running child is physically
stopped. Task verdict, physical execution quiescence and child-result consumption
are separate facts. Late results cannot replace cancellation or revive a parent.
Current authority checks still apply, and no finalization entry point executes
or consumes new work. Tests must cover an unsuccessful parent with an unconsumed
late child, not only a successful parent that already consumed a failed child.

Finalization policy selection uses an exhaustive typed dispatch. New policy
variants must not be silently treated as all-invocations-success. Continuation
private-context initialization/dependency provenance is not an agent.invoke
ToolResult consumption. Verify the proof appropriate to the topology role;
explicitly unsupported roles are a pending implementation boundary, not evidence
that long-task acceptance is complete.

### Canonical serialization with preserved insertion order

The separate command `cargo test -p kolyan-policy --features serde_json/preserve_order`
passed all 32 tests and doc tests with zero failures in
`/tmp/kolyan-l5-policy-preserve-order-tests.log`. In particular, the fixed
schema-two digest and nested key-order golden bytes remain unchanged when JSON
objects preserve insertion order. This verifies Policy's explicit ordering, not
every digest in the workspace or every future serialization feature.

### Actual two-child scheduling matrix result

The first actual-model scheduling matrix finished all 38 rows in
`kolyan-r1-matrix-rid4vp/report.json`: 36 Passed and two Failed, one attempt per
row. Cases exercise two read-only children with measured overlapping Provider
intervals and two writable children with serial intervals, using actual models
and isolated OS workers. The failed MiniMax OpenAI read-only case has an invalid
sequence argument; the failed Qwen 3.7 Plus Anthropic writable case has a missing
expected receipt. Neither category by itself identifies a decoding or scheduling
defect. Keep both failed traces and their assertions while determining causes.

Subsequent exact trace inspection confirms the MiniMax generated
`named_targets: ""`, not the required array, and was refused at admission. The
Qwen second child receives the explicit write input but returns a text refusal,
calling it prompt injection, without generating a file.write call. Keep the
zero-versus-one receipt failure; model completion is not effect completion.
The scheduling matrix's owner confirmed exit 101 after 1115.10 seconds. Neither
failure is established as a scheduler defect by these traces.

### RootOnly child lifecycle after parent cancellation

RootOnly cancellation does not authorize a new child, parent resumption or new
parent consumption. It does allow an already entered, uncancelled non-root
invocation to finish or explicitly resume its own durable wait under its saved
identity and current host authority. AllInvocations cancellation refuses such
resumption. A blanket terminal-Task/parent check must not erase this distinction.

Separate immutable admission/ownership inspection for an existing child's
lifecycle from active parent admission, pump and consumption authorization.
The child entry point verifies the complete original admission, exact binding,
current attempt, committed checkpoint/approval and current child permission
ceiling. RootOnly is checked against the actual persisted Task cancellation
policy, not model arguments. An admitted but never entered child is not eligible
for detached resumption, and partial/missing admission proof fails closed.
Historical proof reads alone are never execution authority.

Detached child execution still uses the shared host and original invocation
scheduling budgets and records physical stopped evidence and usage. It cannot
change the cancelled Task verdict, resume the parent or publish consumption on
its behalf. Tests reconstruct both hosts after root cancellation, resume a
waiting child under RootOnly, and compare the same scenario's refusal under
AllInvocations. Include current host revocation, foreign child/checkpoint,
never-entered child and late terminal publication, with complete failure traces.

### Directory read refusal in actual continuation

The Qwen 3.8 Flash Anthropic named continuation trace
`kolyan-long-continuation-8YE1JL/actual.jsonl` records successful file write/read
in invocation one and successful file edit/read in invocation two. The actual
neutral request `turn-long-02-step-2` contains the complete successful read
ToolResult: `safe/long-proof.txt`, 13 bytes and content `TURN01|TURN02`.
The following model response nevertheless requests another file.read, call
`toolu_a7d728a15b924f0495b125db`, with path `safe`, which is the fixture directory.
Preparation correctly refuses a non-regular-file leaf. This is not evidence of
lost file contents or a missing ToolResult in the recorded neutral request.

Keep this row failed and preserve its request, generated arguments and refusal.
Do not broaden file.read into directory listing or silently rewrite model paths
to make the case pass. This diagnosis concerns the second invocation's third
Step, not a successor invocation; outgoing HTTP bytes are not independently
captured by these neutral request observations.

### Cancellation intent and terminal proof in finalization tests

The initial late-child finalization export
`kolyan-agent-finalization-late-CXavKf/actual.jsonl` distinguishes two paths.
The execution-only cancellation case records ExecutionCancelled but no
TurnCancelled terminal; its root remains RecoveryRequired even after the child
completes. Historical terminal verification correctly refuses that root. The
whole-Task cancellation case records both events and preserves the Cancelled
Task verdict while the previously entered child publishes completion.

Keep cancellation intent separate from physically stopped execution. A fixture
for a terminal cancelled parent must exercise the existing cancellation
coordination and stopped publication, rather than using cancellation intent
plus reconcile as a substitute. Preserve a separate incomplete-terminal refusal
case, and assert that late child completion never consumes or resumes the parent.

The original actual-model long-task job finished both 38-row matrices: ten
independent Session Turns passed 32 rows and failed six; single-Task continuation
passed 36 rows and failed two. Both test functions ended failed. These are
complete historical execution reports, not full acceptance of corrected worker
pinning, per-invocation verification or positive context projection. Those
changes require separately recorded fresh executions.

### Full Server regression after historical proof reads

The independent full `cargo test -p kolyan-server` run passed 105 tests and doc
tests with zero failures and exit zero. Its log is
`/tmp/kolyan-l5-server-full-historical-main.log`. This includes the separated
historical result/consumption reconstruction tests, which write JSONL proof
observations before assertions. The result verifies the Server regression scope;
it does not establish Agent finalization, RootOnly recovery or actual-model
projection acceptance.

### Positive projection offline proof

The new positive projection offline scene passed one test in 23.39 seconds,
using the actual OS workers with scripted model frames. Its exported trace
`kolyan-long-continuation-DhQs13/actual.jsonl` contains ten completed invocations,
nine durable pre-Turn projection readbacks and nine exact Core/Provider request
comparisons. Eight Turns omit complete historical message ranges, meeting the
data-owned minimum of eight. Eleven waits reconstruct the host from saved state.

Full source requests and projection proofs remain artifact-backed before Turn
admission; selected history does not overwrite that source. Per-invocation
physical file checks run before the next invocation. This is positive selection
and durability evidence, not actual-model or trusted-token-budget acceptance:
the counter remains Unsupported and budget mode Inspect. Keep the independent
Strict Unknown and serialized-size refusal checks, and run the separate complete
actual-model projection matrix before claiming network acceptance.
