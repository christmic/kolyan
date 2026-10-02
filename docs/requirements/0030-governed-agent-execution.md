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

### Continuation finalization proof ports (approved implementation)

The host approved implementation of these read-only ports and exact reconstruction
rules. Approval is not an implemented Agent finalizer or a passing acceptance gate.
A Continuation's parent coordinate denotes its completed predecessor, not a
delegated child whose result must be consumed by that predecessor. Validate the
admitted Continuation role, exact predecessor, explicit dependency and its existing
`task.result_consumed` proof in the successor-to-predecessor direction. The current
historical consumption reader can verify that already committed successful edge;
do not replace it with an invented agent.invoke result or consume a second time.

Server provides a read-only `PrivateContextService::load_verified_initialization`
port taking `(&PrivateContextOwner, &FactRef /* exact ownership */, usize /* maxBytes */)`.
It returns the original
`SessionInitialization`, initialization FactRef and ownership FactRef after verifying
the one-record namespace, critical schema/subject/causes, canonical initialization
digest and exact Agent ownership callback. The supplied ownership FactRef must
equal the initialization fact's one and only causal reference. It must never call `initialize`, create
a Session or overwrite later history. Server owns this verifier; Agent owns the
role-aware finalizer. The host approved this implementation boundary.

Initialization alone establishes owner and selected-message identity, not source
derivation. The trusted host must additionally bind the full durable predecessor
trajectory or explicit projection source/plan/provenance to the successor's selected
initial messages before admission. Historical verification must reconstruct that
exact derivation and retain full source evidence. No opaque digest, empty context,
model-authored proof or unsupported-role rejection completes long-task acceptance.

The host orchestrating the complete graph owns its finalization decision. The
historical host-only integration long graph uses `TaskExecutionService::complete`
only after all ten admitted invocations and per-invocation artifact checks; those
historical executions do not establish AgentRunner Continuation finalization.
The separately validated Runner-finalized scenes below exercise the new role-aware
finalizer. That finalizer must
verify every current physical terminal, topology-appropriate dependency and context
proof, and existing Task criteria without new effects/grants/consumption. Root-only
completion cannot close a graph containing unfinished successors. Cancellation and
uncertainty remain refusals under the existing evidence contract.

Approved implementation type/boundary details:

`VerifiedPrivateContextInitialization` contains `initialization:
SessionInitialization`, `initialization_fact: FactRef`, and `ownership_fact:
FactRef`. The original immutable `SessionRecord.initialization`, not its later
messages, supplies the selected projection. Recompute the existing canonical
Server initialization from exact owner and original messages and compare the
complete stored value and initialization draft. `maxBytes` must be nonzero and
at most 16 MiB (the existing Storage initialization ceiling); bound the complete
serialized return envelope, not just its message text. It is a host ceiling,
independent of tool output grants. Missing/null/foreign initialization is refused.

Historical predecessor context is a separate proof, not this initialization.
`HistoricalContextRequest` contains the already verified exact predecessor
`AttemptBinding`, terminal `FactRef`, exact prepared and committed
`ExecutionEvidence` coordinates, and a host byte ceiling. These existing evidence
coordinates bind execution, event ID and cursor; do not substitute journal FactRef
for physical ledger evidence. `VerifiedHistoricalContext` returns the
same binding/terminal/endpoints, the Runtime-verified admission coordinate, the
reconstructed `Vec<Message>` and its canonical digest. Its complete serialized
envelope is bounded by nonzero `maxBytes <= 16 MiB`. These are Server
types, not additional persisted history or a second Runtime admission schema.

Runtime `driver/admission.rs::InputAdmission` remains private. The public
`verified_execution_input(&ledger, &RuntimeTurnKey, max_bytes)` reader reuses that
same strict decoder, returning `VerifiedExecutionInput` with key, immutable
`model_request`, saved nullable `agent_snapshot_digest`, exact `event_id` and
`cursor`. Both the saved event and full returned envelope must fit the nonzero
host ceiling of at most 16 MiB. Server uses this port, never a second local
admission wire decoder. None of these proof reads admits or executes work.

Current Server completion endpoints are exactly
`<execution>/session/Completed/prepared` and
`<execution>/session/Completed/committed`. Prepared payload has `session_id`,
`status`, `messages`, `context_messages`; committed payload has `session_id`,
`status`. The prepared context is a per-Turn delta, not the whole Session history.
Require exact IDs/idempotency keys/kinds/turn/session, strict known payload fields,
Completed status and ordered physical terminal/prepared/committed cursors. Require
the exact terminal FactRef to match the independently verified completed attempt.
Runtime's public verified admission supplies the immutable complete
`ModelRequest.messages`. Build `delta` only from the actual Engine
`<execution>/turn-event/` namespace's cursor-ordered `StepCompleted` and
`ToolExecutionCompleted`: each Step appends the complete Assistant content, and
each tool completion appends its complete User ToolResult block. Preserve every
block and original tool pairing. StepCompleted strictly requires the existing
Runtime fields `step_id`, `outcome`, `step`, with matching outer/inner outcomes.
Step IDs must derive from the exact Turn and ordered Step index; physical
turn-event IDs must match their idempotency keys. FinalAnswer is the last Step:
later Step/tool content, including publication after the physical terminal, is
refused rather than omitted from the reconstructed delta. The sole reconstruction is
`frozenFullContext = admission.messages + delta`.

Use `Session.inputs[exactTurn]` only as a cross-check: require its exact original
`messages` and `history_len` to satisfy
`admission.messages[history_len..] == input.messages`, with checked bounds, and
require the exact Completed prepared payload's `context_messages` to equal
`input.messages + delta`. Its `messages` must match the separately derived
completed conversational commit. An ambiguous input partition must never alter
the immutable reconstructed context. Do not read current
`Session.context_messages` or derive a prefix from latest mutable history.

Freeze all physical reads with `LedgerStore::query(LedgerQuery)` and explicit
`through: Some(exactCommitted.cursor)`, exact `execution_id`, ascending cursor
pages and an exclusive `after` advanced only from validated returned events.
The existing LedgerQuery accepts limits 1..=1024; the reader uses pages
of at most 32, checks page scope/order/limit itself, and refuses overflow or
incomplete evidence rather than falling back to an unbounded audit method.
The independent scan ceilings are 16 MiB cumulative serialized physical
events and 65,536 events, in addition to the caller's complete return-envelope
ceiling. These constants are host-reviewed limits, not token counts or tool grant
widening; pagination/serialization bounds do not claim allocation-free storage.
Require `terminal.cursor < prepared.cursor < committed.cursor`, exact endpoint
identities and all known payload fields. No `execution_events_after(..., 0)`
without a frozen through boundary substitutes for this read.

Later Session history, a newer commit, guessed role prefixes, or incomplete
commit gaps cannot authenticate the requested frozen context. Full omitted source
and explicit projection provenance remain separate required evidence when the
predecessor's admitted input itself was projected.

Dispatch is exhaustive over the current roles `Root`, `SelfCall`, `Delegation`,
`Continuation`. Root verifies its exact root binding/criteria. SelfCall and
Delegation retain parent-to-child result consumption. Continuation verifies its
completed predecessor, successor-to-predecessor dependency consumption, exact
private initialization, and full-source or explicit-range derivation against that
frozen predecessor context. There is no current `ContextReduction` topology enum
variant: explicit context selection is an operation on the Continuation input,
not a fabricated role. Unknown/corrupt/missing proofs refuse without mutations;
active recovery readers and cancellation/uncertain-effect rules are unchanged.

### Invocation source admission fence (approved atomic binding)

The current Agent projection recorder checks a Task snapshot and then appends
to a separate projection stream. A concurrent AttemptStarted can commit between
those actions. Finalization must not accept such a late projection as evidence
that exact source selection preceded execution. Snapshot checks or wall-clock
timestamps do not close this race. The host approved the smaller pre-invocation
source preparation and atomic InvocationAdmitted binding below. There is no
InputBound event/state, optional source, serde default or compatibility path.

Agent owns exact source reconstruction, immutable source/plan/provenance artifact,
original initialization and ownership binding, known projection kind/schema,
canonical digest and the selected request equality. Server must not deserialize
Agent projection payloads or perform context selection. A generic Server source
admission carries a generic identity envelope and an exact immutable FactRef.

`InvocationInputSource` has required variants `Standalone { fact: FactRef }` and
`Derived { fact: FactRef }`. `InvocationDefinition.input_source` and
`AttemptBinding.input_source` are both required and bind exactly the same variant
and reference. Root requires Standalone containing its actual immutable input;
SelfCall and Delegation require Derived from the actual authorized invocation;
Continuation requires Derived from the exact completed predecessor. An ownership
fact alone is not a standalone input or a derived context proof.

#### Host-orchestrated root preparation boundary (approved public contract)

The long-graph host must not encode Agent-private RootInput/ChildInput bodies or
copy their JSON schema into integration fixtures. The existing Runner.start
registers a root-completion criterion and automatically finalizes a terminal root;
that coupled entry point cannot represent a host's already declared ten-node Task
criterion. Do not evade it by deliberately suspending the first root and using a
different execution entry to skip finalization.

Provide an Agent-owned public root-input preparation operation for production
host orchestration. Its request supplies the exact execution coordinates, named
or inline selector, requested permissions and original tool-free ModelRequest.
It reuses the existing root resolver, stable instance reservation, immutable owner,
trusted inventory advertisement, selected request construction and Required input
archive (16 MiB host ceiling). Its typed result contains the resolved snapshot,
exact ownership reference, selected ModelRequest and the verified Standalone
source returned by Server's publisher. It does not register or alter a Task,
admit an invocation, start an attempt, invoke a Provider/tool or issue a grant.
Changed owner/input/inventory retries fail rather than overwriting any source.

The host retains responsibility for its previously declared Task objective,
criteria, limits and cancellation policy; it atomically admits the returned exact
source and runs the returned selected request under that same snapshot. Agent's
ordinary start path must reuse this preparation implementation, not maintain a
second RootInput encoder. Runner role-aware finalization then verifies the real
root source and each derived Continuation, without prematurely closing the Task
after the first completed invocation. Sagan owns the production Agent operation;
the long-task integration owner owns its callers. Concrete public names/signatures
are frozen below; implementation and validation receipts remain independent.

```rust
pub struct RootInputPreparationRequest {
    pub task_id: String,
    pub invocation_id: String,
    pub execution: ExecutionRef,
    pub selector: AgentSelector,
    pub requested_permissions: AgentPermissions,
    pub model_request: ModelRequest,
}
pub struct PreparedRootInput {
    pub snapshot: AgentSnapshot,
    pub ownership: FactRef,
    pub selected_input: ModelRequest,
    pub input_source: VerifiedInvocationInputSource,
}
// Receiver: self: &Arc<Self>; blocking preparation runs on spawn_blocking.
pub async fn prepare_root_input(
    self: &Arc<Self>, request: RootInputPreparationRequest,
) -> Result<PreparedRootInput, RunnerError>;
```

The logical Session must already exist. Tool-free original input and a nonzero
explicit output ceiling are mandatory. Factory inventory construction may occur,
but no Provider is constructed or streamed and no tool executes. The archive also
binds the requested execution coordinates; attempts cannot substitute another
execution. Exact retries preserve immutable identity/source; changed requests,
definitions or inventory cannot overwrite an existing source. Cancelling the
async waiter does not roll back a running blocking publisher; inspect/retry exact
facts, never infer absence of preparation from a dropped future. No Task closure
mode is added: the host owns its declared criteria and explicit finalization;
ordinary start retains its existing coupled lifecycle through shared preparation.

Ordering: reserve immutable identity; persist exact owner and initialization;
Agent prepares and verifies the real input/full source/selection proof; persist
the source fact; atomically admit InvocationDefinition with its required source;
then start the exact source-bound attempt. The current projection recorder's
requirement that the successor already exist is an implementation ordering
restriction, not a necessary storage dependency: migrate it to accept the host's
proposed definition and verify the completed predecessor before successor admission.
Predecessor admission already exists; successor identity/owner/init are independently
persistable. No model/tool enters during source preparation. A preparation interrupted
before admission may leave retained artifacts but never an executable invocation.

The shared source fact has critical schema version 1, subject kind
`task.invocation-input-source` and subject ID equal to the invocation ID. This
dotted subject is exported as `INVOCATION_INPUT_SOURCE_SUBJECT_KIND`; the earlier
undotted draft was incompatible with the unchanged Ledger kind validator. Its fixed
fact kind is `task.invocation_input_source`; its public payload envelope is
`InvocationInputEnvelope {kind, scope, body}`. `kind` is the typed
`InvocationInputKind::{Standalone, Derived}`, matching the source variant;
`scope: InvocationInputScope` contains
`task_id`, `invocation_id`, `agent: AgentIdentity`, `constraints_digest`. These
scope coordinates must exactly equal the Task and proposed definition. Server
strictly decodes that envelope/scope but leaves `body` opaque for Agent validation.
Agent owns the body's known schema, request/origin/initialization/artifact binding.
The public read port is
`TaskCoordinator::load_verified_invocation_input_source(&source, &scope, max_bytes)`.
It returns `VerifiedInvocationInputSource {reference, envelope, causes}` so Agent
never re-decodes the Server wrapper. The nonzero host ceiling is at most 16 MiB,
bounding the source record and complete return envelope. Ledger's independent
128 KiB fact-payload ceiling still applies: larger original ModelRequest/full
source must be stored by Agent as Required artifacts referenced by its body,
under the approved 16 MiB archive bound, not silently truncated or refused by
an undocumented 64 KiB context limit. Require 1..=32
distinct exact causal FactRefs; resolve each bounded critical causal record.
For Continuation, causes include its predecessor's exact completed terminal FactRef.
Server must not infer permission, parse private Agent body fields or issue a grant.

Server alone owns namespace and publication:
`TaskCoordinator::publish_invocation_input_source(envelope, causes)` returns the
typed verified result. Its stream is `task.invocation-input-source.<digest>`,
where digest is lowercase SHA-256 of compact serde JSON array
`["kolyan.server.invocation-input-source.v1", task_id, invocation_id]`.
Fact ID equals stream and position is exactly 1; this dedicated domain stream
never contains Task transition events. Agent callers use the returned reference,
never duplicate the coordinate calculation. Exact raw body/scope/kind/causes retry
returns the same record; changed candidate conflicts and cannot replace an admitted
source. Publisher attempts one CAS then reads the exact winner without overwrite.
Both reader and publisher validate unique critical exact causal references,
reject cycles/nonhistorical same-stream references and refuse unresolved closure.
Causal scanning is bounded independently to 1,024 unique records and 16 MiB
cumulative serialized evidence; it neither performs a global audit nor claims
allocation-free journal reads. It uses explicit enter/exit DFS with gray/black
sets, never recursive calls: cycles are refused and a shared DAG node contributes
one record/read/byte charge. The caller's `max_bytes` bounds the source record and
complete returned envelope, not this independent causal-scan budget; a small
return ceiling is not represented as a total-I/O/peak-allocation guarantee.
Publisher is evidence preparation, not permission
to create an invocation after cancellation; actual admission remains authoritative.

Before InvocationAdmitted append, Server resolves the source's exact
stream/position/fact ID, critical kind/schema/subject/scope/causes and validates
the candidate through the ordinary Task reducer. The admission draft includes
the source reference as a cause beside the previous Task fact. The existing
same-stream CAS atomically commits topology and source, so there is never an
admitted invocation awaiting source backfill. Same fact/content retries are
idempotent; changed source is a command conflict. AttemptStarted compares its
required source to the admitted definition and includes that source in causes.
Replay rejects missing/foreign source, inconsistent causes, wrong role variant,
changed attempt source and noncanonical records. Same-Task causal references must
precede their dependent Task event; different-stream positions are not comparable
timestamps. Existence plus immutable causal reference is the boundary, not a
wall-clock claim. Cancellation/terminal admission rejection stays unchanged.

Data-driven evidence must cover preprepared-source admission, concurrent CAS
loss/replay, exact retry, changed/foreign/missing/noncritical source, attempted
late-source backfill, cancellation/terminal rejection, corrupt replay and
fresh store reconstruction. Export all observations before comparison. Agent
additionally checks wrong projection schema/body/ownership and selected immutable
request, including refusal of a physically entered successor followed by late
source publication. Root input proof must contain the real request, and delegated
child source must bind the actual invoke admission, not a manufactured owner-only fact.
Only after the source fence and these gates may the new Runner-finalized long
graph claim source-before-Turn acceptance. Its full and projected named/inline
offline cases precede separately recorded complete real-model matrices.

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

The owner collected exit 101 for projection matrix handle 13015 after 3271.30
seconds. Report `kolyan-r1-matrix-XxGHwT/report.json` contains all 38 terminal rows:
19 Passed and 19 Failed, each with exactly one attempt. All 19 failure details
report ToolTimedOut. MiniMax Anthropic is 0/2 passed, MiniMax OpenAI 2/2,
Qwen Anthropic 12/16 and Qwen OpenAI 5/18. This historical binary used the earlier
case-owned immutable worker copies, not the new run-scoped WorkerRun installation
and readiness protocol. Preserve its original log
`/tmp/kolyan-l5-long-positive-projection-actual-matrix-v1.log`, report and traces;
later readiness, reader or finalizer gates do not retroactively pass these rows.
This matrix also used host `service.complete`, not Runner role-aware finalization.

### Mandatory-source Runner-finalized long graph: offline gate

The migrated host uses Agent's public `prepare_root_input` before Task registration
and invocation admission. It takes the returned snapshot, immutable ownership,
selected request and verified Standalone source; it does not pre-save a competing
root instance or encode Agent-private RootInput/ChildInput JSON. The Task retains
its declared final-node criterion, not Runner.start's root-only auto-finalization.
Every Continuation, in both full and explicit-projection mode, freezes its exact
private initialization and calls `prepare_continuation_input` before admission.
Agent retains the full root/current/source/plan/provenance as Required artifacts;
InvocationAdmitted and AttemptStarted bind the exact returned source. Fresh
Runners repeat preparation with the same evidence without new Task transitions
or physical effects. The old host-only scene assertions remain in the target.

The complete `agent_long_task` offline target finished with owner handle 76100
exit 0: 16 passed, zero failed and five ignored, in 286.67 seconds. Its subsequent
target strict Clippy also exited 0. Logs are
`/tmp/kolyan-l5-long-required-source-full-offline-v1.log` and
`/tmp/kolyan-l5-long-required-source-strict-v1.log`. The ignored network matrices
and independent startup diagnostic are not counted as passed acceptance.

The new Runner-finalized test executes all four independently scoped scenes:

| Selector | Selection | Owner JSONL directory | Independent main JSONL directory |
| --- | --- | --- | --- |
| named | full | kolyan-long-continuation-VZwXp8 | kolyan-long-continuation-jZFAc4 |
| named | explicit projection | kolyan-long-continuation-MJ7JFv | kolyan-long-continuation-sQ5YYM |
| inline | full | kolyan-long-continuation-gwVzGq | kolyan-long-continuation-CQFcJP |
| inline | explicit projection | kolyan-long-continuation-7KIQik | kolyan-long-continuation-f4DODT |

Each directory exports `actual.jsonl`. Each scene verifies one Task/logical owner,
ten admitted invocations (Root plus nine Continuations), 30 actually recorded
model Steps, 20 actual OS tool receipts and eleven wait/rebuild boundaries,
including a wait after an earlier receipt. Each invocation checks its physical
artifact before progressing. Final completion retains and reads back the artifact.
Runner role-aware finalization is called twice through rebuilt hosts: physical
ledger snapshots are taken before the first call, after the first call and after
the second call and must all be identical. The second call also leaves the Task
journal unchanged; the first call may publish its legitimate Task verdict.

The independent main focused gate 94158 exited 0 with one passed test in 166.85
seconds, log `/tmp/kolyan-main-runner-long-offline-v2.log`, using the actual
`--test agent_long_task runner_finalized_long_graph_complete_offline_real_os`
target/filter. Main's v1 log used the wrong target and is not long-task acceptance.
Main also reports full-workspace/all-target strict Clippy handle 75728 exit 0;
this does not replace complete workspace test acceptance.

All these model frames are explicitly offline/scripted; tools and persistence
are real. Positive projection remains Inspect with an Unsupported token counter,
not trusted-token acceptance. The new actual entrypoint
`continuation::finalization::runner_finalized_long_graph_actual_model_matrices`
plans 76 rows (19 deployments × named/inline × full/projection), but has not run.
Network remains on host hold: workspace gate 80663 exited 101, log
`/tmp/kolyan-main-workspace-offline-final-v1.log`, at the missing mandatory source
in the cancellation JSON fixture described below. Preserve the earlier 32/6,
36/2 and 19/19 live reports and all
their failure traces unchanged. This offline gate does not complete L5 acceptance.

#### Affected Server request-source fixtures and pending live scope

`task_cancellation` deserialized a JSON AttemptBinding without its required
input_source; governance/recovery setup and binding helpers contain the same
omission. These Server-level fixtures must publish their actual host request and
explicit topology origin through the public source publisher before admission,
then reuse exactly that admitted reference for each positive or deliberately
patched negative binding. Root is Standalone, child is Derived, and Continuation
includes the exact predecessor completion cause. No default source or imitation
of Agent-private bodies is permitted; preserve every original negative axis and
assertion. Focused cancellation/governance/recovery and all affected offline
targets plus strict all-target checks are pending this migration.

The required-source contract also affects existing Server actual entrypoints:
`task_execution_live_matrix` has nineteen planned deployment rows and
`task_cancellation_live_matrix` has nineteen deployments × two cancellation
policies, thirty-eight rows. These 57 rows need separately recorded fresh live
regression after the offline repair; they are currently not run and on network
hold. They supplement, rather than replace or count toward, Agent's 380-row base
allocation or the separate new 57-row native-effect matrix. Old live successes
cannot establish acceptance of the new mandatory-source binding.

### Stopped cancellation with a committed external wait

The late-child export `kolyan-agent-finalization-late-mJlFrD/actual.jsonl`
contains both ExecutionCancelled and TurnCancelled after a committed compound
external-wait suspension, yet the root is observed as RecoveryRequired.
Evidence inspection currently selects cancellation intent before the physical
terminal and checks only the current resumable suspension, which cancellation
correctly invalidates. Thus a previously committed wait is misclassified as an
unrecorded external effect. This is a Server evidence classification defect.

Prefer an actual Turn terminal over cancellation intent as the stopped source.
For cancellation published at a suspension boundary, inspect the validated
pre-cancellation compound suspension solely as historical effect evidence.
The cancellation must follow that stopped boundary without a new admission,
checkpoint or execution effect. Every unresolved effect still requires its exact
prepared, authorized and externally-waiting publication; corrupt or missing proof
remains RecoveryRequired or an explicit refusal. Cancellation intent alone is
not that historical-terminal exception, and an earlier suspension invalidated
by resumed work must never satisfy it. Do not change the active suspension loader
or authorize resumption, new consumption, receipt fabrication or child rollback.
Add independent data-driven Server tests and rerun the Agent late-child gate.

The fresh joint regression passed all 84 Agent and 106 Server tests, plus doc
tests, with exit zero in `/tmp/kolyan-l5-cancelled-wait-joint-v2.log`.
Strict Agent and Server all-target Clippy also finished with exit zero in
`/tmp/kolyan-l5-cancelled-wait-joint-clippy-v2.log`. The new nine-case export
`kolyan-cancelled-wait-proof-7w2fJ0/actual.jsonl` checks stopped handoff,
intent-only refusal, missing or unknown wait proof, resumed work, a new effect
after intent, post-terminal proof backfill, foreign grant and foreign suspension
scope. Every observation is exported before comparison, and inspection leaves
the ledger unchanged. A cancelled wait never becomes active resume authority.

The joint Agent export `kolyan-agent-finalization-late-t7QtFe/actual.jsonl`
keeps the incomplete-stop case refused and observes the actually stopped parent
as Cancelled after late child completion, allowing the independent finalizer to
record TaskFailed without reviving or consuming for that parent. The separate
whole-Task cancellation retains TaskCancelled; that verdict does not itself
claim every physical execution is terminal. These are offline regression
results, not acceptance of the outstanding real-model or Continuation gates.

A subsequent independent data case publishes a complete StepCompleted after
cancellation intent but before the suspension-boundary TurnCancelled. The old
classifier incorrectly returned Cancelled in
`kolyan-cancelled-wait-proof-81IVSu/actual.jsonl`; its gate exited 101. A late
completion is evidence of activity, even when no new StepStarted appears in that
interval, so it invalidates the historical stopped-wait exception. The new guard
refuses that exception without changing active suspension or receipt semantics.
The original nine cases and assertions remain intact; the tenth is additive.
Full Server regression then passed 109 tests and doc tests with exit zero in
`/tmp/kolyan-l5-cancelled-wait-late-step-server.log`. Strict Agent/Server all-target
Clippy passed with exit zero. The joint Agent run retained 84 passing tests but
failed its newly added Continuation finalization test, exposing a separate reader
schema mismatch; that joint gate is not green.

### Worker readiness and run-scoped immutable installation proposal

Status: design approved for implementation; runtime acceptance remains pending. This design
changes test-fixture/Host assembly, not Turn authority, normal tool deadlines,
macOS security policy or the interpretation of the failed acceptance matrices.
Use the explicitly separate preparation budget below; tool deadlines remain unchanged.

#### Evidence and limits

The complete offline gate `/tmp/kolyan-agent-topology-full-offline-v2.log`
ended exit 101: eleven tests passed and three test targets failed. Those targets
contain four failed data rows, including the new child-approval row. Four actual
worker paths match AMFI/ASP observations for PIDs 60307, 60308, 60309 and 60348.
All four corresponding tool futures were dropped near the original 30-second
deadline without returning an inner result. The earlier two-row offline pass
does not replace this failed complete gate. The queued network gate 79904 was
stopped with exit 130 before test entry; it is not a real-model execution.

The independent startup experiment completed twelve predeclared measurements
in `kolyan-worker-startup-uP28E0/actual.jsonl`. Four existing failed pins, each
launched twice, returned exit zero in approximately 32–50 ms. Two new copies of
the same SHA-256 bytes, each launched twice, exceeded 30 seconds on all four
launches. Their four samples show `_dyld_start` and approximately 96 KiB footprint,
not worker Rust frames. The new copies' actual PIDs 62630, 62659, 62708 and 62772
match AMFI/ASP startup events; deadline cleanup coincides with ASP interruption.

This supports an independent pre-application startup wait for fresh copies on
this host, not a universal inode-only cause: path and inode changed together.
The second launch also timed out, so it is not evidence of a completed assessment
or a warm executable. The experiment uses a direct native read-only request and
regular-file stdin, not the production sandbox/pipe or an effect-less bootstrap.
Its exit-zero test result means evidence/invariants were collected, not that all
workers launched successfully or that the original tool failure was repaired.
Retain `/tmp/kolyan-agent-worker-startup-uP28E0-system.json` and all PID samples.

#### Existing source and API boundaries

`tests/agent/tools/worker.rs::initialize_from` reads the build input once, creates
a new case-owned `trusted-worker/worker`, fsyncs it and writes a SHA-256 manifest.
`verified_worker` reopens that exact case path and rejects symlinks, missing pins
or changed bytes. Recovery correctly avoids the mutable Cargo output, but each
fresh case currently creates another executable inode and path. Root, restart,
delegation, topology, parallel and long-task fixtures use this initializer.

`kolyan-tool-worker/src/main.rs` has no clap dependency or help/bootstrap branch.
It requires exactly four arguments, then builds `WorkerConfig` and calls
`execute_request`. Invalid argument counts return `WorkerError::Configuration`
before stdin consumption or file-operation execution. A failed `--help` or empty
argv invocation is not a supported successful readiness handshake. There is no
executed effect-less readiness evidence yet; do not rename the existing successful
read experiment into such evidence. `WorkerConfig::validate` alone does not prove
the OS can enter the native executable.

#### Proposed preparation and execution phases

1. A trusted test-run/Host installation context reads a selected build input once
   and creates one private immutable installed executable. Persist the installation
   manifest before attempting readiness. Do not hard-link, symlink or repeatedly
   hash/reselect the mutable Cargo artifact during recovery.
2. Launch exactly one native effect-less bootstrap for that installation. Proposed
   explicit worker mode: a sole `--bootstrap-check` argument, recognized before
   normal workspace/limit argument parsing. It receives EOF, no workspace, no
   provider keys, no model data and no file-operation request. The dedicated branch
   returns a bounded versioned JSON success marker and exits zero; it must never
   call `execute_request`, `execute_exact` or initialize a tool workspace/stage.
   This flag and marker are proposals, not current APIs. Invalid modes still fail.
3. The Host verifies exit zero, the exact marker/version, output bounds and the
   same installed path/hash/identity before persisting a Ready observation. Marker
   output alone cannot authenticate a substituted executable. Failure, timeout,
   unexpected output or recorder failure blocks preparation; no model admission.
4. Each fresh case persists its own immutable reference pin to that Ready installed
   executable, then enters normal Runner admission. Every subsequent tool execution
   still performs exact preparation, grant validation, sandbox enforcement and its
   unchanged 30-second tool deadline. Readiness never issues a tool grant or receipt.

The bootstrap has an independently explicit finite Host preparation timeout and
bounded process cleanup, distinct from `Turn` tool execution time. The new test-run
installation dataset supplies a 600-second preparation ceiling; this is a Host
setup allowance, not a larger tool limit or a promise that assessment completes.
Native test executables on this host were observed waiting several minutes before
Rust entry, so the existing 30-second operation deadline cannot represent that
separate preparation phase. A single bootstrap timeout remains a failed preparation,
preserves its evidence and refuses every case's model admission. Export it
as preparation time rather than hiding it in model latency or reporting an enlarged
tool limit. Use one launch, not retries, repeated help calls or a warm-until-success
loop. OS provenance/security assessment can itself have operating-system side
effects; "effect-less" means no model/tool/domain operation, not zero OS activity.

Bootstrap readiness proves that application entry and the small handshake worked
at that observation. It does not promise permanent OS admission, sandbox availability,
future timely tool completion or safe arbitrary operations. A later actual tool
timeout remains a failure under the original deadline and cannot trigger a hidden
bootstrap, retry, unsandboxed fallback or automatic effect replay.

#### Installed artifact and durable case pin

Prefer an explicit suite/run-scoped installation context passed to case Host
assembly, rather than a process-global secret directory or implicit mutable cache.
One run installs once outside all model workspaces and uses the same physical
executable for its cases and reconstructed Hosts. Separate runs install separately;
concurrent owners do not overwrite, re-sign or warm an active artifact. A digest
is a content coordinate, not authority to attach to an arbitrary existing path.

Proposed installation evidence binds a schema version, host-issued installation ID,
exact physical path, SHA-256, observed device/inode and bootstrap observation ID.
Each case persists its own required versioned pin with that exact reference.
Reconstruction checks the manifest, non-symlink path/components, protected ownership,
hash and same observed identity; missing or changed artifacts fail closed. Do not
recopy a current build, choose another equal-hash path or silently reinstall at resume.
Device/inode comparisons are host observations, not immortal object generations.

Keep the installation root private, outside every model-writable root, and explicitly
protected by file and shell assembly. Preserve only the narrow executable-loader
access actually required by the existing sandbox; do not grant general filesystem
access to share a worker. Never place credentials in this root or inherit their
environment into bootstrap/tools. Case state and staging remain independent.
Cross-process recovery retains the installed artifact and pin for the complete
case lifecycle, including approvals; cleanup occurs only after its dependent cases
are no longer resumable. A missing installation after restart is explicit failure.

This replaces, rather than aliases, the case-local executable-copy contract after
review. Existing pin replacement/deletion, approval exact-preparation and one-effect
tests must migrate their fixture installation inputs without weakening assertions.
A changed build input must leave an admitted installation intact; changed installed
bytes/path/identity must still refuse approval before effects.

#### Evidence and acceptance before rollout

Record every installation selection and bootstrap intent before spawn, then the
actual child PID, argv, empty stdin contract, environment policy, UTC observation
times, elapsed preparation time, explicit timeout, stdout/stderr, exit or kill/reap
outcome and available stack/system evidence. Preserve failure records. Recorder
failure refuses Ready. Export every actual model request and all normal tool/ledger
events unchanged, showing that Ready precedes the first model admission. Bootstrap
is a separate Host preparation event, never ModelRequested or EffectReceipt.

Data-driven gates must establish: successful effect-less marker with zero file
operations; readiness failure/timeout/unknown marker blocks all model and tool ports;
one installation/one bootstrap shared across multiple cases; fixed path/hash across
Host reconstruction and approval; unchanged source-build replacement; changed/missing
installed artifact refusal; zero second bootstrap after rebuilding from a valid pin;
and exact sandbox protection of the shared installation from sibling tools.
Unknown Ready evidence and unsupported manifest versions fail without old-format
fallback. Actual startup tests report all planned outcomes without retry selection.

Only after those gates, run a new complete offline Agent gate with
the original case inputs, assertions and tool deadlines. Keep 39465 failed. If the
new full gate passes, start one separately recorded topology real-model matrix for
all configured deployments, then the affected root/restart/delegation/long-task
matrices. A bootstrap pass or two-scene pass alone cannot establish that rollout
fixed the original complete regression or full L5 acceptance.

#### Interpreter controls versus native readiness: proposed test boundary

Status: design approved for implementation; complete-gate acceptance pending.
This refinement changes only fault-control fixtures and the test-owned process
observer. It does not add a configurable production bootstrap launcher.

The focused bounded-control gate 19127 completed exit zero, but complete offline
gates 39042 and 4198 each failed with seventeen tests passing and one failing.
In gate 39042, the copied `nonzero_exit` executable (PID 68367) timed out instead
of reaching its expected nonzero exit. Gate 4198 exported all seven controls
before comparison; `malformed_json` (PID 68773) and `nonzero_exit` (PID 68778)
both timed out with empty output. The remaining controls included a parent that
exited zero with a valid marker while a descendant held its pipes: it correctly
failed at the existing deadline rather than becoming Ready.

The exact failed control paths/PIDs match ASP interrupted waits and subsequent
provenance observations in `/tmp/kolyan-bootstrap-full-v2-fault-system.log` and
`/tmp/kolyan-bootstrap-full-v3-fault-system.log`. A freshly copied executable
script can therefore encounter startup assessment before the intended fault is
observed. This does not establish a permanent security denial or general scheduler
overload. Preserve these failed gates; do not increase their deadlines, retry,
clear attributes or treat the expected-error mismatch as a passing timeout.

Use a private, test-owned explicit `LaunchPlan` with two non-interchangeable
variants and one shared bounded process-observation implementation:

- `NativeBootstrap`: constructed only by the normal readiness entrypoint from
  the verified `InstalledWorker`; program is its exact pinned physical path and
  argv is solely `--bootstrap-check`. There is no interpreter selection, fallback
  or automatic warm call. Reverify the installation before issuing Ready.
- `InterpreterFixture`: available only to fault-control tests, with the fixed
  system `/bin/sh` interpreter and a literal absolute path to a read-only,
  non-executable data script. Record the script's physical path, SHA-256 and
  observed file identity separately from the actual program. Reject symlinks,
  executable/writable scripts and unsupported interpreter selections. Scripts
  remain trusted test data; neither a model nor a provider supplies them.

Both variants clear the environment, receive EOF stdin, use a dedicated process
group and preserve the same bounded stdout/stderr and full EOF-wait deadline.
Cleanup uses group termination and finite nonblocking reap polling. No blocking
capture thread join or unconditional `Child::wait` is allowed. Export direct-child
exit, EOF state, timeout/overflow, kill/reap result, cleanup exhaustion and total
elapsed time through cleanup, not merely the time before capture completes.
OS scheduling, process creation and evidence filesystem operations are not
hard-real-time guarantees; expose failure to reap instead of claiming completion.

The shared observer returns typed process observations, never `ReadyEvidence`.
Native readiness alone validates the exact marker, clean exit, complete capture,
deadline and installed identity and persists Ready. Interpreter controls interpret
their observations against dataset expectations and can never create Ready,
installation authorization, tool receipts or model admission—even if their script
prints the exact native marker. Use distinct observation kind/event provenance
and export actual program, argv and script digest before spawn, then PID/timing.
Do not label an interpreter execution as an actual native-worker bootstrap.

The control dataset owns scripts, existing expected errors, preparation/cleanup
and output bounds, total elapsed bound and direct-exit expectations. Retain the
one-second fault deadline and the existing comparisons. Export every planned
case's complete observation before comparing any case. Independent tests must
prove that valid-marker interpreter output does not yield Ready, actual recorded
program/argv differ from native mode, changed scripts fail before spawn, recorder
failure refuses launch, and direct-child exit with inherited descendant pipes
cannot make total capture unbounded. Keep the actual native success/reconstruction
tests separate; interpreter success cannot substitute for native acceptance.

After main review, implement the shared observer and data-script controls, then
run the complete offline Agent gate and strict Clippy. Start no new actual-model
matrix until that complete offline gate passes. Normal native preparation remains
one launch with its approved 600-second setup ceiling; normal tool execution
remains thirty seconds, with no new retry or fallback.

### Recursive invocation and multiple approval real model results

The complete topology matrix covered nineteen configured Provider/protocol/model
deployments with two scenarios each: two nested self-calls, and two delegated
children whose independent approvals survive host reconstruction. Every row ran
once. The report contains 32 Passed and 6 Failed rows, with no pending or skipped
rows. This is partial acceptance, not a successful complete L5 gate.

Evidence is retained in
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-r1-matrix-pTS8aS/report.json`
and `/tmp/kolyan-agent-topology-readiness-live-v1.log`. The execution ended with
exit 101. Each trace below is the `actual.jsonl` under the retained
`kolyan-agent-topology-<trace>` directory in that same temporary evidence root.

| Deployment and scenario | Trace | Observed failure |
| --- | --- | --- |
| MiniMax OpenAI compatible, nested self-calls | IhVXrQ | Generated `named_targets` as a string despite an array schema |
| MiniMax OpenAI compatible, two approvals | 7JEtBR | Parent called `file.read` instead of delegating, producing a parent approval rather than the required child waits |
| Qwen3.7-max OpenAI compatible, nested self-calls | NeAopa | Generated `agent.invoke` in the environment tool permissions, which permit only read, write, edit and shell |
| MiniMax Anthropic compatible, nested self-calls | Ih1ySx | Generated string `named_targets`, refused by strict decoding |
| MiniMax Anthropic compatible, two approvals | Eo5LhA | Generated string `named_targets`, refused by strict decoding |
| Qwen3.8-flash Anthropic compatible, nested self-calls | 0Go6lL | A later generated delegation request exceeded its effective permission ceiling |

These six traces do not show Worker startup failures. Preserve the failed rows,
raw requests, responses and neutral calls; do not coerce wrong argument types,
expand permissions or classify a parent approval as a child approval to pass them.
Independent recorded-output tests must distinguish schema rejection, permission
rejection and incorrect scenario routing. The existing schema examples were
extended from nineteen to twenty-one rows with separate string and null
`named_targets` cases; both correctly fail schema, decoding and preparation.

This matrix used the compiled implementation from before mandatory immutable
input-source admission. It cannot validate the newer source binding. Complete
the new contract migration and its offline gates before launching the affected
fresh real-model matrices. Keep historical failures distinct from later results.

### Independent native effect, cancellation and recovery acceptance proposal

Status: approved-partial for implementation of receipt-after-cancel,
lost-receipt uncertainty and RootOnly late-child native effects. The host approved
the existing public receipt/admission seams; native-active PID observation and
any new production seam remain unapproved. None of the four native gaps has been
executed or accepted by this proposal. This is a separate test scope from the
matrix-entrypoint audit. The fresh strict-child Root regression has owner-confirmed
exit zero: 22 tests passed, one historical startup diagnostic ignored, and seven
actual-model entrypoints filtered out (`/tmp/kolyan-agent-strict-child-root-offline-v1.log`,
handle 85985). Root strict Clippy also exited zero
(`/tmp/kolyan-agent-strict-child-root-strict-v1.log`, handle 41702).
Neither result establishes the new native cases or authorizes network acceptance
while the complete migrated long-task suite remains pending.

#### Contract and existing observation seams

Keep durable command intent, active execution control, physical effects, effect
receipts and Task verdict separate. `DurableTurnDriver::cancel` persists execution
cancellation; Runtime checks that fact at its existing admissions/boundaries.
It does not discover a running worker PID or deliver an immediate cancellation
token to that worker. Active-Step interruption requires the host to deliver the
existing `TurnControl`. A command acknowledgement is not a stopped-process fact,
and neither path guarantees rollback of an already committed atomic rename.

The test may decorate existing public ports, without replacing their effects:

- A test-only `ToolExecutor` delegates preparation and forwards the original
  `ToolInvocation`, including independent scope, grant, revision and control.
  It may acknowledge entry before forwarding or hold the actual returned
  `ToolOutcome::Completed` before returning it to Runtime. It must not construct
  a substitute result, change arguments, issue grants or retry the call.
- A test-only `LedgerStore` delegates the required queries, claims and atomic
  admissions to the same real SQLite store. An explicitly selected exact receipt
  append may fail before publication with `LedgerError::Storage`, or acknowledge
  successful durable publication and then synchronously publish cancellation
  through the ordinary Runtime API before returning. Never block an async thread
  waiting for another task from inside this synchronous store port. Fault matching
  uses the independently saved execution/Step/call/scope and event identity, not
  a loose tool-name or message-string predicate.
- Host scheduling may await bounded one-shot acknowledgements and release gates
  around existing admission/receipt boundaries. Gates are deterministic ordering
  controls, not OS sleeps or guessed elapsed-time triggers. A failed acknowledgement
  is a fixture failure and must release/drop owned work and record cleanup.

Every native effect still uses the production isolated tool factory, verified
run-scoped installation and persisted case pin, exact preparation/grant, real
Seatbelt worker and unchanged thirty-second tool limit. Rebuilt hosts verify the
same input source, installation evidence, snapshot and execution coordinates.
Bootstrap remains the separate single-launch preparation operation; it cannot
be called to recover a failed effect. Counters remain Unsupported in Inspect;
these tests do not establish trusted token-budget admission.

#### Four independently reported scenarios

| Dataset case | Deterministic injection and required evidence | Acceptance oracle |
| --- | --- | --- |
| `agent_running_native_worker_cancel` | Keep distinct probes for durable cancellation at admission and host-token delivery to an active executor. Full native-inflight coverage additionally requires attestation of the actual file-worker PID/start before cancellation, and stop/reap evidence afterward. | Intent-only may not be reported as physical stop. A pre-entry cancellation prevents forwarding to the worker. An entered effect must be inspected for actual file state and receipt/uncertainty; no rollback or clean-stop claim without proof. |
| `cancel_after_committed_native_receipt` | Let the real worker complete a Write. Delegate its exact successful receipt append, retain the committed cursor/payload, then publish cancellation before returning that append to Runtime. | Physical bytes/hash and the one original receipt remain. No automatic cleanup rollback, second write, extra model request or new authorization occurs on reconstruction/recovery of the cancelled execution. Inspect the durable terminal/stop evidence independently of intent. |
| `native_commit_without_receipt_uncertain` | Observe the actual successful native Write result and physical bytes. Refuse its exact receipt publication before delegating that append; all prepared/authorized/started facts remain real. Preserve the actual result as test observation, not a durable receipt. | Missing receipt after Started is uncertain/recovery-required or an explicit recovery refusal, never a fabricated success or rollback. Rebuild against the unchanged store without fault injection; recovery cannot replay the worker or ask the model to recreate the effect. No synthetic receipt or trusted reconciliation is supplied. |
| `cancelled_parent_late_native_child` | Admit a real child and persist the parent's exact external-wait suspension. Hold the child's original invocation at the host forwarding boundary. Publish RootOnly Task cancellation and drive the parent's existing cancellation boundary to an actual stopped terminal, then release the child to its real native Write. | The child retains its own scope, receipt and physical effect. RootOnly does not imply child cancellation. The cancelled parent never resumes, consumes the late child result, polls a model or changes TaskCancelled to success. Repeated read-only finalization verifies the existing verdict without treating it as proof all children were physically stopped. |

The receipt-loss case injects persistence failure after an actual completed
worker operation; it is not a process crash or evidence that the OS rolled back.
The late-child case intentionally selects RootOnly and an already admitted
child. AllInvocations, child-only cancellation and arbitrary shell crash windows
are separate scopes, not implicit coverage of this row. If an existing API
refuses the intended boundary operation, preserve its exact typed failure and
report the unmet scenario instead of substituting a fabricated terminal/result.

#### Unresolved native-inflight observation boundary

Current `IsolatedFileTools::execute_bounded` constructs its sandbox internally;
the public ToolExecutor and SandboxExecutor results expose no file-worker PID
or post-spawn acknowledgement. `EffectStarted` precedes private re-preparation,
worker-plan construction and process spawn. A polled executor future or a host
entry gate therefore cannot attest that the native file worker is running.
The outer observer's Drop timing cannot prove that its internal reaper finished.

Within the instruction to add no production support capability, the first row
must remain a required gap at that precise level. Implement its admission/control
probes as separately labelled partial coverage, not as a passing native-inflight
case. Existing Sandbox process-group cancellation tests remain process-only
evidence. Do not add a worker pause flag, substitute a slow fake worker, use an
OS sleep to guess spawn, or infer a PID from unrelated system-log activity.
Main must review any future observation-only seam before production changes;
this proposal does not authorize one. The other three rows can proceed using
the existing public host receipt/admission seams after review.

##### Observation-only lifecycle port (approved-partial implementation)

This is a production process-diagnostics/cleanup observation boundary, not a
test-only executor or an authorization port. Approved implementation is a concrete
bounded nonblocking channel (`try_send` plus an explicit dropped counter), optional
host injection in Sandbox/Tools, and genuine shell-native cancellation evidence.
Ordinary file-worker protocol changes and file-native-inflight claims remain out
of scope. The callback-shaped sketch below is superseded by the channel contract:
no arbitrary synchronous observer code runs inside the process owner. Sandbox owns
process lifecycle observations; Tools owns
their exact invocation correlation; the host owns collection and existing active
control delivery. Neither Agent/Core nor a new model executor launches processes.

Suggested narrow contracts, with final Rust names subject to owner review:

```rust
// kolyan-sandbox; concrete sender/receiver, bounded at construction.
fn sandbox_process_observation_channel(capacity: usize)
    -> Result<(SandboxProcessObservationSender, SandboxProcessObservationReceiver), SandboxError>;
// Sender uses try_send only. Receiver/sender expose the shared dropped counter.
// A distinct launch identity survives PID reuse; timestamps are diagnostics.
// Each event carries the same launch identity and actual PID/process-group ID.
enum SandboxProcessEvent {
    Spawned { /* launch, pid, pgid */ },
    OutputObserved { /* launch, stream, bounded original bytes */ },
    CancellationObserved { /* launch, explicit-control or dropped-future */ },
    TerminationAttempted { /* launch, actual signal result */ },
    Reaped { /* launch, successful wait status incl raw exit/signal */ },
    CleanupFailed { /* launch, bounded actual wait/group/capture failure */ },
}
// Sandbox construction may accept an optional observer; the ordinary admission,
// process owner and execution algorithm remain the only execution path.
// Tools host injection selects a scoped sink using existing exact identity:
// Tools accept the sender at host assembly and bind a bounded strict context
// containing tool name, independent ToolExecutionScope and prepared digest.
// No callback factory, authorization result or worker pause is introduced.
```

The Tools factory receives independently verified host scope/prepared digest,
not model fields or scope copied from a grant. The sink is attached to the exact
internally constructed sandbox immediately before normal launch; it cannot
replace the executable, arguments, limits, policy, grant or result. No readiness
decision, pause acknowledgement, retry or permit is returned. Prefer optional
host injection to new fields in every SandboxRequest. Observer availability does
not affect preparation digest or grant semantics because it carries no authority.

`Spawned` is emitted only after successful `Command::spawn` and installation of
the process-owning cleanup guard, with the actual direct-child PID and process
group. Even observer failure must not leave an unowned spawned child. In the
current backend the spawned program
is `/usr/bin/sandbox-exec`; this fact alone proves neither successful exec of the
pinned file worker nor native operation entry. `OutputObserved` retains bytes
read from the actual stdout/stderr pipes without removing, rewriting or replacing
the ordinary result. Each pipe preserves byte order; cross-pipe arrival order is
not a causal oracle. Observation chunks must share the existing output bound;
there is no independent unbounded buffering or new stdout allowance. The sink
uses nonblocking bounded delivery; missing/dropped observation makes the test
inconclusive/failed, not the native effect retriable. Sink panics must not unwind
through the process owner or skip cleanup; surface diagnostics separately without
fabricating an execution outcome. Production deadlines and authorization stay
unchanged; a recorder must never suspend the worker to create a timing window.

`CancellationObserved` records the reaper's actual observation of existing control,
separately from the host's earlier cancellation publication/delivery. A successful
kill call is only `TerminationAttempted`. `Reaped` requires successful actual
`Child::wait`/equivalent successful reap, including on the independent dropped-
future cleanup path; callback completion or a destructor starting is not reap.
An unsuccessful wait yields `CleanupFailed`, never `Reaped`. Leader reaping does
not independently prove every descendant is stopped: preserve the separate actual
group-cleanup result and capture completion. No task/receipt/Ready fact may be
manufactured from an observer event.

###### What can be witnessed without changing the worker protocol

Ordinary `kolyan-tool-worker::execute_request` consumes and validates bounded
stdin, calls `execute_exact`, and outputs only the final result. The separate
`--bootstrap-check` response proves entry of that separate launch, not readiness
or operation entry of this invocation. `execute_exact` uses prepared regular-file
identities and nofollow access; substituting a FIFO/device or fake pausing worker
would violate the actual file contract. There is therefore no existing ordinary
file-worker message that can serve as a deterministic native-operation-start
acknowledgement. Larger input, elapsed time or post-spawn PID inspection do not
fix that causal gap. Do not claim complete file-native inflight acceptance from
this observation port alone.

A separate genuine shell-native case can use the existing tool and unchanged
schema/production path. The data-owned real command first performs a genuine
bounded builtin arithmetic operation, prints one exact bounded phase marker with
`/bin/sh`'s builtin `printf`, then continuously executes builtin arithmetic work
(for example `while :; do n=$((n+1)); done`). The marker follows actual command
work, not a synthetic acknowledgement before forwarding to the tool. No sleep,
fake worker, injected pause,
subprocess fallback or changed tool deadline is used. An actual matching output
observation proves the shell reached that command phase, not file-worker readiness
and not absence of later natural failure. Since the command has no intentional
normal exit after the phase marker, host control can be delivered causally after
the observed phase. If it nevertheless exits before cancellation, export that
outcome and refuse the inflight claim; never select a successful retry. This row
is labelled shell-native evidence and cannot silently satisfy the file-worker row.

Full file-native coverage needs a separately reviewed ordinary-worker progress
protocol at a truthful boundary: `RequestValidated` is not operation entry; opening
a descriptor is not successful content I/O; a positive read/write progress marker
is not commit. Even progress delivery cannot guarantee the fast operation has not
already finished when host cancellation arrives. Without an actual naturally
waiting supported operation or stronger truthful evidence, deterministic file-
inflight cancellation remains unresolved. No acknowledgement gate delaying normal
file execution is proposed here.

###### Proposed causal data-driven acceptance

Add independent dataset rows for pre-spawn cancellation, spawn-only/no operation
witness, shell phase followed by durable cancel plus existing TurnControl delivery,
natural exit before cancellation, observer loss, and dropped-future cleanup.
The host awaits bounded notifications rather than estimated sleeps. Capture
exact scope/prepared/grant, installed worker identity where applicable, actual
launch/program provenance, original output, phase observation, durable cancel
fact, control delivery, reaper cancellation observation, signal result, successful
reap/raw status, group-cleanup/capture result, physical effects and full durable
terminal/receipt evidence. A recorder loss or timeout must still request cleanup
and export its actual result before comparison. Keep physical stopped evidence
separate from effect rollback; preserve missing-receipt uncertainty and forbid
model/tool replay on reconstruction. A forged phase string from a Provider or
unrelated process cannot satisfy the exact scoped pipe observation. Export all
rows, including the explicit file-worker gap, before asserting expectations.

Ownership after approval: Sandbox owner implements lifecycle/capture observation;
Tools owner implements scoped host injection across file/shell/combined assembly;
native integration owner implements the data and causal framework. Agent merely
uses the same production tool factory. This document update is design evidence,
not implementation or acceptance of native-inflight cancellation.

###### Agent shell-native integration flow (approved implementation scope)

The independent `tests/agent/native_running.rs` module and its owned subtree/data/
expected files compose the actual AgentRunner, SQLite journal/Ledger, Session store,
verified run-scoped worker installation and production IsolatedToolSet with the
new bounded observer. Root registers only this module; the existing native-effects
scenarios and their assertions are unchanged. Offline Provider frames are data;
a future live run supplies a real Provider without rewriting generated calls.
No live acceptance is implied by the offline gate.

A forwarding ToolExecutor decorator captures the original invocation's control
and exact prepared/grant/scope for host observation, then forwards that very
invocation unchanged. It cannot create a permit or replace control/scope/arguments.
The phase witness must be bytes from the exact scoped sandbox stdout observation,
with one actual launch/PID identity and matching independently captured digest;
Provider reasoning/text containing the same marker cannot satisfy it.

Four separately classified causal rows are required: (1) actual shell phase,
ordinary Runtime durable cancellation, original control delivery, await original
execution, reconcile and proof-only finalization; (2) drop the actual root future
after the phase, await independent process cleanup observations, then inspect/
reconcile without claiming TaskStopped from reap; (3) deliberately unavailable
observer delivery records explicit loss and refuses an inflight claim while still
preserving the real execution outcome; (4) allow the real root to finish before
consuming the phase observation and record natural-exit-before-control, never
retroactively label it cancelled. The latter two are negative observation oracles,
not successful inflight cancellation. Neither retries a model/tool or selects a
passing sample.

Capture raw requests/model deltas, original invocation/control handoff, process
observations/loss, host intent/control causal order, physical bytes/hash, complete
durable facts and source artifacts. Rebuild the host against the same installation
and stores and repeat read-only reconciliation/finalization, checking zero new
model/effect calls. For the dropped-future row, missing durable terminal/receipt
must remain a typed recovery/finalization refusal or uncertainty even though the
direct child was successfully reaped. Export every row before any scenario/expected
trace comparison. File-native operation-entry/interruption remains an explicit gap.

Phase and cleanup observation deadlines are independent fixture bounds. Dropping
the root after a missing/invalid phase still drains verified lifecycle events to
reap, group cleanup and both capture completions, or exports explicit unproven
cleanup. Invalid-context events are exported but never admitted as proof; they do
not bypass delivery of the original cancellation control or future-drop cleanup.
The actual Core cancellation path may drop its tool future after host control
delivery. In that case the sandbox truthfully records `dropped_future`, not a
fabricated `explicit_control` event; durable intent and Turn terminal evidence
remain separate mandatory assertions for the cancellation row.

The first offline trait-entry failure wrote the actual physical effect but
exported no process observations, then hit the phase deadline without publishing
durable cancellation. The root factory already supplied its sender. Source
inspection identified a production wiring omission: inherent Shell `execute`
attached the observer after grant validation, but the independent `ToolExecutor`
implementation constructed a new sandbox without attaching it. Neither marker
matching nor Provider decoding can explain the missing Spawned event. The fix
binds the same independently supplied scope/prepared digest after exact trait
grant validation, without changing invocation, authority, deadlines or output
limits. A separate real-macOS dyn-ToolExecutor regression must observe native pipe
phase before original-control cancellation, then verify actual reap/group/capture
and physical bytes. It is independent of the existing inherent-execute tests;
the Agent four-row gate verifies the complete durable cancellation path above it.

###### Actual-Provider native-running matrix (approved test-only scope)

The owned `native_running` subtree adds an explicitly ignored actual-Provider
entry using the existing deployment registry and production Provider builders:
all 19 configured provider/protocol/model combinations times the four existing
causal cases, 76 planned rows. The independent inventory exports `planned`, zero
attempts and `network_executed:false`; registration and offline gates are not
network acceptance. The live path clears scripted frames and supplies the real
Provider to the same Runner/production IsolatedToolSet assembly. Model-generated
calls, IDs, arguments and events are forwarded unchanged. Do not rewrite a
candidate, select successful samples, add implicit retries or silently omit a
configuration. Missing credentials, absent/wrong/repeated calls, natural early
exit, observation loss, clock expiry and Provider errors remain recorded failures
for the appropriate positive case. Loss and natural-exit rows remain explicitly
negative inflight oracles, with real effects and execution results still checked.

Three independent fixture clocks are mandatory. Initial model/invocation waiting
starts at root execution; every actual Provider open+stream also receives its own
model-wait bound, including final-answer requests after tool return. Fixture
timeout errors retain a separate fixture classification, not a fabricated SDK
diagnosis. The phase deadline starts at the original invocation capture, not
before model inference, and cannot be restarted by repeated calls. Capture
notification is test-local, nonblocking and carries no authority or permit. The
cleanup deadline starts at host cleanup independently of both prior clocks.
Phase failure must still cancel the original control or drop the original future
and export actual cleanup or explicit unproven cleanup. No clock changes the
production 30-second tool deadline, output bound, grant, scope or admission.

The live semantic oracle does not demand the offline fixed model request count.
It still requires one original environment invocation, exact scoped native pipe
phase and launch, physical content, unchanged call forwarding, cancellation
causality where applicable, actual lifecycle cleanup and zero new model/effect
work during reconstruction. All raw requests/events/errors, source artifacts,
Ledger/journal, physical bytes and per-row outcome are exported before assertions;
the matrix retains every failed row and continues the remaining plan. New focused
offline/clock tests and strict checks require a fresh whole-workspace gate; the
earlier frozen workspace snapshot is not evidence for these new files. Actual
network execution remains separately authorized, and file-worker native-entry/
interruption evidence remains open; shell results cannot substitute for it.

#### Data, trajectories and staged verification

Proposed independent files are `tests/agent/native_effects.rs` and its test-only
submodules, with `tests/fixtures/agent/native_effects.json` and
`tests/expected/agent/native_effects.jsonl`. Keep case IDs aligned with the
existing coverage inventory; update that inventory only with real declaration
paths and separately verified execution evidence. The dataset owns native
requests, seeded workspace state, selected fault boundary, cancellation policy,
gate/cleanup bounds and semantic expectations. It must not silently alter old
case data or add a retry loop. Native-running capability absence is an explicit
coverage limitation, not a skipped case represented as Passed.

For every planned row retain source/prepared requests, input-source/archive
references, provider text/reasoning and usage, original calls/results, independent
scope/grant, gate acknowledgements, exact fault identity, cancellation intent and
host-control delivery, immutable pin/readiness evidence, full Ledger and journal,
and independent physical file bytes/hash. PID/start/stop/reap fields are required
for the native-inflight claim and otherwise explicitly unavailable, not zero or
inferred. Record reconstruction/recovery results and effect/model invocation
counts. Write and read back every row's actual JSONL, including failures, before
comparing the complete dataset. Assertions check causal facts and exact authority,
not nondeterministic prose or elapsed-time guesses.

Implementation sequence after review: receipt-preservation; lost-receipt
uncertainty; RootOnly late-child recovery; then the separately labelled admission
and host-control probes with the native-inflight gap retained. Run the complete
fresh offline Root regression and strict target gate after new fixture assembly.
Only after main accepts the complete migrated suite may applicable network rows
be added to the current configuration-driven matrix. Models must generate the
calls there; deterministic host fault injection is still allowed, but offline
scripted Provider frames never count as actual-model evidence. This document and
an inventory audit alone complete none of those execution gates.

#### Approved native public-seam offline evidence

The owner collected exit zero for native gate 12583 after the new fixture's
TaskState spelling was corrected to the existing typed wire values. The initial
63528 failure remains in `/tmp/kolyan-agent-native-effects-offline-v1.log`;
all three rows exported before its first comparison. The corrected local gate
is `/tmp/kolyan-agent-native-effects-offline-v2.log`. Main independently confirmed
81650 exit zero and read all three exports ZDovep, 7QqFLN and qsREjC.

The subsequently strengthened complete fresh Root offline gate 76811 exited zero
with 23 tests passing, one historical startup diagnostic ignored and seven network
entrypoints filtered out (`/tmp/kolyan-agent-native-effects-full-root-offline-v3.log`).
Its native rows compare the receipt-before-cancellation cursor, exact physical
content/hash, one actual native execution, uncertainty, zero result consumption
and unchanged effect/authorization/model counts after rebuilding the host.
The owner-confirmed strict Root Clippy gate 27173 also exited zero
(`/tmp/kolyan-agent-native-effects-root-strict-v3.log`). Its matching complete
offline native exports are `kolyan-native-effect-yGZfL5/actual.jsonl`,
`kolyan-native-effect-a8WnY6/actual.jsonl` and
`kolyan-native-effect-NjAMp0/actual.jsonl` in the retained OS temporary directory.
The receipt-loss row remains RecoveryRequired, and finalization refuses a success
verdict. The RootOnly row has a real suspended-parent TurnCancelled before the
child is released; its native receipt stays child-owned and parent consumption
is refused. The implementation uses the existing Session stopped-suspension
path before whole-Task cancellation, because Task command intent alone does not
publish that physical terminal. No native-running PID/start/reap claim is made.

These are real OS worker effects with scripted Provider output, not actual-Provider
acceptance. The coverage inventory now points to the three concrete declarations
and still retains the native-inflight gap. Its static audit does not execute them.

#### Additional actual-Provider native fault matrix (approved scope, not run)

Add an independent entrypoint planned as
`native_effects::actual_model_native_effects_matrix` in `agent_root` for every
configured deployment and each of the three approved native fault cases: nineteen
deployments times three cases equals 57 new rows. These rows supplement, never
replace, the prior complete matrices; one attempt per row, new immutable run
installation and fresh stores. Network is held until main accepts the complete
migrated workspace gate 80663 and authorizes its execution. This paragraph defines
scope, not an assertion that this new entrypoint is already implemented or run.

Keep the same observation/comparison framework and production Runner/services/OS
tools. Data must contain distinct root-write and parent-delegation objectives,
exact target path/content expectations and named child identity/permission input.
The actual Provider produces all root/child ToolCalls. Do not send the fixture's
offline call ID, insert a call or manufacture Completed. The root-write cases
select the actual validated write invocation under its independently admitted
scope; the late-child case selects the actual child write only after a real
parent external wait exists. Bind the host fault once to that invocation's native
call ID, prepared digest, exact scope/grant and receipt identity. Required path
and content remain strict; optional spelling or generated IDs cannot authorize
another target/effect. Invalid routing, wrong arguments and extra writes are
failed rows, never coerced outputs or hidden retries.

Live Provider opening, request planning and descriptors must delegate to the
actual configured Provider. Preserve raw neutral requests and all stream events,
reasoning, partial responses, reported usage/cache fields and errors. Use Inspect
with the honest Unsupported counter; no estimated-token admission fallback.
Missing credentials, transport failures, schema failures, unexpected approvals,
missing child waits or fault boundaries never reached are explicit failed rows.
Export failure evidence even when there is no receipt or model terminal.

Do not impose the scripted case's exact number of model Steps on arbitrary live
reasoning. Retain exact expected native effect/receipt/consumption counts and
unchanged request/effect/authorization counts across the recovery interval.
Physical stop is checked using the same real stopped-suspension path, not inferred
from intent. The host may inject the approved receipt-publication failure or
cancellation only after the selected real boundary is observed. All rows export
before assertions; a missing boundary cannot be reported as NotRun or Passed.

The prior inventory currently contains 418 rows (nineteen times twenty-two),
whereas main's intended fresh base allocation is 380 (nineteen times twenty).
Retain all existing inventory declarations and have main explicitly identify its
execution subset; do not silently remove one 38-row historical/baseline entry.
The additional native scope is always 57: 437 rows if main explicitly selects
380 base rows, or 475 if it executes the complete 418-row inventory. Neither
number is a run result. The affected plan separates this planned, unregistered
addition from executable existing entrypoints until implementation and its fresh
offline/strict gates are complete.

An independently approved optional nonblocking Sandbox/Tools observer and native
Shell port are being designed by their production owner. After the public API is
frozen, add distinctly labelled shell-native cases using the same test framework.
Shell PID/stop observations cannot satisfy the file-worker native-inflight gap;
keep both capability claims and evidence identities separate.

#### Native fault causal assertions and actual Provider entry implementation

The first receipt-preservation scenario cancels a Runtime execution, not the
whole Task. It must return a typed `TurnError::Cancelled` through the Runner's
execution error chain, with exactly one actual `TurnCancelled`. Its committed
receipt cursor is strictly less than `ExecutionCancelled`, which is strictly
less than the stopped Turn cursor. The Task is `Failed`, not `TaskCancelled`:
no coordinator Task cancellation was requested. An error string alone proves
none of this. Older artifacts without these fields are retained historical
observations, not acceptance of the strengthened contract.

The lost-receipt row requires exactly one `EffectStarted` followed by exactly
one `EffectUncertain`, no `EffectReceipt`, no `EffectReconciled`, and no
`TurnCompleted`. A physical write does not imply durable success. The RootOnly
row must observe the real parent's stopped Turn before calling coordinator
Task cancellation. In the journal, the exact stopped observation position must
precede the Task cancellation position and be its causal reference. Ledger
cursors and journal positions are separate coordinate systems, never compared
to each other. The late native receipt remains bound to the independently
verified admitted child's execution and the entire actual issued scope.

The independent `native_effects::actual_model_native_effects_matrix` entry now
exists and is registered in the affected inventory: nineteen configured actual
deployments times these three original cases, one attempt per row. This is
implementation, not a claim of actual network acceptance. Its live source
delegates the unchanged model request and every actual stream event/error to
the configured Provider; it neither consumes script frames nor inserts calls.
Building the live port does not open a stream. Inspect retains the unsupported
counter with no token estimate; the dataset's 32768 inspection window is an
explicit host assumption, not trusted model-specific token admission.

The dataset separately declares strict write arguments and a parent-delegation
objective. Fault selection binds the first actual prepared write, generated call
ID, exact independent execution/snapshot, digest, grant, and receipt identity.
Wrong arguments/routing and repeated writes are explicit refusals and fail the
row, never repaired or silently deduplicated. Before child drive, strict original
`AgentInvokeInput` must equal the declared named target, private input, write-only
permissions, empty delegation ceiling and serial request; actual Runner wait
verification supplies the admitted child's identity. Reconstruction opens the
same stores and immutable worker reference, and must cause no additional effects
or model requests.

The live child orchestration wait/drive allowance is declared as 600000ms in
the dataset to accommodate actual Provider calls. This is separate from the
unchanged 10000ms held-admission handshake and 30000ms native tool deadline;
offline orchestration retains its 40000ms drive bound. There is no tool deadline
increase, network replay, retry or native warm loop. All rows persist actual
requests, events, physical state, ledger/journal and failure observations before
any row comparison. New entry registration does not authorize network execution:
fresh complete offline/strict and main's migrated workspace release are required.

Owner verification for the causal/actual-entry implementation (no network):
focused handle 18855 exited 0 with four tests; the subsequent complete Root
handle 81653 exited 101 with 27 passed, one failed, one ignored and eight
filtered. Its log is `/tmp/kolyan-agent-native-actual-entry-full-offline-v2.log`.
The native-effects three-row report and all four independent unit tests passed,
as did the inventory audit. The remaining failing test belongs to the separately
implemented native-running shell module; this is not a complete Root acceptance.
Strict Root handle 36516 exited 0, with log
`/tmp/kolyan-agent-native-actual-entry-strict-v2.log`. Actual Provider entry remains
unexecuted; neither these unit tests nor real OS scripted Provider rows prove
the 57 actual-network rows.

The new three native JSONL artifacts are `kolyan-native-effect-A0t8jP`,
`kolyan-native-effect-YRTeZV` and `kolyan-native-effect-v7e4tA` under the retained
OS temporary directory. The first has receipt/cancel/stopped cursors 17/18/21,
one stopped Turn, typed returned cancellation and Task Failed. The second has
Started/Uncertain cursors 16/17, zero receipts/reconciliation/TurnCompleted and
Task RecoveryRequired. The third has root stopped cursor 40, stopped observation
journal position 7 preceding Task cancellation position 8 with its exact causal
reference, and one late native receipt in the independently verified child scope.

The failed full-v1 log and artifacts remain unchanged. In `lcw2Kw` and `ulWb0i`,
the faulty test predicate looked for nonexistent `input.grant` rather than the
actual top-level `prepared_grant`. Actual matching receipts were present, injected
fault counts were zero, the Runner returned no start error and Task Completed.
This establishes a test-owned receipt-contract error, not a model failure or
inferred scheduling race. The corrected predicate preserves exact grant equality
at the real field, plus original call ID, prepared digest and full scope equality.

### Worktree self iteration acceptance

The human authorized a separate worktree experiment: Kolyan itself implements a
small capability, then the host independently verifies and reviews it before any
mainline integration. This supplements long-task acceptance; it does not replace
the configured deployment matrices or authorize automatic merging.

The candidate is a pure `InvocationState::is_terminal` query, matching exactly
Completed, Failed and Cancelled. Admitted, Running, Suspended and RecoveryRequired
are not terminal. It must not change transitions, retry admission, authorization,
serialization or Task success rules. The host checked that this helper does not
already exist. All seven states require separate data-driven assertions in a
test file, not production inline tests. The allowed patch is limited to the type
module, its explicit test module declaration and an independent test/data file.

Decompose the task into inspection and planning, implementation, test addition,
and review of independent validation. A real configured Provider generates every
read/write/edit/shell call through production bound Agent factories and isolated
tools. The existing public API chain is Runner input-source preparation, bound
Provider and tool factories, Server task admission and execution, then Runner
finalization. There is no invented Runner continuation-start method. This task
withholds Shell rather than falsely treating its workspace-write authority as
read-only. A Codex subagent may implement the test host, but must not author the
candidate patch or fabricate Kolyan tool calls. Restore a fresh Runner from the
same durable stores at stage boundaries; where a Continuation is used, preserve
the predecessor source and completion proof rather than copying model prose into
an unbound request. Retain actual requests, reasoning, calls, results, physical
file digests, journal/ledger and stage outcomes before comparison.

The worktree starts from main's exact Git revision plus a copied, frozen source
snapshot of the in-flight implementation. Record that baseline separately from
Kolyan's candidate diff. Copy only tracked and nonignored project files; never
copy credentials, build outputs or Git administration. Never commit the inherited
large migration as the small candidate patch. Git control, worker installation,
state stores and credential handling remain host-owned and inaccessible to tools.

Model tools retain the existing sandbox and production deadlines. Do not expose
the home directory, Rust credentials, package registries or network merely to
make Cargo work inside Shell. The host runs fixed allowlisted Cargo checks after
the candidate is complete and supplies their real exit status and bounded logs
as explicitly host-produced input for the final review stage. Kolyan may perform
workspace-local shell inspection, but cannot issue Git commit, merge or push.

Keep committed case data separate from run configuration. Worktree/private paths,
branch/revision and frozen baseline pins belong to a project-local ignored
`tests/config/*.local.json` file, explicitly selected by
`KOLYAN_SELF_ITERATION_RUN_CONFIG`; never discover them through a global config or
silently fall back to another checkout. Model credentials remain environment-only.
The host owns a separate fixed seven-state executable oracle outside the writable
worktree; candidate tests alone cannot certify their own implementation.

Acceptance requires unchanged files outside the candidate allowlist; exact
terminal semantics; separate tests for all seven states; fresh focused tests,
formatting and strict checks; and an independently reviewed candidate diff.
Snapshot inheritance and a successful Provider response alone are not acceptance.
On failure retain the worktree and all evidence. Mainline integration requires
the human's confirmation after review. The first actual experiment ran and failed
during inspection; it is not accepted.

### Complete task inputs for self iteration

Every stage receives the complete data-owned task specification: all four admitted
candidate paths, their initial and current presence, exact seven-state semantics,
dataset schema, all stage restrictions and the current stage's authority. Verified
module entries include `src/tasks.rs` and `src/lib.rs`, with their pinned physical
content clearly identified as host observations rather than model tool results.
Initially absent test files are intentional new artifacts, not required reads.
This does not prescribe or rewrite a model's tool calls. An independent offline
test checks the complete specification for every stage and rejects changed module
facts against the frozen inventory.

The first actual trace is retained at
`/tmp/kolyan-self-iteration-host.RjVdol/self-iteration-run-KRNdv2/actual.jsonl`.
Both admitted existing files were successfully read. The next model step requested
the nonexistent `src/tasks/mod.rs`; the actual module is `src/tasks.rs`. The native
file operation returned a not-found error and the configured Turn failure policy
stopped inspection. The candidate inventory was unchanged; no candidate validation
ran. The input referred to four admitted files without enumerating them in this
stage, and the model's recorded reasoning explicitly identified that omission.
This establishes incomplete host task context and an actual incorrect model path,
not lost earlier tool results or a Provider decoding failure. The revised fixture
is a distinct case revision; it cannot erase or count the failed run as passed.

The revised actual experiment finished in 272.10 seconds with exit 101. Its
complete trace remains at
`/tmp/kolyan-self-iteration-host.RjVdol/self-iteration-run-aOfGHL/actual.jsonl`.
The actual model wrote exactly the four admitted files through file tools. The
production helper adds six lines and has the correct seven-state semantics.
The independent host executed all seven fixed commands: full Server tests
(116 passed), focused candidate tests, strict checks, library build, oracle
compilation and oracle execution exited zero. The oracle exported seven correct
rows including repeated results and unchanged Copy values. Formatting exited one
for the candidate test file. All original test modules were preserved, and the
candidate dataset matched the independent exact schema and state expectations.

Independent review also found candidate assertions for repeated calls and Copy
preservation before the complete observation export. The candidate output omits
those repeated and preservation observations, then compares newly executed calls
rather than the exported results. These violate the task's evidence requirement
even though the boolean results pass. The final model review returned without an
actual model-generated read receipt in that stage; the host rejected it rather
than manufacturing calls or accepting prose as tool evidence. This experiment is
not accepted and the candidate has not been merged. A bounded repair continuation
with preserved validation feedback and fresh independent checks is the next
workflow improvement; it must not silently retry this run or erase its failures.

### Bounded repair after independent validation

System improvements discovered through Kolyan self iteration are authored and
verified by the supervising development agent, not delegated back to the candidate
as a condition for fixing its host. Keep separate provenance for model-authored
candidate changes and developer-authored system or test-framework changes. The
developer identifies the actual failing contract, writes a regression, implements
the scoped correction, and runs independent checks before another explicitly
versioned experiment. Failed evidence remains immutable. Candidate integration
still requires independent acceptance. The human has authorized the supervising
developer to merge after that acceptance, without a further routine confirmation.

The first developer-authored feedback batch corrects host failure closure and
incremental native-effects matrix evidence. A rejection records the existing
coordinator's durable Task failure with a stable fact identity, preserves actual
invocation outcomes, and reports a closure failure separately from the original
error. A failure before Task admission does not manufacture a Task. Existing
terminal facts are retained. The matrix records its full plan before any network
row, persists each actual observation and row state immediately, and still runs
every planned row once without discarding later cases when an earlier one fails.
Add separate regressions without changing old cases or expected verdicts.

Implement this batch in the isolated `codex/agent-feedback` worktree while the
mainline actual matrices continue on their frozen implementation. This is not a
host-authored repair of Kolyan's retained four-file candidate and does not turn
the old failed experiment or matrices into passed results.

This first feedback batch is implemented in the separate feedback worktree, not
merged into main. Its complete Root offline target passed 50 tests with 13
explicitly ignored tests; strict checks and formatting passed. Three independent
matrix regressions prove the upfront plan, failure isolation, incremental exports
and interrupted-row evidence. Three failure-closure tests cover unadmitted tasks,
durable rejection and idempotent reconstruction, preserved cancellation, and
storage failure reported without replacing the original rejection.

A separately executed pinned actual-artifact test copied the retained failed
self-iteration journal and replayed host closure. It passed: all four completed
invocations and their attempts were identical, the aggregate Task changed from
Ready to Failed through one added fact, and source journal and trace hashes stayed
unchanged. The new copied-store trace is
`kolyan-feedback-actual-closure-YaNZp1/actual.jsonl`. This is a real-artifact
regression with zero network calls, not a fresh actual-model run. The feedback
implementation still requires fresh live validation before complete acceptance;
the mainline network matrices continue on their older frozen code. An initial
feedback compile failure from an incorrect AgentIdentity import was retained;
the corrected import uses the verified public Server type, not a new duplicate.

The frozen mainline ordinary Root matrix subsequently finished 37 of 38 rows
passed, with one failed named Qwen OpenAI-compatible case lacking a real Shell
receipt. Its report remains `kolyan-r1-matrix-Teycao/report.json`; no retry is
performed. The batch continued into the separately planned approval-restart
matrix. These actual outcomes are independent of the feedback worktree's passing
offline checks and cannot be retroactively changed by the developer's fixes.

A revised experiment must admit its full bounded workflow before execution,
rather than increase an existing immutable Task limit after failure. Reserve at
most six invocations and attempts: inspection, implementation, tests, initial
review, at most one repair, and final review. The initial and final reviews remain
read-only. A review without actual model-generated reads and successful matching
receipts fails immediately; missing evidence must not trigger a hidden retry.

Only a completed, evidenced initial review may authorize the predefined repair
stage when independent validation or review finds candidate defects. Bind the
repair input to the candidate digests, actual failed validation logs, review
observations and predecessor completion proof. Reconstruct its host from durable
stores and prepare a lawful Continuation within the same Task, with the same
four-path candidate scope and no additional authority. The actual model must
generate every corrective edit. After repair rerun all fixed checks and the
independent oracle, then require evidenced final review. A second failure ends
the experiment without success finalization. The existing failed four-stage Task
cannot gain extra capacity or have its baseline reset to the modified candidate.

This workflow is now being implemented, but has no executed acceptance yet. Keep
the current failed candidate intact while selecting a separately pinned original
snapshot for the revised experiment. Candidate inheritance must be explicit; no
host-authored repair can be presented as Kolyan self iteration.

The revised read-only review returns one bounded JSON object in its final Text
content, not its Reasoning: `schema_version` is 1, `candidate_digests` maps each
of the four admitted candidate paths to its exact current SHA-256, `verdict` is
`accept` or `repair`, and `findings` is a list of objects containing only `path`
and nonempty `issue`. Deny unknown fields, wrong types, duplicate candidate keys,
unknown paths, mismatched digests, more than 16 findings and more than 32768 bytes
of final Text. Accept requires no findings; repair requires at least one finding.
Malformed review or missing fresh successful file.read receipts for any of the
four paths rejects the experiment immediately, without a repair retry. Bind the
verdict to the exported read receipts and current immutable candidate inventory.
Model review is a bounded input for repair scheduling, not independent correctness
proof: it cannot override a failed fixed check or the developer's merge audit.
Even an accepted initial review cannot skip independently failed validation.
Final review must accept and all post-repair fixed checks must pass; otherwise
close the Task as Failed without altering prior successful invocation facts.

A fresh original-snapshot worktree is prepared for this version at
`Kolyan-self-iteration-r3`, branch `codex/kolyan-self-iteration-r3`. Its 669-file
manifest and binary diff match the originally pinned snapshot. The two initially
absent test files remain absent; baseline source copies are experiment setup,
not developer-authored candidate repairs. The previous failed candidate is not
modified. Its private host directory and explicit ignored project-local config
are separate from the prior run; this setup is not a live-run acceptance claim.

Irrecoverable host rejection must also record a durable Task failure through the
existing coordinator command, using a stable fact identity and explicit reason.
It must preserve actual invocation outcomes and cannot invent stopped workers or
successful validation. In the retained revised run, all four invocation exports
left the aggregate Task Ready and the host rejected acceptance without recording
that final failure. This is a test-host lifecycle gap to correct in the revised
workflow, not evidence that the original run finalized as Failed or Completed.

### Fresh complete gate findings

The complete offline workspace gate retained in
`/tmp/kolyan-main-workspace-offline-final-v5.log` exited 101 at three HTTP process
tests. Their actual Turn JSON contains `pending_approvals` and `external_waits`,
whereas the checked-in OpenAPI still required `pending_approval`. The canonical
schema and affected HTTP consumers were migrated without a compatibility fallback
or weaker response validation. Their full target passed six offline tests; the
independent schema dataset exported 19 rows, including 15 rejected shapes.

The next complete offline gate exited 101 later at the JSON-RPC process test.
Its actual suspension returned `waiting.pending_approvals`, but an old consumer
read `approval.approval_id` and sent null on approval. The retained artifact is
`kolyan-process-offline-jQuUIK`; this is an observed caller contract error, not a
Provider failure. All three process readers, including live branches, were
migrated to the verified nested waiting coordinates. The complete process target
passed six offline tests with one network test ignored. Nine new coordinate rows
were exported before comparison. The fresh complete workspace gate in
`/tmp/kolyan-main-workspace-offline-final-v7.log` and strict workspace gate in
`/tmp/kolyan-main-workspace-strict-final-v7.log` both exited zero for the frozen
implementation, including these repairs and the supplemental inventory.
The selected Agent
plan is 475 existing rows plus 76 supplemental rows, 551 total; Server contributes
57 additional rows. These 608 planned rows were released for fresh actual network
execution after the complete gates passed; they are not yet accepted. The main
host owns 228 ordinary Root/delegation rows, with separate owners for 57 native
effect rows, 76 native-running rows and 190 long-task plus 57 Server rows. Each
entry executes once, exports every failed row, and retains earlier failed reports.
The separate self-iteration experiment does not replace them. Newly migrated
HTTP and JSON-RPC live consumers also require their own affected acceptance audit;
the 608-row Agent and Task selection is not a claim that outer APIs were rerun.

The native-effects actual matrix finished its single attempt in 445.80 seconds:
41 of 57 rows passed and 16 failed. Its report is retained at
`kolyan-r1-matrix-EWzTaL/report.json`, with the complete log in
`/tmp/kolyan-native57-actual-release-20261002-v1.log`. This is not acceptance.
The fixture constructed the whole plan before execution but persisted its
aggregate report only after all observations; interrupted host execution could
lose in-memory row summaries despite retained individual JSONL files. Move future
plan and per-row state persistence before their respective execution boundaries,
without repeating or rewriting this matrix's results.

The supplemental native-running matrix remains in flight and already has failed
rows. One exact retained sample is `kolyan-agent-native-running-live-apmHDa`:
the model generated a short Shell write-and-read call at step zero, followed by a
different Shell call with the phase marker and long loop at step one. Each native
observer context matches its own original prepared call and scope. The test host
captured only the first invocation and rejected later events as a scope mismatch.
That is an observation-model limitation, not evidence of a substituted production
grant. The sample also violates the current case's explicit single-call condition.
A future observer must retain original per-invocation and per-launch identities;
whether preparatory calls are allowed requires an explicit revised case contract,
not silently weaker assertions. Preserve the running frozen matrix unchanged.

The affected outer API inventory adds 152 planned rows after the owner's original
247-row allocation: HTTP process scenarios 76, HTTP ledger isolation 19, and
JSON-RPC process scenarios 57. Their explicit plan is retained in
`/tmp/kolyan-l5-allocated-outer-api-live-v1-plan.jsonl`. These rows have not started
and cannot be counted as part of the previously selected 608 rows. The combined
selected acceptance scope is therefore 760 planned rows, not 760 passed rows.

### Developer owned native observation repair

This reviewed design authorizes developer implementation in the separate
`Kolyan-feedback` worktree. Fresh network allocation follows local acceptance. Preserve the
mainline implementation, all existing one-call inputs/assertions, and the failed
actual matrix unchanged. This is developer-authored fixture repair, not a
model-authored Kolyan candidate or acceptance of self iteration.

The sole actual native-running matrix finished with owner handle `15068` exiting
101: 36 Passed, 40 Failed, zero Skipped/NotRun, 76 total attempts and exactly one
attempt per row. The retained report is `kolyan-r1-matrix-Y6hrqK/report.json`, the
upfront plan is its `planned_inventory.json`, and the log is
`/tmp/kolyan-agent-native-running-live-76-20261002-v1.log`. All 76 individual JSONL
files contain their actual summary. Duration was 1417.10 seconds. By scenario:
Cancel 6/19, Drop 6/19, Loss 12/19, Natural 12/19 passed. These are observed
outcomes, not complete acceptance; no failed row was retried or overwritten.

The 40 failures have the following disjoint diagnostic buckets, using priority
matcher error, cwd refusal, historical conflict, observation loss, physical
content, absent native cancellation, absent phase. Other failures can coexist in
a row, so these are evidence-based primary classifications, not exclusive root
cause claims or model rankings:

| Primary observed failure | Rows |
| --- | --- |
| First-invocation matcher rejects a legitimate later invocation | 3 |
| Workspace-relative cwd preparation refused | 5 |
| Historical reader reports conflicting physical terminal outcomes | 1 |
| Observation loss prevents native cleanup proof | 10 |
| Physical bytes differ from the declared case expectation | 16 |
| Phase bytes exist, but no native cancellation event exists | 4 |
| Required phase is not realized | 1 |

No recorded `original_provider_error` or fixture model timeout was present in
this batch. This does not establish universal SDK correctness. The historical
conflict row `kolyan-agent-native-running-live-vhYlWm` actually received a response
with MaxOutputTokens, 8192 output tokens and no tool call; its Ledger has two
TurnCompleted facts encoding Incomplete differently. Keep that independent
terminal-proof investigation outside the observer repair rather than calling it
a transport failure or fabricating a successful file effect.

#### Original invocation and launch registry

Replace the test host's single captured invocation with a bounded registry keyed
by the exact independently received execution/step/snapshot scope, tool name and
prepared digest. Retain the original prepared call (including its native call ID
and arguments), scoped grant, policy revision, control and monotonic capture
instant. The digest already binds the call ID and arguments; do not derive any
identity from a scripted step number, model prose, marker, or an assumed command.
Do not impose a new host-ID character restriction on native Provider call IDs.

Registration is observation only: forward the same ToolInvocation unchanged,
without a permit, replacement control, altered arguments or a wait for observer
approval. A duplicate/overflow record is an explicit fixture failure and cannot
overwrite a previous original record or masquerade as an admitted new grant.
Registry capacity is a declared test-host memory bound; overflow requires cleanup
of known original controls and dropping the original future, with untracked
cleanup explicitly unproven. Production authorization remains independent.

Resolve every process event against its exact original registry record, then
bind a fresh launch only on that record's actual Spawned event. Retain launch UUID,
PID and process group separately per invocation. Unknown scope/digest, substituted
launch/PID, duplicate launch or missing Spawned cannot contribute proof. Legitimate
later calls with their own recorded bindings are not foreign scopes. Export raw
rejected events and the precise mismatch reason, but never admit them by checking
only tool name, execution ID, or a matching marker. Do not share stdout accumulators
or reap/capture counters across launches.

The root factory's effective Agent permission is already Shell-only, and actual
requests in the retained MiniMax sample advertise only Shell. Keep this explicit
scope; do not fix a multi-Shell problem by pretending a file call was captured.
Other matrices cover file tools. A file process event cannot become this matrix's
Shell phase witness even if its bytes contain the marker.

#### Preserve one-call cases; add a separate preparatory contract

Existing offline/live one-call cases keep their original input, expected call
count, physical content, exact phase marker, causal assertions and verdict.
The repaired registry should diagnose a second legitimate invocation accurately,
but it must still fail a one-call scenario as extra invocation. It must not turn
`Y6hrqK` into a passed report or select a replacement sample silently.

New data-driven cases separately declare one short preparatory Shell invocation
followed by one long-running target invocation. Require successful original
preparation result plus actual preparation launch cleanup, then choose the target
only from an exactly matched later invocation's actual stdout phase. Preserve
both call IDs, arguments, scopes, digests and launches. The declared preparation
count/order and actual physical effects are independent assertions; a phase-like
marker from the preparatory call or Provider text is not a target witness.
Ambiguous target markers, multiple eligible targets and extra calls fail rather
than letting the host pick a convenient successful process. Never classify a
command string as read-only authority or rewrite it into the requested scenario.

Each invocation keeps its original capture-based phase clock; inference and
preparation do not consume the later target's clock. Bound every actual model
open/stream as before. Cancellation/drop starts one independent cleanup clock.
Use the selected target's original control and the ordinary Runtime durable
cancel path, or drop the original root future; do not construct new authority.
Signal delivery, successful wait/reap, group cleanup and capture completion must
be verified for that target, not borrowed from the completed preparation process.
Failed phase/scope matching still triggers cleanup and complete failure export.
Keep production tool deadlines, output ceilings and channel semantics unchanged.

Actual phase output alone cannot prove the process is still running when host
control arrives. A finite command may already have naturally exited. Require
actual native cancellation and raw termination evidence for the positive
interruption claim; retain natural-exit races as explicit failed realization.
Observation loss likewise remains insufficient proof, not a reason to retry,
ignore diagnostics, enlarge production limits or infer TaskStopped from reap.

#### Proposed owned files and local acceptance

After review, changes are confined to feedback-worktree `tests/agent/native_running`
and its module declarations, with new private registry/observer modules and
separate unit tests, additional data fixtures and expected JSONL. Reuse the real
AgentRunner and original production IsolatedToolSet. No Root registration,
shared helper, SDK, Core/Runtime, Sandbox authority or worker protocol change is
needed for this batch. Existing one-call tests remain intact.

Separate data rows cover short preparation then long target Cancel/Drop, same-step
multiple native call IDs, reused IDs in different steps, foreign scope/digest,
substituted launch/PID, missing Spawned, duplicate/overflow registry entries,
preparation marker spoofing, naturally ended target and loss/timeout cleanup.
Export all rows before comparison, including errors. Re-run the existing offline
four cases, the new exact-binding and real-OS preparatory cases, then focused
strict checks and a fresh whole-workspace gate. Any later network matrix requires
a new explicit allocation and independently versioned evidence. File-worker
native operation-entry/interruption remains unproven and is not covered by Shell.

### Native effect feedback repair scope

The retained 57-row report remains 41 Passed and 16 Failed. Read-only inspection
classified nine exact child-input mismatches, two invalid field types, two exact
write-content mismatches and three unexpected child waits. The recorded neutral
requests and completed tool arguments do not establish lost context or a Provider
stream decoding defect. There is no independent outbound wire capture in this
inspection; do not claim SDK wire equivalence from neutral records alone.

Developer repair is limited to test Host scenario assembly and state handling.
Direct-write cases must grant only Write; only the declared late-child case may
grant its exact named delegation target. A returned unexpected suspension must be
exported and rejected as an unexpected scenario path before stopped-execution
recovery or reconciliation is attempted. Preserve existing cases and all exact
input, type, content and effect assertions. Add separate data-driven regressions
for permission assembly and unexpected-suspension handling; do not coerce invalid
field types, trim write bytes or accept mismatched private input. Report parameter
mismatch separately from a genuinely foreign execution scope.

All these fixes are Codex developer feedback, not Kolyan model-authored candidate
repairs. Preserve the failed candidate, original actual reports and original
mainline running jobs. New passing offline gates cannot rewrite prior verdicts.

### Historical reads of unsuccessful completed Turns

The retained native-running trace `kolyan-agent-native-running-live-vhYlWm`
contains a canonical TurnCompleted boundary with reason Incomplete and a separate
TurnCompleted observation describing the same incomplete response. Task evidence
correctly classifies this as Failed because no admitted final answer exists.
The historical result reader incorrectly treats every TurnCompleted kind as
successful and rejects that lawful failed observation as a conflicting terminal.
This is a Server proof-classification bug, not a Provider decode failure.

Correct the historical contradiction predicate: only a TurnCompleted boundary
explicitly proving FinalAnswer contradicts an observed Failed attempt. Completion
notifications without that positive success proof cannot upgrade a failed result.
Keep exact attempt binding, observation source, physical inspect, failure reason,
size ceilings and actual contradictory cancellation/failure checks unchanged.
Do not parse Debug response text into authority or convert Incomplete to success.
Add separate data-driven regressions for Incomplete, Refused and MaxSteps with
their completion notifications, genuine successful completion contradictions,
and cancellation contradictions. Export observations before comparisons and prove
the read does not mutate either journal. Existing historical tests remain intact.

The feedback-worktree implementation passed all six historical/result tests,
including seven new exported data rows. Four unsuccessful completion variants
remain Failed and readable; genuine FinalAnswer, TurnCancelled and
ExecutionCancelled contradictions remain rejected. Both journals stayed unchanged
and repeated reads matched. The retained JSONL is
`kolyan-history-incomplete-7P2AkV/actual.jsonl`. The whole Server target passed 116
tests, and its warning-denied all-target Clippy gate passed. This is offline proof,
not a fresh Provider matrix. An initial fixture construction failure omitted a
cross-stream input-source cause; the corrected fixture copies the full declared
causal predecessors into a separate journal, without weakening production checks.

The feedback whole-workspace gate in
`/tmp/kolyan-feedback-workspace-offline-v1.log` failed at the native sandbox
detached-session probe: its nested test executable under the separately selected
temporary build target was denied launch. The probe did not execute, so this does
not prove escaped isolation or successful isolation for that row. The exact cause
is not yet established. Preserve this failure and diagnose it before claiming
complete workspace acceptance; do not enlarge executable authority to make the
test pass. Parallel observer and native-effect repairs still require their own
finished local gates and a subsequent fresh whole-workspace gate.

### Physical terminal proof ordering

Independent source review found additional proof boundaries that must be closed
before accepting the historical-reader batch. A successful result must derive
its response from an admitted Step before its exact physical terminal boundary;
no later Step or model activity may backfill or replace that stopped response.
A completed observation must reject a later contradictory non-FinalAnswer Turn
boundary, even when both facts use the TurnCompleted lifecycle kind. Repeated
diagnostic notifications of the same terminal must not become new success proof.

ExecutionCancelled records a durable control intention. Without a verified Turn
stop boundary it cannot prove a physically stopped Cancelled invocation; leave
the physical proof unresolved rather than inventing stopped work. Preserve the
ordinary waiting-cancellation protocol's explicit stop facts and the distinction
between read-only historical proof and active reconciliation authority.

Add independent exported data for a genuine completed result, an unsuccessful
boundary after completion, a Step after the terminal and a backfilled final answer,
a genuine stopped cancellation and cancellation intention without stop. Include
real Core-produced MaxSteps coverage rather than only an Incomplete Step under a
MaxSteps label. Keep existing tests and positive stopped outcomes intact. Migrate
incorrect implementation semantics; do not add a compatibility branch or weaken
proof because an older fixture assumed control intent was physical termination.
This is a new required Server proof repair, not yet implemented acceptance.

### Actual model preparatory Shell acceptance

Add an independent 38-row actual-model plan: each of the 19 configured deployment
and protocol combinations runs one preparatory-call Cancel case and one Drop case.
These are new explicit two-call cases, not replacements for the original four
single-call cases or their failed 76-row report. The dataset contains no scripted
model frames. Trusted input asks for one short preparation call, successful result
feedback, then a separate foreground target that emits its exact phase and remains
running until Host cancellation. The Host never constructs either model tool call.

Plan and export every row before execution; each gets one attempt. Observe target
index one only through its original exact binding and launch. Compare two actual
model requests and invocations, preparation cleanup before target phase, target
native cancellation before reap, independent cleanup and capture, exact physical
bytes and no model/effect replay during reconstruction. Retain full actual calls,
results, reasoning and both journals before comparisons, including failed rows.
New input may specify foreground execution and exact bytes clearly; it cannot
relax existing cases, normalize model commands or increase production deadlines.
Local inventory proof is not network acceptance. This adds 38 planned rows to the
previous 760 selected rows, for 798 selected rows; none of the new rows has run yet.

The added inventory exported all 38 unique deployment/case labels with zero
scripted frames and zero attempts at
`kolyan-native-preparatory-plan-U1IUzG/plan.json`. The focused native-running
target passed 11 offline tests, including the original four causal cases and
the two additional real-OS preparatory cases; two actual-network entries remained
explicitly ignored. No network outcome is implied by this gate.

The canonical-path feedback whole-workspace gate in
`/tmp/kolyan-feedback-workspace-private-v2.log` exited zero, including the existing
detached-session sandbox probe. This validates that compiled snapshot only;
later physical-proof, bounded-workflow and actual-preparatory additions require
fresh final gates. Separately, the frozen mainline actual approval-restart matrix
finished all 38 rows Passed, with report `kolyan-r1-matrix-HoVOB7/report.json`.
The other mainline matrix entries continue; earlier failed reports are retained.

### Independent review before candidate merge

The bounded self-iteration workflow passed 20 offline tests, with four actual
network entries ignored, in `/tmp/kolyan-feedback-self-independent-final-v4.log`.
Fresh read proof now verifies the exact model Step, prepared effect, authorization,
start, receipt and completion in causal cursor order, together with the candidate
content digest. Nineteen receipt dataset rows include a real native-worker positive
case and explicitly non-authoritative adversarial copies. This is framework
verification, not acceptance of a model-authored candidate.

The independent physical-terminal gate passed its 22 exported dataset rows in
`/tmp/kolyan-feedback-terminal-independent-v2.log`. A real Core NoProgress run
issues three model requests and executes two tools before TurnFailed; it does
not first publish a NoProgress completion boundary. Therefore the suspected
double-boundary conflict was not reproduced and did not justify changing the
producer contract. Keep control intention separate from physical stop evidence.

Both owner batches are frozen. Fresh whole-workspace, strict lint and format gates
are running against the combined feedback snapshot. The fresh r3 model experiment
has not started and its candidate remains at the pinned original baseline. Merge
authorization remains conditional on independent candidate acceptance; no candidate
or feedback code has been merged on the strength of these focused gates alone.

The combined feedback snapshot subsequently passed the complete workspace gate
in `/tmp/kolyan-feedback-workspace-independent-v3.log`, strict all-target lint in
`/tmp/kolyan-feedback-strict-independent-v3.log` and the format check in
`/tmp/kolyan-feedback-format-independent-v3.log`, each with exit zero. Source is
frozen for the new live runs. The r3 model-authored experiment started once with
trace `self-iteration-run-3MRLAf/actual.jsonl` in the pinned r3 private Host directory.
The new 38-row preparatory Shell matrix started once with report
`kolyan-r1-matrix-DiqmN9/report.json`. Neither live run is yet accepted.

The frozen mainline delegation matrix finished 68 Passed and eight Failed out of
76 rows in `kolyan-r1-matrix-rNITJj/report.json`; no rows were skipped. Root report
Teycao's failed named Qwen Flash row belongs to trace `kolyan-agent-root-VD4LHF`,
not the following successful inline row. In its second Turn the actual request
includes earlier tool results and the new instruction to read and run Shell.
The model issues the read only, then claims both checks completed without a new
Shell call or receipt. Keep that row Failed; this trace does not establish lost
context or a transport decoding defect.

### Shell declared working directory contract

Actual preparatory Drop trace `kolyan-native-preparatory-live-9Jnzmv` contains a
model-generated Shell call with an empty path. Preparation rejects it before any
process starts. The current advertised schema accepts any string, so it does not
expose the runtime's nonempty-path constraint. Correct that declaration without
accepting empty paths, rewriting calls or weakening sandbox preparation.

Omitting path or supplying null selects the workspace root, matching the current
optional input contract. An explicit string path is a nonempty,
NUL-free workspace-relative directory; `.` selects the root and `..` components
are forbidden. Describe these rules and publish minLength one and maxLength
4096 for string values, with the nullable type declared explicitly. Keep filesystem existence, symlink containment, trusted-root exclusion and
byte-bound validation in preparation; schema text is not security authority.
Declare command as nonempty and NUL-free with whitespace-only input rejected by
preparation. Do not publish a global command maximum because it is instance
configuration. Reject unknown properties as before.

Add separate data-driven tests that export schema and observed preparation before
comparison: omitted path, null and `.` succeed, empty and absolute paths, parent traversal
and NUL fail without process execution; retain existing cases and assertions.
Run focused tools and integration gates. Fresh network evidence must use the new
source snapshot; preserve the current failed row and all currently running frozen
matrices. Work on this repair must not mutate the model-authored candidate.

The r3 experiment ended rejected after 349.82 seconds. All four initial-review
reads passed exact provenance checks; the review Text enclosed its otherwise
typed repair verdict in a Markdown fence, which the strict parser rejected.
Fixed format validation failed while the other six commands passed. Independent
source review additionally found Copy/repeat assertions before complete export,
missing before/after facts in exported observations, unchecked dataset version,
unknown-field acceptance and missing unique-state coverage checks. The helper's
seven compiled oracle cases and 116 passing tests do not establish those missing
quality properties. Preserve this candidate and its Failed Task; do not merge it.

The new preparatory matrix ended 36 Passed and two Failed out of 38 rows in
519.40 seconds. The empty-path failure above had no process admission. The other
failure, GLM 5.2 OpenAI Drop trace `kolyan-native-preparatory-live-XmS9rb`, issued
write, an extra xxd query and then the target loop instead of exactly two calls.
Its requests contain the actual preceding results; each of three process scopes
and grants matched its launch and all three were cleaned up. Preserve the failed
two-call comparison; do not classify extra work as lost context or failed cleanup.

The declaration repair lives in a separate developer worktree so current live
snapshots and both failed candidates remain unchanged. Its first focused gate
passed 21 Shell tests, including 14 new exported input-contract dataset rows;
strict tools lint, format and layout checks passed. Full tools and native-running
integration gates remain in progress. These local gates are not fresh actual-model
acceptance of the changed advertisement.

The declaration repair's complete tools gate passed 68 tests, and its affected
native-running integration gate passed 11 tests with two network entries ignored.
Final whole-workspace strict lint, format and layout checks passed; the final
focused gate again passed 21 Shell tests. A fresh 38-row actual-model run started
once against that frozen repair snapshot, with report
`kolyan-r1-matrix-r9sM8D/report.json` and log
`/tmp/kolyan-shell-contract-preparatory38-live-v2.log`. It is still running, so
these checks do not yet establish that the changed advertisement resolves every
actual-model case. The prior 36/38 report remains unchanged.

The frozen mainline parallel-and-writable-serial actual matrix finished all 38
rows Passed in 1111.70 seconds, with report `kolyan-r1-matrix-xG6WTL/report.json`.
Its specific entry returned zero and the existing batch continued into recursive
self calls and multiple child approvals. This is scheduling acceptance for the
selected 19 deployment combinations, not closure of the failed delegation or
self-iteration cases.

### Native review output and complete candidate observations

The rejected r3 requests set output_format to null while asking for strict review
JSON in prose. The project parameter binding for
`qwen/anthropic_messages/deepseek-v4-pro` explicitly selects the structured_output
profile. The next revision must use the existing neutral OutputFormat for the
initial and final review, rather than inventing protocol fields, switching models
or stripping Markdown fences from a rejected response. Ordinary implementation,
test and repair stages keep their ordinary output contract.

Before Task admission, check the selected canonical protocol/model feature table
for StructuredOutput. Reject unsupported selections explicitly; do not silently
send unsupported fields or downgrade this revision to prompt-only review. Native
schema includes the four exact candidate keys, typed version, verdict and findings,
required fields and closed objects. Application checks still enforce byte bounds,
digest equality, unique keys, finding bounds and accept/repair semantics. Verify
the actual structured_output accompanies the same strict final Text. Four fresh
successful reads remain mandatory; structured generation supplies no authority.

Use a new explicit case revision, Task and worktree from the pinned original
baseline. Reuse the bounded workflow through a data-owned revision overlay rather
than duplicate the complete dataset or alter old inputs and assertions. Keep the
r3 failure and candidate immutable. No hidden retry, artificial model response,
host formatting of candidate code or extra repair budget is permitted.

Strengthen the candidate test evidence contract: collect all seven cases with
state, actual before/after Copy values and three actual helper results, then export
one JSONL row per case tagged candidate_invocation_state_v1 before comparing any
of those values. The fixed Host validation must require seven unique complete
rows matching the expected states, unchanged before/after and all three repeated
results. Absence of rows or missing observation fields is failure, not an empty
successful comparison. This augments the compiled public oracle; it does not
replace the final independent source review of assertion ordering, strict DTOs,
version validation, coverage and preservation of old tests.

Add independent native-review request/schema/preflight tests and observation
validator datasets, export before comparison, retain the old review parser cases,
and run local gates before the new actual experiment. The developer workspace
implements native-review preflight, request configuration and strict output
equality, with 44 data rows; the observation validator has 27 rows. Independent
focused regression passed 23 tests with five explicitly ignored network entries.
Full regression and actual candidate acceptance remain separate gates; no
candidate has been merged.

### Native review wire schema and local validation

Independent review of the local official anthropic-sdk-python implementation,
src/anthropic/lib/_parse/_transform.py, found that transform_schema preserves
typed objects, required fields, enum and closed objects but moves unsupported
constraints into description. Pattern, const, string length and maximum array
length must not be presumed supported solely because the model advertises
StructuredOutput.

For this experiment, explicitly construct the review wire schema from that
supported subset. Represent version one with a singleton enum. Describe digest
and size constraints on the wire while preserving the full local schema,
strict review parser, digest equality and read receipts. Validate the final
response against the complete local schema as well as output equality. This is
not a fallback to prose-only generation and does not change Provider feature
selection or implement a speculative generic schema transformer.

Add new independent wire-schema tests without replacing the existing 44 data
rows. They must export both schemas before comparison, prove unsupported
keywords are absent recursively from the wire and prove the complete local
constraints still reject invalid output. Require fresh local gates before the
single new r4 actual experiment; preserve the rejected r3 source and report.

The wire/local split is implemented in the separate developer workspace. New
20-row tests passed; the original 44-row dataset and tests retain their frozen
hashes. Independent reviews found no blocking bypass in the overlay, observation
acceptance or native-review wiring. Final whole-workspace strict lint passed;
the final whole regression passed. The r4 actual experiment then failed in its
tests stage: file.read contained an undeclared limit field, and the host selected
default FailTurn. Its original trace and rejection closure remain unchanged;
no merge occurred. structured_output is parsed by the adapter from final Text; it is
not an independently signed source, execution or correctness credential.

The frozen-mainline recursive-self and multiple-child-approval actual matrix
finished 34 Passed and four Failed out of 38, with report
kolyan-r1-matrix-m1Reai/report.json. The ten-Turn long-task matrix finished 37
Passed and one Failed out of 38, report kolyan-r1-matrix-xAcVaW/report.json.
In its failed long03, the second model request contained the first successful
read result; two distinct read receipts violated the exact-one-read case while
the edited file was correct. These failures remain failures, not skipped rows,
and neither matrix establishes full L5 acceptance.

### Recoverable tool errors in self iteration

Use the existing Turn ToolDispatchPolicy rather than invent another retry loop.
The self-iteration host must explicitly select Serial and ContinueBatch and
export this actual selection with the stage specification. A malformed ordinary
file call remains strictly rejected, performs no file effect and generates an
is_error ToolResult for the next model request. The model, not the host, must
produce a corrected call. Every attempt consumes the existing tool/Step budgets;
do not increase limits, add hidden retries, strip unknown fields or expand grants.
Uncertain outcomes, cancellation, timeouts and malformed batch identity remain
fatal. New independent regressions reproduced two gaps in the existing core:
execution InvalidBatch and preparation TimedOut were incorrectly fed back under
ContinueBatch, followed by FinalAnswer. Add each missing fatal variant to its
respective branch; preserve the reproductions and expected stopped outcomes.
Do not change global Turn defaults or reinterpret
the frozen r4 failure as successful.

Add an independent data-driven regression for erroneous-call feedback, corrected
call and termination, plus no-progress/budget and fatal-error boundaries. Export
the actual requests, events and results before comparison. Preserve old inputs
and assertions. Run the next actual self iteration in a fresh original-baseline
worktree with new revision and Task identity; retain four allowed files, native
review, seven fixed validations, one evidenced repair and final source audit.
No host edits to the candidate are allowed. Merge only the verified candidate,
not the experiment baseline's unrelated in-flight changes.

Independent validation of the separate error-feedback workspace passed all 98
Core tests, the nine new data-driven error-feedback cases, formatting, strict
workspace/all-target lint and the full workspace regression. The two fatal-error
reproductions now stop rather than produce FinalAnswer. This verifies the host
wiring and kernel repair, not acceptance of a model-authored candidate.

The single r5 actual run ended failed after 204.35 seconds. Its tests-step-1
request contains the original rejected read call's is_error ToolResult; the
model then generated two valid file.write calls itself. The next request returned
HTTP 429 with code Throttling.AllocationQuota, explicitly stating that the monthly
token-plan quota was exhausted and resets at 10-21 16:00 UTC. This is external
quota exhaustion, not transient throttling or renewed failure to feed back the
tool error. The Task failure closure retains that original reason.

The partial candidate remains rejected: it has not registered its test module,
and dataset version/coverage assertions precede observation export. It never
reached fixed validation or native review. Preserve its files and actual trace;
do not host-repair, merge or quietly rerun it. Further actual acceptance requires
available provider quota or an explicitly configured and verified alternative
supporting the required native output contract. Do not infer that support from
vendor names or silently switch models to conceal the failed run.

### Process lifecycle evidence under high output

Actual native running cases produced thousands of dropped observations while
stdout and stderr shared a queue with cancellation, capture and reap events.
Missing lifecycle evidence does not prove that cleanup failed, but it prevents
independent verification of the cleanup. Fix this transport boundary without
changing process ownership, permissions, cancellation or output ceilings.

Use separate bounded lifecycle and output queues. The channel capacity remains
1 through 1024 per queue; total queued events are at most twice that capacity,
plus two receiver merge heads. Output cannot occupy lifecycle capacity. Neither
queue applies execution backpressure. Full or closed queues still record loss.
Retain dropped() as the aggregate loss counter and expose lifecycle_dropped()
and output_dropped() separately. Refused oversized host correlation counts as
lifecycle loss. These counters never constitute proof of execution or cleanup.

Merge accepted events in producer order using an internal sequence, with no new
public event fields. Preserve per-pipe output ordering and capture completion
ordering; cross-pipe delivery is not an OS causality guarantee. Sequence assignment,
nonblocking send and receiver head selection must have a common short critical
section, with no await, process I/O or user callbacks under it. Both queues may
still lose events if their own capacity is exhausted or the receiver disappears.
Do not claim a durable or universally lossless observer.

Add independent data-driven queue tests and actual macOS high-output cancellation
and future-drop tests. Verify actual PID/group correlation, reap, group cleanup
and both capture completions. Export observations before comparison. Deliberate
output pressure must show output loss without lifecycle loss; aggregate loss must
remain positive. Preserve existing zero-loss positive cases and disconnected-sink
negative cases unchanged. Do not sample retained bytes, fabricate missing events,
extend deadlines or relax assertions to make an old failed run pass.

Completion of this repair requires fresh sandbox tests, formatting, strict
all-target checks and workspace regression. It does not establish model-authored
self-iteration acceptance or replace the outstanding actual model matrix.

Independent acceptance of this repair passed in the error-feedback workspace:
Sandbox 41 tests, workspace regression, workspace all-target strict lint and
formatting all exited successfully. The nine queue fixture rows, receiver wake
and concurrent-producer tests passed alongside the two real macOS pressure
scenarios. Relevant source hashes remained unchanged during final verification.
Existing retained-output fixtures and native-running comparison code were not
modified. Full regression evidence is
`/tmp/kolyan-lifecycle-workspace-final-v1.log`; strict-check evidence is
`/tmp/kolyan-lifecycle-strict-final-v1.log`.

The independently observed cancel and future-drop runs each lost 224 output
events, zero lifecycle events and 224 aggregate events. Both recorded their
actual process identity, the distinct cancellation cause, successful termination
and group cleanup, SIGKILL reap and two successful captures of 524288 bytes each.
These are real native process observations, not real model acceptance. The
original model failures remain failed.

The existing serial actual-model batch compiles each entry from the main
workspace and has no independent source snapshot. Keep main Rust and Cargo
files frozen until that batch terminates; only then integrate the independently
verified repair and verify the merged source. No self-iteration candidate has
passed its complete gate or been merged.

### Main integration after the original actual batch

The original serial process has terminated. Its shell exit code was zero because
the script finished, while all six test entries exited 101. The command actually
dispatched 247 rows, not 399: 58 passed and 189 failed, including 182 explicit
quota failures and seven other failures. The additional 152 rows existed only
as a plan and were never dispatched. They remain NotRun, not successes or skips.
No live owner process remains to require main-source freezing.

All seven non-quota failures have now been attributed: six are real MiniMax
HTTP 529 overload errors, and one violates the exact-one-read case. Overloads
occur both before effect admission and after successful write receipts. In the
latter cases the next actual model request contains the successful ToolResult;
the write must not be replayed to recover from a failed model completion call.
These failures do not establish lost context, worker startup failure or quota
exhaustion. Their reports remain failed. Both protocol clients already retry
selected pre-response transport errors, with transport_retries defaulting to one;
they do not retry received HTTP overload statuses. Any added status retry must
be explicit, preserve error provenance and never rerun a Turn or tool effect.

Nineteen independently verified files were integrated into main in three bounded
patch batches. Before/after SHA256 checks matched the audited source exactly.
The batches contain two Core fatal guards and the Shell declaration tests;
Server physical-terminal proof corrections with independent regressions; and
bounded process observation queues with native pressure tests. Existing tests
only gain module declarations where required. No experiment candidate, fixture
weakening, credential configuration or unrelated baseline tree was copied.

Merged-source formatting, workspace all-target strict checks and the fresh main
workspace regression all exited successfully. All nineteen source fingerprints
remained unchanged through the final checks, and the pre-existing staged-file
set was preserved. This is merged offline/native-OS verification, not acceptance
of the failed real-model matrix. Evidence paths are
`/tmp/kolyan-main-l5-merged-workspace-v1.log` and
`/tmp/kolyan-main-l5-merged-strict-v1.log`. Original failed reports remain intact.

### Production Agent host assembly

Entry selection is pending the user's choice between the existing HTTP service
and a one-shot local CLI. The boundaries below describe the proposed host
delivery; they do not authorize a transport contract change or prove that the
reusable Agent library lacks execution. No entry wiring is implemented yet.

The final source audit distinguishes reusable execution from product assembly.
AgentRunner is production library code, with durable start, resume_approval and
pump_agent_children operations. However, all current EnvironmentToolFactory
implementations and AgentRunner construction sites are in tests. The service's
App still constructs SessionExecutionService and TurnExecutor directly. Its
default four-tool sandbox assembly is real, but it does not deliver a production
Agent entry point. Test-host success must not be reported as service integration.

Close this delivery gap in the selected trusted process assembly layer. Agent already depends on
Server, so Server library must not depend on Agent. Do not introduce another
model loop, distributed coordinator, UI, additional tools or parallel permission
rules. Reuse the service's real configured Provider, physical resource admission,
IsolatedToolSet, Policy, ledger and Session storage. Keep host-selected worker,
staging and control paths out of model-controlled arguments.

The production host must assemble the catalog, persistent instance registry and
invocation bindings, TaskExecutionService, real ProviderFactory and exact
EnvironmentToolFactory. Registered and inline definitions retain immutable
revision/content binding and stable invocation identity. Provider construction
must verify the selected model belongs to the admitted configuration, use the
existing protocol adapters and parameter table, and never select a substitute
model. Context recording must acknowledge the complete preparation or fail;
unsupported token counting is explicit, not an invented exact measurement.

Static Agent ceilings and dynamic Policy decisions remain independent. Both
advertisement and actual preparation/execution enforce effective permissions.
Read-only parallel admission requires enforcement evidence from the concrete
adapter; a read-only label or scope hint is insufficient. Shell claims the
enforced workspace and is never narrowed by guessing what its command does.
Durable waiting releases running futures. After reconstruction, approvals and
child-result consumption recover original identities, bindings, budgets and
checkpoint proofs; neither a new host instance nor user approval grants a replay.

Implement in dependency order: concrete host factories and durable assembly;
the actual service entry and its bounded input/result mapping; then process-level
acceptance. The transport contract must explicitly cover Agent selection and
waiting-child approval coordinates before being wired. Do not expose raw grants,
checkpoints or proof bindings as public resume authority. Preserve existing
HTTP/RPC behavior and tests unless an explicit contract migration is specified.
A public but unused builder alone does not complete the production entry gate.

New data-driven process cases must invoke that production entry, not instantiate
a second AgentRunner in the test framework. Cover named/inline roots, all four
tools, self-call, two children, multiple waiting approvals, reconstruction before
resume and consumption, ten-Turn long tasks and cancellation. A scripted model
proves deterministic process boundaries only; each applicable real configured
model remains an independent acceptance row. Export actual requests, effects,
waits, receipts and trajectory before comparison, including failed attempts.
Quota exhaustion and the historical rejected self-iteration candidates remain
unaccepted, not exclusions or success evidence.

### Bounded model request opening retries

Implement the user's earlier request for correctly layered transient retries at
the protocol HTTP opening boundary. Reuse the frozen serialized body, endpoint,
model and options for every attempt. A successful HTTP response ends this loop:
later framing, JSON, UTF-8, incomplete-stream or stream transport errors never
reopen a request. This also applies to nonstreaming creation before JSON decode.
Turn, Runtime, tool preparation, effect execution and approval resume do not
gain retry loops. No request is dispatched by a detached background task.

A shared kolyan-protocol-http module owns policy calculation and bounded typed
retry observations; it does not own SDK payloads, model loops or authorization.
Keep existing transport_retries and add an explicit HttpRetryPolicy, disabled
for HTTP statuses by default. One retry counter covers both categories: each
branch is limited by its selected cap and all sends by one plus the larger cap,
not nested or multiplied budgets. Enabled policies bound total opening time and
server-directed delay. A delay that does not fit must stop, not be shortened.
Each subsequent request timeout must fit the remaining opening budget.
Enforce that budget around awaiting headers and unsuccessful error-body reads,
not by shortening reqwest's whole-response timeout: its request timeout also
applies to an already accepted stream. Successful headers end opening control;
a slow valid body retains the original configured transport timeout. Add a
localhost case whose accepted body outlives the opening window and still
completes, proving that long reasoning streams are not cut off by retry policy.
Deadline attribution uses the current attempt: an already received error status
survives a timeout while reading its body (for example, a 401 following 529 is
still 401). A dispatched request whose headers time out is an opening transport
deadline, not the preceding attempt's HTTP status. Only exhaustion before a new
dispatch retains the prior cause; never misclassify an authentication failure
as temporary overload merely because the previous attempt returned 529.

Follow the inspected official Python SDK status/header mechanism: explicit
x-should-retry, then 408, 409, eligible 429 and 5xx including 529. Observe
retry-after-ms, numeric retry-after and HTTP-date in priority order. Fallback
uses 0.5-second exponential backoff, eight-second ceiling and 0.75 through one
jitter. OpenAI's inspected SDK refuses finite hints above 120 seconds; Anthropic's
inspected SDK allows much longer hints, so retain separate profiles and enforce
the host's bounded window without pretending those defaults are identical.

Kolyan additionally refuses known permanent structured error codes, including
Throttling.AllocationQuota and insufficient_quota, even with x-should-retry:true.
Unclassified 429 is not evidence of temporary rate limiting and fails closed;
recognised transient rate-limit codes may retry. This is an explicit host policy,
not a claim that the official SDK parses those codes in the same way. Never use
diagnostic previews or string-substring guesses as the classifier. Parse only
the already bounded error envelope and retain its original status and body.

Reports contain actual attempts, status, bounded request ID/error code and
selected waiting basis/duration, not credentials or raw headers. Successful
retry reports accompany the accepted protocol stream/response and are mapped
to clearly local Provider metadata. Failed retries retain a typed report plus
their original terminal error; Provider classification still uses that cause.
Neutral `ProviderError.diagnostics` carries optional local, bounded JSON retry
observations without importing either SDK or the HTTP-policy crate into the
model contract. Constructors default to no diagnostics. Adapters preserve
status/kind/phase and expose the report under `local_http_opening_retry`; error
messages alone are not a machine-readable retry contract. Diagnostics never
contain raw headers, authentication or full request/response bodies.
Do not impersonate Stainless headers or promise exactly-once model billing.
Existing error variants and direct status-check assertions remain unchanged.

Expose explicit retry selection through trusted service/provider configuration.
Use the same optional `http_retry` field in service JSON and each live-test
Provider TOML section. Omission disables HTTP-status retry; do not mutate the
existing fixtures to silently enable it. Enabled settings include `max_retries`,
`max_elapsed_ms`, `max_server_delay_ms` and the exact protocol `profile`
(`OpenAi` or `Anthropic`). Validate limits on deserialization and reject an
enabled profile that disagrees with the selected client. These are trusted host
settings, not model-generated extensions or tool permission grants.
Do not silently turn the original failed matrices into retried successes. Add
independent data-driven localhost HTTP fixtures covering recovery, exhaustion,
disabled retry, permanent quota, header overrides, delay precedence/limits,
transport/status mixed budgets, cancellation during waiting and post-acceptance
stream failures. Export actual requests/attempts/results before comparison.
Add a receipt-backed integration case proving that a completion-request retry
after a successful tool call does not repeat the effect. Original tests and
their assertions remain intact; actual Provider acceptance remains unavailable
until a verified endpoint has quota, and localhost evidence is labelled as such.

Mechanism sources: the inspected local official Python SDKs' _base_client.py
retry functions and [OpenAI retry guidance](https://developers.openai.com/api/docs/guides/rate-limits).
Those sources support eligibility and delay mechanics, not claims that Kolyan's
explicit host caps, quota exclusions or defaults equal every official SDK.

### 请求开启重试开发验收

2026-10-02 的实现先在 `Kolyan-error-feedback` 开发工作区验证，再将经过审查的
53 个文件集成到 Main 工作区。尚未提交或推送，不代表实际模型矩阵全部通过。

- 共享策略覆盖 44 条数据，报告覆盖 6 条数据；服务配置覆盖 8 条数据。
- OpenAI SDK 覆盖 30 条 localhost 场景，Anthropic 覆盖 36 条；分别包含
  及时响应头加慢正文、错误正文超时保留当前状态、响应头超时不冒充旧状态。
- 中立 Provider 分类和本地报告分别覆盖 11 条 OpenAI、11 条 Anthropic 数据。
  另有 2 条 OpenAI 受限文件效果测试，不能作为原生沙箱验收证据。
- AgentRunner 四条跨层场景使用真实 macOS 沙箱 worker：两协议分别验证
  完成请求 529 后恢复和写入后永久 quota 拒绝。恢复行均为 3 次 HTTP 尝试、
  2 次逻辑模型请求、1 次工具执行和 1 份收据；quota 行均为 2 次 HTTP 尝试，
  保留同一文件与收据且不重放。重发的完成请求原始 body 完全相同，包含原工具结果。
  场景数据在 `tests/fixtures/agent/opening_retry.json`，框架在
  `tests/agent/opening_retry/`；请求、账本、收据、物理文件观察先导出再比较。

独立 workspace 回归日志 `kolyan-error-feedback-opening-workspace-v1.log` 为 exit 0，
包括既有长任务图和审批恢复。随后纠正当前尝试的超时归因，独立 SDK 最终回归
`kolyan-error-feedback-opening-sdk-final-v3.log`、Provider 最终回归
`kolyan-error-feedback-provider-opening-final-v2.log`、跨层效果回归
`kolyan-error-feedback-opening-integration-final-v2.log` 和全 target 严格检查
`kolyan-error-feedback-opening-strict-final-v2.log` 均 exit 0；日志位于本机 `/tmp/`。
格式与差异检查通过。两条本轮新建、未合入的 Anthropic fixture 曾错误期待旧状态，
已按当前尝试契约纠正并加强 status 断言，失败轨迹仍保留；原仓库测试断言未弱化。

Main 集成后的完整 workspace 测试 `kolyan-main-opening-workspace-v2.log` 和
全 target 严格检查 `kolyan-main-opening-strict-v2.log` 均 exit 0；格式、源码布局
与差异检查通过。53 个集成文件逐一校验与审查版本一致。首次 Main 编译因集成清单
漏掉新增的 Anthropic 超时测试数据而失败，补齐数据后重新运行上述完整检查；
首次失败日志保留，原测试断言未改动。需要 API Key 的 ignored 用例未因此执行。

这些结果证明本地协议故障下不会重放已完成效果，不证明真实 LLM 的输出质量、
计费幂等或原自我迭代候选合格。原模型失败、quota 和拒绝候选保持原状态；
实际 Provider 的完整验收与合格自我迭代候选的独立验收仍未完成。

### MiniMax 自我迭代验收

用户明确授权使用现有 MiniMax 配置替代额度耗尽的千问。新增独立的 MiniMax-M3
Anthropic 场景与 Task 身份，不改写旧候选、旧用例或旧验收结果。凭据仍只读取环境变量。
[MiniMax 官方参数文档](https://platform.minimax.io/docs/api-reference/text-anthropic-api)
未声明原生 JSON Schema 输出支持；不得为通过测试虚报 StructuredOutput 能力。

新场景显式采用本地严格 JSON 审核契约，不请求未声明支持的原生输出参数。
拒绝 Markdown 围栏、非对象、重复字段、错误版本、未知字段、错误文件摘要及无依据结论。
完整本地 schema、四份当前审核阶段真实读取收据、候选写入来源与固定独立检查
仍是验收条件；本地审核契约不冒充服务端受约束解码。既有原生输出场景保持原契约。

沿用受限四文件范围、只读审核、一次证据驱动修复和最终源码审查。使用新 worktree，
由真实模型执行检查、实现、测试、初审、可选修复、终审；每阶段从持久事实恢复上下文。
先跑新增数据驱动无网络检查，再运行真实 MiniMax 场景，先导出请求、账本、收据和
固定检查日志再比较。只有最终候选独立验收合格才合入；宿主不得代改候选。

首个独立 MiniMax 场景 r6 已真实运行并拒绝。检查、实现、测试和初审均调用实际模型
及 macOS 沙箱文件工具；Server 全量 118 项、新增测试、严格检查、构建和独立校验程序
通过，格式检查失败。初审最终 Text 在 JSON 对象前包含解释段落，严格解析拒绝，
未进入修复；Task 持久拒绝保留原原因。候选测试将观察写入 stderr，且在导出前进行
数据断言，独立源码审查同样不允许合入。场景日志为本机
`/tmp/kolyan-self-iteration-minimax-r6-actual-v1.log`；原始请求、账本、工具收据与
候选字节位于私有 host 目录中的 `self-iteration-run-8U1urA/actual.jsonl`。

独立 r7 输入已明确观察使用 stdout 单行 JSON、状态字段为不带 JSON 引号的枚举名称、
所有数据断言在导出之后，并在审核阶段的 system 指令中强调最终 Text 只能是 JSON
对象。新场景无网络检查 27 项通过；基线 117 项通过。真实运行的七行候选观察比较
及初审严格 JSON、四份当前读取收据均通过，初审因格式失败准入唯一一次模型修复。

r7 修复阶段实际执行 16 个 Step 后以 MaxSteps 停止，未到终审，候选仍拒绝。模型
误认为工具去掉了末尾换行；四次实际 file.write 的入参本身均无末尾换行，均为
5012 UTF-8 字节，收据返回同一长度与摘要。该证据不支持工具截断或上下文丢失的
解释。保留原日志 `/tmp/kolyan-self-iteration-minimax-r7-actual-v1.log` 与私有 host
`self-iteration-run-PUt7bK/actual.jsonl`；不代改候选，不修改 Step 预算取得通过。

新增两个独立验证要求。第一，测试宿主在检查阶段结束状态前导出实际 TurnOutcome，
拒绝原因保留 MaxSteps、取消或原 Provider 错误，不能统一丢失为没有最终回答。
第二，数据驱动原生 macOS 沙箱测试覆盖没有末尾换行、一个换行、两个换行及
空白尾部的 write/edit；实际执行前后字节、授权、收据和文件摘要先导出再比较。
协议 localhost 输入须标为模拟模型，不能冒充真实 LLM 验收。原工具和候选测试
不得改变断言，也不得用自动补换行、trim、格式化或模型话术替代字节保真证明。

上述两个验证已窄范围集成到 Main 工作区，共八个文件；原 r2 场景的控制流程和
既有断言保持不变。实际结束状态数据覆盖 12 行，聚焦测试 10 项通过、2 项 ignored；
原生 macOS 文件字节保真测试的八行 write/edit 场景全部通过。受影响 Agent root
完整回归 47 项通过、0 项失败、12 项 ignored，日志为
`/tmp/kolyan-main-agent-root-byte-outcome-v1.log`。集成测试 crate 全 target 严格检查、
格式和差异检查通过。这是新增测试与诊断的模块回归，不是本次重新执行全 workspace，
也不是 ignored 真实模型场景全部通过。r7 候选仍未合入；下一轮输入澄清须使用新身份，
明确实际 JSON 字符串末尾换行的编码，不得由宿主修补旧候选取得验收。

r8 使用独立任务、会话、场景版本与新 worktree。输入在既有 r7 契约上新增
文件内容编码说明：JSON 字符串中的 `\n` 解码后为真实 LF，而 `\\n` 解码后为
反斜杠和字母 n；需要末尾 LF 时应在 content 最末明确包含该字符。模型必须以
实际参数、工具收据和读取内容判断字节，不能仅凭格式检查差异断言工具删除字符。
说明进入持久输入并在实际模型请求中可见；数据驱动测试须导出各阶段构造请求后，
验证新增说明可见、身份独立以及旧场景请求不变。

r8 沿用原文件范围、六阶段流程、审核收据、严格 JSON、唯一一次修复和 Step 预算。
宿主不得补换行、格式化候选或修改原测试。真实模型运行仍须分别记录各阶段结束原因、
请求、效果收据、固定检查和独立源码验收；到达终审且全部条件成立才能合入。
输入说明测试通过不代表真实候选合格。

为遵循用户的千问额度限制，新增独立 MiniMax 真实验收入口，复用现有数据集和
原始执行框架，不改既有全厂商矩阵入口、十九组合规划断言或历史结果。选择范围
在运行前导出并校验：只能来自项目中明确配置的 MiniMax 部署，不接入千问、不静默
跳过失败，也不借选择厂商删除正常或异常场景。具名与内联根、跨 Turn 会话、审批
重建、子调用、递归与多子审批、只读并行与写操作串行、十 Turn 长任务及当前
Continuation 和最终结果链路均须有独立入口与实际轨迹。真实效果仍以授权、收据、
账本因果和物理文件为准，不以模型声称完成替代；MiniMax 子矩阵通过不能宣称原
全厂商矩阵通过。各入口的计划与执行状态分别记录，未运行仍标为未运行。

r8 已实际运行结束，日志 `/tmp/kolyan-self-iteration-minimax-r8-actual-v1.log`
为 exit 101，286.93 秒。六阶段实际 Step 数分别为 2、4、6、2、3、2；修复阶段
未耗尽预算，但最终格式仍 exit 1，新 Rust 文件仍无末尾 LF。实际严格检查通过，
七行观察正确；独立源码审查发现版本、数量及完整集合断言位于观察导出之前，
违反明确契约，候选拒绝且未合入。宿主未修补候选，旧测试保留。完整请求和账本
保存在私有 host 的 `self-iteration-run-VvdUgm/actual.jsonl`。新的输入框架独立检查
29 项通过、9 项 ignored，含实际完整 prompt 构造路径；不等价于候选验收。

MiniMax 专用入口已集成到 Main 工作区：九入口覆盖 44 行，两协议使用原数据和
执行框架。十个新文件与审查版本字节一致，八个父文件只增加模块声明。Main 选择
检查 10 项通过，集成测试 crate 全 target 严格检查、格式与差异检查通过。
本轮长任务完整无网络模型回归为 16 项通过、5 项 ignored，包含实际 macOS 工具、
十 Turn、审批恢复、Continuation、显式投影及 Runner 最终结果链路。

24 行根与子调用真实矩阵、20 行长任务真实矩阵已经分别启动，日志为
`/tmp/kolyan-main-minimax-root-live-v1.log` 与
`/tmp/kolyan-main-minimax-long-live-v1.log`，目前尚未形成全量终态验收。
已观察到子调用失败：模型实际返回 `named_targets: ""`，而 schema 与 Rust 契约
要求数组；另有超过有效权限上限的调用被拒绝。原始 Provider 完成事件与中立
ToolCall 均保留相同非法参数，不能归因于中立映射丢失数组，也不得把字符串转换
为空数组或放宽权限来取得通过。仍须根据完整结果继续排查真实输入契约的可用性。

### 模型可见委派契约收尾

真实失败和源码核对确认声明缺口：`agent.invoke.children.maxItems` 沿用协议全局
上限 8，而实际 `InvokePrepareLimits.max_children` 可为 1 或 2。模型可见上限必须
由 Runner 已验证的当前宿主配置投影，且根调用、子调用、审批恢复和父恢复共用
同一声明路径；不得显示更大的数量后在准备阶段才拒绝。并行布尔值仍只是调度请求，
不表示并发许可，实际资源证明和宿主并发上限保持原职责。

工具字段说明须区分本次调用的 target 与子 Agent 将来的 delegation.named_targets。
权限是子 Agent 和其后代的上限；中间层自己不读文件，不等于可以移除后代需要的
读取权限。inline 定义静态上限与本次请求权限不能混为同一对象。空工具或空委派
目标集合继续只接受数组，显式说明空值应写 `[]`，不接受空字符串；空集合 schema
同时标注 maxItems 0。禁止布尔字段保持 const false 并声明 boolean 类型。
这些说明和数量投影不颁发权限，不改写模型返回值，不改变 DTO 或输入来源证明。

新增独立数据驱动测试，导出实际声明和结果后比较：宿主数量上限 1、2、8，
声明与原准备上限一致；禁止与允许的分支、空数组和错误字符串、将 target 复制到
子委派权限中的越权、零工具中间层向后代授予读取仍须拒绝。重建相同配置声明
一致，配置改变使旧声明在 Provider 调用前被拒绝。原测试、原失败轨迹与正在运行
的真实矩阵保留；新声明的真实验证使用独立运行记录，不能覆盖旧失败或承诺模型
必然服从说明。

真实长任务的 Anthropic MiniMax 出现 HTTP 529 overloaded_error，失败发生在模型
请求开启阶段，尚未接受该请求的事件流；现有项目测试配置未启用已实现的 HTTP
状态重试。测试 Provider 配置显式启用最多两次重试，总开启窗口 120 秒、最大服务端
等待 30 秒，分别使用 OpenAI 和 Anthropic 的已有 SDK 延时配置。总窗口涵盖请求、
错误体读取和等待；不能通过单次重试重新计算总窗口。未知或永久配额 429、鉴权错误、
schema 错误、已接受事件流后的失败仍不重试。模型输出不合法不能作为 HTTP 重试。
测试和策略说明沿用现有协议实现，不新增 Runtime/Turn 重试器。

配置值进入项目内的非秘密 Provider TOML，新增数据驱动检查导出所选策略和
529、401、永久配额、未知限流及 200 状态决定后比较。已启动进程仍使用原冻结配置，
原 529 失败保持失败；后续验证使用新的日志与新实际目录，不覆盖旧记录。

### 子调用失败时的完整比较轨迹

递归和多子审批场景必须在检查任何子调用收据数量前，导出全部子调用的实际
收据统计及总体比较。一个子调用缺少收据或重复读取时，不能阻止后续子调用统计
和总体因果计数写入 JSONL。统计只读取实际账本，不由预期值生成；原有逐调用
和总体断言保持原义，失败仍失败。新增独立数据用例验证正常、缺少和重复收据时
全部观察均先写出，再执行断言；模拟统计仅验证测试框架，不冒充真实模型验收。

真实新一轮内联调用仍出现 definition.permissions.tools 对象而非数组、以及
definition.permissions.delegation.named_targets 空字符串。外层权限正确不代表
内层定义正确；两处都必须严格校验。基础权限 schema 的数组说明和空数组示例
须同时覆盖内联静态定义与外层请求权限，不给静态定义错误套用宿主的动态授权
上限，不提供自动类型转换或默认权限。新增独立嵌套参数数据验证原准备器与
JSON Schema 的一致拒绝、数组示例有效和重建稳定。说明改善不能保证模型遵循。

### 当前验证结果与剩余验收

第一轮 MiniMax 根与子调用矩阵已结束：24 行中 13 行通过、11 行失败，日志
`/tmp/kolyan-main-minimax-root-live-v1.log`，exit 101。数量投影和字段语义改进后，
第二轮为独立运行 `/tmp/kolyan-main-minimax-root-live-v2.log`；不能覆盖第一轮结果。
第二轮已结束，exit 101，690.18 秒：24 行中 17 行通过、7 行失败。委派 8 行中
6 行通过，只读/写入调度 4 行中 2 行通过，递归/多子审批 4 行中 2 行通过，根审批
重建 4 行全部通过，根普通执行 4 行中 3 行通过。最后的 Anthropic inline 根在
请求开启阶段 Transport operation timed out；仅凭此错误不能判断厂商故障、重试
次数或总窗口耗尽，需结合实际重试记录核对。不是全矩阵通过。

独立审查核对两条新委派失败：OpenAI parent-result-restart 的第一步就返回
named_targets 空字符串，尚未进入恢复；Anthropic inline 的外层权限正确，但
内联定义的 named_targets 为字符串、tools 为对象。实际事件参数与中立参数一致，
错误发生在严格反序列化，不支持映射器丢失类型或恢复上下文丢失的解释。
OpenAI 只读并行失败中，一个子调用直接声称未暴露工具，没有 read 收据，另一个
实际读取并有收据；父请求收到两者结果。原始 Completed metadata 回显 file.read，
但它不是出站 HTTP 抓包，也不能证明厂商内部如何呈现工具。逐子收据断言保持失败。

Agent 单测 91 项通过；全 workspace 无网络模型回归 exit 0，日志
`/tmp/kolyan-main-discovery-retry-workspace-v1.log`。全 workspace all-target 严格检查
exit 0，日志 `/tmp/kolyan-main-discovery-retry-strict-v1.log`。这些检查完成时尚未包含
后来新增的嵌套定义说明；真实模型 ignored 用例不在这些通过结果内。
子调用完整比较轨迹改进的聚焦回归为 3 项通过、2 项 ignored，含正常和异常统计
数据及原两种拓扑执行；集成 crate all-target 严格检查、格式与差异检查通过，日志
`/tmp/kolyan-main-topology-export-v1.log` 和
`/tmp/kolyan-main-topology-export-strict-v1.log`。正在运行的真实根矩阵冻结于此次
轨迹改进之前，不冒称其已使用改进后的框架。

原长任务真实运行已结束，exit 101，2280.81 秒：20 行中 16 行通过、4 行失败。
最终结果 6/8、Continuation 3/4、投影 3/4、普通十 Turn 长任务 4/4 通过；四行
失败均记录 HTTP 529 请求开启错误。该运行仍使用关闭重试的冻结配置。
新项目配置的十行重试决策数据通过，但不代表原 HTTP 529 已修复或新长任务矩阵
已实际通过。原进程终态核对后，已启动新配置的独立二十行验证，日志为
`/tmp/kolyan-main-minimax-long-live-v2.log`，尚未全量终态。

嵌套静态定义说明与新增十一条数据已实现；基于真实准备器的有效数组、字符串和
对象拒绝、静态定义上限不等于宿主授权以及重建一致性均先导出再比较。Main 独立
Agent 全量回归 92 项通过，日志 `/tmp/kolyan-main-nested-schema-agent-v1.log`；
Agent 与集成测试 crate all-target 严格检查通过，日志
`/tmp/kolyan-main-nested-schema-strict-v1.log`，格式与差异检查通过。受影响集成回归
已结束，Agent root 54 项、长任务 21 项通过，分别有 17 和 9 项 ignored，日志
`/tmp/kolyan-main-nested-schema-integration-v1.log`。第二轮真实根矩阵早于此次修改
冻结，不能当成嵌套说明的真实验证。

### 宿主选择工具错误反馈策略

AgentRunner 的工具错误处理须由可信宿主显式选择，不能由模型入参或 Agent 定义
提高权限。默认保持 Turn 的 FailTurn，不改变旧场景。新增宿主构造选项只选择现有
ToolErrorPolicy，不增加隐式 HTTP、Step、Turn 或工具重试器，不改变现有串行工具
批次默认或子 Agent 并行准入规则。

宿主显式选择 ContinueBatch 时，复用 Core 已有机制：严格准备失败或策略拒绝
保持拒绝，将实际调用身份和错误 ToolResult 回填下一次模型请求；模型若提交新的
调用，该调用重新经过 schema、权限、当前策略、审批和执行隔离校验。未准备成功
的调用不签发授权，不创建子实例，不产生效果收据。已知工具失败如由 Core 允许
反馈，也不意味着该效果可自动重放；效果是否已进入、是否不确定仍按既有工具
错误和持久事实契约处理。

Uncertain、Cancelled、TimedOut、InvalidBatch 继续按 Core 的致命规则停止，不得
为获得最终回答吞掉错误。HTTP 请求重试仍由协议开启层处理。每次拒绝和后续
模型调用消耗原有 Step/ToolCall 预算；持续错误应在原预算内结束，不无限纠错。
根、子、根审批、子审批和父接续的所有执行器共用宿主选项；审批后的历史 checkpoint
保存的 dispatch 策略和剩余预算仍是恢复依据，新 Runner 不改写已挂起 Turn 的策略。

新增模块数据测试覆盖默认失败、显式反馈后合法调用、反馈后继续越权仍拒绝、持续
错误耗尽预算以及致命错误不反馈，导出实际请求、拒绝、账本与效果后比较。新增
独立集成/真实模型入口复用原模型矩阵配置，只运行授权的 MiniMax 两协议；普通
子调用、递归、双子审批与读写调度在反馈策略下仍比较每个真实效果和因果链。
不删改原 FailTurn 用例、不伪造模型调用，不把未出现拒绝的普通成功运行当成真实
纠错证明；纠错轨迹是否实际发生须明确导出。

宿主选项已接入五条执行器构造路径，根默认保持 FailTurn，恢复沿用历史 dispatch。
新增独立反馈策略集成入口覆盖原八种具名/内联/自调用、父重建、只读并行、写入
串行、两层递归和双子审批场景，两协议真实运行计划共十六行。原入口仍显式走
FailTurn，原场景与断言不变。每个执行的实际失败和错误反馈数量随账本导出，
错误反馈数量不等于纠错成功证明。无网络模型、真实 macOS 工具集成的八行全部
通过，聚焦检查为 2 项通过、1 项 ignored，日志
`/tmp/kolyan-main-host-feedback-integration-v2.log`。第一版新入口因 Rust closure
借用错误编译失败，保留 v1 日志；修正只涉及新框架，不改旧断言。新的模块行为
测试已完成：十八条数据覆盖默认失败、拒绝回填、预算、准备与执行阶段四类致命
错误、不同宿主策略的审批重建，以及启用真实 RoutedTools 准备器后的两种严格类型
拒绝。所有数据先导出再比较，非法调用没有授权、效果或子实例；后续合法 read
有独立授权和收据。模型与效果适配器在模块测试中明确标为模拟，不冒充网络或
原生工具验收。

Main 独立 Agent 全量回归 94 项通过，日志
`/tmp/kolyan-main-host-feedback-agent-v1.log`；根集成回归 56 项通过、18 项 ignored，
日志 `/tmp/kolyan-main-host-feedback-root-v1.log`。Agent 与集成 crate all-target
严格检查、格式和差异检查通过，日志 `/tmp/kolyan-main-host-feedback-strict-v3.log`。
之前 v1/v2 的严格检查失败分别为新测试框架的未使用构造函数与在途新测试编译
错误，保留原日志，未改旧断言取得通过。当前全 workspace 回归和全量严格检查
已另行启动，尚未全量终态。

十六行 MiniMax 两协议真实反馈矩阵已启动，日志
`/tmp/kolyan-main-minimax-host-feedback-live-v1.log`，原 FailTurn 矩阵保留。
普通成功但没有错误反馈不能证明模型纠错；仍按逐调用收据、子结果回填与审批
重建原条件验收，失败保持失败，不自动改写模型调用或重启旧场景。

根超时独立核对：第三次模型请求到错误为 120.009 秒，符合首发送耗尽总开启窗口；
配置 transport retries 1、HTTP retries 2 不保证窗口耗尽后继续发送。当前单次发送
报告隐藏，尝试次数只能据源码推断，不能当成抓包事实。出错请求包含前两次
write/edit 成功结果，没有上下文丢失或工具重放证据；底层为何超时仍未确定。

真实反馈运行中的 OpenAI inline 案例 `btO6QR` 证实错误反馈存在：模型生成的
空字符串 named_targets 与中立调用一致；下一次实际请求完整包含原调用、
is_error true 的严格类型错误和原用户输入。模型识别拒绝后以 EndTurn 回答失败，
没有新调用、子准入或等待，因此原委派验收仍失败。当前根便捷路径固定注册
root-final-answer 的 ExecutionCompleted 条件，此处 Task Completed 只证明根
执行以 FinalAnswer 停止，不证明自然语言目标或实际子工具效果完成。不得把该
状态当作业务目标验收，也不为取得通过放宽逐子效果断言。此差异须在模块说明中
明确；宿主业务后置条件仍是当前未接入根便捷路径的能力边界。

### HTTP 开启失败诊断契约

一次实际发送也必须保留本地开启报告，不以发生重试为错误诊断的前提。
协议错误通过 opening_report 暴露完整开启报告；retry_report 仅表达实际发生多次
发送的重试报告，单次错误仍返回 None。Provider 错误诊断读取 opening_report，
成功流继续按真实多次发送判断。两个观察入口分别表达开启与重试，不引入重试器
或数据回退，原单次 HTTP 错误没有重试报告的断言保留。
本地 Provider 诊断 kind 同样区分 local_http_opening_report（零次或一次发送）与
local_http_opening_retry（多次发送）；不能把新增单次诊断标成重试。报告结构一致，
调用方可按 kind 识别含义，原成功流和真实重试的 kind 不变。
报告区分每次实际发送的原始 decision 与整个开启操作的 terminal_stop：例如
第一次 transport 错误仍允许 Retry，但总窗口已经耗尽，最终停止原因必须另记
ElapsedLimit，不改写原 decision，也不虚构第二次发送。成功报告的 terminal_stop
为空；尚未发送就耗尽预算可记录零次发送与停止原因。

协议错误包装保留原始 cause；Provider 保持原错误分类、Open/Stream 阶段和 HTTP
status，仅附加有界本地诊断。该数据不进入请求、厂商原始 metadata 或工具上下文。
成功流仍仅在真实多次发送时输出原有重试诊断，不增加流事件，不重放已接受的流。
不改变 SDK 请求映射、重试资格、等待时间、总预算或工具执行策略。

验收新增独立数据驱动场景，覆盖零/一次/多次发送、原 decision 与最终停止原因
不一致、永久 HTTP 错误及无 status 的开启失败。两协议和 Provider 均须验证单次
报告传播与原 cause/classification 保持，先导出实际诊断再比较，不修改旧断言。
本批正在运行的真实矩阵使用修改前版本，不能当作新诊断已生效的证据。

全 workspace 回归已终态通过，日志
`/tmp/kolyan-main-host-feedback-workspace-v1.log`；全 workspace all-target 严格
检查已通过，日志 `/tmp/kolyan-main-host-feedback-workspace-strict-v1.log`。
这两项不替代 ignored 的网络矩阵验收。

真实反馈失败的独立核对：VAV08z 首次模型调用收到 529 后第二次开启超时，尚未
进入父恢复场景；wosETC 子请求声明 file.read，但模型直接回答没有工具；MDL9HC
子模型返回拒绝文字；qkOuMH 两次实际输出 named_targets 空字符串，对应两次
类型错误均回填，第二次并未如模型文字所称改为数组。后两种子调用完成后父请求
包含真实子结果，缺少 read 收据仍保持失败。中立请求和响应 metadata 不替代
真实出站 wire 捕获，因此不能据此断言某层 SDK 漏传工具或厂商实现原因。

同批递归 Cof1JE 的完整比较轨迹记录三个实例、三个会话、两个 agent.invoke 收据
与两次子结果消费，但零个 file.read 收据；末级模型直接返回拒绝，未产生读取调用。
这证明本次实际递归准入和结果回填发生，不证明任务目标完成。原预期读取一次
的断言保留，不通过改变预期、制造调用或强制无限重试获得通过。

该十六行 MiniMax 反馈矩阵已终态退出 101，运行 1003.31 秒，六行通过、十行失败。
其中委派三行通过五行失败，并行三行通过一行失败，拓扑四行全部失败。此结果
不支持真实模型全覆盖通过或完整递归/双子审批已验收；失败轨迹与矩阵日志保留。
长任务二十行矩阵仍在运行，不因该矩阵失败重新启动。

开启诊断已实现并由 Main 独立验证：五个协议/Provider crate 共 58 项模块测试
通过，日志 `/tmp/kolyan-main-opening-diagnostic-tests-v4.log`；原 Provider 两套
开启集成共四项通过，日志 `/tmp/kolyan-main-opening-diagnostic-integration-v2.log`，
包含开启失败不重放已收据化工具效果。受影响 all-target 严格检查通过，日志
`/tmp/kolyan-main-opening-diagnostic-strict-v2.log`，格式、差异及源码布局检查通过。
新增数据分别验证报告生命周期、真实 localhost HTTP 发送和纯 Provider 映射；
模拟报告不冒充真实发送，localhost 不冒充真实厂商调用，全部先导出再比较。

保留本批失败门禁日志：v1 单测因新 Provider 测试缺少 workspace dev-dependency
编译失败；v2 单测把单次报告错误暴露为重试报告，触发原断言；v1 Provider 集成
在 kind 修正落盘前捕获旧版本，三种单次错误被错标为重试。最终门禁在所有 owner
冻结后执行，旧场景和旧断言未改。新增依赖仅测试使用，不增加生产依赖。
全 workspace 修改后回归与严格检查已另行运行，日志分别为
`/tmp/kolyan-main-opening-diagnostic-workspace-v1.log` 和
`/tmp/kolyan-main-opening-diagnostic-workspace-strict-v1.log`；严格检查已退出 0，
运行 26.30 秒，回归仍在进行。
真实长任务第一组 finalization 八行结束，六行通过、两行开启超时；余下十二行
继续使用原进程。该进程不包含新诊断代码，不能据此声称新报告已在厂商端验收。

### 委派声明的真实出站验证

新增独立数据驱动集成测试，从生产 Runner 的 prepare_root_input 获取已选定输入，
通过实际两套 Provider/SDK 发送到测试持有的 localhost 接收器，保存 HTTP 路径、
请求体和中立输入后逐项比较。覆盖完整委派、仅 self 的空 named_targets、没有
委派、没有环境工具但保留 self 权限的输入；比较工具名称、描述、完整 input_schema、
空数组示例、禁止项 false 和宿主子数上限，不仅比较工具数量。

测试不修改生产可见性、不扩大权限、不启动子任务，也不把固定的本地响应当作
真实模型推理。日志不保存认证头；测试代码负责保存全部数据后比较。该证据只
能证明本地请求映射和序列化，对既有失败厂商请求没有抓包时不能补称已验证。
原递归和审批场景、类型及效果断言不变；模型纠正失败仍保持失败。

独立审计 vlnj2f、SI85Vt、tApXmn：三者均在首个子调用准入前停止，空字符串、
不合法数组或权限扩大错误都回填下一轮。没有 child、receipt 或审批暂停。模型
文字声称尝试了合法空数组与实际参数不符，不能把这些失败归因于审批恢复或
据其回答认定序列化错误。失败的真实轨迹保留，尚未达到拓扑验收。

### 严格参数错误的字段定位

委派输入解析继续使用现有严格 Serde 契约，新增路径跟踪，使类型错误反馈包含
实际解析器报告的 children 索引和字段路径，以及原类型错误原因。路径只表示
解析器实际定位；自定义 inline 解码内部定位不足时不得伪造更深路径。错误仍是
Invalid 类别，准备失败不创建实例、不授权、不执行，不转换字符串或填入默认值。

Invalid 错误载荷为有界 UTF-8 文本，最长 1024 字节，截断显式标记，不附带完整
输入或请求；上限不含 Display 前缀和外层 ToolResult 封装。
新增数据覆盖空字符串、错误数组元素、非数组 tools、错误布尔、未知/缺失字段、
超长多字节错误与合法空数组；模块验证解析和权限不变量，集成沿用原错误回填。
依赖仅用于解析器路径跟踪，不改变 Provider/SDK 或 Turn 错误策略。该改进解决
定位不足，不宣称能保证真实模型纠正；实际矩阵继续独立验收。

该批实施与独立门禁：生产 prepare_root_input 到两协议真实 HTTP 请求体的八行
完整声明比較通过，Main 轨迹 `kolyan-agent-advertisement-wire-bepK20/actual.jsonl`，
日志 `/tmp/kolyan-main-advertisement-wire-focused-v2.log`。框架读取数据中的预期
子数上限，认证头不落盘；这个证据只证明本地出站保真，不代表先前厂商接收事实。

字段路径十二行数据先导出后比较，外层与 inline 内层错误都保留实际定位；合法
输入完整往返不变，错误载荷 UTF-8 字节上限及截断标记通过。Main 聚焦日志
`/tmp/kolyan-main-invoke-input-path-focused-v2.log`，Agent 全量 95 项通过，日志
`/tmp/kolyan-main-invoke-input-path-agent-v1.log`；根集成 57 项通过、18 项 ignored，
日志 `/tmp/kolyan-main-invoke-input-path-root-v1.log`。Agent 与集成 all-target 严格
检查通过，日志 `/tmp/kolyan-main-invoke-input-path-strict-v1.log`，格式、差异与源码
布局检查通过。原用例与断言未删改，新增数据与源码分文件。

上一批开启诊断版本的全 workspace 回归已终态退出 0；不将其作为后续字段定位
与新出站测试的全仓证明，后续修改使用上述独立门禁。
冻结新代码并取得门禁后，另行启动相同十六行 MiniMax 反馈场景的新版本验收，
日志 `/tmp/kolyan-main-minimax-host-feedback-live-v2.log`，第一版六通过十失败的
结果保留。本次显式启用 SDK 请求体调试，日志由测试进程输出重定向至临时文件，
不保存认证头，不进入仓库；失败时可对照出站对象、字段定位反馈与实际效果。
该调试不是网络抓包或厂商接收证明，不增加盲目重试。仍运行中的二十行长任务
使用原句柄与旧二进制，不重启、不声称已包含本批修改。

### 剩余目标三项验收清单

以下三项是当前目标的剩余收尾工作，不替代前面的完整范围。按一、二、三推进；
已启动的真实矩阵继续使用原进程，本次记录不启动新的实现或重复测试。

1. 复杂委派与审批的真实运行可靠性。

   核对非法参数、拒绝调用与权限拒绝的实际原因，区分请求映射、错误回填与
   模型行为，不能根据模型文字归因。必要的修正仍遵循原严格契约，不能转换
   非法类型、扩大权限、制造模型调用或放宽旧断言。真实纠错验收必须实际出现
   错误反馈以及后续合法的新调用；普通成功但没有错误反馈不算纠错证据。
   具名、内联、自调用、多子、两层递归与双子审批按原数据逐项验收：比较实际
   子实例、私有上下文、效果收据、父子结果消费与因果链；审批须真实挂起、
   重建宿主后确认恢复，确认前无效果，恢复后不重复执行。异常场景保持拒绝、
   取消、预算与失败的原条件。完成条件是授权 MiniMax 两协议的对应真实场景
   满足原预期，并留有完整 JSONL、请求与比较结果；FinalAnswer 不替代目标完成。

2. 长任务全矩阵及开启超时排查。

   跑完当前二十行矩阵，覆盖普通十 Turn 长任务、接续、上下文投影和最终结论。
   对已观察的两项开启超时，使用实际发送次数、各次 decision、总窗口停止原因、
   原 cause 与 SDK 出站对象定位，不仅记录顶层报错。旧二进制没有新诊断时
   明确证据缺口；待原进程终态后才能有依据地启动新版本针对性验证，不把旧失败
   覆盖为成功。重试只发生在允许的开启层预算内，不重放已接受的流、Turn 或效果。
   完成条件是每行均有终态与完整轨迹，正常场景满足原预期，异常场景满足其失败
   契约，超时原因与修正验证有对应证据；跨 Turn 上下文、长期审批重建、收据和
   幂等收尾均可核对。尚未跑完或普通场景仍失败时，不宣称长任务全面验收。

3. Kolyan 自我迭代及合入验证。

   在独立 worktree 中让 Kolyan 通过受治理的四种工具完成一个真实的小能力改动，
   保留基线、输入、运行配置、模型轨迹、实际文件变化与验证结果。候选代码由
   Kolyan 产生；Codex 可以修宿主、工具与测试框架，不能代写候选后冒充自我迭代。
   Codex 独立审查功能、范围、架构、权限及质量，再验证格式、编译、模块测试、
   相关集成/真实场景和必要回归。发现的问题反馈到框架修复并新增数据驱动用例。
   完成条件是一个符合原要求的候选经过独立验收后合入主干，可从最终代码和轨迹
   证明不是空改动、伪造成功或宿主代写。既有被拒绝候选保留拒绝记录，不因此
   降低标准；此前尚无候选完成合入，本项仍待完成。

三项均完成且通过原完整规格的逐项审计后，才能宣布目标达成。单测、静态检查、
一次成功运行或这一清单的记录本身，均不构成完整验收。

### 0031 首批集成后的 MiniMax 真实回归

使用 Main `0f4f053` 加已冻结 0032/0033 源码构建的 Agent 测试二进制；该源码
随后分别提交为 `9ea1392` 与 `e93464c`。运行过程中未改变用例或断言，未重新
编译运行中的二进制。后续 Runtime effect-proof 改动不被算入这份旧二进制证明。

实际命令：`cargo test --offline --locked -p kolyan-integration-tests --test
agent_root minimax_live::actual_minimax_ -- --ignored --nocapture --test-threads=1`。
这个 substring 同时选择三个嵌套模块，实际是五个函数，不只是 root/approval。
终态退出 101，耗时 580.84 秒：Rust 函数 2 passed、3 failed；数据行 15/24 通过，
9 行失败。日志 `/tmp/kolyan-evolution-minimax-root-approval-v1.log`。

| 矩阵 | 数据结果 | 实际 report 临时目录 |
| --- | --- | --- |
| named/inline root | 4/4 | `kolyan-r1-matrix-SkF67J` |
| named/inline 审批重建 | 4/4 | `kolyan-r1-matrix-7ilVsF` |
| named/inline/self 委派与 parent-result 重建 | 1/8 | `kolyan-r1-matrix-iXTefb` |
| 两子并行/串行策略 | 3/4 | `kolyan-r1-matrix-kjAW3S` |
| 两层自调用与双子审批重建 | 3/4 | `kolyan-r1-matrix-bb9EEL` |

每个目录下 report.json 为实际结果，父目录
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/`；完整请求、模型事件、事实及
工具过程的 JSONL 路径由对应日志记录。具名与内联、两个协议均有实际执行，
不是只检查模型回复字符串。原失败矩阵与候选拒绝记录保持不变。

九个失败中，五个报告明确为严格解析的 named_targets 空字符串，分类现在为
ToolError::Failed。主控读取首个 actual.jsonl 的 Provider Completed metadata.output
及中立 ToolCallCompleted，二者均含原始 `named_targets:""`；该次出站 schema
明确要求 array 且说明使用 []，未发现 Provider 将合法数组改成字符串的证据。
这只定位该次模型产物与当前失败原因，不证明模型的普遍行为，也不改变严格解析。
其余四个报告缺少预期效果收据；不能仅据零计数统一归因，须继续逐行核对。

另行启动精确选择的十六行 host-feedback 矩阵，验证真实错误回填与新调用纠正。
日志 `/tmp/kolyan-evolution-minimax-host-feedback-v1.log`；未取得终态前不记为通过。
它不覆盖本次九个失败，也不代替 goal/host/context 的新主线验收。

该次精确选择的 host-feedback 运行已终态：退出 101，耗时 437.14 秒；Rust
函数 0 passed、1 failed，十六个数据行 6 passed、10 failed。真实 MiniMax-M3
两协议运行；现有输入及断言保持不变，无人工合成模型调用替代失败。

| 矩阵 | 数据结果 | 实际 report 临时目录 |
| --- | --- | --- |
| named/inline/self 委派与 parent-result 重建 | 3/8 | `kolyan-r1-matrix-8sxOQt` |
| 两子并行/串行策略 | 2/4 | `kolyan-r1-matrix-dBiiIu` |
| 两层自调用与双子审批重建 | 1/4 | `kolyan-r1-matrix-zafsVq` |

上述同一临时父目录下 report.json 已逐行读取。失败不是静默跳过：四行未出现
预期 child admission 暂停，两行未出现双子等待，一行 parent-result 缺效果收据，
一行深度自调用只有两个而非三个实例，两行分别审批后的子效果收据缺失。
这些是实际断言结果，不是最终根因。新错误分类及 ContinueBatch 错误回填并未
使本矩阵全通过；须精确关联每行 actual.jsonl 并检查每次请求、模型产物、授权
及恢复过程，不能仅凭缺收据统一归因于模型，也不能调低数量或改旧断言。

逐行原始轨迹核对得到：七行在委派参数纠正阶段失败，其中还出现越权权限
或修改相同 revision 的 inline 定义；三行因子模型拒绝已提供的读取工具而缺
效果收据。本次没有发现这十行在中立请求中丢失工具结果或恢复后丢失子结果。
这不是对全部协议 wire 的一致性证明，也不是对该模型所有任务的泛化结论。
主控独立检查 `3PHlWY/actual.jsonl` 第 21 行原始 Provider arguments 已含
`named_targets:""`，第 26 行下一请求保留相同 call ID 的 is_error 结果；
`wTpjpV` 第 82 行实际子请求提供 file.read；`aSGNjo` 第 135 行实际子请求
要求“先审批再调用读取”，而工具只有 file.read，没有独立审批工具。
上述目录分别带 `kolyan-agent-delegation-` 或 `kolyan-agent-topology-` 前缀。

### 明确任务契约的新增真实矩阵

批准新增 `explicit-contract-v1` 数据矩阵，沿用原八种委派、并行和拓扑业务
场景，两协议十六行。它验证明确合法的模型可见任务契约，而非替代原失败验收。
每行记录 baseline scope/case ID、原数据摘要、新输入摘要、contract revision、
完整输入和允许修改字段；只允许新 case ID 与任务文字变化，效果数、实例数、
审批、重建、结果消费和并发断言保持原强度。原 fixture、测试函数体及失败
记录不改。模型仍自行生成全部调用和子输入；不替换 arguments 或硬编码调用。

输入明确：调用 file.read 本身触发宿主审批；没有独立申请审批工具。无委派
权限用 `named_targets:[]`，不是空字符串或包含空字符串的数组；callee 与它
未来可委派的目标不是同一概念。inline 使用宿主实际附上的完整不可变定义，
修复参数不修改同 revision 的定义。深度自调用中间子保留允许 self 的权限但
按任务委派读取，叶子只读取一次；两审批子各自读取一次、分别暂停与确认恢复，
不能用一子的重复效果代替另一子。

新增 `tests/fixtures/agent/explicit_contract.json` 和独立 delegation 子模块及
parallel/topology wrapper；父模块只新增声明。wrapper 在原 case 副本上应用
声明允许的任务文字，复用既有 runner/比较器，不扩大生产可见性。新增离线
测试验证八行覆盖及输入外全部字段一致，保存完整 baseline/new plan 后比较；
再跑真实 OS worker 场景。实际 MiniMax 两协议单次逐行运行，完整实际请求、
模型事件、审批、恢复、收据及消费轨迹先落盘再比较。结果单独登记为新输入
验收；新矩阵通过不能把旧矩阵的失败改成通过。

新增矩阵已集成到 Main `0146648` 后的源码，尚未取得网络验收。主控独立运行
`cargo test --offline --locked -p kolyan-integration-tests --test agent_root
explicit_contract -- --nocapture`，终态退出 0：2 个测试函数通过，1 个网络函数
ignored；八个数据场景均通过真实本机 Runner、原生工具进程及审批重建比较，
但离线模型帧仍是测试数据，不是实际模型生成的调用。日志
`/tmp/kolyan-explicit-contract-main-v1.log` 保留八份实际 JSONL 路径。

完整 Agent 集成回归退出 0：59 passed、0 failed、19 ignored，日志
`/tmp/kolyan-explicit-contract-main-root-v1.log`；Integration crate 全目标
Clippy、格式及差异检查通过。网络 ignored 项不计为通过。原 fixture 文件
及旧测试函数未修改，三个父模块仅新增模块声明。新增的数据摘要、允许字段
守卫和完整场景库存先落盘并实际读回，再执行断言。

此外，此前已冻结的 Tools/Server preparation 集成源码全仓离线回归终态
退出 0：821 passed、0 failed、62 ignored，日志
`/tmp/kolyan-evolution-workspace-main-v1.log`。该运行不包含上述新增矩阵源码，
不能用其数量宣称新矩阵已经全仓或网络验收。
