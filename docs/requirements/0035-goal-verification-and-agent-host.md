# Goal Verification and Usable Agent Host

## Status and outcome

Design in progress under [the evolution program](0031-agent-evolution-program.md).
This is the first mainline capability from the phase review, not a claim of
implementation. Baseline `3043095` has physical execution completion but no
business-goal verifier or production AgentRunner assembly.

The required outcome is explicit, evidence-backed postconditions and a usable
host that reports execution, goal satisfaction and remaining work separately.
A final answer or final refusal alone cannot satisfy a tool-effect objective.
Unmet postconditions can lead to bounded new work, never blind replay.

## Confirmed implementation seams

- Agent `runner/admission.rs::admit` registers only ExecutionCompleted.
- Agent `runner.rs::finish` invokes finalization when invocations stop;
  `runner/finalization.rs::finalize_saved_task` verifies physical results and
  result consumption before calling TaskExecutionService::complete.
- Server `task_driver.rs::record_stopped` derives completion evidence from actual
  Runtime facts. ArtifactDigest currently verifies serialized ModelResponse
  bytes, not a workspace file or an arbitrary business artifact.
- Server `task_driver.rs::complete` revalidates physical evidence before success;
  `tasks/reducer/completion.rs::complete` requires previously observed evidence
  for every criterion. Preserve this proof boundary.
- Server `tasks/reducer/topology.rs::admit` allows Continuation only after a
  completed predecessor and with an explicit dependency. A correction should
  use this new-work relationship, not retry the predecessor.
- Server `task_driver/retry.rs::authorize_retry` refuses a whole-attempt retry
  when any effect receipt exists. Goal correction must not evade this rule.
- `services/kolyan-server/src/assembly.rs` assembles real isolated tools and
  providers but not AgentRunner. `apps/kolyan-agent` is a placeholder;
  `crates/kolyan-cli` is not yet a shipped one-shot Agent entry.

## Required contracts

### Goal definition

Goals are trusted host inputs, separate from model-selected tool arguments.
Persist the exact versioned postcondition definitions with Task admission,
including stable criterion IDs, scope, bounded predicate content and digest.
Recovery cannot consult a replacement verifier or reinterpret old evidence.
Reject unknown predicate kinds, invalid schemas, excessive bounds and duplicate
IDs before any model call or tool effect.

The first useful deterministic predicates must operate on evidence Kolyan
actually possesses: admitted model-result content and physically verified tool
results/receipts. They must name the observed property and temporal scope.
For example, a receipt-backed successful write proves an admitted operation
completed; it does not prove that its file remains unchanged afterward.
Workspace-at-verification assertions require an independently scoped physical
read/measurement, not a model statement or a hash supplied by the model.

Physical ExecutionCompleted remains meaningful for lower-level execution tasks,
but a production business goal must declare nontrivial postconditions explicitly.
Do not retrofit hidden semantic interpretation into that existing criterion.

### Assessment and evidence

Assess only independently loaded, scope-checked evidence. A serializable evidence
object passed by a caller is not sufficient proof. Bind the decision to Task,
criterion definition/digest, verifier revision, actual invocation/attempt and
immutable fact coordinates. Verification must not issue grants or run a model.

Distinguish Satisfied, Unsatisfied, Indeterminate and CheckerFailed. These are
goal outcomes, not alternative physical Turn outcomes. Preserve bounded reasons
and evidence references. Missing input, an uncertain effect or unsupported
verification cannot become Satisfied. A final refusal remains visible.

Persist assessments as validated authoritative facts. A successful Task must
consume all required satisfied evidence through the existing Server completion
boundary. Replay and finalization revalidate ownership, definition, source and
the admitted verifier contract. Trace/UI projections cannot authorize success.

The first implementation will be deterministic; model-based semantic review is
a later optional verifier with explicit uncertainty, cost and failure policy.
No fail-open model verdict or text-matching heuristic proves a filesystem goal.

### Bounded correction

An unsatisfied goal does not make the completed physical attempt unsuccessful
retroactively. Keep the Task nonterminal while an admitted correction remains
possible; distinguish goal waiting from active execution in host results.

Correction creates a new Continuation and attempt, using verified predecessor
context and result dependencies. Bound correction count, shared invocations,
Steps, tokens and elapsed work with a persisted policy. Reconstruct the remaining
budget after restart. Never reset limits through a new Runner instance.

Each continuation uses fresh identity and independent preparation, policy and
grants. A previous receipt is evidence, not permission to repeat an effect.
Uncertain effects require existing reconciliation first. Cancellation, exhausted
limits and checker failure stop automatic correction explicitly.

Define exactly which continuation evidence may satisfy which original goal;
an unrelated child, Task or later invocation cannot supply it. Completion must
still account for admitted child outcomes and required result consumption.

### Host assembly

Compose the existing AgentRunner, exact catalog, ownership stores, real providers,
context preparation, isolated tools and Task/Session services. Do not copy the
loop or use test factories. Secrets are environment references in project-local
configuration, not persisted Agent definitions or logs.

The host must expose start, query, cancellation and approval resume with stable
Task/execution/approval identities. Approval suspension releases the running
loop; a rebuilt host can query and resume the exact durable continuation.
Clearly report physical disposition, goal assessment and remaining work.

Server remains the shared endpoint for interactive clients. A one-shot CLI
reuses host assembly locally without requiring a server process. It returns a
non-success outcome for unmet/indeterminate goals and provides pending approval
coordinates rather than inventing approval. A desktop/TUI Agent app remains
a client, not a second runtime.

## Implementation increments and design gates

### First tool goal and effect proof inspection

The first concrete tool predicate is FileWriteCommittedV1: a specified physical
target received the specified UTF-8 content through a governed file.write.
Bind workspace/parent physical directory identities and leaf, expected SHA-256
and byte length. Do not bind an old target inode: atomic replacement changes it.
This proves the operation at completion, not the file's later current state.
Goal checker code interprets the tool-specific binding; Server does not acquire
a dependency on concrete tool adapters just to implement a universal loop.

Satisfied requires an independently verified actual preparation, authorization,
start and Completed receipt, a non-error ToolResult, matching physical target,
and matching prepared content/result digest and byte length. Display paths and
model assertions cannot substitute for physical resource binding. Unknown
checker revision, missing proof or incomplete inspection fails closed.

Runtime owns the first released proof-read slice. Add a read-only
`inspect_effect_proof` entry in a dedicated module, internally reusing
`reconciliation::receipt::PreparedEvidence::from_facts` and `validate_receipt`.
It must not inspect through an executor, reauthorize, append or execute work.

The host request supplies ExecutionKey, exact effect ID, an exact physical
terminal coordinate (event ID and cursor), and explicit input/result byte bounds.
The reader independently loads execution admission, preparation, authorization,
start, receipt and terminal using exact IDs. Verify kind, execution/Turn identity,
payload binding and strict cursor order: admission < prepared < authorized <
started < receipt < terminal. It verifies the terminal coordinate, not an
untrusted bare cursor. Initial terminal kinds are actual stopped Runtime facts;
Server subsequently requires the appropriate verified successful attempt.

Expose an opaque, non-deserializable verified result with read-only preparation,
scope, definitive result and source-coordinate accessors. Do not expose a grant
issuance or execution method. A verified Failed receipt is distinct from an
uncertain/missing receipt; neither proves goal satisfaction. This proof alone
does not establish Task membership, checker acceptance or a goal verdict.

Bound effect/request identities according to existing valid identity contracts;
bound every loaded evidence payload before deserializing, including admission
and terminal. The caller may tighten limits but cannot exceed documented host
hard ceilings. No fallback to full-history unbounded reads. Report invalid
request, missing evidence, binding/ordering mismatch, bounds and storage failure
distinctly. Synchronous ledger access must be moved off async workers by hosts.

Add separate data-driven Runtime tests for valid completed/error receipts,
missing facts, wrong kind/identity/Session/Turn/Step, changed grant or revision,
changed result digest, cursor ordering, foreign terminal, bound limits and
SQLite reconstruction. Export all actual observations before comparisons;
prove zero writes/executor calls. This is accepted only as a prerequisite slice,
not as completed goal verification or production Agent delivery.

### Task integration and correction ownership

#### Concrete Task contract for the next implementation slice

The following contract is proposed for independent review before releasing the
Task write set. It extends the existing entities rather than introducing a
second goal journal or loop. The Runtime effect reader is its prerequisite.

```rust,ignore
struct GoalCheckerKey { kind: String, revision: String }
struct GoalCriterion {
    id: String, invocation_id: String, checker: GoalCheckerKey,
    predicate: serde_json::Value, predicate_digest: String,
}
// Add Goal(GoalCriterion) to CompletionCriterion.
enum GoalVerdict { Satisfied, Unsatisfied, Indeterminate, CheckerFailed }
struct GoalAssessment {
    criterion_id: String, predicate_digest: String,
    checker: GoalCheckerKey, attempt: AttemptBinding,
    source: ExecutionEvidence, verdict: GoalVerdict,
    reason: String, proof: serde_json::Value,
}
trait TaskGoalVerifier: Send + Sync {
    fn validate_criterion(&self, criterion: &GoalCriterion) -> Result<(), TaskError>;
    fn verify_assessment(&self, snapshot: &TaskSnapshot,
        assessment: &GoalAssessment) -> Result<(), TaskError>;
}
```

GoalCheckerKey names an exact host-registered implementation, never a model
plugin or arbitrary executable. The registry rejects unknown kind/revision and
validates the bounded predicate at registration before admitting an invocation.
The predicate digest is SHA-256 of the compact serialized immutable predicate;
the criterion ID, owner and checker key are also bound by the admitted Task.
No model may alter the predicate or select a replacement checker on recovery.

TaskGoalVerifier is an enforcing host port, not an observer. TaskCoordinator
receives its configured implementation through an explicit constructor/builder.
Without that implementation, any Task containing Goal criteria is rejected;
existing explicitly execution-only Tasks remain valid and are not silently
converted to semantic goals. The concrete implementation independently reloads
Runtime source evidence and invokes a registered deterministic checker.

Append and replay both enforce the port for registration, assessment and Task
completion. The port receives the already-replayed Task prefix, so its Runtime
reader must not call TaskCoordinator::snapshot or a helper that does so. That
would recursively verify the same assessment. Source loading uses exact lookup
or bounded query, not a full-history fallback. Physical Task membership and the
exact ExecutionBound/terminal/result binding are rechecked before a verdict.

Add a critical TaskEvent::GoalAssessed and a derived assessment history to
TaskSnapshot, with the exact journal FactRef for each assessment. Its causes
include the preceding Task fact and exact stopped-attempt observation. The
assessment is separate from AttemptOutcome::Completed: no retrofit changes the
physical attempt or its usage. An assessment cannot masquerade as an ordinary
AttemptObservation or mark_recovery event.

The first integration assesses only the criterion's exact invocation and latest
physically completed attempt. No child or ordinary Continuation can satisfy it.
The later correction slice extends this through explicit persisted correction
edges; it must not simply remove this ownership check. Multiple identical fact
retries remain idempotent; a changed payload under one fact ID conflicts. A new
assessment after Satisfied is rejected. A later non-satisfied assessment requires
new admitted correction evidence, not a different narrative about the same run.

Add CompletionEvidence::GoalSatisfied with criterion ID, physical source,
assessment FactRef and digest. It is accepted only when it exactly references
the latest independently reverified Satisfied assessment. Physical completion
records do not fabricate it. The reducer gathers this evidence separately from
attempt observations and requires it alongside all existing physical and child
consumption checks. Public complete_task and replay cannot bypass this check.

WaitingReason gains an explicit GoalAssessment/GoalUnmet reason. A completed
physical attempt with no assessment or a non-satisfied verdict does not project
as ordinary Ready. Cancellation and terminal states still dominate. Initial
goal evaluation returns this waiting disposition; automatic correction is not
enabled until the separate persisted budget/edge slice is verified.

Hard bounds for this slice: at most 128 Goal criteria, 64 KiB per predicate,
64 KiB per assessment proof, 8192 UTF-8 bytes per reason and 1024 referenced
source coordinates per assessment. Existing stricter transport/journal bounds
still apply. A bounded exhausted search is Indeterminate, not evidence that no
effect occurred. Proof shape, source coordinates and verdict must be recomputed
by the concrete reader/checker, not accepted from caller JSON.

Server owns these types and enforcing transition hooks, with no concrete Tools
dependency. The Agent host composes the first FileWriteCommittedV1 checker and
the Runtime reader. Its predicate binds workspace and parent directory identity
chains, leaf, expected UTF-8 byte length and lowercase SHA-256, excluding old
target inode and display-path aliases. It compares both prepared write content
and definitive result; a forged result hash, is_error flag, unrelated effect,
uncertain receipt or physically non-successful attempt cannot satisfy it.

The first Task integration dataset includes missing checker, unknown revision,
malformed/digest-mismatched predicate, forged Satisfied input, missing/foreign
proof, wrong terminal, definitive failure, exact success, duplicate/conflicting
assessment, direct coordinator completion bypass, replay tampering and
reconstruction between assessment and Task completion. All actual rows and
full source content are exported before comparison. Host live tests and bounded
correction remain required subsequent consumers; this slice alone does not
complete requirement 0035.

Persist goal assessments separately from already-stopped attempts. Reuse Task
criteria, journal and completion; do not create a duplicate GoalTask entity.
New critical goal facts require independent source validation both on command
append and replay, including public coordinator completion paths. A caller's
serialized assessment cannot bypass the TaskExecutionService check.

The successor contract must bind root goal anchor, completed predecessor,
Unsatisfied assessment references, exact criterion IDs, successor invocation
and persisted correction ordinal. Ordinary continuations and unrelated children
are not correction evidence merely because they share a Task. Preserve source
and result-consumption proofs, shared budgets and cancellation boundaries.

Unsatisfied with admitted correction available yields an explicit goal wait;
without correction or with exhausted budgets it yields goal failure. Indeterminate
and CheckerFailed block success and automatic correction, with their own reasons.
Do not alter a completed attempt via mark_recovery or project these states as
ordinary Ready. Existing TaskLimits has no Task-total Step/elapsed ceiling;
such correction ceilings must be implemented, not assumed present.

Explicit physical Refused is already excluded from successful FinalAnswer
evidence. Ordinary FinalAnswer text that expresses refusal or falsely claims a
write can still pass execution-only criteria; the new goal gate rejects a
missing required effect in either case.

1. Settle the concrete predicate/evidence schema and test matrix using the above
   source seams. Review how assessment integrates with Task replay/completion;
   do not add an advisory verifier that the actual success path ignores.
2. Implement deterministic goal assessment, durable binding and explicit host
   result classification. Migrate all callers deliberately; retain old scenarios
   and assertions without compatibility adapters or hidden defaults.
3. Implement bounded correction with existing continuation proofs and budgets.
   Add restart/cancellation cases before enabling automatic correction.
4. Assemble the production host and one-shot CLI, then expose the minimal Server
   Agent operations with a separate documented protocol/schema increment.

Each increment requires its concrete decisions and case data before coding.
This draft establishes requirements; predicate shapes and host API names are
not settled by the conceptual labels above. Record those decisions here before
the corresponding implementation begins.

## Acceptance

Add separate module tests and data-driven integration/live fixtures. Preserve
existing execution-only, delegation and approval tests. Export actual input,
result, assessment, correction, receipts and fact coordinates before comparing.

Required cases include satisfied and unsatisfied deterministic goals; final
refusal; missing/wrong/foreign evidence; unknown predicate; checker error;
changed contract/verifier; duplicate assessment; recovery after assessment but
before Task completion; permitted continuation vs unrelated source; corrective
success; exhausted correction budget; cancellation; long approval/rebuilt host;
uncertain effect and no duplicate receipt-backed effect.

For the integrated host, both authorized MiniMax protocols must drive real
single/multiple-Step tasks and actual tools, with normal, denied, malformed,
goal-unmet and correction scenarios. Observing a final answer or test-host
script is not production-host acceptance. Each row states what it exercises.

Completion requires source-level integration through Task success, retained
negative cases, a working real host, offline regression and actual-model evidence.
A pure evaluator, interface, CLI stub or passing JSON-shape test alone is not
the requested business-goal capability.
