# Bounded goal correction

## Purpose and delivery boundaries

This increment implements E1 of [0031](0031-agent-evolution-program.md), following
the goal contracts in [0035](0035-goal-verification-and-agent-host.md). A stopped
FinalAnswer with a verified Unsatisfied goal can lead to explicitly admitted new
work. It cannot rewrite the old attempt, turn uncertain effects into safe retries,
or declare success from the model's answer. The goal definition remains immutable.

Delivery has three enforcing increments: C0 durable execution budgets, C1 exact
correction edges and goal evaluation, and C2 Runner/production-host orchestration
with real-model acceptance. C0 alone is not goal-correction acceptance. The design
below distinguishes existing interfaces from new APIs being implemented.

## Verified implementation seams

TaskCoordinator currently shares command admission and replay through one reducer
and FactJournal CAS. Goal assessment is restricted to the latest completed Root;
another assessment for the same criterion is rejected. TaskExecutionService::complete
returns GoalUnmet waiting rather than automatically failing or correcting it.
Agent finalization rechecks terminal and result-consumption evidence before
assessment. Continuation requires a completed predecessor and explicit consumption.

TaskLimits currently bounds attempts, invocations, tokens and Steps per Turn,
not Task-total Steps or elapsed time. TurnExecutor::with_absolute_deadline_at_ms
already intersects a durable cutoff, and checkpoints retain their original cutoff.
Neither an in-memory semaphore nor a new relative timeout is a Task-wide budget.

## C0 durable execution budget contract

Introduce TaskExecutionBudgetPolicy with explicit id, revision,
max_reserved_steps and deadline_at_ms. IDs are bounded nonempty identities;
both numeric limits are positive. Host configuration chooses the absolute Unix
cutoff once. The policy is a ceiling, not permission to call a model or tool.

TaskCoordinator::configure_execution_budget publishes one immutable critical
task.execution_budget_configured fact after Task registration and before any
attempt starts. It may follow inventory admission, but cannot retroactively
budget executed work. A repeated identical fact is idempotent; another policy
fact, changed revision or changed limits is a conflict. Not configuring this
optional capability explicitly means no Task-total reservation policy, not a
compatibility branch or an inferred unlimited budget for a configured Task.

TaskSnapshot reconstructs policy/reference and an exact map of attempt bindings
to positive reserved max_steps. A new start_budgeted_attempt command atomically
checks all existing attempt constraints, reserves the entire planned Turn ceiling
and starts the attempt in the same CAS. Sum reservations with checked arithmetic;
root, child, continuation and safe retry all share one pool. The ordinary
start_attempt command is rejected when the policy exists. No hidden zero-cost
start or caller-supplied serialized budget proof is accepted.

This first policy charges reserved slots, not measured usage. It does not refund
unused slots, timeouts, cancellations or a crash before execution. Conservative
debit deliberately bounds actual Steps without guessing the outcome. Concurrent
starts use the same CAS; a losing writer must reload/replan explicitly, never
execute from its failed admission. A later verified release policy is a separate
increment, not part of the first contract.

TaskExecutionService::run consumes the reconstructed policy. Before any model or
tool work, it caps the Turn ceiling by the remaining pool and per-Turn limit,
rejects zero remaining slots, and uses start_budgeted_attempt. The executor's
absolute cutoff is intersected with the saved policy. Session/Runtime then
persist and enforce that effective cutoff through the existing deadline anchor.
No new model loop, database or distributed lease is introduced.

Resume and approval acceptance require the exact saved reservation/binding and
checkpoint max_steps no greater than the reserved ceiling. A configured policy
requires a checkpoint cutoff no later than its saved absolute deadline. Restoring
the same attempt neither reserves twice nor renews the deadline. Historical
queries and persistent approval rejection do not execute work and must remain
possible after expiry; C0 cannot block the separate pure rejection path.

New critical facts include the policy reference and actual invocation source as
causes. Replay validates the same arithmetic, envelope, source, reservation and
attempt transitions as append. Missing/corrupted/foreign sources and integer
overflow fail without partial durable writes. Policy facts do not authenticate
arbitrary untrusted storage or grant external effects.

## C1 exact correction edges

Correction admission binds the immutable Root anchor, latest completed evaluated
predecessor, exact Unsatisfied assessment references and digests, selected
criterion IDs, successor definition/source, policy revision and sequential
correction ordinal. The source also retains predecessor completion, required
context and result-consumption causes. One atomic fact admits the edge and the
successor; two competing successors cannot spend the same ordinal.

Only Unsatisfied with complete trustworthy source coverage is correctable.
Indeterminate, CheckerFailed, a physical refusal, missing sources and an unknown
model opening/effect block automatic correction. Actual opening proof is required
according to 0034, not inferred from zero tools. Storage corruption is an
operational error, not feedback asking the model to fix the goal.

An ordinary Continuation cannot acquire goal ownership by sharing a Task. Goal
evaluation must select only the current explicitly admitted correction owner for
each criterion, retain all older assessments and reject stale success evidence.
The checker receives an authenticated anchor/evaluation binding; changing a
criterion's invocation_id in a temporary clone is not a valid ownership proof.
Correction must not widen the saved Agent definition, permissions or Skill/MCP
bindings, and rebuild must recheck the current execution authority.

The following C1 contract is approved for isolated implementation. Production
opening admission remains an integration prerequisite, not a reason to defer
disjoint policy and ownership work. No implementation may bypass the current
root-only checker before the complete interface migration. C2 is not enabled
until the enforcing C1 contract and production authority adapter are verified.

### Policy and exact public inputs

New DTOs reject unknown fields. GoalCorrectionPolicy carries policy_id,
policy_revision, root_invocation_id and max_corrections. Identities follow the
existing bounded identity rules; max_corrections is 1..=128. Configure through
TaskCoordinator::configure_goal_correction(task_id, fact_id, policy), after Task
registration and before the first invocation admission. All Goal criteria must
anchor the same declared Root. The later real Root must match that immutable
anchor and the registered Task Agent/constraints. Identical fact retry is
idempotent; another policy/revision/anchor/limit conflicts. Absent policy means
correction admission is unavailable, not unlimited. Configuration grants no work.

GoalAssessmentSelection contains criterion_id, assessment_reference: FactRef and
assessment_digest. GoalCorrectionAdmission contains root_invocation_id,
predecessor: AttemptBinding, ordinal: u32, unsatisfied: Vec<GoalAssessmentSelection>,
successor: InvocationDefinition and policy_reference: FactRef. The host submits
it through TaskCoordinator::admit_goal_correction(task_id, fact_id, candidate).
Selections are nonempty, sorted and unique, at most 128; digests are 64 lowercase
hex characters. Configuring correction for more than 128 Goal criteria refuses
rather than truncates. The serialized candidate is bounded to 256 KiB; input
source bodies retain their existing independent 16 MiB limit and are referenced,
not duplicated in the edge. Submitted DTOs are candidates, not authority.

GoalEvaluationOwner explicitly distinguishes Root { invocation_id } from
Correction { invocation_id, ordinal, edge: FactRef }. GoalEvaluationCoordinate
contains root_invocation_id and owner. GoalAssessment requires evaluation with
no default; its digest includes the coordinate. Root uses the explicit Root
variant. Preserve existing attempt/source/predicate/checker fields. Do not accept
old assessment formats, manufacture an edge or temporarily rewrite a criterion.

TaskSnapshot reconstructs a required nullable policy with exact reference,
correction-edge history and a criterion-owner map. Registration initializes
None/empty; actual Root admission initializes owners even when correction is
not configured. State comes only from committed facts.

### Atomic admission and replay

New critical Task events are GoalCorrectionConfigured and GoalCorrectionAdmitted,
with kinds task.goal_correction_configured and task.goal_correction_admitted.
Their subject kinds are task.goal-correction-policy and task.goal-correction;
subject IDs are task_id and successor.invocation_id respectively. Use the
existing versioned envelope, reducer and journal CAS. A single admission fact
validates the edge, admits the successor, spends the ordinal and transfers the
selected owners. Do not separately append InvocationAdmitted for that successor.

Admission and replay enforce the same invariants:

- The Task is active with no cancellation/recovery barrier; the exact policy
  reference belongs to the observed prefix. Ordinal is the last committed
  ordinal plus one using checked arithmetic, within the immutable ceiling.
- The unique actual Root matches the policy and immutable criteria. All selected
  criteria currently belong to the same predecessor's latest Completed attempt,
  whose exact binding and source match its physical observation.
- Every selection identifies that owner's latest verified Unsatisfied assessment
  with exact reference/digest/checker/predicate and complete source coverage.
  Satisfied, Indeterminate and CheckerFailed cannot transfer ownership. Different
  current owners require separate successors, not an implicit merged predecessor.
- The new successor is a Continuation, parented to and dependent only on the
  predecessor, with an already committed Derived input source. It obeys existing
  invocation/topology bounds. Ordinary Continuation never transfers goal ownership.
- The source retains exact policy, assessment, predecessor-completion, original
  initialization/ownership and frozen context/projection provenance. Source
  publication precedes edge publication. Full reference identity and actual
  committed contents are checked, not just fact IDs.
- Correction does not consume the predecessor result. Actual start still requires
  exact result consumption, private initialization and a new C0 budget reservation.
  It cannot replay effects from the stopped attempt.

Required edge causes include the prior Task fact, policy, Derived source, selected
assessments and predecessor completion. Existing input-source admission and
causal-closure checks must recognize this atomic event. Same fact ID/payload
retry resolves to the committed state; different payload conflicts. Concurrent
different candidates for one ordinal have at most one winner. Return CAS Conflict
without implicit retry or execution; read-only reload may identify the winner.

### Strong model and effect source verification

GoalSourceReader and LedgerTaskGoalVerifier require the actual FactJournal and
explicit ModelOpeningInspectionLimits in addition to the existing source limits.
No no-op store or default proof is permitted. Use Runtime inspect_model_openings
over the reader's verified exact scoped observed end, including event ID and
cursor. Preserve terminal identity and post-terminal activity checks. Every
actual request must have a matching completed preparation/opening/Step chain;
the final FinalAnswer must match it too. Empty Steps or zero tools are not proof.

NotAdmitted refusal, Uncertain, missing/unknown/corrupted evidence or unresolved
effects cannot make work correctable. Storage, identity and protocol failures are
operational errors, not a new Unsatisfied assessment. Exhausted bounded reading
may yield explicit Indeterminate but never infer absence or authorize correction.
Historical complete business mismatches remain Unsatisfied; later physical file
changes cannot rewrite the old assessment.

VerifiedGoalSource privately retains VerifiedModelOpenings. Assessment stores
bounded recomputable opening coordinates/digest separately from checker proof:
input admission, inspected-through coordinate and ordered preparation/opening/
completion references. The reader supplies them; replay recomputes them. Caller
proof JSON is never promoted to model-opening authority.

VerifiedGoalEvaluationBinding has private fields and no Deserialize. Only the
verifier constructs its immutable criterion Root anchor, exact evaluated attempt
and Root/correction coordinate. GoalChecker::assess receives the original
GoalCriterion, this verified binding and VerifiedGoalSource. FileWriteCommittedChecker
checks the immutable Root anchor and exact source/evaluation attempt before
selecting receipts. It does not borrow another invocation's successful write.

### Definition authority and current execution authority

Add an enforcing GoalCorrectionAuthorityVerifier::verify_candidate(prefix,
candidate, verified_input_source) port, configured by the coordinator's explicit
with_goal_correction_authority builder. Configuration, correction admission and
correction replay refuse absent implementation. The port validates immutable
definition and source commitments; it must not consult current ACL during
historical replay. Actual start independently checks current execution authority.
Revocation must not make past facts unreadable or prevent persistent Deny.

Server does not decode private Agent sources or treat equal definition_id as
permission equality. The Agent adapter verifies exact definition revision,
effective authority ceilings, original Skills/MCP bindings and private input
initialization/context. No expansion is permitted. Instances may differ, so their
complete constraints digests need not be equal. A synthetic Server test verifier
is explicitly not production Agent-authority acceptance. C2 requires the real
adapter and guarded Runner/Host assembly before automatic dispatch is enabled.

### Latest owners and retained history

Each owner generation may publish at most one assessment per criterion. Admission
requires the current owner; an older owner cannot publish delayed success. An
edge immediately makes selected criteria wait for the new owner. Unselected
criteria retain their owners and evidence; history is never deleted. Completion
requires exact current-owner Satisfied assessments, all admitted invocations
physically completed, required results consumed and complete strong sources.
Replay evaluates each assessment against its historical prefix; current
completion evaluates current owners. Ordinary Continuation cannot contribute
GoalSatisfied simply because its file happens to match.

### Implementation ownership and mandatory cases

The isolated Server policy/edge slice owns new tasks/correction modules and
independent tests/data, plus narrow tasks.rs, tasks/types.rs, tasks/coordinator.rs,
tasks/reducer.rs, tasks/input.rs and tasks/reducer/completion.rs changes. It does
not modify Sagan's Session/service files or Runtime. The second slice owns
goal-source, goal types/registry/transitions and independent opening tests.
Main coordinates the Agent checker signature and all constructor/fixture
migrations, preserving old assertions. Shared public exports are narrow Main
changes; Runner/Host orchestration remains C2, not an implicit extra loop.

Both Memory and reopened SQLite must exercise pre-work policy, exact retry,
changed/late policy, Root mismatch, two corrections to success, partial criteria,
ordinary Continuation, stale/foreign assessment, source corruption, permission
expansion, ordinal/Steps/deadline exhaustion, cancellation and recovery barriers.
Add missing/unknown/uncertain opening/effect, refusal, post-terminal activity and
read-bound exhaustion. Real CAS barriers cover same-candidate idempotency and
different-candidate single winner. Rebuild cuts surround source/edge/start/
assessment publication. Export full candidates, sources, facts, physical evidence
and results before assertions; sync/close and physically reread every actual row.
Rejected admissions create no new fact, instance, grant, model or effect. C1
tests cannot claim production orchestration or real-model C2 acceptance.

## C2 production orchestration and outcomes

The Host creates a new bounded successor through the existing Runner, private
context and Task/Session/Runtime chain. Feedback contains the verified mismatch,
source coordinates and permitted work, never a fabricated tool result or grant.
Correction count, Task-wide reserved Steps, original deadline, invocation/attempt
and observed token ceilings remain independently enforced.

Verified success completes the Task from latest exact evidence. Remaining admitted
correction work is durable waiting; exhausted correction limits produce a durable
goal failure. Rebuild recovers the exact edge/attempt/checkpoint and cannot start
the correction twice. Cancellation forbids new admission; long approval waits
retain original budgets. Confirmed effects stay recorded and are not replayed as
whole-attempt retries. Self-development experiments are not correction acceptance.

## Data driven acceptance

C0 adds independent module data for configuration, exact limit, exhausted pool,
per-Turn ceiling, overflow, changed policy, late configuration, unbudgeted bypass,
changed binding, cancellation, concurrent CAS and Memory/reopened SQLite replay.
Service integration must prove the actual capped Runtime input, model-opening
count, checkpoint ceiling, unchanged resume reservation and absolute cutoff.

C1/C2 add satisfied Root, explicit mismatch then corrective success, wrong/stale/
foreign assessment, ordinary continuation rejection, immutable goal identity,
multiple criteria, exhausted ordinal/Steps/deadline, uncertain effects/opening,
rebuild cuts around edge/admission/assessment, approval acceptance/rejection and
cancellation. Native tools verify exact bytes and retained effect receipts.

Live MiniMax cases cover both supported protocols and named/inline definitions.
Inputs cause model calls; tests never insert tools into model responses. Each row
exports actual requests/events/results, original and correction facts, independent
goal proofs and physical file postconditions. Test code syncs/closes JSONL and
physically rereads all rows before comparison. Preserve old cases and failures;
offline fixtures are not live-model evidence. Secrets and raw private logs stay
outside Git. No passing subset substitutes for the complete specified increment.

## Ownership and build discipline

Main owns C0 Task budget/reducer/source/export and TaskExecutionService run/resume
hunks, new independent tests and final integration. Production Host and persistent
denial are a separate worker write set; merge shared files by narrow hunks only.
Opening writer and proof consumers remain 0034's write set. Hooks use 0039.

Use one fixed target per active write set, serial Cargo gates and parallel code
development. Follow the single-source build-cache rules in code-conventions.
Module, relevant service integration, strict all-target checks and normal hooks
precede each bounded commit. Full regression and live C2 gates remain required.

## C0 implementation and verification

2026-10-03: the durable policy, atomic reservation/start, mandatory configured
budget path and exact resume ceiling are implemented. C1 correction ownership
and C2 automatic orchestration remain unimplemented. This is not a live-model or
self-development acceptance claim.

Server module gate `/tmp/kolyan-task-budget-main-module-v2.log` exited 0 with
126 passed, zero failed. The new independent matrix physically reread 20 cases
for each of Memory and reopened SQLite. Two additional real CAS races synchronized
writers after validation and before append: exactly one reservation succeeded,
the pool was spent once, and removing the exact policy cause made replay fail.
The underlying journal remained unchanged during the read-only corruption probe.
Arithmetic boundaries cover the full unsigned policy limit and over-reservation;
they do not claim to construct billions of reservations to trigger a sum overflow.

Service gate `/tmp/kolyan-task-budget-main-service-v5.log` exited 0 with all seven
exported rows reread and compared. It uses the actual Task/Session/Runtime,
SQLite journals, filesystem sessions and governed file tool with an explicitly
synthetic model. Cases cover capping 8 requested Steps to 4, a completed Root
followed by a Continuation capped to the remaining 2, exhausted successor
admission, zero model calls after expiry, approval reconstruction without a second
reservation, expiry during approval, and a suspended Root sharing 4 plus 2 slots
with a real SelfCall before consuming the child result and resuming. The two
successor rows retain `child` as their invocation ID; their actual role is
Continuation, distinct from the seventh SelfCall case.

Actual service evidence is retained at
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-task-budget-service-RMgmnf/actual.jsonl`.
Each row includes full requests, budget facts, input admission, execution events,
checkpoint and physical file content. All child errors are exported and checked;
filesystem errors other than NotFound fail rather than becoming an absent file.
An expired initial attempt conservatively retains its reservation and a binding
fact, but no input admission or model request. This does not fabricate a stopped
attempt; reconciliation remains the existing Server responsibility.

Strict workspace/all-target clippy exited 0 in
`/tmp/kolyan-task-budget-main-strict-v1.log`. Failed service runs v1 through v4
are retained: the fixes used existing DTOs, corrected invalid test topology and
distinguished binding from execution admission. No production result-consumption
guard or existing test assertion was relaxed. Main target measured 16 GiB with
560 GiB free; no cleanup was necessary or performed.

Full workspace regression exited 0 in
`/tmp/kolyan-task-budget-main-workspace-v1.log`: 84 test groups, 881 passed,
zero failed and 66 explicitly ignored. Ignored vendor-network cases were not
executed. Formatting, source layout (via `sh`) and diff checks each exited 0.
Normal commit hooks remain a separate gate. Strong model-opening consumers,
forged-checkpoint integration negatives and C1/C2 remain
required before reporting the complete goal-correction increment as accepted.

## Exact resume budget validation

An additive private module matrix starts an actual TaskExecutionService attempt
with configured policy and atomic reservation. Runtime produces its approval
checkpoint; the original service is dropped before reconstructing the checker.
Memory retains the actual backing state; SQLite connections are reopened with
the same filesystem SessionStore. Only a submitted checkpoint or binding copy
is changed; original execution events, source journal and Session are not altered.

Thirteen cases per backend cover exact reservation, an earlier original cutoff,
absent reservation, changed invocation/execution/Session/Turn/Agent/constraints/
source, excess Steps, absent cutoff and widened cutoff. Every submitted checkpoint
must remain structurally valid. The helper stays private. All rows export the
original suspension, submitted copy, requests, snapshots, Task facts and execution
events before physical readback and assertions. Validation must open no additional
model request, prepare or execute no tool, and append no fact or Session update.
This checks the private budget validator with a real Runtime-produced checkpoint,
not the actual Service approval-resume negative path or native/live-model behavior.

### Approval resume consumer validation

The next separately implemented test uses a read-only LedgerStore decorator that
delegates real events unchanged except a selected exact suspension's budget fields
in the returned read payload. It must not alter the source database or fabricate
a checkpoint. Rebuilt TaskExecutionService must reject oversized Steps and absent
or widened cutoffs through its actual resume_approval entry point before any new
grant, model/tool activity or Task/Session writes. Include an unchanged positive
consumer control, reject ambiguous read coordinates, and export source events,
observed payloads and results before assertions. This consumer gate is now
integrated and locally verified below. No production
visibility expansion or new public consumer API is needed.

### Main validator verification

Main verified all four frozen file digests before integrating the independent
tests. No existing assertion, production helper implementation or visibility was
changed. `/tmp/kolyan-task-budget-resume-main-module-v1.log` exited 0: Server
128 passed, zero failed and zero ignored. The 26 rows were physically reread at
`/var/folders/0p/65d_m6956tj7726tbvdgr2gh0000gn/T/kolyan-task-budget-resume-I1bgob/actual.jsonl`.
Each backend has two accepted and eleven rejected validation cases; every row
retains exactly one initial model request, zero effects and unchanged Task facts,
execution events and Session. Earlier-cutoff evidence was produced by the initial
Runtime execution, not fabricated in the reconstructed checkpoint.

Workspace/all-target strict Clippy exited 0 in
`/tmp/kolyan-task-budget-resume-main-strict-v1.log`. These are offline validator
receipts, not approval-resume consumer or MiniMax acceptance.

### Main approval resume consumer verification

Main verified the three frozen consumer source/data digests before import. The
existing validator file adds only `mod consumer`; production code and old
assertions are unchanged. The actual Service.resume_approval path reconstructs
from Runtime-produced suspension with Memory or reopened SQLite and a real
filesystem SessionStore. Counted synthetic Provider/Tool ports are explicit;
this is neither native-tool nor actual-vendor acceptance.

The read-only observer changes only one exact returned suspension budget field.
It never patches source events or the database. Missing/widened cutoff and excess
Steps are rejected by Runtime's admitted-ceiling/content checks before the Task
budget helper. Foreign or multiple observer coordinates are observer refusals,
not claims that a production budget predicate rejected fabricated history.

On 2026-10-03 `/tmp/kolyan-budget-consumer-main-module-v1.log` exited 0 with
129 passed, zero failed or ignored. Main physically reread all twelve new rows
at the path printed as TASK_BUDGET_CONSUMER_EVIDENCE in that log. Two unchanged
positive controls really execute the approved tool, request a final answer and
load verified physical completion. Ten negatives cause no additional model,
preparation/effect, ledger append/claim, Task fact or Session update. The original
reservation and policy remain unchanged; positive history only appends new
completion facts. JSONL is synchronized and closed before comparison.

The joint frozen Skills/approval-consumer source passed workspace all-target
strict Clippy with exit 0 in `/tmp/kolyan-skills-budget-main-strict-v1.log`.
Full workspace regression exited 0 in
`/tmp/kolyan-skills-budget-main-workspace-v1.log`: 90 result groups, 900 passed,
zero failed and 69 ignored, including helper-subprocess result groups. The
ignored vendor entries were not run by this gate. Formatting, source-layout and
diff checks exited 0. C1/C2 correction ownership and orchestration remain open.

### Actual feedback motivating correction ownership

The separate Skills C MiniMax run recorded one completed FinalAnswer whose file
write included literal surrounding quotes: 22 bytes rather than the immutable
20-byte goal. The actual receipt, readback and Unsatisfied assessment agree, and
the Task remains Waiting. This is correct refusal of false completion, not C1/C2
acceptance. Preserve the complete failed row under requirement 0036. C1 must
admit new work from exact trustworthy assessment/opening evidence, rather than
rewriting the original tool arguments, relaxing the goal or replaying the attempt.
