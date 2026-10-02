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

The exact C1 Rust DTOs and narrow checker-interface migration will be frozen
after C0 and production opening admission integrate. No implementation may bypass
the current root-only checker contract before that migration. Complete all C1
constraints before enabling automatic correction in the Host.

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
