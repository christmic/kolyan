# 0016 Turn boundary control and unified continuation

Status: implemented. Supersedes the split ordinary/resumable execution paths
in 0011–0015. Verification results, including observed live-model failures,
are recorded in [testing strategy](../architecture/testing-strategy.md#0016-turn-边界与恢复收尾验证).
Session persistence and distributed scheduling remain outside this increment.

## Problem and scope

The ordinary loop already supports tools, limits and local cancellation. The
approval path duplicates that loop: it rejects ordinary tool batches after
resume, repeats step numbers, does not enforce the step limit on resume,
restarts the initial deadline at suspension, and serializes approved batches
without the configured error policy. Fix these as one execution contract.

Core continues to own Step/Turn semantics. Runtime owns durable commands,
storage, execution ownership and recovery of uncertain effects. Execution is
a runtime attempt identity, not a fourth layer alongside Session/Turn/Step.

## One execution engine

All execution entry points use one loop and the same batch dispatcher. An
invocation either finishes, fails, or returns an approval continuation. A
continuation contains the completed model steps, exact pending calls and
assistant content, model context, approved call ids, consumed tool budget,
original absolute deadline, next step index, and dispatch/timeout settings.

- Derive the next step index from completed steps; validate it on resume.
- Enforce max_steps before every model call, including resumed calls.
- Finish handling a completed model's tool batch before returning MaxSteps.
- Calculate the absolute deadline once at Turn entry; waiting for approval
  counts towards it. No deadline means approval can wait indefinitely.
- Check the full pending batch against remaining tool budget before effects.
- Reevaluate policy on resume; approve only the checkpoint-bound invocation.
- Allow any sequence of ordinary batches and approval batches.
- All required approvals precede effects in the batch. FailTurn policy denial
  rejects the batch before effects; ContinueBatch feeds denied calls back as
  error results. Grants never bypass the tool enforcement boundary.
- Approved calls participate in the same resource-conflict scheduling as
  ordinary allowed calls. Parallelism, serial ordering and error policy apply
  identically before and after suspension.
- Serial FailTurn stops after the first failed call; parallel work already
  admitted may finish, but later stages do not start.

## External boundary control

Introduce an optional asynchronous TurnBoundaryControl port. It receives a
Turn id and an explicit boundary: model step, tool invocation, approval
suspension, approval resume, or terminal outcome. The operation is `admit`,
not a separate read followed by an unguarded state update.

An implementation must atomically order cancellation against admission. If
cancellation wins, admission returns Cancelled and Core launches no new work.
If admission wins, that operation may execute; cancellation prevents later
admissions. Terminal admission orders completion against cancellation so a
completed Turn is not retroactively cancelled. Adapter failures fail closed.
Stable step/call identities let a future Runtime bind admission to its own
transaction and execution fencing. Admission alone does not claim exactly-once
external effects; durable receipts and reconciliation belong to Runtime.

TurnControl remains an optional execution-local fast cancellation signal.
It is checked before dispatch and can interrupt model/tool/approval waits.
The external control port supplies authoritative boundary decisions across
executor reconstruction. A lost fast signal does not permit later admitted
work after external cancellation. Core does not poll a database or find PIDs.

The in-memory reference in tests shares state between reconstructed executors
and uses one lock to order cancellation/admission. It tests the contract; it
does not represent durable storage. A synchronous reject/expire helper creates
a terminal proposal; the owning Runtime must serialize competing user commands.

## Events and validation

Model/tool start observations precede invocation. Policy denial emits no tool
start. A resumed invocation does not emit a second Turn Started event. Errors
in the shared engine produce one terminal event before the stream error.
All successful/error tool results retain their originating call id; only a
complete batch is appended to model context in original call order.

Tests are additive and data-driven. Existing cases remain regression coverage;
identifier literals may change when the identity contract is corrected.
The two older policy trajectory contracts are corrected: approval precedes tool
start, and denied invocations have no start event. Their runners and side-effect
assertions remain in place.
Actual JSONL contains model text/reasoning, calls/results, admission boundaries
and approval decisions; expectations compare stable structure and semantics,
not nondeterministic wording. Only test code writes temporary artifacts.

Required deterministic coverage: ordinary → approval → ordinary → approval,
multiple approvals per batch, cross-resume indices/limits/deadline, serial and
parallel batches, conflicting resources, error policies, cancellation before
model/tool/resume/terminal, cancellation versus completion ordering, unavailable
control, and checkpoint structural validation. Required real coverage: all
configured models on both protocol surfaces, including multiple suspensions
with intervening ordinary tools and cancellation while approval is suspended.

## Deferred Runtime work

Durable CancelTurn commands, transactional ledger-before-effect admission,
complete recovery payloads, execution leases/fencing, process crash recovery,
and uncertain tool receipts require a separate Runtime requirement. The
current TurnDriver's post-completion event mapping is not a durable executor.
