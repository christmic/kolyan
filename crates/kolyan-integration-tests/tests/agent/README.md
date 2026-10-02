# Agent acceptance scope

`agent_root` runs a shared data-driven case for exact named and inline roots.
Each case executes two independent Turns in one durable Session, reconstructing
the host Runner between Turns. The offline model emits only the fixture's frames;
file write/edit/read and shell use real isolated production OS workers. Live mode
uses the existing common Provider constructors and every configured combination.
It does not inject tool calls into network responses or skip Provider errors.

Host authorization is explicit in the dataset: file manifests are scoped to
`workspace/safe`; shell is authorized for the entire isolated `workspace` because
its preparation conservatively claims that resource. Shell cwd `safe` is not a
sandbox boundary. Protected host state remains outside that workspace.

`actual.jsonl` retains full actual requests, model events including reasoning,
context preparation, raw execution ledger and coordination journal before result
assertions. Expected data declares physical proof, tool receipts, actual usage and
history expectations. The ordered expected JSONL declares structural milestones.

Context preparation explicitly uses Inspect with an Unsupported counter and no
token estimate. The fixture's inspection window is a **host scenario assumption**,
not verified model capacity. Neither it nor successful requests establish strict
token-budget admission. Provider-reported usage is retained without invented
counts; missing portions remain unreported in Task accounting. No compaction or
production tokenizer is claimed.

The baseline explicitly admits four tools without user approval. A separate case
in `approval_restart.json` requires approval for write/edit/shell. It declares four
pause points across two Turns, expected physical state before each confirmation,
and model/receipt counts. Every pause exports the full execution ledger and Task
journal, drops the Runner, service, Provider and host store handles, then reopens
stores and uses an empty catalog with the actual `AgentRunner::resume_approval`.
Snapshot equality, absence of pre-confirmation effects, exact-once receipts and
actual Provider/Core request equality are checked. This is host reconstruction
with actual OS workers, not termination of a separate Agent process.

The offline script offset is derived from persisted ModelRequested facts. Live
mode never injects or offsets model output. The separate ignored live filter
`actual_model_root_approval_restart_matrix` plans all 19 configured combinations
for named and inline roots. Offline results are not actual-model approval evidence.
`pending.json` retains unexecuted delegation scenarios only. These tests never
bypass the Runner through a hand-assembled resume executor.

Run offline with `cargo test -p kolyan-integration-tests --test agent_root`.
Run the actual-model matrix explicitly with the project credentials and
`cargo test -p kolyan-integration-tests --test agent_root actual_model_root_matrix -- --ignored`.

## Unexecuted next-stage data

`fixtures/agent/self.json`, `multi.json` and `long.json` contain concrete inputs,
authority/scheduling ceilings, expected topology and evidence, and restart/fault
points. They are **planning data only**, with `execution_state: not_run`; no Rust
target or synthetic child runner claims them as passing. Existing root acceptance
does not consume these files. After production child/continuation contracts are
frozen, one driver must read the same scenario inputs and expectations in offline
and live modes. Only offline uses data-declared model outputs/actions; live uses
actual Provider output and all 19 common configured combinations. Actor labels
route fixture scripts after trusted admission, never allocate runtime identities.

Self cases require saved-definition resolution, private contexts and depth/ceiling
denial. Multi cases distinguish actual read-only overlap from shared-workspace
write serialization, and include child approval restart and duplicate result
refusal. The long plan declares ten independent Turns, at least twenty actual
model Steps, UTF-8 inputs and restart at approval/continuation boundaries. Source
byte evidence, trusted token overflow and safe reduction remain explicitly
unimplemented/unverified; neither the planned counts nor fixture file sizes prove
execution coverage or strict token admission.
