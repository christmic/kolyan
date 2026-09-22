# 0011 Durable approval and resume

## Requirement

An approval may remain pending for minutes, hours, or days. A turn must therefore
not depend on a live task, process, `Future`, mutex, or `TurnControl` instance
while it waits for a human decision.

The durable path is:

1. Execute the model step and evaluate the tool batch.
2. On `RequireApproval`, return `AwaitingApproval` with a complete checkpoint.
3. Persist the checkpoint in an application-owned store.
4. End the current task and process if necessary.
5. Load the checkpoint after approval and resume with `resume_approval`.
6. Execute the approved tool exactly once, then continue from the next step.

The existing in-process `TurnControl` approval is retained only as a short-lived
compatibility API for callers that explicitly accept process-local waiting.

## Checkpoint contract

The checkpoint records the turn id, approval id, call id, tool name, exact tool
arguments, argument fingerprint, policy version, model request context, assistant
tool-call content, completed step results, pending batch, and next step index.
It is the source of truth for resumption; the model is not called again for the
step that already produced the pending call.

`resume_approval` validates the approval id, call identity, arguments, policy
version, and current policy decision before issuing a fresh execution grant.
Tampered, expired, denied, or policy-invalid checkpoints fail closed.

## Storage

The first implementation provides an atomic file-backed `ApprovalStore` in
`kolyan-storage`. Applications may later provide a database implementation with
the same interface. The storage layer owns durability; the core owns execution
semantics and never writes files itself.

## Acceptance tests

- approval returns without keeping a turn task alive;
- checkpoint survives task/process boundary through the file store;
- approval resumes and executes the tool once;
- the model is called once before approval and only for the next step after
  approval;
- wrong approval id, tampered arguments, changed policy, missing, and corrupt
  checkpoints are rejected;
- the real provider matrix exercises the durable path for every configured model
  through both OpenAI-compatible and Anthropic protocols.
