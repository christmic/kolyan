# Delegation integration wiring contract

The new `delegation.rs` reads `fixtures/agent/delegation.json`. Its offline gate
uses production Root routing, the attached `AgentChildWaitVerifier`, real isolated
tools, `drive_agent_children`, `consume_agent_children` and `pump_agent_children`.
Offline responses are data scripts, not model-generated network evidence. The
separate ignored `actual_model_delegation_matrix` uses actual Providers for both
parent and child across the common configured matrix. Live results are recorded
separately from offline results; consult the retained matrix report for status.
Existing self/multi planning datasets remain separate and `not_run`.

## Available production operations

The current `AgentRunner` exposes these real operations:

1. `admit_agent_children(owner, invocation, limits, policy)` returns an exact
   durable `ExternalWait`. Authority must originate from the actual parent tool
   dispatch, not from a fixture-issued substitute grant.
2. `verify_agent_child_wait(owner, issued, wait)` returns actual admitted children.
3. `drive_agent_children(owner, issued, wait, template)` drives real private child
   Sessions through Task/Runtime. The template has no messages or schemas and has
   an explicit output limit. Production supports conservative serial dispatch
   and host-attested bounded read-only fan-out under its shared concurrency budget.
   Model parallel intent alone does not authorize concurrency, and writable
   shared-workspace children remain serialized.
4. `consume_agent_children(owner, issued, wait)` loads verified terminal results
   and commits exact result consumption. It does not itself resume the parent.
5. `recover_agent_child_wait(owner, issued)` reads only committed admission and
   never recreates missing work.
6. `resume_agent_child_approval` restores the saved child authority and private
   context through the production approval-resume chain.

## This dataset's acceptance boundary

The production root routing and public `pump_agent_children` operation are now
present and wired through `with_delegation` and the attached verifier. Child
approval resume is not covered by this new single-child dataset; it must restore
the saved child binding without borrowing a root-only approval API. Confirm
additional contracts with the production owner before wiring tests; do not
hand-build an executor or successful pending result to replace them.

This dataset configures exactly one child and `max_parallel: 1`; it does not
exercise production read-only parallel fan-out or child approval resume. Separate
integration datasets must require overlapping actual admitted child execution
intervals under the enforced read-only ceiling, not a `parallel: true` request,
multiple child identities, or concurrent fixture tasks. Production module gates
for these capabilities are not evidence that this single-child dataset covers them.

The sibling `parallel.rs` implements `fixtures/agent/parallel_runtime.json` through
the same durable host and actual OS tool factory. Two named child instances use
one host budget of two slots. Read-only execution is attested by the fixed
`AuthorityTools` adapter's prepared-claim/grant checks plus the production exact
Read operation's no-write sandbox; schema or model intent alone is insufficient.
The writable case still requests parallel execution, but must observe serial
child Turn intervals and a Provider invocation peak of one.

Both cases compare each child's actual receipt separately, unique instances and
private Sessions, terminal consumption, physical contents and parent completion.
Provider observation spans include invocation opening and stream consumption;
they are not a claim of simultaneous network-byte arrival. The read-only case's
first-call test rendezvous verifies real scheduling before releasing unchanged
Provider requests. Shared Ledger cursor intervals independently establish actual
child Turn overlap. The ignored actual-model entry runs both cases across every
configured deployment; offline scripts do not establish network acceptance.

Inline wire comparisons strictly deserialize and validate `AgentDefinitionInput`
before comparing the complete `AgentDefinition`. Missing optional `display_name`
and explicit null both denote None under the declared schema. Required fields,
revision, model, instructions and permissions remain exact; unknown fields fail.
The comparison never modifies recorded model output or normalizes text. Existing
raw-JSON comparison failures remain preserved as historical reports.

The sibling `topology.rs` reads `fixtures/agent/topology.json` for two additional
flows: root-to-self-to-self with only the leaf reading, and two independent
read-only children simultaneously awaiting approval. The same bounded progress
driver traverses actual external waits, drives and joins descendants bottom-up,
and uses `resume_agent_child_approval` after rebuilding all host services from
serialized confirmation input and the immutable worker pin. It does not hand-build
executors, grants or child results. Before confirmation it checks that each child
has no entered effect or receipt; confirming children cannot advance the parent's
ledger. Identity, private Session, per-invocation read receipts and consumption
counts are independent assertions. The ignored live target runs both data rows
across every configured deployment, using actual models for every actor.

These two flows do not establish mixed approval/external waits in one tool batch,
late-child cancellation, arbitrary DAGs or finalization commit-gap acceptance.
Their offline and network reports remain separate; defining a live entry point
does not mean its network matrix has passed.

## Shared offline/live driver requirements

Read prompts, actor definitions, ceilings, faults and expectations from the same
dataset. Host-select the matrix model for all actors. Offline frames are dataset
responses only; live requests must make actual models emit `agent.invoke` without
inserting calls into their responses. Actor labels route fixture responses and
never allocate authority or invocation identities.

Write complete source requests, model reasoning/content, exact preparation and
grant, parent/child ledgers, journal, private bindings, waits, verified terminal
references and physical files to actual JSONL before comparison, including failures.
After a restart, reopen stores and production Runner interfaces rather than
retaining the previous suspension or in-memory child futures. Assert no repeated
parent model request, child effect or result consumption.

Use the existing configured 19-entry common Provider matrix for live named/inline
children, self calls and parent result restart; missing credentials and ordinary
errors fail. Typed failed/cancelled offline cases inject an explicit Provider-open
failure or persist an execution cancellation, never a forged child terminal.
Inspect/Unsupported context
counting remains explicit and proves no strict token budget or compaction.
