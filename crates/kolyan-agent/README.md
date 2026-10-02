# kolyan-agent

Bounded Agent definition/catalog/snapshot and invocation binding support for requirement 0030.
This crate depends on `kolyan-server` for its identity and durable task execution;
Server does not depend on this crate. The root Runner composes that existing
execution chain rather than implementing another model loop.

- `AgentDefinition::new(AgentDefinitionInput)` validates immutable identity,
  optional display name, model reference, instructions and permission ceilings.
  Definition deserialization runs the same checks.
- `AgentCatalog::register` is idempotent for identical content and rejects
  conflicting content at an existing exact ID/revision. Catalogs have an explicit
  bounded capacity; there is no latest-version alias.
- `AgentSelector::Named(AgentKey)` resolves an exact revision. `Inline` carries a
  complete validated definition; a missing display name confers no authority.
- `resolve` validates requested permissions against host/definition intersection.
  `resolve_child` additionally checks exact named target, inline or self authority,
  parent ceiling, current host allowance and child ceiling. Self calls preserve
  exact definition content and require a different instance ID.
- `resolve_self(parent_snapshot, instance_id, host, requested)` is a pure,
  catalog-independent self-admission function. It uses the saved parent's exact
  definition, intersects parent/host/definition ceilings, requires self authority
  and rejects requested expansion or reuse of the parent's instance. Restoring
  a snapshot never selects replacement catalog content for this operation.
  This is admission only: it creates no context, grants or running child and
  does not itself execute a child or allocate a globally unique instance.
- `AgentSnapshot` binds definition, existing Server identity, schema version and
  effective permissions using domain-separated SHA-256. Deserialization verifies
  the binding and rejects permission expansion. Digests provide content integrity,
  **not signatures or authorization from untrusted storage**.

Only `file.read`, `file.write`, `file.edit` and `shell` are environment capabilities.
Delegation is independent and exact-revision scoped. These are static capability
ceilings: resources, sandbox enforcement, dynamic policy, grants, depth/usage
limits and durable result consumption belong to the respective execution layers.
The host must allocate globally distinct instances and authenticate snapshot
provenance. This catalog only rejects reuse of the direct parent's instance ID.

## Durable invocation ownership

`binding::AgentInvocationBindingStore` saves immutable snapshots and context
ownership using the supplied `FactJournal`. Identical saves are idempotent;
conflicting snapshots, foreign logical owners, corrupt facts and oversized
payloads fail explicitly. SQLite restoration needs no live catalog or retained
store instance. Root contexts equal the logical Session; child contexts are
derived from the exact logical Session, Task and invocation tuple. A child cannot
borrow a sibling's private context. These facts are not task admission, approval
or execution grants. The caller must authenticate the journal and admission.

`AgentInvocationBindingStore::load_with_reference` returns the validated saved
binding with its exact committed `FactRef`, without appending or consulting a
catalog. The store implements Server's mandatory `PrivateContextOwnershipVerifier`:
only a saved Child binding with matching logical/task/invocation identity, physical
context, snapshot digest and exact fact coordinate can initialize a private context.
Use it with `PrivateContextService`; Server owns projection digests and Storage's
atomic no-overwrite initialization. This proves ownership, not child Task admission.
Async callers must run these journal/initialization operations on a blocking worker.
The module dataset exports actual Memory/SQLite initialization and refusal evidence
before comparisons. It does not prove child execution or model-network acceptance.

## Lossless context preparation

`context::prepare_context(request, descriptor, policy, counter)` is a pure
function. It validates model identity, known context window, output reserve,
serialized UTF-8 byte bounds, message/block counts, assistant tool calls and
completed user results. Duplicate, missing, orphaned or interleaved tool results
are explicit errors. Reasoning must belong to an assistant and its opaque payload
is retained verbatim; this module does not validate provider-specific signatures.
Cache breakpoints referring to absent sections fail rather than being removed.

Every system instruction, message, tool definition, reasoning payload, cache
configuration and extension is retained. The only possible change is adding the
explicit output-reserve limit when the request omitted `max_output_tokens`.
Provenance records domain-separated source/prepared request digests, versioned
policy digest, counter identity, byte sizes and half-open retained message ranges.
These are neutral request digests, not hashes of provider-mapped wire bodies.

Strict mode requires `ContextTokenCounter` to return a trusted model-specific
count that accounts for the actual provider mapping, framing, schemas and
extensions. The host supplies and authenticates that implementation. Unknown
counts, counter errors and verified overflow fail explicitly, retaining provenance
when bounded serialization succeeded. An unknown model window also fails.
`SerializedByteEstimator` supplies diagnostic estimates only, never a proven
token upper bound. Inspection mode may return `BudgetStatus::Unverified`; that
result must not authorize strict model admission. Serialized byte limits are
enforced independently and capped at 16 MiB.

**Not implemented:** a production provider tokenizer, generated summaries or
arbitrary lossy compaction. `context::project_context` separately supports an
explicit host-owned whole-message selection before admission, preserving required
anchors and complete tool-call/result pairs. It does not silently select a plan
on overflow or turn inspection estimates into a trusted token bound. Strict
admission still requires the host's trusted counter. Implementation and scoped
acceptance are recorded in [requirement 0030](../../docs/requirements/0030-governed-agent-execution.md).

`provider::ContextPreparingProvider` validates every actual `stream` opening and
requires host `ContextRecorder` acknowledgement before calling the inner Provider.
Source, preparation and rejection evidence are typed. It forbids changes to the
neutral request at this boundary, including adding a missing output limit: initial
projection must happen before Turn admission so Core's recorded input remains
identical to the dispatched request. Strict/Inspect mode is explicit; recording
or preparation errors block dispatch rather than being retried as network errors.
Successful requests and the original event stream are passed through unchanged.
Recording proves preparation acknowledgement, not network completion. Durability,
redaction and counter trust remain host-owned.

## Root Runner

`AgentRunner::start(RootRunRequest)` executes a single named or inline root through
the supplied `TaskExecutionService`. The logical Session must already exist.
The host provides durable `InstanceRegistry` and invocation-binding stores, an
exact catalog, current static allowance, `ProviderFactory` and
`EnvironmentToolFactory`. All host admission paths must share the instance
registry journal/namespace. Factories must build actual host adapters; the Runner
provides no production scripted Provider, permissive policy or raw-tool fallback.

The root path reserves a host instance, resolves the exact snapshot, builds guarded
Provider and environment adapters, saves immutable ownership, registers the Task,
admits its root, and calls the actual Task/Session/Runtime/Turn execution service.
Journal admission and factory construction run on a blocking worker. The Task
budget and Session projection remain owned by their existing services. Task
completion is committed only after the service revalidates physical execution
evidence; waiting or failed execution does not create successful Task evidence.
This root convenience path currently registers only the root's
`ExecutionCompleted` criterion. A model's final refusal can satisfy that criterion;
it does not prove that the natural-language objective or a requested tool effect
was achieved. Callers must not interpret this verdict as business-goal validation.

The caller supplies current messages, host instructions and explicit output limit,
but no tool schemas. The Runner selects the definition's model, appends its
instructions, and obtains exact schemas from the host factory. Unknown/duplicate
inventory, missing admitted adapters and a contradictory named/required choice
fail explicitly. Effective schemas are narrowed by the snapshot. A wrapper also
checks preparation and actual execution against that ceiling, host execution
coordinates, snapshot digest and current policy. Hiding schemas is not authority.
Only the four environment tool names are accepted by the environment factory;
the independent delegation router advertises `agent.invoke` only when explicitly
configured and discoverable. It does not borrow `ProcessExecute` authority.

`with_tool_error_policy(ToolErrorPolicy)` is a trusted host choice for new Turns;
the default remains `FailTurn`. `ContinueBatch` uses Core's existing bounded error
feedback so a subsequent model call may propose a new, independently checked
invocation. It does not grant authority, retry effects or suppress fatal errors.
Restoration retains the checkpoint's dispatch policy and remaining budgets, not
the newly constructed Runner's choice. Tool batches remain serial by default;
child scheduling is separate. The exact contract and acceptance are maintained
in [requirement 0030](../../docs/requirements/0030-governed-agent-execution.md).

Every actual model stream is mediated by the factory's required
`ContextPreparingProvider`, so history and budget clipping are checked at dispatch.
The final-request preparation hook **before immutable Session input persistence**
is still a Server coordination dependency: this batch cannot perform a lossful or
request-changing projection there. Factory counter/recorder trust is explicit;
no diagnostic estimate becomes an authoritative token budget.

Repeated admission uses exact immutable facts and the host's original instance.
An already started attempt is not replayed or implicitly resumed. Future drop does
not erase durable facts; the host must reconcile uncertain/stopped attempts through
the existing service. Partial journal admission is not a cross-store transaction.

`AgentRunner::resume_approval` rebuilds adapters from the exact saved root binding
and attempt, checks current host permission ceilings, and uses Server's persisted
approval decision and checkpoint. It does not consult a replacement catalog,
allocate another instance or replay the initial model request. Restored-Runner
module cases cover named/inline resume and changed owner, approval or allowance;
these are not process-restart or actual-network acceptance.

Child initialization, verified typed results, `agent.invoke`, persistent waits,
self recursion and bounded read-only fanout use the production ports described
below. Independently recoverable Task finalization uses the explicit proof-only
entry point described below. Unit
Provider scripts do not establish actual-model network acceptance or full
Agent-loop acceptance.

## Effect-free invocation preparation

`prepare_agent_invocation` strictly parses `AgentInvokeInput`, resolves every
named/inline/saved-self target, and creates a digest-bound `PreparedCall` using
only `AgentDelegate` and `Delegate`. It does not borrow process-execution authority.
Preparation has no registry, journal, Session, model or grant-issuance dependency;
it does not allocate even a preview child instance. Saved-self resolution ignores
mutable catalog replacement content. The existing catalog resolution shares the
same permission checks and retains its instance-reuse rejection order.

The execution binding includes parent ownership/digest, exact resolved definitions,
requested permissions, each explicit private input, parallel intent and trusted
limits. Unknown fields, missing parallel intent, oversized/empty inputs, target or
permission expansion fail. Child count is bounded to eight, individual input to
16 KiB and complete serialized preparation to 64 KiB. Parallel intent does not
change the bound host concurrency or make unknown-resource Delegate calls
batch-independent. Dynamic policy and current scoped grants still mediate execution.
Multiple children sharing a writable workspace must execute serially unless the
host proves disjoint enforced resource scopes. Read-only concurrency also requires
an enforced capability boundary and a stable host concurrency budget. A parallel
intent or independent name alone is not proof of safe concurrent execution.

`src/invoke/tests/prepare.json` defines preparation cases independently of the test
framework. The framework exports all actual inputs/preparations/refusals to a
retained temporary `actual.jsonl` before comparisons. These pure cases cover named,
inline, saved-self and parallel intent, not child execution or network acceptance.
These preparation cases are complemented by the actual service driving and
persistent-wait datasets described below.

## Child admission and explicit host driving

`AgentRunner::admit_agent_children` validates the fresh preparation, current policy
and exact grant against an independently supplied `DelegationOwner`. It checks the
saved parent binding and admitted attempt, then reserves distinct host instances,
admits real Task children and initializes their private Sessions through Server's
mandatory ownership verifier. Only after all initialization steps succeed does it
commit the bounded admission fact and return `ExternalWait` of kind `agent.children`.
Definitions remain in BindingStore; admission references existing binding facts.
This operation issues no permission grant and opens no model stream.

`recover_agent_child_wait` only returns an already committed exact admission;
`verify_agent_child_wait` checks its authority, graph and initialization. Missing
facts are not permission to repeat an entered effect. Partial cross-store admission
remains explicitly uncertain; it is not a transaction or automatic retry.

`drive_agent_children` executes the actual Task/Session/Runtime chain with guarded
Provider and environment factories. The trusted template contains no messages or
schemas: saved selected private input is projected once and inventory comes from
the factory. Existing verified terminal attempts are loaded rather than rerun.
Terminal results retain Server's Completed/Failed/Cancelled types. Suspended work
stops later shared-workspace children; explicit resume/recovery is still required.
Children default to serial, including unproved model parallel requests.
`AgentExecutionBudget::new(1 | 2 | 4)` and `with_execution_budget` install a stable
host budget shared across Runner instances. Genuine fanout requires saved child
permissions containing no Write/Edit/Shell, an explicit trusted factory
`enforces_read_only_parallel` attestation, and an explicitly unlimited task token
budget. Per-invocation prepared `max_parallel` additionally bounds active work.
Pending children release slots; results retain admitted child order and identity.
Writable work stays serial until trusted resource isolation is provided. A
configured token ceiling stays intact and serial until real reservations exist.
Server still enforces depth, invocation, attempt and Step limits and actual usage.

The same execution budget gates root start, root/child approval resume, serial
child dispatch and parent checkpoint resume. Saved child admission binds the
original invocation ceiling across host reconstruction. Current host permission
checks apply to owner verification, including result consumption. Finite token
budgets serialize active attempts across the Task without removing the ceiling;
independent read-only children may all suspend sequentially for approvals.
Permits span one actual service execution through its stopped/usage publication,
not descendant driving or external waits. Service execution futures are boxed at
all five Runner execution boundaries; this does not increase thread stacks or
change cancellation and error propagation.

The recovery dataset exports requests, effects, Task facts, execution ledgers,
observed Provider-open concurrency and released slots before assertions. A
separate progress file records started cases if the process aborts before a
complete row can be exported. Current cases cover root/nested host revocation,
bound-one recursion, reconstructed-host simultaneous approvals and finite-token
serialization. Cross-group contention and queued cancellation need separate
acceptance cases; these observations are not network-model acceptance.

The child module dataset exercises actual durable service calls with a module-only
scripted Provider. JSONL export precedes result comparisons. This is neither real
network-model acceptance nor completed automatic delegation acceptance.

`with_delegation(AgentDelegationConfig)` explicitly registers `agent.invoke` in
root and child Turn inventories when the saved permissions permit delegation.
It extends the current policy with the independent Delegate manifest, rather
than exposing orchestration through an environment capability. Approval resume
rebuilds the same routing from saved ownership. Preparation remains effect-free;
execution admits private children and returns `AwaitingExternal`.

`AgentChildWaitVerifier` is installed in ExecutionService before Runner creation
and attached once through a weak reference. It verifies existing admission and
typed consumption facts without driving children or granting authority. Missing
host attachment fails closed. `pump_agent_children` loads authority from the
original checkpoint, drives children, consumes verified typed terminal results
and resumes that same parent attempt. These new production entry points compile;
local routed pump/restart tests cover real durable service execution using a
scripted Provider, not actual network-model acceptance. `ChildApprovalResumeRequest`
and `resume_agent_child_approval` independently restore an admitted Child binding,
exact current checkpoint and current adapters/policy. The root-only approval API
is not used for children. Tests rebuild Runner with an empty mutable catalog,
round-trip continuation data, observe zero effects before confirmation and one
afterward, then pump the original parent to a verified completed Task.
Local data-driven continuation cases now exercise root → child → grandchild
self recursion, reconstructed Runner continuation, duplicate resume rejection,
and two concurrent read-only children suspended for separate approvals. Pending
children release scheduling slots; repeated inspection reads their durable waits
without another model request or effect. This is local scripted-model evidence,
not actual-network acceptance or an automatic outer orchestration service.

Invocation discovery intersects the saved parent, current host and exact catalog
revisions. Named targets are advertised as exact key constants, inline/self
branches are omitted when denied, and requested child permissions are narrowed
per target. No reachable targets means no `agent.invoke` inventory or manifest.
Every initial, resumed, child and pump executor uses the same discovery builder.
Historical checkpoint requests dispatch unchanged only while their advertisement
matches current discovery; changes fail before Provider dispatch and require an
explicit host decision, never silent payload or model-output rewriting. Fresh
exact preparation and policy approval remain independent authority checks.

The advertised input schema describes named, inline and `self_call` tagged
targets, strict permission fields and inline definition/model fields. Independent
schema examples compare structural validation, serde decoding and permission/
byte-limit preflight separately; schema validity does not confer authority.
The 19-row schema dataset, 16-row child dataset and four routed pump cases passed
the 73-test module gate. Pump cases preserve slash, Unicode and greater-than-256
byte Provider-native call IDs without applying host-ID restrictions. Read-only
fanout tests measured actual in-flight peaks 2/4, prepared bound 2 below host 4,
and peak 1 for finite token budgets or unproved factories. Every dataset exports
actual JSONL before comparisons. This is not full L5 or actual-model acceptance.

`consume_agent_children` uses Server's typed terminal consumption and physical
result loader for Completed/Failed/Cancelled, not the successful-only interface.
It bounds the entire original ToolResult before writing any consumption fact,
records each exact terminal edge, and verifies existing consumption on restart.
The current feedback policy returns any failed/cancelled child with `is_error`;
it does not fabricate completed child evidence or Task success. The additional
terminal dataset and parent-history isolation assertions passed module gates;
this implementation is not yet automatic root delegation acceptance.

Root finalization keeps Turn and Task verdicts distinct: all admitted invocations
must be Completed before submitting Task success. When the parent finishes and
all children are terminal but at least one is Failed/Cancelled, Runner physically
revalidates the unsuccessful terminal proofs and records Task Failed through the
existing coordinator. Child states and parent feedback are not rewritten. The
additional two-case pump dataset verifies this actual Runtime path, including
`is_error` feedback, preserved child outcomes and absent Task success evidence.

## Proof-only Task finalization and detached children

`finalize_task(TaskFinalizationRequest)` restores the unique exact saved root,
current host permissions and latest admitted attempts. Its exhaustive
`TaskFinalizationPolicy::AllInvocationsSuccessful` dispatch reads historical
physical terminals and consumption proofs before committing a deterministic
Task verdict. It never builds adapters, polls a model, executes tools, consumes
results or retries work. Repeating the same request after rebuilding the Runner
returns the persisted verdict without changing invocation evidence.

Successful parents require exact consumption of policy-required child results.
Failed/cancelled parents must not be revived to consume late children. New
failure finalization requires all admitted executions to have verified stopped
evidence; a cancellation signal alone is insufficient. An already cancelled
Task retains that verdict after owner validation, without claiming every child
is physically stopped. A finite-budget failure already derived from an observed
terminal retains its original verdict rather than appending a second command.
Root run/resume/pump propagate original typed execution failures while attempting
this proof-only closure; a refused closure retains both errors.

Continuation finalization verifies successor-to-predecessor dependency consumption,
the exact private initialization and Server's frozen completed predecessor context.
Full-context inheritance must match that immutable source, not current Session history.
Continuation input preparation requires `prepare_continuation_input` before
invocation admission. The mandatory Runner artifact store retains the full
source, plan and provenance; the Server publisher binds exact ownership,
initialization and completed context endpoints. Invocation admission binds the
returned Derived source, and each attempt references that exact admitted source.
Rebuilt Runners must supply the same artifact store and
`ContinuationProjectionConfig` with the actual trusted counter.
Finalization re-runs selection and matches the immutable
Runtime input without new effects, consumption or projection publication. Missing
Agent ownership, source evidence or host configuration refuses closure. Local
scripted-service evidence is not actual-model long-graph acceptance.
Root sources retain original input, assembled input and frozen tool inventory.
`prepare_root_input(RootInputPreparationRequest)` exposes that same preparation
to host-orchestrated Tasks: it reserves stable identity, retains ownership and
publishes the verified Standalone source without registering a Task, admitting
an invocation, constructing/streaming a Provider or executing tools. Its
`PreparedRootInput` returns snapshot, ownership, selected request and source;
it is not a grant. The host declares its own criteria, admits the exact source,
drives the selected request and explicitly calls `finalize_task`. Ordinary
`start` shares assembly/encoding but retains its existing automatic lifecycle.
The artifact also binds exact execution coordinates and requested permissions;
changed retries cannot overwrite preparation. Blocking publication can finish
after its async waiter is dropped, so cancellation is not proof of absent facts.
Child sources retain the actual invocation authority, resolved intent, parent
ownership and private initialization. All roles require strict Required artifact
documents, including cancellation finalization; ownership alone is not input
evidence. Agent-level concurrent source-publication acceptance remains outstanding.
The host-orchestrated long graph has separate offline/scripted-model acceptance
with real OS tools, persistence and reconstructed Runners for named/inline roots
and full/projected context. That does not establish actual-model network
acceptance. Current evidence and remaining gates are recorded in
[requirement 0030](../../docs/requirements/0030-governed-agent-execution.md);
the root preparation API alone establishes neither end-to-end result.

`resume_agent_child_approval` can restore an already entered, uncancelled child
after persisted `RootOnly` Task cancellation. Immutable admission inspection is
separate from active-parent authority. Exact original admission, binding,
checkpoint, approval, current permissions and host/original invocation budgets
are still required. `AllInvocations`, never-entered children, foreign coordinates
and revocation refuse execution. Terminal publication uses historical evidence
only after that authorized service continuation, not as execution authority.
New admission, parent pumping and consumption keep their active-parent checks;
the cancelled Task cannot be revived by a child finishing. The dedicated local
dataset compares reconstructed hosts under both cancellation policies and
exports full requests and ledgers before checking effects and duplicate refusal.
These are local scripted-model observations, not actual-model network acceptance.

Production and tests are separate according to the repository code conventions.
Run `cargo test -p kolyan-agent` and
`cargo clippy -p kolyan-agent --all-targets -- -D warnings`.
