# kolyan-agent

Bounded Agent definition/catalog/snapshot and invocation binding support for requirement 0030.
This crate depends on `kolyan-server` for its existing `AgentIdentity`; Server
does not depend on this crate. No execution runner or placeholder scheduler is
included.

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
  does not implement an Agent Runner or globally unique instance allocator.
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

**Not implemented:** a production provider tokenizer, lossy compaction, summaries,
context reduction or automatic Runner assembly. No long-task reduction
acceptance is claimed by this module. Overflow never silently discards history.

`provider::ContextPreparingProvider` validates every actual `stream` opening and
requires host `ContextRecorder` acknowledgement before calling the inner Provider.
Source, preparation and rejection evidence are typed. It forbids changes to the
neutral request at this boundary, including adding a missing output limit: initial
projection must happen before Turn admission so Core's recorded input remains
identical to the dispatched request. Strict/Inspect mode is explicit; recording
or preparation errors block dispatch rather than being retried as network errors.
Successful requests and the original event stream are passed through unchanged.
Recording proves preparation acknowledgement, not network completion. Durability,
redaction, counter trust and integration into the Agent Runner remain host-owned.

Production and tests are separate according to the repository code conventions.
Run `cargo test -p kolyan-agent` and
`cargo clippy -p kolyan-agent --all-targets -- -D warnings`.
