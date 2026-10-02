# Governed Shell Output Artifacts and Targeted Reads

## Status, scope and implementation gate

Reviewed E2 design for requirement 0031. Trace-only bounded verified range
implementation is released; Tools/Policy/Host interfaces require the additional
contract freeze below before their implementation. No feature acceptance is
claimed by this specification.
Read-only source baseline: main `dfb0aebd2b9b17963b5b2d58e8762a126725897a`.
Main owns canonical requirements, Policy/Agent permission changes and Host
assembly. This independent candidate does not modify 0031 or any frozen source.
Main must approve the permission, resource ceilings, provenance reader and ACL
interfaces below before Trace/Tools implementation starts.

Deliver a real Shell -> immutable output reference -> artifact.read -> next
ModelRequest consumer through the existing Turn/Runtime chain. This is not
background execution, memory, a mailbox, a new execution loop or a general
filesystem reader. Existing inline Shell cases and assertions remain intact.

## Existing seam and dependencies

- `kolyan-tools/src/isolated_shell/turn.rs` validates the original preparation,
  scoped grant and current policy, captures a bounded process, then serializes
  complete stdout/stderr into ToolResult. Its raw cap is derived from envelope
  size; it cannot currently return a large complete output reference.
- `kolyan-sandbox/src/process.rs` aggregates stdout/stderr retained byte limits,
  cancels on overflow and awaits cleanup. Observation channels are lossy
  diagnostics, never output content or receipt authority.
- `kolyan-trace/src/artifacts.rs` already atomically publishes Required content,
  verifies complete digest/length on read and retains pins across reopen.
  Its access-control boundary is a trusted host-protected directory.
- FactJournal supports exact references and idempotent CAS append, not an
  atomic transaction with ArtifactStore. Publication must expose this gap.
- Current IsolatedToolSet and Host inventories are exactly four tools. The new
  read capability requires an explicit governed Host composite, not an implicit
  fifth-tool change or a file.read escape into the protected state directory.

## Authority and identities

### A reference is not an authorization

ArtifactRef digest/length/retention identify bytes only. A digest, a forged
FactRef, model text, a serialized PreparedGrant or a source agent name cannot
construct read authority. No `read_by_digest` tool or allow-all resolver exists.

The trusted publisher creates a cryptographically random 256-bit opaque
`output_handle`, unrelated to artifact digest. It is an unguessable lookup handle,
NOT a bearer permission: possession never bypasses current owner/ACL checks.
Tokens are created by an OS-backed randomness provider approved by Main, not a
hash of public coordinates. Ordinary diagnostics redact them; authorized model
interaction records can contain the actual received handle. No global secret,
signing key or token-only authorization is introduced in this increment.

Host-owned durable publication stores its handle hash and exact binding. A
returned receipt contains the opaque handle plus public descriptive metadata.
Internal artifact digest is optional descriptive metadata, never tool input
authority. Collisions or attempts to substitute an existing mapping fail closed.
Deduplicated equal bytes retain distinct owner/publication records.

### Exact source ownership

`OutputOriginV1` binds all of:

- independent original ToolExecutionScope (session/turn/execution, step,
  optional agent snapshot digest), original native call ID and tool name;
- exact PreparedCall digest, exact PreparedGrant fingerprint and policy revision;
- adapter revision, output-policy revision and complete execution binding;
- original independently verified Runtime issued/effect-entry provenance;
- completed exit status, stdout/stderr ArtifactRefs and aggregate raw byte count.

Main must supply a bounded canonical PreparedGrant fingerprint contract covering
ALL grant fields, including scope, constraints and approval evidence. This is
not an existing grant `digest()` accessor and Tools must not invent its own
parallel hashing algorithm. Matching a hash alone does not prove grant issuance.

Publication is associated with the actual ToolInvocation. Its source preparation,
grant and effect-entry must resolve through the trusted Runtime proof reader.
The publication cannot require its own future EffectResolved fact as a cause:
that would form a cycle. Runtime completion subsequently commits the exact small
ToolResult and its publication reference through the existing receipt path.
Reads require this completed-result linkage, not merely a pending publication.

### Current reader and ACL

Each read derives its principal from independent Host binding/context, not model
arguments: host namespace + logical session + task + invocation + saved agent
snapshot identity. It also binds the NEW read call's exact ToolExecutionScope.
The source scope is retained unchanged; a later step must not impersonate the
original shell step. Default eligibility is the exact originating invocation;
cross-invocation/task/agent sharing requires explicit trusted ACL grants.

Main-approved `ArtifactRead` capability is separate from FilesystemRead. Its
logical resource is the exact publication/handle identity, not a filesystem path.
Agent permission intersection, tool advertisement and Policy evaluation must
all understand it. No reuse of SkillRead or synthetic workspace path.

The durable ACL is revisioned and is initially established by the trusted
publisher for the exact owner. Only Host management can grant/revoke/change it;
the model has no ACL mutation tool. Every prepare queries current ACL and embeds
publication identity, principal, ACL revision and range into execution_binding.
Every execute independently revalidates scope/grant/current policy and current
ACL. Revocation or revision change between prepare and execute rejects before
content is returned; no automatic reprepare or newly issued grant.

Read authorization has an explicit linearization point: Host atomically admits
the exact read against current ACL revision before returning bytes. A concurrent
revocation preceding admission rejects; revocation afterward blocks future reads
but cannot retract already admitted/delivered bytes. Do not promise stronger
in-flight revocation without a separate approved serialization mechanism.
Expired/unknown/corrupt ACL, reader failures and bounded-scan exhaustion refuse.

## Output and read contracts

### Independent ceilings

1. `max_captured_bytes`: aggregate stdout + stderr raw bytes. Enforced by the
   actual existing process owner; no unbounded spool or observer-based capture.
2. `max_output_bytes`: existing complete serialized ToolResult envelope ceiling,
   including native call ID, escaped content, reference metadata and previews.
3. Artifact object/store quota, targeted read byte ceiling and complete read
   ToolResult ceiling remain explicit independent host/policy constraints.

Artifact mode must NOT reinterpret the old max_output_bytes grant as permission
to capture more. Main freezes a distinct granted capture ceiling and its adapter
requirements; effective capture is the minimum of trusted configured ceiling and
the exact grant. The value and mode are preparation-bound and revision-bound.
No missing-ceiling fallback. An initial proposed object ceiling is 16 MiB, matching
the existing Host artifact store; final operational ceilings are Main policy.

Host explicitly chooses `Inline` or `Artifact` output mode; the model cannot
choose a path, store, quota or larger limit. Inline keeps existing behavior.
Artifact mode publishes both complete streams, including empty streams, and
returns the same truthful exit/is_error semantics with output references.
Nonzero exit with complete capture may return referenced error output. Overflow,
timeout, cancellation or capture failure do not publish a successful complete
output. Retained partial bytes are not silently promoted to complete artifacts.

### Small Shell result

Proposed strict payload shape:
`{version:1, exit_code, publication, stdout:{handle,byte_length,preview},
stderr:{handle,byte_length,preview}}` inside the normal original ToolResult.
Preview is host-bounded, explicitly `complete:false` when abbreviated, includes
byte range and encoding, and consumes the SAME envelope bound. It is not a
summary, sanitized truth, instruction channel or terminal proof. No stdout/stderr
interleaving order is invented. Preview bytes are derived only from exact captured
bytes; framing that cannot fit fails explicitly before starting the process.

### artifact.read

Exact tool name: `artifact.read`. Strict input:
`{handle:string, offset:u64, length:u64}`; all fields required, no unknown fields,
digest/path/owner overrides or implicit defaults. Handle has exact fixed framing.
Nonzero length, checked offset+length, configured/granted bounds and offset <=
object length are validated. Offset == length returns empty bytes/eof; an offset
past EOF rejects. Reading the remaining shorter range returns that exact range.

Offsets and lengths are RAW BYTES, not characters, tokens or encoded string size.
Output explicitly contains offset, returned byte length, total byte length,
eof and `encoding:{utf8|hex}` with lossless data. Valid UTF-8 slices use UTF-8;
invalid slices, including a split multibyte character, use lowercase hex. No
replacement character, trimming, LF normalization or automatic decoding retries.
The complete serialized ToolResult, including hex/JSON expansion, stays bounded.

Trace initially may verify a bounded whole object and then select a byte range;
this preserves current SHA/length integrity without a new chunk-hash format.
Charge complete object bytes/I/O to the inspection budget, not only returned
range bytes. Cap the object before allocation, execute blocking reads off the
async worker and check cancellation before/after verification. Seek-only slicing
without full integrity or verified chunk proof is not accepted. Optimize later
only if measurements justify a new integrity format.

## Publication, acknowledgement and retention

Publication key is deterministic from exact issued call identity and source
coordinates, not random handle or content alone. The publisher retains one
pending publication plan (handle hash, bindings, stream refs) in durable CAS
facts before completing acknowledgement. Publication states distinguish a
candidate from a committed complete result; neither grants read permission alone.

Process executes exactly once under existing effect entry. Complete capture is
published to Required artifacts, verified, then output ownership/initial ACL are
CAS committed. ToolResult is returned only after exact publication is readable;
Runtime subsequently persists its normal resolved receipt/result linkage.
Required orphan artifacts/pending records may remain after interrupted writes;
they are not exposed by scanning digests and are not claimed as successful output.

Lost append acknowledgement triggers at most a bounded READ of the deterministic
record. Accept only byte-for-byte exact committed content and original references;
do not append changed content, regenerate a token after conflict or execute shell
again. If completion cannot be proven, return Uncertain through the existing
effect path. Same rule for process-success plus artifact/ACL/result publication
failure: publication error is not permission to replay an arbitrary shell effect.

Rebuilt reads reopen the same artifact store and FactJournal, verify schema,
scope, ACL revision and physical completed-result provenance, then verify bytes.
An artifact's existence alone never repairs a missing result receipt. This first
increment adds no synthesis of missing Runtime effect receipts; unresolved
effects retain existing explicit reconciliation/Uncertain behavior.

Required pins survive rebuild and ACL revocation. Revocation is loss of access,
not physical deletion. This increment exposes no model deletion/GC; quotas fail
explicitly. A later lifecycle policy may retire unreferenced pending artifacts,
but cannot remove Required completed outputs while recovery/reads depend on them.
State/artifacts/ACL storage must be protected from sibling Shell/File tools.

## Main-owned interfaces to freeze before code

Names below are proposals, not available implementations or accepted signatures:

- Policy: ArtifactRead capability/logical resource; capture constraint distinct
  from result ceiling; bounded exact grant fingerprint; Agent permission schema.
- Host: `OutputPublicationService` over SAME protected ArtifactStore/FactJournal;
  exact source proof reader, deterministic publication protocol, principal
  resolver and current revisioned ACL management/read-admission operation.
- Host -> Tools: `ArtifactAccessPort` exposing trusted publication plus opaque
  validated lookup/authorization results. Returned internal authorization cannot
  be constructed from a public digest/DTO. Async/blocking boundaries are explicit.
- Runtime source reader: resolve original issued/effect entry and completed
  ToolResult/publication linkage with caller-supplied limits. Reuse existing
  strong proof readers; do not introduce a shadow event schema in Tools.
- Host composite: optional explicit output mode and artifact.read inventory,
  preserving IsolatedToolSet's four-tool contract and Agent authority filtering.

## Proposed ownership and bounded batches

Sagan, only after interface release:

- Trace `artifacts.rs`: bounded verified range API and separate tests/data;
- Tools new output DTO/preview/read modules, scoped adapter implementation,
  narrow Shell output-mode hunk and independent tests/data;
- independent cross-crate acceptance framework/datasets registered by Main.

Main/shared owners:

- Policy + Agent permission/resource schema, exact fingerprint and ceilings;
- Server/Host output ownership facts, current ACL/revocation, proof readers,
  durable rebuild, composite inventory and real assembly;
- root Cargo registrations/lock, canonical 0040, final integration/live gates.

Trace is not an authorization issuer. Tools does not own a private competing
ACL journal or trust a model-supplied scope. Host consumers must land with the
adapters: a trait alone is not increment delivery. Split implementation into
bounded file batches and freeze shared interfaces before parallel editing.

## Data-driven acceptance candidate

Dataset records: case ID, backend, owner identities, explicit ceilings/mode,
scripted provider steps or actual-model input, command, expected raw bytes/digest,
read range, ACL changes/fault injection, exact expected outcomes/effect counts.
Framework exports ALL requests/events/ToolResults/receipts/facts/bytes/results,
flushes/syncs/closes files and physically rereads before any comparisons.
Observer diagnostics are separately labelled and cannot satisfy content proof.

Minimum real consumer cases (Memory and reopened SQLite):

1. `shell_ref_read_next_request`: real sandbox Shell emits 24 KiB known bytes,
   capture ceiling 64 KiB and result envelope 4 KiB. Complete output exceeds
   inline capacity. A later real artifact.read returns an exact 128-byte marker
   range; NEXT ModelRequest contains its original call/result pairing and bytes.
   Use a truthful read-only command; no test-side replacement of ToolResult.
2. `rebuild_read`: destroy/rebuild Host after Shell completion; same current
   principal reads original handle without rerunning Shell or rewriting output.
3. `binary_byte_range`: native binary stdout including NUL/invalid UTF-8;
   multibyte split, exact hex bytes and no cross-pipe order claims.
4. `nonzero_exit_output`: exit 7 retains exact referenced stdout/stderr and
   is_error; next request sees the real error result, not fake success.

Independent negative/fault rows:

- fabricated/unknown handle, digest used as handle, handle copied across owner,
  changed scope/prepared/grant/adapter, foreign publication/provenance;
- absent/revoked ACL; changed ACL between prepare/execute; before/after-admission
  revocation race with deterministic barriers, rebuild under narrowed permission;
- unknown schema, malformed owner record, missing Runtime resolved linkage,
  source intent-only, foreign effect entry, corrupt/length-mismatched/missing file;
- exact capture/envelope boundaries, combined pipe overflow, zero/excessive read,
  arithmetic overflow, offset at/past EOF, UTF-8/hex envelope expansion;
- artifact failure after command effect, publication append lost ACK with exact
  stored record, lost ACK with no proof, conflict/changed publication;
- cancellation/drop/timeout, retained partial output not falsely complete,
  Required removal refused and quota exhaustion with no hidden command retry.

Fault injection decorators change operation return/reads, never fabricate source
facts or mutate the authoritative SQLite database to simulate genuine execution.
Every refusal checks no content leak, no new GEN where inapplicable and no replay
of Shell. Read-only native cases and effectful publication-failure cases are
distinct so Uncertain is not confused with a safe preparation rejection.

Offline scripted providers exercise actual SDK-independent execution wiring,
not provider acceptance. Main later schedules both authorized MiniMax protocols:
the MODEL produces Shell/read calls; actual references, next requests and byte
postconditions must be demonstrated. No network runs during design or worker
implementation. Full regression and live receipts are acceptance gates, never
implied by this document or a successful module test.

## Reviewed first implementation boundary

The first independent batch may extend ArtifactStore with an explicitly bounded
whole-object integrity check followed by exact raw-byte range selection. It does
not issue access authority or expose an Agent tool. Object inspection and returned
range ceilings are separate; checked arithmetic, EOF, invalid bounds, missing and
corrupt objects, binary slices and Required retention are data-driven tests.
Production async callers must move blocking inspection off the executor and check
cancellation around it. Agent delivery still requires the actual governed Shell,
publication, ACL, artifact.read and next-request consumers specified above.

Policy, Agent and Host source changes are not released by a Trace-only gate.
A complete publication must authenticate source effect entry and the resolved
result linkage, retain the original grant fingerprint and apply current ACL at
read admission. The reviewed design rejects digest-only authority and effect
replay after a publication failure. Further contracts are frozen in this document,
not independently redefined by Tools or Trace.

## Frozen Trace range interface

The first source batch exposes `ArtifactRangeLimits` with independent
`max_verified_bytes` and `max_returned_bytes`, and
`ArtifactStore::read_range(reference, offset, length, limits)` returning
`ArtifactRange { offset, total_bytes, verified_bytes, bytes, eof }`.
The returned value contains content only; it cannot establish owner, ACL,
source effect completion or permission. Both ceilings must be finite and positive.
The requested length must satisfy its ceiling even if EOF would clip the result.
Complete-object verification retains the existing store ceiling, regular-file
check, complete length check and SHA-256 check, including bytes outside the slice.

The independent dataset includes 23 cases: seven successful byte ranges and
sixteen refusals. It covers binary and split UTF-8 bytes, empty objects and EOF,
overflow and both ceilings, missing/symlink objects and corruption outside the
returned slice. Every case reopens the store and checks that an existing Required
pin still refuses removal even if the caller changes the reference retention.
The four original artifact tests and the original read/put/remove behavior are
retained. This batch does not complete the Shell or Agent acceptance matrix.

## Frozen grant fingerprint contract

The next independent Policy batch may implement
`PreparedGrant::fingerprint(max_bytes: usize) -> Result<String, PreparedError>`.
The explicit bound is 1 through 1,048,576 encoded bytes, including the exact
domain `kolyan.prepared-grant.fingerprint/v1\0` with one final NUL byte.
Before constructing JSON, checked addition must bound all serialized strings:
scope coordinates and optional snapshot, call ID, tool name/revision, preparation
digest, policy revision and optional approval evidence. Exhaustive destructuring
must make future field additions require a reviewed preflight change.

Hash the domain followed by bounded canonical JSON of the complete serialized
grant, using the existing private canonical writer. Include every constraint,
null and approval field; do not maintain a second selected-field hash in Tools.
Preflight bounds raw string bytes; the actual writer additionally enforces escaped
JSON size. The encoding limit is not an exact allocator-memory ceiling. Return
lowercase SHA-256; no issuance, provenance, current-policy or ACL permission can
be inferred from the resulting hash. Existing grant validation stays independent.

Tests require an independent golden hash, every current field mutation, optional
values, UTF-8/escaping and exact byte boundaries. Future capture constraints need
their own added cases when that separate field contract lands. Logical Artifact
resources, capture-grant migration and Host consumers remain separate reviewed
batches, not implicitly released by this fingerprint method.
