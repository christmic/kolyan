# 0037 Governed MCP integration

This specification implements the MCP portion of [the Agent evolution program](0031-agent-evolution-program.md).
Kolyan must discover and execute external tools through its existing governed
Tool, Turn, Runtime and Task boundaries. Transport success alone is not Agent
acceptance. Production Host assembly, durable evidence and real Agent consumers
are required; experimental self-development is only a subsequent feedback run.

Status: implementation in isolated worktrees; not integrated into Main. The
private stdio, HTTP and authorization prototypes have incomplete acceptance.
Their observations below do not establish production or real-model readiness.

## Ownership and execution boundaries

The MCP adapter owns protocol negotiation, discovery, one connection owner,
request correlation and transport cleanup. It must not introduce another Agent
loop, duplicate Turn scheduling or make decisions on behalf of the Host.

The trusted Host owns configuration, network policy, credentials, launch material,
authority and the durable attempt adapter. A model may select an advertised tool;
it cannot supply launch configuration, credentials or an authorization decision.
An MCP description or idempotence annotation grants no permission.

Preparation binds the exact server source, source revision and material epoch,
connection generation, catalog revision, tool name, schema and arguments. Dispatch
requires the exact current grant and the original Core ToolExecutionWindow.
Revocation, changed source, schema, catalog or generation requires fresh admission.
There is no permissive default port, legacy configuration fallback or ambient
environment inheritance.

A durable begin is recorded before any possible tool request write. A terminal
receipt is published only after the actual result is durably committed. Once a
request may have been sent, a missing response or commit acknowledgement is
Uncertain, not permission to retry. Historical terminal verification is read-only
and independent of current dispatch authority. Begin-only recovery never replays
the effect, even with a new connection or grant.

## Protocol profile and bounds

The initial profile is MCP 2025-11-25: stdio and Streamable HTTP, initialization,
tools discovery, pagination, tools/call, ping and supported cancellation. Sampling,
roots, elicitation, resources, prompts and task augmentation remain explicitly
unsupported until separately specified. Clients advertise only implemented
capabilities. Negotiation must accept only the pinned supported version.

JSON-RPC framing rejects invalid UTF-8, duplicate keys, ambiguous result/error
objects, malformed IDs and conflicting responses. One owner routes responses;
callers must not independently read a shared stream. A server queue is bounded
to eight waiting operations. All limits are explicit positive ceilings, reducible
by Host policy, not reasons to renew an operation deadline.

| Boundary | Initial ceiling |
| --- | --- |
| Frame, catalog and schema bytes | 1 MiB each |
| Discovery | 64 pages and 256 tools |
| JSON nesting and pagination cursor | 64 levels and 4 KiB |
| Retained stderr | 64 KiB; no raw secret logging |
| Initialization and complete discovery | 10 seconds each |
| Tool call | 30 seconds |
| Graceful close and TERM grace | 1 second each, then KILL and reap |
| HTTP headers | 32 KiB |
| SSE event, event count and cursor | 1 MiB, 256 and 1024 bytes |
| HTTP control peers and stream reconnections | 4 and 3 |
| Authorization waiters and state lifetime | 16 and at most 300 seconds |

The original window and cancellation control cover queueing, hooks, authority,
launch resolution, credential storage, TCP, TLS, headers, body and commit.
Operation ceilings use the minimum of that cutoff and their own limit. Waiters
do not extend a leader's deadline. Cleanup does not authorize new network work
after expiry. Saved evidence inspection does not require fresh dispatch authority.

## Atomic discovery and argument validation

Initialization verifies the tools capability before sending initialized and
discovering every page. Cursor cycles, duplicate names or any invalid tool reject
the whole catalog. No partially validated catalog is advertised. list_changed
invalidates the snapshot; refresh publishes a new revision. Reconnection changes
generation, so old prepared calls cannot silently target a new server instance.

Tool inputs use a bounded, meta-validated JSON Schema 2020-12 profile. Support
ordinary object, array, string, numeric and composition constraints plus declared
non-validating metadata. Local references must resolve within the bounded schema.
External references, dynamic reference semantics, unsupported identifiers and
unknown validation keywords are rejected rather than ignored. No network schema
fetch, coercion or default insertion is allowed. Additional properties follow the
advertised schema; the adapter must not invent a universal extra-field ban.

## Stdio source confidentiality and process ownership

Public configuration contains only a Host-issued opaque source handle,
configuration revision and material epoch, plus non-secret governance contracts.
Neither public serialization nor Debug may expose program, cwd, argument values,
environment names or values, lengths, or a secret-derived digest. The previous
prototype's recursively serializable launch configuration must be replaced, not
retained as a compatible format. Excluding environment values from a digest is
insufficient protection.

Launch material lives in a private carrier without Clone, Serialize, Deserialize
or Default. Debug and Display redact the whole carrier; owned secret buffers are
zeroized where practical. A mandatory trusted launch-material port atomically
reserves material against the exact invocation, prepared digest, scope, source
revision, epoch, cancellation and original window. There is no public DTO returning
argv or environment. Revision and epoch are random Host identities, not hashes
of credentials; material cannot change in place under an unchanged revision.

The process owner retains the single-use material permit until process reap and
pipe joins. After the final revocation check it moves the material once into its
private command builder. Startup itself requires authority, explicit cwd and an
explicit cleared environment. Executable verification binds actual bytes.

This controlled-process profile is not an OS sandbox. A requested but unavailable
process sandbox returns Unsupported. Secrets in argv are forbidden by default;
exceptional use needs explicit Host configuration approval because OS process
inspection can expose them. Environment or a separately governed future FD channel
is preferred. Carrier isolation cannot prevent a malicious child from emitting
secrets and does not constitute general data-loss prevention.

Partial writes close the connection; cancellation addresses only the outstanding
call, not initialization. Late responses cannot create a second terminal receipt.
Close must reap the leader, join pipes and account for descendants; failures are
explicit. Repeated close is idempotent. Stderr is bounded without logging raw
launch material or constructing public errors from secret-bearing command objects.

## Streamable HTTP transport

HTTP uses the same executor and owner contracts as stdio. Sources reject userinfo,
query and fragment. Host network policy supplies approved resolved socket addresses;
the adapter verifies the actual peer before TLS and request send. HTTPS verifies
roots and SNI. Unauthenticated loopback HTTP requires explicit approval. Ambient
proxies, cookies, arbitrary headers and automatic redirects are forbidden.

Each message has its own POST with JSON and SSE response support. Notifications
accept 202 without a body. Session and protocol headers apply consistently to
POST, GET and DELETE; stateless servers are supported. Session IDs are bounded
ASCII bytes 0x21 through 0x7e, at most 1024 bytes.

SSE parsing handles UTF-8, BOM, CRLF, multiline data, comments, priming events,
IDs and retry values. Empty IDs reset the cursor; NUL IDs are rejected. Only a
complete accepted frame advances the cursor. Bounded ID/digest deduplication
rejects conflicting content and cross-scope reuse.

A lost response stream may reconnect with GET and Last-Event-ID within the
original window and bounded retry count. It never resends tools/call POST. No
usable cursor or failed resumption yields Uncertain. GET 405 explicitly identifies
unsupported idle streams. Session 404 retires the generation; a fresh connection
needs new admission. DELETE has its own admitted window; remote DELETE 405 does
not excuse leaked local tasks or pipes.

## Authorization and credential mutation

The first supported profile is a public preregistered client with PKCE S256,
cryptographically random state, constant-time state validation and a Host-owned
loopback callback. Require the authorization-response issuer in this profile.
Client metadata registration and confidential clients remain explicitly Unsupported;
there is no legacy endpoint guessing, registration fallback or anonymous fallback.

Protected resource metadata and authorization-server discovery bind exact resource,
issuer, client, account slot and credential epoch. Validate metadata issuer and
callback issuer independently. Carry resource in authorization, code exchange and
refresh requests. Host-approved scopes bound requested and returned scopes; extra
consent is a fresh decision, not an automatic widening.

Bind the actual callback listener before exposing the authorization URL. Validate
duplicate or missing parameters, state, issuer, TTL and single consumption. Close
listeners and release retained state on success, failure, expiry and cancellation.
Typed callback parsing without an actual owned listener is not full acceptance.

Credential and pending-flow carriers are private and redacted. Storage is mandatory;
no default memory store or optional mutation guard is acceptable. A cross-process
mutation permit covers load, Intent, durable Sent, one token POST and commit CAS.
Purpose distinguishes Code, Refresh and Revoke; tombstones prevent late commits
from resurrecting revoked credentials. TLS, actual peer policy and redirect refusal
also apply to discovery and token requests. Bearer tokens are never forwarded to
a redirected or different resource.

Refresh is an explicit Host operation bound to expected credential revision. A
single leader performs exchange; at most sixteen waiters retain their own control
and window. A committed newer revision is reused without another POST. Omitted
refresh_token retains the previous token; a supplied token replaces it. Expiry
must be bounded and returned scope cannot exceed approval. invalid_grant retires
the epoch and requires authorization.

After durable Sent, lost response, cancellation, dropped future, storage failure
or failed CAS produces MutationUncertain. Restart inspects durable mutation evidence;
lock release alone does not permit replay or old-token fallback. Publish readiness
only after commit. Optional remote revocation has an independent admitted result
and does not override local credential retirement.

Distinguish Unauthorized401 from Forbidden403. A 401 permits the Host to consider
fresh authorization or explicit refresh; a 403 does not trigger refresh or larger
scope automatically. Public problems contain bounded status and opaque identities,
not raw challenge headers or URLs. Once a tool effect may have started, either
response remains Uncertain and cannot trigger tool POST replay. Authentication
changes during GET recovery stop that recovery and require a fresh connection.

## Delivery sequence and production consumers

1. M1 establishes stdio negotiation, catalog validation, actual processes and
   durable attempt ports; M2 adds HTTP and SSE through the same executor.
2. M3 A establishes strict authorization and code exchange. S1 replaces exposed
   launch material before any production Host integration.
3. M3 B1 implements refresh and durable singleflight. B2 supplies distinct 401
   and 403 decisions without effect replay. M3 C completes callback ownership,
   network, isolation, revocation and secret audits.
4. Main assembles actual Host configuration and credential stores, Agent tools
   and Journal adapters. Opening, Skills and Hooks guards must remain intact.
5. Integrated offline gates precede independent real MiniMax Agent scenarios.
   Self-development may then provide feedback; immediate successful edits are
   not a requirement or replacement for these gates.

Crate ports described here are requirements, not claims that Main already exposes
them. New production modules and tests follow [code conventions](../architecture/code-conventions.md).
Framework, provider configuration and case data remain separate. Existing test
inputs, expectations and assertions are retained; contract migrations change
assembly rather than weakening old acceptance. Build caches follow the capacity
rules in 0031; source freezes and evidence are not disposable compile caches.

## Required acceptance matrices

Each variant has a stable case ID, independent input and oracle. The framework
exports all actual results before assertions, physically rereads JSONL and compares
semantic invariants separately from unconstrained model wording. Keep original
failed runs, actual requests, native markers, durable begin/terminal receipts,
reopened-store observations and cleanup results. Real local processes or TLS
servers are not evidence of an external-model run.

M1 retains all 46 numbered groups and 73 independent rows:

| Groups | Required evidence |
| --- | --- |
| 01 to 07 | Version, capability, pagination, cycles, empty catalog, change and duplicate rejection |
| 08 to 16 | Argument and schema rejection without I/O, denied launch, executable hash, environment, cwd, unsupported sandbox |
| 17 to 25 | Missing, forged, foreign or changed grants; no annotation authority; native marker, isError, protocol errors, structured content |
| 26 to 34 | Cancellation and timeout before or after send, effect then EOF, partial writes, drops, wrong IDs, duplicate responses, UTF-8, JSON, duplicate keys, fragmentation and floods |
| 35 to 42 | EOF, ignored TERM, descendant pipes, repeated close, revocation, stale catalogs, reconnection, changed generation and rebuilt grants |
| 43 to 46 | Actual Journal reconstruction, saved terminal, begin-only uncertainty, store failure and duplicate completion |

M2 retains fifteen A, four B and eleven C independent cases, thirty total. Cover
JSON and SSE, sessions and stateless operation, notification 202, DELETE 405,
session 404 and new grants, cursor GET recovery without tool POST replay, redirect
refusal, unavailable streams, authentication boundaries, post-effect loss and
every transport bound. Retain the M1 matrix unchanged.

M3 retains sixteen groups with three independent variants each, plus six original
window cases. A smaller authorization subset cannot substitute for these 54 cases.

| Group | Three independent variants |
| --- | --- |
| 01 | Advertised, path and root protected resource metadata |
| 02 | Wrong resource, no fixed authorization server, ambiguous challenge |
| 03 | OAuth insertion, OIDC insertion, OIDC appending discovery |
| 04 | Actual S256, missing PKCE, plain-only rejection |
| 05 | Wrong state, missing state, replay |
| 06 | Metadata issuer, callback issuer, missing issuer |
| 07 | Actual resource in authorization, code and refresh bodies |
| 08 | Foreign audience, issuer store mismatch, account scope mismatch |
| 09 | Sixteen waiters, rotation, omitted refresh token |
| 10 | invalid_grant, lost exchange and restart, save or CAS failure |
| 11 | Fresh authorization after 401, no refresh after 403, effect before 401 without replay |
| 12 | Actual DNS peer, redirect without credential forwarding, denied private IP |
| 13 | Cancellation before code, after code, dropped refresh future |
| 14 | Missing store, unavailable store, revoke and commit race |
| 15 | Nested Debug, public Prepared objects, physically inspected synthetic-secret receipts |
| 16 | Excess scope, no registration fallback, explicit unsupported client metadata registration |

The six window cases cover Hook exhaustion with zero network, queue exhaustion
with zero new POST, cumulative authority/storage exhaustion, delayed headers,
waiters without renewed deadlines, and expiry after Sent with one POST and
MutationUncertain. Add actual callback prebinding, duplicate parameters and release;
credential borrow/tombstone races; independent remote revocation; and TLS/commit
stage evidence rather than silently treating them as existing coverage.

S1 additionally proves secret carriers cannot be cloned or serialized, public
nested values contain no synthetic secret, invalid source/revocation causes zero
spawn, resolution never renews the window, epoch changes invalidate grants,
explicit material reaches the child without leaking into public traces, old
terminal recovery is read-only, begin-only remains Uncertain, and close reaps.

## Current evidence and remaining acceptance

Private M3 A has a frozen 62-file source manifest. Its focused authorization gates
reported sixteen plus four rows and passing checks, but its final full MCP gate
was five passed and two failed. Historical command stdout was not archived for
all those gates. The unresolved EOF collector and post-effect uncertainty cases
must retain their original observations; neither cause nor resolution is proven.

A separate diagnostic process exercised complete and killed partial records with
flush acknowledgement, offsets, hashes and newline validation. Its two passing
cases establish that record-boundary mechanism only. They neither exercised the
MCP executor nor explain the historical failures. Host finish/lookup stage
diagnostics, source confidentiality, complete authorization, actual Host assembly,
Main workspace gates and real Agent acceptance remain required.

## Primary protocol references

- [MCP lifecycle](https://modelcontextprotocol.io/specification/2025-11-25/basic/lifecycle),
  [transports](https://modelcontextprotocol.io/specification/2025-11-25/basic/transports),
  [tools](https://modelcontextprotocol.io/specification/2025-11-25/server/tools) and
  [authorization](https://modelcontextprotocol.io/specification/2025-11-25/basic/authorization).
- [Protected resource metadata RFC 9728](https://www.rfc-editor.org/rfc/rfc9728.html),
  [authorization server metadata RFC 8414](https://www.rfc-editor.org/rfc/rfc8414.html),
  [PKCE RFC 7636](https://www.rfc-editor.org/rfc/rfc7636.html) and
  [issuer identification RFC 9207](https://www.rfc-editor.org/rfc/rfc9207.html).
- [Resource indicators RFC 8707](https://www.rfc-editor.org/rfc/rfc8707.html),
  [bearer errors RFC 6750](https://www.rfc-editor.org/rfc/rfc6750.html),
  [refresh RFC 6749](https://www.rfc-editor.org/rfc/rfc6749.html#section-6),
  [OAuth security RFC 9700](https://www.rfc-editor.org/rfc/rfc9700.html) and
  [revocation RFC 7009](https://www.rfc-editor.org/rfc/rfc7009.html).

The reviewed official Rust SDK checkout is modelcontextprotocol-rust-sdk at
dbd238275534c3a8da4d91b7220655e878216988, rmcp 3.4.0. It informs implementation;
optional guards, default storage or discovery fallbacks in that checkout are not
authority to relax Kolyan's stricter Host contracts or the pinned protocol.
