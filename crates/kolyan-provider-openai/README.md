# OpenAI Responses provider accounting

`prepare_wire(&ModelRequest)` runs the same production planning/mapping helper as
generation. It returns immutable generation/count bodies, digests, UTF-8 wire bytes,
identity and count coverage; it never trims or summarizes messages.

`CountProfile::default()` disables count requests. To register a host-supported
endpoint/model, derive the identity from preparation and use
`CountProfile::registered(identity.clone(), counter_revision)`, attach it with
`with_count_profile`, then prepare again. `count_prepared(&prepared, timeout)`
requires the same provider instance (or unchanged clone), endpoint/model/revisions,
profile and complete coverage before sending. Rebinding a parameter table invalidates
old preparation, even if the clone uses the same endpoint.

The report source is always ProviderReported. Official exact count contracts need
separate host evidence for the exact endpoint/model and full field coverage; compatible
JSON or a 200 response is not that evidence. MiniMax has no implicit count registration.
Unknown extensions/modalities refuse counting without deleting generation fields.
There is no Chat Completions count adapter and no silent fallback.

Tests export all local HTTP requests/responses and count outcomes before comparison.
They do not prove real vendor token accounting or the host selection/admission pipeline.
