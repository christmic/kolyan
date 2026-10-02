# Model accounting

Accounting types do not depend on Session, Agent or Runtime.
`MappingIdentity`, `CountProfile`, `PreparedContextWire` and `ProviderInputCount`
have private fields and no Deserialize entry point. Provider adapters supply an opaque
instance token; consumers cannot recover it from a preparation.

Neutral source, generation body and count body each have an independent inclusive
16 MiB compact UTF-8 JSON ceiling, including all fields and escaping, not HTTP headers.
A bounded hashing writer refuses excess before an encoded Vec or canonical copy;
an already-owned input object is not a promise about total allocator/RSS consumption.
Provider mapping checks the source before planning/cloning and the merged wire size
before materializing it. This byte bound does not confer token authority.
Identity strings reject blank/control characters; provider and revisions are at most
128 UTF-8 bytes, model at most 512, endpoint identity exactly 64 hex bytes.

Preparation measures the full mapped generation JSON in UTF-8 bytes
and records neutral, generation and count-input SHA-256 digests. Bytes are not tokens.
CountProfile defaults to unsupported and requires explicit exact-identity registration.
ProviderInputCount describes reported numbers, not Trusted counts or upper bounds.
Zero is a valid report. Official precise endpoint semantics and host-supported model
coverage are separate proofs owned by the host.

Host selection must occur before immutable input persistence, never inside the Provider.
The adapters expose evidence for that consumer; they do not implement selection,
durable admission, compaction, tokenization or memory.
