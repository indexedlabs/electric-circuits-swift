# Native v1 client contract — version 1

This document is the public contract consumed by `ElectricCircuitsSwift`. It is intentionally
smaller than the engine control plane: only the endpoints below are client surface. The Rust/Axum
`GET /v1/openapi.json` document is the authority for the documented native control-plane routes;
`Contracts/native-v1-contract-v1.json` is the machine-readable projection checked by
`Scripts/check-native-v1-contract.mjs`.

## Compatibility

`v1` is an additive JSON contract. A server may add optional response fields, object members, or
new endpoint paths. Clients must ignore unknown object members. Removing or renaming a listed path,
method, response status, required wire member, or changing a listed member's meaning requires a new
native API version. The contract is not an endorsement of undocumented engine routes or of Rust
implementation types. The client has no OpenAPI runtime dependency; the checker is a release gate.

`contractVersion: 1` means this document and its JSON projection must change together. A changed
Rust OpenAPI `info.version`, path, operation, status, response reference, or listed schema member
is drift and fails the gate with the exact path/schema in the diagnostic.

## Control-plane endpoints

| Endpoint | Request | Success | Stable semantics |
| --- | --- | --- | --- |
| `POST /v1/shapes` | `ShapeRequest` | `200 ShapeResponse` | Creates or renews a materialized shape. A supplied `subscription` is the claim/idempotency identity; repeat it to renew. `400` is invalid input, `409` is a claim owned by another shape, and `503` is unavailable/retryable only after the caller's bounded backoff policy. |
| `GET /v1/shapes/{id}` | Escaped path component | `200 ShapeResponse` | Metadata lookup. `404` means the shape is absent/retired and is terminal for that generation. |
| `DELETE /v1/shapes/{id}?subscription=…` | Escaped `id`, optional claim query | `200` | Releases that claim. A client treats `404` as idempotent completion because the requested release is already effective. Other non-2xx responses are surfaced unchanged. |
| `POST /v1/subsets/query` | `SubsetQuery` | `200 SubsetResponse` | One snapshot of `rows` with source `lsn`; a caller compares it with an independently authored SQL result at the same causal source receipt. `400` is terminal invalid input. |
| `POST /v1/subset-feeds` | `ShapeRequest` | `200 ShapeResponse` | Creates/renews a changes-only feed. The Swift client forces `changesOnly: true`; server response has the same handle contract as a shape. `400` is terminal invalid input. |
| `POST /v1/aggregates` | `AggregateRequest` | `200 ShapeResponse` | Creates/renews an aggregate shape. `table` and `fn` are required; `where`, `col`, and `subscription` are optional. `400` is terminal invalid input. |

`ShapeRequest`: `table` is required. `where`, `columns`, `changesOnly`, and `subscription` are
optional. Predicate leaves use `col`, `op`, `value`; null predicates use `col`, `isNull`.
`SubsetQuery` adds optional `orderBy: { col, desc }`, `limit`, and `offset`. `SubsetResponse` has
`rows` and `lsn`. `ShapeResponse` has `shapeId`, `table`, `streamPath`, `streamUrl`, and optional
`subscription` and `leaseSeconds`. `streamUrl` is the durable-stream URL to use; do not construct it
from internal durable-stream topology.

`AggregateRequest` uses `table`, optional `where`, required `fn`, optional `col`, and optional
`subscription`. `ElectricCircuitsSwift.createAggregate` is a public native-v1 handle-creation API.
Aggregate result semantics are not part of the first subset snapshot/live materialization profile or
the PG18.4 top-10 corpus; this contract only promises the documented creation/renewal response.

Authentication is transport-owned: the client forwards caller/configured headers and cookies. The
native OpenAPI only documents its current public error statuses; an authenticated gateway may add
`401` or `403`, which are terminal until credentials change. A client surfaces those and malformed
4xx results as `ClientError.http`; it never retries authorization or malformed request failures
automatically. `408`, `425`, `429`, and `5xx` results surface as
`ClientError.retryableHTTP(status:retryAfter:)`; `retryAfter` is a parsed `Retry-After` directive
when valid, not a server-error-string convention. The coordinator combines that directive with its
own bounded retry policy, and a caller-owned transport may separately refresh credentials/cookies
without the core persisting, interpreting, or exporting them.

`ClientError.http` preserves a typed status but uses the fixed message `HTTP request failed`.
Neither it nor stream/coordinator state may retain response bytes, response headers, cookies,
tokens, or arbitrary provider diagnostics. Telemetry uses only bounded allowlisted attributes.

## Durable stream protocol

The durable stream is a separate public service and is therefore not described by the Rust
OpenAPI document. Given `ShapeResponse.streamUrl`:

| Operation | Request | Result | Stable semantics |
| --- | --- | --- | --- |
| frontier | `HEAD streamUrl` | `stream-next-offset` header | The nonempty header is the opaque resume cursor. Its absence on success is malformed. It fences a subset snapshot: create feed → `HEAD` frontier → snapshot → read from that frontier, under the same source receipt. |
| read | `GET streamUrl?offset=<opaque>&live=long-poll` | JSON array and `stream-next-offset` | Apply rows and checkpoint atomically. A nonempty array without a next offset is malformed. Empty array, empty body, or `204` may still advance the cursor when the header differs. |

An envelope is `{ type, key, value?, old?, headers }`. `headers.operation` is `insert`, `update`,
`upsert`, or `delete`; `key` is an opaque **bare engine key**, including for deletes. It is not a
quoted relation path and must not be parsed into a database key by Foundation core. `value` is
required for non-deletes. `old` is optional diagnostic data. Unknown envelope/object members are
ignored for forward compatibility. A missing required envelope member, invalid operation, truncated
JSON, or a missing required next-offset is malformed and fails closed without advancing the local
cursor.

`404`, `410`, and `stream-closed: true` are terminal generation results. Recreate only after
discarding the retired generation's materialization scope and cursor; never reuse its offset. Other
transport/nonterminal HTTP failures are retryable only under the caller/coordinator's bounded retry
policy. Reset/recreate starts a new scope keyed by principal, template, subscription, and generation.

## Scalar and cursor rules

Rows use JSON objects. `null` maps to `JSONValue.null`; missing object members remain missing and
are distinct from null. Integers decode as `Int64` before decimal/double, preserving PostgreSQL
`bigint` exactly. Decimal values use Foundation `Decimal`; callers should not round-trip them via
`Double`. `offset` is opaque: compare only for equality and send it back unchanged. `lsn` is a
diagnostic/source watermark, never a durable-stream replacement cursor.

Snapshot/live correctness is the public fence: source transaction plus marker → server
`drainedThrough(marker)` receipt (including deferred work) → feed `HEAD`/snapshot/read → atomic
local application → independent SQL/reference result at the same source prefix. Internal Axum
handlers, DS offsets, task names, retry counts, catalog state, and stream storage paths are
implementation details and are not stability promises.
