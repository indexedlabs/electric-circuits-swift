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
| `POST /v1/shapes` | `ShapeRequest`, optional `?claim=…` | `200 ShapeResponse` | Creates or renews a materialized shape. A supplied `subscription` is the claim/idempotency identity; repeat it to renew, naming the `claim` held ("Claim identity"). `400` is invalid input, `409` is a claim owned by another shape, and `503` is unavailable/retryable only after the caller's bounded backoff policy. |
| `GET /v1/shapes/{id}` | Escaped path component | `200 ShapeResponse` | Metadata lookup. `404` means the shape is absent/retired and is terminal for that generation. |
| `DELETE /v1/shapes/{id}?subscription=…&claim=…` | Escaped `id`, optional `subscription` and `claim` queries | `200` | Releases the claim `subscription` holds; with `claim`, only while it is the one that identity names ("Claim identity"), and `200` without effect otherwise. A client treats `404` as idempotent completion because the requested release is already effective. Other non-2xx responses are surfaced unchanged. |
| `POST /v1/subsets/query` | `SubsetQuery` | `200 SubsetResponse` | One snapshot of `rows` with source `lsn`; a caller compares it with an independently authored SQL result at the same causal source receipt. `400` is terminal invalid input. |
| `POST /v1/subset-feeds` | `ShapeRequest` | `200 ShapeResponse` | Creates/renews a changes-only feed. The Swift client forces `changesOnly: true`; server response has the same handle contract as a shape. `400` is terminal invalid input. |
| `POST /v1/aggregates` | `AggregateRequest` | `200 ShapeResponse` | Creates/renews an aggregate shape. `table` and `fn` are required; `where`, `col`, and `subscription` are optional. `400` is terminal invalid input. |

`ShapeRequest`: `table` is required. `where`, `columns`, `changesOnly`, and `subscription` are
optional. Predicate leaves use `col`, `op`, `value`; null predicates use `col`, `isNull`.
`SubsetQuery` adds optional `orderBy: { col, desc }`, `limit`, and `offset`. `SubsetResponse` has
`rows` and `lsn`. `ShapeResponse` has `shapeId`, `table`, `streamPath`, `streamUrl`, and optional
`subscription`, `leaseSeconds` and `claim`. `streamUrl` is the durable-stream URL to use; do not
construct it from internal durable-stream topology.

## Claim identity

A `subscription` is a name the client chooses, and two sessions can use one, one after the other.
A group's sessions always do: a launch that resumes a stored group re-joins it under the
subscription the launch that stored it loaded with ("Group continuity"). The name alone therefore
does not say whose claim a release means, so the engine gives each claim an identity. It is in the
engine from `mh/feed-removals` on; a server from before it answers none. The engine's OpenAPI
document lists `claim` on the five answers and as a query parameter of the six requests below; the
JSON projection gains it with the group load.

**In answers.** `POST /v1/shapes`, `/v1/subset-feeds`, `/v1/aggregates`, `/v1/subsets/load` and
`/v1/subsets/rejoin` answer `claim`: the identity of the claim the request's subscription holds
once the request has taken or renewed it. It is an opaque string (16 hex digits today, at most 64
bytes): compare it only for equality, send it back unchanged, and never store it.

**In requests.** The same five requests take the query parameter `claim`, the identity the caller
holds under that subscription, and `DELETE /v1/shapes/{id}` takes it beside `subscription`. It is
a query parameter so the body a client repeats to renew stays the body it created with.

| Request | `claim` | What the engine does |
| --- | --- | --- |
| create, load, re-join | names the identity the subscription's claim has | Renews the claim and keeps its identity. The answer carries the same `claim`. |
| create, load, re-join | absent, or names an identity the claim does not have | Takes the claim, or renews it when it is held, and gives it a new identity. The answer carries the new `claim`; the one it replaced releases nothing any more. |
| release | names the identity the subscription's claim has | Releases the claim. |
| release | names another identity, or the subscription holds no claim | `200`. Nothing is released and nothing else changes. |
| release | absent | Releases whatever claim the subscription holds, as a server without claim identities does. |

A release with `claim` and no `subscription`, an empty `claim`, and one over 64 bytes are `400`.

**What the client does.**

- It holds the `claim` of the last answer it received for a claim, in memory only
  (`ShapeHandle.claim`). A fence or any other stored record never contains it.
- A session's first request for a subscription names no claim, in every process: a launch that
  resumes a stored group holds no claim, so it re-joins without one and is answered its own.
- Every renewal names the held claim (`ShapeSubscriptionCoordinator`; `createShape(_:holding:)` and
  its siblings), so the identity is kept and the session's release still names its claim after any
  number of renewals, whether or not a renewal's answer arrived. The answer's `claim` replaces the
  held one either way: where it differs (the claim lapsed and was taken again, or a server on the
  way dropped the parameter) the new one is the claim to release.
- `releaseShape(_:)` names the handle's `claim` beside its `subscription`. A handle without one,
  from a server that answers none, releases by `subscription` alone.

**What it buys.** A release reaches only the claim it was issued for. Sent late, or again after its
answer was lost, when a later session has taken the claim under the same subscription, it is
answered `200` and changes nothing: the later session's claim stays held and the engine cannot
evict its group at the shape cap. A gateway between the client and the engine passes `claim`
through in both directions, and names the answer's `claim` in a release it sends on its own for
an answer it could not deliver.

**Against a server without it.** The client sends `claim` nowhere, because it was answered none,
and every release is the one that names no claim. That release keeps the exposure "Group
continuity" describes: arriving after a later session claimed the group, it releases that
session's claim until its next renewal. A gateway from before claim identities that passes
answers through but drops the query parameter leaves the engine answering a new identity at every
renewal, which the client adopts, and turning every release into the one that names no claim.

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

`410` on `POST /v1/shapes` and `POST /v1/subset-feeds` is the engine's recreate answer for a
dormant shape it could not fall through to a fresh create in the same round trip, or a
reactivation join that exceeded its bound (electric-circuits ADR-0011). The client recreates by
re-POSTing the identical request — those two collection endpoints carry no shape id — bounded by
`ShapeSubscriptionRecreatePolicy`, and surfaces a standing `410` past that bound as terminal
`ClientError.http`. `404` is deliberately not part of that vocabulary because it is ambiguous with
an unknown shape id, and `410` on every other listed endpoint remains terminal. This status is not
yet in the Rust OpenAPI projection above; the client handling is additive and forward-compatible,
and the JSON projection gains the status when the engine documents it.

`ClientError.http` preserves a typed status but uses the fixed message `HTTP request failed`.
Neither it nor stream/coordinator state may retain response bytes, response headers, cookies,
tokens, or arbitrary provider diagnostics. Telemetry uses only bounded allowlisted attributes.

## Durable stream protocol

The durable stream is a separate public service and is therefore not described by the Rust
OpenAPI document. Given `ShapeResponse.streamUrl`:

| Operation | Request | Result | Stable semantics |
| --- | --- | --- | --- |
| frontier | `HEAD streamUrl` | `stream-next-offset` header | The nonempty header is the opaque resume cursor. Its absence on success is malformed. It fences a subset snapshot: create feed → `HEAD` frontier → snapshot → read from that frontier, under the same source receipt. |
| read | `GET streamUrl?offset=<opaque>&live=long-poll` | JSON array and `stream-next-offset` | Apply rows and checkpoint atomically. A nonempty array without a next offset is malformed. Empty array, empty body, or `204` may still advance the cursor when the header differs. At the stream's head the server holds the request for its poll window (about 30 seconds) before it answers `204`. |
| read to the head | `GET streamUrl?offset=<opaque>`, with no `live` | JSON array, `stream-next-offset` and `stream-up-to-date` | Answered at once and never held: the entries after the offset, or an empty array at the head. Applied and checkpointed as a read is. `stream-up-to-date`, present and not `false` or `0`, marks an answer that ends at the stream's head as the server wrote it; a long-poll answer may carry it too. The client sends this read with `Cache-Control: no-cache`, because the stream server lets its answer be cached and a cached answer says where the head was. See "Live after a resume". |
| live read (SSE) | `GET streamUrl?offset=<opaque>&live=sse` | `event: data` and `event: control` frames | Each data frame is followed by a control whose `streamNextOffset` ends that batch; apply and checkpoint at the control. The control of a batch that ends at the stream's head carries `upToDate: true`. A read that opens at the tail first sends a control with `upToDate: true` and `streamNextOffset` equal to the requested offset, and repeats it while idle, so an idle answer arrives without new data (durable-streams-rust `handle_sse`, required by its conformance suite). `streamClosed: true` is terminal. |

An envelope is `{ type, key, value?, old?, headers }`. `headers.operation` is `insert`, `update`,
`upsert`, or `delete`; `key` is an opaque **bare engine key**, including for deletes. It is not a
quoted relation path and must not be parsed into a database key by Foundation core. `value` is
required for non-deletes. `old` is optional diagnostic data. Unknown envelope/object members are
ignored for forward compatibility. A missing required envelope member, invalid operation, truncated
JSON, or a missing required next-offset is malformed and fails closed without advancing the local
cursor.

An entry whose `type` is `__circuits.control` is not a row change: it is an instruction to the
stream's readers, `{ type, key, value: { control, reason?, groupId?, resumeLsn?, throughLsn? },
headers: { operation: "control" } }`. The engine writes `control: "must-refetch"` on a shape
group's stream when the group fell behind, detached and dropped output, ahead of anything it writes
after reattaching. A reader fails closed at it with `StreamError.mustRefetch`: the batch that carries
it is not applied and the cursor does not move. The caller reloads the group from a snapshot (a new
group load) and replaces its rows. A control of any other kind fails closed as malformed, without
applying its batch or advancing the cursor. A control counts against the per-batch event limit.
The marker itself does not end the group, unlike a terminal `404`/`410`/`stream-closed`, but a
session that ends at it releases its claim, so the reload is a new group load under the same
subscription. A reader that keeps its own materialization must replace its cursor along with its
rows, since the cursor it holds still sits before the marker and the next read would hit it again.

`404`, `410`, and `stream-closed: true` are terminal generation results. Recreate only after
discarding the retired generation's materialization scope and cursor; never reuse its offset. Other
transport/nonterminal HTTP failures are retryable only under the caller/coordinator's bounded retry
policy. Reset/recreate starts a new scope keyed by principal, template, subscription, and generation.

## Live envelope position

The engine states what a consumer may assume about an envelope's position in its
`docs/ARCHITECTURE.md`, section 7, "Native envelope positions". Every envelope on a changes-only
feed or a group stream carries `headers.lsn`, a PostgreSQL LSN as `HI/LO` hexadecimal text, and
usually `headers.txid`, the 32-bit xid of the transaction that caused it. Along one stream, in
offset order, `lsn` never decreases; equal positions are ordinary, and offset order decides among
them. The subset sources apply its three rules:

1. Between envelopes a position only rejects a replay. An envelope for a key is applied when its
   `lsn` is at or above the last one applied to that key, never only when it is above.
2. A snapshot (a subset page or a group load: `lsn`, `snapshot`, `horizon`) is consulted only for a
   key no stream envelope has been applied to since it was taken. Such an envelope is skipped when
   its `lsn` is below the horizon and its `txid` was visible to the snapshot (without a `txid`,
   when its `lsn` is below the snapshot's). After the first envelope at or past the horizon the
   snapshot is not consulted again.
3. An envelope is the key's row or its absence, never a difference, so when in doubt it is applied.

A tail resumed from a stored cursor does not know which keys the stream has changed since the
snapshot. It consults the snapshot only while its cursor is still the snapshot's own offset, and
after that skips nothing on the snapshot's account; it then rejects, as a replay, only what lies
below a position an applied change stored.

An envelope without a parseable `headers.lsn` has no position, which the engine's contract rules
out. The client refuses the batch that carries it, applies none of it and does not move its cursor
(`CircuitsSubsetSourceError.invalidLiveSourceVersion`); it never applies such an envelope as if it
were ordered. The stream is positional, so a read from the same cursor returns the same batch: the
client leaves it behind only by taking a snapshot, whose frontier is past it.

The engine at `1be10fb`, before that contract, writes `operation`, `txid`, `lsn` and `commit_ts`
on a changes-only feed with a plain predicate, and `operation`, `txid` and `commit_ts`, no `lsn`,
on a feed whose predicate has a subquery. A group stream carries each member's envelopes the way
that member's own feed would. `Tests/Fixtures/engine-live-envelopes.json` holds both, captured
from that engine.

## Feed continuity

The engine shares a changes-only subset feed by `(table, canonical where, sorted columns,
changesOnly)`. Releasing the last claim, by `DELETE` or a lapsed lease, does not retire the feed:
the engine keeps appending to its stream until an idle grace period passes, then retires it
(`stream-closed`, then `404` or `410`). While the feed is registered, a later `POST
/v1/subset-feeds` with the identical request returns the same `shapeId` and `streamUrl`; once it is
retired, the create mints a new `shapeId`. Within one engine catalog a shape ID is never minted
twice. Shape IDs and `streamPath` restart in a new query or store generation, and only `streamUrl`
names that generation.

So a `stream-next-offset` read from a `streamUrl` stays a gap-free position in that stream for as
long as a create for the identical request returns the same `streamUrl`. A client may store the
offset with its materialization and, after re-joining, read on from it instead of taking a new
frontier and snapshot. It compares `streamUrl`, never `shapeId` alone. A read from a stored offset
may repeat a change the client already applied (a feed restored after an engine restart replays
idempotently), but never skips one. `404`, `410` or `stream-closed` on that read is the terminal
result above: discard the stored offset and snapshot.

The offset stays valid across client launches: the feed is shared by its request, so a later
launch that claims it under another subscription is answered with the same `streamUrl`
(observed at `1be10fb` after a release, after a killed client, and after an engine restart with a
change committed while the engine was down). A client that resumes instead of taking a snapshot at
each launch depends on the feed carrying every change to the scope since the stored offset,
including each row that left it; a removal the feed never wrote stays in the client's store until
its next snapshot.

## Group continuity

A group is created by `POST /v1/subsets/load` and belongs to the subscription that load named. Its
stream path is minted per group, so no two groups share a `streamUrl`. While that subscription
holds the group, a load with the same subscription and members renews it and answers with the same
`streamUrl`; a load under any other subscription, or after the engine released the group, creates
another group on another stream. The engine releases a group when its lease lapses or its claim is
released, and retires the stream with it; there is no idle grace as for a feed. Output the engine
dropped from a group stream is marked by the must-refetch control above.

So a client may store a group's `streamUrl`, subscription and offset, claim the group again under
that subscription, and read on from the offset when the answer names the same `streamUrl`. A load
reads the group's snapshot on the engine and returns its rows, which a client that resumes
ignores; the re-join below claims the group without them. Observed at `1be10fb`: `DELETE
/v1/shapes/{id}` releases a group when `{id}` is one of its members' shape ids, and answers `200`
without releasing it when `{id}` is the group's own `shapeId`, the one a load returns for release.

The engine from `mh/feed-removals` on keeps a group whose claim was released or whose lease
lapsed, as it keeps a feed: the members stay live and the stream keeps receiving their changes,
until the live-only idle period passes without a touch or the shape cap needs the room. Until
then the same subscription and members get the same `streamUrl` back, from a load or a re-join.
There a release by the group's own `shapeId` is applied, and it is the same release as one by a
member's id.

Every session of a group shares its subscription and its `shapeId`: the next launch claims under
the subscription the last one loaded with. What tells their claims apart is the claim's identity
("Claim identity"): each load and re-join answers `claim`, and the release names it. So a release
an earlier session sent that arrives after a later session claimed the group (a retry of a
request whose answer was lost, or one sent detached) names the earlier claim, is answered `200`
and changes nothing.

A release that names no `claim` (a client or a server from before claim identities) names the
subscription and the group and nothing else, and releases the later session's claim. That
session reads on unaffected, because the group and its stream are kept, and its next lease
renewal, a third of the lease away at most, takes the claim again. In between nothing holds the
group, so an engine at its shape cap may evict it; the session then sees `stream-closed` and
loads again.

The engine's OpenAPI document does not state this; it is engine behavior (`electric-circuits`
`82c77935`) pinned by its retention and retirement conformance tests. The JSON projection, a drift
gate against that document, gains it when the engine documents it.

## Live after a resume

A client that reads on from a stored offset ("Feed continuity", "Group continuity") holds rows
that were current at that offset. When it may report them current again (the collection layer's
`.live`, a group session's `caughtUp`) follows one rule, for a feed and for a group stream, under
long-poll and under SSE:

> A resumed session is live once it has applied a read answer that the stream marked as ending
> at its head, and not before.

An answer ends at the head when it carries `stream-up-to-date`, or when its SSE control event
carries `upToDate: true`. An answer that brings no entry and names the offset the client already
holds says the same: an empty array or a `204` to a read, an SSE control at the offset the read
started from. Applied means every entry of that answer, and of every answer before it, is
committed to the store together with the answer's `stream-next-offset`. An answer without the
mark has more behind it that the stream already holds, so a session that has committed it is not
live yet, however many batches it has committed.

The session asks where the head is; it does not wait at it. A long-poll at the head is held for
the poll window, so a resumed long-poll session starts with the read to the head from the table
above, which is answered at once, and repeats it from each new offset until an answer is marked.
Only then does it long-poll. A resumed SSE session opens its ordinary read: the server sends what
follows the offset and marks the control that reaches the head, or sends that control at once
when the offset is the head. Either way a resume costs one request more than its tail when
nothing is new, and none when something is.

The mark is the server's statement at the moment it wrote the answer. Whatever is appended
afterwards reaches the session on its tail, as a live change. A session that started from a
snapshot is live when the snapshot commits, as before: its frontier was read before the snapshot.

The rule needs three things of whatever sits between the client and the stream: a read without
`live` reaches the stream and is answered at once; `stream-up-to-date` and `stream-next-offset`
come back with the answer; and the answer is not served from a cache to a request that says
`Cache-Control: no-cache`. Observed on the `durable-streams-server` the engine's conformance
harness runs (it pins 0.1.5): a read without `live` at the head answers `200`, `[]`, the same
offset and `stream-up-to-date: true`, with `Cache-Control: public, max-age=60`; the same read
behind the head answers everything up to it, marked; a long-poll that returns entries is marked,
and so is its `204`; an SSE control carries `upToDate: true`. A server that marks nothing still
gets a correct client, by the empty answer. A server that refuses a read without `live` refuses
the resumed read, and the scope takes a snapshot instead, as for any stored offset the stream
will not serve.

## Group re-join

`POST /v1/subsets/rejoin` claims or renews the group a subscription already holds, without the
engine reading a row. A client that kept its rows and its stream offset sends it to resume, and
every lease renewal sends it. It is in the engine from `mh/feed-removals` on and not at `1be10fb`.
The engine's OpenAPI document lists it; the JSON projection gains it with the group load.

**Request.** `POST /v1/subsets/rejoin`, optionally with `?claim=…` ("Claim identity": a lease
renewal names the claim the session holds, a resume names none), and a JSON body, the body of the
load that created the group:

```json
{ "subscription": "…", "members": [{ "key": "…", "table": "…", "where": { }, "columns": ["…"] }] }
```

`subscription` is required (a load mints one when it is omitted; a re-join has nothing to look for
without it). `members` has 1 to 8 entries, one per table; `key`, `where` and `columns` are
optional, as in a load.

**Answers.**

| Status | Meaning | Client |
| --- | --- | --- |
| `200` | The subscription holds a group with exactly those members: the same count and order, and each member's `key`, `table`, `where` and `columns` equal to the load's. Its claim is taken again (the group was released, or its lease lapsed) or renewed (it is held), with a load's lease, and a claim that was taken again is durable before the answer. | Compare `streamUrl` with the stored one. |
| `412` | No such group. The subscription holds none (never loaded, or retired), holds one with other members, or holds one whose stream is closed. Nothing was created, claimed, released or retired. | Load. |
| `400` | The request is malformed, names no `subscription`, or names a table or column the engine does not have. | Terminal, as for a load. |
| `415`, `422` | Not JSON, or JSON that is not this request. | Terminal. |
| `503` | The engine cannot serve, a load of the same subscription is still in flight, or the re-join kept losing its claim to a release. Carries `Retry-After` where a repeat can succeed. | Retry, bounded. |

A `200` body is a load's answer less the snapshot: `groupId`, `shapeId` (equal to `groupId`),
`subscription`, `leaseSeconds`, `claim`, `streamPath`, `streamUrl`, `offset` and `results`. `results` lists
the members in request order, each with `key`, `table`, `shapeId` and `subscription`. `offset` is
the stream's tail when the claim was taken; a resuming client reads on from its own stored offset,
not from this one. There is no `rows`, `lsn`, `snapshot` or `horizon`.

`412` is this route's own status: no other route of this contract answers it, and a server that
does not have the route cannot answer it. Such a server (an engine or gateway from before the
route) answers `404` or `405`, which the client reads as "no re-join here" and not as "no such
group". The engine answers neither `409` nor `410` on this route. A client that receives `410`
handles it as on a load, by a bounded re-POST; `409` as a subscription that cannot hold this
group.

**What it costs and what it changes.** The engine sends Postgres no query for a re-join, so its
cost and its answer's size follow the number of members, not the group's rows, and a renewal
cannot outgrow `ResponseDecodingLimits`. A re-join never creates a group, and never retires or
replaces one: where a load asked for other members replaces a group nobody holds, a re-join
answers `412` and leaves it.

**What the client does with it.** `CircuitsSubsetGroupSource.resume` sends it under the fenced
subscription:

- `200` naming the fenced `streamUrl`: tail the stored cursor. No load, no rows.
- `412`: decline. The `materialize` that follows loads the group once, under this launch's own
  subscription.
- `200` naming another `streamUrl` (the subscription holds a newer group): load under the same
  subscription for its rows; that load is the snapshot.
- `404` or `405`: load under the fenced subscription, as before the route existed, and tail the
  stored cursor when that answer names the fenced `streamUrl`.

`ShapeSubscriptionCoordinator` renews a group's lease with it, naming the claim it holds. `412`
there ends the session as a retired stream does (`reseedRequired` with `terminal(gone)`, the claim
released, nothing created); `404` or `405` renews by load, which names the claim too.

This section replaces a request to the engine that earlier drafts of this document carried. As
built it differs from that request in five places: `subscription` is required; `offset` is in
the answer; the engine never answers `410` or `409` here; a `412` on resume is followed by a load
under the launch's own subscription, not the fenced one; and a server without the route is
recognised by `404` or `405` and asked with a load.

## Requests to the engine

One more thing group resume needs that the client cannot do alone. It is in the engine from
`mh/feed-removals` on ("Group continuity") and was not at `1be10fb`; it stays written here as
what the client relies on.

**A released group that can be re-joined.** A session that stops cleanly releases its group with
`DELETE /v1/shapes/{shapeId}?subscription=…`, where `shapeId` is the one the load returned, the
group's own. An engine that retires a group at its release leaves the next launch only a
snapshot. For the next launch to resume instead, during a retention period after a release or a
lapsed lease:

- a load or a re-join that names the same subscription and the same members answers with the
  same `streamUrl` and takes the claim again; a load does not create a group, and neither answers
  `409`;
- the stream holds every envelope for the group since any offset the engine handed out, which
  means the engine keeps writing the group's output while nobody holds it. Where it did not, the
  must-refetch control stands ahead of whatever it writes after re-attaching;
- the release the client sends, which names the group's `shapeId`, starts that retention. At
  `1be10fb` it is answered `200` and changes nothing;
- when retention ends the stream closes, and a later load answers with a `streamUrl` that was
  never used before.

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
