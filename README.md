# ElectricCircuitsSwift

Native SwiftPM control-plane client for Electric Circuits. It talks directly to the Rust engine's
versioned `/v1` REST API; it does not depend on the legacy `ElectricSync` module or tRPC.

The first slice includes Sendable Codable models for the Rust predicate/value grammar, shape and
aggregate lifecycle, one-shot subset queries, and an injectable URLSession transport. Every shape
request gets a stable subscription claim by default, so retrying the same request value is
idempotent. The aggregate endpoint intentionally returns the same shape metadata as a shape create
(the aggregate value is emitted by its durable stream), matching the Rust `/v1/aggregates` OpenAPI
contract.

The package has no database dependency. `ElectricCircuitsStore` remains a small async, `Sendable`
blob-cache seam over `Data`; `InMemoryElectricCircuitsStore` is included for tests and previews.
Stream materialization is a separate boundary: `ShapeMaterializer.apply(_:advancingTo:)` gives a
provider one atomic operation for a `ChangeBatch` and its `StreamCursor`. `InMemoryShapeMaterializer`
is the deterministic actor-backed implementation used by the package tests. The LinearLite GRDB
example uses one canonical issue table plus per-view membership and durable optimistic-overlay
tables; it can share rows across multiple filtered subscriptions without making GRDB part of the
core package. Optimistic writes use a client-generated UUIDv4 `client_id` alongside the server's
numeric primary key, so the live feed can reconcile a write without a provisional numeric key.

The separate `ElectricCircuitsCollections` product adds an on-demand, normalized collection layer
without adding a database dependency. A stable `CollectionDefinition` owns one canonical entity set;
typed `CollectionPredicate` values compile logical field IDs into native Circuits columns; and a
`CollectionCoordinator` de-duplicates exact demands behind independently releasable leases. Its
`CollectionStore` transaction includes canonical rows, per-materialization row claims, snapshot
fences, and live cursors. `InMemoryCollectionStore` is the reference provider; applications can
implement the same contract with GRDB or another store.

To revalidate stored stale marks, provide the definition's optional `rebuildDemand` closure and
`subscriptionKind` label, and run `startStaleRevalidation(clock:telemetry:)` in an app-owned task.
The gate starts closed: call `setStaleRevalidationGate(isOpen:)` after first paint, keeping it open
only while foreground and online. The pass shares ordinary leases, re-runs at most two unheld
subscriptions at a time (smallest first), and retries failures with capped backoff. Held leases,
including leases retained by the app's warm pool, clear without another request. A missing
rebuilder, a nil result, or a rebuilt demand with a different normalized identity removes the
stale materialization. A mark an earlier launch's copy carries, in the same principal and
authorization, is re-read under that copy's own `cacheKey`: this generation's demand finds the
same copy, its snapshot replaces the copy's claims in place, and the pass clears the mark and keeps
the copy. A mark clears only when the copy the pass held or re-read is the marked one. Other
principals and authorization scopes are excluded.
Idle listings run every five seconds; opening the gate, local snapshot/feed writes, completed
runs and retry expiry wake the pass immediately. Cancel and await the task when
retiring its scope or generation. Precondition: a retiring generation releases its leases and
cancels and awaits its stale pass before a newer generation's coordinator is created. Generations
of one principal and authorization share stored copies by `cacheKey` and cannot be ordered, and a
copy has one cursor, so two generations live on one copy at once would fail the older lease on its
next batch, and a retiring generation's pass could re-read, rebind or remove a copy a newer
generation is live on. The precondition rules both out; the app enforces it. The store protocol is
unchanged in 0.7.1.

For app-wide scheduling across collections, await `drainStaleRevalidation()` before opening the
next collection's gate or starting an explicit repair read. Drain closes admission and lets
admitted snapshots finish through lease release, including retrying a refused release. It does
not wait for ordinary request-retry timers or release screen-held leases. Admission stays closed
until `setStaleRevalidationGate(isOpen: true)`. Lifecycle gate closure and task retirement take
precedence over drain: they cancel snapshots and release-retry backoff. Closure retains refused
release authority for reopening; retirement makes one final best-effort release without an
endless DELETE retry loop. The app owns the global budget across coordinators.

After a foreground or online transition, call `resumeAndDrainStaleRevalidation()` before any new
repair admission, and proceed only if it returns `true`. This reopens lifecycle permission with
admission closed and resumes cleanup directly from retained lease IDs, including predecessors
that disappeared from the stale listing during snapshot takeover. Refused releases still occupy
the two-slot budget. A `false` result means closure, retirement, cancellation or a concurrent
admission reopen interrupted the handoff; retry on a later eligible transition. Cancelling this
call closes its lifecycle gate and stops retry backoff. With no running pass and no retained work
it returns `true`; retained work without a live pass returns `false`. Screen-held leases remain
owned by their callers. Only explicitly opening the gate admits another background stale read.

A definition can supply `subscriptionKindForDemand` to classify each original stored demand
into a fixed, content-free label such as `push_read` or `thread`. The classifier takes precedence
over the static `subscriptionKind`; the attribute is omitted when neither is configured.

Each `CollectionChange` carries its own `CollectionSourceVersion`; a `CollectionChangeBatch`
`sourceVersion` is only the batch high-water mark and cursor record. Custom collection stores must
persist versioned upserts and tombstones per change so a lower-LSN sibling in a coalesced batch
cannot overwrite or resurrect a newer canonical row. Persist the optional `snapshot` and
`transactionID` metadata in `CollectionSourceVersion` and use `incoming.supersedes(existing)` for
row comparisons; `order` and `Comparable` remain LSN high-water marks. A page snapshot may exclude
a transaction that committed below its LSN. Overlapping materializations compare page visibility,
so a late page cannot regress a feed change or a newer snapshot.

`CircuitsSubsetSource` performs the native on-demand handoff in causal order: establish a named
changes-only feed, read its durable frontier, query the subset snapshot, commit it, then consume live
batches with awaited store application. When the engine returns `snapshot` and `horizon`, overlap
is skipped only for transactions visible to that snapshot below its WAL insert horizon, using
32-bit xid wraparound ordering. The gate retires when the tail reaches the horizon. Engines without
these fields retain LSN positioning; an unparseable or missing feed txid also uses the LSN fallback
below the horizon. It renews the same subset-feed claim through
`ShapeSubscriptionCoordinator`, which adopts the handle the setup create returned rather than
creating the claim a second time, and the producer cannot advance its local cursor until the
consumer's atomic store callback returns.

A demand the store already holds is resumed rather than re-materialized when the source allows it.
When the store holds a copy for the demand's `cacheKey`, `CollectionCoordinator` first calls the
adapter's `resume`, which returns a `CollectionResumeSession` (a live tail from the stored cursor
and source version) or nil. That includes a copy an earlier generation stored, so the first acquire
after a relaunch resumes too: the coordinator first asks the store to bind the copy to this
generation's demand (`CollectionStore.rebindMaterialization`), which changes nothing else about it.
A provider that binds a copy only by snapshot keeps the default, which answers false, and its
later launches snapshot as before, with no extra request. A resumed attempt skips
`replaceSnapshot`: the demand stays `.cached` and reports `.live` once the session calls
`caughtUp`, which it does when the tail has read from the stored cursor to the feed's head. A
batch the tail commits on the way changes the store and not the state, because the feed may hold
more after it. Feed deletes release claims through the ordinary store `apply`. Nil takes the
snapshot path; the default implementation always returns nil. Stale revalidation always snapshots,
and so does an acquire or a refresh the caller asks a snapshot of (below). A session whose feed
refuses the stored cursor throws `CollectionResumeInvalidated`: until the tail goes live, the same
attempt snapshots instead, so no lease sees a failure. A transient failure fails the attempt, and
the next attempt resumes again.

The caller decides when a stored copy may continue and when it must be read again, with a
`CollectionLoadMode`. `acquire(_:)` and `lease.refresh()` are `.resume`: continue from the stored
cursor when the source can, snapshot when it cannot. That fits a first load and a retry after a
failure. `acquire(_:mode: .snapshot)` and `lease.refresh(.snapshot)` take a new snapshot, which
replaces the stored rows and claims, without asking the source to resume. That fits whenever the
stored rows themselves are in doubt: the app marked the scope stale, read its authority again, or
took it over from another holder. A resumed tail applies only what the feed carries after the
stored cursor; it never reads the source.

```swift
let lease = await coordinator.acquire(demand)  // may resume
await lease.refresh()                           // retry: may resume
await lease.refresh(.snapshot)                  // read the source again
let fresh = await coordinator.acquire(demand, mode: .snapshot)
```

A snapshot the caller asked for is owed until one commits. An attempt that fails first is followed
by another snapshot whichever mode the retry names, and `refresh(.snapshot)` is answered by an
attempt that starts after the call, not by a snapshot an earlier attempt was already reading. The
debt ends with the demand's last lease. Leases on one demand share one attempt:
`acquire(_:mode: .snapshot)` replaces a shared attempt that resumed, and joins one that took its
own snapshot as it is, as every acquire did before sources could resume. The stale pass asks the
same way.

`CircuitsSubsetSource` fences each snapshot with the feed's stream URL and the snapshot's
transaction visibility (`subset-v2`), and resumes by re-joining the identical feed request under
this launch's subscription. The engine shares a feed by its request, not its subscription, and
answers with the same stream URL until it retires the feed; the stream is append-only, so the same
URL means the stored offset is still a gap-free position in it
(`Contracts/native-v1-contract-v1.md`, "Feed continuity"). A resume makes one `/v1/subset-feeds`
POST and no frontier `HEAD` or snapshot query. Limited windows, `subset-v1` fences and a different
stream URL snapshot instead, and so does a fence whose last session ended in a terminal stream
outcome, a non-retryable failure other than an authentication refusal (401, 403, 407), or a batch
the source refused; transport loss, exhausted retries and authentication refusals leave it
resumable. A feed that answers with a
different URL is kept for the snapshot that follows on the same source instance; an adapter that
snapshots through another instance first calls `cleanupAbandonedMaterialization`, which releases it.

A resumed tail is live once it has applied a read answer the stream marked as ending at its head,
and not before (`Contracts/native-v1-contract-v1.md`, "Live after a resume"). It asks where the
head is; it does not wait there. The stream server holds a long-poll at the head for its poll
window, about 30 seconds, so a resumed long-poll tail first reads with no `live` parameter, which
the server answers at once: with what follows the stored offset, and with `stream-up-to-date` when
that reaches the head. It repeats that read until an answer is marked, and only then long-polls.
Over SSE the stream's control event carries the mark. So a scope with nothing new is live one
short request after its feed create, and a scope that is behind is live when the last change the
stream held has been committed, under both read modes. The same holds for a resumed group. A
gateway in front of the stream must pass a read without `live`, return `stream-up-to-date` and
`stream-next-offset`, and not cache that answer; the read carries `Cache-Control: no-cache`.
`ShapeStreamReader` and `ShapeSubscriptionCoordinator` expose this as `onCaughtUp`, for a
materializer of your own that starts from a stored cursor.

A live change is positioned by `headers.lsn`, and both sources follow the engine's rules for a
position (`Contracts/native-v1-contract-v1.md`, "Live envelope position"): an envelope for a key
is applied when its `lsn` is at or above the last one applied to that key; a snapshot is consulted
only for a key the stream has not changed since, and a resumed tail whose cursor has left the
snapshot's offset consults none. A batch with an envelope that has no parseable `headers.lsn`
is refused whole: nothing of it is applied, the cursor stays, and the session ends with
`CircuitsSubsetSourceError.invalidLiveSourceVersion` (the group source's error has the same case).
The same holds for a key or row that does not decode. The stream serves that batch again at the
stored cursor, so the source does not resume from a fence whose session ended that way: the next
attempt snapshots, and its frontier is past the batch. A resumed tail that reaches such a batch
before it has reported anything live snapshots in the same attempt. Resume relies on the feed
carrying every change to the scope since the stored cursor, removals included; a snapshot is no
longer taken at each launch to make up for one the feed missed
(`Contracts/native-v1-contract-v1.md`, "Live envelope position" and "Feed continuity").

Each demand has two keys. `CollectionDemandIdentity.cacheKey` names the stored copy: the
principal, authorization, collection, canonical predicate, order, limit and the source's columns
(`*` for every column), never the scope's generation. A `CollectionStore` keeps one
materialization per `cacheKey`, so a later launch's demand finds the copy an earlier launch stored
and its snapshot replaces that copy's claims in place; the stale pass re-reads a marked copy under
its own key and clears the mark. `storageKey` adds the generation and names the in-memory lease, so
two launches never share one. Each source names its server subscription from the materialization ID
plus the generation, so the stored copy's ID is stable while every launch opens its own
subscription. A `CollectionGroupMember`'s `identity(scope:)` has the `cacheKey` of the
single-table demand for the same collection, predicate and columns.

```swift
let definition = CollectionDefinition<Issue, Int64>(
  id: CollectionID(rawValue: "issues"), key: \.id)
let demand = CollectionDemand(
  predicate: IssueFields.status == "open",
  order: [CollectionOrder(field: IssueFields.modified, direction: .descending)])
let lease = await coordinator.acquire(demand)
// Observe canonical rows through the application's store, then release this consumer's interest.
try await lease.release()
```

`CollectionDemand.limit` turns a demand into a live window: the snapshot is the ordered page
(`limit` with exactly one `order` column; the page query adds `<order column> IS NOT NULL`, so rows
without a sort key are never window members even when fewer than `limit` rows qualify), the
changes-only feed still covers the whole predicate, and each tail batch that carries an accepted
change re-queries the page and applies the difference (page rows as upserts, departed keys as
deletes, both versioned by the page snapshot when available). The page must reflect every accepted
transaction in the batch it answers, falling back to the batch LSN on older engines. An older page
is retried on the subscription's retry policy and, if the budget is exhausted, the
window fails with `stalePage` rather than acknowledging changes it does not reflect. Page fetches
and store applications are serialized per materialization, and cursor LSN high-water marks never
regress even when a lower-LSN page already includes the batch's transactions. Construct
`CircuitsSubsetSource` with `keyForRow:` to enable it (for example `keyForRow: \.id`); it must
return the same stable identity the collection definition and store key rows by, and a limited
demand without it is rejected before any server resource exists. Rows that leave the window are
released from the materialization's claim, so hold a separate demand for anything that must stay
local beyond the page.

`CircuitsSubsetGroupSource` loads several tables as one shape group, such as a Record and its
companion tables: one `loadSubsetGroup(_:)` call names the subscription and every member and
returns one handle, one stream, one snapshot position and rows tagged by table. Each
`CollectionGroupMember` names its table, predicate and the `CollectionDefinition` its rows land
in. The source routes each stream envelope to its member by `type`, keys rows by table and key
so equal keys in two tables never collide, and hands every batch to a `CollectionGroupStore` as
one `CollectionGroupChangeBatch`: every member's changes and the group's one cursor, which the
store commits in one transaction. A refused member, a `503` or a broken stream fails the whole
group. `InMemoryCollectionGroupStore` is the reference provider.

A group's lease is renewed by re-joining it (`POST /v1/subsets/rejoin`), which reads no rows, so a
renewal's size follows the number of members and cannot outgrow `ResponseDecodingLimits`. On a
server without that route a renewal is still the load: it re-reads the whole group snapshot, the
client decodes only the claim, and a group whose rows grow past the limit after setup fails at
its next renewal. Enable groups for users only against an engine and gateway that have the
re-join.

A stored group resumes too. Its snapshot fence (`group-v2`) names the group's stream, the
subscription that holds the group and the snapshot's transaction visibility.
`CircuitsSubsetGroupSource.resume` re-joins the group under that stored subscription
(`POST /v1/subsets/rejoin`, `ElectricCircuitsClient.rejoinSubsetGroup(_:)`) and, when the engine
answers with the fenced stream URL, returns a `CollectionGroupResumeSession` that tails the
stream from the stored cursor; the caller replaces nothing in the store. The engine reads no rows
for a re-join, so a resume costs one request whose size follows the number of members, and every
lease renewal is the same request. The engine keeps a group for its subscription to come back to
after a release or a lapsed lease, until its live-only idle period or its shape cap retires it,
so this works after a clean stop as well as after a killed process. Once the engine has retired
the group, the re-join answers `412` and creates nothing: `resume` returns nil, and the
`materialize` that follows loads the group once. A server without the route (`404` or `405`: the
engine at `1be10fb`, or a gateway from before it) is asked with the load the route replaced,
there and at each renewal; see "Group re-join" and "Group continuity" in the contract. A must-refetch marker or a refused
batch on a resumed stream throws `CollectionResumeInvalidated`, and the source then declines that
fence. `InMemoryCollectionGroupStore` keeps rows per collection, principal and authorization, so a
later generation reads and continues what an earlier one stored.

A group has no coordinator, so its caller makes the two decisions a lease makes for a single
table. Whether to resume: pass the same `CollectionLoadMode` to `resume`. With `.snapshot` it makes
no request, releases a load an earlier declined `resume` kept, and returns nil, so the
`materialize` that follows reads the group anew. When the stored rows are current: when the
resumed session calls `caughtUp`, by the rule above, and not at its first batch. The contract's
"Requests to the engine" lists what the engine still has to provide for a group to resume after a
clean stop, and the request that would replace the load a resume makes.

```swift
let demand = CollectionGroupDemand(members: [
  CollectionGroupMember(
    definition: records, table: "public.record", predicate: RecordFields.id == recordID,
    decodeRow: Record.init(row:), decodeKey: { $0 }),
  CollectionGroupMember(
    definition: assignments, table: "public.assignment",
    predicate: AssignmentFields.recordID == recordID,
    decodeRow: Assignment.init(row:), decodeKey: { $0 }),
])
let identity = demand.identity(scope: scope)
// `mode` is the caller's: `.resume` for a first load or a retry, `.snapshot` to read again.
if let record = try await store.groupMaterialization(id),
  let resumed = try await groupSource.resume(
    demand, identity: identity, materializationID: id, record: record, mode: mode)
{
  do {
    // The stored rows are current when `caughtUp` is called: the tail has read to the head.
    try await resumed.run({ batch in try await store.applyGroup(batch, to: id) }, { /* live */ })
    return
  } catch is CollectionResumeInvalidated {
    // The stream no longer serves the stored cursor: load the group again.
  }
}
let session = try await groupSource.materialize(demand, identity: identity, materializationID: id)
try await store.replaceGroupSnapshot(session.snapshot, materializationID: id, demand: identity)
try await session.run { batch in try await store.applyGroup(batch, to: id) }
```

```swift
let client = ElectricCircuitsClient(baseURL: URL(string: "https://engine.example")!)
let shape = try await client.createShape(ShapeRequest(table: "public.items"))
let rows = try await client.querySubset(SubsetQuery(table: "public.items", limit: 20))
try await client.releaseShape(shape)
```

HTTP headers and cookies can be supplied by `URLSessionTransport` (using `headers`, `cookieHeader`,
or Foundation `HTTPCookie` values) or by a custom `HTTPTransport` decorator. Authentication policy,
token refresh, and server-side authorization remain transport/server-owned; this library does not
persist or interpret credentials. A credential-aware transport can retry its own `401`/`403`
response after refreshing a caller-owned credential; the core sees only the final transport result
and never records headers or cookies as telemetry attributes.

```swift
let transport = URLSessionTransport(
  session: .shared,
  headers: ["X-Request-ID": "preview"],
  cookieHeader: "session=example")
let client = ElectricCircuitsClient(baseURL: engineURL, transport: transport)

let store = InMemoryElectricCircuitsStore()
try await store.write(Data("snapshot".utf8), forKey: "shape/s1")
```

`ResponseDecodingLimits` provides finite, immutable application-admission limits: successful
responses are checked before JSON decoding (1 MiB by default), and the built-in
`URLSessionTransport` retains at most `limit` bytes in its library-owned `Data`. It treats the
next byte as the capped `limit + 1` observation and cancels the task. The same configured limit is
passed from `ElectricCircuitsClient` and `ShapeStreamReader` to that transport, so callers that
raise their decoding limit also raise that transport's library-owned accumulation ceiling.
Foundation, URLSession, and the socket stack may buffer independently, and this library does not
control pooled-connection or TCP teardown timing. Non-success responses keep only status and
`Retry-After` classification; their bodies are not accumulated by this package or exposed.
Custom `HTTPTransport` implementations must enforce any receive bound they require themselves.
Durable-stream batches accept at most 10,000 change events before the provider's atomic apply.

For an application with many views, share one `ShapeSubscriptionCapacity` across its
`ShapeSubscriptionCoordinator`s. It has no pending-start queue: when its hard active-claim limit is
full, `start()` fails with `ShapeSubscriptionFailure.capacityExceeded(limit:)` before provider
preflight or a create request. A permit covers the coordinator's create/renew/stream/retry work and
is returned only after an unclaimed start fails or the owned server claim has been released during
stop/reseed. Cancelling the sole initial `start()` waits for any landed claim to be released before
it returns; cancelling a joined waiter never tears down a claim another caller has already received.
Omit `capacity` to preserve the original unbounded-by-this-library behavior.

```swift
let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 100)
let coordinator = ShapeSubscriptionCoordinator(
  client: client, transport: transport, request: request, materializer: materializer,
  capacity: capacity)
```

`Scripts/qualify-subscription-capacity.sh` runs the deterministic 1/10/100/1,000 admission
qualification. It reports active/rejected/create/release/cleanup counters; its only timeout is an
external hang guard, not a performance or latency assertion.

`JSONValue.int(Int64)` and `JSONValue.decimal(Decimal)` preserve database numbers without routing
PostgreSQL integers through `Double`. `ChangeEnvelope`, `EnvelopeHeaders`, and `ChangeBatch` mirror
the Rust durable-stream envelope. `ShapeStreamReader` performs the minimal long-poll loop:
`GET streamUrl?offset=...&live=long-poll`, JSON-array decoding, `stream-next-offset` checkpointing,
204 idle handling, cancellation propagation, and typed terminal errors for 404/410/`stream-closed`.
It advances its local cursor only after `ShapeMaterializer.apply` returns.

Network reachability remains caller-owned. `URLSessionTransport` is the normal HTTP transport, so a
reserved `.invalid` hostname produces a typed, retry-policy-bounded transport outcome without a
provider mutation or diagnostic exposure of request bodies, cookies, or authorization headers. An
application that knows its path is unavailable should express that at its injected `HTTPTransport`
boundary (for example, hold an in-flight long poll until its own path gate opens), rather than adding
Network.framework reachability or a generic scheduler to this package. Cancelling an in-flight
response leaves the provider's durable cursor unchanged; once the caller allows the same live claim
to continue, the next poll resumes from that cursor.

The engine retires a dormant shape whose replay is over budget, and abandons a reactivation join
past its own timeout, as a typed recreate outcome rather than a server fault (electric-circuits
ADR-0011, *Dormant reactivation is bounded*). The two native routes that mint a shape,
`POST /v1/shapes` and `POST /v1/subset-feeds`, normally answer that by falling through to a fresh
create in the same round trip and returning `2xx` with a new shape id. When that fall-through is
exhausted, they answer `410 Gone` and expect the client to recreate. `ShapeSubscriptionCoordinator`
performs that fall-through itself: a byte-identical re-POST that carries no shape id — the retired
id only ever lived on the server — so the same table, predicate, columns, and stable subscription
claim are re-sent and the caller's demand identity survives. A recreated join answers with a fresh
shape id, which is the same replacement the engine's own fall-through produces, so it reaches the
application through the existing `requireReseed` path: the retired identity and the replacement are
both released under that claim, and one `.reseedRequired` publication carries both ids.

`CircuitsSubsetSource` uses the same seam for the changes-only feed it creates before any
coordinator exists, so a persisted materialization ID joining a dormant shape recreates rather than
failing setup before the frontier `HEAD` and snapshot query. It takes the same policy through its
own defaulted `recreatePolicy:` parameter and forwards it to the coordinator it builds for the live
tail.

`ShapeSubscriptionRecreatePolicy` bounds that fall-through (2 recreates and a 250 ms backoff by
default). The bound matters because a join that timed out leaves a detached replay running on the
engine, so each attempt can cost its full join timeout; the backoff avoids a hot loop rather than
pacing that replay. Past the bound a standing `410` is terminal
`ClientError.http(status: 410, …)`. `404` on a create stays terminal — ADR-0011 keeps it out of the
create vocabulary because it is ambiguous with an unknown shape id, and these are collection
endpoints with no shape id at all — as does `410` on any other control route, including
`DELETE /v1/shapes/{id}`. `408`, `425`, `429`, and `5xx` keep their retryable classification.

```swift
let coordinator = ShapeSubscriptionCoordinator(
  client: client, transport: transport, request: request, materializer: materializer,
  recreatePolicy: ShapeSubscriptionRecreatePolicy(
    maximumRecreates: 2, backoff: .milliseconds(250)))
```

`stream-closed: true`, `404`, and `410` are terminal **generation** receipts. The coordinator
releases the old named claim and publishes `.reseedRequired`; it never creates a replacement or
reuses the old provider cursor automatically. The application must create a fresh
principal/template/subscription/generation scope and commit its new snapshot while the old scope
remains selected for visible reads. Only then atomically switch the application's visible-generation
selection to the fresh scope and purge the old scope. Start the explicit fresh claim from the new
snapshot cursor; do not create it automatically from the terminal receipt. The LinearLite GRDB
example contains that scope-specific handoff and qualification. Run
`sh Scripts/qualify-network-recovery.sh` for the DNS,
caller-owned path/cancellation, and closed-stream generation contracts; its only time limits are
external deadlock guards.

The framing provenance is the Rust `apps/engine/src/ds.rs` `Envelope`/`ReadResult` contract and
`packages/conformance/src/engine-native.ts`'s offset-resumable reader in the sibling
`electric-circuits` repository. See `Tests/Fixtures/README.md` and the fixture for the exact body.
`ClientError.retryableHTTP(status:retryAfter:)` retains transient status and a parsed
`Retry-After` directive without requiring retry policy to parse error text. The coordinator applies
the greater of its exponential backoff and that directive, capped by its configured maximum; stop
cancels a pending backoff. Nonretryable `ClientError.http` keeps the HTTP status and a fixed public
classification only: it never copies server error bytes, response headers, cookies, or provider
diagnostics into errors or telemetry. Stream replacement/reseed and foreground renewal remain later slices. The
`Examples/LinearLite` package now includes the typed GRDB provider and a small iOS 16-compatible
SwiftUI frontend. The host selects a recent-subset view while the session still supports the
original full-shape mode for lifecycle tests and embedding. Authentication remains in the injected
transport/server boundary. The live top-N implementation uses the same LSN-aware client-side window
positioning as Electric Circuits: ordinary in-window changes apply locally, while boundary changes
trigger a bounded query-back to refill the page.

Run tests with `swift test`.

## Probe

`ElectricCircuitsSwiftProbe` is a development executable, not a product. It drives the real
collection stack (`CollectionCoordinator` with `CircuitsSubsetSource`, and
`CircuitsSubsetGroupSource`) against an engine URL, with a file-backed store that keeps rows,
cursor and fence, so a second process started on the same store file is a relaunch. It prints one
JSON object per line: state changes, the path each attempt took (snapshot, resume or
resume-declined), every applied change, failures with the SDK's error, and every network request
with a stream read's envelopes as the engine sent them. Commands on stdin ask for the stored rows
and the request counters, acquire or refresh a scope in either load mode
(`{"cmd":"refresh","scope":"mine","mode":"snapshot"}`), or stop the process with or without
releasing its claims. `--live sse`, or `ECS_SWIFT_PROBE_LIVE=sse`, reads over SSE.

```sh
swift run ElectricCircuitsSwiftProbe --engine http://127.0.0.1:3000 --store /tmp/probe.json \
  --scope '{"name":"mine","table":"items","key":"id","where":{"col":"list_id","op":"eq","value":1}}' \
  --scope '{"name":"pair","group":[{"table":"lists","key":"id"},{"table":"items","key":"id"}]}'
```

The engine repository's `packages/conformance/src/conformance-swift-client.test.ts` boots a real
engine, Postgres and durable-streams, spawns the probe, changes rows in Postgres and compares the
probe's rows with SQL. It also writes `Tests/Fixtures/engine-live-envelopes.json`.
`conformance-swift-client-resume.test.ts` beside it checks, under long-poll and SSE, that a
resumed scope and group report live without waiting out a long-poll and only after every change
made while they were down, and that a snapshot refresh reads the source again.

## Support and compatibility

The first supported library profile is Swift tools 6.0, iOS 16+, and macOS 13+. The native `/v1`
wire contract, public API, and provider-schema release rules are explicit in
[Policies/SUPPORT.md](Policies/SUPPORT.md) and [Policies/SEMVER.md](Policies/SEMVER.md). The DocC
catalog begins at `ElectricCircuitsSwift` in Xcode's documentation viewer.

## Install 0.8.0

Custom `CollectionStore` providers must implement `staleMaterializations()` and
`clearStale(_:ifMarkedAt:)`. A snapshot omission or feed delete of a held row releases only that
request's claim and atomically marks every other materialization still holding the row. Marks
advance with later drops and survive snapshots and feed applications; only a matching conditional
clear or removal of the materialization clears them. The store does not re-run stale requests.

Add the `ElectricCircuitsCollectionsTesting` product to a provider's test target to run the shared
`CollectionStoreStaleMarkTests` cases. Supply a fresh store factory, a collection definition and
scope, a row factory with distinct keys, and callbacks to inspect stored keys and claims and seed a
claim without a materialization. Call each case from its own `@Test` function, as in
[`CollectionStoreStaleMarkContractTests`](Tests/ElectricCircuitsCollectionsTests/CollectionStoreStaleMarkContractTests.swift).
The test-support product uses Swift Testing; the two production libraries keep their existing
dependencies.

```swift
dependencies: [
  .package(url: "https://github.com/indexedlabs/electric-circuits-swift.git", from: "0.8.0"),
]
```

Add the `ElectricCircuitsSwift` product to targets that use only the native protocol client. Set
`ElectricCircuitsClient.Configuration(liveReadMode: .sse)` to hold one live SSE response per
stream; `.longPoll` remains the default. Pass a closure instead (`Configuration(liveReadMode: { ... })`)
to have each reader ask for its mode before every live read, for example behind a feature flag. Add
`ElectricCircuitsCollections` for typed demands, leases, snapshot/live coordination, and the
store-provider contract. The optional LinearLite GRDB provider remains a separate example package.
See [the release guide](Docs/RELEASING.md) for the release contract and versioned-source-control
consumer qualification.

## Local package-quality gate

On macOS with Xcode installed, run:

```sh
Scripts/quality.sh
```

This runs strict-concurrency/warnings-as-errors SwiftPM builds and tests for the root and LinearLite
packages, dependency inspection, formatter lint, a warnings-as-errors DocC conversion, isolated
XcodeGen regeneration comparison, a temporary exact-version source-control consumer proof, and an
unsigned generic iOS Simulator host build. To use the exact checked XcodeGen release rather than an
installed binary:

```sh
Scripts/install-xcodegen.sh --bin-dir /tmp/electric-circuits-xcodegen
XCODEGEN_BIN_DIR=/tmp/electric-circuits-xcodegen Scripts/quality.sh
```

## License

Dual-licensed under [MIT](LICENSE-MIT) or [Apache 2.0](LICENSE-APACHE), at your option.
