# LinearLite GRDB provider

This example is an isolated Swift package that keeps the dependency-free
`ElectricCircuitsSwift` core separate from a typed local database provider and a small SwiftUI
frontend. `LinearLiteGRDB` uses GRDB's `DatabaseQueue`; `LinearLiteApp` provides an iOS 16
SwiftUI session/view over that provider.

Run the provider tests and strict build from this directory:

```sh
cd Examples/LinearLite
swift build -Xswiftc -warnings-as-errors
swift test -Xswiftc -warnings-as-errors
```

The app boundary is deliberately one view-scoped materializer per session. `LinearLiteSession`
defaults to the original stable, full-row `public.issues` shape and live stream for embedding and
lifecycle tests. The iOS host explicitly selects `recentSubset(limit: 10)`: Start/Refresh Recent 10
POSTs `/v1/subsets/query` with all Issue columns and `modified DESC`, then atomically replaces that
local view snapshot. The feed is live: the client applies fresh in-window deltas directly and only
performs a bounded query when a full page needs a boundary refill; idle and stale batches advance
only the durable cursor.

To embed it in an iOS app, add this repository as a local Swift package in Xcode (or add the
`Examples/LinearLite` path as a local package dependency), then add the `LinearLiteGRDB` product to
the app target. Create one `DatabaseQueue` at app startup, pass it to
`LinearLiteShapeMaterializer(database:shapeID:)`, and retain one materializer per shape ID:

```swift
let database = try DatabaseQueue(path: databaseURL.path)
let materializer = try LinearLiteShapeMaterializer(database: database, shapeID: shapeID)
let reader = ShapeStreamReader(
  streamURL: shape.streamURL,
  transport: transport,
  materializer: materializer)
try await reader.run()
```

For a SwiftUI screen, inject the same transport into both `ElectricCircuitsClient` and the session;
the transport/server remains responsible for authentication and authorization:

```swift
let session = LinearLiteSession(
  client: client, database: database, subscription: "linearlite-main", transport: transport)
await session.start()
// LinearLiteIssuesView(session: session)
await session.stop()
```

The migration creates one canonical `issues` table with the LinearLite column names (`project_id`
and `kanbanorder`) and `id` as the server-assigned primary key. New writes also carry a separate
client-generated UUIDv4 in `client_id`; this is the reconciliation identity for optimistic writes,
not a replacement for the numeric primary key. Its nullable `last_lsn` watermark prevents an older
overlapping feed from regressing a newer cached row. `subset_view_members(view_id, issue_id,
row_lsn)` stores per-subscription membership and row watermarks, so two filtered views can share one
cached issue row without overwriting or deleting one another's membership. `shape_cursors` remains
keyed by view/shape ID. `issue_overlays` is a durable client-owned write layer keyed by mutation
ID and client ID; it is intentionally separate from server-authoritative rows and is retired when
the feed returns the matching client ID (or an explicit rejection removes it).

A versioned migration converts the previous shape-scoped schema. Equal copies of a row collapse to
one canonical row while memberships are preserved; conflicting copies fail closed with a reseed
error instead of silently choosing a winner. Old unscoped rows still fail closed because their view
ownership cannot be inferred.

Each batch applies full issue rows (not partial patches), changes membership for the current view,
deletes by the bare Int64 primary-key string, prunes canonical rows only when no view or pending
overlay owns them, and commits row changes plus the cursor in one GRDB transaction. UI reads use
`allIssuesIncludingOverlays()`: pending and acknowledged patches project over canonical rows while
rejected patches do not; the authoritative `allIssues()` path remains available for reconciliation.
`LinearLiteShapeMaterializer` is an actor, which serializes concurrent `apply` calls; use its
async cursor/read APIs from UI code.
