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

## Support and compatibility

The first supported library profile is Swift tools 6.0, iOS 16+, and macOS 13+. The native `/v1`
wire contract, public API, and provider-schema release rules are explicit in
[Policies/SUPPORT.md](Policies/SUPPORT.md) and [Policies/SEMVER.md](Policies/SEMVER.md). The DocC
catalog begins at `ElectricCircuitsSwift` in Xcode's documentation viewer.

## Install 0.1.0

```swift
dependencies: [
  .package(url: "https://github.com/indexedlabs/electric-circuits-swift.git", from: "0.1.0"),
]
```

Add the `ElectricCircuitsSwift` product to the target that uses the client. The optional LinearLite
GRDB provider remains a separate example package. See [the release guide](Docs/RELEASING.md) for the
release contract and versioned-source-control consumer qualification.

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
