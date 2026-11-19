# Durable stream fixtures

`shape-stream-upsert-delete.json` is a captured-shape-style JSON response body, not an
encode/decode-only example. It follows the Rust durable-stream client in
`apps/engine/src/ds.rs` and the native conformance reader in
`packages/conformance/src/engine-native.ts`:

- the body is a JSON array of State-Protocol envelopes;
- `headers.operation` is `upsert` or `delete` (insert/update are also accepted as upserts);
- `txid`, `lsn`, `seq`, `last`, and a server-stamped per-envelope `offset` are retained as metadata;
- the response's resume checkpoint is the HTTP `stream-next-offset` header, not the envelope
  offset or an array index.

The stream reader tests additionally exercise the HTTP-only framing cases: `live=long-poll`, an
idle `204`, a terminal `stream-closed: true` response, and terminal `404`/`410` responses.

## Engine live envelopes

`engine-live-envelopes.json` holds envelopes a real engine wrote, read straight from its
durable streams: nothing in `envelopes` is hand-written. It is the output of the engine
repository's `packages/conformance/src/conformance-swift-client.test.ts` ("the live envelopes the
SDK's fixtures pin"), which boots the engine, Postgres and durable-streams, loads three scopes
through this package's probe and runs the statements listed under `steps`. To write it again, at
the engine commit to pin:

```sh
swift build --product ElectricCircuitsSwiftProbe
cd <engine checkout>
ECS_SWIFT_PROBE_FIXTURES=<this directory>/engine-live-envelopes.json \
  corepack pnpm exec vitest run packages/conformance/src/conformance-swift-client.test.ts -t "fixtures pin"
```

`engine` is the engine commit. `plain` is a changes-only feed with a plain predicate, `subquery`
a feed whose predicate has a subquery, and `group` a group stream with one member of each kind.
At `1be10fb`, the commit the checked-in file was written at, the plain feed's envelopes carry
`headers.lsn` and the subquery feed's do not; the group stream carries both.
`EngineLiveEnvelopeContractTests` pins what the SDK does with each. From the engine's `a248886`
(#71, merged to its `main`) on every envelope on all three streams carries `headers.lsn`, and the engine
test asserts that; the file is not rewritten from it here, so that the SDK keeps a test of what it
does with an envelope that has no position.
