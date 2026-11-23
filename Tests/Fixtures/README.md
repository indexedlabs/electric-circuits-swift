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

`engine-live-envelopes.json` records the plain and subquery changes-only feeds used by
`EngineLiveEnvelopeContractTests`. The checked-in capture is from engine `1be10fb`, before the
engine's position fix; its plain feed carries `headers.lsn`, while the subquery feed deliberately
has no position so the SDK's refusal path stays covered. Engine main at `5b2cf3e` stamps
`headers.lsn` on every change, including removals. Group streams and the probe executable's group
paths are intentionally absent from this single-scope change.
