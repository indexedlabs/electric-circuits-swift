# Changelog

All notable changes to `ElectricCircuitsSwift` are documented here. Release impact follows the
[Semantic Versioning policy](Policies/SEMVER.md).

## Unreleased

- `ShapeSubscriptionCoordinator` now answers `410 Gone` from the two native routes that mint a
  shape (`POST /v1/shapes`, `POST /v1/subset-feeds`) with a bounded client-side fall-through: a
  byte-identical re-POST under the same table, predicate, columns, and stable claim. A recreated
  join reaches the application through the existing replacement/`requireReseed` path. `404` on a
  create, `410` on any other control route, and the `408`/`425`/`429`/`5xx` retryable
  classification are unchanged.
- `CircuitsSubsetSource` routes its initial changes-only feed create through the same bounded
  recreate, so a persisted materialization ID joining a dormant shape no longer fails collection
  setup before the frontier HEAD and snapshot query.
- Add `ShapeSubscriptionRecreatePolicy` (default: 2 recreates, 250 ms backoff), its shared
  `recreatingOnGone(clock:willRecreate:_:)`/`isGone(_:)`/`goneStatus` seam, and the defaulted
  `ShapeSubscriptionCoordinator.init(recreatePolicy:)` and
  `CircuitsSubsetSource.init(recreatePolicy:)` parameters. Additive and source-compatible.

## 0.2.1

- Publish the collection-coordination release from the canonical `main` branch and update release
  and consumer qualification references for the `0.2.1` tag.

## 0.2.0

- Add the storage-neutral `ElectricCircuitsCollections` product with typed predicates, exact-demand
  lease sharing, an atomic row-claim/snapshot/cursor store contract, and an in-memory reference store.
- Add the native subset snapshot + changes-feed source adapter with an awaited live-apply boundary and
  stable subset-feed claim renewal.
- Allow `ShapeSubscriptionCoordinator` to own either ordinary shapes or subset-feed claims.
- `CollectionChange` now requires a `CollectionSourceVersion` on each upsert and delete. Collection
  store providers must migrate their live-apply implementation to persist per-change versions and
  tombstones; `CollectionChangeBatch.sourceVersion` remains the high-water cursor record.

## 0.1.0 — Initial release

- Foundation-only SwiftPM client for Electric Circuits native `/v1` lifecycle, subset-query, and
  durable-stream APIs.
- `URLSessionTransport`, bounded response decoding, typed errors, and caller-owned authentication
  and retry policy boundaries.
- Deterministic in-memory store/materializer seams; the optional LinearLite example keeps its GRDB
  provider and iOS SwiftUI host outside the core package.
- Swift tools 6.0; iOS 16+ and macOS 13+ support profile.

This release is source-compatible within its supported line but does not promise a stable binary
module interface. Rebuild consumers from source after updating.
