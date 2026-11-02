import ElectricCircuitsSwift
import Foundation
import Testing

@testable import ElectricCircuitsCollections

private struct CacheRow: Equatable, Sendable {
  let id: Int
}

private let cacheRows = CollectionDefinition<CacheRow, Int>(
  id: CollectionID(rawValue: "record"), key: \.id)

private func scope(
  principal: String = "user", authorization: String = "space", generation: String = "launch-1"
) -> CollectionScope {
  CollectionScope(principal: principal, authorization: authorization, generation: generation)
}

/// Every request answers with the next scripted rows, at a source version that rises per request,
/// and keeps its feed open until the coordinator stops it.
private actor LaunchSource: CollectionSourceAdapter {
  struct Request: Sendable {
    let identity: CollectionDemandIdentity
    let materializationID: CollectionMaterializationID
  }

  private var snapshots: [[Int]]
  private var feeds:
    [Int: AsyncThrowingStream<CollectionChangeBatch<CacheRow, Int>, any Error>.Continuation] = [:]
  private(set) var requests: [Request] = []

  init(snapshots: [[Int]]) { self.snapshots = snapshots }

  func materialize(
    _ demand: CollectionDemand<CacheRow>, identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<CacheRow, Int> {
    let index = requests.count
    requests.append(.init(identity: identity, materializationID: materializationID))
    let rows = snapshots.isEmpty ? [] : snapshots.removeFirst()
    let feed = AsyncThrowingStream<CollectionChangeBatch<CacheRow, Int>, any Error>.makeStream()
    feeds[index] = feed.continuation
    let order = UInt64(index + 1)
    return .init(
      snapshot: .init(
        rows: rows.map(CacheRow.init(id:)), fence: .init(rawValue: "snapshot-\(order)"),
        sourceVersion: .init(rawValue: "0/\(order)", order: order),
        cursor: .init(offset: "\(order)")),
      run: { apply in
        for try await batch in feed.stream { try await apply(batch) }
      },
      stop: { await self.stop(index) })
  }

  private func stop(_ index: Int) {
    feeds.removeValue(forKey: index)?.finish()
  }
}

private func states(
  of lease: CollectionLease, until final: CollectionLoadState
) async -> [CollectionLoadState] {
  var seen: [CollectionLoadState] = []
  for await state in lease.stateUpdates {
    seen.append(state)
    if state == final { break }
  }
  return seen
}

@Suite("Collection cache keys")
struct CollectionCacheKeyTests {
  @Test func generationNamesTheLeaseButNotTheStoredCopy() {
    let demand = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
    let first = demand.identity(for: cacheRows, scope: scope(generation: "launch-1"))
    let second = demand.identity(for: cacheRows, scope: scope(generation: "launch-2"))
    #expect(first.cacheKey == second.cacheKey)
    #expect(first.storageKey != second.storageKey)
    #expect(first != second)
    #expect(!first.cacheKey.contains("launch-1"))
  }

  @Test func everyPartOfTheMeaningChangesTheCacheKey() {
    let base = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
      .identity(for: cacheRows, scope: scope())
    let order = [CollectionOrder<CacheRow>(unsafeFieldID: "starts_at")]
    let variants = [
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
        .identity(for: cacheRows, scope: scope(principal: "other")),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
        .identity(for: cacheRows, scope: scope(authorization: "other")),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
        .identity(
          for: CollectionDefinition<CacheRow, Int>(id: .init(rawValue: "assignment"), key: \.id),
          scope: scope()),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=2")
        .identity(for: cacheRows, scope: scope()),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
        .identity(for: cacheRows, scope: scope(), columns: ["id"]),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1", order: order)
        .identity(for: cacheRows, scope: scope()),
      CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1", order: order, limit: 5)
        .identity(for: cacheRows, scope: scope()),
    ]
    for variant in variants { #expect(variant.cacheKey != base.cacheKey) }
    #expect(Set(variants.map(\.cacheKey)).count == variants.count)
  }

  @Test func everyColumnIsNeverTheSameKeyAsAColumnNamedStar() {
    let demand = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
    let all = demand.identity(for: cacheRows, scope: scope())
    let star = demand.identity(for: cacheRows, scope: scope(), columns: ["*"])
    let none = demand.identity(for: cacheRows, scope: scope(), columns: [])
    #expect(Set([all.cacheKey, star.cacheKey, none.cacheKey]).count == 3)
    // Every column keeps the storage key a demand had before columns were part of it.
    let fiveComponents = ["record", "user", "space", "launch-1", all.canonicalDemand]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
    #expect(all.storageKey == fiveComponents)
    #expect(all.storageKey != star.storageKey)
  }

  @Test func aGroupsTableHasTheKeyOfTheTableLoadForIt() {
    let member = CollectionGroupMember(
      definition: cacheRows, table: "public.record", unsafePredicateIdentity: "day=1",
      decodeRow: { _ in CacheRow(id: 0) }, decodeKey: { _ in 0 })
    let narrowed = CollectionGroupMember(
      definition: cacheRows, table: "public.record", unsafePredicateIdentity: "day=1",
      columns: ["id"], decodeRow: { _ in CacheRow(id: 0) }, decodeKey: { _ in 0 })
    let table = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
    let launch1 = scope(generation: "launch-1")
    let launch2 = scope(generation: "launch-2")

    #expect(member.identity(scope: launch1) == table.identity(for: cacheRows, scope: launch1))
    #expect(
      member.identity(scope: launch2).cacheKey
        == table.identity(for: cacheRows, scope: launch1).cacheKey)
    #expect(
      narrowed.identity(scope: launch1).cacheKey
        == table.identity(for: cacheRows, scope: launch1, columns: ["id"]).cacheKey)

    let group = CollectionGroupDemand(members: [member])
    #expect(group.identity(scope: launch1).cacheKey == group.identity(scope: launch2).cacheKey)
    #expect(
      group.identity(scope: launch1).storageKey != group.identity(scope: launch2).storageKey)
  }

  @Test func persistedIdentityKeepsItsColumnsAndOlderValuesDecodeAsEveryColumn() throws {
    let narrowed = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
      .identity(for: cacheRows, scope: scope(), columns: ["id", "title"])
    let decoded = try JSONDecoder().decode(
      CollectionDemandIdentity.self, from: JSONEncoder().encode(narrowed))
    #expect(decoded == narrowed)
    #expect(decoded.cacheKey == narrowed.cacheKey)

    let older = Data(
      #"{"collection":"record","scope":{"principal":"user","authorization":"space","generation":"launch-1"},"canonicalDemand":"all"}"#
        .utf8)
    #expect(try JSONDecoder().decode(CollectionDemandIdentity.self, from: older).columns == nil)
  }

  @Test func eachLaunchLeasesSeparatelyAndRefreshesTheOneStoredCopy() async throws {
    let store = InMemoryCollectionStore<CacheRow, Int>(key: \.id)
    let source = LaunchSource(snapshots: [[1, 2], [2]])
    let demand = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
    let earlier = CollectionCoordinator(
      definition: cacheRows, scope: scope(generation: "launch-1"), source: source, store: store)
    let later = CollectionCoordinator(
      definition: cacheRows, scope: scope(generation: "launch-2"), source: source, store: store)

    let first = await earlier.acquire(demand)
    #expect(await states(of: first, until: .live).contains(.cached) == false)
    let second = await later.acquire(demand)
    // The later launch draws the earlier launch's copy before its own snapshot.
    #expect(await states(of: second, until: .live) == [.unavailable, .cached, .refreshing, .live])

    let requests = await source.requests
    #expect(requests.count == 2)
    #expect(requests[0].identity.storageKey != requests[1].identity.storageKey)
    #expect(requests[0].identity.cacheKey == requests[1].identity.cacheKey)
    #expect(requests[0].materializationID == requests[1].materializationID)

    let record = try #require(try await store.materialization(for: requests[0].identity))
    #expect(record.id == requests[0].materializationID)
    #expect(record.demand == requests[1].identity)
    #expect(try await store.materialization(for: requests[1].identity) == record)
    // The later snapshot replaced the copy's claims in place: no second copy holds row 1.
    #expect(await store.rowClaims(for: record.id) == [2])
    #expect(Set(await store.rows().keys) == [2])

    // Each launch's lease keeps its own lifecycle.
    try await first.release()
    #expect(await second.state() == .live)
    try await second.release()
    #expect(try await store.materialization(for: requests[1].identity) == record)
  }

  @Test func evictingFromARetiredGenerationLeavesTheNewerLiveCopy() async throws {
    let store = InMemoryCollectionStore<CacheRow, Int>(key: \.id)
    let source = LaunchSource(snapshots: [[1, 2], [2, 3]])
    let demand = CollectionDemand<CacheRow>(unsafePredicateIdentity: "day=1")
    let retired = CollectionCoordinator(
      definition: cacheRows, scope: scope(generation: "launch-1"), source: source, store: store)
    let current = CollectionCoordinator(
      definition: cacheRows, scope: scope(generation: "launch-2"), source: source, store: store)
    let first = await retired.acquire(demand)
    _ = await states(of: first, until: .live)
    try await first.release()
    let live = await current.acquire(demand)
    _ = await states(of: live, until: .live)
    let requests = await source.requests
    let id = requests[1].materializationID
    #expect(id == requests[0].materializationID)

    // The retired generation holds no lease, so eviction is allowed, but the copy is now bound
    // to the newer generation's demand and stays.
    try await retired.evict(demand)

    let record = try #require(try await store.materialization(for: requests[1].identity))
    #expect(record.id == id)
    #expect(record.demand == requests[1].identity)
    #expect(await store.rowClaims(for: id) == [2, 3])
    #expect(Set(await store.rows().keys) == [2, 3])
    // The newer generation's next batch still applies to its copy.
    try await store.apply(
      .init(
        changes: [.upsert(CacheRow(id: 4), sourceVersion: .init(rawValue: "0/3", order: 3))],
        expectedCursor: record.cursor, cursor: .init(offset: "3"),
        sourceVersion: .init(rawValue: "0/3", order: 3)),
      to: id)
    #expect(await store.rowClaims(for: id) == [2, 3, 4])
    #expect(await live.state() == .live)

    // The generation the copy is bound to can still evict it once its lease is gone.
    try await live.release()
    try await current.evict(demand)
    #expect(try await store.materialization(for: requests[1].identity) == nil)
    #expect(await store.rows().isEmpty)
  }
}
