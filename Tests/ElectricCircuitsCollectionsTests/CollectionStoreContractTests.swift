import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

private struct TestIssue: Equatable, Sendable {
  let id: Int
  let title: String
}

private let issues = CollectionDefinition<TestIssue, Int>(
  id: CollectionID(rawValue: "issues"),
  key: \.id
)

private let scope = CollectionScope(
  principal: "user-1",
  authorization: "workspace-1",
  generation: "generation-1"
)

private func demand(_ identity: String) -> CollectionDemandIdentity {
  CollectionDemand<TestIssue>(unsafePredicateIdentity: identity).identity(for: issues, scope: scope)
}

private func version(_ order: UInt64) -> CollectionSourceVersion {
  CollectionSourceVersion(rawValue: "0/\(String(order, radix: 16).uppercased())", order: order)
}

private func assertDataCorrupted<T: Decodable>(_ type: T.Type, _ json: String) {
  #expect {
    try JSONDecoder().decode(type, from: Data(json.utf8))
  } throws: { error in
    if case DecodingError.dataCorrupted = error { return true }
    return false
  }
}

@Suite("Collection store contract")
struct CollectionStoreContractTests {
  @Test func persistedIdentityAndVersionValuesRejectEmptyComponentsAndRoundTrip() throws {
    assertDataCorrupted(CollectionID.self, #"""#)
    assertDataCorrupted(
      CollectionScope.self, #"{"principal":"","authorization":"a","generation":"g"}"#)
    assertDataCorrupted(
      CollectionDemandIdentity.self,
      #"{"collection":"issues","scope":{"principal":"u","authorization":"a","generation":"g"},"canonicalDemand":""}"#
    )
    assertDataCorrupted(CollectionMaterializationID.self, #"""#)
    assertDataCorrupted(SnapshotFence.self, #"""#)
    assertDataCorrupted(CollectionSourceVersion.self, #"{"rawValue":"","order":1}"#)

    let identity = CollectionDemandIdentity(
      collection: .init(rawValue: "issues"),
      scope: .init(principal: "u", authorization: "a", generation: "g"), canonicalDemand: "all")
    let record = CollectionMaterializationRecord(
      id: .init(rawValue: "materialization"), demand: identity, state: .live,
      snapshotFence: .init(rawValue: "fence"), sourceVersion: version(42),
      cursor: .init(offset: "42"))
    let data = try JSONEncoder().encode(record)
    #expect(try JSONDecoder().decode(CollectionMaterializationRecord.self, from: data) == record)
  }

  @Test func sourceVersionComparisonUsesOrderAsItsOnlySemanticIdentity() {
    let first = CollectionSourceVersion(rawValue: "a", order: 7)
    let second = CollectionSourceVersion(rawValue: "b", order: 7)
    #expect(first == second)
    #expect(!(first < second))
    #expect(!(second < first))
    #expect(Set([first, second]).count == 1)
  }

  @Test func equalOrderVersionsUseTheSameStoreFreshnessRuleForSnapshotsRowsAndHighWater()
    async throws
  {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "today")
    let identity = demand("today")
    let first = CollectionSourceVersion(rawValue: "first-representation", order: 7)
    let second = CollectionSourceVersion(rawValue: "second-representation", order: 7)
    let original = TestIssue(id: 1, title: "original")
    let replacement = TestIssue(id: 1, title: "replacement")

    try await store.replaceSnapshot(
      .init(rows: [original], fence: .init(rawValue: "first"), sourceVersion: first,
        cursor: .init(offset: "0")), materializationID: materialization, demand: identity)
    try await store.replaceSnapshot(
      .init(rows: [replacement], fence: .init(rawValue: "second"), sourceVersion: second,
        cursor: .init(offset: "1")), materializationID: materialization, demand: identity)

    #expect(await store.rows(for: identity) == [replacement.id: replacement])
    #expect(try await store.materialization(for: identity)?.sourceVersion == second)
    #expect(try await store.materialization(for: identity)?.cursor == .init(offset: "1"))
  }

  @Test func overlappingSnapshotReplacementRetainsRowsClaimedByAnotherMaterialization() async throws
  {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let september = CollectionMaterializationID(rawValue: "september")
    let today = CollectionMaterializationID(rawValue: "today")
    let shared = TestIssue(id: 1, title: "Shared")

    try await store.replaceSnapshot(
      CollectionSnapshot(rows: [shared], fence: SnapshotFence(rawValue: "september-fence-1")),
      materializationID: september,
      demand: demand("september")
    )
    try await store.replaceSnapshot(
      CollectionSnapshot(rows: [shared], fence: SnapshotFence(rawValue: "today-fence-1")),
      materializationID: today,
      demand: demand("today")
    )

    try await store.replaceSnapshot(
      CollectionSnapshot(rows: [], fence: SnapshotFence(rawValue: "september-fence-2")),
      materializationID: september,
      demand: demand("september")
    )

    #expect(await store.rows()[shared.id] == shared)
    #expect(await store.rowClaims(for: september).isEmpty)
    #expect(await store.rowClaims(for: today) == [shared.id])
  }

  @Test func liveRemovalRetainsRowsClaimedByAnotherMaterialization() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let september = CollectionMaterializationID(rawValue: "september")
    let today = CollectionMaterializationID(rawValue: "today")
    let shared = TestIssue(id: 1, title: "Shared")
    let beginning = StreamCursor(offset: "0")

    try await store.replaceSnapshot(
      CollectionSnapshot(
        rows: [shared], fence: SnapshotFence(rawValue: "september-fence"), cursor: beginning),
      materializationID: september,
      demand: demand("september")
    )
    try await store.replaceSnapshot(
      CollectionSnapshot(rows: [shared], fence: SnapshotFence(rawValue: "today-fence")),
      materializationID: today,
      demand: demand("today")
    )

    try await store.apply(
      CollectionChangeBatch(
        changes: [.delete(shared.id, sourceVersion: version(0))],
        expectedCursor: beginning,
        cursor: StreamCursor(offset: "1")
      ),
      to: september
    )

    #expect(await store.rows()[shared.id] == shared)
    #expect(await store.rowClaims(for: september).isEmpty)
    #expect(await store.rowClaims(for: today) == [shared.id])
  }

  @Test func liveBatchReplayIsIdempotentAndCursorConflictChangesNothing() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "today")
    let original = TestIssue(id: 1, title: "Original")
    let updated = TestIssue(id: 1, title: "Updated")
    let beginning = StreamCursor(offset: "0")
    let next = StreamCursor(offset: "1")

    try await store.replaceSnapshot(
      CollectionSnapshot(
        rows: [original], fence: SnapshotFence(rawValue: "today-fence"), cursor: beginning),
      materializationID: materialization,
      demand: demand("today")
    )
    let batch = CollectionChangeBatch<TestIssue, Int>(
      changes: [.upsert(updated, sourceVersion: version(0))],
      expectedCursor: beginning,
      cursor: next
    )

    try await store.apply(batch, to: materialization)
    try await store.apply(batch, to: materialization)

    #expect(await store.rows() == [updated.id: updated])
    #expect(try await store.materialization(for: demand("today"))?.cursor == next)

    let rejected = CollectionChangeBatch<TestIssue, Int>(
      changes: [.delete(updated.id, sourceVersion: version(0))],
      expectedCursor: beginning,
      cursor: StreamCursor(offset: "2")
    )
    await #expect(
      throws: CollectionStoreError.cursorConflict(
        expected: beginning,
        actual: next,
        advancingTo: StreamCursor(offset: "2")
      )
    ) {
      try await store.apply(rejected, to: materialization)
    }

    #expect(await store.rows() == [updated.id: updated])
    #expect(await store.rowClaims(for: materialization) == [updated.id])
    #expect(try await store.materialization(for: demand("today"))?.cursor == next)
  }

  @Test func sameKeyInDifferentScopesNeverLeaksCanonicalRowsOrClaims() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let first = CollectionMaterializationID(rawValue: "first")
    let second = CollectionMaterializationID(rawValue: "second")
    let firstDemand = demand("all")
    let otherScope = CollectionScope(
      principal: "user-2", authorization: "workspace-1", generation: "generation-1")
    let secondDemand = CollectionDemand<TestIssue>(unsafePredicateIdentity: "all").identity(
      for: issues, scope: otherScope)
    try await store.replaceSnapshot(
      .init(
        rows: [.init(id: 1, title: "first")], fence: .init(rawValue: "f1"),
        sourceVersion: version(1)),
      materializationID: first, demand: firstDemand)
    try await store.replaceSnapshot(
      .init(
        rows: [.init(id: 1, title: "second")], fence: .init(rawValue: "f2"),
        sourceVersion: version(1)),
      materializationID: second, demand: secondDemand)
    #expect(await store.rows(for: firstDemand)[1]?.title == "first")
    #expect(await store.rows(for: secondDemand)[1]?.title == "second")
    try await store.removeMaterialization(first)
    #expect(await store.rows(for: secondDemand)[1]?.title == "second")
  }

  @Test func olderOverlappingSnapshotAndLiveBatchCannotOverwriteNewerCanonicalRow() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "today")
    let identity = demand("today")
    try await store.replaceSnapshot(
      .init(
        rows: [.init(id: 1, title: "new")], fence: .init(rawValue: "new"),
        sourceVersion: version(20), cursor: .init(offset: "20")),
      materializationID: materialization, demand: identity)
    await #expect(
      throws: CollectionStoreError.staleSnapshot(current: version(20), received: version(10))
    ) {
      try await store.replaceSnapshot(
        .init(
          rows: [.init(id: 1, title: "old")], fence: .init(rawValue: "old"),
          sourceVersion: version(10), cursor: .init(offset: "10")),
        materializationID: materialization, demand: identity)
    }
    try await store.apply(
      .init(
        changes: [.upsert(.init(id: 1, title: "older-live"), sourceVersion: version(15))],
        expectedCursor: .init(offset: "20"),
        cursor: .init(offset: "21"), sourceVersion: version(15)), to: materialization)
    #expect(await store.rows(for: identity)[1]?.title == "new")
    #expect(try await store.materialization(for: identity)?.sourceVersion == version(20))
  }

  @Test func olderOverlappingUpsertRetainsItsClaimAfterNewerMaterializationEvicts() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let older = CollectionMaterializationID(rawValue: "older")
    let newer = CollectionMaterializationID(rawValue: "newer")
    let oldRow = TestIssue(id: 1, title: "old bytes")
    let newRow = TestIssue(id: 1, title: "new bytes")

    try await store.replaceSnapshot(
      .init(
        rows: [], fence: .init(rawValue: "older"), sourceVersion: version(1),
        cursor: .init(offset: "0")),
      materializationID: older,
      demand: demand("older")
    )
    try await store.replaceSnapshot(
      .init(rows: [newRow], fence: .init(rawValue: "newer"), sourceVersion: version(20)),
      materializationID: newer,
      demand: demand("newer")
    )
    try await store.apply(
      .init(
        changes: [.upsert(oldRow, sourceVersion: version(10))], expectedCursor: .init(offset: "0"),
        cursor: .init(offset: "1"),
        sourceVersion: version(10)
      ),
      to: older
    )

    #expect(await store.rows()[1] == newRow)
    #expect(await store.rowClaims(for: older) == [1])
    try await store.removeMaterialization(newer)
    #expect(await store.rows()[1] == newRow)
    #expect(await store.rowClaims(for: older) == [1])
  }

  @Test func eachChangeUsesItsOwnVersionAcrossCoalescedMaterializationBatches() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let newer = CollectionMaterializationID(rawValue: "newer")
    let other = CollectionMaterializationID(rawValue: "other")
    let key = TestIssue(id: 1, title: "k@100")
    let stale = TestIssue(id: 1, title: "k@50")
    let later = TestIssue(id: 1, title: "k@110")
    let sibling = TestIssue(id: 2, title: "j@120")

    try await store.replaceSnapshot(
      .init(rows: [key], fence: .init(rawValue: "newer"), sourceVersion: version(100)),
      materializationID: newer, demand: demand("newer"))
    try await store.replaceSnapshot(
      .init(
        rows: [], fence: .init(rawValue: "other"), sourceVersion: version(0),
        cursor: .init(offset: "0")), materializationID: other, demand: demand("other"))

    try await store.apply(
      .init(
        changes: [
          .upsert(stale, sourceVersion: version(50)),
          .upsert(sibling, sourceVersion: version(120)),
        ],
        expectedCursor: .init(offset: "0"), cursor: .init(offset: "1"),
        sourceVersion: version(120)), to: other)
    #expect(await store.rows() == [key.id: key, sibling.id: sibling])

    try await store.apply(
      .init(
        changes: [.upsert(later, sourceVersion: version(110))],
        expectedCursor: .init(offset: "1"), cursor: .init(offset: "2"),
        sourceVersion: version(120)), to: other)
    #expect(await store.rows() == [later.id: later, sibling.id: sibling])
  }

  @Test func orderedDeleteThenSameVersionUpsertKeepsTheUpsert() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "ordered")
    let original = TestIssue(id: 1, title: "original")
    let replacement = TestIssue(id: 1, title: "replacement")
    try await store.replaceSnapshot(
      .init(
        rows: [original], fence: .init(rawValue: "ordered"), sourceVersion: version(1),
        cursor: .init(offset: "0")), materializationID: materialization, demand: demand("ordered"))

    try await store.apply(
      .init(
        changes: [
          .delete(original.id, sourceVersion: version(10)),
          .upsert(replacement, sourceVersion: version(10)),
        ],
        expectedCursor: .init(offset: "0"), cursor: .init(offset: "1"), sourceVersion: version(10)),
      to: materialization)
    #expect(await store.rows()[replacement.id] == replacement)
  }

  @Test func duplicateMaterializationBindingIsRejectedWithoutMutation() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "shared")
    let first = demand("first")
    let second = demand("second")
    try await store.replaceSnapshot(
      .init(rows: [.init(id: 1, title: "first")], fence: .init(rawValue: "first")),
      materializationID: materialization, demand: first)

    await #expect(
      throws: CollectionStoreError.materializationBoundToDifferentDemand(
        materializationID: materialization, existing: first, requested: second
      )
    ) {
      try await store.replaceSnapshot(
        .init(rows: [.init(id: 2, title: "second")], fence: .init(rawValue: "second")),
        materializationID: materialization, demand: second)
    }
    #expect(await store.rows(for: first)[1]?.title == "first")
    #expect(try await store.materialization(for: second) == nil)
  }

  @Test func duplicateDemandBindingIsRejectedWithoutMutation() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let demand = demand("shared")
    let firstMaterialization = CollectionMaterializationID(rawValue: "first")
    let secondMaterialization = CollectionMaterializationID(rawValue: "second")
    let original = TestIssue(id: 1, title: "original")
    let replacement = TestIssue(id: 2, title: "replacement")
    let originalCursor = StreamCursor(offset: "0")

    try await store.replaceSnapshot(
      .init(rows: [original], fence: .init(rawValue: "first"), sourceVersion: version(1),
        cursor: originalCursor), materializationID: firstMaterialization, demand: demand)
    let committed = try #require(await store.materialization(for: demand))

    await #expect(
      throws: CollectionStoreError.demandBoundToDifferentMaterialization(
        demand: demand, existing: firstMaterialization, requested: secondMaterialization)
    ) {
      try await store.replaceSnapshot(
        .init(rows: [replacement], fence: .init(rawValue: "second"), sourceVersion: version(2),
          cursor: .init(offset: "1")), materializationID: secondMaterialization, demand: demand)
    }

    #expect(try await store.materialization(for: demand) == committed)
    #expect(await store.rows(for: demand) == [original.id: original])
    #expect(await store.rowClaims(for: firstMaterialization) == [original.id])
    #expect(await store.rowClaims(for: secondMaterialization).isEmpty)
  }

  @Test func evictingTheLastDeletedMaterializationRetainsNoTombstone() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "deleted")
    let identity = demand("deleted")
    let row = TestIssue(id: 1, title: "deleted")
    try await store.replaceSnapshot(
      .init(rows: [row], fence: .init(rawValue: "snapshot"), sourceVersion: version(1),
        cursor: .init(offset: "0")), materializationID: materialization, demand: identity)
    try await store.apply(
      .init(changes: [.delete(row.id, sourceVersion: version(2))], expectedCursor: .init(offset: "0"),
        cursor: .init(offset: "1"), sourceVersion: version(2)), to: materialization)

    try await store.removeMaterialization(materialization)
    #expect(await store.rows(for: identity).isEmpty)
    #expect(await store.tombstoneCount(for: identity) == 0)
  }

  @Test func tombstoneProtectsAnOverlappingStaleUpsertUntilThatMaterializationAdvances()
    async throws
  {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let stale = CollectionMaterializationID(rawValue: "stale")
    let deleting = CollectionMaterializationID(rawValue: "deleting")
    let identity = demand("overlap")
    let staleIdentity = demand("overlap-stale")
    let row = TestIssue(id: 1, title: "old bytes")

    try await store.replaceSnapshot(
      .init(rows: [], fence: .init(rawValue: "stale"), sourceVersion: version(1),
        cursor: .init(offset: "0")), materializationID: stale, demand: staleIdentity)
    try await store.replaceSnapshot(
      .init(rows: [row], fence: .init(rawValue: "deleting"), sourceVersion: version(1),
        cursor: .init(offset: "0")), materializationID: deleting, demand: identity)
    try await store.apply(
      .init(changes: [.delete(row.id, sourceVersion: version(20))], expectedCursor: .init(offset: "0"),
        cursor: .init(offset: "1"), sourceVersion: version(20)), to: deleting)
    #expect(await store.tombstoneCount(for: identity) == 1)

    try await store.apply(
      .init(changes: [.upsert(row, sourceVersion: version(10))], expectedCursor: .init(offset: "0"),
        cursor: .init(offset: "1"), sourceVersion: version(10)), to: stale)
    #expect(await store.rows(for: identity).isEmpty)
    #expect(await store.tombstoneCount(for: identity) == 1)

    try await store.apply(
      .init(changes: [], expectedCursor: .init(offset: "1"), cursor: .init(offset: "2"),
        sourceVersion: version(20)), to: stale)
    #expect(await store.tombstoneCount(for: identity) == 0)
  }

  @Test func staleSnapshotIsExplicitlyRejectedWithoutMutation() async throws {
    let store = InMemoryCollectionStore<TestIssue, Int>(key: \.id)
    let materialization = CollectionMaterializationID(rawValue: "today")
    let identity = demand("today")
    try await store.replaceSnapshot(
      .init(
        rows: [.init(id: 1, title: "new")], fence: .init(rawValue: "new"),
        sourceVersion: version(20)), materializationID: materialization, demand: identity)
    await #expect(
      throws: CollectionStoreError.staleSnapshot(current: version(20), received: version(10))
    ) {
      try await store.replaceSnapshot(
        .init(
          rows: [.init(id: 1, title: "old")], fence: .init(rawValue: "old"),
          sourceVersion: version(10)), materializationID: materialization, demand: identity)
    }
    #expect(await store.rows(for: identity)[1]?.title == "new")
  }
}
