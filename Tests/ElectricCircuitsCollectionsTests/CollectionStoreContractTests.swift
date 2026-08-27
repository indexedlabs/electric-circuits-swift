import ElectricCircuitsCollections
import ElectricCircuitsSwift
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

@Suite("Collection store contract")
struct CollectionStoreContractTests {
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
        changes: [.delete(shared.id)],
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
      changes: [.upsert(updated)],
      expectedCursor: beginning,
      cursor: next
    )

    try await store.apply(batch, to: materialization)
    try await store.apply(batch, to: materialization)

    #expect(await store.rows() == [updated.id: updated])
    #expect(try await store.materialization(for: demand("today"))?.cursor == next)

    let rejected = CollectionChangeBatch<TestIssue, Int>(
      changes: [.delete(updated.id)],
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
    try await store.replaceSnapshot(
      .init(
        rows: [.init(id: 1, title: "old")], fence: .init(rawValue: "old"),
        sourceVersion: version(10), cursor: .init(offset: "10")),
      materializationID: materialization, demand: identity)
    try await store.apply(
      .init(
        changes: [.upsert(.init(id: 1, title: "older-live"))], expectedCursor: .init(offset: "20"),
        cursor: .init(offset: "21"), sourceVersion: version(15)), to: materialization)
    #expect(await store.rows(for: identity)[1]?.title == "new")
    #expect(try await store.materialization(for: identity)?.sourceVersion == version(20))
  }
}
