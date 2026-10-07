import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Testing

/// Shared stale-mark contract cases. Call each case from a provider's own `@Test` function.
/// The factory must return a fresh, empty store for each call. Inspection and seeding callbacks
/// use the provider's test fixtures, without adding operations to `CollectionStore`.
public struct CollectionStoreStaleMarkTests<Store: CollectionStore>: Sendable {
  private let makeStore: @Sendable () async throws -> Store
  private let definition: CollectionDefinition<Store.Model, Store.Key>
  private let scope: CollectionScope
  private let row: @Sendable (Int) -> Store.Model
  private let storedKeys: @Sendable (Store, CollectionDemandIdentity) async throws -> Set<Store.Key>
  private let claimedKeys:
    @Sendable (Store, CollectionMaterializationID) async throws -> Set<Store.Key>
  private let seedClaim:
    @Sendable (
      Store, Store.Key, CollectionMaterializationID, CollectionDemandIdentity
    ) async throws -> Void

  /// `row` must produce distinct keys for distinct integers. `seedClaimWithoutMaterialization`
  /// retains an existing row under the supplied owner and domain, without creating a record.
  public init(
    makeStore: @escaping @Sendable () async throws -> Store,
    definition: CollectionDefinition<Store.Model, Store.Key>,
    scope: CollectionScope,
    row: @escaping @Sendable (Int) -> Store.Model,
    storedKeys:
      @escaping @Sendable (Store, CollectionDemandIdentity) async throws -> Set<Store.Key>,
    claimedKeys:
      @escaping @Sendable (Store, CollectionMaterializationID) async throws -> Set<Store.Key>,
    seedClaimWithoutMaterialization:
      @escaping @Sendable (
        Store, Store.Key, CollectionMaterializationID, CollectionDemandIdentity
      ) async throws -> Void
  ) {
    self.makeStore = makeStore
    self.definition = definition
    self.scope = scope
    self.row = row
    self.storedKeys = storedKeys
    self.claimedKeys = claimedKeys
    self.seedClaim = seedClaimWithoutMaterialization
  }

  public func reloadOmissionMarksOtherHolders() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "chat", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    try await expectMarks(store, owners: ["push", "chat"], at: 10)
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    #expect(try await claimedKeys(store, id("chat")) == [key(1)])
    #expect(try await storedKeys(store, demand("deck")) == [key(1)])
  }

  public func feedDeleteMarksOtherHolders() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    // Even a holder with newer canonical bytes and a live record must be marked.
    try await snapshot(store, "push", rows: [1], at: 30)
    try await snapshot(store, "chat", rows: [1])
    try await delete(store, "deck", row: 1, at: 10, frontier: 20)
    try await expectMarks(store, owners: ["push", "chat"], at: 10)
    #expect(try await claimedKeys(store, id("deck")).isEmpty)
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    #expect(try await claimedKeys(store, id("chat")) == [key(1)])
    #expect(try await storedKeys(store, demand("deck")) == [key(1)])
  }

  public func reloadOmissionDoesNotMarkOtherScopesOrCollections() async throws {
    let store = try await makeStore()
    let push = demand("push")
    let otherScope = CollectionDemandIdentity(
      collection: definition.id,
      scope: .init(
        principal: scope.principal + "-other", authorization: scope.authorization,
        generation: scope.generation),
      canonicalDemand: push.canonicalDemand)
    let otherCollection = CollectionDemandIdentity(
      collection: .init(rawValue: definition.id.rawValue + "-other"), scope: scope,
      canonicalDemand: push.canonicalDemand)
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "other-scope", rows: [1], demand: otherScope)
    try await snapshot(store, "other-collection", rows: [1], demand: otherCollection)
    #expect(try await claimedKeys(store, id("other-scope")) == [key(1)])
    #expect(try await claimedKeys(store, id("other-collection")) == [key(1)])
    #expect(try await store.staleMaterializations().isEmpty)

    try await snapshot(store, "deck", rows: [], at: 10)

    try await expectMarks(store, owners: ["push"], at: 10)
    #expect(try await claimedKeys(store, id("other-scope")) == [key(1)])
    #expect(try await claimedKeys(store, id("other-collection")) == [key(1)])
    #expect(try await storedKeys(store, otherScope) == [key(1)])
    #expect(try await storedKeys(store, otherCollection) == [key(1)])
  }

  public func dropReleasesOnlyItsOwnClaim() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    #expect(try await claimedKeys(store, id("deck")).isEmpty)
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    #expect(try await storedKeys(store, demand("deck")) == [key(1)])
  }

  public func lastClaimDropRemovesRowWithoutMark() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    #expect(try await storedKeys(store, demand("deck")).isEmpty)
    #expect(try await claimedKeys(store, id("deck")).isEmpty)
    #expect(try await store.staleMaterializations().isEmpty)
    try await snapshot(store, "deck", rows: [1], at: 20)
    try await delete(store, "deck", row: 1, at: 30)
    #expect(try await storedKeys(store, demand("deck")).isEmpty)
    #expect(try await claimedKeys(store, id("deck")).isEmpty)
    #expect(try await store.staleMaterializations().isEmpty)
  }

  public func threeOtherHoldersAreAllMarked() async throws {
    let store = try await makeStore()
    for owner in ["deck", "push", "chat", "discuss"] {
      try await snapshot(store, owner, rows: [1])
    }
    try await snapshot(store, "deck", rows: [], at: 10)
    try await expectMarks(store, owners: ["push", "chat", "discuss"], at: 10)
  }

  public func laterDropAdvancesMarkAndOlderClearLeavesIt() async throws {
    let store = try await makeStore()
    for owner in ["deck", "push", "chat"] {
      try await snapshot(store, owner, rows: [1])
    }
    try await snapshot(store, "deck", rows: [], at: 10)
    try await delete(store, "chat", row: 1, at: 20)
    try await store.clearStale(id("push"), ifMarkedAt: version(10))
    let marks = try await store.staleMaterializations()
    #expect(marks.first { $0.record.id == id("push") }?.markedAt == version(20))
    // Dropping its own row did not clear chat's existing mark.
    #expect(marks.first { $0.record.id == id("chat") }?.markedAt == version(10))
    try await store.clearStale(id("push"), ifMarkedAt: version(30))
    let afterNewerClear = try await store.staleMaterializations()
    #expect(afterNewerClear.first { $0.record.id == id("push") }?.markedAt == version(20))
  }

  public func currentPositionClearRemovesMark() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    try await expectMarks(store, owners: ["push"], at: 10)
    let record = try await store.materialization(for: demand("push"))
    try await store.clearStale(id("push"), ifMarkedAt: version(10))
    #expect(try await store.staleMaterializations().isEmpty)
    #expect(try await store.materialization(for: demand("push")) == record)
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    try await store.clearStale(id("push"), ifMarkedAt: version(10))
  }

  public func removingMaterializationDropsItsMark() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    try await expectMarks(store, owners: ["push"], at: 10)
    try await store.removeMaterialization(id("push"))
    #expect(try await store.staleMaterializations().isEmpty)
    #expect(try await store.materialization(for: demand("push")) == nil)
    #expect(try await claimedKeys(store, id("push")).isEmpty)
    #expect(try await storedKeys(store, demand("deck")).isEmpty)
    try await snapshot(store, "push", rows: [1], at: 20)
    #expect(try await store.staleMaterializations().isEmpty)
  }

  public func removingMaterializationDoesNotMarkOtherHolders() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    #expect(try await claimedKeys(store, id("deck")) == [key(1)])
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    #expect(try await store.staleMaterializations().isEmpty)

    try await store.removeMaterialization(id("deck"))

    #expect(try await store.staleMaterializations().isEmpty)
    #expect(try await store.materialization(for: demand("deck")) == nil)
    #expect(try await claimedKeys(store, id("deck")).isEmpty)
    #expect(try await claimedKeys(store, id("push")) == [key(1)])
    #expect(try await storedKeys(store, demand("push")) == [key(1)])
  }

  public func topNAndSubqueryHoldersAreMarked() async throws {
    let store = try await makeStore()
    let topN = CollectionDemand<Store.Model>(
      unsafePredicateIdentity: "top", order: [.init(unsafeFieldID: "id")], limit: 1
    ).identity(for: definition, scope: scope)
    let subquery = CollectionDemand<Store.Model>(
      unsafePredicateIdentity: "id IN (SELECT row_id FROM membership)"
    ).identity(for: definition, scope: scope)
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "top", rows: [1], demand: topN)
    try await snapshot(store, "subquery", rows: [1], demand: subquery)
    try await snapshot(store, "deck", rows: [], at: 10)
    try await expectMarks(store, owners: ["top", "subquery"], at: 10)
    // A limited or subquery dropping request uses the same rule, too.
    try await store.clearStale(id("subquery"), ifMarkedAt: version(10))
    try await snapshot(store, "top", rows: [], at: 20, demand: topN)
    let marks = try await store.staleMaterializations()
    #expect(marks.first { $0.record.id == id("subquery") }?.markedAt == version(20))
    try await snapshot(store, "deck", rows: [1], at: 20)
    try await snapshot(store, "subquery", rows: [], at: 30, demand: subquery)
    let finalMarks = try await store.staleMaterializations()
    #expect(finalMarks.first { $0.record.id == id("deck") }?.markedAt == version(30))
  }

  public func materializationLessClaimIsNotMarked() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await seedClaim(store, key(1), id("upgrade"), demand("upgrade"))
    #expect(try await store.materialization(for: demand("upgrade")) == nil)
    try await snapshot(store, "deck", rows: [], at: 10)
    #expect(try await store.staleMaterializations().isEmpty)
    #expect(try await claimedKeys(store, id("upgrade")) == [key(1)])
    #expect(try await storedKeys(store, demand("deck")) == [key(1)])
    try await snapshot(store, "deck", rows: [1], at: 20)
    try await delete(store, "deck", row: 1, at: 30)
    #expect(try await store.staleMaterializations().isEmpty)
    #expect(try await claimedKeys(store, id("upgrade")) == [key(1)])
    #expect(try await storedKeys(store, demand("deck")) == [key(1)])
  }

  public func staleMaterializationsReportsCurrentClaimCounts() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "chat", rows: [1, 2, 3])
    try await snapshot(store, "deck", rows: [], at: 10)
    let marks = try await store.staleMaterializations()
    #expect(marks.first { $0.record.id == id("push") }?.claimCount == 1)
    #expect(marks.first { $0.record.id == id("chat") }?.claimCount == 3)
    try await snapshot(store, "chat", rows: [], at: 20)
    let refreshed = try await store.staleMaterializations()
    let chat = try #require(refreshed.first { $0.record.id == id("chat") })
    #expect(chat.claimCount == 0)
    #expect(chat.markedAt == version(10))
    #expect(chat.record == (try await store.materialization(for: demand("chat"))))
  }

  public func unheldDeleteDoesNotMarkAndOlderDropDoesNotRegressMark() async throws {
    let store = try await makeStore()
    try await snapshot(store, "empty", rows: [])
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "chat", rows: [1])
    try await delete(store, "empty", row: 1, at: 10)
    #expect(try await store.staleMaterializations().isEmpty)
    try await snapshot(store, "deck", rows: [], at: 30)
    try await delete(store, "chat", row: 1, at: 20)
    try await expectMarks(store, owners: ["push", "chat"], at: 30)
  }

  public func refreshAndFeedKeepMarkUntilExplicitClear() async throws {
    let store = try await makeStore()
    try await snapshot(store, "deck", rows: [1])
    try await snapshot(store, "push", rows: [1])
    try await snapshot(store, "deck", rows: [], at: 10)
    try await snapshot(store, "push", rows: [1], at: 20)
    try await expectMarks(store, owners: ["push"], at: 10)
    try await store.apply(
      .init(
        changes: [.upsert(row(2), sourceVersion: version(30))], expectedCursor: nil,
        cursor: .init(offset: "30"), sourceVersion: version(30)), to: id("push"))
    try await expectMarks(store, owners: ["push"], at: 10)
  }

  private func id(_ owner: String) -> CollectionMaterializationID {
    .init(rawValue: owner)
  }

  private func key(_ number: Int) -> Store.Key { definition.key(row(number)) }

  private func demand(_ owner: String) -> CollectionDemandIdentity {
    CollectionDemand<Store.Model>(unsafePredicateIdentity: owner)
      .identity(for: definition, scope: scope)
  }

  private func version(_ order: UInt64) -> CollectionSourceVersion {
    .init(rawValue: "0/\(String(order, radix: 16).uppercased())", order: order)
  }

  private func snapshot(
    _ store: Store, _ owner: String, rows: [Int], at order: UInt64 = 1,
    demand identity: CollectionDemandIdentity? = nil
  ) async throws {
    try await store.replaceSnapshot(
      .init(
        rows: rows.map(row), fence: .init(rawValue: "snapshot-\(order)"),
        sourceVersion: version(order)),
      materializationID: id(owner), demand: identity ?? demand(owner))
  }

  private func delete(
    _ store: Store, _ owner: String, row number: Int, at order: UInt64,
    frontier: UInt64? = nil
  ) async throws {
    let record = try #require(await store.materialization(for: demand(owner)))
    try await store.apply(
      .init(
        changes: [.delete(key(number), sourceVersion: version(order))],
        expectedCursor: record.cursor, cursor: .init(offset: "delete-\(order)"),
        sourceVersion: version(frontier ?? order)), to: id(owner))
  }

  private func expectMarks(_ store: Store, owners: Set<String>, at order: UInt64) async throws {
    let marks: [CollectionStaleMaterialization] = try await store.staleMaterializations()
    #expect(marks.count == owners.count)
    #expect(Set(marks.map { $0.record.id.rawValue }) == owners)
    for mark in marks {
      #expect(mark.markedAt == version(order))
      #expect(mark.record == (try await store.materialization(for: mark.record.demand)))
    }
  }
}
