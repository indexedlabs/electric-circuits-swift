import ElectricCircuitsCollectionsTesting
import Testing

@testable import ElectricCircuitsCollections

private struct StaleMarkRow: Sendable {
  let id: Int
}

@Suite("Shared collection store stale marks")
struct CollectionStoreStaleMarkContractTests {
  private let contract = CollectionStoreStaleMarkTests(
    makeStore: { InMemoryCollectionStore<StaleMarkRow, Int>(key: \.id) },
    definition: .init(id: .init(rawValue: "issues"), key: \.id),
    scope: .init(principal: "user", authorization: "workspace", generation: "1"),
    row: { StaleMarkRow(id: $0) },
    storedKeys: { store, demand in Set(await store.rows(for: demand).keys) },
    claimedKeys: { store, owner in await store.rowClaims(for: owner) },
    seedClaimWithoutMaterialization: { store, key, owner, demand in
      await store.seedClaimWithoutMaterialization(key, owner: owner, demand: demand)
    }
  )

  @Test func reloadOmissionMarksOtherHolders() async throws {
    try await contract.reloadOmissionMarksOtherHolders()
  }

  @Test func feedDeleteMarksOtherHolders() async throws {
    try await contract.feedDeleteMarksOtherHolders()
  }

  @Test func dropReleasesOnlyItsOwnClaim() async throws {
    try await contract.dropReleasesOnlyItsOwnClaim()
  }

  @Test func lastClaimDropRemovesRowWithoutMark() async throws {
    try await contract.lastClaimDropRemovesRowWithoutMark()
  }

  @Test func threeOtherHoldersAreAllMarked() async throws {
    try await contract.threeOtherHoldersAreAllMarked()
  }

  @Test func laterDropAdvancesMarkAndOlderClearLeavesIt() async throws {
    try await contract.laterDropAdvancesMarkAndOlderClearLeavesIt()
  }

  @Test func currentPositionClearRemovesMark() async throws {
    try await contract.currentPositionClearRemovesMark()
  }

  @Test func removingMaterializationDropsItsMark() async throws {
    try await contract.removingMaterializationDropsItsMark()
  }

  @Test func topNAndSubqueryHoldersAreMarked() async throws {
    try await contract.topNAndSubqueryHoldersAreMarked()
  }

  @Test func materializationLessClaimIsNotMarked() async throws {
    try await contract.materializationLessClaimIsNotMarked()
  }

  @Test func staleMaterializationsReportsCurrentClaimCounts() async throws {
    try await contract.staleMaterializationsReportsCurrentClaimCounts()
  }

  @Test func unheldDeleteDoesNotMarkAndOlderDropDoesNotRegressMark() async throws {
    try await contract.unheldDeleteDoesNotMarkAndOlderDropDoesNotRegressMark()
  }

  @Test func refreshAndFeedKeepMarkUntilExplicitClear() async throws {
    try await contract.refreshAndFeedKeepMarkUntilExplicitClear()
  }
}
