import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

private struct CoordinatorIssue: Equatable, Sendable {
  let id: Int
  let title: String
}

private actor ScriptedIssueSource: CollectionSourceAdapter {
  enum Outcome: Sendable {
    case success([CoordinatorIssue])
    case failure
  }

  private var outcomes: [Outcome]
  private var continuations:
    [CollectionMaterializationID: AsyncThrowingStream<
      CollectionChangeBatch<CoordinatorIssue, Int>, any Error
    >.Continuation] = [:]
  private(set) var starts: [CollectionDemandIdentity] = []
  private(set) var stopped: [CollectionMaterializationID] = []

  init(outcomes: [Outcome]) {
    self.outcomes = outcomes
  }

  func materialize(
    _ demand: CollectionDemand<CoordinatorIssue>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<CoordinatorIssue, Int> {
    starts.append(identity)
    let outcome = outcomes.isEmpty ? .success([]) : outcomes.removeFirst()
    guard case .success(let rows) = outcome else { throw SourceFailure() }
    let updates = AsyncThrowingStream<CollectionChangeBatch<CoordinatorIssue, Int>, any Error>
      .makeStream()
    continuations[materializationID] = updates.continuation
    return CollectionSourceSession(
      snapshot: CollectionSnapshot(
        rows: rows,
        fence: SnapshotFence(rawValue: "fence-\(starts.count)"),
        cursor: StreamCursor(offset: "0")
      ),
      run: { apply in
        for try await batch in updates.stream {
          try await apply(batch)
        }
      },
      stop: { await self.stop(materializationID) }
    )
  }

  func stop(_ materializationID: CollectionMaterializationID) {
    guard let continuation = continuations.removeValue(forKey: materializationID) else { return }
    continuation.finish()
    stopped.append(materializationID)
  }

  struct SourceFailure: Error {}
}

private let coordinatorIssues = CollectionDefinition<CoordinatorIssue, Int>(
  id: CollectionID(rawValue: "issues"),
  key: \.id
)

private let coordinatorScope = CollectionScope(
  principal: "user-1",
  authorization: "workspace-1",
  generation: "generation-1"
)

private func wait(
  for expected: CollectionLoadState,
  lease: CollectionLease,
  iterations: Int = 10_000
) async -> Bool {
  for _ in 0..<iterations {
    if await lease.state() == expected { return true }
    await Task.yield()
  }
  return false
}

@Suite("Collection coordinator contract")
struct CollectionCoordinatorContractTests {
  @Test func identicalConcurrentDemandsShareSourceUntilFinalLeaseReleases() async throws {
    let row = CoordinatorIssue(id: 1, title: "Shared")
    let source = ScriptedIssueSource(outcomes: [.success([row])])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues,
      scope: coordinatorScope,
      source: source,
      store: store
    )
    let demand = CollectionDemand<CoordinatorIssue>(predicateIdentity: "status=open")

    let first = await coordinator.acquire(demand)
    let second = await coordinator.acquire(demand)

    #expect(await wait(for: .live, lease: first))
    #expect(await wait(for: .live, lease: second))
    #expect(await source.starts.count == 1)
    #expect(await store.rows() == [row.id: row])

    await first.release()
    #expect(await source.stopped.isEmpty)
    #expect(await second.state() == .live)

    await second.release()
    #expect(await source.stopped.count == 1)
    #expect(await store.rows() == [row.id: row])
  }

  @Test func disjointDemandsUseDistinctMaterializationsAndShareCanonicalTable() async throws {
    let firstRow = CoordinatorIssue(id: 1, title: "September")
    let secondRow = CoordinatorIssue(id: 2, title: "October")
    let source = ScriptedIssueSource(outcomes: [.success([firstRow]), .success([secondRow])])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues,
      scope: coordinatorScope,
      source: source,
      store: store
    )

    let september = await coordinator.acquire(
      CollectionDemand(predicateIdentity: "window=september"))
    let october = await coordinator.acquire(CollectionDemand(predicateIdentity: "window=october"))

    #expect(await wait(for: .live, lease: september))
    #expect(await wait(for: .live, lease: october))
    #expect(await source.starts.count == 2)
    #expect(await store.rows() == [firstRow.id: firstRow, secondRow.id: secondRow])

    await september.release()
    await october.release()
  }

  @Test func failedCachedRefreshRetainsLastSuccessfulRowsAndCursor() async throws {
    let row = CoordinatorIssue(id: 1, title: "Cached")
    let source = ScriptedIssueSource(outcomes: [.success([row]), .failure])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues,
      scope: coordinatorScope,
      source: source,
      store: store
    )
    let demand = CollectionDemand<CoordinatorIssue>(predicateIdentity: "status=open")
    let identity = demand.identity(for: coordinatorIssues, scope: coordinatorScope)

    let first = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: first))
    let committed = try #require(await store.materialization(for: identity))
    await first.release()

    let second = await coordinator.acquire(demand)
    #expect(await wait(for: .failed(.sourceUnavailable), lease: second))
    #expect(await store.rows() == [row.id: row])
    #expect(try await store.materialization(for: identity) == committed)

    await second.release()
  }
}
