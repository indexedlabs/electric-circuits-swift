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
  private var stopFailures: Int
  private var stopCalls = 0
  private var heldRuns: Int
  private var runGate: CheckedContinuation<Void, Never>?
  private var runGateLanded = false
  private var runGateLandedWaiters: [CheckedContinuation<Void, Never>] = []
  private var heldMaterializations: Int
  private var materializationGate: CheckedContinuation<Void, Never>?
  private var materializationLanded = false
  private var materializationLandedWaiters: [CheckedContinuation<Void, Never>] = []
  private var heldStops: Int
  private var stopGate: CheckedContinuation<Void, Never>?
  private var stopGateLanded = false
  private var stopGateLandedWaiters: [CheckedContinuation<Void, Never>] = []
  private var startWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
  private var continuations:
    [CollectionMaterializationID: AsyncThrowingStream<
      CollectionChangeBatch<CoordinatorIssue, Int>, any Error
    >.Continuation] = [:]
  private(set) var starts: [CollectionDemandIdentity] = []
  private(set) var stopped: [CollectionMaterializationID] = []

  init(
    outcomes: [Outcome], stopFailures: Int = 0, heldRuns: Int = 0,
    heldMaterializations: Int = 0, heldStops: Int = 0
  ) {
    self.outcomes = outcomes
    self.stopFailures = stopFailures
    self.heldRuns = heldRuns
    self.heldMaterializations = heldMaterializations
    self.heldStops = heldStops
  }

  func materialize(
    _ demand: CollectionDemand<CoordinatorIssue>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<CoordinatorIssue, Int> {
    starts.append(identity)
    let satisfiedWaiters = startWaiters.filter { $0.count <= starts.count }
    startWaiters.removeAll { $0.count <= starts.count }
    for waiter in satisfiedWaiters { waiter.continuation.resume() }
    let outcome = outcomes.isEmpty ? .success([]) : outcomes.removeFirst()
    guard case .success(let rows) = outcome else { throw SourceFailure() }
    let updates = AsyncThrowingStream<CollectionChangeBatch<CoordinatorIssue, Int>, any Error>
      .makeStream()
    continuations[materializationID] = updates.continuation
    materializationLanded = true
    let landedWaiters = materializationLandedWaiters
    materializationLandedWaiters.removeAll()
    for waiter in landedWaiters { waiter.resume() }
    if heldMaterializations > 0 {
      heldMaterializations -= 1
      await withCheckedContinuation { materializationGate = $0 }
    }
    return CollectionSourceSession(
      snapshot: CollectionSnapshot(
        rows: rows,
        fence: SnapshotFence(rawValue: "fence-\(starts.count)"),
        cursor: StreamCursor(offset: "0")
      ),
      run: { apply in
        await self.waitForRunGateIfNeeded()
        for try await batch in updates.stream {
          try await apply(batch)
        }
      },
      stop: { try await self.stop(materializationID) }
    )
  }

  func stop(_ materializationID: CollectionMaterializationID) async throws {
    stopCalls += 1
    if heldStops > 0 {
      heldStops -= 1
      stopGateLanded = true
      let waiters = stopGateLandedWaiters
      stopGateLandedWaiters.removeAll()
      for waiter in waiters { waiter.resume() }
      await withCheckedContinuation { stopGate = $0 }
    }
    if stopFailures > 0 {
      stopFailures -= 1
      throw SourceFailure()
    }
    guard let continuation = continuations.removeValue(forKey: materializationID) else { return }
    continuation.finish()
    stopped.append(materializationID)
  }

  private func waitForRunGateIfNeeded() async {
    guard heldRuns > 0 else { return }
    heldRuns -= 1
    runGateLanded = true
    let waiters = runGateLandedWaiters
    runGateLandedWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
    await withCheckedContinuation { runGate = $0 }
  }

  func waitForRunGateLanding() async {
    guard !runGateLanded else { return }
    await withCheckedContinuation { runGateLandedWaiters.append($0) }
  }

  func releaseRunGate() {
    runGate?.resume()
    runGate = nil
  }

  func waitForMaterializationLanding() async {
    guard !materializationLanded else { return }
    await withCheckedContinuation { materializationLandedWaiters.append($0) }
  }

  func releaseMaterializationGate() {
    materializationGate?.resume()
    materializationGate = nil
  }

  func waitForStopGateLanding() async {
    guard !stopGateLanded else { return }
    await withCheckedContinuation { stopGateLandedWaiters.append($0) }
  }

  func releaseStopGate() {
    stopGate?.resume()
    stopGate = nil
  }

  func waitForStarts(_ count: Int) async {
    guard starts.count < count else { return }
    await withCheckedContinuation { startWaiters.append((count, $0)) }
  }

  func send(_ batch: CollectionChangeBatch<CoordinatorIssue, Int>) {
    continuations.values.first?.yield(batch)
  }

  func releaseAttempts() -> Int { stopCalls }

  struct SourceFailure: Error {}
}

private actor LiveApplyFailingStore: CollectionStore {
  typealias Model = CoordinatorIssue
  typealias Key = Int

  let base: InMemoryCollectionStore<CoordinatorIssue, Int>

  init(base: InMemoryCollectionStore<CoordinatorIssue, Int>) {
    self.base = base
  }

  func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?
  {
    try await base.materialization(for: demand)
  }

  func replaceSnapshot(
    _ snapshot: CollectionSnapshot<CoordinatorIssue>,
    materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    try await base.replaceSnapshot(snapshot, materializationID: materializationID, demand: demand)
  }

  func apply(
    _: CollectionChangeBatch<CoordinatorIssue, Int>, to _: CollectionMaterializationID
  ) async throws {
    throw ApplyFailure()
  }

  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    try await base.removeMaterialization(materializationID)
  }

  struct ApplyFailure: Error {}
}

private actor ReleaseCompletion {
  private var finished = false

  func markFinished() { finished = true }
  func hasFinished() -> Bool { finished }
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
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "status=open")

    let first = await coordinator.acquire(demand)
    let second = await coordinator.acquire(demand)

    #expect(await wait(for: .live, lease: first))
    #expect(await wait(for: .live, lease: second))
    #expect(await source.starts.count == 1)
    #expect(await store.rows() == [row.id: row])

    try await first.release()
    #expect(await source.stopped.isEmpty)
    #expect(await second.state() == .live)

    try await second.release()
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
      CollectionDemand(unsafePredicateIdentity: "window=september"))
    let october = await coordinator.acquire(
      CollectionDemand(unsafePredicateIdentity: "window=october"))

    #expect(await wait(for: .live, lease: september))
    #expect(await wait(for: .live, lease: october))
    #expect(await source.starts.count == 2)
    #expect(await store.rows() == [firstRow.id: firstRow, secondRow.id: secondRow])

    try await september.release()
    try await october.release()
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
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "status=open")
    let identity = demand.identity(for: coordinatorIssues, scope: coordinatorScope)

    let first = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: first))
    let committed = try #require(await store.materialization(for: identity))
    try await first.release()

    let second = await coordinator.acquire(demand)
    #expect(await wait(for: .failed(.sourceUnavailable), lease: second))
    #expect(await store.rows() == [row.id: row])
    #expect(try await store.materialization(for: identity) == committed)

    try await second.release()
  }

  @Test func awaitedLiveBatchMutatesTheStoreBeforeTheCoordinatorRemainsLive() async throws {
    let original = CoordinatorIssue(id: 1, title: "before")
    let updated = CoordinatorIssue(id: 1, title: "after")
    let source = ScriptedIssueSource(outcomes: [.success([original])])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "open")
    let lease = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: lease))
    await source.send(
      .init(
        changes: [.upsert(updated)], expectedCursor: .init(offset: "0"), cursor: .init(offset: "1"),
        sourceVersion: .init(rawValue: "v1", order: 1)))
    for _ in 0..<10_000 where await store.rows()[1] != updated { await Task.yield() }
    #expect(await store.rows()[1] == updated)
    #expect(await lease.state() == .live)
    try await lease.release()
  }

  @Test func failedReleaseRetainsLeaseAuthorityForAJoinedRetry() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([])], stopFailures: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    #expect(await wait(for: .live, lease: lease))
    await #expect(throws: ScriptedIssueSource.SourceFailure.self) { try await lease.release() }
    #expect(await source.releaseAttempts() == 1)
    try await lease.release()
    #expect(await source.releaseAttempts() == 2)
    #expect(await source.stopped.count == 1)
  }

  @Test func repeatedReleaseFailureRemainsObservableWithoutDiscardingAuthority() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([])], stopFailures: 2)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    #expect(await wait(for: .live, lease: lease))
    await #expect(throws: ScriptedIssueSource.SourceFailure.self) { try await lease.release() }
    await #expect(throws: ScriptedIssueSource.SourceFailure.self) { try await lease.release() }
    #expect(await source.releaseAttempts() == 2)
    #expect(await lease.state() == .failed(.sourceUnavailable))
  }

  @Test func releaseWaitsForCleanupWhenTheRemoteSessionLandsBeforeItReturns() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([])], heldMaterializations: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    await source.waitForMaterializationLanding()

    let completion = ReleaseCompletion()
    let release = Task {
      try await lease.release()
      await completion.markFinished()
    }
    for _ in 0..<1_000 { await Task.yield() }
    let completedBeforeCleanup = await completion.hasFinished()
    #expect(!completedBeforeCleanup)
    #expect(await source.stopped.isEmpty)

    await source.releaseMaterializationGate()
    try await release.value
    #expect(await completion.hasFinished())
    #expect(await source.stopped.count == 1)
    #expect(await lease.state() == .unavailable)
    #expect(await source.releaseAttempts() == 1)
  }

  @Test func liveStoreApplyFailureFailsClosedWithoutAdvancingTheCommittedCursor() async throws {
    let original = CoordinatorIssue(id: 1, title: "before")
    let attempted = CoordinatorIssue(id: 1, title: "after")
    let source = ScriptedIssueSource(outcomes: [.success([original])])
    let base = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let store = LiveApplyFailingStore(base: base)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "open")
    let identity = demand.identity(for: coordinatorIssues, scope: coordinatorScope)
    let lease = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: lease))
    let before = try #require(await base.materialization(for: identity))

    await source.send(
      .init(
        changes: [.upsert(attempted)], expectedCursor: .init(offset: "0"),
        cursor: .init(offset: "1"),
        sourceVersion: .init(rawValue: "v1", order: 1)))

    #expect(await wait(for: .failed(.storeUnavailable), lease: lease))
    #expect(await source.stopped.count == 1)
    #expect(await base.rows() == [original.id: original])
    #expect(try await base.materialization(for: identity) == before)

    try await lease.release()
    #expect(await source.releaseAttempts() == 1)
    #expect(await lease.state() == .unavailable)
  }

  @Test func refreshWaitsForCancellationResistantPriorRunBeforeReplacement() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([]), .success([])], heldRuns: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    #expect(await wait(for: .live, lease: lease))
    let refresh = Task { await lease.refresh() }
    for _ in 0..<1_000 { await Task.yield() }
    #expect(await source.starts.count == 1)
    await source.releaseRunGate()
    await refresh.value
    for _ in 0..<10_000 where await source.starts.count != 2 { await Task.yield() }
    #expect(await source.starts.count == 2)
    try await lease.release()
  }

  @Test func concurrentRefreshesJoinTheHeldPriorRunBeforeOneReplacementStarts() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([]), .success([])], heldRuns: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    #expect(await wait(for: .live, lease: lease))
    await source.waitForRunGateLanding()

    let firstRefresh = Task { await lease.refresh() }
    let secondRefresh = Task { await lease.refresh() }
    await Task.yield()
    #expect(await source.starts.count == 1)

    await source.releaseRunGate()
    await firstRefresh.value
    await secondRefresh.value
    await source.waitForStarts(2)
    #expect(await source.starts.count == 2)
    #expect(await wait(for: .live, lease: lease))
    try await lease.release()
  }

  @Test func acquireDuringHeldFinalReleaseRetainsTheNewLeaseAndRestartsIt() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([]), .success([])], heldStops: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "open")
    let first = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: first))

    let release = Task { try await first.release() }
    await source.waitForStopGateLanding()
    let second = await coordinator.acquire(demand)
    await source.releaseStopGate()
    try await release.value

    #expect(await wait(for: .live, lease: second))
    #expect(await source.starts.count == 2)
    try await second.release()
  }

  @Test func refreshStopFailureRetainsCleanupAuthorityForTheNextRefresh() async throws {
    let source = ScriptedIssueSource(outcomes: [.success([]), .success([])], stopFailures: 1)
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let lease = await coordinator.acquire(.init(unsafePredicateIdentity: "open"))
    #expect(await wait(for: .live, lease: lease))

    await lease.refresh()
    #expect(await lease.state() == .failed(.sourceUnavailable))
    #expect(await source.releaseAttempts() == 1)

    await lease.refresh()
    #expect(await source.releaseAttempts() == 2)
    #expect(await wait(for: .live, lease: lease))
    #expect(await source.starts.count == 2)
    try await lease.release()
  }

  @Test func evictionRejectsAnActiveDemandWithoutTouchingItsMaterialization() async throws {
    let row = CoordinatorIssue(id: 1, title: "cached")
    let source = ScriptedIssueSource(outcomes: [.success([row])])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "open")
    let lease = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: lease))

    await #expect(throws: CollectionEvictionError.activeDemand) {
      try await coordinator.evict(demand)
    }
    #expect(await store.rows() == [row.id: row])
    try await lease.release()
  }

  @Test func evictionRemovesAnInactiveCachedMaterializationAndItsUnclaimedRows() async throws {
    let row = CoordinatorIssue(id: 1, title: "cached")
    let source = ScriptedIssueSource(outcomes: [.success([row])])
    let store = InMemoryCollectionStore<CoordinatorIssue, Int>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: coordinatorIssues, scope: coordinatorScope, source: source, store: store)
    let demand = CollectionDemand<CoordinatorIssue>(unsafePredicateIdentity: "open")
    let identity = demand.identity(for: coordinatorIssues, scope: coordinatorScope)
    let lease = await coordinator.acquire(demand)
    #expect(await wait(for: .live, lease: lease))
    try await lease.release()

    try await coordinator.evict(demand)
    #expect(try await store.materialization(for: identity) == nil)
    #expect(await store.rows().isEmpty)
  }
}
