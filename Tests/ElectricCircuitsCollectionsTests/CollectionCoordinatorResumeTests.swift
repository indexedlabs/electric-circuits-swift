import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

private struct ResumeRow: Equatable, Sendable {
  let id: Int
}

/// Snapshots `snapshotRows`, and answers `resume` per `mode`. Every session runs until it is
/// stopped; a resumed session reports its caught-up poll only when the test signals it.
private actor ResumingSource: CollectionSourceAdapter {
  typealias Model = ResumeRow
  typealias Key = Int

  enum Mode: Sendable {
    /// Continue from the record's stored cursor.
    case resume
    /// Return a session that claims to start somewhere the store does not hold.
    case wrongCursor
    case throwing
    /// Resume, then fail the tail as lost transport would.
    case transientTailFailure
    /// Resume, then refuse the stored cursor before any read, as a `410` would.
    case refusedCursor
    /// Resume, report one idle poll, then refuse the cursor.
    case refusedCursorAfterCatchUp
    /// Resume, commit one batch on the first signal and report caught up on the second.
    case appliesThenCatchesUp
  }

  struct ResumeFailure: Error {}

  private let mode: Mode
  private let snapshotRows: [Int]
  /// How many of the first `materialize` calls fail, as a source that is briefly unavailable.
  private var failingMaterializations: Int
  private(set) var materializations = 0
  private(set) var resumes = 0
  private(set) var stops = 0
  /// Batches a resumed session's `apply` has returned from.
  private(set) var appliedBatches = 0
  private var sessions: [Int: AsyncStream<Void>.Continuation] = [:]
  private var nextSession = 0
  private var holdsStops = false
  private var heldStops: [CheckedContinuation<Void, Never>] = []
  /// Stops that have been asked for and are held.
  private(set) var stopsWaiting = 0

  init(mode: Mode = .resume, snapshotRows: [Int] = [], failingMaterializations: Int = 0) {
    self.mode = mode
    self.snapshotRows = snapshotRows
    self.failingMaterializations = failingMaterializations
  }

  func materialize(
    _: CollectionDemand<ResumeRow>, identity _: CollectionDemandIdentity,
    materializationID _: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<ResumeRow, Int> {
    materializations += 1
    if failingMaterializations > 0 {
      failingMaterializations -= 1
      throw ResumeFailure()
    }
    let order = UInt64(200 + materializations)
    let (index, events) = openSession()
    return CollectionSourceSession(
      snapshot: CollectionSnapshot(
        rows: snapshotRows.map(ResumeRow.init(id:)),
        fence: SnapshotFence(rawValue: "snapshot-\(materializations)"),
        sourceVersion: CollectionSourceVersion(rawValue: "v\(order)", order: order),
        cursor: StreamCursor(offset: "snapshot-\(materializations)")),
      run: { _ in for await _ in events {} },
      stop: { await self.stop(index) })
  }

  func resume(
    _: CollectionDemand<ResumeRow>, identity _: CollectionDemandIdentity,
    materializationID _: CollectionMaterializationID, record: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<ResumeRow, Int>? {
    resumes += 1
    let cursor: StreamCursor
    switch mode {
    case .resume, .transientTailFailure, .refusedCursor, .refusedCursorAfterCatchUp,
      .appliesThenCatchesUp:
      guard let stored = record.cursor else { return nil }
      cursor = stored
    case .wrongCursor:
      cursor = StreamCursor(offset: "elsewhere")
    case .throwing:
      throw ResumeFailure()
    }
    let (index, events) = openSession()
    let mode = mode
    return CollectionResumeSession(
      cursor: cursor, sourceVersion: record.sourceVersion,
      run: { apply, caughtUp in
        switch mode {
        case .appliesThenCatchesUp:
          var applied = false
          for await _ in events {
            guard applied else {
              try await apply(
                CollectionChangeBatch(
                  changes: [.upsert(ResumeRow(id: 3), sourceVersion: version(11))],
                  expectedCursor: cursor, cursor: StreamCursor(offset: "a-11"),
                  sourceVersion: version(11)))
              applied = true
              await self.noteApplied()
              continue
            }
            await caughtUp()
          }
        case .transientTailFailure:
          throw ResumeFailure()
        case .refusedCursor:
          throw CollectionResumeInvalidated(cause: ResumeFailure())
        case .refusedCursorAfterCatchUp:
          for await _ in events {
            await caughtUp()
            throw CollectionResumeInvalidated(cause: ResumeFailure())
          }
        case .resume, .wrongCursor, .throwing:
          for await _ in events { await caughtUp() }
        }
      },
      stop: { await self.stop(index) })
  }

  /// Delivers one idle poll to every running session.
  func signalCaughtUp() {
    for session in sessions.values { session.yield(()) }
  }

  private func noteApplied() { appliedBatches += 1 }

  private func openSession() -> (Int, AsyncStream<Void>) {
    nextSession += 1
    let stream = AsyncStream<Void>.makeStream()
    sessions[nextSession] = stream.continuation
    return (nextSession, stream.stream)
  }

  /// Holds every later `stop` until `releaseStops()`, as a slow release request does.
  func holdStops() { holdsStops = true }

  func releaseStops() {
    holdsStops = false
    for continuation in heldStops { continuation.resume() }
    heldStops.removeAll()
  }

  private func stop(_ index: Int) async {
    if holdsStops {
      stopsWaiting += 1
      await withCheckedContinuation { heldStops.append($0) }
    }
    guard let session = sessions.removeValue(forKey: index) else { return }
    stops += 1
    session.finish()
  }
}

/// A store whose snapshot commits wait while it is held, so a test can place one against a
/// refresh that is replacing the attempt that read it.
private actor HeldSnapshotStore: CollectionStore {
  typealias Model = ResumeRow
  typealias Key = Int

  let base = InMemoryCollectionStore<ResumeRow, Int>(key: \.id)
  private var holding = false
  private var held: [CheckedContinuation<Void, Never>] = []
  /// Snapshots that were read and are waiting to commit.
  private(set) var waiting = 0

  func hold() { holding = true }

  func release() {
    holding = false
    for continuation in held { continuation.resume() }
    held.removeAll()
  }

  func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?
  {
    try await base.materialization(for: demand)
  }

  func staleMaterializations() async throws -> [CollectionStaleMaterialization] {
    try await base.staleMaterializations()
  }

  func clearStale(
    _ materializationID: CollectionMaterializationID, ifMarkedAt position: CollectionSourceVersion
  ) async throws {
    try await base.clearStale(materializationID, ifMarkedAt: position)
  }

  func replaceSnapshot(
    _ snapshot: CollectionSnapshot<ResumeRow>, materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    if holding {
      waiting += 1
      await withCheckedContinuation { held.append($0) }
    }
    try await base.replaceSnapshot(snapshot, materializationID: materializationID, demand: demand)
  }

  func apply(
    _ batch: CollectionChangeBatch<ResumeRow, Int>,
    to materializationID: CollectionMaterializationID
  ) async throws {
    try await base.apply(batch, to: materializationID)
  }

  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    try await base.removeMaterialization(materializationID)
  }

  func rebindMaterialization(
    _ materializationID: CollectionMaterializationID, to demand: CollectionDemandIdentity
  ) async throws -> Bool {
    try await base.rebindMaterialization(materializationID, to: demand)
  }
}

private let resumeScope = CollectionScope(principal: "u", authorization: "a", generation: "g")
private let bareRows = CollectionDefinition<ResumeRow, Int>(
  id: CollectionID(rawValue: "rows"), key: \.id)
private let demandA = CollectionDemand<ResumeRow>(unsafePredicateIdentity: "a")
private let demandB = CollectionDemand<ResumeRow>(unsafePredicateIdentity: "b")
private let identityA = demandA.identity(for: bareRows, scope: resumeScope)
private let identityB = demandB.identity(for: bareRows, scope: resumeScope)
private let materializationA = CollectionMaterializationID(rawValue: "materialization-a")
private let materializationB = CollectionMaterializationID(rawValue: "materialization-b")
private let rowDefinition = CollectionDefinition<ResumeRow, Int>(
  id: CollectionID(rawValue: "rows"), subscriptionKind: "push_read",
  rebuildDemand: { $0.canonicalDemand == identityA.canonicalDemand ? demandA : nil },
  key: \.id)

private func version(_ order: UInt64) -> CollectionSourceVersion {
  CollectionSourceVersion(rawValue: "v\(order)", order: order)
}

/// Stores demand A holding rows 1 and 2. With `markingA`, demand B then drops shared row 1,
/// which marks A stale at position 20.
private func seededStore(markingA: Bool = false) async throws
  -> InMemoryCollectionStore<ResumeRow, Int>
{
  let store = InMemoryCollectionStore<ResumeRow, Int>(key: \.id)
  try await store.replaceSnapshot(
    CollectionSnapshot(
      rows: [ResumeRow(id: 1), ResumeRow(id: 2)], fence: SnapshotFence(rawValue: "stored-a"),
      sourceVersion: version(10), cursor: StreamCursor(offset: "a-10")),
    materializationID: materializationA, demand: identityA)
  guard markingA else { return store }
  try await store.replaceSnapshot(
    CollectionSnapshot(
      rows: [ResumeRow(id: 1)], fence: SnapshotFence(rawValue: "stored-b"),
      sourceVersion: version(10), cursor: StreamCursor(offset: "b-10")),
    materializationID: materializationB, demand: identityB)
  try await store.replaceSnapshot(
    CollectionSnapshot(
      rows: [], fence: SnapshotFence(rawValue: "stored-b-2"), sourceVersion: version(20),
      cursor: StreamCursor(offset: "b-20")),
    materializationID: materializationB, demand: identityB)
  #expect(try await store.staleMaterializations().map(\.record.id) == [materializationA])
  return store
}

private func eventually(_ condition: @Sendable () async throws -> Bool) async -> Bool {
  for _ in 0..<2_500 {
    if (try? await condition()) == true { return true }
    try? await Task.sleep(for: .milliseconds(2))
  }
  return (try? await condition()) == true
}

@Suite("Collection coordinator resume")
struct CollectionCoordinatorResumeTests {
  @Test func cachedDemandResumesAndReportsLiveOnlyAfterTheTailCatchesUp() async throws {
    let store = try await seededStore()
    let stored = try #require(try await store.materialization(for: identityA))
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    for _ in 0..<100 { await Task.yield() }
    #expect(await lease.state() == .cached)

    await source.signalCaughtUp()
    #expect(await eventually { await lease.state() == .live })
    // No replaceSnapshot: the stored rows, claims and fence are the ones the tail continues.
    #expect(await source.materializations == 0)
    #expect(try await store.materialization(for: identityA) == stored)
    #expect(try await store.materialization(for: identityA)?.snapshotFence == stored.snapshotFence)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])

    try await lease.release()
    #expect(await source.stops == 1)
  }

  /// A batch the resumed tail commits does not report the demand live: the feed may hold more
  /// after it. Only the session's `caughtUp` does, once it has read to the feed's head.
  @Test func aResumedTailThatCommittedABatchStaysCachedUntilItCatchesUp() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .appliesThenCatchesUp, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    await source.signalCaughtUp()
    #expect(await eventually { await source.appliedBatches == 1 })
    #expect(await store.rowClaims(for: materializationA) == [1, 2, 3])
    #expect(try await store.materialization(for: identityA)?.cursor?.offset == "a-11")
    #expect(await lease.state() == .cached)

    await source.signalCaughtUp()
    #expect(await eventually { await lease.state() == .live })
    #expect(await source.materializations == 0)
    try await lease.release()
  }

  /// A later launch's demand finds the copy an earlier generation stored by its `cacheKey`. The
  /// store binds that copy to this generation without a snapshot, and the tail continues it.
  @Test func aCopyAnEarlierGenerationStoredResumesBoundToThisGeneration() async throws {
    let store = try await seededStore()
    let stored = try #require(try await store.materialization(for: identityA))
    let source = ResumingSource(snapshotRows: [9])
    let nextLaunch = CollectionScope(principal: "u", authorization: "a", generation: "g2")
    let nextIdentity = demandA.identity(for: rowDefinition, scope: nextLaunch)
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: nextLaunch, source: source, store: store)

    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    await source.signalCaughtUp()

    #expect(await eventually { await lease.state() == .live })
    #expect(await source.materializations == 0)
    let rebound = try #require(try await store.materialization(for: nextIdentity))
    #expect(rebound.id == materializationA)
    #expect(rebound.demand == nextIdentity)
    #expect(rebound.cursor == stored.cursor)
    #expect(rebound.snapshotFence == stored.snapshotFence)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])

    // The copy is this generation's now: its eviction removes it.
    try await lease.release()
    try await coordinator.evict(demandA)
    #expect(try await store.materialization(for: nextIdentity) == nil)
    #expect(await store.rows().isEmpty)
  }

  @Test func aResumeStartingAwayFromTheStoredCursorSnapshotsInstead() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .wrongCursor, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)

    #expect(await eventually { await lease.state() == .live })
    #expect(await source.resumes == 1)
    #expect(await source.stops == 1)
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    try await lease.release()
  }

  @Test func aThrowingResumeFailsTheAttemptAsASourceFailure() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .throwing, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)

    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await source.materializations == 0)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])

    // A resume the source could not set up says nothing about the stored cursor, so the retry
    // asks again rather than re-querying the page.
    await lease.refresh(.resume)
    #expect(await eventually { await source.resumes == 2 })
    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await source.materializations == 0)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])
    try await lease.release()
  }

  /// A cursor the feed refuses is an ordinary reseed: the same attempt stops the resumed session
  /// and snapshots, so the lease sees no failure and needs no refresh, whichever source instance
  /// a later attempt asks.
  @Test func aRefusedCursorSnapshotsInTheSameAttempt() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .refusedCursor, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)

    #expect(await eventually { await lease.state() == .live })
    #expect(await source.resumes == 1)
    #expect(await source.stops == 1)
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    try await lease.release()
  }

  /// A transient failure says nothing about the stored cursor: the attempt fails without a
  /// snapshot, and the retry resumes again rather than re-querying the page.
  @Test func aTransientResumedTailFailureResumesAgainOnRetry() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .transientTailFailure, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await source.materializations == 0)

    await lease.refresh(.resume)
    #expect(await eventually { await source.resumes == 2 })
    #expect(await source.materializations == 0)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])
    try await lease.release()
  }

  /// Once the resumed tail has been reported live, a refused cursor fails the attempt as a
  /// snapshot session's tail would.
  @Test func aCursorRefusedAfterTheTailWentLiveFailsTheAttempt() async throws {
    let store = try await seededStore()
    let source = ResumingSource(mode: .refusedCursorAfterCatchUp, snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    await source.signalCaughtUp()

    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await source.materializations == 0)
    try await lease.release()
  }

  /// Revalidation must replace the marked claims, so the stale pass snapshots a cached demand
  /// even when its source could resume it.
  @Test func stalePassSnapshotsAMarkedDemandInsteadOfResumingIt() async throws {
    let store = try await seededStore(markingA: true)
    let source = ResumingSource(snapshotRows: [2])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let pass = Task { await coordinator.startStaleRevalidation() }
    await coordinator.setStaleRevalidationGate(isOpen: true)

    #expect(await eventually { try await store.staleMaterializations().isEmpty })
    #expect(await source.resumes == 0)
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [2])

    pass.cancel()
    await pass.value
  }

  /// A screen whose attempt chose to resume before the stale pass joined it is restarted as a
  /// snapshot, so the pass still gets the fresh row set it revalidates against.
  @Test func stalePassJoiningAResumedScreenRestartsItAsASnapshot() async throws {
    let store = try await seededStore(markingA: true)
    let source = ResumingSource(snapshotRows: [2])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let screen = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    #expect(await screen.state() == .cached)

    let pass = Task { await coordinator.startStaleRevalidation() }
    await coordinator.setStaleRevalidationGate(isOpen: true)

    #expect(await eventually { try await store.staleMaterializations().isEmpty })
    #expect(await source.resumes == 1)
    #expect(await source.materializations == 1)
    #expect(await source.stops == 1)
    #expect(await screen.state() == .live)
    #expect(await store.rowClaims(for: materializationA) == [2])

    try await screen.release()
    pass.cancel()
    await pass.value
  }

  /// Without a way to rebuild the marked demand, the stale pass removes an unheld copy. A resumed
  /// screen still settling on that copy holds it: the pass joins and snapshots it instead.
  @Test func stalePassKeepsTheCopyAResumedScreenIsSettlingOn() async throws {
    let store = try await seededStore(markingA: true)
    let source = ResumingSource(snapshotRows: [2])
    let coordinator = CollectionCoordinator(
      definition: bareRows, scope: resumeScope, source: source, store: store)
    let screen = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    #expect(await screen.state() == .cached)

    let pass = Task { await coordinator.startStaleRevalidation() }
    await coordinator.setStaleRevalidationGate(isOpen: true)

    #expect(await eventually { try await store.staleMaterializations().isEmpty })
    #expect(await source.materializations == 1)
    #expect(await screen.state() == .live)
    #expect(try await store.materialization(for: identityA)?.id == materializationA)
    #expect(await store.rowClaims(for: materializationA) == [2])

    try await screen.release()
    pass.cancel()
    await pass.value
  }

  /// A screen that resumed and already reports live never re-read the source, so the stale pass
  /// still revalidates it through a snapshot before it clears the mark.
  @Test func stalePassSnapshotsAResumedScreenThatIsAlreadyLive() async throws {
    let store = try await seededStore(markingA: true)
    let source = ResumingSource(snapshotRows: [2])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let screen = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    await source.signalCaughtUp()
    #expect(await eventually { await screen.state() == .live })
    #expect(await source.materializations == 0)

    let pass = Task { await coordinator.startStaleRevalidation() }
    await coordinator.setStaleRevalidationGate(isOpen: true)

    #expect(await eventually { try await store.staleMaterializations().isEmpty })
    #expect(await source.resumes == 1)
    #expect(await source.materializations == 1)
    #expect(await screen.state() == .live)
    #expect(await store.rowClaims(for: materializationA) == [2])

    try await screen.release()
    pass.cancel()
    await pass.value
  }
}

/// The caller decides, per acquire and per refresh, whether a stored copy may continue from its
/// cursor (`.resume`) or must be read again (`.snapshot`).
@Suite("Collection coordinator load mode")
struct CollectionCoordinatorLoadModeTests {
  @Test func aPlainRefreshReadsTheSourceAgainWhileExplicitResumeContinues() async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    await source.signalCaughtUp()
    #expect(await eventually { await lease.state() == .live })

    // The no-argument refresh is the public re-read behavior. Explicit resume is available for
    // retrying an attempt whose cursor is still trustworthy.
    await lease.refresh()
    #expect(await eventually { await source.materializations == 1 })
    #expect(await source.resumes == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    await lease.refresh(.resume)
    #expect(await eventually { await source.resumes == 2 })
    await source.signalCaughtUp()
    #expect(await eventually { await lease.state() == .live })
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    #expect(await store.rowClaims(for: materializationA) == [9])
    #expect(try await store.materialization(for: identityA)?.snapshotFence.rawValue == "snapshot-1")
    try await lease.release()
  }

  @Test func aSnapshotAcquireLoadsAStoredCopyWithoutAskingTheSourceToResume() async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)

    let lease = await coordinator.acquire(demandA, mode: .snapshot)

    #expect(await eventually { await lease.state() == .live })
    #expect(await source.resumes == 0)
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    try await lease.release()

    // The same acquire with `.resume`, as without a mode, continues what that snapshot stored.
    let resumed = await coordinator.acquire(demandA, mode: .resume)
    #expect(await eventually { await source.resumes == 1 })
    #expect(await source.materializations == 1)
    try await resumed.release()
  }

  /// A snapshot the caller asked for is owed until one commits: the attempt that fails first is
  /// followed by a snapshot even when the retry says resume is allowed. After it, resume is
  /// allowed again.
  @Test func aSnapshotStaysOwedUntilOneCommits() async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9], failingMaterializations: 1)
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let lease = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })

    await lease.refresh(.snapshot)
    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await source.materializations == 1)
    #expect(await store.rowClaims(for: materializationA) == [1, 2])

    await lease.refresh()
    #expect(await eventually { await lease.state() == .live })
    #expect(await source.materializations == 2)
    #expect(await source.resumes == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])

    await lease.refresh(.resume)
    #expect(await eventually { await source.resumes == 2 })
    #expect(await source.materializations == 2)
    try await lease.release()
  }

  /// A snapshot refresh is answered by an attempt that starts after it. The attempt it replaces
  /// can still commit a snapshot it read before the refresh was asked for; that snapshot does
  /// not settle the request, and the replacement does not resume from it.
  @Test func aSnapshotRefreshIsNotSettledByASnapshotReadBeforeIt() async throws {
    let store = HeldSnapshotStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    await store.hold()
    let lease = await coordinator.acquire(demandA)
    // The first attempt has read its snapshot and waits to commit it.
    #expect(await eventually { await store.waiting == 1 })

    await source.holdStops()
    let refresh = Task { await lease.refresh(.snapshot) }
    // The refresh is asked for, and is stopping the first attempt's session.
    #expect(await eventually { await source.stopsWaiting == 1 })
    await store.release()
    #expect(await eventually { await lease.state() == .live })
    #expect(try await store.materialization(for: identityA)?.snapshotFence.rawValue == "snapshot-1")

    await source.releaseStops()
    await refresh.value
    #expect(await eventually { await source.materializations == 2 })
    #expect(await eventually { await lease.state() == .live })
    #expect(await source.resumes == 0)
    #expect(try await store.materialization(for: identityA)?.snapshotFence.rawValue == "snapshot-2")
    try await lease.release()
  }

  /// A lease that asks for a snapshot and joins a demand whose attempt resumed replaces that
  /// attempt, whether or not it had reported live: a resumed tail never read the source.
  @Test(arguments: [false, true])
  func aSnapshotAcquireReplacesAResumedAttemptItJoins(alreadyLive: Bool) async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let screen = await coordinator.acquire(demandA)
    #expect(await eventually { await source.resumes == 1 })
    if alreadyLive {
      await source.signalCaughtUp()
      #expect(await eventually { await screen.state() == .live })
    }

    let second = await coordinator.acquire(demandA, mode: .snapshot)

    #expect(await eventually { await source.materializations == 1 })
    #expect(await eventually { await second.state() == .live })
    #expect(await screen.state() == .live)
    #expect(await source.resumes == 1)
    #expect(await source.stops == 1)
    #expect(await store.rowClaims(for: materializationA) == [9])
    try await screen.release()
    try await second.release()
  }

  /// A lease that joins while the demand's last lease is being released is served by the
  /// attempt that starts after the release, not by the one on its way out. Asked for a snapshot,
  /// that attempt takes one, though the store now holds a copy it could resume.
  @Test func aSnapshotAcquireDuringAReleaseSnapshotsInTheAttemptThatFollows() async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let first = await coordinator.acquire(demandA, mode: .snapshot)
    #expect(await eventually { await first.state() == .live })

    await source.holdStops()
    let releasing = Task { try await first.release() }
    #expect(await eventually { await source.stopsWaiting == 1 })
    let second = await coordinator.acquire(demandA, mode: .snapshot)
    await source.releaseStops()
    try await releasing.value

    #expect(await eventually { await source.materializations == 2 })
    #expect(await eventually { await second.state() == .live })
    #expect(await source.resumes == 0)
    try await second.release()
  }

  /// An attempt that took its own snapshot already read the source, so a later lease that asks
  /// for a snapshot shares it. `refresh(.snapshot)` is the way to read again regardless.
  @Test func aSnapshotAcquireSharesAnAttemptThatSnapshotted() async throws {
    let store = try await seededStore()
    let source = ResumingSource(snapshotRows: [9])
    let coordinator = CollectionCoordinator(
      definition: rowDefinition, scope: resumeScope, source: source, store: store)
    let first = await coordinator.acquire(demandA, mode: .snapshot)
    #expect(await eventually { await first.state() == .live })

    let second = await coordinator.acquire(demandA, mode: .snapshot)
    // A replaced attempt would report `.refreshing` from the acquire on.
    #expect(await second.state() == .live)
    #expect(await source.materializations == 1)
    #expect(await source.stops == 0)

    await second.refresh(.snapshot)
    #expect(await eventually { await source.materializations == 2 })
    #expect(await eventually { await first.state() == .live })
    #expect(await source.resumes == 0)
    try await first.release()
    try await second.release()
  }
}
