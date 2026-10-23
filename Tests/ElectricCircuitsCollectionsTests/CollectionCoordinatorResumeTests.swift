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
  }

  struct ResumeFailure: Error {}

  private let mode: Mode
  private let snapshotRows: [Int]
  private(set) var materializations = 0
  private(set) var resumes = 0
  private(set) var stops = 0
  private var sessions: [Int: AsyncStream<Void>.Continuation] = [:]
  private var nextSession = 0

  init(mode: Mode = .resume, snapshotRows: [Int] = []) {
    self.mode = mode
    self.snapshotRows = snapshotRows
  }

  func materialize(
    _: CollectionDemand<ResumeRow>, identity _: CollectionDemandIdentity,
    materializationID _: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<ResumeRow, Int> {
    materializations += 1
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
    case .resume, .transientTailFailure, .refusedCursor, .refusedCursorAfterCatchUp:
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
      run: { _, caughtUp in
        switch mode {
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

  private func openSession() -> (Int, AsyncStream<Void>) {
    nextSession += 1
    let stream = AsyncStream<Void>.makeStream()
    sessions[nextSession] = stream.continuation
    return (nextSession, stream.stream)
  }

  private func stop(_ index: Int) {
    guard let session = sessions.removeValue(forKey: index) else { return }
    stops += 1
    session.finish()
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
    await lease.refresh()
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

    await lease.refresh()
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
