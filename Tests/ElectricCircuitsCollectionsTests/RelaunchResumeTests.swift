import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

/// A store that implements only the original `CollectionStore` requirements, as a provider
/// written before `rebindMaterialization` existed does.
private actor SnapshotBindingStore: CollectionStore {
  typealias Model = ContractItem
  typealias Key = Int64

  let base = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)

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
    _ snapshot: CollectionSnapshot<ContractItem>, materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    try await base.replaceSnapshot(snapshot, materializationID: materializationID, demand: demand)
  }

  func apply(
    _ batch: CollectionChangeBatch<ContractItem, Int64>,
    to materializationID: CollectionMaterializationID
  ) async throws {
    try await base.apply(batch, to: materializationID)
  }

  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    try await base.removeMaterialization(materializationID)
  }
}

private actor StateLog {
  private(set) var states: [CollectionLoadState] = []
  func append(_ state: CollectionLoadState) { states.append(state) }
}

/// Commits the first live batch and holds every later one until `release()`, so a test can look
/// at a demand between two batches of one read-up.
private actor SecondBatchGate: CollectionStore {
  typealias Model = ContractItem
  typealias Key = Int64

  let base = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
  /// Live batches that reached `apply`, held or not.
  private(set) var batches = 0
  private var open = false
  private var held: [CheckedContinuation<Void, Never>] = []

  func release() {
    open = true
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
    _ snapshot: CollectionSnapshot<ContractItem>, materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    try await base.replaceSnapshot(snapshot, materializationID: materializationID, demand: demand)
  }

  func apply(
    _ batch: CollectionChangeBatch<ContractItem, Int64>,
    to materializationID: CollectionMaterializationID
  ) async throws {
    batches += 1
    if batches > 1, !open { await withCheckedContinuation { held.append($0) } }
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

/// A later launch is a new process: a new generation, a new source and coordinator, and the
/// store the earlier launch left. The engine keeps a released changes-only feed's stream, so the
/// later launch's create for the same request names the same stream, and the stored cursor is
/// still a position in it.
@Suite("Resume across launches")
struct RelaunchResumeTests {
  /// Launch 1 loads the scope by snapshot, goes live and releases, as a terminating app does.
  private func firstLaunch<Store: CollectionStore<ContractItem, Int64>>(
    _ engine: ContractEngine, store: Store
  ) async throws -> CollectionMaterializationRecord {
    let coordinator = CollectionCoordinator(
      definition: contractItems, scope: contractScope("launch-1"),
      source: contractSource(engine), store: store)
    let lease = await coordinator.acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })
    // The tail is reading at the snapshot's frontier before the launch ends.
    #expect(await contractEventually { await engine.count(.streamRead) == 1 })
    try await lease.release()
    #expect(
      await engine.kinds() == [.feedCreate, .frontier, .snapshotQuery, .streamRead, .release])
    return try #require(try await store.materialization(for: contractIdentity("launch-1")))
  }

  @Test func aLaterLaunchResumesTheStoredCopyFromItsCursorWithoutASnapshot() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let stored = try await firstLaunch(engine, store: store)
    #expect(stored.cursor?.offset == ContractEngine.start)
    // The engine keeps appending to the feed's stream while no launch is running.
    let next = await engine.append("s1", fixture.plain.body)
    let before = await engine.requests.count

    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await contractEventually { await lease.state() == .live })
    #expect(
      await contractEventually {
        try await store.materialization(for: contractIdentity("launch-2"))?.cursor?.offset == next
      })
    #expect(
      await store.rows() == [
        1: ContractItem(id: 1, title: "one-b"), 2: ContractItem(id: 2, title: "two"),
      ])
    // One feed create, then the stream from the stored offset: no frontier read, no snapshot.
    #expect(Array(await engine.kinds(from: before).prefix(2)) == [.feedCreate, .streamRead])
    #expect(
      await engine.requests(of: .streamRead).dropFirst().first?.offset == ContractEngine.start)
    #expect(await engine.count(.snapshotQuery) == 1)
    #expect(await engine.count(.frontier) == 1)
    // Each launch claims the feed under its own subscription, and the engine answers both with
    // the one stream.
    let creates = await engine.requests(of: .feedCreate)
    #expect(creates.count == 2)
    #expect(creates.first?.subscription != creates.last?.subscription)
    #expect(creates.allSatisfy { $0.subscription?.hasPrefix(stored.id.rawValue + ".") == true })
    // The same copy, with the fence its snapshot wrote, now bound to this launch's demand.
    let record = try #require(try await store.materialization(for: contractIdentity("launch-2")))
    #expect(record.id == stored.id)
    #expect(record.snapshotFence == stored.snapshotFence)
    #expect(record.demand == contractIdentity("launch-2"))
    #expect(await store.rowClaims(for: stored.id) == [1, 2])
    try await lease.release()
  }

  /// With nothing new on the stream, the stored rows are current as soon as the stream says the
  /// stored offset is its head. A long-poll at the head is held for the poll window (about 30 s
  /// on the stream server; this engine never answers one unasked), so a resumed tail first asks
  /// with a read the server answers at once. Over SSE the stream says so when the read opens.
  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func aLaterLaunchWithNothingNewGoesLiveWithoutWaitingOutAPoll(mode: LiveReadMode) async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let stored = try await firstLaunch(engine, store: store)

    let lease = await contractLaunch(
      engine, store: store, generation: "launch-2", liveReadMode: mode
    ).acquire(listOneItems)

    #expect(await contractEventually { await lease.state() == .live })
    #expect(await engine.count(.snapshotQuery) == 1)
    #expect(await engine.count(.frontier) == 1)
    #expect(await engine.count(.feedCreate) == 2)
    let record = try #require(try await store.materialization(for: contractIdentity("launch-2")))
    #expect(record.cursor == stored.cursor)
    #expect(record.snapshotFence == stored.snapshotFence)
    if mode == .longPoll {
      // The catch-up read, then the live tail from the same offset.
      #expect(await contractEventually { await engine.count(.streamRead) == 3 })
      let resumed = await engine.requests(of: .streamRead).dropFirst()
      #expect(resumed.map(\.live) == [nil, "long-poll"])
      #expect(resumed.map(\.offset) == [ContractEngine.start, ContractEngine.start])
    }
    try await lease.release()
  }

  /// A resumed tail that is two reads behind is not current after the first: the demand stays
  /// cached, with its first batch committed, until the batch that ends at the stream's head is
  /// committed too.
  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func aLaterLaunchReportsLiveOnlyAfterEveryChangeUpToTheHead(mode: LiveReadMode) async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = SecondBatchGate()
    _ = try await firstLaunch(engine, store: store)
    await engine.append("s1", EngineEnvelopeFixture.body([fixture.plain.envelopes[0]]))
    let head = await engine.append(
      "s1", EngineEnvelopeFixture.body(Array(fixture.plain.envelopes.dropFirst())))

    let coordinator = CollectionCoordinator(
      definition: contractItems, scope: contractScope("launch-2"),
      source: contractSource(engine, liveReadMode: mode), store: store)
    let lease = await coordinator.acquire(listOneItems)

    // The first batch is committed and the second, which the stream already holds, is not.
    #expect(await contractEventually { await store.batches == 2 })
    #expect(await store.base.rows()[1] == ContractItem(id: 1, title: "one-b"))
    #expect(await lease.state() == .cached)

    await store.release()
    #expect(await contractEventually { await lease.state() == .live })
    #expect(
      try await store.materialization(for: contractIdentity("launch-2"))?.cursor?.offset == head)
    #expect(await engine.count(.snapshotQuery) == 1)
    try await lease.release()
  }

  /// The one case that needs a snapshot: the engine retired the feed, so the create mints a new
  /// stream that holds nothing of the gap since the stored cursor.
  @Test func aLaterLaunchSnapshotsWhenTheEngineRetiredTheFeed() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot, laterContractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let stored = try await firstLaunch(engine, store: store)
    await engine.retireFeed()
    let before = await engine.requests.count

    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await contractEventually { await lease.state() == .live })
    // The snapshot uses the create that discovered the new stream; nothing is created twice.
    // (A snapshot is live when it commits, before its tail has sent its first read.)
    #expect(await contractEventually { await engine.kinds(from: before).count >= 4 })
    #expect(
      Array(await engine.kinds(from: before).prefix(4))
        == [.feedCreate, .frontier, .snapshotQuery, .streamRead])
    #expect(await engine.requests(of: .streamRead).last?.stream == "s2")
    #expect(await store.rows()[1] == ContractItem(id: 1, title: "one-b"))
    let record = try #require(try await store.materialization(for: contractIdentity("launch-2")))
    #expect(record.id == stored.id)
    #expect(record.snapshotFence != stored.snapshotFence)
    #expect(record.snapshotFence.rawValue.contains(ContractEngine.streamURL("s2")))
    try await lease.release()
  }

  /// A batch the source refuses, waiting at the stored cursor, is found by the resumed read
  /// before anything is reported live, so the same attempt snapshots and no lease sees a failure.
  @Test func aLaterLaunchThatResumesIntoARefusedEnvelopeSnapshotsInTheSameAttempt() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot, laterContractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    _ = try await firstLaunch(engine, store: store)
    let afterRefused = await engine.append("s1", fixture.subquery.body)
    let before = await engine.requests.count

    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)
    let log = StateLog()
    let observing = Task { for await state in lease.stateUpdates { await log.append(state) } }

    #expect(await contractEventually { await lease.state() == .live })
    // The snapshot is live when it commits; its tail's first read follows.
    #expect(await contractEventually { await engine.kinds(from: before).count >= 7 })
    #expect(
      Array(await engine.kinds(from: before).prefix(7))
        == [
          .feedCreate, .streamRead, .release, .feedCreate, .frontier, .snapshotQuery, .streamRead,
        ])
    #expect(
      await engine.requests(of: .streamRead).suffix(2).map(\.offset)
        == [ContractEngine.start, afterRefused])
    #expect(await store.rows()[1] == ContractItem(id: 1, title: "one-b"))
    try await lease.release()
    await observing.value
    #expect(await log.states.contains(.failed(.sourceUnavailable)) == false)
  }

  /// A provider that cannot bind a copy to a new generation without a snapshot keeps the
  /// behaviour it had: a later launch snapshots, with no extra request.
  @Test func aStoreThatCannotRebindSnapshotsTheCopyAnEarlierLaunchStored() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot, laterContractSnapshot])
    let store = SnapshotBindingStore()
    let stored = try await firstLaunch(engine, store: store)
    let before = await engine.requests.count

    let coordinator = CollectionCoordinator(
      definition: contractItems, scope: contractScope("launch-2"),
      source: contractSource(engine), store: store)
    let lease = await coordinator.acquire(listOneItems)

    #expect(await contractEventually { await lease.state() == .live })
    #expect(await contractEventually { await engine.kinds(from: before).count >= 4 })
    #expect(
      Array(await engine.kinds(from: before).prefix(4))
        == [.feedCreate, .frontier, .snapshotQuery, .streamRead])
    let record = try #require(try await store.materialization(for: contractIdentity("launch-2")))
    #expect(record.id == stored.id)
    #expect(record.demand == contractIdentity("launch-2"))
    try await lease.release()
  }
}

/// `rebindMaterialization` is how a later generation takes over a copy without a snapshot.
@Suite("Collection store rebinding")
struct CollectionStoreRebindTests {
  private let earlier = contractIdentity("launch-1")
  private let later = contractIdentity("launch-2")
  private let copy = CollectionMaterializationID(rawValue: "copy-1")

  private func storedCopy() async throws -> InMemoryCollectionStore<ContractItem, Int64> {
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    try await store.replaceSnapshot(
      CollectionSnapshot(
        rows: [ContractItem(id: 1, title: "one"), ContractItem(id: 2, title: "two")],
        fence: SnapshotFence(rawValue: "fence-1"),
        sourceVersion: CollectionSourceVersion(rawValue: "0/10", order: 0x10),
        cursor: StreamCursor(offset: "10", lsn: "0/10")),
      materializationID: copy, demand: earlier)
    return store
  }

  @Test func rebindingChangesOnlyTheDemandTheCopyIsBoundTo() async throws {
    let store = try await storedCopy()
    let before = try #require(try await store.materialization(for: earlier))

    #expect(try await store.rebindMaterialization(copy, to: later))

    let after = try #require(try await store.materialization(for: later))
    #expect(after.demand == later)
    #expect(after.id == before.id)
    #expect(after.snapshotFence == before.snapshotFence)
    #expect(after.cursor == before.cursor)
    #expect(after.sourceVersion.rawValue == before.sourceVersion.rawValue)
    #expect(await store.rowClaims(for: copy) == [1, 2])
    #expect(await store.rows().count == 2)
    // The tail continues the rebound copy from the cursor it kept.
    try await store.apply(
      CollectionChangeBatch(
        changes: [
          .delete(2, sourceVersion: CollectionSourceVersion(rawValue: "0/20", order: 0x20))
        ],
        expectedCursor: before.cursor, cursor: StreamCursor(offset: "11", lsn: "0/20"),
        sourceVersion: CollectionSourceVersion(rawValue: "0/20", order: 0x20)),
      to: copy)
    #expect(await store.rowClaims(for: copy) == [1])
    #expect(try await store.materialization(for: later)?.demand == later)
  }

  @Test func rebindingRefusesACopyTheKeyDoesNotHold() async throws {
    let store = try await storedCopy()
    let otherDemand = CollectionDemand<ContractItem>(unsafePredicateIdentity: "list_id=2")
      .identity(for: contractItems, scope: contractScope("launch-2"))

    #expect(
      try await store.rebindMaterialization(
        CollectionMaterializationID(rawValue: "copy-2"), to: later) == false)
    #expect(try await store.rebindMaterialization(copy, to: otherDemand) == false)
    #expect(try await store.materialization(for: earlier)?.demand == earlier)
  }
}
