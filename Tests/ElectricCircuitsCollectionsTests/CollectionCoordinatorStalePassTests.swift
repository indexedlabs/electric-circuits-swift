import ElectricCircuitsSwift
import Foundation
import Testing

@testable import ElectricCircuitsCollections

private struct StaleRow: Equatable, Sendable {
  let id: Int
}

/// Every request waits for the test to supply its snapshot. A completed snapshot keeps its
/// feed alive until the coordinator releases it, so concurrency includes snapshot and release.
private actor StaleSource: CollectionSourceAdapter {
  struct Request: Sendable {
    let identity: CollectionDemandIdentity
    let demand: CollectionDemand<StaleRow>
    let materializationID: CollectionMaterializationID
  }

  private(set) var requests: [Request] = []
  private(set) var stopped: [Int] = []
  private(set) var cancelled: [Int] = []
  private(set) var maximumActive = 0
  private var active: Set<Int> = []
  private var holdsCancellation = false
  private var holdsStops = false
  private var stopWaiters: [CheckedContinuation<Void, Never>] = []
  private var stopFailures = 0
  private(set) var stopAttempts: [Int] = []

  func holdStops() { holdsStops = true }
  func failStops(_ count: Int) { stopFailures = count }
  func resumeStops() {
    holdsStops = false
    let waiting = stopWaiters
    stopWaiters.removeAll()
    for continuation in waiting { continuation.resume() }
  }
  private var cancelledSnapshots: [CheckedContinuation<CollectionSnapshot<StaleRow>, any Error>] =
    []

  func holdCancellation() { holdsCancellation = true }

  func resumeCancellation() {
    holdsCancellation = false
    let waiting = cancelledSnapshots
    cancelledSnapshots.removeAll()
    for continuation in waiting { continuation.resume(throwing: CancellationError()) }
  }
  private var pending: [Int: CheckedContinuation<CollectionSnapshot<StaleRow>, any Error>] = [:]
  private var feeds:
    [Int: AsyncThrowingStream<CollectionChangeBatch<StaleRow, Int>, any Error>.Continuation] = [:]

  func materialize(
    _ demand: CollectionDemand<StaleRow>, identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<StaleRow, Int> {
    let index = requests.count
    requests.append(.init(identity: identity, demand: demand, materializationID: materializationID))
    active.insert(index)
    maximumActive = max(maximumActive, active.count)
    let snapshot = try await withTaskCancellationHandler {
      try Task.checkCancellation()
      return try await withCheckedThrowingContinuation {
        pending[index] = $0
      }
    } onCancel: {
      Task { await self.cancel(index) }
    }
    try Task.checkCancellation()
    let stream = AsyncThrowingStream<CollectionChangeBatch<StaleRow, Int>, any Error>.makeStream()
    feeds[index] = stream.continuation
    return .init(
      snapshot: snapshot,
      run: { apply in
        for try await batch in stream.stream { try await apply(batch) }
      },
      stop: { try await self.stop(index) })
  }

  func succeed(_ index: Int, rows: [Int] = [], at order: UInt64 = 100) {
    pending.removeValue(forKey: index)?.resume(returning: staleSnapshot(rows, at: order))
  }

  func emit(_ index: Int, _ batch: CollectionChangeBatch<StaleRow, Int>) {
    feeds[index]?.yield(batch)
  }

  func fail(_ index: Int) {
    active.remove(index)
    pending.removeValue(forKey: index)?.resume(throwing: Failure())
  }

  private func cancel(_ index: Int) {
    guard let continuation = pending.removeValue(forKey: index) else { return }
    active.remove(index)
    cancelled.append(index)
    if holdsCancellation {
      cancelledSnapshots.append(continuation)
    } else {
      continuation.resume(throwing: CancellationError())
    }
  }

  private func stop(_ index: Int) async throws {
    guard feeds[index] != nil else { return }
    stopAttempts.append(index)
    if stopFailures > 0 {
      stopFailures -= 1
      throw Failure()
    }
    if holdsStops {
      await withCheckedContinuation { stopWaiters.append($0) }
    }
    guard let feed = feeds.removeValue(forKey: index) else { return }
    active.remove(index)
    stopped.append(index)
    feed.finish()
  }

  /// Also unblocks fixtures when an assertion aborts a test before normal lease release.
  func finish() async {
    resumeCancellation()
    resumeStops()
    stopFailures = 0
    for index in Array(pending.keys) { cancel(index) }
    for index in Array(feeds.keys) { try? await stop(index) }
  }

  private struct Failure: Error {}
}

/// The production store is unchanged. This forwarding fixture observes conditional clears
/// and removals and deliberately lists large subscriptions first to test pass scheduling.
private actor StaleStore: CollectionStore {
  typealias Model = StaleRow
  typealias Key = Int

  struct Clear: Sendable {
    let id: CollectionMaterializationID
    let position: CollectionSourceVersion
    let remaining: CollectionSourceVersion?
    let stoppedCount: Int
  }

  let base = InMemoryCollectionStore<StaleRow, Int>(key: \.id)
  let source: StaleSource
  private(set) var clears: [Clear] = []
  private(set) var removals: [CollectionMaterializationID] = []
  private(set) var listings = 0
  private var listingFailures = 0

  func failListings(_ count: Int) { listingFailures = count }
  private struct ListingFailure: Error {}

  init(source: StaleSource) { self.source = source }

  func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?
  {
    try await base.materialization(for: demand)
  }

  func staleMaterializations() async throws -> [CollectionStaleMaterialization] {
    listings += 1
    if listingFailures > 0 {
      listingFailures -= 1
      throw ListingFailure()
    }
    return try await base.staleMaterializations().sorted { $0.claimCount > $1.claimCount }
  }

  func clearStale(
    _ materializationID: CollectionMaterializationID, ifMarkedAt position: CollectionSourceVersion
  ) async throws {
    let stoppedCount = await source.stopped.count
    try await base.clearStale(materializationID, ifMarkedAt: position)
    let remaining = try await base.staleMaterializations()
      .first { $0.record.id == materializationID }?.markedAt
    clears.append(
      .init(
        id: materializationID, position: position, remaining: remaining,
        stoppedCount: stoppedCount))
  }

  func replaceSnapshot(
    _ snapshot: CollectionSnapshot<StaleRow>, materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    try await base.replaceSnapshot(snapshot, materializationID: materializationID, demand: demand)
  }

  func apply(
    _ batch: CollectionChangeBatch<StaleRow, Int>, to materializationID: CollectionMaterializationID
  ) async throws {
    try await base.apply(batch, to: materializationID)
  }

  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    try await base.removeMaterialization(materializationID)
    removals.append(materializationID)
  }
}

/// Sleep never elapses until the test advances it. Cancellation always releases a sleeper.
private actor StaleClock: ShapeSubscriptionClock {
  private var sleepers:
    [UUID: (deadline: Duration, continuation: CheckedContinuation<Void, any Error>)] = [:]
  private var now = Duration.zero
  var pendingDelays: [Duration] { sleepers.values.map { $0.deadline - now } }
  private(set) var delays: [Duration] = []

  func sleep(for duration: Duration) async throws {
    let id = UUID()
    try await withTaskCancellationHandler {
      try Task.checkCancellation()
      try await withCheckedThrowingContinuation {
        (continuation: CheckedContinuation<Void, any Error>) in
        delays.append(duration)
        sleepers[id] = (now + duration, continuation)
      }
    } onCancel: {
      Task { await self.cancel(id) }
    }
  }

  func advance() {
    guard let deadline = sleepers.values.map(\.deadline).max() else { return }
    advance(by: deadline - now)
  }

  func advance(by duration: Duration) {
    now += duration
    let ready = sleepers.filter { $0.value.deadline <= now }
    for (id, sleeper) in ready {
      sleepers.removeValue(forKey: id)
      sleeper.continuation.resume()
    }
  }

  private func cancel(_ id: UUID) {
    sleepers.removeValue(forKey: id)?.continuation.resume(throwing: CancellationError())
  }
}

private actor StaleSpanSink: TelemetrySink {
  struct Span: Sendable {
    let name: String
    let attributes: [String: String]
  }

  private(set) var spans: [Span] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let data = request.httpBody ?? Data()
    let body = try JSONSerialization.jsonObject(with: data) as? [String: Any]
    for resource in body?["resourceSpans"] as? [[String: Any]] ?? [] {
      for scope in resource["scopeSpans"] as? [[String: Any]] ?? [] {
        for span in scope["spans"] as? [[String: Any]] ?? [] {
          var attributes: [String: String] = [:]
          for attribute in span["attributes"] as? [[String: Any]] ?? [] {
            guard let key = attribute["key"] as? String,
              let value = attribute["value"] as? [String: Any]
            else { continue }
            attributes[key] = value["stringValue"] as? String
          }
          spans.append(.init(name: span["name"] as? String ?? "", attributes: attributes))
        }
      }
    }
    return HTTPResponse(
      data: Data(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
  }

  func revalidations() -> [Span] { spans.filter { $0.name == "sync.revalidate" } }
}

private func staleVersion(_ order: UInt64) -> CollectionSourceVersion {
  .init(rawValue: "position-\(order)", order: order)
}

private func staleSnapshot(_ rows: [Int], at order: UInt64) -> CollectionSnapshot<StaleRow> {
  .init(
    rows: rows.map { StaleRow(id: $0) }, fence: .init(rawValue: "snapshot-\(order)"),
    sourceVersion: staleVersion(order))
}

private let staleScope = CollectionScope(
  principal: "user", authorization: "authorized", generation: "generation-1")

private struct StaleFixture: Sendable {
  typealias Coordinator = CollectionCoordinator<StaleRow, Int, StaleSource, StaleStore>
  enum Rebuilder: Equatable, Sendable {
    case known, absent, returnsNil, wrongPredicate, wrongOrder, wrongLimit, wrongSource
  }

  let scope: CollectionScope
  let rebuilder: Rebuilder
  let source: StaleSource
  let store: StaleStore
  let definition: CollectionDefinition<StaleRow, Int>
  let coordinator: Coordinator
  let clock = StaleClock()
  let sink = StaleSpanSink()
  let telemetry: TelemetryReporter

  init(
    rebuilder: Rebuilder = .known, scope: CollectionScope = staleScope,
    source: StaleSource = StaleSource(), store: StaleStore? = nil,
    collectionID: String = "items", subscriptionKind: String? = "push_read",
    subscriptionKindForDemand: (@Sendable (CollectionDemandIdentity) -> String)? = nil
  ) {
    self.scope = scope
    self.rebuilder = rebuilder
    self.source = source
    let store = store ?? StaleStore(source: source)
    self.store = store
    let bare = CollectionDefinition<StaleRow, Int>(id: .init(rawValue: collectionID), key: \.id)
    let known = Dictionary(
      uniqueKeysWithValues: ["one", "two", "three", "four", "five", "dropper", "later"].map {
        name in
        let demand = CollectionDemand<StaleRow>(
          unsafePredicateIdentity: name,
          order: rebuilder == .wrongSource
            ? [.init(unsafeFieldID: "id", sourceName: "original_column")] : [])
        return (demand.identity(for: bare, scope: scope).canonicalDemand, demand)
      })
    let rebuild: (@Sendable (CollectionDemandIdentity) -> CollectionDemand<StaleRow>?)?
    switch rebuilder {
    case .known: rebuild = { known[$0.canonicalDemand] }
    case .wrongPredicate, .wrongOrder, .wrongLimit, .wrongSource:
      rebuild = { identity in
        guard let demand = known[identity.canonicalDemand] else { return nil }
        guard demand.predicateIdentity != "later" else { return demand }
        switch rebuilder {
        case .wrongPredicate: return .init(unsafePredicateIdentity: "wrong")
        case .wrongOrder:
          return .init(
            unsafePredicateIdentity: demand.predicateIdentity,
            order: [.init(unsafeFieldID: "id", direction: .descending)])
        case .wrongLimit: return .init(unsafePredicateIdentity: demand.predicateIdentity, limit: 1)
        case .wrongSource:
          return .init(
            unsafePredicateIdentity: demand.predicateIdentity,
            order: [.init(unsafeFieldID: "id", sourceName: "different_column")])
        default: return nil
        }
      }
    case .absent: rebuild = nil
    case .returnsNil: rebuild = { _ in nil }
    }
    let definition = CollectionDefinition<StaleRow, Int>(
      id: bare.id, subscriptionKind: subscriptionKind,
      subscriptionKindForDemand: subscriptionKindForDemand, rebuildDemand: rebuild, key: \.id)
    self.definition = definition
    coordinator = Coordinator(definition: definition, scope: scope, source: source, store: store)
    telemetry = TelemetryReporter(
      configuration: .init(tracesEndpoint: URL(string: "https://telemetry.invalid/traces")),
      sink: sink)
  }

  func demand(_ owner: String) -> CollectionDemand<StaleRow> {
    .init(
      unsafePredicateIdentity: owner,
      order: rebuilder == .wrongSource
        ? [.init(unsafeFieldID: "id", sourceName: "original_column")] : [])
  }

  func identity(_ owner: String) -> CollectionDemandIdentity {
    demand(owner).identity(for: definition, scope: scope)
  }

  func id(_ owner: String) -> CollectionMaterializationID { .init(rawValue: owner) }

  func seed(_ owner: String, rows: [Int], at order: UInt64 = 1) async throws {
    try await store.replaceSnapshot(
      staleSnapshot(rows, at: order), materializationID: id(owner), demand: identity(owner))
  }

  func mark(_ owners: [String] = ["one"], rows: [Int] = [1]) async throws {
    try await seed("dropper", rows: rows)
    for owner in owners { try await seed(owner, rows: rows) }
    try await seed("dropper", rows: [], at: 10)
    let marked = try await store.base.staleMaterializations()
    #expect(Set(marked.map(\.record.id)) == Set(owners.map(id)))
  }

  func marks() async throws -> [CollectionStaleMaterialization] {
    try await store.base.staleMaterializations()
  }

  /// Wall time bounds failed assertions only; the injected clock controls pass time.
  func eventually(
    advancingClock: Bool = true, _ condition: @Sendable () async throws -> Bool
  ) async throws -> Bool {
    let deadline = ContinuousClock.now + .seconds(2)
    repeat {
      if try await condition() { return true }
      if advancingClock { await clock.advance() }
      try await Task.sleep(for: .milliseconds(1))
    } while ContinuousClock.now < deadline
    return try await condition()
  }

  func withPass(
    gateOpen: Bool = true, _ body: @Sendable () async throws -> Void
  ) async throws {
    await coordinator.setStaleRevalidationGate(isOpen: gateOpen)
    let task = Task {
      await coordinator.startStaleRevalidation(clock: clock, telemetry: telemetry)
    }
    do {
      try await body()
    } catch {
      await stopPass(task)
      await telemetry.shutdown()
      throw error
    }
    await stopPass(task)
    await telemetry.shutdown()
  }

  func stopPass(_ task: Task<Void, Never>) async {
    task.cancel()
    await source.finish()
    await task.value
  }

  func expectSpans(_ outcomes: [String], released: [String], returned: [String]) async throws {
    let arrived = try await eventually {
      await sink.revalidations().count >= outcomes.count
    }
    try #require(arrived, "Expected exported sync.revalidate spans")
    await telemetry.flush()
    let spans = await sink.revalidations()
    #expect(spans.count == outcomes.count)
    #expect(spans.map { $0.attributes["sync.outcome"] ?? "missing" } == outcomes)
    #expect(spans.map { $0.attributes["sync.claims_released"] ?? "missing" } == released)
    #expect(spans.map { $0.attributes["sync.rows_returned"] ?? "missing" } == returned)
    for span in spans {
      #expect(span.attributes["electric.table"] == "items")
      #expect(span.attributes["sync.subscription_kind"] == "push_read")
      let age = try #require(span.attributes["sync.seconds_since_mark"].flatMap(Double.init))
      let duration = try #require(span.attributes["sync.duration_seconds"].flatMap(Double.init))
      #expect(age.isFinite && age >= 0)
      #expect(duration.isFinite && duration >= 0)
    }
  }
}

private actor StaleCompletion {
  private(set) var finished = false
  func finish() { finished = true }
}

@Suite("Collection coordinator stale pass", .timeLimit(.minutes(1)))
struct CollectionCoordinatorStalePassTests {
  @Test func drainingLongSnapshotPausesAdmissionWithoutCancelling() async throws {
    let f = StaleFixture()
    for (index, owner) in ["one", "two", "three"].enumerated() {
      try await f.seed(owner, rows: Array(1...(index + 1)).map { $0 + index * 10 })
    }
    try await f.seed("dropper", rows: [1, 11, 12, 21, 22, 23])
    try await f.seed("dropper", rows: [], at: 10)
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      let completed = StaleCompletion()
      let drain = Task {
        await f.coordinator.drainStaleRevalidation()
        await completed.finish()
      }
      try #require(
        try await f.eventually(advancingClock: false) { await f.clock.pendingDelays.isEmpty })
      await f.clock.advance(by: .seconds(60))
      #expect(await f.source.cancelled.isEmpty)
      #expect(await completed.finished == false)
      #expect(await f.source.requests.count == 2)
      await f.source.succeed(0)
      await f.source.succeed(1)
      await drain.value
      #expect(await f.source.stopped.count == 2)
      #expect(await f.store.clears.count == 2)
      #expect(await f.source.requests.count == 2)
      #expect(try await f.marks().count == 1)
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 3 })
      await f.source.succeed(2)
      try #require(try await f.eventually(advancingClock: false) { try await f.marks().isEmpty })
    }
  }

  @Test func nextTableOpensOnlyAfterDrainFinishesLeaseRelease() async throws {
    let source = StaleSource()
    let first = StaleFixture(source: source)
    let next = StaleFixture(source: source, collectionID: "next_table")
    try await first.mark()
    try await next.mark()
    await source.holdStops()
    try await first.withPass {
      try await next.withPass(gateOpen: false) {
        try #require(
          try await first.eventually(advancingClock: false) { await source.requests.count == 1 })
        let rotation = Task {
          await first.coordinator.drainStaleRevalidation()
          await next.coordinator.setStaleRevalidationGate(isOpen: true)
        }
        await source.succeed(0)
        try #require(
          try await first.eventually(advancingClock: false) { await source.stopAttempts == [0] })
        #expect(await source.requests.count == 1)
        #expect(await source.stopped.isEmpty)
        await source.resumeStops()
        await rotation.value
        try #require(
          try await next.eventually(advancingClock: false) { await source.requests.count == 2 })
        #expect(await source.requests[1].identity.collection == next.definition.id)
        #expect(await source.stopped == [0])
        #expect(await source.maximumActive == 1)
        await source.succeed(1)
        try #require(
          try await next.eventually(advancingClock: false) { await next.store.clears.count == 1 })
      }
    }
  }

  @Test func drainKeepsRefusedReleaseOwnedUntilRetryActuallyReleases() async throws {
    let f = StaleFixture()
    try await f.mark()
    await f.source.failStops(2)
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays.contains(.milliseconds(250))
        })
      let completed = StaleCompletion()
      let drain = Task {
        await f.coordinator.drainStaleRevalidation()
        await completed.finish()
      }
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.milliseconds(250)]
        })
      #expect(await completed.finished == false)
      #expect(await f.source.stopped.isEmpty)
      await f.clock.advance(by: .milliseconds(250))
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.milliseconds(500)]
        })
      #expect(await completed.finished == false)
      #expect(await f.source.stopAttempts == [0, 0])
      await f.clock.advance(by: .milliseconds(500))
      await drain.value
      #expect(await f.source.stopped == [0])
      #expect(await f.source.requests.count == 1)
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().count == 1)
      // A failed run can retain an ordinary retry timer; drain does not await that timer.
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays.contains(.milliseconds(250))
        })
    }
  }

  @Test func drainDoesNotWaitForRequestRetryTimer() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.fail(0)
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays.contains(.milliseconds(250))
        })
      await f.coordinator.drainStaleRevalidation()
      #expect(await f.source.requests.count == 1)
      await f.clock.advance(by: .seconds(30))
      #expect(await f.source.requests.count == 1)
      #expect(try await f.marks().count == 1)
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      await f.source.succeed(1)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
    }
  }

  @Test func lifecycleClosureCancelsDrainingSnapshotAndDrainNeverReopens() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      let drain = Task { await f.coordinator.drainStaleRevalidation() }
      try #require(
        try await f.eventually(advancingClock: false) { await f.clock.pendingDelays.isEmpty })
      await f.coordinator.setStaleRevalidationGate(isOpen: false)
      await drain.value
      #expect(await f.source.cancelled == [0])
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().count == 1)
      await f.clock.advance(by: .seconds(60))
      #expect(await f.source.requests.count == 1)
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      await f.source.succeed(1)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
    }
  }

  @Test func accountRetirementCancelsSnapshotDuringDrain() async throws {
    let f = StaleFixture()
    try await f.mark()
    await f.coordinator.setStaleRevalidationGate(isOpen: true)
    let pass = Task { await f.coordinator.startStaleRevalidation(clock: f.clock) }
    do {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      let drain = Task { await f.coordinator.drainStaleRevalidation() }
      try #require(
        try await f.eventually(advancingClock: false) { await f.clock.pendingDelays.isEmpty })
      pass.cancel()
      await drain.value
      await pass.value
      #expect(await f.source.cancelled == [0])
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().count == 1)
      #expect(await f.source.requests.count == 1)
    } catch {
      await f.stopPass(pass)
      await f.telemetry.shutdown()
      throw error
    }
    await f.telemetry.shutdown()
  }

  @Test func lifecycleClosureStopsRefusedReleaseRetriesAndReopenUsesRetainedAuthority() async throws
  {
    let f = StaleFixture()
    try await f.mark()
    await f.source.failStops(Int.max)
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays.contains(.milliseconds(250))
        })
      let drain = Task { await f.coordinator.drainStaleRevalidation() }
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.milliseconds(250)]
        })
      // No clock advance, stop-success override or source.finish can unblock either call.
      await f.coordinator.setStaleRevalidationGate(isOpen: false)
      await drain.value
      #expect(await f.source.stopAttempts == [0])
      #expect(await f.source.stopped.isEmpty)
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().count == 1)
      #expect(await f.clock.pendingDelays.isEmpty)
      await f.clock.advance(by: .seconds(60))
      #expect(await f.source.stopAttempts == [0])
      // This generation is still alive. Reopening must release the old authority before
      // acquiring a replacement, rather than forgetting the refused lease on gate closure.
      await f.source.failStops(0)
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      #expect(await f.source.stopped == [0])
      #expect(await f.source.stopAttempts == [0, 0])
      await f.source.succeed(1)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
      #expect(await f.source.stopped == [0, 1])
    }
  }

  @Test func accountRetirementStopsPermanentReleaseRefusalAfterOneFinalAttempt() async throws {
    let f = StaleFixture()
    try await f.mark()
    await f.source.failStops(Int.max)
    await f.coordinator.setStaleRevalidationGate(isOpen: true)
    let pass = Task { await f.coordinator.startStaleRevalidation(clock: f.clock) }
    do {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays.contains(.milliseconds(250))
        })
      let drain = Task { await f.coordinator.drainStaleRevalidation() }
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.milliseconds(250)]
        })
      pass.cancel()
      // Await retirement with DELETE still permanently refused, without driving fake time.
      await pass.value
      await drain.value
      #expect(await f.source.stopAttempts == [0, 0])
      #expect(await f.source.stopped.isEmpty)
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().count == 1)
      #expect(await f.clock.pendingDelays.isEmpty)
      await f.clock.advance(by: .seconds(60))
      #expect(await f.source.stopAttempts == [0, 0])
    } catch {
      await f.stopPass(pass)
      await f.telemetry.shutdown()
      throw error
    }
    // Fixture cleanup is strictly after the retirement assertions above.
    await f.source.finish()
    await f.telemetry.shutdown()
  }

  @Test func drainingSharedRerunLeavesScreenLeaseLive() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      let screen = await f.coordinator.acquire(f.demand("one"))
      let drain = Task { await f.coordinator.drainStaleRevalidation() }
      await f.source.succeed(0)
      await drain.value
      #expect(await screen.state() == .live)
      #expect(await f.source.stopped.isEmpty)
      #expect(await f.source.requests.count == 1)
      #expect(await f.store.clears.count == 1)
      try await screen.release()
      #expect(await f.source.stopped == [0])
    }
  }

  @Test(arguments: [true, false])
  func demandKindClassifierUsesOriginalStoredIdentity(classified: Bool) async throws {
    let old = StaleFixture()
    try await old.mark()
    let expected = old.identity("one")
    let classifier: (@Sendable (CollectionDemandIdentity) -> String)?
    if classified {
      classifier = { @Sendable identity in identity == expected ? "thread" : "other" }
    } else {
      classifier = nil
    }
    let f = StaleFixture(
      scope: .init(principal: "user", authorization: "authorized", generation: "next-launch"),
      source: old.source, store: old.store, subscriptionKind: "push_read",
      subscriptionKindForDemand: classifier)
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(try await f.eventually { await f.sink.revalidations().count == 1 })
      #expect(
        await f.sink.revalidations().first?.attributes["sync.subscription_kind"]
          == (classified ? "thread" : "push_read"))
    }
    await old.telemetry.shutdown()
  }

  @Test func classifierDistinguishesDemandsWithinOneCollection() async throws {
    let reference = StaleFixture()
    let thread = reference.identity("one")
    let f = StaleFixture(
      rebuilder: .absent, subscriptionKind: "fallback",
      subscriptionKindForDemand: { $0 == thread ? "thread" : "discuss" })
    try await f.seed("one", rows: [1])
    try await f.seed("two", rows: [2])
    try await f.seed("dropper", rows: [1, 2])
    try await f.seed("dropper", rows: [], at: 10)
    try await f.withPass {
      try #require(try await f.eventually { await f.sink.revalidations().count == 2 })
      let spans = await f.sink.revalidations()
      #expect(
        Set(spans.compactMap { $0.attributes["sync.subscription_kind"] }) == ["thread", "discuss"])
      #expect(spans.allSatisfy { $0.attributes["sync.outcome"] == "unrebuildable" })
    }
    await reference.telemetry.shutdown()
  }

  @Test func missingKindClassifierAndStaticLabelOmitsAttribute() async throws {
    let f = StaleFixture(rebuilder: .absent, subscriptionKind: nil)
    try await f.mark()
    try await f.withPass {
      try #require(try await f.eventually { await f.sink.revalidations().count == 1 })
      #expect(await f.sink.revalidations().first?.attributes["sync.subscription_kind"] == nil)
    }
  }

  // Keep gate closure and the screen acquire in one actor turn. This exercises the exact
  // interval before the cancelled stale run can release its lease, without scheduler luck.
  private func closeAndAcquire(
    _ coordinator: isolated StaleFixture.Coordinator, demand: CollectionDemand<StaleRow>
  ) -> (CollectionLease, [Task<Void, Never>]) {
    let cancelled = coordinator.closeStaleRevalidationGate()
    return (coordinator.acquire(demand), cancelled)
  }

  @Test func screenJoiningCancelledStaleRunStillReachesLive() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      let (screen, cancelled) = await closeAndAcquire(f.coordinator, demand: f.demand("one"))
      for task in cancelled { await task.value }
      // The screen joined before final release: its shared request must still be running.
      #expect(await f.source.cancelled.isEmpty)
      #expect(await f.source.requests.count == 1)
      await f.source.succeed(0)
      try #require(try await f.eventually(advancingClock: false) { await screen.state() == .live })
      #expect(await f.store.clears.isEmpty)
      try await screen.release()
      #expect(await f.source.stopped == [0])
    }
  }

  @Test func screenJoiningDuringFinalReleaseGetsRestarted() async throws {
    let f = StaleFixture()
    try await f.mark()
    await f.source.holdCancellation()
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      let closing = Task { await f.coordinator.setStaleRevalidationGate(isOpen: false) }
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.cancelled == [0] })
      // Final release is waiting for the cancelled source to finish. Join on that side of
      // cancellation too: cleanup must start a replacement for this remaining screen lease.
      let screen = await f.coordinator.acquire(f.demand("one"))
      await f.source.resumeCancellation()
      await closing.value
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      await f.source.succeed(1)
      try #require(try await f.eventually(advancingClock: false) { await screen.state() == .live })
      #expect(await f.store.clears.isEmpty)
      try await screen.release()
      #expect(await f.source.stopped == [1])
    }
  }

  @Test(arguments: [StaleFixture.Rebuilder.wrongPredicate, .wrongOrder, .wrongLimit, .wrongSource])
  fileprivate func mismatchedRebuilderRemovesClaimsAndFreesSlots(rebuilder: StaleFixture.Rebuilder)
    async throws
  {
    let f = StaleFixture(rebuilder: rebuilder)
    try await f.seed("one", rows: [1])
    try await f.seed("two", rows: [2])
    try await f.seed("later", rows: [3, 4])
    try await f.seed("dropper", rows: [1, 2, 3, 4])
    try await f.seed("dropper", rows: [], at: 10)
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.removals.count == 2 })
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      #expect(await f.source.requests[0].identity == f.identity("later"))
      #expect(await f.store.base.rows().keys.sorted() == [3, 4])
      await f.source.succeed(0)
      try #require(try await f.eventually(advancingClock: false) { try await f.marks().isEmpty })
      #expect(await f.store.base.rows().isEmpty)
      try await f.expectSpans(
        ["unrebuildable", "unrebuildable", "refreshed"],
        released: ["1", "1", "2"], returned: ["0", "0", "0"])
    }
  }

  @Test func idleListingWaitsFiveSecondsAndFindsExternalMarks() async throws {
    let f = StaleFixture()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      #expect(await f.store.listings == 1)
      for expectedListings in 2...4 {
        await f.clock.advance(by: .seconds(5))
        try #require(
          try await f.eventually(advancingClock: false) {
            let count = await f.store.listings
            let delays = await f.clock.pendingDelays
            return count == expectedListings && delays == [.seconds(5)]
          })
      }
      try await f.mark()
      await f.clock.advance(by: .milliseconds(4999))
      #expect(await f.store.listings == 4)
      #expect(await f.source.requests.isEmpty)
      await f.clock.advance(by: .milliseconds(1))
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
    }
  }

  @Test func failedListingsBackOffBeforePromptRecovery() async throws {
    let f = StaleFixture()
    try await f.mark()
    await f.store.failListings(2)
    try await f.withPass {
      for (index, delay) in [Duration.milliseconds(250), .milliseconds(500)].enumerated() {
        try #require(
          try await f.eventually(advancingClock: false) {
            await f.clock.pendingDelays.contains(delay)
          })
        #expect(await f.store.listings == index + 1)
        await f.coordinator.setStaleRevalidationGate(isOpen: false)
        await f.coordinator.setStaleRevalidationGate(isOpen: true)
        await f.clock.advance(by: delay - .milliseconds(1))
        #expect(await f.store.listings == index + 1)
        #expect(await f.source.requests.isEmpty)
        await f.clock.advance(by: .milliseconds(1))
      }
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
    }
  }

  @Test func openingGateListsImmediatelyWithoutAdvancingClock() async throws {
    let f = StaleFixture()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      await f.coordinator.setStaleRevalidationGate(isOpen: false)
      try await f.mark()
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
    }
  }

  @Test func localSnapshotWakesIdleListing() async throws {
    let f = StaleFixture()
    try await f.seed("one", rows: [1])
    try await f.seed("dropper", rows: [1])
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      let screen = await f.coordinator.acquire(f.demand("dropper"))
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      #expect(await f.source.requests[1].identity == f.identity("one"))
      await f.source.succeed(1)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
      try await screen.release()
    }
  }

  @Test func localFeedDropWakesIdleListing() async throws {
    let f = StaleFixture()
    try await f.seed("one", rows: [1])
    let screen = await f.coordinator.acquire(f.demand("dropper"))
    try #require(try await f.eventually { await f.source.requests.count == 1 })
    await f.source.succeed(0, rows: [1])
    try #require(try await f.eventually { await screen.state() == .live })
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) {
          await f.clock.pendingDelays == [.seconds(5)]
        })
      await f.source.emit(
        0,
        .init(
          changes: [.delete(1, sourceVersion: staleVersion(110))], expectedCursor: nil,
          cursor: .init(offset: "feed-110"), sourceVersion: staleVersion(110)))
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 2 })
      #expect(await f.source.requests[1].identity == f.identity("one"))
      await f.source.succeed(1, at: 120)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
      try await screen.release()
    }
  }

  @Test func earlierLaunchRefreshUsesCurrentScopeAndReleasesOldClaim() async throws {
    let old = StaleFixture()
    try await old.mark()
    let current = StaleFixture(
      scope: .init(principal: "user", authorization: "authorized", generation: "next-launch"),
      source: old.source, store: old.store)
    try await current.withPass {
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.source.requests.count == 1
        })
      #expect(await current.source.requests[0].identity == current.identity("one"))
      #expect(await current.source.requests[0].materializationID != old.id("one"))
      await current.source.succeed(0)
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.store.removals.contains(old.id("one"))
        })
      #expect(try await current.store.materialization(for: old.identity("one")) == nil)
      #expect(try await current.store.materialization(for: current.identity("one")) != nil)
      #expect(await current.store.base.rows().isEmpty)
      #expect(try await current.marks().isEmpty)
      #expect(await current.source.stopped == [0])
    }
    await old.telemetry.shutdown()
  }

  @Test func earlierLaunchHeldDemandUsesCurrentEntryWithoutAnotherRequest() async throws {
    let old = StaleFixture()
    try await old.mark()
    let current = StaleFixture(
      rebuilder: .absent,
      scope: .init(principal: "user", authorization: "authorized", generation: "next-launch"),
      source: old.source, store: old.store)
    let screen = await current.coordinator.acquire(current.demand("one"))
    try #require(try await current.eventually { await current.source.requests.count == 1 })
    await current.source.succeed(0)
    try #require(try await current.eventually { await screen.state() == .live })
    try await current.withPass {
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.store.removals == [old.id("one")]
        })
      #expect(await current.source.requests.count == 1)
      #expect(await current.source.stopped.isEmpty)
      #expect(await screen.state() == .live)
      #expect(await current.store.base.rows().isEmpty)
      try await current.expectSpans(["held"], released: ["0"], returned: ["0"])
      try await screen.release()
    }
    await old.telemetry.shutdown()
  }

  @Test func earlierLaunchFailureKeepsOldClaimsUntilCurrentSnapshotSucceeds() async throws {
    let old = StaleFixture()
    try await old.mark()
    let current = StaleFixture(
      scope: .init(principal: "user", authorization: "authorized", generation: "next-launch"),
      source: old.source, store: old.store)
    try await current.withPass {
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.source.requests.count == 1
        })
      await current.source.fail(0)
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.clock.pendingDelays.contains(.milliseconds(250))
        })
      #expect(await current.store.removals.isEmpty)
      #expect(await current.store.base.rowClaims(for: old.id("one")) == [1])
      #expect(try await current.marks().map(\.record.demand) == [old.identity("one")])
      await current.clock.advance(by: .milliseconds(250))
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.source.requests.count == 2
        })
      await current.source.succeed(1)
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.store.removals == [old.id("one")]
        })
      #expect(await current.store.base.rows().isEmpty)
    }
    await old.telemetry.shutdown()
  }

  @Test func earlierLaunchUnrebuildableRemovesStoredIdentity() async throws {
    let old = StaleFixture()
    try await old.mark()
    let current = StaleFixture(
      rebuilder: .wrongLimit,
      scope: .init(principal: "user", authorization: "authorized", generation: "next-launch"),
      source: old.source, store: old.store)
    try await current.withPass {
      try #require(
        try await current.eventually(advancingClock: false) {
          await current.store.removals == [old.id("one")]
        })
      #expect(await current.source.requests.isEmpty)
      #expect(await current.store.base.rows().isEmpty)
      try await current.expectSpans(["unrebuildable"], released: ["1"], returned: ["0"])
    }
    await old.telemetry.shutdown()
  }

  @Test func closedGateRunsNothingAndOpeningItRuns() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass(gateOpen: false) {
      // Offer clock wakeups while closed, then use the same pass when opening the gate.
      for _ in 0..<100 {
        await f.clock.advance()
        await Task.yield()
      }
      #expect(await f.source.requests.isEmpty)
      #expect(await f.store.clears.isEmpty)
      #expect(await f.store.removals.isEmpty)
      #expect(try await f.marks().count == 1)
      await f.coordinator.setStaleRevalidationGate(isOpen: true)
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.source.requests.count == 1)
    }
  }

  @Test func fiveMarkedMaterializationsNeverExceedTwoConcurrentReruns() async throws {
    let f = StaleFixture()
    let owners = ["one", "two", "three", "four", "five"]
    // Disjoint rows prevent completing one snapshot from marking another subscription again.
    for (index, owner) in owners.enumerated() {
      try await f.seed(owner, rows: [index + 1])
    }
    try await f.seed("dropper", rows: [1, 2, 3, 4, 5])
    try await f.seed("dropper", rows: [], at: 10)
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count >= 2 })
      for _ in 0..<100 {
        await f.clock.advance()
        await Task.yield()
      }
      #expect(await f.source.requests.count == 2)
      for index in 0..<5 {
        try #require(try await f.eventually { await f.source.requests.count > index })
        await f.source.succeed(index)
      }
      try #require(try await f.eventually { await f.store.clears.count == 5 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.source.maximumActive == 2)
      #expect(await f.source.requests.count == 5)
      #expect(await f.source.stopped.count == 5)
      #expect(await f.store.clears.count == 5)
      let identities = await f.source.requests.map(\.identity)
      #expect(Set(identities) == Set(owners.map(f.identity)))
    }
  }

  @Test func smallestClaimCountsAreScheduledFirst() async throws {
    let f = StaleFixture()
    try await f.seed("one", rows: [1])
    try await f.seed("two", rows: [2, 3])
    try await f.seed("three", rows: [4, 5, 6])
    try await f.seed("dropper", rows: [1, 2, 3, 4, 5, 6])
    try await f.seed("dropper", rows: [], at: 10)
    #expect(try await f.store.staleMaterializations().map(\.claimCount) == [3, 2, 1])
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count >= 2 })
      let firstWave = await f.source.requests.map(\.identity)
      #expect(Set(firstWave) == [f.identity("one"), f.identity("two")])
      await f.source.succeed(0)
      try #require(try await f.eventually { await f.source.requests.count == 3 })
      #expect(await f.source.requests[2].identity == f.identity("three"))
      await f.source.succeed(1)
      await f.source.succeed(2)
      try #require(try await f.eventually { await f.store.clears.count == 3 })
      #expect(try await f.marks().isEmpty)
    }
  }

  @Test func twoDropsBeforeThePassProduceOneRerun() async throws {
    let f = StaleFixture()
    try await f.mark(rows: [1, 2])
    // Reclaim and drop one of the rows again before starting the pass.
    try await f.seed("dropper", rows: [2], at: 20)
    try await f.seed("dropper", rows: [], at: 30)
    #expect(try await f.marks().map(\.markedAt) == [staleVersion(30)])
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      await f.source.succeed(0)
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.store.clears.map(\.position) == [staleVersion(30)])
      #expect(await f.source.requests.count == 1)
      try await f.expectSpans(["refreshed"], released: ["2"], returned: ["0"])
    }
  }

  @Test func heldDemandClearsItsMarkWithoutAnotherRequest() async throws {
    // A held lease needs no rebuilder, including a lease retained by an app warm pool.
    let f = StaleFixture(rebuilder: .absent)
    try await f.seed("dropper", rows: [1])
    let lease = await f.coordinator.acquire(f.demand("one"))
    try #require(try await f.eventually { await f.source.requests.count == 1 })
    await f.source.succeed(0, rows: [1])
    try #require(try await f.eventually { await lease.state() == .live })
    try await f.seed("dropper", rows: [], at: 110)
    #expect(try await f.marks().count == 1)
    try await f.withPass {
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.source.requests.count == 1)
      #expect(await f.source.stopped.isEmpty)
      #expect(await lease.state() == .live)
      #expect(await f.store.clears.map(\.position) == [staleVersion(110)])
      try await f.expectSpans(["held"], released: ["0"], returned: ["0"])
      try await lease.release()
    }
  }

  @Test func releasedDemandRerunsOnceAndEmptyAnswerReleasesItsClaim() async throws {
    let f = StaleFixture()
    try await f.seed("dropper", rows: [1])
    let lease = await f.coordinator.acquire(f.demand("one"))
    try #require(try await f.eventually { await f.source.requests.count == 1 })
    await f.source.succeed(0, rows: [1])
    try #require(try await f.eventually { await lease.state() == .live })
    let original = try #require(try await f.store.materialization(for: f.identity("one")))
    try await lease.release()
    try await f.seed("dropper", rows: [], at: 110)
    #expect(await f.store.base.rowClaims(for: original.id) == [1])
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 2 })
      #expect(await f.source.requests[1].materializationID == original.id)
      #expect(await f.source.requests[1].demand.predicateIdentity == "one")
      await f.source.succeed(1, at: 120)
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.store.base.rowClaims(for: original.id).isEmpty)
      #expect(await f.store.base.rows().isEmpty)
      #expect(await f.source.stopped == [0, 1])
      #expect(await f.store.clears.count == 1)
      #expect(await f.store.clears.first?.stoppedCount == 2)
      #expect(await f.source.requests.count == 2)
      try await f.expectSpans(["refreshed"], released: ["1"], returned: ["0"])
    }
  }

  @Test func failureKeepsMarkUntilRetryAfterInjectedBackoffSucceeds() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(
        try await f.eventually(advancingClock: false) { await f.source.requests.count == 1 })
      for (index, delay) in [Duration.milliseconds(250), .milliseconds(500)].enumerated() {
        await f.source.fail(index)
        try #require(
          try await f.eventually(advancingClock: false) {
            await f.clock.pendingDelays.contains(delay)
          })
        #expect(try await f.marks().map(\.markedAt) == [staleVersion(10)])
        #expect(await f.store.clears.isEmpty)
        await f.clock.advance(by: delay - .milliseconds(1))
        #expect(await f.source.requests.count == index + 1)
        await f.clock.advance(by: .milliseconds(1))
        // Retry expiry wakes the listing; it must not wait for the five-second idle poll.
        try #require(
          try await f.eventually(advancingClock: false) {
            await f.source.requests.count == index + 2
          })
      }
      await f.source.succeed(2)
      try #require(
        try await f.eventually(advancingClock: false) { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      try await f.expectSpans(
        ["failed", "failed", "refreshed"], released: ["0", "0", "1"], returned: ["0", "0", "0"])
    }
  }

  @Test func dropDuringRerunPreservesNewerMarkAndRunsAgain() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      try await f.seed("dropper", rows: [1], at: 20)
      try await f.seed("dropper", rows: [], at: 30)
      #expect(try await f.marks().map(\.markedAt) == [staleVersion(30)])
      await f.source.succeed(0, rows: [1])
      try #require(try await f.eventually { await f.source.requests.count == 2 })
      #expect(await f.store.clears.count == 1)
      #expect(await f.store.clears.first?.position == staleVersion(10))
      #expect(await f.store.clears.first?.remaining == staleVersion(30))
      #expect(try await f.marks().map(\.markedAt) == [staleVersion(30)])
      await f.source.succeed(1, at: 120)
      try #require(try await f.eventually { await f.store.clears.count == 2 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.store.clears.map(\.position) == [staleVersion(10), staleVersion(30)])
      #expect(await f.source.stopped == [0, 1])
      try await f.expectSpans(
        ["refreshed", "refreshed"], released: ["0", "1"], returned: ["1", "0"])
    }
  }

  @Test(arguments: [StaleFixture.Rebuilder.absent, .returnsNil])
  fileprivate func unrebuildableIdentityRemovesMaterialization(rebuilder: StaleFixture.Rebuilder)
    async throws
  {
    let f = StaleFixture(rebuilder: rebuilder)
    try await f.mark()
    try await f.withPass {
      try #require(try await f.eventually { await f.store.removals.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(try await f.store.materialization(for: f.identity("one")) == nil)
      #expect(await f.store.removals == [f.id("one")])
      #expect(await f.store.base.rowClaims(for: f.id("one")).isEmpty)
      #expect(await f.store.base.rows().isEmpty)
      #expect(await f.store.clears.isEmpty)
      #expect(await f.source.requests.isEmpty)
      try await f.expectSpans(["unrebuildable"], released: ["1"], returned: ["0"])
    }
  }

  @Test func screenAcquiringDuringRerunSharesLeaseAndMarkClearsOnce() async throws {
    let f = StaleFixture()
    try await f.mark()
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      let screen = await f.coordinator.acquire(f.demand("one"))
      await f.source.succeed(0, rows: [1])
      try #require(try await f.eventually { await screen.state() == .live })
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.source.requests.count == 1)
      #expect(await f.source.stopped.isEmpty)
      #expect(await f.store.clears.count == 1)
      try await f.expectSpans(["refreshed"], released: ["0"], returned: ["1"])
      try await screen.release()
      #expect(await f.source.stopped == [0])
    }
  }

  @Test(arguments: [
    CollectionScope(principal: "other", authorization: "authorized", generation: "generation-1"),
    CollectionScope(principal: "user", authorization: "new-authority", generation: "generation-1"),
  ])
  func accountRetirementCancelsInflightRunWithoutClearingMark(nextScope: CollectionScope)
    async throws
  {
    let f = StaleFixture()
    try await f.mark()
    await f.coordinator.setStaleRevalidationGate(isOpen: true)
    let pass = Task {
      await f.coordinator.startStaleRevalidation(clock: f.clock, telemetry: f.telemetry)
    }
    do {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      // This is the app-owned generation task's retirement, before constructing the new scope.
      pass.cancel()
      try #require(try await f.eventually { await f.source.cancelled == [0] })
      await pass.value
      #expect(await f.store.clears.isEmpty)
      #expect(try await f.marks().map(\.markedAt) == [staleVersion(10)])
      #expect(await f.store.base.rowClaims(for: f.id("one")) == [1])
      let next = StaleFixture(scope: nextScope, source: f.source, store: f.store)
      try await next.seed("two", rows: [2])
      try await next.seed("later", rows: [2])
      try await next.seed("later", rows: [], at: 20)
      try await next.withPass {
        try #require(try await next.eventually { await next.source.requests.count == 2 })
        #expect(await next.source.requests[1].identity == next.identity("two"))
        await next.source.succeed(1)
        try #require(try await next.eventually { await next.store.clears.count == 1 })
        #expect(try await f.marks().map(\.record.demand) == [f.identity("one")])
        #expect(await next.store.removals.isEmpty)
      }
    } catch {
      pass.cancel()
      await f.source.finish()
      await pass.value
      await f.telemetry.shutdown()
      throw error
    }
    await f.telemetry.shutdown()
  }

  @Test func newCoordinatorOverStoreWithExistingMarksRunsThem() async throws {
    let original = StaleFixture()
    try await original.mark()
    // The new coordinator has no acquired entries or in-process mark notifications.
    let restarted = StaleFixture(source: original.source, store: original.store)
    try await restarted.withPass {
      try #require(try await restarted.eventually { await restarted.source.requests.count == 1 })
      #expect(await restarted.source.requests[0].materializationID == original.id("one"))
      await restarted.source.succeed(0)
      try #require(try await restarted.eventually { await restarted.store.clears.count == 1 })
      #expect(try await restarted.marks().isEmpty)
      #expect(await restarted.source.requests.count == 1)
      try await restarted.expectSpans(["refreshed"], released: ["1"], returned: ["0"])
    }
    await original.telemetry.shutdown()
  }

  @Test(arguments: [[2, 3], [2, 3, 4]])
  func refreshedClaimsReleasedReportsNetDecreaseNotKeyChurn(rows: [Int]) async throws {
    let f = StaleFixture()
    try await f.mark(rows: [1, 2])
    try await f.withPass {
      try #require(try await f.eventually { await f.source.requests.count == 1 })
      await f.source.succeed(0, rows: rows)
      try #require(try await f.eventually { await f.store.clears.count == 1 })
      #expect(try await f.marks().isEmpty)
      #expect(await f.store.base.rowClaims(for: f.id("one")) == Set(rows))
      try await f.expectSpans(["refreshed"], released: ["0"], returned: [String(rows.count)])
    }
  }
}
