import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor CapacityTransport: HTTPTransport {
  private(set) var createRequests = 0
  private(set) var streamRequests = 0
  private(set) var releaseRequests = 0
  private var streamWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
  private var nextStreamID = 0
  private var streamContinuations: [Int: CheckedContinuation<Void, Never>] = [:]

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    switch request.httpMethod {
    case "POST":
      createRequests += 1
      return capacityShapeResponse(id: "s\(createRequests)")
    case "GET":
      nextStreamID += 1
      let streamID = nextStreamID
      streamRequests += 1
      resumeStreamWaiters()
      await withTaskCancellationHandler(
        operation: {
          guard !Task.isCancelled else { return }
          await withCheckedContinuation { streamContinuations[streamID] = $0 }
        },
        onCancel: { Task { await self.releaseStream(id: streamID) } })
      try Task.checkCancellation()
      throw CancellationError()
    case "DELETE":
      releaseRequests += 1
      return capacityResponse("", status: 404)
    default:
      throw CancellationError()
    }
  }

  func waitForStreamRequests(_ count: Int) async {
    guard streamRequests < count else { return }
    await withCheckedContinuation { streamWaiters.append((count, $0)) }
  }

  private func resumeStreamWaiters() {
    let ready = streamWaiters.filter { streamRequests >= $0.count }
    streamWaiters.removeAll { streamRequests >= $0.count }
    for waiter in ready { waiter.continuation.resume() }
  }

  private func releaseStream(id: Int) {
    streamContinuations.removeValue(forKey: id)?.resume()
  }
}

private actor RetryCapacityTransport: HTTPTransport {
  private(set) var createRequests = 0

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    guard request.httpMethod == "POST" else { throw CancellationError() }
    createRequests += 1
    return capacityResponse("unavailable", status: 503)
  }
}

private actor RetrySleepGate: ShapeSubscriptionClock {
  private var activeSleepIDs: Set<Int> = []
  private var nextID = 0
  private var sleepContinuations: [Int: CheckedContinuation<Void, Never>] = [:]
  private var countWaiters: [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []

  func sleep(for _: Duration) async throws {
    nextID += 1
    let id = nextID
    activeSleepIDs.insert(id)
    resumeCountWaiters()
    await withTaskCancellationHandler(
      operation: {
        guard !Task.isCancelled else {
          releaseSleep(id: id)
          return
        }
        await withCheckedContinuation { continuation in
          if Task.isCancelled {
            continuation.resume()
          } else {
            sleepContinuations[id] = continuation
          }
        }
      },
      onCancel: { Task { await self.releaseSleep(id: id) } })
    try Task.checkCancellation()
  }

  func waitForSleepers(_ count: Int) async {
    guard activeSleepIDs.count < count else { return }
    await withCheckedContinuation { countWaiters.append((count, $0)) }
  }

  func sleeperCount() -> Int { activeSleepIDs.count }

  private func releaseSleep(id: Int) {
    guard activeSleepIDs.remove(id) != nil else { return }
    sleepContinuations.removeValue(forKey: id)?.resume()
  }

  private func resumeCountWaiters() {
    let ready = countWaiters.filter { activeSleepIDs.count >= $0.count }
    countWaiters.removeAll { activeSleepIDs.count >= $0.count }
    for waiter in ready { waiter.continuation.resume() }
  }
}

private actor PreflightCountingMaterializer: ShapeMaterializer {
  private var preflightReads = 0

  func currentCursor() async throws -> StreamCursor? {
    preflightReads += 1
    return nil
  }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {}

  func preflightCount() -> Int { preflightReads }
}

private actor FailingPreflightMaterializer: ShapeMaterializer {
  func currentCursor() async throws -> StreamCursor? { throw CapacityPreflightError.unavailable }
  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {}
}

private enum CapacityPreflightError: Error, Sendable {
  case unavailable
}

private actor BlockingPreflightMaterializer: ShapeMaterializer {
  private var entered = false
  private var entryWaiters: [CheckedContinuation<Void, Never>] = []
  private var gate: CheckedContinuation<Void, Never>?

  func currentCursor() async throws -> StreamCursor? {
    entered = true
    let waiters = entryWaiters
    entryWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
    await withTaskCancellationHandler(
      operation: { await withCheckedContinuation { gate = $0 } },
      onCancel: { Task { await self.releaseGate() } })
    try Task.checkCancellation()
    return nil
  }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {}

  func waitForEntry() async {
    guard !entered else { return }
    await withCheckedContinuation { entryWaiters.append($0) }
  }

  private func releaseGate() {
    gate?.resume()
    gate = nil
  }
}

private actor TerminalCapacityTransport: HTTPTransport {
  private(set) var releaseRequests = 0

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    switch request.httpMethod {
    case "POST": return capacityShapeResponse(id: "terminal")
    case "GET": return capacityResponse("gone", status: 404)
    case "DELETE":
      releaseRequests += 1
      return capacityResponse("", status: 404)
    default: throw CancellationError()
    }
  }
}

private actor ReleaseFailureCapacityTransport: HTTPTransport {
  private var releases = 0
  private var streamContinuation: CheckedContinuation<Void, Never>?

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    switch request.httpMethod {
    case "POST": return capacityShapeResponse(id: "release-failure")
    case "GET":
      await withTaskCancellationHandler(
        operation: {
          guard !Task.isCancelled else { return }
          await withCheckedContinuation { streamContinuation = $0 }
        },
        onCancel: { Task { await self.releaseStream() } })
      try Task.checkCancellation()
      throw CancellationError()
    case "DELETE":
      releases += 1
      return releases == 1
        ? capacityResponse("temporary", status: 503) : capacityResponse("", status: 404)
    default: throw CancellationError()
    }
  }

  private func releaseStream() {
    streamContinuation?.resume()
    streamContinuation = nil
  }
}

/// Deliberately ignores create cancellation: the server may have accepted the claim while the
/// client lost its response. The DELETE gate makes the public cancellation-return boundary
/// observable without a timing assertion.
private actor HeldCreateAndDeleteTransport: HTTPTransport {
  private var createEntered = false
  private var createWaiters: [CheckedContinuation<Void, Never>] = []
  private var createReleased = false
  private var createGate: CheckedContinuation<Void, Never>?
  private var deleteEntered = false
  private var deleteWaiters: [CheckedContinuation<Void, Never>] = []
  private var deleteReleased = false
  private var deleteGate: CheckedContinuation<Void, Never>?
  private(set) var createRequests = 0
  private(set) var deleteRequests = 0

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    switch request.httpMethod {
    case "POST":
      createRequests += 1
      createEntered = true
      let waiters = createWaiters
      createWaiters.removeAll()
      for waiter in waiters { waiter.resume() }
      if !createReleased {
        await withCheckedContinuation { continuation in
          if createReleased {
            continuation.resume()
          } else {
            createGate = continuation
          }
        }
      }
      return capacityShapeResponse(id: "held")
    case "DELETE":
      deleteRequests += 1
      deleteEntered = true
      let waiters = deleteWaiters
      deleteWaiters.removeAll()
      for waiter in waiters { waiter.resume() }
      if !deleteReleased {
        await withCheckedContinuation { continuation in
          if deleteReleased {
            continuation.resume()
          } else {
            deleteGate = continuation
          }
        }
      }
      return capacityResponse("", status: 404)
    case "GET":
      throw CancellationError()
    default:
      throw CancellationError()
    }
  }

  func waitForCreate() async {
    guard !createEntered else { return }
    await withCheckedContinuation { createWaiters.append($0) }
  }

  func openCreate() {
    guard !createReleased else { return }
    createReleased = true
    createGate?.resume()
    createGate = nil
  }

  func waitForDelete() async {
    guard !deleteEntered else { return }
    await withCheckedContinuation { deleteWaiters.append($0) }
  }

  func openDelete() {
    guard !deleteReleased else { return }
    deleteReleased = true
    deleteGate?.resume()
    deleteGate = nil
  }
}

private struct CancelledStartReturn: Sendable {
  let wasCancelled: Bool
  let capacity: ShapeSubscriptionCapacity.Snapshot
}

private actor CancelledStartBoundary {
  private var returned: CancelledStartReturn?

  func record(_ result: CancelledStartReturn) { returned = result }
  func result() -> CancelledStartReturn? { returned }
}

private func capacityResponse(_ body: String, status: Int = 200) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
      headerFields: nil)!)
}

private func capacityShapeResponse(id: String) -> HTTPResponse {
  capacityResponse(
    "{\"shapeId\":\"\(id)\",\"table\":\"public.items\",\"streamPath\":\"/v1/streams/\(id)\",\"streamUrl\":\"https://streams.test/\(id)\"}"
  )
}

private func capacityCoordinator(
  transport: any HTTPTransport,
  materializer: any ShapeMaterializer,
  capacity: ShapeSubscriptionCapacity,
  subscription: String,
  retryPolicy: ShapeSubscriptionRetryPolicy = .init(maxRetries: 0),
  clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock()
) -> ShapeSubscriptionCoordinator {
  ShapeSubscriptionCoordinator(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport),
    transport: transport,
    request: ShapeRequest(table: "public.items", subscription: subscription),
    materializer: materializer,
    retryPolicy: retryPolicy,
    clock: clock,
    capacity: capacity)
}

// The capacity qualification owns up to 1,000 cancellation-aware stream tasks. Swift Testing's
// process-wide runner must not overlap that lifecycle qualification with unrelated suites.
@Suite("Shape subscription capacity", .serialized)
struct SubscriptionCapacityTests {
  @Test func capacityClampsInvalidLimitToOne() async {
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 0)
    #expect(await capacity.maximumActiveSubscriptions == 1)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 0, rejected: 0))
  }

  @Test func capacityRejectsNPlusOneBeforeProviderPreflightOrCreate() async throws {
    let transport = CapacityTransport()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let first = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "first")
    let rejectedMaterializer = PreflightCountingMaterializer()
    let second = capacityCoordinator(
      transport: transport, materializer: rejectedMaterializer, capacity: capacity,
      subscription: "second")

    _ = try await first.start()
    await #expect(throws: ShapeSubscriptionFailure.capacityExceeded(limit: 1)) {
      _ = try await second.start()
    }

    #expect(await rejectedMaterializer.preflightCount() == 0)
    #expect(await transport.createRequests == 1)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 1))

    try await first.stop()
    try await second.stop()
  }

  @Test func concurrentStartsShareOnePermitAndJoinedStopReturnsItOnce() async throws {
    let transport = CapacityTransport()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let coordinator = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "shared")

    async let first: ShapeHandle = coordinator.start()
    async let second: ShapeHandle = coordinator.start()
    let (firstHandle, secondHandle) = try await (first, second)
    #expect(firstHandle == secondHandle)
    await transport.waitForStreamRequests(1)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 0))

    async let stopOne: Void = coordinator.stop()
    async let stopTwo: Void = coordinator.stop()
    _ = try await (stopOne, stopTwo)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
    #expect(await transport.releaseRequests == 1)
  }

  @Test func retrySleepersAreBoundedByAdmittedCoordinatorsAndCancellationReturnsPermits()
    async throws
  {
    let transport = RetryCapacityTransport()
    let clock = RetrySleepGate()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 2)
    let first = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "retry-1",
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(1), jitterRatio: 0),
      clock: clock)
    let second = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "retry-2",
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(1), jitterRatio: 0),
      clock: clock)
    let rejected = capacityCoordinator(
      transport: transport, materializer: PreflightCountingMaterializer(), capacity: capacity,
      subscription: "retry-3")

    let firstStart = Task { try await first.start() }
    let secondStart = Task { try await second.start() }
    await clock.waitForSleepers(2)
    #expect(await clock.sleeperCount() == 2)
    #expect(await capacity.snapshot() == .init(limit: 2, active: 2, admitted: 2, rejected: 0))

    await #expect(throws: ShapeSubscriptionFailure.capacityExceeded(limit: 2)) {
      _ = try await rejected.start()
    }
    #expect(await transport.createRequests == 2)

    try await first.stop()
    try await second.stop()
    await #expect(throws: CancellationError.self) { _ = try await firstStart.value }
    await #expect(throws: CancellationError.self) { _ = try await secondStart.value }
    #expect(await clock.sleeperCount() == 0)
    #expect(await capacity.snapshot() == .init(limit: 2, active: 0, admitted: 2, rejected: 1))
    try await rejected.stop()
  }

  @Test func preflightFailureAndCancelledStartReturnPermitBeforeAClaimCanExist() async throws {
    let transport = CapacityTransport()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let failed = capacityCoordinator(
      transport: transport, materializer: FailingPreflightMaterializer(), capacity: capacity,
      subscription: "failed-preflight")

    await #expect(throws: ShapeSubscriptionFailure.materializer) { _ = try await failed.start() }
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
    #expect(await transport.createRequests == 0)

    let blockedMaterializer = BlockingPreflightMaterializer()
    let cancelled = capacityCoordinator(
      transport: transport, materializer: blockedMaterializer, capacity: capacity,
      subscription: "cancelled-preflight")
    let start = Task { try await cancelled.start() }
    await blockedMaterializer.waitForEntry()
    start.cancel()
    await #expect(throws: CancellationError.self) { _ = try await start.value }
    try? await cancelled.stop()
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 2, rejected: 0))
    #expect(await transport.createRequests == 0)

    let reusable = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "reusable")
    _ = try await reusable.start()
    try await reusable.stop()
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 3, rejected: 0))
  }

  @Test func cancelledStartWaitsForLandedClaimReleaseAndPermitReturnBeforeItReturns() async throws {
    let transport = HeldCreateAndDeleteTransport()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let coordinator = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "cancelled-landed")
    let boundary = CancelledStartBoundary()
    let start = Task {
      let wasCancelled: Bool
      do {
        _ = try await coordinator.start()
        wasCancelled = false
      } catch is CancellationError {
        wasCancelled = true
      } catch {
        wasCancelled = false
      }
      await boundary.record(
        CancelledStartReturn(wasCancelled: wasCancelled, capacity: await capacity.snapshot()))
    }

    await transport.waitForCreate()
    start.cancel()
    await transport.openCreate()
    await transport.waitForDelete()

    // The delete is held. A cancellation result here would expose a still-owned claim and permit.
    #expect(await boundary.result() == nil)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 0))

    await transport.openDelete()
    await start.value
    let result = try #require(await boundary.result())
    #expect(result.wasCancelled)
    #expect(result.capacity == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
    #expect(await transport.deleteRequests == 1)
  }

  @Test func cancellingAJoinedWaiterAfterAnotherCallerReceivedTheClaimDoesNotStopIt() async throws {
    let transport = HeldCreateAndDeleteTransport()
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let coordinator = capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "joined-cancellation")

    // The first caller owns waiter ID 1 because it reaches the held create before the second task
    // exists. Hold only waiter 2 after the shared create so the first public `start()` can return
    // and acknowledge the claim before cancellation reaches the joined caller.
    let first = Task { try await coordinator.start() }
    await transport.waitForCreate()
    await coordinator.holdStartReturnForTesting(2)
    let second = Task { try await coordinator.start() }
    await coordinator.waitForInitialStartCallersForTesting(2)

    await transport.openCreate()
    _ = try await first.value
    await coordinator.waitForStartReturnGateForTesting(2)

    second.cancel()
    await coordinator.releaseStartReturnGateForTesting(2)
    await #expect(throws: CancellationError.self) { _ = try await second.value }

    // The received claim remains live: the joined caller's cancellation has no DELETE or permit
    // release side effect after another caller crossed the public-success boundary.
    #expect(await transport.deleteRequests == 0)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 0))

    let stop = Task { try await coordinator.stop() }
    await transport.waitForDelete()
    #expect(await capacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 0))
    await transport.openDelete()
    try await stop.value
    #expect(await transport.deleteRequests == 1)
    #expect(await capacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
  }

  @Test func terminalAndJoinedStopReturnTheirPermitOnlyAfterClaimRelease() async throws {
    let terminalTransport = TerminalCapacityTransport()
    let terminalCapacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let terminal = capacityCoordinator(
      transport: terminalTransport, materializer: InMemoryShapeMaterializer(),
      capacity: terminalCapacity,
      subscription: "terminal")
    let updates = await terminal.stateUpdates
    let terminalState = Task { () -> ShapeSubscriptionState in
      for await state in updates {
        if case .reseedRequired = state { return state }
      }
      return .idle
    }

    _ = try await terminal.start()
    guard case .reseedRequired = await terminalState.value else {
      Issue.record("terminal stream did not publish reseedRequired")
      return
    }
    #expect(
      await terminalCapacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
    #expect(await terminalTransport.releaseRequests == 1)
    try await terminal.stop()
    #expect(await terminalTransport.releaseRequests == 1)
    #expect(
      await terminalCapacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))

    let releaseTransport = ReleaseFailureCapacityTransport()
    let releaseCapacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    let release = capacityCoordinator(
      transport: releaseTransport, materializer: InMemoryShapeMaterializer(),
      capacity: releaseCapacity,
      subscription: "release")
    _ = try await release.start()
    await #expect(throws: ShapeSubscriptionFailure.self) { try await release.stop() }
    #expect(
      await releaseCapacity.snapshot() == .init(limit: 1, active: 1, admitted: 1, rejected: 0))
    try await release.stop()
    #expect(
      await releaseCapacity.snapshot() == .init(limit: 1, active: 0, admitted: 1, rejected: 0))
  }

  @Test func capacityQualificationRecordsOneTenOneHundredAndOneThousand() async throws {
    for count in [1, 10, 100, 1_000] {
      let receipt = try await qualifyCapacity(count: count)
      #expect(receipt.activeBeforeCleanup == count)
      #expect(receipt.rejected == 1)
      #expect(receipt.createRequests == count)
      #expect(receipt.releaseRequests == count)
      #expect(receipt.activeAfterCleanup == 0)
      print("subscription-capacity-qualification \(receipt)")
    }
  }
}

private struct CapacityQualificationReceipt: Equatable, Sendable, CustomStringConvertible {
  let count: Int
  let activeBeforeCleanup: Int
  let rejected: Int
  let createRequests: Int
  let releaseRequests: Int
  let activeAfterCleanup: Int

  var description: String {
    "count=\(count) active=\(activeBeforeCleanup) rejected=\(rejected) creates=\(createRequests) releases=\(releaseRequests) cleanupActive=\(activeAfterCleanup)"
  }
}

private func qualifyCapacity(count: Int) async throws -> CapacityQualificationReceipt {
  let transport = CapacityTransport()
  let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: count)
  let coordinators = (0..<count).map {
    capacityCoordinator(
      transport: transport, materializer: InMemoryShapeMaterializer(), capacity: capacity,
      subscription: "qualification-\($0)")
  }

  for coordinator in coordinators { _ = try await coordinator.start() }
  await transport.waitForStreamRequests(count)
  let before = await capacity.snapshot()

  let rejected = capacityCoordinator(
    transport: transport, materializer: PreflightCountingMaterializer(), capacity: capacity,
    subscription: "qualification-over-cap")
  await #expect(throws: ShapeSubscriptionFailure.capacityExceeded(limit: count)) {
    _ = try await rejected.start()
  }
  let afterRejection = await capacity.snapshot()

  for coordinator in coordinators { try await coordinator.stop() }
  try await rejected.stop()
  let afterCleanup = await capacity.snapshot()
  return CapacityQualificationReceipt(
    count: count,
    activeBeforeCleanup: before.active,
    rejected: afterRejection.rejected,
    createRequests: await transport.createRequests,
    releaseRequests: await transport.releaseRequests,
    activeAfterCleanup: afterCleanup.active)
}
