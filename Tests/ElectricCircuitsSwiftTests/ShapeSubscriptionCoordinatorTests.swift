import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor RecordingClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  func sleep(for duration: Duration) async throws {
    delays.append(duration)
  }
}

private actor HoldingClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  func sleep(for duration: Duration) async throws {
    delays.append(duration)
    try await Task.sleep(for: .seconds(3_600))
  }
}

private actor CoordinatorTransport: HTTPTransport {
  private var responses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []

  init(responses: [HTTPResponse]) { self.responses = responses }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    guard !responses.isEmpty else { throw CancellationError() }
    return responses.removeFirst()
  }
}

/// Holds the terminal stream response until the test explicitly releases it. This models a
/// terminal result that has not crossed the transport boundary yet; task yields cannot establish
/// that boundary.
private actor HeldEpochResetTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var terminalOpened = false
  private var streamRequestObserved = false
  private var releaseRequestObserved = false
  private var terminalGate: CheckedContinuation<Void, Never>?
  private var streamRequestWaiter: CheckedContinuation<Void, Never>?
  private var releaseRequestWaiter: CheckedContinuation<Void, Never>?

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      return shapeResponse()
    case "GET":
      streamRequestObserved = true
      streamRequestWaiter?.resume()
      streamRequestWaiter = nil
      try await withTaskCancellationHandler(
        operation: {
          if !terminalOpened {
            await withCheckedContinuation { terminalGate = $0 }
          }
          try Task.checkCancellation()
        },
        onCancel: { Task { await self.openTerminal() } })
      return response("reset", status: 410)
    case "DELETE":
      releaseRequestObserved = true
      releaseRequestWaiter?.resume()
      releaseRequestWaiter = nil
      return response("", status: 404)
    default:
      throw CancellationError()
    }
  }

  func waitForStreamRequest() async {
    guard !streamRequestObserved else { return }
    await withCheckedContinuation { streamRequestWaiter = $0 }
  }

  func openTerminal() {
    terminalOpened = true
    terminalGate?.resume()
    terminalGate = nil
  }

  func waitForReleaseRequest() async {
    guard !releaseRequestObserved else { return }
    await withCheckedContinuation { releaseRequestWaiter = $0 }
  }
}

private actor CancellationTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    if request.httpMethod == "POST" {
      return shapeResponse(leaseSeconds: nil)
    }
    if request.httpMethod == "DELETE" {
      return response("", status: 404)
    }
    while true {
      try Task.checkCancellation()
      await Task.yield()
    }
  }
}

private actor RenewalTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    switch request.httpMethod {
    case "POST": return shapeResponse()
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE": return response("", status: 404)
    default: throw CancellationError()
    }
  }
}

private actor ReleaseRetryTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var deletes = 0
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST": return shapeResponse()
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE":
      deletes += 1
      return deletes == 1 ? response("lost", status: 503) : response("", status: 404)
    default: throw CancellationError()
    }
  }
}

private actor LeaseTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST": return shapeResponse(leaseSeconds: 6)
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE": return response("", status: 404)
    default: throw CancellationError()
    }
  }
}

private actor ReplacementTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var posts = 0
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      posts += 1
      return shapeResponse(id: posts == 1 ? "s1" : "s2")
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE": return response("", status: 404)
    default: throw CancellationError()
    }
  }
}

private actor GatedRenewTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var posts = 0
  private var renewEntered = false
  private var gate: CheckedContinuation<Void, Never>?
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      posts += 1
      if posts == 1 { return shapeResponse() }
      renewEntered = true
      await withCheckedContinuation { gate = $0 }
      return shapeResponse()
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE": return response("", status: 404)
    default: throw CancellationError()
    }
  }
  func releaseRenew() {
    gate?.resume()
    gate = nil
  }
}

/// Separates a terminal stream result from an already accepted renew response. The renew deliberately
/// ignores cancellation until the test opens its gate, which makes the lifecycle ordering observable.
private actor TerminalDuringRenewTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var posts = 0
  private var terminalSent = false
  private var terminalOpened = false
  private var renewEntered = false
  private var renewCancelled = false
  private var terminalGate: CheckedContinuation<Void, Never>?
  private var renewGate: CheckedContinuation<Void, Never>?
  private var renewEnteredWaiter: CheckedContinuation<Void, Never>?
  private var renewCancelledWaiter: CheckedContinuation<Void, Never>?
  private var terminalSentWaiter: CheckedContinuation<Void, Never>?

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      posts += 1
      if posts == 1 { return shapeResponse() }
      renewEntered = true
      renewEnteredWaiter?.resume()
      renewEnteredWaiter = nil
      await withTaskCancellationHandler(
        operation: { await withCheckedContinuation { renewGate = $0 } },
        onCancel: { Task { await self.recordRenewCancellation() } })
      return shapeResponse()
    case "GET":
      if !terminalOpened {
        await withCheckedContinuation { terminalGate = $0 }
      }
      terminalSent = true
      terminalSentWaiter?.resume()
      terminalSentWaiter = nil
      return response("gone", status: 404)
    case "DELETE":
      return response("", status: 404)
    default:
      throw CancellationError()
    }
  }

  func openTerminal() {
    terminalOpened = true
    terminalGate?.resume()
    terminalGate = nil
  }

  func releaseRenew() {
    renewGate?.resume()
    renewGate = nil
  }

  private func recordRenewCancellation() {
    renewCancelled = true
    renewCancelledWaiter?.resume()
    renewCancelledWaiter = nil
  }

  func hasRenewEntered() -> Bool { renewEntered }
  func hasSentTerminal() -> Bool { terminalSent }
  func waitForRenewEntry() async {
    guard !renewEntered else { return }
    await withCheckedContinuation { renewEnteredWaiter = $0 }
  }
  func waitForRenewCancellation() async {
    guard !renewCancelled else { return }
    await withCheckedContinuation { renewCancelledWaiter = $0 }
  }
  func waitForTerminal() async {
    guard !terminalSent else { return }
    await withCheckedContinuation { terminalSentWaiter = $0 }
  }
}

private actor ExhaustedReleaseTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var deletes = 0

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      return shapeResponse()
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE":
      deletes += 1
      return deletes == 1 ? response("temporary", status: 503) : response("", status: 404)
    default:
      throw CancellationError()
    }
  }
}

private actor GatedCreateTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var gate: CheckedContinuation<Void, Never>?
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    if request.httpMethod == "POST" {
      await withTaskCancellationHandler(
        operation: { await withCheckedContinuation { gate = $0 } },
        onCancel: { Task { await self.releaseCreate() } })
      return shapeResponse()
    }
    if request.httpMethod == "DELETE" { return response("", status: 404) }
    throw CancellationError()
  }
  func releaseCreate() {
    gate?.resume()
    gate = nil
  }
}

private actor GatedMaterializer: ShapeMaterializer {
  private var entered = false
  private var released = false
  private var cursor: StreamCursor?
  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    if self.cursor == cursor { return }
    guard self.cursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: self.cursor, advancingTo: cursor)
    }
    entered = true
    while !released { await Task.yield() }
    self.cursor = cursor
  }
  func currentCursor() async throws -> StreamCursor? { cursor }
  func hasEntered() -> Bool { entered }
  func releaseApply() { released = true }
}

private enum UnavailableStore: Error, Sendable {
  case locked
}

/// Models the one operation every durable provider already has to perform before a reader can
/// safely start: loading its committed cursor. The initial red test demonstrates that the old
/// coordinator claimed server-side state before discovering this local failure.
private actor UnavailableCursorMaterializer: ShapeMaterializer {
  func currentCursor() async throws -> StreamCursor? { throw UnavailableStore.locked }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {
    throw UnavailableStore.locked
  }
}

private actor ProtectedDataUnavailableMaterializer: ShapeMaterializer {
  func currentCursor() async throws -> StreamCursor? {
    throw MaterializerAvailabilityError.protectedDataUnavailable
  }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {
    throw MaterializerAvailabilityError.protectedDataUnavailable
  }
}

private actor AvailabilityDropsAfterClaimMaterializer: ShapeMaterializer {
  private var cursorReads = 0
  private var enteredContinuation: CheckedContinuation<Void, Never>?
  private var failureContinuation: CheckedContinuation<Void, Never>?
  private var failureOpened = false

  func currentCursor() async throws -> StreamCursor? {
    cursorReads += 1
    if cursorReads == 1 { return nil }
    enteredContinuation?.resume()
    enteredContinuation = nil
    if !failureOpened {
      await withCheckedContinuation { failureContinuation = $0 }
    }
    throw MaterializerAvailabilityError.protectedDataUnavailable
  }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {
    throw MaterializerAvailabilityError.protectedDataUnavailable
  }

  func waitForPostClaimCursorRead() async {
    guard cursorReads < 2 else { return }
    await withCheckedContinuation { enteredContinuation = $0 }
  }

  func releaseAsProtectedDataUnavailable() {
    failureOpened = true
    failureContinuation?.resume()
    failureContinuation = nil
  }
}

private func response(_ body: String, status: Int = 200, headers: [String: String] = [:])
  -> HTTPResponse
{
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
      headerFields: headers)!)
}

private func shapeResponse(id: String = "s1", leaseSeconds: Int? = nil) -> HTTPResponse {
  let lease = leaseSeconds.map { ",\"leaseSeconds\":\($0)" } ?? ""
  return response(
    "{\"shapeId\":\"\(id)\",\"table\":\"public.items\",\"streamPath\":\"/v1/streams/\(id)\",\"streamUrl\":\"https://streams.test/\(id)\"\(lease)}"
  )
}

private let oneRow =
  "[{\"type\":\"public.items\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"

@Suite("Shape subscription coordinator")
struct ShapeSubscriptionCoordinatorTests {
  @Test func terminalFencesAcceptedRenewAndCompensatesStableClaim() async throws {
    let transport = TerminalDuringRenewTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())
    let updates = await coordinator.stateUpdates
    let reseedPublication = Task { () throws -> ShapeSubscriptionState in
      for await update in updates {
        if case .reseedRequired = update { return update }
      }
      throw CancellationError()
    }

    _ = try await coordinator.start()
    let renewal = Task { try await coordinator.renew() }
    await transport.waitForRenewEntry()
    #expect(await transport.hasRenewEntered())
    await transport.openTerminal()
    await transport.waitForTerminal()
    #expect(await transport.hasSentTerminal())
    // The coordinator has observed the terminal result only after it cancels the accepted renew.
    // This named gate establishes that generation fence before the transport returns its response.
    await transport.waitForRenewCancellation()
    await transport.releaseRenew()

    await #expect(throws: CancellationError.self) { _ = try await renewal.value }
    let published = try await reseedPublication.value
    #expect(
      published
        == .reseedRequired(
          .init(
            reason: .terminal(.notFound),
            previous: ShapeHandle(
              response: ShapeResponse(
                shapeId: "s1", table: "public.items", streamPath: "/v1/streams/s1",
                streamURL: URL(string: "https://streams.test/s1")!,
                subscription: "stable-claim")))))
    #expect(await coordinator.state == published)
    let deletes = await transport.requests.filter { $0.httpMethod == "DELETE" }
    #expect(deletes.count == 1)
    #expect(
      deletes.allSatisfy {
        URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems
          == [URLQueryItem(name: "subscription", value: "stable-claim")]
      })
  }

  @Test func exhaustedReleaseRetainsClaimForLaterStopRetry() async throws {
    let transport = ExhaustedReleaseTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())

    _ = try await coordinator.start()
    await #expect(throws: ShapeSubscriptionFailure.self) { try await coordinator.stop() }
    try await coordinator.stop()

    #expect(await coordinator.state == .stopped)
    let deletes = await transport.requests.filter { $0.httpMethod == "DELETE" }
    #expect(deletes.count == 2)
    #expect(
      deletes.allSatisfy {
        URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems
          == [URLQueryItem(name: "subscription", value: "stable-claim")]
      })
  }

  @Test func leaseRenewalUsesOneThirdCadence() async throws {
    let transport = LeaseTransport()
    let clock = HoldingClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0), clock: clock)
    _ = try await coordinator.start()
    for _ in 0..<100 {
      if await clock.delays.count == 1 { break }
      await Task.yield()
    }
    #expect(await clock.delays == [.seconds(2)])
    try await coordinator.stop()
  }

  @Test func releaseRetriesResponseLossAndTreats404AsSuccess() async throws {
    let transport = ReleaseRetryTransport()
    let clock = RecordingClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(1), jitterRatio: 0),
      clock: clock)

    _ = try await coordinator.start()
    for _ in 0..<100 {
      if await transport.requests.count >= 2 { break }
      await Task.yield()
    }
    try await coordinator.stop()
    #expect(await coordinator.state == .stopped)
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "GET", "DELETE", "DELETE"])
    let deletes = await transport.requests.filter { $0.httpMethod == "DELETE" }
    #expect(
      deletes.allSatisfy {
        URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems
          == [URLQueryItem(name: "subscription", value: "stable-claim")]
      })
    #expect(await clock.delays == [.seconds(1)])
  }

  @Test func replacementIsCompensatedAndReportsReseedRequired() async throws {
    let transport = ReplacementTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())

    _ = try await coordinator.start()
    let replacement = ShapeSubscriptionReseedRequired(
      reason: .replacement(previousShapeID: "s1", replacementShapeID: "s2"),
      previous: ShapeHandle(
        response: ShapeResponse(
          shapeId: "s1", table: "public.items", streamPath: "/v1/streams/s1",
          streamURL: URL(string: "https://streams.test/s1")!, subscription: "stable-claim")),
      replacement: ShapeHandle(
        response: ShapeResponse(
          shapeId: "s2", table: "public.items", streamPath: "/v1/streams/s2",
          streamURL: URL(string: "https://streams.test/s2")!, subscription: "stable-claim")))
    await #expect(throws: ShapeSubscriptionFailure.reseedRequired(replacement)) {
      _ = try await coordinator.renew()
    }
    #expect(await coordinator.state == .reseedRequired(replacement))
    let deletes = await transport.requests.filter { $0.httpMethod == "DELETE" }
    #expect(deletes.count == 2)
    try await coordinator.stop()
  }

  @Test func stopWaitsForRenewResponseAndCompensatesLandedClaim() async throws {
    let transport = GatedRenewTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())
    _ = try await coordinator.start()
    let renew = Task { try await coordinator.renew() }
    for _ in 0..<100 {
      if await transport.requests.count >= 2 { break }
      await Task.yield()
    }
    let stop = Task { try await coordinator.stop() }
    for _ in 0..<100 {
      if await coordinator.state == .stopping { break }
      await Task.yield()
    }
    #expect(await coordinator.state == .stopping)
    await transport.releaseRenew()
    _ = try await stop.value
    await #expect(throws: CancellationError.self) { _ = try await renew.value }
    #expect(await coordinator.state == .stopped)
    #expect((await transport.requests.filter { $0.httpMethod == "DELETE" }).count == 1)
  }

  @Test func stopWaitsForMaterializerApplyBeforeStopped() async throws {
    let body = oneRow
    let transport = CoordinatorTransport(responses: [
      shapeResponse(), response(body, headers: ["stream-next-offset": "7"]),
      response("", status: 404),
    ])
    let materializer = GatedMaterializer()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: materializer, retryPolicy: .init(maxRetries: 0), clock: RecordingClock())
    _ = try await coordinator.start()
    for _ in 0..<100 {
      if await materializer.hasEntered() { break }
      await Task.yield()
    }
    #expect(await materializer.hasEntered())
    let stop = Task { try await coordinator.stop() }
    for _ in 0..<100 {
      if await coordinator.state == .stopping { break }
      await Task.yield()
    }
    #expect(await coordinator.state == .stopping)
    await materializer.releaseApply()
    try await stop.value
    #expect(try await materializer.currentCursor() == StreamCursor(offset: "7"))
    #expect(await coordinator.state == .stopped)
  }

  @Test func concurrentStartsCancelledByStopReleaseOnlyLandedClaim() async throws {
    let transport = GatedCreateTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())
    let first = Task { try await coordinator.start() }
    let second = Task { try await coordinator.start() }
    for _ in 0..<100 {
      if await transport.requests.count == 1 { break }
      await Task.yield()
    }
    let stop = Task { try await coordinator.stop() }
    _ = try await stop.value
    await #expect(throws: CancellationError.self) { _ = try await first.value }
    await #expect(throws: CancellationError.self) { _ = try await second.value }
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "DELETE"])
    #expect(await coordinator.state == .stopped)
  }

  @Test func initialCreateRetriesStableSubscriptionAfterTransientResponseLoss() async throws {
    let transport = CoordinatorTransport(responses: [
      response("temporary", status: 503),
      shapeResponse(),
      response("gone", status: 404),
      response("", status: 404),
    ])
    let clock = RecordingClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "stable-claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(1), jitterRatio: 0),
      clock: clock)
    let stateUpdates = await coordinator.stateUpdates
    let terminalState = Task { () -> ShapeSubscriptionState in
      for await state in stateUpdates {
        if case .reseedRequired = state { return state }
      }
      return .idle
    }

    _ = try await coordinator.start()
    #expect(await clock.delays == [.seconds(1)])
    let observedTerminal = await terminalState.value
    #expect(
      {
        if case .reseedRequired = observedTerminal { return true }
        return false
      }())
    let requests = await transport.requests
    #expect(requests.prefix(2).map(\.httpMethod) == ["POST", "POST"])
    #expect(
      requests.prefix(2).allSatisfy {
        $0.httpBody.flatMap { String(data: $0, encoding: .utf8) }?.contains("stable-claim") == true
      })
    let deletes = requests.filter { $0.httpMethod == "DELETE" }
    #expect(deletes.count == 1)
    #expect(
      URLComponents(url: deletes[0].url!, resolvingAgainstBaseURL: false)?.queryItems
        == [URLQueryItem(name: "subscription", value: "stable-claim")])
    try await coordinator.stop()
  }

  @Test func retryUsesBoundedBackoffAndResumesFromCommittedCursor() async throws {
    let transport = CoordinatorTransport(responses: [
      shapeResponse(),
      response("temporary", status: 503),
      response(oneRow, headers: ["stream-next-offset": "7"]),
      response("gone", status: 404),
      response("", status: 404),
    ])
    let clock = RecordingClock()
    let materializer = InMemoryShapeMaterializer()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: materializer,
      retryPolicy: .init(
        maxRetries: 2, baseDelay: .seconds(1), maximumDelay: .seconds(2), jitterRatio: 0,
        randomUnit: { 0.5 }),
      clock: clock)

    _ = try await coordinator.start()
    for _ in 0..<1_000 {
      if await transport.requests.count >= 5 { break }
      await Task.yield()
    }
    #expect(await transport.requests.count == 5)
    for _ in 0..<1_000 {
      if case .reseedRequired = await coordinator.state { break }
      await Task.yield()
    }
    #expect(
      await coordinator.state
        == .reseedRequired(
          .init(
            reason: .terminal(.notFound),
            previous: ShapeHandle(
              response: ShapeResponse(
                shapeId: "s1", table: "public.items", streamPath: "/v1/streams/s1",
                streamURL: URL(string: "https://streams.test/s1")!, subscription: "claim")))))
    #expect(await materializer.cursor() == StreamCursor(offset: "7"))
    #expect(await clock.delays == [.seconds(1)])
    let requests = await transport.requests
    #expect(requests.map(\.httpMethod) == ["POST", "GET", "GET", "GET", "DELETE"])
    #expect(requests[1].url?.query?.contains("offset=-1") == true)
    #expect(requests[2].url?.query?.contains("offset=-1") == true)
    #expect(requests[3].url?.query?.contains("offset=7") == true)
    try await coordinator.stop()
  }

  @Test func epochResetIsTerminalAndIsNotRetried() async throws {
    let transport = HeldEpochResetTransport()
    let clock = RecordingClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(maxRetries: 4, baseDelay: .seconds(1), jitterRatio: 0),
      clock: clock)
    let updates = await coordinator.stateUpdates
    let terminalState = Task { () -> ShapeSubscriptionState in
      for await state in updates {
        if case .reseedRequired = state { return state }
      }
      return .idle
    }

    _ = try await coordinator.start()
    await transport.waitForStreamRequest()
    // The named response gate, rather than a number of scheduler yields, establishes that the
    // terminal result may be consumed. `reseedRequired` is published only after its DELETE has
    // succeeded, so these two observations fence both terminal handling and claim release.
    await transport.openTerminal()
    await transport.waitForReleaseRequest()
    let observedTerminal = await terminalState.value
    #expect(
      observedTerminal
        == .reseedRequired(
          .init(
            reason: .terminal(.gone),
            previous: ShapeHandle(
              response: ShapeResponse(
                shapeId: "s1", table: "public.items", streamPath: "/v1/streams/s1",
                streamURL: URL(string: "https://streams.test/s1")!, subscription: "claim")))))
    #expect(await coordinator.state == observedTerminal)
    try await coordinator.stop()

    #expect(await transport.requests.count == 3)
    #expect(await clock.delays.isEmpty)
  }

  @Test func renewalKeepsStableShapeAndReleaseIsOneShot() async throws {
    let transport = RenewalTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(maxRetries: 0),
      clock: RecordingClock())

    let handle = try await coordinator.start()
    #expect(try await coordinator.renew().id == handle.id)
    try await coordinator.stop()
    try await coordinator.stop()
    let requests = await transport.requests
    #expect(requests.map(\.httpMethod).filter { $0 == "DELETE" }.count >= 1)
  }

  @Test func stopCancelsLongPollAndStillReleasesClaim() async throws {
    let transport = CancellationTransport()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(maxRetries: 0))

    _ = try await coordinator.start()
    for _ in 0..<100 {
      if await transport.requests.count >= 2 { break }
      await Task.yield()
    }
    try await coordinator.stop()
    #expect(await coordinator.state == .stopped)
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "GET", "DELETE"])
  }

  @Test func unavailableDurableCursorFailsBeforeCreatingAServerClaim() async throws {
    let transport = CoordinatorTransport(responses: [shapeResponse()])
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: UnavailableCursorMaterializer(), retryPolicy: .init(maxRetries: 0))

    await #expect(throws: ShapeSubscriptionFailure.materializer) {
      _ = try await coordinator.start()
    }
    #expect(await transport.requests.isEmpty)
    #expect(await coordinator.state == .failed(.materializer))
  }

  @Test func protectedDataAvailabilityIsTypedAndRedactedBeforeAnyClaim() async throws {
    let transport = CoordinatorTransport(responses: [shapeResponse()])
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: ProtectedDataUnavailableMaterializer(), retryPolicy: .init(maxRetries: 0))

    await #expect(
      throws: ShapeSubscriptionFailure.materializerUnavailable(.protectedDataUnavailable)
    ) {
      _ = try await coordinator.start()
    }
    #expect(await transport.requests.isEmpty)
    #expect(
      await coordinator.state
        == .failed(.materializerUnavailable(.protectedDataUnavailable)))
  }

  @Test func postClaimAvailabilityLossRemainsTypedUntilCallerStopsAndReleases() async throws {
    let transport = CoordinatorTransport(responses: [shapeResponse(), response("", status: 404)])
    let materializer = AvailabilityDropsAfterClaimMaterializer()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: materializer, retryPolicy: .init(maxRetries: 0))
    let updates = await coordinator.stateUpdates
    let failed = Task { () throws -> ShapeSubscriptionState in
      for await update in updates {
        if case .failed = update { return update }
      }
      throw CancellationError()
    }

    _ = try await coordinator.start()
    await materializer.waitForPostClaimCursorRead()
    #expect(await transport.requests.map(\.httpMethod) == ["POST"])
    await materializer.releaseAsProtectedDataUnavailable()

    #expect(
      try await failed.value
        == .failed(.materializerUnavailable(.protectedDataUnavailable)))
    // The coordinator deliberately does not autonomously release a caller-owned lifecycle. The
    // teardown owner remains responsible for stop, which joins the failed reader then releases.
    try await coordinator.stop()
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "DELETE"])
  }

  @Test func retryPolicyAddsJitterButNeverExceedsMaximum() {
    let policy = ShapeSubscriptionRetryPolicy(
      maxRetries: 5, baseDelay: .seconds(2), maximumDelay: .seconds(3), jitterRatio: 1,
      randomUnit: { 0 })
    #expect(policy.delay(forRetry: 1) == .seconds(0))
    #expect(policy.delay(forRetry: 4) <= .seconds(3))
  }
}
