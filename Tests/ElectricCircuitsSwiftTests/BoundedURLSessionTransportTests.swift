import Foundation
@preconcurrency import Network
import Testing

@testable import ElectricCircuitsSwift

private actor BoundedTransportGate {
  private var opened = false
  private var waiters: [UUID: CheckedContinuation<Void, Error>] = [:]
  private var cancelledWaiters: Set<UUID> = []

  func open() {
    guard !opened else { return }
    opened = true
    let waiting = waiters.values
    waiters.removeAll()
    for waiter in waiting { waiter.resume() }
  }

  func wait() async throws {
    guard !opened else { return }
    let id = UUID()
    try Task.checkCancellation()
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { continuation in
        if opened {
          continuation.resume()
        } else if cancelledWaiters.remove(id) != nil {
          continuation.resume(throwing: CancellationError())
        } else {
          waiters[id] = continuation
        }
      }
    } onCancel: {
      Task { await self.cancelWait(id) }
    }
  }

  private func cancelWait(_ id: UUID) {
    if let waiter = waiters.removeValue(forKey: id) {
      waiter.resume(throwing: CancellationError())
    } else {
      // Cancellation may win the race with continuation registration. Remember it so the
      // continuation is never installed after its task has already been cancelled.
      cancelledWaiters.insert(id)
    }
  }
}

private enum FixtureLivenessError: Error, Equatable, Sendable {
  case deadlineExceeded(String)
  case requestFinishedBefore(String)
}

private enum FixtureWaitOutcome: Equatable, Sendable {
  case event
  case requestTerminal
  case deadline
}

/// Waits are causally established by a named receipt. The sleep is only a diagnostic liveness
/// deadline; it is never used to establish ordering or successful completion.
private func awaitFixtureGate(
  _ gate: BoundedTransportGate, named name: String, deadline: Duration = .seconds(2)
) async throws {
  try await withThrowingTaskGroup(of: FixtureWaitOutcome.self) { group in
    group.addTask {
      try await gate.wait()
      return .event
    }
    group.addTask {
      try await Task.sleep(for: deadline)
      return .deadline
    }
    defer { group.cancelAll() }
    guard let outcome = try await group.next() else {
      throw FixtureLivenessError.deadlineExceeded(name)
    }
    guard outcome == .event else { throw FixtureLivenessError.deadlineExceeded(name) }
  }
}

private func awaitFixtureGate(
  _ gate: BoundedTransportGate, orRequestTerminal terminal: BoundedTransportGate,
  named name: String,
  deadline: Duration = .seconds(2)
) async throws {
  try await withThrowingTaskGroup(of: FixtureWaitOutcome.self) { group in
    group.addTask {
      try await gate.wait()
      return .event
    }
    group.addTask {
      try await terminal.wait()
      return .requestTerminal
    }
    group.addTask {
      try await Task.sleep(for: deadline)
      return .deadline
    }
    defer { group.cancelAll() }
    guard let outcome = try await group.next() else {
      throw FixtureLivenessError.deadlineExceeded(name)
    }
    switch outcome {
    case .event:
      return
    case .requestTerminal:
      throw FixtureLivenessError.requestFinishedBefore(name)
    case .deadline:
      throw FixtureLivenessError.deadlineExceeded(name)
    }
  }
}

private struct ObservedRequest<Value: Sendable>: Sendable {
  let task: Task<Value, Error>
  let terminal: BoundedTransportGate
}

/// Guarantees that each test-owned request is cancelled and joined on every exit path. The terminal
/// receipt lets a loopback wait fail at the request's real terminal point instead of waiting for a
/// server event that cannot occur.
private func withObservedRequest<Value: Sendable, Result: Sendable>(
  _ operation: @escaping @Sendable () async throws -> Value,
  _ body: (ObservedRequest<Value>) async throws -> Result
) async throws -> Result {
  let terminal = BoundedTransportGate()
  let task = Task {
    do {
      let value = try await operation()
      await terminal.open()
      return value
    } catch {
      await terminal.open()
      throw error
    }
  }
  let request = ObservedRequest(task: task, terminal: terminal)
  do {
    let result = try await body(request)
    task.cancel()
    _ = try? await task.value
    return result
  } catch {
    task.cancel()
    _ = try? await task.value
    throw error
  }
}

private actor ChunkedOverflowServer {
  private let listener: NWListener
  private let ready = BoundedTransportGate()
  private let requestArrived = BoundedTransportGate()
  private let initialChunkSent = BoundedTransportGate()
  private let overflowChunkSent = BoundedTransportGate()
  private let listenerStopped = BoundedTransportGate()
  private var connection: NWConnection?
  private var connectionStopped: BoundedTransportGate?
  private var sawRequest = false

  init() throws {
    let listener = try NWListener(using: .tcp, on: .any)
    self.listener = listener
    listener.stateUpdateHandler = { [ready, listenerStopped] state in
      switch state {
      case .ready:
        Task { await ready.open() }
      case .failed, .cancelled:
        Task { await listenerStopped.open() }
      default:
        break
      }
    }
    listener.newConnectionHandler = { [weak self] connection in
      Task { await self?.accept(connection) }
    }
    listener.start(queue: .global(qos: .userInitiated))
  }

  func endpoint() async throws -> URL {
    try await awaitFixtureGate(ready, named: "loopback listener ready")
    return URL(string: "http://127.0.0.1:\(listener.port!.rawValue)")!
  }

  func waitForRequest(until terminal: BoundedTransportGate) async throws {
    try await awaitFixtureGate(
      requestArrived, orRequestTerminal: terminal, named: "loopback request arrival")
  }
  func waitForInitialChunk(until terminal: BoundedTransportGate) async throws {
    try await awaitFixtureGate(
      initialChunkSent, orRequestTerminal: terminal, named: "loopback initial chunk send")
  }
  func waitForOverflowChunk(until terminal: BoundedTransportGate) async throws {
    try await awaitFixtureGate(
      overflowChunkSent, orRequestTerminal: terminal, named: "loopback overflow chunk send")
  }

  func sendInitialExactLimitChunk() {
    send(
      Data(
        "HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\nConnection: close\r\n\r\n5\r\nabcde\r\n"
          .utf8)
    ) {
      await self.initialChunkSent.open()
    }
  }

  func sendOverflowChunk() {
    send(Data("1\r\nx\r\n0\r\n\r\n".utf8)) { await self.overflowChunkSent.open() }
  }

  func finishExactLimitResponse() {
    send(Data("0\r\n\r\n".utf8)) {}
  }

  func stop() async throws {
    connection?.cancel()
    if let connectionStopped {
      try await awaitFixtureGate(connectionStopped, named: "loopback connection shutdown")
    }
    listener.cancel()
    try await awaitFixtureGate(listenerStopped, named: "loopback listener shutdown")
  }

  private func accept(_ connection: NWConnection) {
    self.connection = connection
    let connectionStopped = BoundedTransportGate()
    self.connectionStopped = connectionStopped
    connection.stateUpdateHandler = { state in
      if case .failed = state {
        Task { await connectionStopped.open() }
      } else if case .cancelled = state {
        Task { await connectionStopped.open() }
      }
    }
    connection.start(queue: .global(qos: .userInitiated))
    receiveRequest(on: connection, buffered: Data())
  }

  private func receiveRequest(on connection: NWConnection, buffered: Data) {
    connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) {
      [weak self] data, _, complete, error in
      Task {
        await self?.receivedRequest(
          on: connection, buffered: buffered, data: data, complete: complete, error: error)
      }
    }
  }

  private func receivedRequest(
    on connection: NWConnection, buffered: Data, data: Data?, complete: Bool, error: NWError?
  ) {
    var next = buffered
    if let data { next.append(data) }
    if !sawRequest, next.range(of: Data("\r\n\r\n".utf8)) != nil {
      sawRequest = true
      Task { await requestArrived.open() }
      return
    }
    if !(complete || error != nil) { receiveRequest(on: connection, buffered: next) }
  }

  private func send(_ data: Data, completion: @escaping @Sendable () async -> Void) {
    connection?.send(
      content: data, completion: .contentProcessed { _ in Task { await completion() } })
  }
}

private actor TransportOverflow: HTTPTransport {
  func send(_: URLRequest) async throws -> HTTPResponse {
    throw HTTPTransportError.responseTooLarge(limit: 5, observed: 6)
  }
}

private actor TransportBoundMaterializer: ShapeMaterializer {
  private var checkpoint: StreamCursor?
  private(set) var applications = 0

  init(cursor: StreamCursor?) { checkpoint = cursor }

  func currentCursor() async throws -> StreamCursor? { checkpoint }
  func apply(
    _: ChangeBatch, expecting _: StreamCursor?, advancingTo _: StreamCursor
  ) async throws { applications += 1 }
}

private struct StaticProtocolPlan: Sendable {
  let status: Int
  let headers: [String: String]
  let body: Data
  let finishes: Bool
}

private actor StaticProtocolReceipt {
  let requestStarted = BoundedTransportGate()
  let stopLoadingCalled = BoundedTransportGate()
  private var stopCalls = 0

  func recordRequestStarted() async { await requestStarted.open() }
  func recordStopLoading() async {
    stopCalls += 1
    await stopLoadingCalled.open()
  }
  func stopCallCount() -> Int { stopCalls }
}

private struct StaticProtocolConfiguration: Sendable {
  let plan: StaticProtocolPlan
  let receipt: StaticProtocolReceipt
}

/// This URLProtocol fixture has one locked response script per configuration generation. Its
/// unchecked Sendable conformance is limited to the lock-protected current configuration; each
/// callback captures that generation's actor-isolated receipt before doing any response work.
private final class StaticProtocolState: @unchecked Sendable {
  private let lock = NSLock()
  private var configuration = StaticProtocolConfiguration(
    plan: .init(status: 200, headers: [:], body: Data(), finishes: true),
    receipt: StaticProtocolReceipt())

  func configure(_ plan: StaticProtocolPlan) -> StaticProtocolReceipt {
    let receipt = StaticProtocolReceipt()
    lock.withLock {
      configuration = .init(plan: plan, receipt: receipt)
    }
    return receipt
  }

  func snapshot() -> StaticProtocolConfiguration { lock.withLock { configuration } }
}

private final class StaticResponseURLProtocol: URLProtocol, @unchecked Sendable {
  static let state = StaticProtocolState()
  private var receipt: StaticProtocolReceipt?

  override class func canInit(with request: URLRequest) -> Bool {
    request.url?.host == "bounded-static.test"
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

  override func startLoading() {
    guard let client, let url = request.url else { return }
    let configuration = Self.state.snapshot()
    receipt = configuration.receipt
    let plan = configuration.plan
    client.urlProtocol(
      self,
      didReceive: HTTPURLResponse(
        url: url, statusCode: plan.status, httpVersion: "HTTP/1.1", headerFields: plan.headers)!,
      cacheStoragePolicy: .notAllowed)
    if !plan.body.isEmpty { client.urlProtocol(self, didLoad: plan.body) }
    if plan.finishes { client.urlProtocolDidFinishLoading(self) }
    Task { await configuration.receipt.recordRequestStarted() }
  }

  override func stopLoading() {
    if let receipt {
      Task { await receipt.recordStopLoading() }
    }
  }
}

private func staticSession() -> URLSession {
  let configuration = URLSessionConfiguration.ephemeral
  configuration.protocolClasses = [StaticResponseURLProtocol.self]
  return URLSession(configuration: configuration)
}

/// Runs an operation with a real loopback listener and closes its connection/listener on both
/// the success and throwing paths. This keeps a failed assertion from turning into a leaked
/// Network.framework callback that can hold the Swift Testing helper open.
private func withChunkedServer<Result: Sendable>(
  _ operation: (ChunkedOverflowServer) async throws -> Result
) async throws -> Result {
  let server = try ChunkedOverflowServer()
  do {
    let result = try await operation(server)
    try await server.stop()
    return result
  } catch {
    try? await server.stop()
    throw error
  }
}

@Suite("bounded URLSession transport", .serialized)
struct BoundedURLSessionTransportTests {
  @Test func chunkedExactLimitResponseSucceedsWithoutIncreasingTheCeiling() async throws {
    try await withChunkedServer { server in
      let session = URLSession(configuration: .ephemeral)
      defer { session.invalidateAndCancel() }
      let endpoint = try await server.endpoint()
      let request = URLRequest(url: endpoint.appending(path: "exact"))
      try await withObservedRequest(
        { try await URLSessionTransport(session: session).send(request, maximumResponseBytes: 5) }
      ) { send in
        try await server.waitForRequest(until: send.terminal)
        await server.sendInitialExactLimitChunk()
        try await server.waitForInitialChunk(until: send.terminal)
        await server.finishExactLimitResponse()
        let response = try await send.task.value
        #expect(response.data == Data("abcde".utf8))
      }
    }
  }

  @Test func chunkedUnknownLengthResponseMapsControlOverflowAtLimitPlusOne() async throws {
    try await withChunkedServer { server in
      let session = URLSession(configuration: .ephemeral)
      defer { session.invalidateAndCancel() }
      let endpoint = try await server.endpoint()
      let client = ElectricCircuitsClient(
        baseURL: endpoint, transport: URLSessionTransport(session: session),
        responseDecodingLimits: .init(
          maximumDecodedResponseBytes: 5, maximumChangeEventsPerStreamBatch: 1))
      try await withObservedRequest({
        try await client.querySubset(SubsetQuery(table: "public.items"))
      }) {
        request in
        try await server.waitForRequest(until: request.terminal)
        await server.sendInitialExactLimitChunk()
        try await server.waitForInitialChunk(until: request.terminal)
        await server.sendOverflowChunk()
        try await server.waitForOverflowChunk(until: request.terminal)
        await #expect(throws: ClientError.responseTooLarge(limit: 5, observed: 6)) {
          try await request.task.value
        }
      }
    }
  }

  @Test func loopbackWaitSurfacesAnEarlyRequestTerminalReceipt() async throws {
    let _: Void = try await withChunkedServer { server in
      let _: Void = try await withObservedRequest({ () throws -> Void in
        throw URLError(.cannotConnectToHost)
      }) {
        request in
        await #expect(
          throws: FixtureLivenessError.requestFinishedBefore("loopback request arrival")
        ) {
          try await server.waitForRequest(until: request.terminal)
        }
      }
    }
  }

  @Test func transportOverflowMapsToStreamFailureBeforeProviderApplyOrCursorMovement() async throws
  {
    let cursor = StreamCursor(offset: "41")
    let materializer = TransportBoundMaterializer(cursor: cursor)
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://engine.test/shape/s1")!, transport: TransportOverflow(),
      materializer: materializer, startingAt: cursor,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 5, maximumChangeEventsPerStreamBatch: 1))

    await #expect(throws: StreamError.responseTooLarge(limit: 5, observed: 6)) {
      try await reader.run()
    }
    #expect(try await materializer.currentCursor() == cursor)
    #expect(await materializer.applications == 0)
  }

  @Test func knownSuccessfulContentLengthIsRejectedBeforeAdmission() async throws {
    _ = StaticResponseURLProtocol.state.configure(
      .init(
        status: 200, headers: ["Content-Length": "6"], body: Data("abcdef".utf8), finishes: true))
    let request = URLRequest(url: URL(string: "https://bounded-static.test/preflight")!)
    let session = staticSession()
    defer { session.invalidateAndCancel() }
    await #expect(throws: HTTPTransportError.responseTooLarge(limit: 5, observed: 6)) {
      try await URLSessionTransport(session: session).send(request, maximumResponseBytes: 5)
    }
  }

  @Test func hugeNonSuccessBodyPreservesStatusRetryAfterAndIsNeverReturned() async throws {
    _ = StaticResponseURLProtocol.state.configure(
      .init(
        status: 429, headers: ["Retry-After": "3"], body: Data(repeating: 0x78, count: 64 * 1024),
        finishes: true))
    let session = staticSession()
    defer { session.invalidateAndCancel() }
    let transport = URLSessionTransport(session: session)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://bounded-static.test")!, transport: transport,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 5, maximumChangeEventsPerStreamBatch: 1))
    await #expect(throws: ClientError.retryableHTTP(status: 429, retryAfter: .seconds(3))) {
      try await client.querySubset(SubsetQuery(table: "public.items"))
    }
  }

  @Test func headAndNoContentResponsesRemainStatusOnly() async throws {
    for (method, status) in [("HEAD", 200), ("GET", 204)] {
      _ = StaticResponseURLProtocol.state.configure(
        .init(
          status: status, headers: [:], body: Data(repeating: 0x78, count: 4_096), finishes: true))
      var request = URLRequest(url: URL(string: "https://bounded-static.test/status-only")!)
      request.httpMethod = method
      let session = staticSession()
      defer { session.invalidateAndCancel() }
      let response = try await URLSessionTransport(session: session).send(
        request, maximumResponseBytes: 5)
      #expect(response.response.statusCode == status)
      #expect(response.data.isEmpty)
    }
  }

  @Test func callerCancellationStopsTheSessionTaskAndReturnsCancellation() async throws {
    let receipt = StaticResponseURLProtocol.state.configure(
      .init(status: 200, headers: [:], body: Data(), finishes: false))
    let request = URLRequest(url: URL(string: "https://bounded-static.test/cancel")!)
    let session = staticSession()
    defer { session.invalidateAndCancel() }
    try await withObservedRequest(
      { try await URLSessionTransport(session: session).send(request) },
      { send in
        try await awaitFixtureGate(receipt.requestStarted, named: "URLProtocol request start")
        send.task.cancel()
        await #expect(throws: CancellationError.self) { try await send.task.value }
        try await awaitFixtureGate(receipt.stopLoadingCalled, named: "URLProtocol stopLoading")
        #expect(await receipt.stopCallCount() == 1)
      })
  }
}
