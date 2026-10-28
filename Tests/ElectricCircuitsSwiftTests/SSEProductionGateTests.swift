import Foundation
import Testing

@testable import ElectricCircuitsSwift

private let sseGateURL = URL(string: "https://sse-gate.test/stream")!

private enum SSEGateFailure: Error {
  case timedOut(String)
}

/// All URLProtocol callbacks for a connection are driven by the test. The lock protects registry
/// access from Foundation's start/stop callbacks; tests in this suite run serially.
private final class SSEGateServer: @unchecked Sendable {
  struct Read: Sendable {
    let request: URLRequest
    let generation: Int
  }

  private let lock = NSLock()
  private var reads: [Read] = []
  private var connections: [Int: SSEGateProtocol] = [:]
  private var stopped: Set<Int> = []
  private var generation = 0
  private var peak = 0

  func open(_ connection: SSEGateProtocol) {
    guard let client = connection.client, let url = connection.request.url else { return }
    if connection.request.httpMethod == "POST" {
      let body = Data(
        #"{"shapeId":"sse-gate","table":"public.items","streamPath":"/stream","streamUrl":"https://sse-gate.test/stream"}"#.utf8)
      client.urlProtocol(
        connection,
        didReceive: HTTPURLResponse(
          url: url, statusCode: 200, httpVersion: nil,
          headerFields: ["Content-Type": "application/json"])!, cacheStoragePolicy: .notAllowed)
      client.urlProtocol(connection, didLoad: body)
      client.urlProtocolDidFinishLoading(connection)
    } else if connection.request.httpMethod == "DELETE" {
      client.urlProtocol(
        connection,
        didReceive: HTTPURLResponse(
          url: url, statusCode: 204, httpVersion: nil, headerFields: nil)!,
        cacheStoragePolicy: .notAllowed)
      client.urlProtocolDidFinishLoading(connection)
    } else {
      client.urlProtocol(
        connection,
        didReceive: HTTPURLResponse(
          url: url, statusCode: 200, httpVersion: nil,
          headerFields: ["Content-Type": "text/event-stream; charset=utf-8"])!,
        cacheStoragePolicy: .notAllowed)
      lock.withLock {
        connections[reads.count] = connection
        reads.append(Read(request: connection.request, generation: generation))
        peak = max(peak, connections.count)
      }
    }
  }

  func stop(_ connection: SSEGateProtocol) {
    lock.withLock {
      if let index = connections.first(where: { $0.value === connection })?.key {
        connections.removeValue(forKey: index)
        stopped.insert(index)
      }
    }
  }

  @discardableResult
  func send(_ data: Data, to index: Int) -> Bool {
    guard let connection = lock.withLock({ connections[index] }) else { return false }
    connection.client?.urlProtocol(connection, didLoad: data)
    return true
  }

  func finish(_ index: Int) {
    guard let connection = lock.withLock({ connections.removeValue(forKey: index) }) else { return }
    connection.client?.urlProtocolDidFinishLoading(connection)
  }

  /// A server restart kills every held response. The next connection belongs to a new generation.
  func restart() {
    let old = lock.withLock {
      generation += 1
      let old = Array(connections.values)
      connections.removeAll()
      return old
    }
    for connection in old {
      connection.client?.urlProtocol(connection, didFailWithError: URLError(.networkConnectionLost))
    }
  }

  func snapshot() -> [Read] { lock.withLock { reads } }
  func peakLiveReads() -> Int { lock.withLock { peak } }
  func wasStopped(_ index: Int) -> Bool { lock.withLock { stopped.contains(index) } }
}

private final class SSEGateRegistry: @unchecked Sendable {
  private let lock = NSLock()
  private var server = SSEGateServer()
  func install(_ server: SSEGateServer) { lock.withLock { self.server = server } }
  func current() -> SSEGateServer { lock.withLock { server } }
}

private final class SSEGateProtocol: URLProtocol, @unchecked Sendable {
  static let registry = SSEGateRegistry()
  private var server: SSEGateServer?

  override class func canInit(with request: URLRequest) -> Bool {
    request.url?.host == "sse-gate.test"
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest { request }

  override func startLoading() {
    let server = Self.registry.current()
    self.server = server
    server.open(self)
  }

  override func stopLoading() { server?.stop(self) }
}

private actor SSEGateStore: ShapeMaterializer {
  private var rows: [String: ChangeRow] = [:]
  private var cursor: StreamCursor? = StreamCursor(offset: "10")
  private var commits: [(keys: [String], offset: String)] = []

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?, advancingTo next: StreamCursor
  ) throws {
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(expected: expectedCursor, actual: cursor, advancingTo: next)
    }
    var updated = rows
    for envelope in batch.envelopes {
      if envelope.headers.operation == .delete {
        updated.removeValue(forKey: envelope.key)
      } else {
        updated[envelope.key] = envelope.value
      }
    }
    rows = updated
    cursor = next
    commits.append((batch.envelopes.map(\.key), next.offset))
  }

  func row(_ key: String) -> ChangeRow? { rows[key] }
  func keys() -> [String] { rows.keys.sorted() }
  func offsets() -> [String] { commits.map(\.offset) }
  func appliedKeys() -> [[String]] { commits.map(\.keys) }
}

private actor SSEGateOutcome {
  private(set) var finished = false
  func finish() { finished = true }
}

private actor SSEFallbackTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var pollCount = 0

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    requests.append(request)
    try await onResponse(HTTPURLResponse(
      url: request.url!, statusCode: 200, httpVersion: nil,
      headerFields: ["Content-Type": "application/json"])!)
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    pollCount += 1
    return HTTPResponse(
      data: Data(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: pollCount == 1 ? 204 : 404, httpVersion: nil,
        headerFields: nil)!)
  }
}

private actor SSEKeepAliveTransport: HTTPTransport {
  private(set) var streamCalls = 0

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    streamCalls += 1
    if streamCalls == 1 {
      try await onResponse(HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!)
      // A data frame held for the next cursor must be discarded when the control frame is only
      // a keep-alive. This is the reader-level verification of the SSE event contract.
      try await onBytes(gateData("pending"))
      try await onBytes(Data("event: control\ndata: {}\n\n".utf8))
      return
    }
    try await onResponse(HTTPURLResponse(
      url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    Issue.record("Long-poll fallback was unexpectedly used")
    return HTTPResponse(
      data: Data(), response: HTTPURLResponse(
        url: request.url!, statusCode: 500, httpVersion: nil, headerFields: nil)!)
  }
}

private struct SSEGateRig {
  let server = SSEGateServer()
  let store = SSEGateStore()
  let session: URLSession
  let transport: URLSessionTransport
  let client: ElectricCircuitsClient
  let responseDecodingLimits: ResponseDecodingLimits

  init(initialLimit: Int = ResponseDecodingLimits.default.maximumDecodedResponseBytes) {
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [SSEGateProtocol.self]
    // The SDK's 45-second frame watchdog must fire before Foundation's own idle timeout.
    configuration.timeoutIntervalForRequest = 180
    configuration.timeoutIntervalForResource = 300
    session = URLSession(configuration: configuration)
    transport = URLSessionTransport(session: session)
    responseDecodingLimits = .init(maximumDecodedResponseBytes: initialLimit)
    client = ElectricCircuitsClient(
      baseURL: URL(string: "https://sse-gate.test")!, transport: transport,
      responseDecodingLimits: responseDecodingLimits,
      configuration: .init(liveReadMode: .sse))
    SSEGateProtocol.registry.install(server)
  }

  func coordinator() -> ShapeSubscriptionCoordinator {
    ShapeSubscriptionCoordinator(
      client: client, transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "sse-gate"), materializer: store,
      responseDecodingLimits: responseDecodingLimits,
      retryPolicy: .init(maxRetries: 2, baseDelay: .zero, maximumDelay: .zero, jitterRatio: 0))
  }

  func requireSSERead(_ index: Int, offset: String, timeout: Duration = .seconds(10)) async throws {
    try await gateWait("stream request \(index)", timeout: timeout) {
      server.snapshot().count > index
    }
    let request = server.snapshot()[index].request
    let url = try #require(request.url)
    let components = try #require(URLComponents(url: url, resolvingAgainstBaseURL: false))
    try #require(components.queryItems?.first(where: { $0.name == "live" })?.value == "sse")
    #expect(components.queryItems?.first(where: { $0.name == "offset" })?.value == offset)
    #expect(request.value(forHTTPHeaderField: "Accept") == "text/event-stream")
  }
}

private func gateWait(
  _ description: String, timeout: Duration = .seconds(10),
  until condition: () async throws -> Bool
) async throws {
  let clock = ContinuousClock()
  let deadline = clock.now.advanced(by: timeout)
  while !(try await condition()) {
    guard clock.now < deadline else { throw SSEGateFailure.timedOut(description) }
    try await Task.sleep(for: .milliseconds(10))
  }
}

private func gateData(_ key: String) -> Data {
  Data(
    "event: data\ndata: [{\"type\":\"public.items\",\"key\":\"\(key)\",\"value\":{\"id\":\"\(key)\"},\"headers\":{\"operation\":\"upsert\"}}]\n\n".utf8)
}

private func gateControl(_ offset: String) -> Data {
  Data("event: control\ndata: {\"streamNextOffset\":\"\(offset)\"}\n\n".utf8)
}

/// Generates a complete SSE data event of the exact wire length without retaining a second
/// event-sized fixture. The row's string proves that the decoder received the whole payload.
@discardableResult
private func sendSizedGateEvent(_ payloadSize: Int, server: SSEGateServer, read: Int) -> Int {
  let jsonPrefix = Data(#"[{"type":"public.items","key":"large","value":{"padding":""#.utf8)
  let jsonSuffix = Data(#""},"headers":{"operation":"upsert"}}]"#.utf8)
  let prefix = Data("event: data\ndata: ".utf8) + jsonPrefix
  let suffix = jsonSuffix + Data("\n\n".utf8)
  let paddingCount = payloadSize - jsonPrefix.count - jsonSuffix.count
  guard server.send(prefix, to: read) else { return paddingCount }
  let chunk = Data(repeating: 0x78, count: 64 * 1024)
  var remaining = paddingCount
  while remaining > 0 {
    let count = min(remaining, chunk.count)
    guard server.send(Data(chunk.prefix(count)), to: read) else { return paddingCount }
    remaining -= count
  }
  server.send(suffix, to: read)
  return paddingCount
}

@Suite("SSE production-readiness gates", .serialized)
struct SSEProductionGateTests {
  @Test(
    arguments: [16 * 1024 * 1024, 16 * 1024 * 1024 + 1, 64 * 1024 * 1024, 64 * 1024 * 1024 + 1])
  func largeEventsParseWholeAndCommit(size: Int) async throws {
    let rig = SSEGateRig(initialLimit: size <= 16 * 1024 * 1024 + 1 ? 16 * 1024 * 1024 : 64 * 1024 * 1024)
    defer { rig.session.invalidateAndCancel() }
    let coordinator = rig.coordinator()
    do {
      _ = try await coordinator.start()
      try await rig.requireSSERead(0, offset: "10")
      let count = sendSizedGateEvent(size, server: rig.server, read: 0)
      rig.server.send(gateControl("20"), to: 0)
      try await gateWait("large event commit", timeout: .seconds(120)) {
        try await rig.store.currentCursor()?.offset == "20"
      }
      if case .string(let padding)? = await rig.store.row("large")?["padding"] {
        #expect(padding.utf8.count == count)
        #expect(padding.utf8.allSatisfy { $0 == 0x78 })
      } else {
        Issue.record("The complete large row was not decoded")
      }
      #expect(await rig.store.offsets() == ["20"])
      #expect(rig.server.snapshot().count == 1)
      try await coordinator.stop()
    } catch {
      try? await coordinator.stop()
      throw error
    }
  }

  @Test func eventPast512MiBFailsWithTypedError() async throws {
    let rig = SSEGateRig()
    defer { rig.session.invalidateAndCancel() }
    let reader = ShapeStreamReader(
      streamURL: sseGateURL, transport: rig.transport, materializer: rig.store,
      configuration: await rig.client.configuration)
    let outcome = SSEGateOutcome()
    let task = Task {
      do {
        try await reader.run()
        await outcome.finish()
      } catch {
        await outcome.finish()
        throw error
      }
    }
    do {
      try await rig.requireSSERead(0, offset: "10")
      let ceiling = 512 * 1024 * 1024
      sendSizedGateEvent(ceiling + 1, server: rig.server, read: 0)
      try await gateWait("oversized event refusal", timeout: .seconds(120)) { await outcome.finished }
      do {
        _ = try await task.value
        Issue.record("An event above the 512 MiB ceiling unexpectedly completed")
      } catch let error as StreamError {
        if case .responseTooLarge(let limit, _) = error {
          #expect(limit == ceiling)
        } else {
          Issue.record("Unexpected stream error: \(error)")
        }
      }
      #expect(await rig.store.offsets().isEmpty)
      #expect(try await rig.store.currentCursor()?.offset == "10")
    } catch {
      task.cancel()
      _ = await task.result
      throw error
    }
    task.cancel()
    _ = await task.result
  }

  @Test func severalDataEventsCommitTogetherAtOneControlFrame() async throws {
    let rig = SSEGateRig()
    defer { rig.session.invalidateAndCancel() }
    let coordinator = rig.coordinator()
    do {
      _ = try await coordinator.start()
      try await rig.requireSSERead(0, offset: "10")
      rig.server.send(gateData("first"), to: 0)
      rig.server.send(gateData("second"), to: 0)
      #expect(await rig.store.offsets().isEmpty)
      rig.server.send(gateControl("20"), to: 0)
      try await gateWait("combined data commit") { await rig.store.offsets() == ["20"] }
      #expect(await rig.store.appliedKeys() == [["first", "second"]])
      try await coordinator.stop()
    } catch {
      try? await coordinator.stop()
      throw error
    }
  }

  @Test func cleanResponseEndsReopenFromCommittedOffsetWithoutRetryState() async throws {
    let rig = SSEGateRig()
    defer { rig.session.invalidateAndCancel() }
    let coordinator = rig.coordinator()
    do {
      _ = try await coordinator.start()
      try await rig.requireSSERead(0, offset: "10")
      rig.server.send(gateData("first"), to: 0)
      rig.server.send(gateControl("20"), to: 0)
      try await gateWait("clean-end baseline") { await rig.store.offsets() == ["20"] }
      for index in 0..<4 {
        rig.server.finish(index)
        try await rig.requireSSERead(index + 1, offset: "20")
        let state = await coordinator.state
        if case .failed = state { Issue.record("clean response end failed the subscription") }
        if case .waitingToRetry = state { Issue.record("clean response end entered retry state") }
      }
      try await coordinator.stop()
    } catch {
      try? await coordinator.stop()
      throw error
    }
  }

  @Test func parserSplitsEveryByteAndDropsKeepAliveAndTrailingPartialEvent() throws {
    let first = gateData("first")
    let control = gateControl("20")
    let keepAlive = Data(": keep-alive\n\n".utf8)
    let partial = Data("event: data\ndata: [{\"incomplete\":true}".utf8)
    let source = first + keepAlive + control + partial
    let expected = [
      SSEParser.Event(name: "data", data: Data("[{\"type\":\"public.items\",\"key\":\"first\",\"value\":{\"id\":\"first\"},\"headers\":{\"operation\":\"upsert\"}}]".utf8)),
      SSEParser.Event(name: "control", data: Data("{\"streamNextOffset\":\"20\"}".utf8)),
    ]
    for split in 0...source.count {
      var parser = SSEParser(maximumEventBytes: 1_024)
      var events: [SSEParser.Event] = []
      for byte in source.prefix(split) {
        if let event = try parser.append(byte) { events.append(event) }
      }
      for byte in source.dropFirst(split) {
        if let event = try parser.append(byte) { events.append(event) }
      }
      #expect(events == expected)
    }
  }

  @Test func cutHeldResponseThenServerRestartResumesWithoutDuplicateOrMissingRows() async throws {
    let rig = SSEGateRig()
    defer { rig.session.invalidateAndCancel() }
    let coordinator = rig.coordinator()
    do {
      _ = try await coordinator.start()
      try await rig.requireSSERead(0, offset: "10")
      rig.server.send(gateData("first"), to: 0)
      rig.server.send(gateControl("20"), to: 0)
      try await gateWait("first committed batch") { await rig.store.offsets() == ["20"] }
      rig.server.send(gateData("second"), to: 0)
      rig.server.send(Data("event: control\ndata: {\"streamNextOffset\":\"30\"}".utf8), to: 0)
      rig.server.restart()
      try await rig.requireSSERead(1, offset: "20")
      #expect(rig.server.snapshot().map(\.generation) == [0, 1])
      #expect(await rig.store.keys() == ["first"])
      #expect(await rig.store.offsets() == ["20"])
      rig.server.send(gateData("second"), to: 1)
      rig.server.send(gateControl("30"), to: 1)
      try await gateWait("replayed batch") { await rig.store.offsets() == ["20", "30"] }
      #expect(await rig.store.keys() == ["first", "second"])
      #expect(await rig.store.appliedKeys() == [["first"], ["second"]])
      #expect(rig.server.peakLiveReads() == 1)
      try await coordinator.stop()
    } catch {
      try? await coordinator.stop()
      throw error
    }
  }

  @Test func silentHeldResponseClosesAfter45SecondsAndResumesCommittedOffset() async throws {
    let rig = SSEGateRig()
    defer { rig.session.invalidateAndCancel() }
    let coordinator = rig.coordinator()
    do {
      _ = try await coordinator.start()
      try await rig.requireSSERead(0, offset: "10")
      rig.server.send(gateData("first"), to: 0)
      rig.server.send(gateControl("20"), to: 0)
      try await gateWait("pre-stall commit") { await rig.store.offsets() == ["20"] }
      for index in 1...3 {
        try await rig.requireSSERead(index, offset: "20", timeout: .seconds(55))
        #expect(rig.server.wasStopped(index - 1))
      }
      #expect(rig.server.peakLiveReads() == 1)
      rig.server.send(gateData("second"), to: 3)
      rig.server.send(gateControl("30"), to: 3)
      try await gateWait("post-stall commit") { await rig.store.offsets() == ["20", "30"] }
      #expect(await rig.store.appliedKeys() == [["first"], ["second"]])
      try await coordinator.stop()
    } catch {
      try? await coordinator.stop()
      throw error
    }
  }
}


@Test("SSE fallback and keep-alive")
func sseFallbackAndKeepAliveVerification() async throws {
  let transport = SSEFallbackTransport()
  let reader = ShapeStreamReader(
    streamURL: sseGateURL, transport: transport, materializer: InMemoryShapeMaterializer(),
    configuration: .init(liveReadMode: .sse))
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }
  let requests = await transport.requests
  #expect(requests.count == 3)
  #expect(requests[0].url?.query?.contains("live=sse") == true)
  #expect(requests[1].url?.query?.contains("live=long-poll") == true)
  #expect(requests[2].url?.query?.contains("live=long-poll") == true)
}

@Test("SSE keep-alive control does not commit pending data")
func sseKeepAliveControlIsNoOp() async throws {
  let transport = SSEKeepAliveTransport()
  let store = InMemoryShapeMaterializer()
  let reader = ShapeStreamReader(
    streamURL: sseGateURL, transport: transport, materializer: store,
    configuration: .init(liveReadMode: .sse))
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }
  #expect(try await store.currentCursor() == nil)
  #expect(await transport.streamCalls == 2)
}

@Test("Long-poll remains the default live read")
func longPollRemainsDefaultLiveRead() async throws {
  let transport = SSEFallbackTransport()
  let reader = ShapeStreamReader(
    streamURL: sseGateURL, transport: transport, materializer: InMemoryShapeMaterializer())
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }
  let requests = await transport.requests
  #expect(requests.count == 2)
  #expect(requests.allSatisfy { $0.url?.query?.contains("live=long-poll") == true })
  #expect(requests.allSatisfy { $0.value(forHTTPHeaderField: "Accept") == "application/json" })
}
