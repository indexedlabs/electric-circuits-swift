import Foundation
import Testing

@testable import ElectricCircuitsSwift

private func transportFaultResponse(
  _ body: String,
  status: Int = 200,
  headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test/v1/shapes")!, statusCode: status,
      httpVersion: nil, headerFields: headers)!)
}

private let transportFaultShape =
  #"{"shapeId":"s1","table":"public.items","streamPath":"/v1/streams/s1","streamUrl":"https://streams.test/s1","subscription":"claim"}"#

private let transportFaultEvent =
  #"[{"type":"public.items","key":"1","value":{"id":1},"headers":{"operation":"upsert"}}]"#

private actor FaultScriptTransport: HTTPTransport {
  private var responses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []

  init(_ responses: [HTTPResponse]) {
    self.responses = responses
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    guard !responses.isEmpty else { throw CancellationError() }
    return responses.removeFirst()
  }
}

private enum SSEFaultStep: Sendable {
  case bytes([Data], thenFail: Bool)
  case status(Int, contentType: String?)
}

private actor SSEFaultTransport: HTTPTransport {
  private var streamSteps: [SSEFaultStep]
  private(set) var streamRequests: [URLRequest] = []
  private(set) var requests: [URLRequest] = []

  init(_ streamSteps: [SSEFaultStep]) { self.streamSteps = streamSteps }

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    streamRequests.append(request)
    guard !streamSteps.isEmpty else { throw CancellationError() }
    switch streamSteps.removeFirst() {
    case .bytes(let chunks, let thenFail):
      try await onResponse(HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!)
      for chunk in chunks { try await onBytes(chunk) }
      if thenFail { throw URLError(.networkConnectionLost) }
    case .status(let status, let contentType):
      var headers: [String: String] = [:]
      if let contentType { headers["Content-Type"] = contentType }
      try await onResponse(HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
    }
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      return HTTPResponse(
        data: Data(transportFaultShape.utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil,
          headerFields: ["Content-Type": "application/json"])!)
    case "DELETE":
      return HTTPResponse(
        data: Data(), response: HTTPURLResponse(
          url: request.url!, statusCode: 204, httpVersion: nil, headerFields: nil)!)
    case "GET":
      return HTTPResponse(
        data: Data(), response: HTTPURLResponse(
          url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
    default:
      throw CancellationError()
    }
  }
}

private func sseFaultData(_ key: String) -> Data {
  Data(
    "event: data\ndata: [{\"type\":\"public.items\",\"key\":\"\(key)\",\"value\":{\"id\":\"\(key)\"},\"headers\":{\"operation\":\"upsert\"}}]\n\n".utf8)
}

private func sseFaultControl(_ offset: String) -> Data {
  Data("event: control\ndata: {\"streamNextOffset\":\"\(offset)\"}\n\n".utf8)
}

private actor RetryAfterGateClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  private var continuation: CheckedContinuation<Void, Error>?
  private var released = false
  private(set) var cancelled = false

  func sleep(for duration: Duration) async throws {
    delays.append(duration)
    try Task.checkCancellation()
    try await withTaskCancellationHandler(
      operation: {
        try await withCheckedThrowingContinuation {
          (continuation: CheckedContinuation<Void, Error>) in
          if cancelled {
            continuation.resume(throwing: CancellationError())
          } else if released {
            released = false
            continuation.resume(returning: ())
          } else {
            self.continuation = continuation
          }
        }
      },
      onCancel: {
        Task { await self.cancelSleep() }
      })
  }

  func waitForSleep() async {
    while delays.isEmpty { await Task.yield() }
  }

  func release() {
    if let continuation {
      continuation.resume(returning: ())
      self.continuation = nil
    } else {
      released = true
    }
  }

  private func cancelSleep() {
    cancelled = true
    continuation?.resume(throwing: CancellationError())
    continuation = nil
  }
}

private struct ImmediateFaultClock: ShapeSubscriptionClock {
  func sleep(for _: Duration) async throws { try Task.checkCancellation() }
}

private actor RefreshingCredentialTransport: HTTPTransport {
  private nonisolated let authFailureStatus: Int
  private var refreshed = false
  private(set) var requests: [URLRequest] = []
  private(set) var observedStatuses: [Int] = []
  private(set) var refreshCount = 0

  init(authFailureStatus: Int) {
    self.authFailureStatus = authFailureStatus
  }

  nonisolated func prepare(_ request: URLRequest) -> URLRequest {
    var request = request
    request.setValue("Bearer initial-opaque-secret", forHTTPHeaderField: "Authorization")
    request.setValue("session=opaque-cookie", forHTTPHeaderField: "Cookie")
    return request
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    guard !refreshed else { return transportFaultResponse(#"{"rows":[],"lsn":"0/1"}"#) }

    let authFailure = transportFaultResponse("credential expired", status: authFailureStatus)
    observedStatuses.append(authFailure.response.statusCode)
    refreshed = true
    refreshCount += 1
    var retry = request
    retry.setValue("Bearer refreshed-opaque-secret", forHTTPHeaderField: "Authorization")
    requests.append(retry)
    return transportFaultResponse(#"{"rows":[],"lsn":"0/1"}"#)
  }
}

private actor RecordedTelemetrySink: TelemetrySink {
  private(set) var requests: [URLRequest] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return transportFaultResponse("", status: 202)
  }
}

private actor ResponseGateTransport: HTTPTransport {
  private var entered = false
  private var continuation: CheckedContinuation<Void, Never>?
  private(set) var cancelled = false

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    entered = true
    return try await withTaskCancellationHandler(
      operation: {
        await withCheckedContinuation { continuation = $0 }
        try Task.checkCancellation()
        return transportFaultResponse(
          transportFaultEvent, headers: ["stream-next-offset": "2"])
      },
      onCancel: {
        Task { await self.cancel() }
      })
  }

  func waitForEntry() async {
    while !entered { await Task.yield() }
  }

  private func cancel() {
    cancelled = true
    continuation?.resume()
    continuation = nil
  }
}

private actor FailingApplyMaterializer: ShapeMaterializer {
  private(set) var applyStarted = false
  private(set) var cursor: StreamCursor?
  private(set) var rows: [String: ChangeRow] = [:]
  private var continuation: CheckedContinuation<Void, Never>?

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo next: StreamCursor
  ) async throws {
    applyStarted = true
    await withCheckedContinuation { continuation = $0 }
    try Task.checkCancellation()
    throw ProviderFailure.rejected
  }

  func waitForApply() async {
    while !applyStarted { await Task.yield() }
  }

  func reject() {
    continuation?.resume()
    continuation = nil
  }

  enum ProviderFailure: Error, Sendable { case rejected }
}

private struct SecretProviderFailure: Error, Sendable, CustomStringConvertible {
  static let secret = "provider-diagnostic-secret"
  var description: String { Self.secret }
}

private actor SecretProviderMaterializer: ShapeMaterializer {
  func currentCursor() async throws -> StreamCursor? { nil }

  func apply(
    _: ChangeBatch,
    expecting _: StreamCursor?,
    advancingTo _: StreamCursor
  ) async throws {
    throw SecretProviderFailure()
  }
}

@Suite("Native transport fault matrix")
struct TransportFaultMatrixTests {
  @Test func retryAfterOverridesShorterClientBackoffBeforeCreateRecovers() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse("slow down", status: 429, headers: ["Retry-After": "3"]),
      transportFaultResponse(transportFaultShape),
      transportFaultResponse("gone", status: 404),
      transportFaultResponse("", status: 404),
    ])
    let clock = RetryAfterGateClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(10), jitterRatio: 0),
      clock: clock)

    let start = Task { try await coordinator.start() }
    await clock.waitForSleep()
    // A valid server directive is a lower bound; it wins over the shorter client backoff.
    #expect(await clock.delays == [.seconds(3)])
    await clock.release()
    _ = try await start.value
    try await coordinator.stop()
  }

  @Test(arguments: [401, 403])
  func callerOwnedCredentialRefreshKeepsCredentialsOpaque(_ status: Int) async throws {
    let transport = RefreshingCredentialTransport(authFailureStatus: status)
    let sink = RecordedTelemetrySink()
    let telemetry = TelemetryReporter(
      configuration: .init(tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: telemetry)

    #expect(try await client.querySubset(SubsetQuery(table: "public.items")).lsn == "0/1")
    await telemetry.flush()

    #expect(await transport.observedStatuses == [status])
    #expect(await transport.refreshCount == 1)
    let requests = await transport.requests
    #expect(requests.count == 2)
    #expect(
      requests[0].value(forHTTPHeaderField: "Authorization") == "Bearer initial-opaque-secret")
    #expect(
      requests[1].value(forHTTPHeaderField: "Authorization") == "Bearer refreshed-opaque-secret")
    #expect(
      requests.allSatisfy {
        $0.value(forHTTPHeaderField: "Cookie") == "session=opaque-cookie"
      })
    let exported = await sink.requests.compactMap { $0.httpBody }.map {
      String(decoding: $0, as: UTF8.self)
    }
    .joined(separator: "\n")
    #expect(!exported.contains("opaque-secret"))
    #expect(!exported.contains("opaque-cookie"))
    await telemetry.shutdown()
  }

  @Test func transientFiveHundredRecoveryUsesTypedRetryState() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse("unavailable", status: 503),
      transportFaultResponse(transportFaultShape),
      transportFaultResponse("gone", status: 404),
      transportFaultResponse("", status: 404),
    ])
    let clock = RetryAfterGateClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(2), maximumDelay: .seconds(10), jitterRatio: 0),
      clock: clock)

    let start = Task { try await coordinator.start() }
    await clock.waitForSleep()
    #expect(
      await coordinator.state
        == .waitingToRetry(
          operation: "create", attempt: 1, delay: .seconds(2)))
    await clock.release()
    _ = try await start.value
    try await coordinator.stop()
  }

  @Test func retryableHTTPExposesRetryAfterWithoutStringParsing() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse("slow down", status: 429, headers: ["Retry-After": "3"])
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)

    await #expect(throws: ClientError.retryableHTTP(status: 429, retryAfter: .seconds(3))) {
      _ = try await client.querySubset(SubsetQuery(table: "public.items"))
    }
  }

  @Test(arguments: [401, 403, 409])
  func terminalControlErrorsKeepStatusButRedactBodiesAndResponseHeaders(_ status: Int) async throws
  {
    let secret = "control-body-secret-\(status)"
    let headerSecret = "header-token-\(status)"
    let cookieSecret = "cookie-secret-\(status)"
    let transport = FaultScriptTransport([
      transportFaultResponse(
        secret, status: status,
        headers: ["Authorization": "Bearer \(headerSecret)", "Set-Cookie": "s=\(cookieSecret)"])
    ])
    let sink = RecordedTelemetrySink()
    let telemetry = TelemetryReporter(
      configuration: .init(tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: telemetry)

    do {
      _ = try await client.querySubset(SubsetQuery(table: "public.items"))
      Issue.record("expected nonretryable HTTP failure")
    } catch let error as ClientError {
      #expect(error == .http(status: status, message: "HTTP request failed"))
      let description = String(describing: error)
      #expect(!description.contains(secret))
      #expect(!description.contains(headerSecret))
      #expect(!description.contains(cookieSecret))
    } catch {
      Issue.record("unexpected error type: \(String(describing: error))")
    }
    await telemetry.flush()
    let exported = await sink.requests.compactMap { $0.httpBody }.map {
      String(decoding: $0, as: UTF8.self)
    }
    .joined(separator: "\n")
    #expect(!exported.contains(secret))
    #expect(!exported.contains(headerSecret))
    #expect(!exported.contains(cookieSecret))
    await telemetry.shutdown()
  }

  @Test func terminalStreamErrorAndProviderDiagnosticAreRedacted() async throws {
    let streamSecret = "stream-body-secret"
    let streamHeaderSecret = "stream-header-secret"
    let streamTransport = FaultScriptTransport([
      transportFaultResponse(
        streamSecret, status: 403, headers: ["Set-Cookie": "s=\(streamHeaderSecret)"])
    ])
    let sink = RecordedTelemetrySink()
    let telemetry = TelemetryReporter(
      configuration: .init(tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/s1")!, transport: streamTransport,
      materializer: InMemoryShapeMaterializer(), telemetry: telemetry)

    await #expect(throws: ClientError.http(status: 403, message: "HTTP request failed")) {
      try await reader.run()
    }
    await telemetry.flush()
    var exported = await sink.requests.compactMap { $0.httpBody }.map {
      String(decoding: $0, as: UTF8.self)
    }
    .joined(separator: "\n")
    #expect(!exported.contains(streamSecret))
    #expect(!exported.contains(streamHeaderSecret))

    let providerTransport = FaultScriptTransport([
      transportFaultResponse(transportFaultShape),
      transportFaultResponse(transportFaultEvent, headers: ["stream-next-offset": "2"]),
      transportFaultResponse("", status: 404),
    ])
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: providerTransport,
        telemetry: telemetry),
      transport: providerTransport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: SecretProviderMaterializer(), retryPolicy: .init(maxRetries: 0),
      telemetry: telemetry)
    _ = try await coordinator.start()
    for _ in 0..<1_000 {
      if case .failed(.materializer) = await coordinator.state { break }
      await Task.yield()
    }
    let state = await coordinator.state
    #expect(state == .failed(.materializer))
    #expect(!String(describing: state).contains(SecretProviderFailure.secret))
    await telemetry.flush()
    exported = await sink.requests.compactMap { $0.httpBody }.map {
      String(decoding: $0, as: UTF8.self)
    }
    .joined(separator: "\n")
    #expect(!exported.contains(SecretProviderFailure.secret))
    try await coordinator.stop()
    await telemetry.shutdown()
  }

  @Test func retryAfterIsBoundedAndStopCancelsThePendingRetry() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse("slow down", status: 429, headers: ["Retry-After": "3600"])
    ])
    let clock = RetryAfterGateClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(
        maxRetries: 1, baseDelay: .seconds(1), maximumDelay: .seconds(5), jitterRatio: 0),
      clock: clock)

    let start = Task { try await coordinator.start() }
    await clock.waitForSleep()
    #expect(await clock.delays == [.seconds(5)])
    try await coordinator.stop()
    await #expect(throws: CancellationError.self) { _ = try await start.value }
    #expect(await clock.cancelled)
    #expect(await transport.requests.count == 1)
    #expect(await coordinator.state == .stopped)
  }

  @Test func retryExhaustionRetainsTypedCauseForCallerAction() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse("temporary", status: 503),
      transportFaultResponse("temporary", status: 503),
    ])
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: InMemoryShapeMaterializer(),
      retryPolicy: .init(maxRetries: 1, baseDelay: .zero, maximumDelay: .zero, jitterRatio: 0),
      clock: ImmediateFaultClock())

    let expected = ShapeSubscriptionFailure.retryExhausted(
      operation: "create", attempts: 2, cause: .http(status: 503))
    await #expect(throws: expected) { _ = try await coordinator.start() }
    #expect(await coordinator.state == .failed(expected))
  }

  @Test func cancellationDuringInFlightStreamResponseLeavesProviderUntouched() async throws {
    let transport = ResponseGateTransport()
    let materializer = InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "1"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/s1")!, transport: transport,
      materializer: materializer)

    let task = Task { try await reader.run() }
    await transport.waitForEntry()
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(await transport.cancelled)
    #expect(try await materializer.currentCursor() == StreamCursor(offset: "1"))
    #expect(await materializer.rows().isEmpty)
  }

  @Test func rejectedProviderApplicationDoesNotAdvanceCursor() async throws {
    let transport = FaultScriptTransport([
      transportFaultResponse(transportFaultEvent, headers: ["stream-next-offset": "2"])
    ])
    let materializer = FailingApplyMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/s1")!, transport: transport,
      materializer: materializer, startingAt: .beginning)

    let task = Task { try await reader.run() }
    await materializer.waitForApply()
    await materializer.reject()
    await #expect(throws: FailingApplyMaterializer.ProviderFailure.self) { try await task.value }
    #expect(await materializer.cursor == nil)
    #expect(await materializer.rows.isEmpty)
  }

  @Test func sseDroppedHeldResponseAndServerErrorReconnectOnSSE() async throws {
    let transport = SSEFaultTransport([
      .bytes([sseFaultData("1")], thenFail: true),
      .status(503, contentType: nil),
      .bytes([sseFaultData("1"), sseFaultControl("1")], thenFail: false),
      .status(404, contentType: nil),
    ])
    let materializer = InMemoryShapeMaterializer()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport,
        configuration: .init(liveReadMode: .sse)),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: materializer,
      retryPolicy: .init(maxRetries: 3, baseDelay: .zero, maximumDelay: .zero, jitterRatio: 0),
      clock: ImmediateFaultClock())

    _ = try await coordinator.start()
    for _ in 0..<2_000 {
      if case .reseedRequired = await coordinator.state { break }
      await Task.yield()
    }
    let requests = await transport.streamRequests
    #expect(requests.count == 4)
    #expect(requests.allSatisfy { $0.url?.query?.contains("live=sse") == true })
    #expect(await materializer.cursor() == StreamCursor(offset: "1"))
    #expect(await materializer.rows().keys.sorted() == ["1"])
    try await coordinator.stop()
  }

  @Test func sseReconnectsResumeFromCommittedOffsetWithoutDuplicatesOrGaps() async throws {
    let transport = SSEFaultTransport([
      .bytes(
        [sseFaultData("1"), sseFaultControl("1"), sseFaultData("2")], thenFail: true),
      .bytes([sseFaultData("2"), sseFaultControl("2")], thenFail: false),
      .status(404, contentType: nil),
    ])
    let materializer = InMemoryShapeMaterializer()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport,
        configuration: .init(liveReadMode: .sse)),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: materializer,
      retryPolicy: .init(maxRetries: 2, baseDelay: .zero, maximumDelay: .zero, jitterRatio: 0),
      clock: ImmediateFaultClock())

    _ = try await coordinator.start()
    for _ in 0..<2_000 {
      if case .reseedRequired = await coordinator.state { break }
      await Task.yield()
    }
    let requests = await transport.streamRequests
    #expect(requests.count == 3)
    #expect(URLComponents(url: requests[0].url!, resolvingAgainstBaseURL: false)?.queryItems?.first(where: { $0.name == "offset" })?.value == "-1")
    #expect(URLComponents(url: requests[1].url!, resolvingAgainstBaseURL: false)?.queryItems?.first(where: { $0.name == "offset" })?.value == "1")
    #expect(await materializer.cursor() == StreamCursor(offset: "2"))
    #expect(await materializer.rows().keys.sorted() == ["1", "2"])
    try await coordinator.stop()
  }

  @Test func sseFallsBackOnlyWhenServerRefusesSSE() async throws {
    let transport = SSEFaultTransport([.status(400, contentType: nil)])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/s1")!, transport: transport,
      materializer: InMemoryShapeMaterializer(), configuration: .init(liveReadMode: .sse))

    await #expect(throws: StreamError.terminal(
      path: "/s1", status: 404, reason: .notFound)) {
      try await reader.run()
    }
    let streamRequests = await transport.streamRequests
    let requests = await transport.requests
    #expect(streamRequests.count == 1)
    #expect(streamRequests[0].url?.query?.contains("live=sse") == true)
    #expect(requests.count == 1)
    #expect(requests[0].url?.query?.contains("live=long-poll") == true)
  }
}
