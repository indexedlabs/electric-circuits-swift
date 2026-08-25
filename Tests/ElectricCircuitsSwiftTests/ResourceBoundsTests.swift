import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor BoundsTransport: HTTPTransport {
  private var responses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []

  init(responses: [HTTPResponse]) { self.responses = responses }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    guard !responses.isEmpty else {
      while true {
        try Task.checkCancellation()
        await Task.yield()
      }
    }
    return responses.removeFirst()
  }
}

private actor BoundsMaterializer: ShapeMaterializer {
  private var values: [String: ChangeRow] = [:]
  private var checkpoint: StreamCursor?
  private(set) var applications = 0

  init(rows: [String: ChangeRow] = [:], cursor: StreamCursor? = nil) {
    values = rows
    checkpoint = cursor
  }

  func currentCursor() async throws -> StreamCursor? { checkpoint }

  func apply(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    guard checkpoint == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: checkpoint, advancingTo: cursor)
    }
    var next = values
    for event in batch.envelopes {
      switch event.headers.operation {
      case .delete: next.removeValue(forKey: event.key)
      case .insert, .update, .upsert: next[event.key] = event.value
      }
    }
    values = next
    checkpoint = cursor
    applications += 1
  }

  func rows() -> [String: ChangeRow] { values }
}

private actor BoundsTelemetrySink: TelemetrySink {
  private(set) var requests: [URLRequest] = []
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return HTTPResponse(
      data: Data(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 202, httpVersion: nil, headerFields: nil)!)
  }
}

private actor BoundsClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  func sleep(for duration: Duration) async throws { delays.append(duration) }
}

private func boundsResponse(
  _ body: String, status: Int = 200, headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://streams.test/shape/s1")!, statusCode: status,
      httpVersion: nil, headerFields: headers)!)
}

private let oneChange =
  #"[{"type":"public.items","key":"1","value":{"id":1},"headers":{"operation":"upsert"}}]"#

@Suite("Decoded response and stream batch admission bounds")
struct ResourceBoundsTests {
  @Test func immutableLimitsClampInvalidInputsAndHaveFiniteDefaults() {
    let limits = ResponseDecodingLimits(
      maximumDecodedResponseBytes: 0, maximumChangeEventsPerStreamBatch: -1)
    #expect(limits.maximumDecodedResponseBytes == 1)
    #expect(limits.maximumChangeEventsPerStreamBatch == 1)
    #expect(ResponseDecodingLimits.default.maximumDecodedResponseBytes > 0)
    #expect(ResponseDecodingLimits.default.maximumChangeEventsPerStreamBatch > 0)
  }

  @Test func clientAcceptsAnExactlyBoundedDecodedResponse() async throws {
    let body = #"{"rows":[],"lsn":"0/10"}"#
    let transport = BoundsTransport(responses: [boundsResponse(body)])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: Data(body.utf8).count, maximumChangeEventsPerStreamBatch: 1))

    #expect(
      try await client.querySubset(SubsetQuery(table: "public.items"))
        == SubsetResponse(rows: [], lsn: "0/10"))
  }

  @Test func clientRejectsOneByteOverBeforeDecodeWithBoundedDiagnostics() async throws {
    let secret = "Bearer super-secret-token"
    let body = "x".padding(toLength: 9, withPad: "x", startingAt: 0)
    let transport = BoundsTransport(responses: [boundsResponse(body)])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 8, maximumChangeEventsPerStreamBatch: 1))

    await #expect(throws: ClientError.responseTooLarge(limit: 8, observed: 9)) {
      _ = try await client.querySubset(
        SubsetQuery(
          table: "public.items", where: .leaf(column: "token", op: .eq, value: .string(secret))))
    }
  }

  @Test func streamRejectsOversizedOrOverfullBatchBeforeProviderApplyAndCursorAdvance() async throws
  {
    let prior = StreamCursor(offset: "41")
    let oversized = oneChange + "!"
    let byteTransport = BoundsTransport(responses: [
      boundsResponse(oversized, headers: ["stream-next-offset": "42"])
    ])
    let materializer = BoundsMaterializer(rows: ["old": ["id": .int(0)]], cursor: prior)
    let byteReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: byteTransport,
      materializer: materializer, startingAt: prior,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: Data(oneChange.utf8).count,
        maximumChangeEventsPerStreamBatch: 1))
    await #expect(
      throws: StreamError.responseTooLarge(
        limit: Data(oneChange.utf8).count, observed: Data(oneChange.utf8).count + 1)
    ) {
      try await byteReader.run()
    }
    #expect(await materializer.rows() == ["old": ["id": .int(0)]])
    #expect(try await materializer.currentCursor() == prior)
    #expect(await materializer.applications == 0)

    let events =
      "[" + Array(repeating: oneChange.dropFirst().dropLast(), count: 2).joined(separator: ",")
      + "]"
    let eventTransport = BoundsTransport(responses: [
      boundsResponse(events, headers: ["stream-next-offset": "42"])
    ])
    let eventReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: eventTransport,
      materializer: materializer, startingAt: prior,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 10_000, maximumChangeEventsPerStreamBatch: 1))
    await #expect(throws: StreamError.batchTooLarge(limit: 1, observed: 2)) {
      try await eventReader.run()
    }
    #expect(await materializer.rows() == ["old": ["id": .int(0)]])
    #expect(try await materializer.currentCursor() == prior)
    #expect(await materializer.applications == 0)
  }

  @Test func exactEventLimitAndEmptyCursorAdvancementPass() async throws {
    let oneTransport = BoundsTransport(responses: [
      boundsResponse(oneChange, headers: ["stream-next-offset": "42"])
    ])
    let applied = BoundsMaterializer(cursor: .beginning)
    let oneReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: oneTransport,
      materializer: applied,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 10_000, maximumChangeEventsPerStreamBatch: 1))
    let oneTask = Task { try await oneReader.run() }
    while await applied.applications == 0 { await Task.yield() }
    oneTask.cancel()
    await #expect(throws: CancellationError.self) { try await oneTask.value }
    #expect(await applied.rows()["1"] == ["id": .int(1)])
    #expect(try await applied.currentCursor() == StreamCursor(offset: "42"))

    let empty = "[]"
    let emptyTransport = BoundsTransport(responses: [
      boundsResponse(empty, headers: ["stream-next-offset": "43"])
    ])
    let emptyReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: emptyTransport,
      materializer: applied,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: Data(empty.utf8).count, maximumChangeEventsPerStreamBatch: 1))
    let emptyTask = Task { try await emptyReader.run() }
    while (try? await applied.currentCursor()) != StreamCursor(offset: "43") { await Task.yield() }
    emptyTask.cancel()
    await #expect(throws: CancellationError.self) { try await emptyTask.value }
    #expect(await applied.rows()["1"] == ["id": .int(1)])
  }

  @Test func malformedPrecedenceAndCancellationLeaveProviderStateUnchanged() async throws {
    let prior = StreamCursor(offset: "41")
    let materializer = BoundsMaterializer(rows: ["old": ["id": .int(0)]], cursor: prior)
    let overLimitMalformed = BoundsTransport(responses: [
      boundsResponse("not-json!", headers: ["stream-next-offset": "42"])
    ])
    let overLimitReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: overLimitMalformed,
      materializer: materializer, startingAt: prior,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 8, maximumChangeEventsPerStreamBatch: 1))
    await #expect(throws: StreamError.responseTooLarge(limit: 8, observed: 9)) {
      try await overLimitReader.run()
    }

    let malformed = BoundsTransport(responses: [boundsResponse("not-json")])
    let malformedReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: malformed,
      materializer: materializer, startingAt: prior,
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 8, maximumChangeEventsPerStreamBatch: 1))
    await #expect {
      try await malformedReader.run()
    } throws: { error in
      guard case StreamError.decoding = error else { return false }
      return true
    }
    #expect(await materializer.rows() == ["old": ["id": .int(0)]])
    #expect(try await materializer.currentCursor() == prior)

    let blocking = BoundsTransport(responses: [])
    let cancellationReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: blocking,
      materializer: materializer, startingAt: prior)
    let task = Task { try await cancellationReader.run() }
    while await blocking.requests.isEmpty { await Task.yield() }
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(await materializer.rows() == ["old": ["id": .int(0)]])
    #expect(try await materializer.currentCursor() == prior)
  }

  @Test func oversizedTerminalAndRetryableHTTPBodiesKeepTheirStatusSemantics() async throws {
    let oversized = "too-long!"
    let limits = ResponseDecodingLimits(
      maximumDecodedResponseBytes: 8, maximumChangeEventsPerStreamBatch: 1)
    for (status, headers, expected) in [
      (
        404, [String: String](),
        StreamError.terminal(path: "/shape/s1", status: 404, reason: .notFound)
      ),
      (
        200, ["stream-closed": "true"],
        StreamError.terminal(path: "/shape/s1", status: 200, reason: .closed)
      ),
    ] {
      let reader = ShapeStreamReader(
        streamURL: URL(string: "https://streams.test/shape/s1")!,
        transport: BoundsTransport(responses: [
          boundsResponse(oversized, status: status, headers: headers)
        ]),
        materializer: BoundsMaterializer(), responseDecodingLimits: limits)
      await #expect(throws: expected) { try await reader.run() }
    }

    let retryableReader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: BoundsTransport(responses: [boundsResponse(oversized, status: 503)]),
      materializer: BoundsMaterializer(), responseDecodingLimits: limits)
    await #expect(throws: ClientError.retryableHTTP(status: 503, retryAfter: nil)) {
      try await retryableReader.run()
    }

    let transport = BoundsTransport(responses: [
      boundsResponse(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/shape/s1","streamUrl":"https://streams.test/shape/s1"}"#
      ),
      boundsResponse(oversized, status: 503),
      boundsResponse(oversized, status: 404),
      boundsResponse("", status: 404),
    ])
    let clock = BoundsClock()
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport,
        responseDecodingLimits: .init(
          maximumDecodedResponseBytes: 10_000, maximumChangeEventsPerStreamBatch: 1)),
      transport: transport, request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: BoundsMaterializer(),
      retryPolicy: .init(maxRetries: 1, baseDelay: .zero, maximumDelay: .zero, jitterRatio: 0),
      clock: clock, responseDecodingLimits: limits)
    _ = try await coordinator.start()
    for _ in 0..<10_000 {
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
                shapeId: "s1", table: "public.items", streamPath: "/shape/s1",
                streamURL: URL(string: "https://streams.test/shape/s1")!, subscription: "claim")))))
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "GET", "GET", "DELETE"])
    #expect(await clock.delays == [.zero])
  }

  @Test func coordinatorClassifiesOverLimitAsTerminalAndDoesNotRetryOrLeakPayloadToTelemetry()
    async throws
  {
    let secret = "Bearer super-secret-token"
    let transport = BoundsTransport(responses: [
      boundsResponse(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/shape/s1","streamUrl":"https://streams.test/shape/s1"}"#
      ),
      boundsResponse(secret, headers: ["stream-next-offset": "42"]),
    ])
    let sink = BoundsTelemetrySink()
    let telemetry = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!, authorization: secret,
        maxQueueSize: 8, maxBatchSize: 8), sink: sink)
    let materializer = BoundsMaterializer(cursor: .beginning)
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: telemetry),
      transport: transport, request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: materializer, retryPolicy: .init(maxRetries: 8),
      clock: ContinuousShapeSubscriptionClock(),
      responseDecodingLimits: .init(
        maximumDecodedResponseBytes: 8, maximumChangeEventsPerStreamBatch: 1),
      telemetry: telemetry)

    _ = try await coordinator.start()
    for _ in 0..<10_000 {
      if case .failed = await coordinator.state { break }
      await Task.yield()
    }
    #expect(await coordinator.state == .failed(.stream(.responseTooLarge(limit: 8, observed: 9))))
    #expect(await transport.requests.count == 2)
    #expect(await materializer.rows().isEmpty)
    #expect(try await materializer.currentCursor() == .beginning)
    await telemetry.flush()
    let emitted = await sink.requests.compactMap(\.httpBody).map {
      String(decoding: $0, as: UTF8.self)
    }.joined()
    #expect(!emitted.contains(secret))
    #expect(emitted.contains("response_too_large"))
    await telemetry.shutdown()
  }
}
