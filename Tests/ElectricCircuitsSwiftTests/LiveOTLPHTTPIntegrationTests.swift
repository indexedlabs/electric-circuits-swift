import Foundation
@preconcurrency import Network
import Testing

@testable import ElectricCircuitsSwift

private struct CapturedOTLPRequest: Sendable {
  let method: String
  let path: String
  let headers: [String: String]
  let body: Data
}

private actor AsyncGate {
  private var opened = false
  private var waiters: [UUID: CheckedContinuation<Void, Never>] = [:]

  func open() {
    guard !opened else { return }
    opened = true
    let pending = waiters.values
    waiters.removeAll()
    for waiter in pending {
      waiter.resume()
    }
  }

  func wait() async {
    guard !opened else { return }
    let id = UUID()
    await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        if opened {
          continuation.resume()
        } else {
          waiters[id] = continuation
        }
      }
    } onCancel: {
      Task { await self.cancelWait(id) }
    }
  }

  private func cancelWait(_ id: UUID) {
    waiters.removeValue(forKey: id)?.resume()
  }
}

private actor CollectorRecord {
  private var requests: [CapturedOTLPRequest] = []
  private var requestGates: [Int: AsyncGate] = [:]

  func append(_ request: CapturedOTLPRequest) async {
    requests.append(request)
    for (count, gate) in requestGates where requests.count >= count {
      await gate.open()
      requestGates.removeValue(forKey: count)
    }
  }

  func waitForRequests(_ count: Int) async {
    guard requests.count < count else { return }
    let gate = requestGates[count] ?? AsyncGate()
    requestGates[count] = gate
    await gate.wait()
  }

  func snapshot() -> [CapturedOTLPRequest] { requests }
}

private enum CollectorResponse: Sendable {
  case status(Int)
  case blackhole
}

/// A disposable real loopback OTLP/HTTP collector. It owns the listener and independently records
/// the complete HTTP request before replying (or intentionally withholding a reply).
private final class LiveOTLPCollector: @unchecked Sendable {
  private let listener: NWListener
  private let response: CollectorResponse
  private let record = CollectorRecord()
  private let arrived = AsyncGate()
  private let ready = AsyncGate()
  private let stopped = AsyncGate()
  private let lock = NSLock()
  private var connections: [NWConnection] = []
  private var port: UInt16?

  init(response: CollectorResponse) throws {
    listener = try NWListener(using: .tcp, on: .any)
    self.response = response
    listener.stateUpdateHandler = { [ready, stopped] state in
      switch state {
      case .ready:
        Task { await ready.open() }
      case .cancelled, .failed:
        Task { await stopped.open() }
      default:
        break
      }
    }
    listener.newConnectionHandler = { [weak self] connection in self?.accept(connection) }
    listener.start(queue: .global(qos: .userInitiated))
  }

  func endpoint(path: String) async -> URL {
    await ready.wait()
    let port = listener.port!.rawValue
    lock.withLock { self.port = port }
    return URL(string: "http://127.0.0.1:\(port)\(path)")!
  }

  func waitForRequests(_ count: Int) async { await record.waitForRequests(count) }
  func waitForArrival() async { await arrived.wait() }
  func requests() async -> [CapturedOTLPRequest] { await record.snapshot() }

  /// Cancels every accepted connection and the listener, then requires either Network's terminal
  /// state or an independent connection-refused probe on this exact ephemeral port.
  func stop() async -> Bool {
    lock.withLock {
      for connection in connections {
        connection.cancel()
      }
      connections.removeAll()
      listener.cancel()
    }
    if await completesWithin(.seconds(1), { await self.stopped.wait() }) { return true }
    return await portHasDisappeared()
  }

  private func accept(_ connection: NWConnection) {
    lock.withLock { connections.append(connection) }
    connection.start(queue: .global(qos: .userInitiated))
    receive(connection, buffer: Data())
  }

  private func receive(_ connection: NWConnection, buffer: Data) {
    connection.receive(minimumIncompleteLength: 1, maximumLength: 64 * 1024) {
      [weak self] data, _, complete, error in
      guard let self else { return }
      var buffer = buffer
      if let data { buffer.append(data) }
      if buffer.range(of: Data("\r\n\r\n".utf8)) != nil {
        Task { await self.arrived.open() }
      }
      if let request = Self.parseRequest(buffer) {
        Task { await self.record.append(request) }
        switch self.response {
        case .status(let status):
          let reason = (200..<300).contains(status) ? "Accepted" : "Failure"
          let reply = Data(
            "HTTP/1.1 \(status) \(reason)\r\nContent-Length: 0\r\nConnection: close\r\n\r\n".utf8)
          connection.send(
            content: reply, completion: .contentProcessed { _ in connection.cancel() })
        case .blackhole:
          break
        }
        return
      }
      if complete || error != nil {
        connection.cancel()
        return
      }
      self.receive(connection, buffer: buffer)
    }
  }

  private static func parseRequest(_ data: Data) -> CapturedOTLPRequest? {
    let separator = Data("\r\n\r\n".utf8)
    guard let range = data.range(of: separator) else { return nil }
    let headerText = String(decoding: data[..<range.lowerBound], as: UTF8.self)
    let lines = headerText.split(separator: "\r\n", omittingEmptySubsequences: false)
    guard let requestLine = lines.first?.split(separator: " "), requestLine.count >= 2 else {
      return nil
    }
    var headers: [String: String] = [:]
    for line in lines.dropFirst() {
      guard let colon = line.firstIndex(of: ":") else { continue }
      headers[String(line[..<colon]).lowercased()] = String(line[line.index(after: colon)...])
        .trimmingCharacters(in: .whitespaces)
    }
    let bodyStart = range.upperBound
    let body: Data
    if headers["transfer-encoding"]?.lowercased().contains("chunked") == true {
      guard let decoded = decodeChunkedBody(data, startingAt: bodyStart) else { return nil }
      body = decoded
    } else {
      let contentLength = Int(headers["content-length"] ?? "0") ?? 0
      guard data.count >= bodyStart + contentLength else { return nil }
      body = data.subdata(in: bodyStart..<(bodyStart + contentLength))
    }
    return CapturedOTLPRequest(
      method: String(requestLine[0]), path: String(requestLine[1]), headers: headers,
      body: body)
  }

  private static func decodeChunkedBody(_ data: Data, startingAt start: Int) -> Data? {
    let carriageReturnLineFeed = Data("\r\n".utf8)
    var position = start
    var body = Data()
    while true {
      guard let line = data.range(of: carriageReturnLineFeed, in: position..<data.count),
        let count = Int(
          String(decoding: data[position..<line.lowerBound], as: UTF8.self), radix: 16)
      else { return nil }
      position = line.upperBound
      guard data.count >= position + count + carriageReturnLineFeed.count else { return nil }
      body.append(data[position..<(position + count)])
      position += count + carriageReturnLineFeed.count
      if count == 0 { return body }
    }
  }

  private func portHasDisappeared() async -> Bool {
    guard let port = lock.withLock({ self.port }) else { return false }
    var request = URLRequest(url: URL(string: "http://127.0.0.1:\(port)/collector-stopped")!)
    request.timeoutInterval = 0.2
    do {
      _ = try await URLSession.shared.data(for: request)
      return false
    } catch let error as URLError {
      return error.code == .cannotConnectToHost || error.code == .networkConnectionLost
    } catch {
      return false
    }
  }
}

private actor TransportFailureGate {
  private let gate = AsyncGate()
  func receive(_ health: TelemetryHealth) async {
    if health.transportFailures > 0 { await gate.open() }
  }
  func wait() async { await gate.wait() }
}

private actor CausalStreamTransport: HTTPTransport {
  private let firstPoll = AsyncGate()
  private let releaseFirstResponse = AsyncGate()
  private let secondPoll = AsyncGate()
  private let releaseSecondResponse = AsyncGate()
  private var calls = 0
  private(set) var requests: [URLRequest] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let call = calls
    calls += 1
    if call == 0 {
      await firstPoll.open()
      await releaseFirstResponse.wait()
      let validBody =
        "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"
      return HTTPResponse(
        data: Data(validBody.utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil,
          headerFields: ["stream-next-offset": "10"])!)
    }
    if call == 1 {
      await secondPoll.open()
      await releaseSecondResponse.wait()
      return HTTPResponse(
        data: Data("gone".utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
    }
    throw ClientError.invalidResponse
  }

  func waitForFirstPoll() async { await firstPoll.wait() }
  func allowFirstResponse() async { await releaseFirstResponse.open() }
  func waitForSecondPoll() async { await secondPoll.wait() }
  func allowSecondResponse() async { await releaseSecondResponse.open() }
}

private final class CompletionRace: @unchecked Sendable {
  private let lock = NSLock()
  private var continuation: CheckedContinuation<Bool, Never>?

  init(_ continuation: CheckedContinuation<Bool, Never>) { self.continuation = continuation }

  func resolve(_ value: Bool) {
    let continuation = lock.withLock {
      let continuation = self.continuation
      self.continuation = nil
      return continuation
    }
    continuation?.resume(returning: value)
  }
}

private func completesWithin(
  _ duration: Duration, _ operation: @escaping @Sendable () async -> Void
)
  async -> Bool
{
  await withCheckedContinuation { continuation in
    let race = CompletionRace(continuation)
    let waiter = Task {
      await operation()
      race.resolve(true)
    }
    Task {
      try? await Task.sleep(for: duration)
      race.resolve(false)
      waiter.cancel()
    }
  }
}

@Suite("live OTLP/HTTP exporter", .serialized)
struct LiveOTLPHTTPIntegrationTests {
  @Test func blackholeExporterTimesOutWithoutBlockingTheCaller() async throws {
    let collector = try LiveOTLPCollector(response: .blackhole)
    let failures = TransportFailureGate()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: await collector.endpoint(path: "/v1/metrics"), exportTimeout: 0.1),
      sink: URLSessionTelemetrySink(),
      healthCallback: { health in Task { await failures.receive(health) } })
    reporter.recordCounter("electric.http.requests")
    let arrived = await completesWithin(.seconds(2)) { await collector.waitForArrival() }
    #expect(arrived)
    guard arrived else {
      await reporter.shutdown()
      #expect(await collector.stop())
      return
    }
    #expect(await completesWithin(.seconds(2)) { await failures.wait() })
    await reporter.shutdown()
    #expect(await collector.stop())
  }

  @Test func configuredCollectorReceivesValidOTLPTraceAndMetricWithoutSecrets() async throws {
    let collector = try LiveOTLPCollector(response: .status(202))
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: await collector.endpoint(path: "/qualified/v1/traces"),
        metricsEndpoint: await collector.endpoint(path: "/qualified/v1/metrics"),
        authorization: "Bearer collector-secret", serviceName: "swift-qualified",
        environment: "test",
        samplingRate: 1, exportTimeout: 0.5), sink: URLSessionTelemetrySink())
    let span = reporter.beginSpan(name: "electric.client.request", kind: .client)
    reporter.endSpan(
      span,
      attributes: [
        "http.method": "POST", "electric.table": "issues", "authorization": "collector-secret",
        "cookie": "session-secret", "row": "row-secret", "predicate": "predicate-secret",
        "subscription": "raw-subscription-secret", "primary_key": "pk-secret",
      ])
    reporter.recordCounter("electric.http.requests", value: 1, attributes: ["http.method": "POST"])
    await reporter.flush()
    await collector.waitForRequests(2)
    let requests = await collector.requests()
    #expect(Set(requests.map(\.path)) == ["/qualified/v1/traces", "/qualified/v1/metrics"])
    #expect(requests.allSatisfy { $0.method == "POST" })
    #expect(requests.allSatisfy { $0.headers["authorization"] == "Bearer collector-secret" })
    #expect(requests.allSatisfy { $0.headers["content-type"] == "application/json" })
    #expect(requests.allSatisfy { (try? JSONSerialization.jsonObject(with: $0.body)) != nil })
    let payloads = requests.map { String(decoding: $0.body, as: UTF8.self) }
    #expect(
      payloads.contains {
        $0.contains("resourceSpans") && $0.contains("electric.client.request")
          && $0.contains("traceId")
      })
    #expect(
      payloads.contains {
        $0.contains("resourceMetrics") && $0.contains("electric.http.requests")
          && $0.contains("aggregationTemporality")
      })
    let allPayload = payloads.joined(separator: "\n")
    for secret in [
      "collector-secret", "session-secret", "row-secret", "predicate-secret",
      "raw-subscription-secret", "pk-secret",
    ] {
      #expect(!allPayload.contains(secret))
    }
    await reporter.shutdown()
    #expect(await collector.stop())
  }

  @Test func exportTimeoutIsClampedToTheDocumentedFiniteRange() {
    #expect(TelemetryConfiguration(exportTimeout: -1).exportTimeout == 0.05)
    #expect(TelemetryConfiguration(exportTimeout: 100).exportTimeout == 60)
    var configuration = TelemetryConfiguration(exportTimeout: -1)
    configuration.samplingRate = 0.5  // Legal post-init mutation cannot alter immutable timeout.
    #expect(configuration.exportTimeout == 0.05)
  }

  @Test func samplingAndDisabledTelemetryDoNotProduceCollectorTraffic() async throws {
    let collector = try LiveOTLPCollector(response: .status(202))
    let endpoint = await collector.endpoint(path: "/v1/traces")
    let unsampled = TelemetryReporter(
      configuration: TelemetryConfiguration(tracesEndpoint: endpoint, samplingRate: 0),
      sink: URLSessionTelemetrySink())
    unsampled.endSpan(unsampled.beginSpan(name: "electric.unsampled", kind: .client))
    await unsampled.flush()
    #expect(await collector.requests().isEmpty)

    let sampled = TelemetryReporter(
      configuration: TelemetryConfiguration(tracesEndpoint: endpoint, samplingRate: 1),
      sink: URLSessionTelemetrySink())
    sampled.endSpan(sampled.beginSpan(name: "electric.sampled", kind: .client))
    await sampled.flush()
    await collector.waitForRequests(1)
    #expect((await collector.requests()).count == 1)

    let disabled = TelemetryReporter(configuration: .disabled, sink: URLSessionTelemetrySink())
    disabled.recordCounter("electric.disabled")
    await disabled.flush()
    #expect((await disabled.health()).accepted == 0)
    #expect((await collector.requests()).count == 1)
    await unsampled.shutdown()
    await sampled.shutdown()
    await disabled.shutdown()
    #expect(await collector.stop())
  }

  @Test(arguments: [401, 403, 429, 500])
  func nonSuccessCollectorResponsesAreContained(_ status: Int) async throws {
    let collector = try LiveOTLPCollector(response: .status(status))
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: await collector.endpoint(path: "/v1/metrics"), exportTimeout: 0.5),
      sink: URLSessionTelemetrySink())
    reporter.recordCounter("electric.http.requests")
    await reporter.flush()
    await collector.waitForRequests(1)
    #expect((await reporter.health()).nonSuccessResponses == 1)
    #expect((await reporter.health()).transportFailures == 0)
    await reporter.shutdown()
    #expect(await collector.stop())
  }

  @Test func blackholeQueueIsBoundedAndCannotGateMaterializationOrCursorCommit() async throws {
    let collector = try LiveOTLPCollector(response: .blackhole)
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: await collector.endpoint(path: "/v1/metrics"), maxQueueSize: 2,
        maxBatchSize: 1, exportTimeout: 0.5), sink: URLSessionTelemetrySink())
    reporter.recordCounter("electric.queue.1")
    reporter.recordCounter("electric.queue.2")
    reporter.recordCounter("electric.queue.3")
    reporter.recordCounter("electric.queue.4")
    await collector.waitForArrival()
    let health = await reporter.health()
    // One event can already be in flight while the bounded two-event queue fills.
    #expect(health.accepted <= 3)
    #expect(health.dropped >= 1)

    let transport = CausalStreamTransport()
    let materializer = InMemoryShapeMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/causal")!, transport: transport,
      materializer: materializer, telemetry: reporter)
    let readerTask = Task { try await reader.run() }
    await transport.waitForFirstPoll()
    let firstRequests = await transport.requests
    #expect(firstRequests.count == 1)
    #expect(
      URLComponents(url: firstRequests[0].url!, resolvingAgainstBaseURL: false)?.queryItems == [
        URLQueryItem(name: "offset", value: "-1"),
        URLQueryItem(name: "live", value: "long-poll"),
      ])
    await transport.allowFirstResponse()
    await transport.waitForSecondPoll()
    let resumedRequests = await transport.requests
    #expect(resumedRequests.count == 2)
    #expect(
      URLComponents(url: resumedRequests[1].url!, resolvingAgainstBaseURL: false)?.queryItems == [
        URLQueryItem(name: "offset", value: "10"),
        URLQueryItem(name: "live", value: "long-poll"),
      ])
    await transport.allowSecondResponse()
    await #expect(
      throws: StreamError.terminal(path: "/shape/causal", status: 404, reason: .notFound)
    ) {
      try await readerTask.value
    }
    #expect(await materializer.cursor() == StreamCursor(offset: "10"))
    #expect(await materializer.rows().map(\.key) == ["1"])
    await reporter.shutdown()
    #expect(await collector.stop())
  }
}
