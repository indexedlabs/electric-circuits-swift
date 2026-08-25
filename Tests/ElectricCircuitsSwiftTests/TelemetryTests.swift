import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor RecordingTelemetrySink: TelemetrySink {
  private(set) var requests: [URLRequest] = []
  let status: Int

  init(status: Int = 202) { self.status = status }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return HTTPResponse(
      data: Data(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: nil)!)
  }
}

private actor BlockingTelemetrySink: TelemetrySink {
  private(set) var started = false

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    started = true
    while true {
      try Task.checkCancellation()
      await Task.yield()
    }
  }
}

private struct SinkFailure: Error {}
private struct FailingTelemetrySink: TelemetrySink {
  func send(_ request: URLRequest) async throws -> HTTPResponse { throw SinkFailure() }
}

private actor TraceRecordingTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let body =
      #"{"shapeId":"s","table":"public.items","streamPath":"/s","streamUrl":"https://streams.test/s"}"#
    return HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
  }
}

private final class PreparedRequestRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var values: [URLRequest] = []
  func append(_ request: URLRequest) { lock.withLock { values.append(request) } }
  func requests() -> [URLRequest] { lock.withLock { values } }
}

private struct PreparedTraceTransport: HTTPTransport {
  let traceparent: String
  let recorder = PreparedRequestRecorder()

  func prepare(_ request: URLRequest) -> URLRequest {
    var request = request
    request.setValue(traceparent, forHTTPHeaderField: "traceparent")
    return request
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    recorder.append(request)
    if request.httpMethod == "GET" {
      return HTTPResponse(
        data: Data("gone".utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
    }
    if request.httpMethod == "HEAD" {
      return HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil,
          headerFields: ["stream-next-offset": "1"])!)
    }
    let body =
      #"{"shapeId":"s","table":"public.items","streamPath":"/s","streamUrl":"https://streams.test/s"}"#
    return HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
  }
}

private final class ReentrantCallbackState: @unchecked Sendable {
  private let lock = NSLock()
  weak var reporter: TelemetryReporter?
  private var callbackCount = 0

  func callback() {
    lock.withLock { callbackCount += 1 }
    reporter?.recordCounter("electric.callback.event")
  }

  func count() -> Int { lock.withLock { callbackCount } }
}

@Suite("OpenTelemetry reporting", .serialized)
struct TelemetryTests {
  @Test func reporterEmitsTraceAndDeltaMetricWithRedactedAttributes() async throws {
    let sink = RecordingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!,
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!,
        authorization: "Bearer secret", serviceName: "circuits", environment: "test",
        maxQueueSize: 8, maxBatchSize: 8), sink: sink)
    let context = reporter.beginSpan(name: "electric.client.request", kind: .client)
    reporter.endSpan(
      context, attributes: ["http.method": "POST", "where": "secret", "url": "https://secret"])
    reporter.recordCounter("electric.http.requests", value: 1, attributes: ["http.method": "POST"])
    await reporter.flush()

    let requests = await sink.requests
    #expect(requests.count == 2)
    #expect(
      requests.allSatisfy { $0.value(forHTTPHeaderField: "Authorization") == "Bearer secret" })
    let bodies = requests.compactMap(\.httpBody).map { String(decoding: $0, as: UTF8.self) }
    #expect(
      bodies.contains {
        $0.contains("electric.client.request") && $0.contains("\"kind\":3")
      })
    #expect(
      bodies.contains {
        $0.contains("electric.http.requests") && $0.contains("\"aggregationTemporality\":1")
      })
    #expect(!bodies.joined().contains("secret"))
    await reporter.shutdown()
  }

  @Test func injectPreservesExistingTraceparentAndNoopIsSilent() async throws {
    var request = URLRequest(url: URL(string: "https://engine.test/v1/shapes")!)
    request.setValue(
      "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01", forHTTPHeaderField: "traceparent")
    let reporter = TelemetryReporter.noop
    reporter.injectTraceContext(into: &request)
    #expect(
      request.value(forHTTPHeaderField: "traceparent")
        == "00-0123456789abcdef0123456789abcdef-0123456789abcdef-01")
    await reporter.shutdown()
  }

  @Test func nativeClientInjectsW3CTraceparentAndSamplingCanSuppressSpans() async throws {
    let transport = TraceRecordingTransport()
    let sink = RecordingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!, samplingRate: 0),
      sink: sink)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: reporter)
    _ = try await client.createShape(ShapeRequest(table: "public.items", subscription: "fixed"))
    await reporter.flush()
    let traceparent = try #require(
      await transport.requests.first?.value(forHTTPHeaderField: "traceparent"))
    let parts = traceparent.split(separator: "-")
    #expect(
      parts.count == 4 && parts[0] == "00" && parts[1].count == 32 && parts[2].count == 16
        && parts[3] == "00")
    #expect(await sink.requests.isEmpty)
    await reporter.shutdown()
  }

  @Test func transportPreparedTraceparentWinsForControlAndStreamRequests() async throws {
    let traceID = "0123456789abcdef0123456789abcdef"
    let parentSpanID = "fedcba9876543210"
    let traceparent = "00-\(traceID)-\(parentSpanID)-01"
    let sink = RecordingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    var configured = URLRequest(url: URL(string: "https://engine.test")!)
    configured = URLSessionTransport(headers: ["traceparent": traceparent]).prepare(configured)
    reporter.injectTraceContext(into: &configured)
    #expect(configured.value(forHTTPHeaderField: "traceparent") == traceparent)

    let transport = PreparedTraceTransport(traceparent: traceparent)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: reporter)
    let handle = try await client.createShape(
      ShapeRequest(table: "public.items", subscription: "fixed"))
    _ = try await client.streamCursor(for: handle)
    let reader = ShapeStreamReader(
      streamURL: handle.stream.url, transport: transport, materializer: InMemoryShapeMaterializer(),
      telemetry: reporter)
    await #expect(
      throws: StreamError.terminal(path: "/s", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(transport.recorder.requests().count == 3)
    #expect(
      transport.recorder.requests().allSatisfy {
        $0.value(forHTTPHeaderField: "traceparent") == traceparent
      })
    await reporter.flush()
    let payloads = await sink.requests.compactMap(\.httpBody).map {
      String(decoding: $0, as: UTF8.self)
    }
    #expect(
      payloads.contains {
        $0.contains("\"name\":\"electric.client.request\"")
          && $0.contains("\"traceId\":\"\(traceID)\"")
          && $0.contains("\"parentSpanId\":\"\(parentSpanID)\"")
      })
    #expect(
      payloads.contains {
        $0.contains("\"name\":\"electric.stream.poll\"")
          && $0.contains("\"traceId\":\"\(traceID)\"")
          && $0.contains("\"parentSpanId\":\"\(parentSpanID)\"")
      })
    await reporter.shutdown()
  }

  @Test func malformedPreparedTraceparentIsReplaced() async throws {
    let malformed = "00-00000000000000000000000000000000-0123456789abcdef-01"
    let transport = PreparedTraceTransport(traceparent: malformed)
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: RecordingTelemetrySink())
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport, telemetry: reporter)
    _ = try await client.createShape(ShapeRequest(table: "public.items", subscription: "fixed"))
    let replacement = try #require(
      transport.recorder.requests().first?.value(forHTTPHeaderField: "traceparent"))
    #expect(replacement != malformed)
    let fields = replacement.split(separator: "-")
    #expect(
      fields.count == 4 && fields[0] == "00" && fields[1].count == 32 && fields[2].count == 16)
    await reporter.shutdown()
  }

  @Test(arguments: [
    "ff-0123456789abcdef0123456789abcdef-fedcba9876543210-01",
    "00-0123456789abcdef0123456789abcdef-0000000000000000-01",
    "00-0123456789abcdef0123456789abcdef-fedcba9876543210-gg",
    "00-0123456789abcdef0123456789abcdef-fedcba987654321-01",
  ])
  func malformedTraceparentFieldsAreNeverPreserved(_ malformed: String) async throws {
    var request = URLRequest(url: URL(string: "https://engine.test")!)
    request.setValue(malformed, forHTTPHeaderField: "traceparent")
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: RecordingTelemetrySink())
    reporter.injectTraceContext(into: &request)
    #expect(request.value(forHTTPHeaderField: "traceparent") != malformed)
    await reporter.shutdown()
  }

  @Test func uppercaseTraceAndParentIDsAreRejectedAndReplaced() async throws {
    let supplied = "00-ABCDEF0123456789ABCDEF0123456789-FEDCBA9876543210-03"
    let sink = RecordingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    var request = URLRequest(url: URL(string: "https://engine.test")!)
    request.setValue(supplied, forHTTPHeaderField: "traceparent")
    let span = reporter.beginSpan(
      name: "electric.client.request", kind: .client,
      parentTraceparent: request.value(forHTTPHeaderField: "traceparent"))
    reporter.injectTraceContext(into: &request, span: span)
    reporter.endSpan(span)
    await reporter.flush()
    let replacement = try #require(request.value(forHTTPHeaderField: "traceparent"))
    #expect(replacement != supplied)
    #expect(replacement == replacement.lowercased())
    let payload = try #require(await sink.requests.first?.httpBody)
    let text = String(decoding: payload, as: UTF8.self)
    #expect(!text.contains("\"parentSpanId\":\"fedcba9876543210\""))
    await reporter.shutdown()
  }

  @Test func nonSuccessAndBackpressureAreReportedWithoutThrowingToCaller() async throws {
    let sink = RecordingTelemetrySink(status: 503)
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        tracesEndpoint: URL(string: "https://collector.test/v1/traces")!,
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!, maxQueueSize: 1,
        maxBatchSize: 1), sink: sink)
    reporter.recordCounter("electric.http.requests", value: 1)
    reporter.recordCounter("electric.http.requests", value: 1)
    await reporter.flush()
    let health = await reporter.health()
    #expect(health.dropped >= 1)
    #expect(health.nonSuccessResponses == 1)
    await reporter.shutdown()
  }

  @Test func exporterOutageOnlyUpdatesLocalHealth() async throws {
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!),
      sink: FailingTelemetrySink())
    reporter.recordCounter("electric.http.requests")
    await reporter.flush()
    #expect((await reporter.health()).transportFailures == 1)
    await reporter.shutdown()
  }

  @Test func shutdownDropsAttemptsAndNaNIsAnEncodingFailure() async throws {
    let active = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!),
      sink: RecordingTelemetrySink())
    active.recordGauge("electric.materializer.duration_ms", value: .nan)
    await active.flush()
    #expect((await active.health()).encodingFailures == 1)
    #expect((await active.health()).transportFailures == 0)
    await active.shutdown()
    active.recordCounter("electric.http.requests")
    #expect((await active.health()).dropped == 1)
  }

  @Test func healthCallbackRunsOnWorkerAndDoesNotRecurseThroughTelemetry() async throws {
    let state = ReentrantCallbackState()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!),
      sink: RecordingTelemetrySink(), healthCallback: { _ in state.callback() })
    state.reporter = reporter
    reporter.recordCounter("electric.http.requests")
    #expect(state.count() == 0)  // producer path only schedules the worker
    await reporter.flush()
    #expect(state.count() == 1)
    #expect((await reporter.health()).accepted == 2)
    await reporter.shutdown()
  }

  @Test func enqueueAfterWorkerExitSchedulesAnotherWorker() async throws {
    let sink = RecordingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!),
      sink: sink)
    reporter.recordCounter("electric.http.requests")
    await reporter.flush()  // fence the worker's atomic empty-and-unschedule transition
    reporter.recordCounter("electric.http.requests")
    await reporter.flush()
    #expect(await sink.requests.count == 2)
    await reporter.shutdown()
  }

  @Test func shutdownCancelsABlockedSink() async throws {
    let sink = BlockingTelemetrySink()
    let reporter = TelemetryReporter(
      configuration: TelemetryConfiguration(
        metricsEndpoint: URL(string: "https://collector.test/v1/metrics")!),
      sink: sink)
    reporter.recordCounter("electric.http.requests")
    while await sink.started == false { await Task.yield() }
    await reporter.shutdown()
    #expect((await reporter.health()).accepted == 1)
  }
}
