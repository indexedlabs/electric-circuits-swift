import Foundation

/// Destination used by the built-in, Foundation-only OTLP/HTTP JSON exporter.
/// Implementations must return promptly on cancellation; telemetry failures are intentionally
/// contained and never affect a client request or stream materialization.
public protocol TelemetrySink: Sendable {
  func send(_ request: URLRequest) async throws -> HTTPResponse
}

/// Foundation's URLSession transport is also suitable for telemetry export.
public struct URLSessionTelemetrySink: TelemetrySink {
  public init() {}
  public func send(_ request: URLRequest) async throws -> HTTPResponse {
    let (data, response) = try await URLSession.shared.data(for: request)
    guard let response = response as? HTTPURLResponse else { throw ClientError.invalidResponse }
    return HTTPResponse(data: data, response: response)
  }
}

/// Opt-in exporter settings. Leaving both endpoints nil is a no-op configuration.
public struct TelemetryConfiguration: Sendable, Equatable {
  public var tracesEndpoint: URL?
  public var metricsEndpoint: URL?
  public var authorization: String?
  public var serviceName: String
  public var environment: String?
  /// A value in `0...1`; values outside this range are clamped.
  public var samplingRate: Double
  public var maxQueueSize: Int
  public var maxBatchSize: Int
  /// Per-export HTTP deadline. A timed-out export is recorded as a local transport failure and
  /// never delays the application's request or materialization path. Values are clamped to
  /// `0.05...60` seconds so every configured exporter has a finite bound.
  public let exportTimeout: TimeInterval

  public init(
    tracesEndpoint: URL? = nil, metricsEndpoint: URL? = nil, authorization: String? = nil,
    serviceName: String = "electric-circuits-swift", environment: String? = nil,
    samplingRate: Double = 1, maxQueueSize: Int = 256, maxBatchSize: Int = 64,
    exportTimeout: TimeInterval = 5
  ) {
    self.tracesEndpoint = tracesEndpoint
    self.metricsEndpoint = metricsEndpoint
    self.authorization = authorization
    self.serviceName = String(serviceName.prefix(128))
    self.environment = environment.map { String($0.prefix(128)) }
    self.samplingRate = min(1, max(0, samplingRate))
    self.maxQueueSize = max(1, maxQueueSize)
    self.maxBatchSize = max(1, maxBatchSize)
    self.exportTimeout = min(60, max(0.05, exportTimeout))
  }

  public static let disabled = TelemetryConfiguration(samplingRate: 0)
}

public enum TelemetrySpanKind: String, Sendable {
  case client = "CLIENT"
  case internalSpan = "INTERNAL"

  fileprivate var otlpProtoValue: Int {
    switch self {
    case .internalSpan: return 1
    case .client: return 3
    }
  }
}

/// W3C trace context. IDs are lowercase fixed-width hex strings and are safe to transmit in the
/// `traceparent` header, but not exported as arbitrary attributes.
public struct TelemetrySpan: Sendable {
  public let traceID: String
  public let spanID: String
  public let parentSpanID: String?
  public let sampled: Bool
  fileprivate let name: String
  fileprivate let kind: TelemetrySpanKind
  fileprivate let startedAt: UInt64
}

private struct ParsedTraceparent: Sendable {
  let traceID: String
  let spanID: String
  let sampled: Bool
}

public struct TelemetryHealth: Sendable, Equatable {
  public var accepted: Int = 0
  public var dropped: Int = 0
  public var encodingFailures: Int = 0
  public var transportFailures: Int = 0
  public var nonSuccessResponses: Int = 0
  public init() {}
}

/// A bounded, best-effort reporter. It owns exactly one draining worker at a time; calls to
/// `recordCounter` and `endSpan` are synchronous and cannot delay application synchronization.
public final class TelemetryReporter: @unchecked Sendable {
  public static let noop = TelemetryReporter(configuration: .disabled, sink: nil)

  private enum Event: Sendable {
    case span(TelemetrySpan, UInt64, [String: String])
    case counter(String, Double, [String: String])
    case gauge(String, Double, [String: String])
  }
  private struct State {
    var queued: [Event] = []
    var worker: Task<Void, Never>?
    var stopped = false
    var health = TelemetryHealth()
    var notifyingHealth = false
    var healthNotificationPending = false
  }
  /// The worker captures this storage, never the reporter. This deliberately separates reporter
  /// lifetime from an in-flight URLSession/sink await.
  private final class Storage: @unchecked Sendable {
    let lock = NSLock()
    var state = State()
  }

  private let configuration: TelemetryConfiguration
  private let sink: (any TelemetrySink)?
  private let storage = Storage()
  private let healthCallback: (@Sendable (TelemetryHealth) -> Void)?

  public init(
    configuration: TelemetryConfiguration = .disabled, sink: (any TelemetrySink)? = nil,
    healthCallback: (@Sendable (TelemetryHealth) -> Void)? = nil
  ) {
    self.configuration = configuration
    self.sink = sink
    self.healthCallback = healthCallback
  }

  public func beginSpan(name: String, kind: TelemetrySpanKind, parent: TelemetrySpan? = nil)
    -> TelemetrySpan
  {
    let traceID = parent?.traceID ?? Self.hexID(bytes: 16)
    return TelemetrySpan(
      traceID: traceID, spanID: Self.hexID(bytes: 8), parentSpanID: parent?.spanID,
      sampled: parent?.sampled ?? Self.shouldSample(configuration.samplingRate), name: name,
      kind: kind, startedAt: Self.unixNanoseconds())
  }

  /// Starts a span from a validated W3C parent. Invalid or unsupported headers deliberately start
  /// a fresh root so they cannot poison propagation or exported trace identity.
  public func beginSpan(
    name: String, kind: TelemetrySpanKind, parentTraceparent: String?
  ) -> TelemetrySpan {
    guard let parent = Self.parseTraceparent(parentTraceparent) else {
      return beginSpan(name: name, kind: kind)
    }
    return TelemetrySpan(
      traceID: parent.traceID, spanID: Self.hexID(bytes: 8), parentSpanID: parent.spanID,
      sampled: parent.sampled, name: name, kind: kind, startedAt: Self.unixNanoseconds())
  }

  /// Adds a W3C traceparent only when the caller has not already supplied one.
  public func injectTraceContext(into request: inout URLRequest, span: TelemetrySpan? = nil) {
    if Self.parseTraceparent(request.value(forHTTPHeaderField: "traceparent")) != nil { return }
    guard configuration.tracesEndpoint != nil || configuration.metricsEndpoint != nil else {
      return
    }
    let span = span ?? beginSpan(name: "electric.http", kind: .client)
    request.setValue(
      "00-\(span.traceID)-\(span.spanID)-\(span.sampled ? "01" : "00")",
      forHTTPHeaderField: "traceparent")
  }

  public func endSpan(_ span: TelemetrySpan, attributes: [String: String] = [:]) {
    guard span.sampled else { return }
    enqueue(.span(span, Self.unixNanoseconds(), Self.sanitize(attributes)))
  }

  /// Records a monotonic delta sum. Values at this boundary are never cumulative.
  public func recordCounter(_ name: String, value: Double = 1, attributes: [String: String] = [:]) {
    guard value.isFinite else {
      recordEncodingFailure()
      return
    }
    enqueue(.counter(name, value, Self.sanitize(attributes)))
  }

  /// Records a point-in-time gauge, including duration measurements.
  public func recordGauge(_ name: String, value: Double, attributes: [String: String] = [:]) {
    guard value.isFinite else {
      recordEncodingFailure()
      return
    }
    enqueue(.gauge(name, value, Self.sanitize(attributes)))
  }

  public func health() async -> TelemetryHealth { storage.lock.withLock { storage.state.health } }

  /// Waits for the currently scheduled bounded worker. New events can schedule a later worker.
  public func flush() async {
    let task = storage.lock.withLock { storage.state.worker }
    await Self.wait(task)
  }

  /// Idempotently rejects future events, clears queued work, and cancels an in-flight export.
  public func shutdown() async {
    let task: Task<Void, Never>? = storage.lock.withLock {
      guard !storage.state.stopped else { return storage.state.worker }
      storage.state.stopped = true
      storage.state.queued.removeAll(keepingCapacity: false)
      if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
      let task = storage.state.worker
      storage.state.worker = nil
      return task
    }
    task?.cancel()
    await Self.wait(task)
    Self.deliverPendingHealth(storage: storage, callback: healthCallback)
  }

  private func enqueue(_ event: Event) {
    storage.lock.withLock {
      let enabled = configuration.tracesEndpoint != nil || configuration.metricsEndpoint != nil
      guard enabled, sink != nil else { return }
      guard !storage.state.stopped else {
        storage.state.health.dropped += 1
        if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
        return
      }
      guard storage.state.queued.count < configuration.maxQueueSize else {
        storage.state.health.dropped += 1
        if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
        scheduleWorkerLocked()
        return
      }
      storage.state.queued.append(event)
      storage.state.health.accepted += 1
      if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
      scheduleWorkerLocked()
    }
  }

  private func recordEncodingFailure() {
    storage.lock.withLock {
      let enabled = configuration.tracesEndpoint != nil || configuration.metricsEndpoint != nil
      guard enabled, sink != nil, !storage.state.stopped else { return }
      storage.state.health.encodingFailures += 1
      if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
      scheduleWorkerLocked()
    }
  }

  /// Called only while `storage.lock` is held.
  private func scheduleWorkerLocked() {
    guard storage.state.worker == nil else { return }
    let storage = storage
    let configuration = configuration
    let sink = sink
    let callback = healthCallback
    storage.state.worker = Task {
      await Self.drain(
        storage: storage, configuration: configuration, sink: sink, callback: callback)
    }
  }

  private static func drain(
    storage: Storage, configuration: TelemetryConfiguration, sink: (any TelemetrySink)?,
    callback: (@Sendable (TelemetryHealth) -> Void)?
  ) async {
    while !Task.isCancelled {
      deliverPendingHealth(storage: storage, callback: callback)
      let events: [Event] = storage.lock.withLock {
        guard !storage.state.stopped, !storage.state.queued.isEmpty else {
          storage.state.worker = nil  // atomic empty+unschedule while holding the queue lock
          return []
        }
        let count = min(configuration.maxBatchSize, storage.state.queued.count)
        let batch = Array(storage.state.queued.prefix(count))
        storage.state.queued.removeFirst(count)
        return batch
      }
      guard !events.isEmpty else { return }
      await export(
        events, storage: storage, configuration: configuration, sink: sink, callback: callback)
    }
  }

  private static func export(
    _ events: [Event], storage: Storage, configuration: TelemetryConfiguration,
    sink: (any TelemetrySink)?, callback: (@Sendable (TelemetryHealth) -> Void)?
  ) async {
    guard let sink else { return }
    let spans = events.compactMap { event -> [String: Any]? in
      guard case .span(let span, let ended, let attributes) = event else { return nil }
      return Self.spanJSON(span, ended: ended, attributes: attributes)
    }
    let metrics = events.compactMap { event -> [String: Any]? in
      switch event {
      case .counter(let name, let value, let attributes):
        return Self.metricJSON(name, value, attributes, sum: true)
      case .gauge(let name, let value, let attributes):
        return Self.metricJSON(name, value, attributes, sum: false)
      case .span: return nil
      }
    }
    if !spans.isEmpty, let endpoint = configuration.tracesEndpoint {
      await post(
        endpoint, body: traceBody(spans, configuration: configuration), sink: sink,
        storage: storage, configuration: configuration, callback: callback)
    }
    if !metrics.isEmpty, let endpoint = configuration.metricsEndpoint {
      await post(
        endpoint, body: metricsBody(metrics, configuration: configuration), sink: sink,
        storage: storage, configuration: configuration, callback: callback)
    }
  }

  private static func post(
    _ endpoint: URL, body: [String: Any], sink: any TelemetrySink, storage: Storage,
    configuration: TelemetryConfiguration, callback: (@Sendable (TelemetryHealth) -> Void)?
  ) async {
    let data: Data
    do {
      guard JSONSerialization.isValidJSONObject(body) else {
        updateHealth(storage: storage) { $0.encodingFailures += 1 }
        return
      }
      data = try JSONSerialization.data(withJSONObject: body, options: [.sortedKeys])
    } catch {
      updateHealth(storage: storage) { $0.encodingFailures += 1 }
      return
    }
    do {
      var request = URLRequest(url: endpoint)
      request.httpMethod = "POST"
      request.timeoutInterval = configuration.exportTimeout
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
      request.setValue("application/json", forHTTPHeaderField: "Accept")
      if let authorization = configuration.authorization {
        request.setValue(authorization, forHTTPHeaderField: "Authorization")
      }
      request.httpBody = data
      let response = try await sink.send(request)
      if !(200..<300).contains(response.response.statusCode) {
        updateHealth(storage: storage) { $0.nonSuccessResponses += 1 }
      }
    } catch is CancellationError {
      // Explicit shutdown cancels a blocked sink; cancellation is not an application failure.
    } catch {
      updateHealth(storage: storage) { $0.transportFailures += 1 }
    }
  }

  private static func updateHealth(storage: Storage, _ mutate: (inout TelemetryHealth) -> Void) {
    storage.lock.withLock {
      mutate(&storage.state.health)
      if !storage.state.notifyingHealth { storage.state.healthNotificationPending = true }
    }
  }

  /// Called only by the reporter worker or explicit shutdown, never by the producer path.
  private static func deliverPendingHealth(
    storage: Storage, callback: (@Sendable (TelemetryHealth) -> Void)?
  ) {
    let health = storage.lock.withLock { () -> TelemetryHealth? in
      guard storage.state.healthNotificationPending, !storage.state.notifyingHealth else {
        return nil
      }
      storage.state.healthNotificationPending = false
      storage.state.notifyingHealth = true
      return storage.state.health
    }
    guard let health else { return }
    callback?(health)
    storage.lock.withLock { storage.state.notifyingHealth = false }
  }

  private static func wait(_ task: Task<Void, Never>?) async { _ = await task?.value }
  private static func shouldSample(_ rate: Double) -> Bool {
    rate >= 1 || (rate > 0 && Double.random(in: 0..<1) < rate)
  }
  private static func unixNanoseconds() -> UInt64 {
    UInt64(Date().timeIntervalSince1970 * 1_000_000_000)
  }

  private static func hexID(bytes: Int) -> String {
    UUID().uuidString.replacingOccurrences(of: "-", with: "").prefix(bytes * 2).lowercased()
  }

  /// Strict W3C v00 parser: exactly four fields, lowercase ASCII hexadecimal widths, nonzero
  /// trace/span identifiers, and a two-lowercase-hex-digit flags field. Valid caller headers
  /// remain unchanged on the outbound request.
  private static func parseTraceparent(_ value: String?) -> ParsedTraceparent? {
    guard let value else { return nil }
    let fields = value.split(separator: "-", omittingEmptySubsequences: false)
    guard fields.count == 4, fields[0].lowercased() == "00",
      let traceID = normalizedHex(fields[1], width: 32),
      let spanID = normalizedHex(fields[2], width: 16),
      let flags = normalizedHex(fields[3], width: 2),
      traceID.contains(where: { $0 != "0" }), spanID.contains(where: { $0 != "0" }),
      let flagsValue = UInt8(flags, radix: 16)
    else { return nil }
    return ParsedTraceparent(traceID: traceID, spanID: spanID, sampled: flagsValue & 1 == 1)
  }

  private static func normalizedHex(_ value: Substring, width: Int) -> String? {
    let bytes = value.utf8
    guard bytes.count == width,
      bytes.allSatisfy({ byte in
        (48...57).contains(byte) || (97...102).contains(byte)
      })
    else { return nil }
    return String(value)
  }

  private static let allowedAttributes: Set<String> = [
    "http.method", "http.status_code", "http.route", "electric.operation", "electric.table",
    "electric.stream_path", "error.type",
  ]
  private static func sanitize(_ attributes: [String: String]) -> [String: String] {
    var result: [String: String] = [:]
    for attribute in attributes.sorted(by: { $0.key < $1.key }).prefix(16) {
      guard allowedAttributes.contains(attribute.key) else { continue }
      result[String(attribute.key.prefix(64))] = String(attribute.value.prefix(128))
    }
    return result
  }

  private static func resource(configuration: TelemetryConfiguration) -> [String: Any] {
    var attributes: [[String: Any]] = [
      ["key": "service.name", "value": ["stringValue": configuration.serviceName]]
    ]
    if let environment = configuration.environment {
      attributes.append(["key": "deployment.environment", "value": ["stringValue": environment]])
    }
    return ["attributes": attributes]
  }
  private static func attributesJSON(_ attributes: [String: String]) -> [[String: Any]] {
    attributes.map { ["key": $0.key, "value": ["stringValue": $0.value]] }
  }
  private static func spanJSON(_ span: TelemetrySpan, ended: UInt64, attributes: [String: String])
    -> [String: Any]
  {
    [
      "traceId": span.traceID, "spanId": span.spanID, "parentSpanId": span.parentSpanID ?? "",
      "name": span.name,
      "kind": span.kind.otlpProtoValue, "startTimeUnixNano": String(span.startedAt),
      "endTimeUnixNano": String(ended),
      "attributes": attributesJSON(attributes),
    ]
  }
  private static func metricJSON(
    _ name: String, _ value: Double, _ attributes: [String: String], sum: Bool
  ) -> [String: Any] {
    let point: [String: Any] = [
      "asDouble": value, "timeUnixNano": String(unixNanoseconds()),
      "attributes": attributesJSON(attributes),
    ]
    return sum
      ? [
        "name": name,
        "sum": ["aggregationTemporality": 1, "isMonotonic": true, "dataPoints": [point]],
      ] : ["name": name, "gauge": ["dataPoints": [point]]]
  }
  private static func traceBody(_ spans: [[String: Any]], configuration: TelemetryConfiguration)
    -> [String: Any]
  {
    [
      "resourceSpans": [
        ["resource": resource(configuration: configuration), "scopeSpans": [["spans": spans]]]
      ]
    ]
  }
  private static func metricsBody(_ metrics: [[String: Any]], configuration: TelemetryConfiguration)
    -> [String: Any]
  {
    [
      "resourceMetrics": [
        [
          "resource": resource(configuration: configuration),
          "scopeMetrics": [["metrics": metrics]],
        ]
      ]
    ]
  }
}
