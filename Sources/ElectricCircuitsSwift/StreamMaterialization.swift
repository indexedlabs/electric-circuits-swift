import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

// MARK: - Durable stream wire models

/// A row carried by a native shape stream. The package deliberately keeps row columns opaque;
/// an application (for example, a GRDB provider) decides how to decode them into its records.
public typealias ChangeRow = [String: JSONValue]

/// The operation stamped in `headers.operation` by the Rust durable-stream writer.
public enum ChangeOperation: String, Codable, Equatable, Sendable {
  case insert
  case update
  case upsert
  case delete
}

/// Headers carried with a durable-stream envelope. `offset` is a server-assigned delivery
/// position; `lsn` is an optional Postgres commit LSN and must not be used in place of `offset`.
public struct EnvelopeHeaders: Codable, Equatable, Sendable {
  public var operation: ChangeOperation
  public var txid: String?
  public var offset: String?
  public var lsn: String?
  public var seq: UInt64?
  public var last: Bool?

  public init(
    operation: ChangeOperation,
    txid: String? = nil,
    offset: String? = nil,
    lsn: String? = nil,
    seq: UInt64? = nil,
    last: Bool? = nil
  ) {
    self.operation = operation
    self.txid = txid
    self.offset = offset
    self.lsn = lsn
    self.seq = seq
    self.last = last
  }
}

/// One State-Protocol change event from a native shape stream.
public struct ChangeEnvelope: Codable, Equatable, Sendable {
  public var type: String
  /// Bare primary-key string used by this engine for LinearLite shape rows, not a quoted relation path.
  public var key: String
  public var value: ChangeRow?
  public var old: ChangeRow?
  public var headers: EnvelopeHeaders

  public init(
    type: String,
    key: String,
    value: ChangeRow? = nil,
    old: ChangeRow? = nil,
    headers: EnvelopeHeaders
  ) {
    self.type = type
    self.key = key
    self.value = value
    self.old = old
    self.headers = headers
  }
}

/// A stream entry that is not a row change: the engine's instruction to the stream's readers. A
/// shape group that fell behind and detached writes a `must-refetch` control when it reattaches;
/// the output before it is incomplete, and its readers reload the group from a snapshot.
public struct StreamControl: Codable, Equatable, Sendable {
  /// The envelope `type` the engine writes controls under. It names no table.
  public static let envelopeType = "__circuits.control"
  public static let mustRefetch = "must-refetch"

  public var control: String
  public var reason: String?
  /// The group that detached, and the commits whose output it dropped.
  public var groupID: String?
  public var resumeLSN: String?
  public var throughLSN: String?

  public init(
    control: String, reason: String? = nil, groupID: String? = nil, resumeLSN: String? = nil,
    throughLSN: String? = nil
  ) {
    self.control = control
    self.reason = reason
    self.groupID = groupID
    self.resumeLSN = resumeLSN
    self.throughLSN = throughLSN
  }

  public var isMustRefetch: Bool { control == Self.mustRefetch }

  enum CodingKeys: String, CodingKey {
    case control, reason
    case groupID = "groupId"
    case resumeLSN = "resumeLsn"
    case throughLSN = "throughLsn"
  }
}

/// The JSON body of one durable-stream read. Rust returns a JSON array, not an object containing
/// metadata, so this wrapper encodes and decodes as that array while giving materializers a stable
/// value type. Entries of type `StreamControl.envelopeType` are not row changes; they decode into
/// `controls` rather than `envelopes`.
public struct ChangeBatch: Codable, Equatable, Sendable {
  public var envelopes: [ChangeEnvelope]
  public var controls: [StreamControl]

  public init(envelopes: [ChangeEnvelope] = [], controls: [StreamControl] = []) {
    self.envelopes = envelopes
    self.controls = controls
  }

  public init(_ envelopes: [ChangeEnvelope]) {
    self.envelopes = envelopes
    controls = []
  }

  public var isEmpty: Bool { envelopes.isEmpty && controls.isEmpty }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.unkeyedContainer()
    for envelope in envelopes {
      try container.encode(envelope)
    }
    for control in controls {
      try container.encode(ControlEnvelope(value: control))
    }
  }

  public init(from decoder: Decoder) throws {
    var container = try decoder.unkeyedContainer()
    var decoded: [ChangeEnvelope] = []
    var controls: [StreamControl] = []
    while !container.isAtEnd {
      let entry = try container.superDecoder()
      let kind = try entry.container(keyedBy: EntryKind.CodingKeys.self)
      if try kind.decodeIfPresent(String.self, forKey: .type) == StreamControl.envelopeType {
        controls.append(try ControlEnvelope(from: entry).value)
      } else {
        decoded.append(try ChangeEnvelope(from: entry))
      }
    }
    envelopes = decoded
    self.controls = controls
  }

  private enum EntryKind {
    enum CodingKeys: String, CodingKey { case type }
  }

  /// A control's wire form: an envelope whose `value` carries the control.
  private struct ControlEnvelope: Codable {
    var type = StreamControl.envelopeType
    var key: String
    var value: StreamControl
    var headers = Headers()

    struct Headers: Codable { var operation = "control" }

    init(value: StreamControl) {
      key = value.control
      self.value = value
    }
  }
}

/// An opaque durable-stream checkpoint. The offset is the only resume authority. `lsn` is an
/// optional diagnostic/consistency watermark and is retained without parsing or comparing it.
public struct StreamCursor: Codable, Equatable, Hashable, Sendable {
  public var offset: String
  public var lsn: String?

  public init(offset: String, lsn: String? = nil) {
    self.offset = offset
    self.lsn = lsn
  }

  public static let beginning = StreamCursor(offset: "-1")
}

/// Identity of one materialized view. Providers must use the complete scope when naming their
/// local rows, memberships, and checkpoint; a server shape ID alone is not sufficient when an
/// account, query template, or feed generation changes.
public struct MaterializationScope: Codable, Equatable, Hashable, Sendable {
  public let principal: String
  public let template: String
  public let subscription: String
  public let generation: String

  public init(
    principal: String,
    template: String,
    subscription: String,
    generation: String
  ) {
    precondition(
      !principal.isEmpty && !template.isEmpty && !subscription.isEmpty && !generation.isEmpty)
    self.principal = principal
    self.template = template
    self.subscription = subscription
    self.generation = generation
  }

  /// A stable, collision-resistant storage key. Length prefixes avoid ambiguity from separators
  /// appearing in application-controlled IDs.
  public var storageKey: String {
    [principal, template, subscription, generation]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
  }
}

/// A redacted, actionable reason a durable provider cannot safely read or commit local sync
/// state. Providers may throw this from `currentCursor()` before a subscription claim is created;
/// callers decide when the underlying storage becomes available and retry `start()` themselves.
/// Database driver details, file paths, and protected row values deliberately do not cross this
/// boundary.
public enum MaterializerAvailabilityError: Error, Equatable, Sendable {
  case protectedDataUnavailable
  case databaseUnavailable
}

/// Application-owned readiness check for a durable provider. An iOS host can bridge protected
/// data, account-key, or database-open state here without introducing UIKit or a storage driver
/// into the core package. It must throw only a redacted `MaterializerAvailabilityError` for
/// expected unavailability; unexpected driver errors remain ordinary provider failures.
public protocol MaterializerAvailabilityProbe: Sendable {
  func checkAvailability() async throws
}

/// Default probe for providers that have no external availability gate.
public struct AlwaysAvailableMaterializerAvailability: MaterializerAvailabilityProbe, Sendable {
  public init() {}
  public func checkAvailability() async throws {}
}

/// An application-owned atomic materialization boundary. A provider must perform all row
/// mutations and cursor persistence in one durable transaction: if `apply` throws, neither the
/// rows nor the checkpoint may change. The server cursor is the transition/replay identity: a
/// provider compare-and-sets from `expectedCursor` to `cursor`, treats an already-current `cursor`
/// as an idempotent replay, and rejects every other current cursor. A decorator that atomically
/// reseeds from a SQL snapshot may strengthen only the diagnostic LSN at the same durable offset;
/// after `apply`, the reader re-reads that committed checkpoint and rejects any offset divergence.
/// An empty batch may still advance the cursor. This core package owns no transaction primitive and
/// therefore has no database dependency; these are provider contract requirements rather than
/// implementation details.
public protocol ShapeMaterializer: Sendable {
  /// The scope whose rows and checkpoint this materializer owns, when the provider can expose it.
  /// Legacy providers may leave this nil while migrating to explicit scope identity.
  var materializationScope: MaterializationScope? { get }
  /// Reads the last cursor durably committed with the materialized rows, when one exists.
  ///
  /// The default keeps existing materializer conformances source-compatible; providers with
  /// durable state should override it so a reader can resume without replaying from the beginning.
  func currentCursor() async throws -> StreamCursor?
  /// Atomically applies a batch and CAS-commits its checkpoint from `expectedCursor` to `cursor`.
  /// Providers should make this boundary crash-safe so a process restart can reopen the same scope
  /// and replay the last acknowledged server cursor without duplicate rows or cursor regression.
  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws
}

extension ShapeMaterializer {
  public var materializationScope: MaterializationScope? { nil }
  public func currentCursor() async throws -> StreamCursor? { nil }
}

/// Actor-backed materializer for deterministic tests, previews, and small demos. Each instance is
/// one shape's key-to-row working set; a real provider can use the same reader with a typed GRDB
/// table instead.
public actor InMemoryShapeMaterializer: ShapeMaterializer {
  private var values: [String: ChangeRow]
  private var currentCursor: StreamCursor?

  public init(
    initialRows: [String: ChangeRow] = [:],
    initialCursor: StreamCursor? = nil
  ) {
    values = initialRows
    currentCursor = initialCursor
  }

  public nonisolated var materializationScope: MaterializationScope? { nil }

  public func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    // A replay after a process acknowledgement can present the same server cursor again. It is
    // already durable; any distinct current cursor must fail closed instead of regressing.
    if currentCursor == cursor { return }
    guard currentCursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: currentCursor, advancingTo: cursor)
    }
    var nextValues = values
    for envelope in batch.envelopes {
      switch envelope.headers.operation {
      case .delete:
        nextValues.removeValue(forKey: envelope.key)
      case .insert, .update, .upsert:
        // A non-delete envelope without a value is malformed at the product boundary. Keeping
        // the prior row would silently hide data loss, so fail before advancing the cursor.
        guard let value = envelope.value else {
          throw StreamError.missingValue(key: envelope.key)
        }
        nextValues[envelope.key] = value
      }
    }
    // This assignment is deliberately last: a provider's equivalent operation must commit rows
    // and this cursor as one transaction.
    values = nextValues
    currentCursor = cursor
  }

  /// Stable, sorted row snapshot for tests and diagnostics.
  public func snapshot() -> [(key: String, row: ChangeRow)] {
    values.keys.sorted().compactMap { key in
      values[key].map { (key: key, row: $0) }
    }
  }

  public func rows() -> [String: ChangeRow] { values }

  public func currentCursor() async throws -> StreamCursor? { currentCursor }

  /// Compatibility alias for tests and diagnostics.
  public func cursor() -> StreamCursor? { currentCursor }
}

public enum StreamTerminalReason: String, Equatable, Sendable {
  case notFound
  case gone
  case closed
}

/// Typed failures from the long-poll reader. Cancellation is intentionally not wrapped: callers
/// receive Swift's `CancellationError` from the injected transport or the reader itself.
public enum StreamError: Error, Equatable, Sendable {
  case terminal(path: String, status: Int, reason: StreamTerminalReason)
  case missingNextOffset(path: String)
  case missingValue(key: String)
  case cursorConflict(expected: StreamCursor?, actual: StreamCursor?, advancingTo: StreamCursor)
  case committedCursorMismatch(result: StreamCursor, committed: StreamCursor)
  case startingCursorMismatch(durable: StreamCursor?, requested: StreamCursor)
  case invalidResponse
  /// The response was rejected before JSON decoding. `observed` is capped at `limit + 1`.
  case responseTooLarge(limit: Int, observed: Int)
  /// The decoded batch was rejected before any provider apply. `observed` is capped at `limit + 1`.
  case batchTooLarge(limit: Int, observed: Int)
  case decoding(String)
  /// The stream carries a must-refetch control: output before it was dropped, so the stream can
  /// no longer complete what a reader holds. The reader fails closed without applying the batch
  /// or advancing the cursor; reload from a snapshot.
  case mustRefetch(path: String, control: StreamControl)
}

public typealias ShapeStreamError = StreamError

private struct SSEStalled: Error, Sendable {}

// MARK: - Stream reader

/// Reads one native durable shape stream using long-polling or opt-in SSE. Responses with an advertised
/// checkpoint, including empty responses, are applied exactly once; idle responses without one are
/// ignored. Retry/backoff is intentionally outside this narrow slice; callers can restart the reader
/// with the last committed cursor using their own policy.
public struct ShapeStreamReader: Sendable {
  public let streamURL: URL
  public let transport: any HTTPTransport
  public let materializer: any ShapeMaterializer
  public let initialCursor: StreamCursor
  public let telemetry: TelemetryReporter
  public let responseDecodingLimits: ResponseDecodingLimits
  private let resumesFromMaterializer: Bool
  private let liveReadState: LiveReadState
  private let silenceTimeout: Duration
  private static let sseSilenceTimeout: Duration = .seconds(45)

  public init(
    streamURL: URL,
    transport: any HTTPTransport,
    materializer: any ShapeMaterializer,
    startingAt cursor: StreamCursor? = nil,
    telemetry: TelemetryReporter = .noop,
    responseDecodingLimits: ResponseDecodingLimits = .default,
    configuration: ElectricCircuitsClient.Configuration = .default
  ) {
    self.init(
      streamURL: streamURL, transport: transport, materializer: materializer,
      startingAt: cursor, telemetry: telemetry, responseDecodingLimits: responseDecodingLimits,
      configuration: configuration, sseSilenceTimeout: Self.sseSilenceTimeout)
  }

  init(
    streamURL: URL,
    transport: any HTTPTransport,
    materializer: any ShapeMaterializer,
    startingAt cursor: StreamCursor? = nil,
    telemetry: TelemetryReporter = .noop,
    responseDecodingLimits: ResponseDecodingLimits = .default,
    configuration: ElectricCircuitsClient.Configuration = .default,
    sseSilenceTimeout: Duration
  ) {
    self.liveReadState = LiveReadState(
      configuration: configuration,
      maximumEventBytes: responseDecodingLimits.maximumDecodedResponseBytes)
    self.streamURL = streamURL
    self.transport = transport
    self.materializer = materializer
    self.telemetry = telemetry
    self.responseDecodingLimits = responseDecodingLimits
    self.silenceTimeout = sseSilenceTimeout
    initialCursor = cursor ?? .beginning
    resumesFromMaterializer = cursor == nil
  }

  /// Polls until cancelled or a terminal stream response is received.
  public func run() async throws {
    var cursor = initialCursor
    var expectedCursor = try await materializer.currentCursor()
    if resumesFromMaterializer {
      if let expectedCursor { cursor = expectedCursor }
    } else {
      guard
        expectedCursor == initialCursor || (expectedCursor == nil && initialCursor == .beginning)
      else {
        throw StreamError.startingCursorMismatch(durable: expectedCursor, requested: initialCursor)
      }
    }
    while true {
      try Task.checkCancellation()
      if await liveReadState.mode == .sse {
        let response = SSEReadSession(
          reader: self, cursor: cursor, expectedCursor: expectedCursor,
          maximumEventBytes: await liveReadState.maximumEventBytes)
        do {
          try await readSSE(at: cursor, into: response)
        } catch {
          if Task.isCancelled || error is CancellationError { throw CancellationError() }
          if error is SSEStalled {
            await liveReadState.setMaximumEventBytes(await response.maximumEventBytes)
            (cursor, expectedCursor) = await response.checkpoint()
            continue
          }
          if let streamError = error as? StreamError, case .terminal = streamError { throw error }
          if shouldFallBackFromSSE(error, sawControl: await response.sawControl) {
            // This state belongs to the reader's source lifetime, including coordinator retries.
            await liveReadState.fallBack()
          } else {
            throw error
          }
        }
        await liveReadState.setMaximumEventBytes(await response.maximumEventBytes)
        (cursor, expectedCursor) = await response.checkpoint()
      } else if let result = try await poll(at: cursor) {
        cursor = try await commit(result, expecting: expectedCursor)
        expectedCursor = cursor
      }
    }
  }

  private actor LiveReadState {
    private let configuration: ElectricCircuitsClient.Configuration
    private var fellBack = false
    private(set) var maximumEventBytes: Int
    init(configuration: ElectricCircuitsClient.Configuration, maximumEventBytes: Int) {
      self.configuration = configuration
      self.maximumEventBytes = maximumEventBytes
    }
    /// Resolved before each live read; a fallback from SSE outlasts any later answer.
    var mode: LiveReadMode { fellBack ? .longPoll : configuration.liveReadMode }
    func fallBack() { fellBack = true }
    func setMaximumEventBytes(_ value: Int) { maximumEventBytes = value }
  }

  private func commit(_ result: PollResult, expecting expectedCursor: StreamCursor?) async throws
    -> StreamCursor
  {
    try Task.checkCancellation()
    // Every materializer fails closed here, whether or not it reads `controls`: committing past
    // a must-refetch marker would move the cursor over output the stream dropped.
    if let control = result.batch.controls.first(where: { $0.isMustRefetch }) {
      throw StreamError.mustRefetch(path: streamURL.path, control: control)
    }
    // A control kind this reader does not know may also say the stream cannot complete what the
    // reader holds; skipping it would commit past an instruction, so it fails closed as well.
    if let control = result.batch.controls.first {
      throw StreamError.decoding("unsupported stream control '\(control.control)'")
    }
    let span = telemetry.beginSpan(name: "electric.materializer.apply", kind: .internalSpan)
    let started = DispatchTime.now().uptimeNanoseconds
    do {
      try await materializer.apply(
        result.batch, expecting: expectedCursor, advancingTo: result.cursor)
    } catch {
      telemetry.endSpan(
        span, attributes: ["electric.operation": "apply", "error.type": "materializer"])
      telemetry.recordCounter(
        "electric.materializer.failures", attributes: ["electric.operation": "apply"])
      throw error
    }
    telemetry.endSpan(span, attributes: ["electric.operation": "apply"])
    telemetry.recordGauge(
      "electric.materializer.duration_ms",
      value: Double(DispatchTime.now().uptimeNanoseconds - started) / 1_000_000,
      attributes: ["electric.operation": "apply"])
    // Preserve a provider's strengthened diagnostic LSN while checking its durable offset.
    if let committedCursor = try await materializer.currentCursor() {
      guard committedCursor.offset == result.cursor.offset else {
        throw StreamError.committedCursorMismatch(result: result.cursor, committed: committedCursor)
      }
      return committedCursor
    }
    return result.cursor
  }

  private func readSSE(at cursor: StreamCursor, into receiver: SSEReadSession) async throws {
    var request = try streamRequest(at: cursor, mode: .sse)
    request = transport.prepare(request)
    let span = telemetry.beginSpan(
      name: "electric.stream.sse", kind: .client,
      parentTraceparent: request.value(forHTTPHeaderField: "traceparent"))
    telemetry.injectTraceContext(into: &request, span: span)
    let streamRequest = request
    do {
      try await withThrowingTaskGroup(of: Void.self) { group in
        group.addTask {
          try await self.transport.stream(
            streamRequest,
            onResponse: { try await receiver.receive($0) },
            onBytes: { try await receiver.receive($0) })
        }
        group.addTask {
          try await receiver.waitForSilence(timeout: silenceTimeout)
        }
        defer { group.cancelAll() }
        _ = try await group.next()
      }
      let attributes = ["http.method": "GET", "http.route": "/stream"]
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
    } catch {
      let attributes = ["http.method": "GET", "http.route": "/stream", "error.type": "stream"]
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw error
    }
    // The receiver is response-local: trailing partial events and uncommitted data are dropped.
  }

  private func shouldFallBackFromSSE(_ error: Error, sawControl: Bool) -> Bool {
    guard !sawControl else { return false }
    if error is SSEFallback { return true }
    if error is StreamingUnsupported { return true }
    if let clientError = error as? ClientError {
      if case .http(let status, _) = clientError {
        return (400..<500).contains(status) && status != 404 && status != 410
      }
    }
    // Transport errors, retryable HTTP responses (including 408/425/429), and parser/provider
    // failures stay on SSE so the coordinator can apply its shared retry policy and resume from
    // the committed cursor. A non-event-stream response is the distinct SSEFallback case above.
    return false
  }

  private struct SSEFallback: Error {}

  private actor SSEReadSession {
    let reader: ShapeStreamReader
    var cursor: StreamCursor
    var expectedCursor: StreamCursor?
    var parser: SSEParser
    var pending = ChangeBatch()
    private(set) var maximumEventBytes: Int
    private let clock = ContinuousClock()
    private var lastActivity: ContinuousClock.Instant
    private var receivedHeaders = false
    private var headerWaiters: [CheckedContinuation<Void, Never>] = []
    private var committing = false
    private(set) var sawControl = false

    init(
      reader: ShapeStreamReader, cursor: StreamCursor, expectedCursor: StreamCursor?,
      maximumEventBytes: Int
    ) {
      self.reader = reader
      self.cursor = cursor
      self.expectedCursor = expectedCursor
      self.maximumEventBytes = maximumEventBytes
      parser = SSEParser(maximumEventBytes: maximumEventBytes)
      lastActivity = ContinuousClock().now
    }

    func checkpoint() -> (StreamCursor, StreamCursor?) { (cursor, expectedCursor) }

    func waitForSilence(timeout: Duration) async throws {
      if !receivedHeaders {
        await withTaskCancellationHandler(
          operation: {
            await withCheckedContinuation { (continuation: CheckedContinuation<Void, Never>) in
              if receivedHeaders {
                continuation.resume()
              } else {
                headerWaiters.append(continuation)
              }
            }
          },
          onCancel: { Task { await self.resumeHeaderWaiters() } })
        try Task.checkCancellation()
      }
      while true {
        if committing {
          try await Task.sleep(for: .milliseconds(10))
          continue
        }
        let deadline = lastActivity.advanced(by: timeout)
        if clock.now >= deadline { throw SSEStalled() }
        try await Task.sleep(until: deadline, clock: clock)
        if !committing && clock.now >= lastActivity.advanced(by: timeout) { throw SSEStalled() }
      }
    }

    func receive(_ headers: HTTPURLResponse) throws {
      receivedHeaders = true
      resumeHeaderWaiters()
      lastActivity = clock.now
      let status = headers.statusCode
      if headers.value(forHTTPHeaderField: "stream-closed")?.lowercased() == "true" {
        throw StreamError.terminal(path: reader.streamURL.path, status: status, reason: .closed)
      }
      if status == 404 || status == 410 {
        throw StreamError.terminal(
          path: reader.streamURL.path, status: status, reason: status == 404 ? .notFound : .gone)
      }
      guard (200..<300).contains(status) else { throw nativeHTTPError(response: headers) }
      let mediaType = headers.value(forHTTPHeaderField: "Content-Type")?
        .split(separator: ";", maxSplits: 1).first?
        .trimmingCharacters(in: .whitespacesAndNewlines).lowercased()
      guard mediaType == "text/event-stream" else { throw SSEFallback() }
    }

    private func resumeHeaderWaiters() {
      for waiter in headerWaiters { waiter.resume() }
      headerWaiters.removeAll()
    }

    func receive(_ bytes: Data) async throws {
      lastActivity = clock.now
      try Task.checkCancellation()
      for event in try parser.append(bytes) {
        switch event.name {
        case "data":
          let limits = reader.responseDecodingLimits
          let batch: ChangeBatch
          do { batch = try JSONDecoder().decode(ChangeBatch.self, from: event.data) } catch {
            throw StreamError.decoding("Invalid SSE data event")
          }
          lastActivity = clock.now
          guard
            batch.envelopes.count + batch.controls.count
              <= limits.maximumChangeEventsPerStreamBatch - pending.envelopes.count
              - pending.controls.count
          else {
            throw StreamError.batchTooLarge(
              limit: limits.maximumChangeEventsPerStreamBatch,
              observed: limits.maximumChangeEventsPerStreamBatch + 1)
          }
          pending.envelopes.append(contentsOf: batch.envelopes)
          pending.controls.append(contentsOf: batch.controls)
        case "control":
          let control: Control
          do { control = try JSONDecoder().decode(Control.self, from: event.data) } catch {
            throw StreamError.decoding("Invalid SSE control event")
          }
          lastActivity = clock.now
          sawControl = true
          if let offset = control.streamNextOffset, !offset.isEmpty, offset != cursor.offset {
            let lsn = pending.envelopes.reversed().compactMap { $0.headers.lsn }.first ?? cursor.lsn
            committing = true
            defer {
              committing = false
              lastActivity = clock.now
            }
            cursor = try await reader.commit(
              PollResult(batch: pending, cursor: StreamCursor(offset: offset, lsn: lsn)),
              expecting: expectedCursor)
            expectedCursor = cursor
            pending = ChangeBatch()
          }
          if control.streamClosed == true {
            throw StreamError.terminal(path: reader.streamURL.path, status: 200, reason: .closed)
          }
        default: break
        }
        maximumEventBytes = parser.maximumEventBytes
      }
    }

    private struct Control: Decodable {
      let streamNextOffset: String?
      let streamClosed: Bool?
    }
  }

  private struct PollResult: Sendable {
    let batch: ChangeBatch
    let cursor: StreamCursor
  }

  private func streamRequest(at cursor: StreamCursor, mode: LiveReadMode) throws -> URLRequest {
    var components = URLComponents(url: streamURL, resolvingAgainstBaseURL: false)
    var query = components?.queryItems ?? []
    query.removeAll { $0.name == "offset" || $0.name == "live" }
    query.append(URLQueryItem(name: "offset", value: cursor.offset))
    query.append(URLQueryItem(name: "live", value: mode.rawValue))
    components?.queryItems = query
    guard let url = components?.url else { throw StreamError.invalidResponse }

    var request = URLRequest(url: url)
    request.httpMethod = "GET"
    request.setValue(
      mode == .sse ? "text/event-stream" : "application/json", forHTTPHeaderField: "Accept")
    return request
  }

  private func poll(at cursor: StreamCursor) async throws -> PollResult? {
    var request = try streamRequest(at: cursor, mode: .longPoll)
    request = transport.prepare(request)
    let span = telemetry.beginSpan(
      name: "electric.stream.poll", kind: .client,
      parentTraceparent: request.value(forHTTPHeaderField: "traceparent"))
    telemetry.injectTraceContext(into: &request, span: span)
    let response: HTTPResponse
    do {
      response = try await transport.send(
        request, maximumResponseBytes: responseDecodingLimits.maximumDecodedResponseBytes)
    } catch let error as HTTPTransportError {
      let attributes = [
        "http.method": "GET", "http.route": "/stream", "error.type": "response_too_large",
      ]
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw StreamError.responseTooLarge(
        limit: error.responseLimit, observed: error.observedResponseBytes)
    } catch {
      telemetry.endSpan(
        span,
        attributes: ["http.method": "GET", "http.route": "/stream", "error.type": "transport"])
      telemetry.recordCounter(
        "electric.http.requests", attributes: ["http.method": "GET", "error.type": "transport"])
      throw error
    }
    let headers = response.response
    let attributes = [
      "http.method": "GET", "http.route": "/stream", "http.status_code": "\(headers.statusCode)",
    ]
    let nextOffset = headers.value(forHTTPHeaderField: "stream-next-offset")
    let closed = headers.value(forHTTPHeaderField: "stream-closed")?.lowercased() == "true"

    if closed {
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw StreamError.terminal(path: streamURL.path, status: headers.statusCode, reason: .closed)
    }
    if headers.statusCode == 404 || headers.statusCode == 410 {
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      let reason: StreamTerminalReason = headers.statusCode == 404 ? .notFound : .gone
      throw StreamError.terminal(path: streamURL.path, status: headers.statusCode, reason: reason)
    }
    if headers.statusCode == 204 {
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      return emptyPollResult(nextOffset: nextOffset, preserving: cursor)
    }
    guard (200..<300).contains(headers.statusCode) else {
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw nativeHTTPError(response: headers)
    }
    guard response.data.count <= responseDecodingLimits.maximumDecodedResponseBytes else {
      let limitAttributes = [
        "http.method": "GET", "http.route": "/stream", "error.type": "response_too_large",
      ]
      telemetry.endSpan(span, attributes: limitAttributes)
      telemetry.recordCounter("electric.http.requests", attributes: limitAttributes)
      throw StreamError.responseTooLarge(
        limit: responseDecodingLimits.maximumDecodedResponseBytes,
        observed: responseDecodingLimits.observedByteCount(for: response.data))
    }
    telemetry.endSpan(span, attributes: attributes)
    telemetry.recordCounter("electric.http.requests", attributes: attributes)

    // Rust's `DsClient::read` treats a successful empty body like an empty JSON array.
    if response.data.allSatisfy({ $0 == 0x20 || $0 == 0x09 || $0 == 0x0A || $0 == 0x0D }) {
      return emptyPollResult(nextOffset: nextOffset, preserving: cursor)
    }
    let batch: ChangeBatch
    do {
      batch = try JSONDecoder().decode(ChangeBatch.self, from: response.data)
    } catch {
      throw StreamError.decoding(String(describing: error))
    }
    let entries = batch.envelopes.count + batch.controls.count
    guard entries <= responseDecodingLimits.maximumChangeEventsPerStreamBatch else {
      throw StreamError.batchTooLarge(
        limit: responseDecodingLimits.maximumChangeEventsPerStreamBatch,
        observed: min(entries, responseDecodingLimits.maximumChangeEventsPerStreamBatch + 1))
    }
    guard !batch.isEmpty else {
      return emptyPollResult(nextOffset: nextOffset, preserving: cursor)
    }
    guard let nextOffset, !nextOffset.isEmpty else {
      throw StreamError.missingNextOffset(path: streamURL.path)
    }
    let lsn = batch.envelopes.reversed().compactMap { $0.headers.lsn }.first ?? cursor.lsn
    return PollResult(batch: batch, cursor: StreamCursor(offset: nextOffset, lsn: lsn))
  }

  private func emptyPollResult(nextOffset: String?, preserving cursor: StreamCursor) -> PollResult?
  {
    guard let nextOffset, !nextOffset.isEmpty, nextOffset != cursor.offset else { return nil }
    return PollResult(
      batch: ChangeBatch(), cursor: StreamCursor(offset: nextOffset, lsn: cursor.lsn))
  }
}

public typealias LongPollShapeStreamReader = ShapeStreamReader
