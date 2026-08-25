/// A small native Swift client for Electric Circuits' Rust `/v1` control plane.
///
/// This package intentionally stops at control-plane handles. Reading a durable stream and
/// persisting it locally are explicit seams (`DurableStreamHandle`) for a later package slice.
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// A client-generated UUIDv4 used to correlate an optimistic write with the authoritative row
/// eventually delivered by a live feed. The server may continue to assign its own primary key;
/// this value is an application-level identity and must be persisted with the row when the schema
/// supports it.
public struct ClientID: RawRepresentable, Codable, Hashable, Sendable, CustomStringConvertible {
  public let rawValue: String

  /// Generates a canonical lower-case UUIDv4 string.
  public init() {
    self.init(uuid: UUID())
  }

  public init?(rawValue: String) {
    guard let uuid = UUID(uuidString: rawValue), Self.isV4(uuid) else { return nil }
    self.rawValue = uuid.uuidString.lowercased()
  }

  public init(uuid: UUID) {
    precondition(Self.isV4(uuid), "ClientID requires a UUIDv4")
    rawValue = uuid.uuidString.lowercased()
  }

  public var description: String { rawValue }

  public init(from decoder: Decoder) throws {
    let value = try decoder.singleValueContainer().decode(String.self)
    guard let clientID = Self(rawValue: value) else {
      throw DecodingError.dataCorruptedError(
        in: try decoder.singleValueContainer(), debugDescription: "expected UUIDv4 client ID")
    }
    self = clientID
  }

  public func encode(to encoder: Encoder) throws {
    var container = encoder.singleValueContainer()
    try container.encode(rawValue)
  }

  private static func isV4(_ uuid: UUID) -> Bool {
    let bytes = uuid.uuid
    return (bytes.6 & 0xf0) == 0x40 && (bytes.8 & 0xc0) == 0x80
  }
}

// MARK: - JSON and predicates

/// JSON values used by predicates and subset rows.
///
/// PostgreSQL `bigint` values must not pass through `Double`: IEEE-754 cannot represent every
/// integer above 2^53. Integer and decimal values therefore have lossless representations, while
/// `number` remains available for non-finite/otherwise unrepresentable JSON numbers.
public enum JSONValue: Codable, Equatable, Sendable {
  case null
  case bool(Bool)
  case int(Int64)
  case decimal(Decimal)
  case number(Double)
  case string(String)
  case array([JSONValue])
  case object([String: JSONValue])

  public init(from decoder: Decoder) throws {
    let c = try decoder.singleValueContainer()
    if c.decodeNil() {
      self = .null
    } else if let value = try? c.decode(Bool.self) {
      self = .bool(value)
    } else if let value = try? c.decode(Int64.self) {
      self = .int(value)
    } else if let value = try? c.decode(Decimal.self) {
      self = .decimal(value)
    } else if let value = try? c.decode(Double.self) {
      self = .number(value)
    } else if let value = try? c.decode(String.self) {
      self = .string(value)
    } else if let value = try? c.decode([JSONValue].self) {
      self = .array(value)
    } else {
      self = .object(try c.decode([String: JSONValue].self))
    }
  }

  public func encode(to encoder: Encoder) throws {
    var c = encoder.singleValueContainer()
    switch self {
    case .null: try c.encodeNil()
    case .bool(let value): try c.encode(value)
    case .int(let value): try c.encode(value)
    case .decimal(let value): try c.encode(value)
    case .number(let value): try c.encode(value)
    case .string(let value): try c.encode(value)
    case .array(let value): try c.encode(value)
    case .object(let value): try c.encode(value)
    }
  }
}

public enum LeafOperator: String, Codable, Sendable {
  case eq, neq, lt, lte, gt, gte, like
}

public struct PredicateSubquery: Codable, Equatable, Sendable {
  public var table: String
  public var project: String
  public var `where`: Predicate?

  public init(table: String, project: String, where predicate: Predicate? = nil) {
    self.table = table
    self.project = project
    self.where = predicate
  }
}

/// The Rust `PredicateJson` untagged grammar.
public indirect enum Predicate: Codable, Equatable, Sendable {
  case leaf(column: String, op: LeafOperator, value: JSONValue)
  case isNull(column: String, isNull: Bool)
  case and([Predicate])
  case or([Predicate])
  case not(Predicate)
  case `in`(column: String, subquery: PredicateSubquery, negated: Bool = false)

  public init(from decoder: Decoder) throws {
    let c = try decoder.container(keyedBy: Keys.self)
    if c.contains(.column), c.contains(.op) {
      self = .leaf(
        column: try c.decode(String.self, forKey: .column),
        op: try c.decode(LeafOperator.self, forKey: .op),
        value: try c.decode(JSONValue.self, forKey: .value))
      return
    }
    if c.contains(.isNull) {
      self = .isNull(
        column: try c.decode(String.self, forKey: .column),
        isNull: try c.decode(Bool.self, forKey: .isNull))
      return
    }
    if c.contains(.and) {
      self = .and(try c.decode([Predicate].self, forKey: .and))
      return
    }
    if c.contains(.or) {
      self = .or(try c.decode([Predicate].self, forKey: .or))
      return
    }
    if c.contains(.not) {
      self = .not(try c.decode(Predicate.self, forKey: .not))
      return
    }
    if c.contains(.in) {
      self = .in(
        column: try c.decode(String.self, forKey: .column),
        subquery: try c.decode(PredicateSubquery.self, forKey: .in),
        negated: try c.decodeIfPresent(Bool.self, forKey: .negated) ?? false)
      return
    }
    throw DecodingError.dataCorrupted(
      .init(codingPath: decoder.codingPath, debugDescription: "Invalid predicate"))
  }

  public func encode(to encoder: Encoder) throws {
    var c = encoder.container(keyedBy: Keys.self)
    switch self {
    case .leaf(let column, let op, let value):
      try c.encode(column, forKey: .column)
      try c.encode(op, forKey: .op)
      try c.encode(value, forKey: .value)
    case .isNull(let column, let value):
      try c.encode(column, forKey: .column)
      try c.encode(value, forKey: .isNull)
    case .and(let value): try c.encode(value, forKey: .and)
    case .or(let value): try c.encode(value, forKey: .or)
    case .not(let value): try c.encode(value, forKey: .not)
    case .in(let column, let subquery, let negated):
      try c.encode(column, forKey: .column)
      try c.encode(subquery, forKey: .in)
      if negated { try c.encode(true, forKey: .negated) }
    }
  }

  private enum Keys: String, CodingKey {
    case column = "col"
    case op, value, isNull, and, or, not, `in`, negated
  }
}

// MARK: - Native DTOs

public struct ShapeRequest: Codable, Equatable, Sendable {
  public var table: String
  public var `where`: Predicate?
  public var columns: [String]?
  public var changesOnly: Bool?
  public var subscription: String?

  /// A request with a stable claim ID, making retries and repeated creates idempotent.
  public init(
    table: String, where predicate: Predicate? = nil, columns: [String]? = nil,
    changesOnly: Bool? = nil, subscription: String? = UUID().uuidString
  ) {
    self.table = table
    self.where = predicate
    self.columns = columns
    self.changesOnly = changesOnly
    self.subscription = subscription
  }
}

public struct ShapeResponse: Codable, Equatable, Sendable {
  public var shapeId: String
  public var table: String
  public var streamPath: String
  public var streamURL: URL
  public var subscription: String?
  public var leaseSeconds: Int?
  public var state: String?
  public var subscriptions: Int?
  public init(
    shapeId: String, table: String, streamPath: String, streamURL: URL,
    subscription: String? = nil, leaseSeconds: Int? = nil, state: String? = nil,
    subscriptions: Int? = nil
  ) {
    self.shapeId = shapeId
    self.table = table
    self.streamPath = streamPath
    self.streamURL = streamURL
    self.subscription = subscription
    self.leaseSeconds = leaseSeconds
    self.state = state
    self.subscriptions = subscriptions
  }
  enum CodingKeys: String, CodingKey {
    case shapeId, table, streamPath
    case streamURL = "streamUrl"
    case subscription, leaseSeconds, state, subscriptions
  }
}

public struct DurableStreamHandle: Equatable, Sendable {
  public let path: String
  public let url: URL
  public init(path: String, url: URL) {
    self.path = path
    self.url = url
  }
}

public struct SubsetOrderBy: Codable, Equatable, Sendable {
  public var column: String
  public var descending: Bool
  public init(column: String, descending: Bool = false) {
    self.column = column
    self.descending = descending
  }
  enum CodingKeys: String, CodingKey {
    case column = "col"
    case descending = "desc"
  }
}

public struct SubsetQuery: Codable, Equatable, Sendable {
  public var table: String
  public var `where`: Predicate?
  public var columns: [String]?
  public var orderBy: SubsetOrderBy?
  public var limit: Int?
  public var offset: Int?
  public init(
    table: String, where predicate: Predicate? = nil, columns: [String]? = nil,
    orderBy: SubsetOrderBy? = nil, limit: Int? = nil, offset: Int? = nil
  ) {
    self.table = table
    self.where = predicate
    self.columns = columns
    self.orderBy = orderBy
    self.limit = limit
    self.offset = offset
  }
}

public struct SubsetResponse: Codable, Equatable, Sendable {
  public var rows: [JSONValue]
  public var lsn: String
  public init(rows: [JSONValue], lsn: String) {
    self.rows = rows
    self.lsn = lsn
  }
}

public enum AggregateFunction: String, Codable, Sendable {
  case count, sum, avg, min, max
}
public struct AggregateRequest: Codable, Equatable, Sendable {
  public var table: String
  public var `where`: Predicate?
  public var function: AggregateFunction
  public var column: String?
  public var subscription: String?
  public init(
    table: String, function: AggregateFunction, column: String? = nil,
    where predicate: Predicate? = nil, subscription: String? = UUID().uuidString
  ) {
    self.table = table
    self.function = function
    self.column = column
    self.where = predicate
    self.subscription = subscription
  }
  enum CodingKeys: String, CodingKey {
    case table, `where`
    case function = "fn"
    case column = "col"
    case subscription
  }
}

// MARK: - Transport and client

public struct HTTPResponse: Sendable {
  public let data: Data
  public let response: HTTPURLResponse
  public init(data: Data, response: HTTPURLResponse) {
    self.data = data
    self.response = response
  }
}

/// A response that the Foundation transport stopped before it accumulated more than the requested
/// admission limit. The byte observation is capped at `limit + 1` and carries no response content.
public enum HTTPTransportError: Error, Equatable, Sendable {
  case responseTooLarge(limit: Int, observed: Int)

  public var responseLimit: Int {
    switch self {
    case .responseTooLarge(let limit, _): return limit
    }
  }

  public var observedResponseBytes: Int {
    switch self {
    case .responseTooLarge(_, let observed): return observed
    }
  }
}

/// Application-admission limits for Foundation HTTP responses.
///
/// These limits are checked before this package decodes a response. The built-in Foundation
/// transport uses the same limit while reading a successful response, retaining no more than
/// `maximumDecodedResponseBytes + 1` bytes before cancelling the task. Invalid values clamp to
/// one, so every configuration remains finite and deterministic.
public struct ResponseDecodingLimits: Sendable, Equatable {
  /// Maximum bytes admitted to a JSON decoder from one HTTP response. Default: 1 MiB.
  public let maximumDecodedResponseBytes: Int
  /// Maximum change envelopes admitted from one successful durable-stream batch. Default: 10,000.
  public let maximumChangeEventsPerStreamBatch: Int

  public init(
    maximumDecodedResponseBytes: Int = 1_048_576,
    maximumChangeEventsPerStreamBatch: Int = 10_000
  ) {
    self.maximumDecodedResponseBytes = max(1, maximumDecodedResponseBytes)
    self.maximumChangeEventsPerStreamBatch = max(1, maximumChangeEventsPerStreamBatch)
  }

  public static let `default` = ResponseDecodingLimits()

  /// Bounds a diagnostic observation without retaining or exposing response content.
  func observedByteCount(for data: Data) -> Int {
    min(data.count, maximumDecodedResponseBytes + 1)
  }
}

public protocol HTTPTransport: Sendable {
  func prepare(_ request: URLRequest) -> URLRequest
  func send(_ request: URLRequest) async throws -> HTTPResponse
  /// Sends a request with a caller-specific successful-response byte ceiling. Custom transports
  /// can preserve their existing behavior by implementing only `send(_:)`; the default forwards
  /// to it. The built-in Foundation transport enforces the ceiling while receiving bytes.
  func send(_ request: URLRequest, maximumResponseBytes: Int) async throws -> HTTPResponse
}

extension HTTPTransport {
  /// Lets a transport apply caller-owned request configuration before telemetry adds propagation.
  /// The identity default keeps existing custom transports source-compatible.
  public func prepare(_ request: URLRequest) -> URLRequest { request }
  public func send(_ request: URLRequest, maximumResponseBytes _: Int) async throws -> HTTPResponse
  {
    try await send(request)
  }
}

public struct URLSessionTransport: HTTPTransport {
  public let session: URLSession
  /// Direct calls to `send(_:)` use this finite ceiling. `ElectricCircuitsClient` and
  /// `ShapeStreamReader` pass their own response-admission ceiling to the overload instead.
  public let maximumResponseBytes: Int
  /// Headers and cookie string applied to every request. Authentication remains a transport/server
  /// concern: applications can provide a credential-aware `HTTPTransport` without this package
  /// needing to know how tokens are obtained or refreshed.
  public let headers: [String: String]
  public let cookieHeader: String?

  public init(
    session: URLSession = .shared,
    headers: [String: String] = [:],
    cookieHeader: String? = nil,
    maximumResponseBytes: Int = ResponseDecodingLimits.default.maximumDecodedResponseBytes
  ) {
    self.session = session
    self.headers = headers
    self.cookieHeader = cookieHeader
    self.maximumResponseBytes = max(1, maximumResponseBytes)
  }

  /// Convenience form for callers that already hold Foundation cookies. The cookies are reduced
  /// to a request header at construction time, keeping this transport value `Sendable`.
  public init(
    session: URLSession = .shared,
    headers: [String: String] = [:],
    cookies: [HTTPCookie],
    maximumResponseBytes: Int = ResponseDecodingLimits.default.maximumDecodedResponseBytes
  ) {
    self.init(
      session: session, headers: headers,
      cookieHeader: HTTPCookie.requestHeaderFields(with: cookies)["Cookie"],
      maximumResponseBytes: maximumResponseBytes)
  }

  public func prepare(_ request: URLRequest) -> URLRequest {
    var request = request
    for (name, value) in headers where request.value(forHTTPHeaderField: name) == nil {
      request.setValue(value, forHTTPHeaderField: name)
    }
    if let cookieHeader, request.value(forHTTPHeaderField: "Cookie") == nil {
      request.setValue(cookieHeader, forHTTPHeaderField: "Cookie")
    }
    return request
  }

  public func send(_ request: URLRequest) async throws -> HTTPResponse {
    try await send(request, maximumResponseBytes: maximumResponseBytes)
  }

  public func send(_ request: URLRequest, maximumResponseBytes: Int) async throws -> HTTPResponse {
    let limit = max(1, maximumResponseBytes)
    let prepared = prepare(request)
    let bytesAndResponse: (URLSession.AsyncBytes, URLResponse)
    do {
      bytesAndResponse = try await session.bytes(for: prepared)
    } catch {
      if Task.isCancelled || (error as? URLError)?.code == .cancelled {
        throw CancellationError()
      }
      throw error
    }
    let (bytes, response) = bytesAndResponse
    guard let response = response as? HTTPURLResponse else {
      bytes.task.cancel()
      throw ClientError.invalidResponse
    }
    let isHead = prepared.httpMethod?.caseInsensitiveCompare("HEAD") == .orderedSame
    let isSuccess = (200..<300).contains(response.statusCode)
    guard isSuccess, response.statusCode != 204, !isHead else {
      bytes.task.cancel()
      return HTTPResponse(data: Data(), response: response)
    }
    if response.expectedContentLength > Int64(limit) {
      bytes.task.cancel()
      throw HTTPTransportError.responseTooLarge(limit: limit, observed: limit + 1)
    }
    var data = Data()
    if response.expectedContentLength >= 0 {
      data.reserveCapacity(min(limit, Int(response.expectedContentLength)))
    }
    do {
      for try await byte in bytes {
        try Task.checkCancellation()
        guard data.count < limit else {
          bytes.task.cancel()
          throw HTTPTransportError.responseTooLarge(limit: limit, observed: limit + 1)
        }
        data.append(byte)
      }
    } catch {
      bytes.task.cancel()
      if Task.isCancelled { throw CancellationError() }
      throw error
    }
    return HTTPResponse(data: data, response: response)
  }
}

public struct ShapeHandle: Sendable, Equatable {
  public let response: ShapeResponse
  public init(response: ShapeResponse) { self.response = response }
  public var id: String { response.shapeId }
  public var subscription: String? { response.subscription }
  public var stream: DurableStreamHandle {
    DurableStreamHandle(path: response.streamPath, url: response.streamURL)
  }
}

public enum ClientError: Error, Equatable, Sendable {
  case invalidResponse
  case http(status: Int, message: String)
  /// A transient native HTTP response. `retryAfter` is the server's parsed directive when it sent
  /// a valid `Retry-After` header; callers must still bound it with their own retry policy.
  case retryableHTTP(status: Int, retryAfter: Duration?)
  /// The response was rejected before JSON decoding. `observed` is capped at `limit + 1`.
  case responseTooLarge(limit: Int, observed: Int)
  case decoding(String)
}

/// The persistence boundary for applications that materialize durable streams.
///
/// The package intentionally depends only on Foundation `Data`; applications may implement this
/// protocol with GRDB, another database, a file store, or their own actor. Stream reading,
/// reconnect, and materialization policy remain separate from this control-plane package.
public protocol ElectricCircuitsStore: Sendable {
  func read(forKey key: String) async throws -> Data?
  func write(_ data: Data, forKey key: String) async throws
  func removeValue(forKey key: String) async throws
}

/// Deterministic actor-isolated store for previews, tests, and small applications.
public actor InMemoryElectricCircuitsStore: ElectricCircuitsStore {
  private var values: [String: Data]

  public init(initialValues: [String: Data] = [:]) {
    values = initialValues
  }

  public func read(forKey key: String) async throws -> Data? {
    values[key]
  }

  public func write(_ data: Data, forKey key: String) async throws {
    values[key] = data
  }

  public func removeValue(forKey key: String) async throws {
    values.removeValue(forKey: key)
  }

  /// A stable, sorted view useful for deterministic tests and diagnostics.
  public func keys() -> [String] {
    values.keys.sorted()
  }
}

public actor ElectricCircuitsClient {
  public let baseURL: URL
  public let responseDecodingLimits: ResponseDecodingLimits
  private let transport: any HTTPTransport
  private let telemetry: TelemetryReporter
  private let encoder: JSONEncoder
  private let decoder: JSONDecoder

  public init(
    baseURL: URL, transport: any HTTPTransport = URLSessionTransport(),
    telemetry: TelemetryReporter = .noop,
    responseDecodingLimits: ResponseDecodingLimits = .default
  ) {
    self.baseURL = baseURL
    self.responseDecodingLimits = responseDecodingLimits
    self.transport = transport
    self.telemetry = telemetry
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    self.encoder = encoder
    self.decoder = JSONDecoder()
  }

  public func createShape(_ request: ShapeRequest) async throws -> ShapeHandle {
    try await sendShape(path: "v1/shapes", request: request)
  }
  /// Repeats a create using the request's stable subscription claim to renew its lease.
  public func renewShape(_ request: ShapeRequest) async throws -> ShapeHandle {
    try await createShape(request)
  }
  public func createSubsetFeed(_ request: ShapeRequest) async throws -> ShapeHandle {
    var request = request
    request.changesOnly = true
    return try await sendShape(path: "v1/subset-feeds", request: request)
  }
  public func createAggregate(_ request: AggregateRequest) async throws -> ShapeHandle {
    ShapeHandle(response: try await send(path: "v1/aggregates", method: "POST", body: request))
  }
  public func getShape(id: String) async throws -> ShapeResponse {
    try decode(
      await sendRaw(
        components: ["v1", "shapes", id], method: "GET", body: Optional<Empty>.none, query: []))
  }
  public func releaseShape(_ handle: ShapeHandle) async throws {
    do {
      _ = try await sendRaw(
        components: ["v1", "shapes", handle.id], method: "DELETE", body: Optional<Empty>.none,
        query: handle.subscription.map { [URLQueryItem(name: "subscription", value: $0)] } ?? [])
    } catch ClientError.http(status: 404, message: _) {
      // DELETE is idempotent from a client's perspective: an already-retired shape is released.
    }
  }
  public func querySubset(_ request: SubsetQuery) async throws -> SubsetResponse {
    try await send(path: "v1/subsets/query", method: "POST", body: request)
  }

  /// Reads the durable stream's current frontier without consuming a batch. Subset-feed clients
  /// use this HEAD checkpoint to fence the initial snapshot before starting long-poll delivery.
  public func streamCursor(for handle: ShapeHandle) async throws -> StreamCursor {
    var request = URLRequest(url: handle.stream.url)
    request.httpMethod = "HEAD"
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    request = transport.prepare(request)
    let span = telemetry.beginSpan(
      name: "electric.stream.cursor", kind: .client,
      parentTraceparent: request.value(forHTTPHeaderField: "traceparent"))
    telemetry.injectTraceContext(into: &request, span: span)
    let result: HTTPResponse
    do {
      result = try await transport.send(
        request, maximumResponseBytes: responseDecodingLimits.maximumDecodedResponseBytes)
    } catch let error as HTTPTransportError {
      let attributes = [
        "http.method": "HEAD", "http.route": "/stream", "error.type": "response_too_large",
      ]
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw ClientError.responseTooLarge(
        limit: error.responseLimit, observed: error.observedResponseBytes)
    } catch {
      telemetry.endSpan(
        span,
        attributes: ["http.method": "HEAD", "http.route": "/stream", "error.type": "transport"])
      telemetry.recordCounter(
        "electric.http.requests", attributes: ["http.method": "HEAD", "error.type": "transport"])
      throw error
    }
    let attributes = [
      "http.method": "HEAD", "http.route": "/stream",
      "http.status_code": "\(result.response.statusCode)",
    ]
    telemetry.endSpan(span, attributes: attributes)
    telemetry.recordCounter("electric.http.requests", attributes: attributes)
    guard (200..<300).contains(result.response.statusCode) else {
      throw nativeHTTPError(response: result.response)
    }
    guard let offset = result.response.value(forHTTPHeaderField: "stream-next-offset"),
      !offset.isEmpty
    else { throw ClientError.decoding("stream HEAD response missing stream-next-offset") }
    return StreamCursor(offset: offset)
  }

  private struct Empty: Encodable {}
  private func sendShape(path: String, request: ShapeRequest) async throws -> ShapeHandle {
    var response: ShapeResponse = try await send(path: path, method: "POST", body: request)
    if let subscription = request.subscription {
      if let echoed = response.subscription, echoed != subscription {
        throw ClientError.decoding("shape response subscription does not match request")
      }
      // The claim is part of the client's durable ownership identity. Older servers may omit its
      // echo, but a handle returned for this request must still carry it for retries and release.
      response.subscription = subscription
    }
    return ShapeHandle(response: response)
  }
  private func send<T: Decodable, B: Encodable>(path: String, method: String, body: B?) async throws
    -> T
  { try decode(await sendRaw(path: path, method: method, body: body, query: [])) }
  private func sendRaw<B: Encodable>(path: String, method: String, body: B?, query: [URLQueryItem])
    async throws -> HTTPResponse
  {
    try await sendRaw(
      components: path.split(separator: "/").map(String.init), method: method, body: body,
      query: query)
  }

  private func sendRaw<B: Encodable>(
    components: [String], method: String, body: B?, query: [URLQueryItem]
  )
    async throws -> HTTPResponse
  {
    var request = URLRequest(url: endpointURL(components: components, query: query))
    request.httpMethod = method
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    if let body {
      request.httpBody = try encoder.encode(body)
      request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    }
    request = transport.prepare(request)
    let span = telemetry.beginSpan(
      name: "electric.client.request", kind: .client,
      parentTraceparent: request.value(forHTTPHeaderField: "traceparent"))
    telemetry.injectTraceContext(into: &request, span: span)
    let result: HTTPResponse
    do {
      result = try await transport.send(
        request, maximumResponseBytes: responseDecodingLimits.maximumDecodedResponseBytes)
    } catch let error as HTTPTransportError {
      let limitAttributes = [
        "http.method": method, "http.route": "/\(components.first ?? "v1")",
        "error.type": "response_too_large",
      ]
      telemetry.endSpan(span, attributes: limitAttributes)
      telemetry.recordCounter("electric.http.requests", attributes: limitAttributes)
      throw ClientError.responseTooLarge(
        limit: error.responseLimit, observed: error.observedResponseBytes)
    } catch {
      telemetry.endSpan(
        span,
        attributes: [
          "http.method": method, "http.route": "/\(components.first ?? "v1")",
          "error.type": "transport",
        ])
      telemetry.recordCounter(
        "electric.http.requests", attributes: ["http.method": method, "error.type": "transport"])
      throw error
    }
    let attributes = [
      "http.method": method, "http.route": "/\(components.first ?? "v1")",
      "http.status_code": "\(result.response.statusCode)",
    ]
    guard (200..<300).contains(result.response.statusCode) else {
      telemetry.endSpan(span, attributes: attributes)
      telemetry.recordCounter("electric.http.requests", attributes: attributes)
      throw nativeHTTPError(response: result.response)
    }
    guard result.data.count <= responseDecodingLimits.maximumDecodedResponseBytes else {
      let limitAttributes = [
        "http.method": method, "http.route": "/\(components.first ?? "v1")",
        "error.type": "response_too_large",
      ]
      // The response's bytes and caller credentials are never telemetry attributes.
      telemetry.endSpan(span, attributes: limitAttributes)
      telemetry.recordCounter("electric.http.requests", attributes: limitAttributes)
      throw ClientError.responseTooLarge(
        limit: responseDecodingLimits.maximumDecodedResponseBytes,
        observed: responseDecodingLimits.observedByteCount(for: result.data))
    }
    telemetry.endSpan(span, attributes: attributes)
    telemetry.recordCounter("electric.http.requests", attributes: attributes)
    return result
  }

  /// Builds a URL one path component at a time. Shape IDs are server-generated but are still
  /// escaped here because this method is also reachable with caller-supplied IDs.
  private func endpointURL(path: String, query: [URLQueryItem]) -> URL {
    endpointURL(
      components: path.split(separator: "/", omittingEmptySubsequences: true).map(String.init),
      query: query)
  }

  private func endpointURL(components: [String], query: [URLQueryItem]) -> URL {
    let allowed = CharacterSet.urlPathAllowed.subtracting(CharacterSet(charactersIn: "/"))
    let escaped = components.map { $0.addingPercentEncoding(withAllowedCharacters: allowed) ?? $0 }
    let separator = baseURL.absoluteString.hasSuffix("/") ? "" : "/"
    // Construct from the escaped URL string: URL.appendingPathComponent normalizes an escaped
    // slash back into a path separator on some Foundation implementations.
    let raw = baseURL.absoluteString + separator + escaped.joined(separator: "/")
    let url = URL(string: raw) ?? baseURL
    return query.isEmpty ? url : url.appending(queryItems: query)
  }
  private func decode<T: Decodable>(_ result: HTTPResponse) throws -> T {
    do { return try decoder.decode(T.self, from: result.data) } catch {
      throw ClientError.decoding(String(describing: error))
    }
  }
}

/// Converts a native non-success response into a caller-actionable typed outcome. This lives at
/// the HTTP boundary so both control requests and durable-stream polling retain Retry-After rather
/// than asking a coordinator to parse response text or headers it can no longer see.
func nativeHTTPError(response: HTTPURLResponse) -> ClientError {
  let status = response.statusCode
  if status == 408 || status == 425 || status == 429 || (500...599).contains(status) {
    return .retryableHTTP(status: status, retryAfter: retryAfterDuration(from: response))
  }
  // Error pages can include echoed credentials or private request context. The public error
  // retains only status and this fixed classification; bytes and headers never cross the boundary.
  return .http(status: status, message: "HTTP request failed")
}

private func retryAfterDuration(from response: HTTPURLResponse) -> Duration? {
  guard
    let raw = response.value(forHTTPHeaderField: "Retry-After")?.trimmingCharacters(
      in: .whitespacesAndNewlines),
    !raw.isEmpty
  else { return nil }
  if let seconds = Double(raw), seconds.isFinite, seconds >= 0 {
    return .seconds(seconds)
  }
  // RFC 9110 permits an HTTP-date as well as delay-seconds. A past date is an immediate retry;
  // the coordinator still bounds the resulting delay by its policy.
  let formats = [
    "EEE',' dd MMM yyyy HH':'mm':'ss zzz",
    "EEEE',' dd-MMM-yy HH':'mm':'ss zzz",
    "EEE MMM d HH':'mm':'ss yyyy",
  ]
  for format in formats {
    let formatter = DateFormatter()
    formatter.locale = Locale(identifier: "en_US_POSIX")
    formatter.timeZone = TimeZone(secondsFromGMT: 0)
    formatter.dateFormat = format
    if let date = formatter.date(from: raw) {
      return .seconds(max(0, date.timeIntervalSinceNow))
    }
  }
  return nil
}
