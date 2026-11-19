import ElectricCircuitsSwift
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// What one request was, by the engine route or stream operation it reached.
enum ProbeRequestKind: String, CaseIterable, Sendable {
  /// `POST /v1/subset-feeds`: create or renew a changes-only feed.
  case feedCreate
  /// `POST /v1/subsets/query`: one snapshot page.
  case snapshotQuery
  /// `POST /v1/subsets/load`: create or renew a group, which also reads its snapshot.
  case groupLoad
  /// `POST /v1/subsets/rejoin`: claim or renew a group the engine already holds; no rows read.
  case groupRejoin
  /// `DELETE /v1/shapes/{id}`: release a claim.
  case release
  /// `HEAD <stream>`: the stream's frontier.
  case streamHead
  /// `GET <stream>?offset=…`: one stream read, with `live=…` or, to find the head, without.
  case streamRead
  case other
}

/// Every request each scope made, counted by kind. A stream read that returned changes is also
/// counted under `streamReadsWithData`, and a request that threw under `failed`.
actor ProbeRequestLog {
  private var counts: [String: [String: Int]] = [:]

  func count(_ scope: String, _ name: String) {
    counts[scope, default: [:]][name, default: 0] += 1
  }

  func counters() -> [String: [String: Int]] {
    var result = counts
    for scope in result.keys {
      for kind in ProbeRequestKind.allCases where result[scope]?[kind.rawValue] == nil {
        result[scope]?[kind.rawValue] = 0
      }
    }
    return result
  }
}

/// The SDK's transport for one scope: every request goes to the engine or the stream URL the
/// engine returned, exactly as the SDK built it, and is counted and reported on the way.
///
/// `streamOrigin` replaces the scheme, host and port of stream requests only, for a stack whose
/// engine hands out a stream URL this process cannot reach (the app's gateway does the same). The
/// SDK still sees and stores the URL the engine returned.
struct ProbeTransport: HTTPTransport {
  let base: URLSessionTransport
  let scope: String
  let log: ProbeRequestLog
  let output: ProbeOutput
  let streamOrigin: URL?

  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    try await send(request, maximumResponseBytes: base.maximumResponseBytes)
  }

  func send(_ request: URLRequest, maximumResponseBytes: Int) async throws -> HTTPResponse {
    let kind = Self.kind(of: request)
    await log.count(scope, kind.rawValue)
    do {
      let response = try await base.send(
        routed(request, kind: kind), maximumResponseBytes: maximumResponseBytes)
      await report(kind, request: request, response: response)
      return response
    } catch {
      if !(error is CancellationError) {
        await log.count(scope, "failed")
        output.emit(
          "request",
          fields(kind, request: request).merging(["error": .string(String(describing: error))]) {
            $1
          })
      }
      throw error
    }
  }

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    let kind = Self.kind(of: request)
    await log.count(scope, kind.rawValue)
    output.emit("request", fields(kind, request: request).merging(["live": .string("sse")]) { $1 })
    try await base.stream(routed(request, kind: kind), onResponse: onResponse, onBytes: onBytes)
  }

  static func kind(of request: URLRequest) -> ProbeRequestKind {
    let method = request.httpMethod ?? "GET"
    let path = request.url?.path ?? ""
    switch method {
    case "POST" where path.hasSuffix("/v1/subset-feeds"): return .feedCreate
    case "POST" where path.hasSuffix("/v1/subsets/query"): return .snapshotQuery
    case "POST" where path.hasSuffix("/v1/subsets/load"): return .groupLoad
    case "POST" where path.hasSuffix("/v1/subsets/rejoin"): return .groupRejoin
    case "DELETE" where path.contains("/v1/shapes/"): return .release
    case "HEAD": return .streamHead
    case "GET" where request.url?.query?.contains("offset=") == true: return .streamRead
    default: return .other
    }
  }

  private func routed(_ request: URLRequest, kind: ProbeRequestKind) -> URLRequest {
    guard kind == .streamHead || kind == .streamRead, let streamOrigin, let url = request.url,
      var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
    else { return request }
    components.scheme = streamOrigin.scheme
    components.host = streamOrigin.host
    components.port = streamOrigin.port
    var routed = request
    routed.url = components.url ?? url
    return routed
  }

  private func fields(_ kind: ProbeRequestKind, request: URLRequest) -> [String: JSONValue] {
    var fields: [String: JSONValue] = [
      "scope": .string(scope), "kind": .string(kind.rawValue),
      "method": .string(request.httpMethod ?? "GET"),
    ]
    if let url = request.url,
      var components = URLComponents(url: url, resolvingAgainstBaseURL: false)
    {
      if let offset = components.queryItems?.first(where: { $0.name == "offset" })?.value {
        fields["offset"] = .string(offset)
      }
      if kind == .streamRead {
        // `none` is a read without `live`, which the stream answers at once.
        fields["live"] = .string(
          components.queryItems?.first { $0.name == "live" }?.value ?? "none")
      }
      if kind == .release {
        fields["subscription"] =
          components.queryItems?.first { $0.name == "subscription" }?.value.map(JSONValue.string)
          ?? .null
      }
      // The claim the request names: on a release, the one it releases; on a create, a load or
      // a re-join, the one the caller holds. `named` rather than `claim`, which is the answer's.
      if let named = components.queryItems?.first(where: { $0.name == "claim" })?.value {
        fields["named"] = .string(named)
      }
      components.query = nil
      fields["url"] = .string(components.string ?? url.absoluteString)
    }
    if case .object(let body)? = ProbeJSON.body(request.httpBody) {
      if let subscription = body["subscription"] { fields["subscription"] = subscription }
    }
    return fields
  }

  private func report(_ kind: ProbeRequestKind, request: URLRequest, response: HTTPResponse) async {
    var fields = fields(kind, request: request)
    let headers = response.response
    fields["status"] = .int(Int64(headers.statusCode))
    switch kind {
    case .streamRead, .streamHead:
      if let next = headers.value(forHTTPHeaderField: "stream-next-offset") {
        fields["next"] = .string(next)
      }
      if headers.value(forHTTPHeaderField: "stream-closed")?.lowercased() == "true" {
        fields["closed"] = .bool(true)
      }
      if headers.value(forHTTPHeaderField: "stream-up-to-date") != nil {
        fields["upToDate"] = .bool(true)
      }
      // The engine's envelopes exactly as the stream served them.
      if kind == .streamRead, case .array(let envelopes)? = ProbeJSON.body(response.data),
        !envelopes.isEmpty
      {
        fields["envelopes"] = .array(envelopes)
        await log.count(scope, "streamReadsWithData")
      }
    case .feedCreate, .groupLoad, .groupRejoin, .snapshotQuery:
      if case .object(let body)? = ProbeJSON.body(response.data, limit: 4_194_304) {
        for name in [
          "shapeId", "streamUrl", "leaseSeconds", "offset", "lsn", "snapshot", "horizon", "claim",
        ] {
          if let value = body[name] { fields[name] = value }
        }
        if case .array(let rows)? = body["rows"] { fields["rows"] = .int(Int64(rows.count)) }
        // A group's members, each with the shape the engine keeps for it.
        if let members = body["results"] { fields["members"] = members }
      }
    case .release, .other:
      break
    }
    output.emit("request", fields)
  }
}
