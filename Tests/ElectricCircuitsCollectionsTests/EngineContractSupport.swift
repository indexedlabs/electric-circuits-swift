import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// The envelopes a real engine wrote, captured by the engine repository's
/// `conformance-swift-client.test.ts` through this package's probe (`Tests/Fixtures/README.md`).
struct EngineEnvelopeFixture: Decodable {
  struct Stream: Decodable {
    let envelopes: [JSONValue]

    /// The envelopes as one stream read's body.
    var body: String { EngineEnvelopeFixture.body(envelopes) }
  }

  let engine: String
  let plain: Stream
  let subquery: Stream
  let group: Stream

  static func load() throws -> EngineEnvelopeFixture {
    let url = URL(fileURLWithPath: #filePath)
      .deletingLastPathComponent()
      .deletingLastPathComponent()
      .appendingPathComponent("Fixtures/engine-live-envelopes.json")
    return try JSONDecoder().decode(EngineEnvelopeFixture.self, from: Data(contentsOf: url))
  }

  static func body(_ envelopes: [JSONValue]) -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return String(decoding: (try? encoder.encode(envelopes)) ?? Data("[]".utf8), as: UTF8.self)
  }
}

/// A row of the fixture's `items` table.
struct ContractItem: Equatable, Sendable {
  let id: Int64
  let listID: Int64
  let title: String

  init(id: Int64, listID: Int64 = 1, title: String) {
    self.id = id
    self.listID = listID
    self.title = title
  }

  init(row: ChangeRow) throws {
    guard case .int(let id) = row["id"], case .int(let listID) = row["list_id"],
      case .string(let title) = row["title"]
    else { throw ContractDecodeFailure() }
    self.init(id: id, listID: listID, title: title)
  }
}

/// A row of the fixture's `lists` table.
struct ContractList: Equatable, Sendable {
  let id: Int64
  let owner: String

  init(id: Int64, owner: String) {
    self.id = id
    self.owner = owner
  }

  init(row: ChangeRow) throws {
    guard case .int(let id) = row["id"], case .string(let owner) = row["owner"] else {
      throw ContractDecodeFailure()
    }
    self.init(id: id, owner: owner)
  }
}

struct ContractDecodeFailure: Error {}

func contractKey(_ key: String) throws -> Int64 {
  guard let id = Int64(key) else { throw ContractDecodeFailure() }
  return id
}

/// The engine's surface as the SDK uses it, with the engine's rules for what a stream is:
///
/// - A stream is positional. A read at an offset answers the batch stored at that offset, however
///   often it is asked, and says so when that batch ends at the stream's head
///   (`stream-up-to-date`). A batch here is one `append`, so a reader that is several appends
///   behind gets them one read at a time and only the last is marked.
/// - A read without `live` (a catch-up read) never waits: at the head it answers an empty batch
///   marked up to date. A `live=long-poll` read at the head waits for the poll window, which no
///   test sits through; `idle` ends it for the next such reads with a `204`. A `live=sse` read
///   sends each batch and its control, sends an up-to-date control at once when it opens at the
///   head, and stays open.
/// - A changes-only feed is shared by its request, whatever subscription claims it, and keeps its
///   stream after a release. `retireFeed()` is the engine retiring it: the next create mints another.
/// - A group belongs to its subscription. A load under a subscription that holds a group renews
///   it on the same stream; any other load creates a group on a new stream. A re-join
///   (`POST /v1/subsets/rejoin`) claims the group its subscription holds and answers the claim
///   with no rows, or `412` when there is none; it never creates one. What a release does to a
///   group, and whether the route exists at all, are the initializer's `releasesGroups` and
///   `rejoinRoute`.
/// - A claim has an identity. A feed create, a group load and a re-join answer the identity of
///   the claim their subscription holds afterwards (`claim`): the one it had when the request
///   named it (`?claim=…`), a new one otherwise. A release that names a claim releases it only
///   while it is the one held, and is a success either way; one that names none releases
///   whatever claim the subscription holds. `heldClaim` is what a subscription holds now.
actor ContractEngine: HTTPTransport {
  struct Request: Equatable, Sendable {
    let kind: Kind
    let subscription: String?
    let stream: String?
    let offset: String?
    /// A stream read's `live` parameter; nil for a catch-up read.
    var live: String? = nil
    /// The claim the request named in `?claim=…`: the one a release releases, or the one the
    /// caller of a create, a load or a re-join holds.
    var claim: String? = nil
  }

  enum Kind: Hashable, Sendable {
    case feedCreate, frontier, snapshotQuery, groupLoad, groupRejoin, streamRead, release
  }

  static let start = "0000000000000000_0000000000000000"
  static let streamBase = "https://streams.test/stores/g1/queries/q1/shape/"

  private(set) var requests: [Request] = []
  private var batches: [String: [String: (body: String, next: String)]] = [:]
  private var tails: [String: String] = [:]
  private var idleAnswers: [String: Int] = [:]
  private var statuses: [String: [Int]] = [:]
  private var parked: [UUID: CheckedContinuation<Void, Never>] = [:]
  private var feedGeneration = 1
  private var snapshotBodies: [String]
  private var groups: [String: String] = [:]
  private var groupCount = 0
  /// The identity of the claim each subscription holds.
  private var claims: [String: String] = [:]
  private var claimCount = 0
  private var groupRows: String
  private let groupPosition: String
  private let releasesGroups: Bool
  private let rejoinRoute: Bool
  /// Refusals the next claim requests of a kind answer, one each, before anything else.
  private var claimRefusals: [Kind: [(status: Int, retryAfter: String?)]] = [:]

  /// `snapshots` answer successive `/v1/subsets/query` calls (the last repeats); `groupRows` is
  /// the `rows` array every group load answers with, and `groupPosition` its `lsn`, `snapshot`
  /// and `horizon` members.
  ///
  /// With `releasesGroups` true a release retires the group and its stream, as an engine that
  /// keeps a group only while it is claimed does. With false a released group is kept for its
  /// subscription to come back to, as the engine that has the re-join route keeps it (and as the
  /// engine at `1be10fb` happened to, for a release that names the group's own id). `lapseGroup`
  /// is the engine retiring a group either way.
  ///
  /// With `rejoinRoute` false the engine has no `POST /v1/subsets/rejoin` and answers it `404`,
  /// as the engine at `1be10fb` does.
  init(
    snapshots: [String], groupRows: String = "[]",
    groupPosition: String = #""lsn":"0/1BF2C98","snapshot":"767:767:","horizon":"0/1BF2C98""#,
    releasesGroups: Bool = true, rejoinRoute: Bool = true
  ) {
    snapshotBodies = snapshots
    self.groupRows = groupRows
    self.groupPosition = groupPosition
    self.releasesGroups = releasesGroups
    self.rejoinRoute = rejoinRoute
  }

  static func streamURL(_ stream: String) -> String { streamBase + stream }

  /// Appends a batch at the stream's tail and returns the offset after it.
  @discardableResult
  func append(_ stream: String, _ body: String) -> String {
    let offset = tails[stream] ?? Self.start
    let count = batches[stream]?.count ?? 0
    let next = "0000000000000000_" + String(repeating: "0", count: 12) + String(1001 + count)
    batches[stream, default: [:]][offset] = (body, next)
    tails[stream] = next
    wake()
    return next
  }

  /// The next `count` reads at the stream's tail answer `204` instead of waiting.
  func idle(_ stream: String, count: Int = 1) {
    idleAnswers[stream, default: 0] += count
    wake()
  }

  /// The next reads of the stream answer these statuses, one each, before anything else.
  func refuse(_ stream: String, statuses: [Int]) {
    self.statuses[stream, default: []] += statuses
    wake()
  }

  /// The next `kind` requests (a feed create, a group load or a group re-join) are refused with
  /// `status`, one each, as an engine at its shape cap refuses a create: `503` with `Retry-After`.
  func refuseClaims(_ kind: Kind, status: Int, retryAfter: String? = nil, count: Int = 1) {
    claimRefusals[kind, default: []] += Array(repeating: (status, retryAfter), count: count)
  }

  private func refusal(of kind: Kind, to request: URLRequest) -> HTTPResponse? {
    guard var pending = claimRefusals[kind], !pending.isEmpty else { return nil }
    let refused = pending.removeFirst()
    claimRefusals[kind] = pending
    return reply(
      #"{"error":"refused"}"#, to: request, status: refused.status,
      headers: refused.retryAfter.map { ["Retry-After": $0] } ?? [:])
  }

  /// The engine retires the feed: the next create for the same request mints another stream.
  func retireFeed() { feedGeneration += 1 }

  /// The engine retires a group: its lease lapsed and nobody came back for it.
  func lapseGroup(subscription: String) {
    groups.removeValue(forKey: subscription)
    claims.removeValue(forKey: subscription)
  }

  /// The identity of the claim `subscription` holds, or nil when it holds none.
  func heldClaim(_ subscription: String) -> String? { claims[subscription] }

  /// Takes or renews `subscription`'s claim as the engine does: the identity is kept for a
  /// request that names it and is new for any other.
  private func grant(_ subscription: String, to request: URLRequest) -> String {
    let named = requests.last?.claim
    if let held = claims[subscription], held == named { return held }
    claimCount += 1
    claims[subscription] = "claim-\(claimCount)"
    return "claim-\(claimCount)"
  }

  func setGroupRows(_ rows: String) { groupRows = rows }

  var feedStream: String { "s\(feedGeneration)" }

  func count(_ kind: Kind) -> Int { requests.filter { $0.kind == kind }.count }

  func requests(of kind: Kind) -> [Request] { requests.filter { $0.kind == kind } }

  /// The kinds of every request from index `from` on, without the open-ended stream reads.
  func kinds(from: Int = 0) -> [Kind] { requests.dropFirst(from).map(\.kind) }

  private func wake() {
    let waiting = parked.values
    parked.removeAll()
    for continuation in waiting { continuation.resume() }
  }

  private func park() async throws {
    let id = UUID()
    await withTaskCancellationHandler {
      await withCheckedContinuation { continuation in
        if Task.isCancelled {
          continuation.resume()
        } else {
          parked[id] = continuation
        }
      }
    } onCancel: {
      Task { await self.unpark(id) }
    }
    try Task.checkCancellation()
  }

  private func unpark(_ id: UUID) {
    parked.removeValue(forKey: id)?.resume()
  }

  private func record(_ kind: Kind, _ request: URLRequest, subscription: String? = nil) {
    let components = request.url.flatMap {
      URLComponents(url: $0, resolvingAgainstBaseURL: false)
    }
    let path = request.url?.path ?? ""
    requests.append(
      Request(
        kind: kind,
        subscription: subscription
          ?? components?.queryItems?.first { $0.name == "subscription" }?.value,
        stream: path.contains("/shape/") ? path.components(separatedBy: "/shape/").last : nil,
        offset: components?.queryItems?.first { $0.name == "offset" }?.value,
        live: components?.queryItems?.first { $0.name == "live" }?.value,
        claim: components?.queryItems?.first { $0.name == "claim" }?.value))
  }

  private struct Claim: Decodable {
    let subscription: String?
  }

  private func subscription(of request: URLRequest) -> String? {
    request.httpBody.flatMap { try? JSONDecoder().decode(Claim.self, from: $0) }?.subscription
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch (request.httpMethod ?? "GET", path) {
    case ("POST", "/v1/subset-feeds"):
      let subscription = subscription(of: request)
      record(.feedCreate, request, subscription: subscription)
      if let refused = refusal(of: .feedCreate, to: request) { return refused }
      let stream = feedStream
      let claim = grant(subscription ?? "", to: request)
      return reply(
        #"{"shapeId":"\#(stream)","table":"public.items","streamPath":"shape/\#(stream)","#
          + #""streamUrl":"\#(Self.streamURL(stream))","subscription":"\#(subscription ?? "")","#
          + #""claim":"\#(claim)"}"#,
        to: request)
    case ("POST", "/v1/subsets/query"):
      record(.snapshotQuery, request)
      let body = snapshotBodies.count > 1 ? snapshotBodies.removeFirst() : snapshotBodies[0]
      return reply(body, to: request)
    case ("POST", "/v1/subsets/load"):
      let subscription = subscription(of: request) ?? ""
      record(.groupLoad, request, subscription: subscription)
      if let refused = refusal(of: .groupLoad, to: request) { return refused }
      if groups[subscription] == nil {
        groupCount += 1
        groups[subscription] = "group-\(groupCount)"
      }
      let stream = groups[subscription]!
      let claim = grant(subscription, to: request)
      return reply(
        #"{"groupId":"shape/\#(stream)","shapeId":"shape/\#(stream)","claim":"\#(claim)","#
          + #""subscription":"\#(subscription)","streamPath":"shape/\#(stream)","#
          + #""streamUrl":"\#(Self.streamURL(stream))","offset":"\#(tails[stream] ?? Self.start)","#
          + #"\#(groupPosition),"rows":\#(groupRows),"#
          + #""results":[{"key":"lists","table":"public.lists","shapeId":"s11"},"#
          + #"{"key":"items","table":"public.items","shapeId":"s12"}]}"#,
        to: request)
    case ("POST", "/v1/subsets/rejoin"):
      let subscription = subscription(of: request) ?? ""
      record(.groupRejoin, request, subscription: subscription)
      guard rejoinRoute else { return reply("Not Found", to: request, status: 404) }
      if let refused = refusal(of: .groupRejoin, to: request) { return refused }
      guard let stream = groups[subscription] else {
        return reply(#"{"error":"no such group"}"#, to: request, status: 412)
      }
      // A load's answer less the snapshot: no `rows`, `lsn`, `snapshot` or `horizon`.
      let claim = grant(subscription, to: request)
      return reply(
        #"{"groupId":"shape/\#(stream)","shapeId":"shape/\#(stream)","claim":"\#(claim)","#
          + #""subscription":"\#(subscription)","streamPath":"shape/\#(stream)","#
          + #""streamUrl":"\#(Self.streamURL(stream))","offset":"\#(tails[stream] ?? Self.start)","#
          + #""results":[{"key":"lists","table":"public.lists","shapeId":"s11"},"#
          + #"{"key":"items","table":"public.items","shapeId":"s12"}]}"#,
        to: request)
    case ("DELETE", _) where path.hasPrefix("/v1/shapes/"):
      record(.release, request)
      let released = requests.last?.subscription
      let named = requests.last?.claim
      // A release that names a claim the subscription no longer holds changes nothing.
      if let released, named == nil || claims[released] == named {
        claims.removeValue(forKey: released)
        // An engine that keeps a group only while it is claimed retires it here.
        if path.contains("group-"), releasesGroups { groups.removeValue(forKey: released) }
      }
      return reply("", to: request)
    case ("HEAD", _):
      record(.frontier, request)
      let stream = requests.last?.stream ?? ""
      return reply("", to: request, headers: ["stream-next-offset": tails[stream] ?? Self.start])
    case ("GET", _):
      record(.streamRead, request)
      let stream = requests.last?.stream ?? ""
      let offset = requests.last?.offset ?? ""
      let live = requests.last?.live
      while true {
        if var pending = statuses[stream], !pending.isEmpty {
          let status = pending.removeFirst()
          statuses[stream] = pending
          return reply("", to: request, status: status)
        }
        if let batch = batches[stream]?[offset] {
          var headers = ["stream-next-offset": batch.next]
          if batch.next == tails[stream] { headers["stream-up-to-date"] = "true" }
          return reply(batch.body, to: request, headers: headers)
        }
        let atHead = ["stream-next-offset": offset, "stream-up-to-date": "true"]
        if live == nil { return reply("[]", to: request, headers: atHead) }
        if let remaining = idleAnswers[stream], remaining > 0 {
          idleAnswers[stream] = remaining - 1
          return reply("", to: request, status: 204, headers: atHead)
        }
        try await park()
      }
    default:
      return reply("unexpected", to: request, status: 500)
    }
  }

  /// A `live=sse` read, as the class comment describes it.
  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    record(.streamRead, request)
    let stream = requests.last?.stream ?? ""
    var offset = requests.last?.offset ?? ""
    func head(_ status: Int, _ fields: [String: String]) -> HTTPURLResponse {
      HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: fields)!
    }
    if var pending = statuses[stream], !pending.isEmpty {
      let status = pending.removeFirst()
      statuses[stream] = pending
      try await onResponse(head(status, [:]))
      return
    }
    try await onResponse(head(200, ["Content-Type": "text/event-stream"]))
    var saidUpToDate = false
    while true {
      if let batch = batches[stream]?[offset] {
        try await onBytes(Data("event: data\ndata:\(batch.body)\n\n".utf8))
        offset = batch.next
        saidUpToDate = batch.next == tails[stream]
        try await onBytes(Self.control(offset, upToDate: saidUpToDate))
      } else if !saidUpToDate {
        saidUpToDate = true
        try await onBytes(Self.control(offset, upToDate: true))
      } else {
        try await park()
      }
    }
  }

  /// An SSE control event. This model leaves `upToDate` out of a control that does not end at
  /// the head, the least a server may say there.
  private static func control(_ offset: String, upToDate: Bool) -> Data {
    let flag = upToDate ? #","upToDate":true"# : ""
    return Data("event: control\ndata:{\"streamNextOffset\":\"\(offset)\"\(flag)}\n\n".utf8)
  }

  private func reply(
    _ body: String, to request: URLRequest, status: Int = 200, headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

let contractItems = CollectionDefinition<ContractItem, Int64>(
  id: CollectionID(rawValue: "items"), key: \.id)
let contractLists = CollectionDefinition<ContractList, Int64>(
  id: CollectionID(rawValue: "lists"), key: \.id)

/// The fixture's plain scope: `items` in list 1.
let listOneItems = CollectionDemand<ContractItem>(
  unsafePredicateIdentity: "list_id=1",
  sourcePredicate: .leaf(column: "list_id", op: .eq, value: .int(1)))

/// The snapshot the engine served before the fixture's changes, with its position.
let contractSnapshot =
  #"{"rows":[{"id":1,"list_id":1,"title":"one"},{"id":2,"list_id":1,"title":"two"}],"#
  + #""lsn":"0/1BF2C98","snapshot":"767:767:","horizon":"0/1BF2C98"}"#

/// A later snapshot, as the engine would serve it after the fixture's changes.
let laterContractSnapshot =
  #"{"rows":[{"id":1,"list_id":1,"title":"one-b"},{"id":2,"list_id":1,"title":"two"}],"#
  + #""lsn":"0/1BF3100","snapshot":"775:775:","horizon":"0/1BF3100"}"#

func contractScope(_ generation: String) -> CollectionScope {
  CollectionScope(principal: "user", authorization: "native", generation: generation)
}

typealias ContractCoordinator = CollectionCoordinator<
  ContractItem, Int64, CircuitsSubsetSource<ContractItem, Int64>,
  InMemoryCollectionStore<ContractItem, Int64>
>

func contractSource(_ engine: ContractEngine, liveReadMode: LiveReadMode = .longPoll)
  -> CircuitsSubsetSource<ContractItem, Int64>
{
  CircuitsSubsetSource<ContractItem, Int64>(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: engine,
      configuration: .init(liveReadMode: liveReadMode)),
    transport: engine,
    table: "items",
    retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
    decodeRow: ContractItem.init(row:),
    decodeKey: contractKey)
}

/// One launch: a new source and coordinator for `generation` over a store that outlives them.
func contractLaunch(
  _ engine: ContractEngine, store: InMemoryCollectionStore<ContractItem, Int64>,
  generation: String, liveReadMode: LiveReadMode = .longPoll
) -> ContractCoordinator {
  CollectionCoordinator(
    definition: contractItems, scope: contractScope(generation),
    source: contractSource(engine, liveReadMode: liveReadMode), store: store)
}

func contractIdentity(_ generation: String) -> CollectionDemandIdentity {
  listOneItems.identity(for: contractItems, scope: contractScope(generation))
}

/// Polls with real sleeps rather than yields: the HTTP path crosses several actors per step.
func contractEventually(_ condition: @Sendable () async throws -> Bool) async -> Bool {
  for _ in 0..<2_500 {
    if (try? await condition()) == true { return true }
    try? await Task.sleep(for: .milliseconds(2))
  }
  return (try? await condition()) == true
}
