import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private struct KeyedRow: Equatable, Sendable {
  let id: String
  let clientID: String
  let recordID: String
  let title: String

  init(row: ChangeRow) throws {
    guard case .string(let id) = row["id"],
      case .string(let clientID) = row["client_id"],
      case .string(let recordID) = row["record_id"],
      case .string(let title) = row["title"]
    else { throw DecodeError() }
    self.id = id
    self.clientID = clientID
    self.recordID = recordID
    self.title = title
  }

  struct DecodeError: Error {}
}

private actor KeyedScriptedTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var queryBodies: [String]
  private var blockQueries = false
  private var queryStarted = false
  private var queryReleased = false
  private var queryWaiters: [CheckedContinuation<Void, Never>] = []
  private let streamStatus: Int?

  init(
    queryBodies: [String] = [#"{"rows":[],"lsn":"0/10"}"#], streamStatus: Int? = nil
  ) {
    self.queryBodies = queryBodies
    self.streamStatus = streamStatus
  }

  func setBlockQueries(_ value: Bool) { blockQueries = value }

  func waitForQuery() async {
    if queryStarted { return }
    await withCheckedContinuation { queryWaiters.append($0) }
  }

  func releaseQueries() {
    queryReleased = true
    for waiter in queryWaiters { waiter.resume() }
    queryWaiters.removeAll()
  }

  func queryCount() -> Int {
    requests.filter { $0.httpMethod == "POST" && $0.url?.path == "/v1/subsets/query" }.count
  }

  func feedCount() -> Int {
    requests.filter { $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds" }.count
  }

  func queryRequests() -> [URLRequest] {
    requests.filter { $0.httpMethod == "POST" && $0.url?.path == "/v1/subsets/query" }
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch (request.httpMethod, request.url?.path) {
    case ("POST", "/v1/subset-feeds"):
      return response(
        #"{"shapeId":"shape-1","table":"public.records","streamPath":"/streams/shape-1","streamUrl":"https://streams.test/streams/shape-1"}"#,
        request: request)
    case ("HEAD", "/streams/shape-1"):
      return response("", request: request, headers: ["stream-next-offset": "10"])
    case ("GET", "/streams/shape-1"):
      return response("closed", request: request, status: streamStatus ?? 500)
    case ("POST", "/v1/subsets/query"):
      queryStarted = true
      for waiter in queryWaiters { waiter.resume() }
      queryWaiters.removeAll()
      if blockQueries && !queryReleased {
        await withCheckedContinuation { queryWaiters.append($0) }
      }
      let body = queryBodies.isEmpty ? #"{"rows":[],"lsn":"0/30"}"# : queryBodies.removeFirst()
      return response(body, request: request)
    case ("DELETE", "/v1/shapes/shape-1"):
      return response("", request: request, status: 204)
    default:
      return response("unexpected", request: request, status: 500)
    }
  }

  private func response(
    _ body: String, request: URLRequest, status: Int = 200, headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

private func row(_ id: String, _ clientID: String, _ recordID: String, _ title: String) -> String {
  #"{"id":"\#(id)","client_id":"\#(clientID)","record_id":"\#(recordID)","title":"\#(title)"}"#
}

private func page(_ rows: [String], lsn: String = "0/10") -> String {
  #"{"rows":[\#(rows.joined(separator: ","))],"lsn":"\#(lsn)"}"#
}

private func tail(
  operation: ChangeOperation, key: String, value: String? = nil, old: String? = nil,
  lsn: String = "0/20"
) -> ChangeBatch {
  let decode: (String?) -> ChangeRow? = { raw in
    guard let raw, let data = raw.data(using: .utf8) else { return nil }
    return try? JSONDecoder().decode(ChangeRow.self, from: data)
  }
  return ChangeBatch([
    ChangeEnvelope(
      type: "public.records", key: key, value: decode(value), old: decode(old),
      headers: EnvelopeHeaders(operation: operation, lsn: lsn))
  ])
}

private func makeWindow(
  _ transport: KeyedScriptedTransport,
  keyColumn: String = "client_id",
  onChanges: (@Sendable ([CollectionChange<KeyedRow, String>]) async throws -> Void)? = nil
) -> KeyedWindow<KeyedRow, String> {
  KeyedWindow(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport),
    transport: transport,
    table: "public.records",
    keyColumn: keyColumn,
    decodeRow: KeyedRow.init(row:),
    decodePrimaryKey: { $0 },
    primaryKeyForRow: \.id,
    secondaryKeyForRow: { row in
      keyColumn == "client_id" ? .string(row.clientID) : .string(row.recordID)
    },
    onChanges: onChanges
  )
}

@Suite("Keyed window")
struct KeyedWindowTests {
  @Test func S1TailWinsOverPageSnapshotWhilePageIsInFlight() async throws {
    let transport = KeyedScriptedTransport(queryBodies: [page([row("1", "c1", "r1", "old")])])
    await transport.setBlockQueries(true)
    let window = makeWindow(transport)
    try await window.start(subscription: "s1")
    let adding = Task { try await window.addKeys([.string("c1")]) }
    await transport.waitForQuery()
    let live = try await window.applyTail(
      tail(
        operation: .update, key: "1", value: row("1", "c1", "r1", "tail"), lsn: "0/20"))
    await transport.releaseQueries()
    let pageChanges = try await adding.value
    #expect(live.count == 1)
    #expect(pageChanges.isEmpty)
    #expect(await window.debugState().heldPrimaryKeyCount == 1)
    try await window.stop()
  }

  @Test func S2DeletesUseSecondaryToPrimaryReverseMap() async throws {
    let transport = KeyedScriptedTransport(
      queryBodies: [page([row("1", "client-1", "record-1", "held")])])
    let window = makeWindow(transport)
    try await window.start(subscription: "s2")
    _ = try await window.addKeys([.string("client-1")])
    let changes = try await window.applyTail(tail(operation: .delete, key: "1", lsn: "0/20"))
    guard case .delete(let key, _) = try #require(changes.first) else {
      Issue.record("expected delete for the primary key")
      return
    }
    #expect(key == "1")
    try await window.stop()
  }

  @Test func S2CompositePrimaryKeyDeleteAlsoUsesReverseMap() async throws {
    let transport = KeyedScriptedTransport(
      queryBodies: [page([row("1:field", "client-1", "record-1", "held")])])
    let window = makeWindow(transport)
    try await window.start(subscription: "s2-composite")
    _ = try await window.addKeys([.string("client-1")])
    let changes = try await window.applyTail(
      tail(operation: .delete, key: "1:field", lsn: "0/20"))
    guard case .delete(let key, _) = try #require(changes.first) else {
      Issue.record("expected composite primary-key delete")
      return
    }
    #expect(key == "1:field")
    try await window.stop()
  }

  @Test func S3UpdatesMoveRowsIntoAndOutOfClaimedSecondaryKeys() async throws {
    let transport = KeyedScriptedTransport(
      queryBodies: [page([
        row("1", "old", "record-1", "old row"), row("2", "outside", "record-2", "outside row")
      ])])
    let window = makeWindow(transport)
    try await window.start(subscription: "s3")
    _ = try await window.addKeys([.string("old"), .string("new")])
    let out = try await window.applyTail(
      tail(operation: .update, key: "1", value: row("1", "outside", "record-1", "moved out")))
    let into = try await window.applyTail(
      tail(operation: .update, key: "2", value: row("2", "new", "record-2", "moved in"), lsn: "0/30"))
    #expect(out.contains { if case .delete(let key, _) = $0 { return key == "1" }; return false })
    #expect(into.contains { if case .upsert(let row, _) = $0 { return row.id == "2" }; return false })
    try await window.stop()
  }

  @Test func S4UnheldTailSkipsBeforeModelDecoding() async throws {
    let transport = KeyedScriptedTransport()
    let window = KeyedWindow<KeyedRow, String>(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      table: "public.records",
      keyColumn: "client_id",
      decodeRow: { (_: ChangeRow) throws -> KeyedRow in throw KeyedRow.DecodeError() },
      decodePrimaryKey: { $0 },
      primaryKeyForRow: \.id,
      secondaryKeyForRow: { .string($0.clientID) }
    )
    try await window.start(subscription: "s4")
    let changes = try await window.applyTail(
      tail(operation: .update, key: "unknown", value: row("unknown", "other", "r", "bad")))
    #expect(changes.isEmpty)
    try await window.stop()
  }

  @Test func S5JoinsAnInFlightPageAndHeldKeysAnswerWithoutFetching() async throws {
    let transport = KeyedScriptedTransport(queryBodies: [page([row("1", "c1", "r1", "held")])])
    await transport.setBlockQueries(true)
    let window = makeWindow(transport)
    try await window.start(subscription: "s5")
    let first = Task { try await window.addKeys([.string("c1")]) }
    await transport.waitForQuery()
    let second = Task { try await window.addKeys([.string("c1")]) }
    await transport.releaseQueries()
    _ = try await first.value
    _ = try await second.value
    #expect(await transport.queryCount() == 1)
    _ = try await window.addKeys([.string("c1")])
    #expect(await transport.queryCount() == 1)
    try await window.stop()
  }

  @Test func S6OlderPageCannotOverwriteAnAcceptedTailVersion() async throws {
    let transport = KeyedScriptedTransport(queryBodies: [page([row("1", "c1", "r1", "old")])])
    await transport.setBlockQueries(true)
    let window = makeWindow(transport)
    try await window.start(subscription: "s6")
    let adding = Task { try await window.addKeys([.string("c1")]) }
    await transport.waitForQuery()
    _ = try await window.applyTail(
      tail(operation: .update, key: "1", value: row("1", "c1", "r1", "new"), lsn: "0/20"))
    await transport.releaseQueries()
    let pageChanges = try await adding.value
    #expect(pageChanges.isEmpty)
    try await window.stop()
  }

  @Test func S7RestartRepagesHeldKeysAndReleasesOmittedRows() async throws {
    let transport = KeyedScriptedTransport(
      queryBodies: [page([row("1", "c1", "r1", "held")]), page([], lsn: "0/30")],
      streamStatus: 410)
    let window = makeWindow(transport)
    try await window.start(subscription: "s7")
    _ = try await window.addKeys([.string("c1")])
    await #expect(throws: StreamError.self) { try await window.run() }
    #expect(await window.debugState().state == .ended)
    let changes = try await window.restart(subscription: "s7-restart")
    #expect(changes.contains { if case .delete(let key, _) = $0 { return key == "1" }; return false })
    #expect(await window.debugState().claimedKeyCount == 0)
    #expect(await transport.feedCount() == 2)
    try await window.stop()
  }

  @Test func S8CancellationMidPageLeavesNoClaimsOrPendingPages() async throws {
    let transport = KeyedScriptedTransport(queryBodies: [page([row("1", "c1", "r1", "held")])])
    await transport.setBlockQueries(true)
    let window = makeWindow(transport)
    try await window.start(subscription: "s8")
    let adding = Task { try await window.addKeys([.string("c1")]) }
    await transport.waitForQuery()
    adding.cancel()
    await transport.releaseQueries()
    await #expect(throws: CancellationError.self) { try await adding.value }
    #expect(await window.debugState().claimedKeyCount == 0)
    #expect(await window.debugState().inFlightPageCount == 0)
    try await window.stop()
  }

  @Test func S9OneHundredFourteenKeysUseTwoPagesAndOneFeed() async throws {
    // Synthetic 2026-27 hockey season: 114 plausible event rows, with no customer data.
    let season = (1...114).map { index in
      row(
        "event-\(index)", "season-client-\(index)", "game-\(index)",
        "2026-27 Ice Bears vs Wolves game \(index)")
    }
    let transport = KeyedScriptedTransport(
      queryBodies: [
        page(Array(season.prefix(100))),
        page(Array(season.dropFirst(100)), lsn: "0/20"),
      ])
    let window = makeWindow(transport)
    try await window.start(subscription: "s9")
    let keys = (1...114).map { JSONValue.string("season-client-\($0)") }
    _ = try await window.addKeys(keys)
    #expect(await transport.feedCount() == 1)
    #expect(await transport.queryCount() == 2)
    #expect(await window.debugState().claimedKeyCount == 114)
    #expect(await window.debugState().heldPrimaryKeyCount == 114)
    let feedRequest = try #require(
      await transport.requests.first(where: {
        $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds"
      }))
    let feed = try JSONDecoder().decode(ShapeRequest.self, from: try #require(feedRequest.httpBody))
    #expect(feed.changesOnly == true)
    #expect(feed.where == nil)
    let requests = await transport.queryRequests()
    let queries = try requests.map { try JSONDecoder().decode(SubsetQuery.self, from: #require($0.httpBody)) }
    #expect(queries.count == 2)
    #expect(queries.allSatisfy { if case .or(let leaves) = $0.where { return leaves.count <= 100 }; return false })
    try await window.stop()
  }
}
