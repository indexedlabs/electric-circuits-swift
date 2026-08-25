import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import Testing

/// Layer-A scripted native-route transport. It verifies Swift client behavior at the injectable
/// HTTP boundary only; it is not an Axum, PostgreSQL, durable-stream, or failover qualification.
private actor NativeScriptedRouteTransport: HTTPTransport {
  private var snapshots: [SubsetResponse]
  private var streamResponses: [HTTPResponse]
  private var headOffsets: [String]
  private let blocksWhenStreamIsExhausted: Bool
  private let blocksBeforeStreamRead: Int?
  private var shapeNumber = 0
  private var streamReads = 0
  private var streamGateReleased = false
  private var streamGateWaiters: [CheckedContinuation<Void, Never>] = []
  private(set) var requests: [URLRequest] = []

  init(
    snapshots: [SubsetResponse],
    streamResponses: [HTTPResponse],
    headOffsets: [String],
    blocksWhenStreamIsExhausted: Bool = false,
    blocksBeforeStreamRead: Int? = nil
  ) {
    self.snapshots = snapshots
    self.streamResponses = streamResponses
    self.headOffsets = headOffsets
    self.blocksWhenStreamIsExhausted = blocksWhenStreamIsExhausted
    self.blocksBeforeStreamRead = blocksBeforeStreamRead
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subset-feeds"):
      shapeNumber += 1
      let body = """
        {"shapeId":"feed-\(shapeNumber)","table":"public.issues","streamPath":"/v1/streams/feed-\(shapeNumber)","streamUrl":"https://engine.test/v1/streams/feed-\(shapeNumber)","subscription":"top-ten"}
        """
      return response(body, request: request)
    case ("HEAD", _):
      return response(
        "", request: request, headers: ["stream-next-offset": headOffsets.removeFirst()])
    case ("POST", "/v1/subsets/query"):
      let snapshot = snapshots.removeFirst()
      return HTTPResponse(
        data: try JSONEncoder().encode(snapshot),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
    case ("GET", _):
      streamReads += 1
      if let blocksBeforeStreamRead,
        streamReads >= blocksBeforeStreamRead,
        !streamGateReleased
      {
        await withCheckedContinuation { continuation in
          streamGateWaiters.append(continuation)
        }
      }
      if streamResponses.isEmpty {
        if blocksWhenStreamIsExhausted {
          while true {
            try Task.checkCancellation()
            await Task.yield()
          }
        }
        throw CancellationError()
      }
      return streamResponses.removeFirst()
    case ("DELETE", _):
      return response("{}", request: request)
    default:
      Issue.record("unexpected native route \(request.httpMethod ?? "nil") \(path)")
      return response("unexpected route", request: request, status: 500)
    }
  }

  func requestIndex(method: String, path: String) -> Int? {
    requests.firstIndex { $0.httpMethod == method && $0.url?.path == path }
  }

  func requestsFor(method: String, path: String) -> [URLRequest] {
    requests.filter { $0.httpMethod == method && $0.url?.path == path }
  }

  func deleteCount() -> Int {
    requests.filter { $0.httpMethod == "DELETE" }.count
  }

  func streamRequestCount() -> Int {
    requests.filter { $0.httpMethod == "GET" && $0.url?.path.hasPrefix("/v1/streams/") == true }
      .count
  }

  func releaseStreamGate() {
    streamGateReleased = true
    let waiters = streamGateWaiters
    streamGateWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
  }

  private func response(
    _ body: String,
    request: URLRequest,
    status: Int = 200,
    headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

private func nativeIssue(id: Int64, modified: Int64, title: String? = nil) -> JSONValue {
  .object([
    "id": .int(id), "title": .string(title ?? "Issue \(id)"),
    "description": .string("contract fixture"), "status": .string("backlog"),
    "priority": .string("high"), "username": .string("ada"), "project_id": .int(7),
    "created": .int(1), "modified": .int(modified), "kanbanorder": .number(Double(id)),
  ])
}

private struct NativeSourceIssue: Equatable, Sendable {
  let id: Int
  let modified: Int
  let title: String

  var row: JSONValue {
    nativeIssue(id: Int64(id), modified: Int64(modified), title: title)
  }
}

/// Independent test oracle: it knows only the source rows and documented `modified DESC, id DESC`
/// ordering, not the Swift window or materializer implementation.
private func expectedTopTen(_ source: [NativeSourceIssue]) -> [NativeSourceIssue] {
  Array(
    source.sorted {
      if $0.modified != $1.modified { return $0.modified > $1.modified }
      return $0.id > $1.id
    }.prefix(10))
}

/// A source mutation is committed only after the reader has captured the durable-stream head, then
/// is visible in both the independent snapshot and the at-least-once tail replay.
private actor HeadSnapshotOverlapTransport: HTTPTransport {
  private var source: [NativeSourceIssue]
  private let overlapMutation: NativeSourceIssue
  private var sawHead = false
  private var committedOverlap = false
  private var deliveredTail = false
  private var shapeCreated = false
  private(set) var requests: [URLRequest] = []

  init(source: [NativeSourceIssue], overlapMutation: NativeSourceIssue) {
    self.source = source
    self.overlapMutation = overlapMutation
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subset-feeds"):
      shapeCreated = true
      return response(
        """
        {"shapeId":"overlap-feed","table":"public.issues","streamPath":"/v1/streams/overlap-feed","streamUrl":"https://engine.test/v1/streams/overlap-feed","subscription":"top-ten"}
        """, request: request)
    case ("HEAD", "/v1/streams/overlap-feed"):
      guard shapeCreated else { return response("shape required", request: request, status: 409) }
      sawHead = true
      return response("", request: request, headers: ["stream-next-offset": "100"])
    case ("POST", "/v1/subsets/query"):
      guard sawHead else { return response("head required", request: request, status: 409) }
      source.removeAll { $0.id == overlapMutation.id }
      source.append(overlapMutation)
      committedOverlap = true
      let snapshot = SubsetResponse(rows: expectedTopTen(source).map(\.row), lsn: "0/101")
      return HTTPResponse(
        data: try JSONEncoder().encode(snapshot),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
    case ("GET", "/v1/streams/overlap-feed"):
      guard committedOverlap else {
        return response("snapshot required", request: request, status: 409)
      }
      if !deliveredTail {
        deliveredTail = true
        let envelope = nativeEnvelope(
          id: overlapMutation.id,
          operation: .insert,
          modified: overlapMutation.modified,
          title: overlapMutation.title,
          lsn: "0/101")
        return try nativeStreamResponse(ChangeBatch([envelope]), nextOffset: "101")
      }
      return nativeTerminalResponse()
    case ("DELETE", "/v1/shapes/overlap-feed"):
      return response("{}", request: request)
    default:
      return response("unexpected route", request: request, status: 500)
    }
  }

  func requestIndex(method: String, path: String) -> Int? {
    requests.firstIndex { $0.httpMethod == method && $0.url?.path == path }
  }

  func firstTailRequest() -> URLRequest? {
    requests.first { $0.httpMethod == "GET" && $0.url?.path == "/v1/streams/overlap-feed" }
  }

  func createRequest() -> URLRequest? {
    requests.first { $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds" }
  }

  func overlapWasCommittedAfterHeadAndDeliveredOnTail() -> Bool {
    sawHead && committedOverlap && deliveredTail
  }

  private func response(
    _ body: String,
    request: URLRequest,
    status: Int = 200,
    headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

private func nativePage(_ ids: [Int], lsn: String, titleForID: [Int: String] = [:])
  -> SubsetResponse
{
  SubsetResponse(
    rows: ids.map { id in
      nativeIssue(id: Int64(id), modified: Int64(id), title: titleForID[id])
    }, lsn: lsn)
}

private func nativeEnvelope(
  id: Int,
  operation: ChangeOperation,
  modified: Int? = nil,
  title: String? = nil,
  lsn: String
) -> ChangeEnvelope {
  ChangeEnvelope(
    type: "public.issues",
    key: String(id),
    value: operation == .delete
      ? nil
      : {
        nativeIssue(id: Int64(id), modified: Int64(modified ?? id), title: title)
          .objectValue!
      }(),
    old: operation == .delete ? ["id": .int(Int64(id))] : nil,
    headers: EnvelopeHeaders(operation: operation, lsn: lsn))
}

extension JSONValue {
  fileprivate var objectValue: ChangeRow? {
    guard case .object(let value) = self else { return nil }
    return value
  }
}

private func nativeStreamResponse(
  _ batch: ChangeBatch,
  nextOffset: String
) throws -> HTTPResponse {
  HTTPResponse(
    data: try JSONEncoder().encode(batch),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test/v1/streams/feed")!, statusCode: 200,
      httpVersion: nil, headerFields: ["stream-next-offset": nextOffset])!)
}

private func nativeTerminalResponse(closed: Bool = false) -> HTTPResponse {
  HTTPResponse(
    data: Data(),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test/v1/streams/feed")!,
      statusCode: closed ? 204 : 410,
      httpVersion: nil,
      headerFields: closed ? ["stream-closed": "true"] : nil)!)
}

@MainActor
private func waitForNativeContract(
  _ predicate: @escaping @MainActor () async -> Bool
) async throws {
  for _ in 0..<10_000 {
    if await predicate() { return }
    await Task.yield()
  }
  throw CancellationError()
}

@Suite("Native scripted top-10 contract", .serialized)
struct NativeScriptedTopTenContractTests {
  @MainActor private func session(
    transport: any HTTPTransport,
    database: DatabaseQueue
  ) -> LinearLiteSession {
    LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database,
      subscription: "top-ten",
      transport: transport,
      mode: .recentSubset(limit: 10))
  }

  @Test @MainActor
  func headSnapshotOverlapConvergesToTheIndependentTopTenOracleExactlyOnce() async throws {
    let initial = (1...10).map {
      NativeSourceIssue(id: $0, modified: $0, title: "Issue \($0)")
    }
    let overlap = NativeSourceIssue(
      id: 11, modified: 11, title: "Committed between head and snapshot")
    let expected = expectedTopTen(initial + [overlap])
    let transport = HeadSnapshotOverlapTransport(source: initial, overlapMutation: overlap)
    let session = session(transport: transport, database: try DatabaseQueue())

    await session.start()
    try await waitForNativeContract { session.connectionState == .terminal(.gone) }

    let head = try #require(
      await transport.requestIndex(method: "HEAD", path: "/v1/streams/overlap-feed"))
    let snapshot = try #require(
      await transport.requestIndex(method: "POST", path: "/v1/subsets/query"))
    let tail = try #require(
      await transport.requestIndex(method: "GET", path: "/v1/streams/overlap-feed"))
    #expect(head < snapshot && snapshot < tail)
    #expect(await transport.overlapWasCommittedAfterHeadAndDeliveredOnTail())
    let create = try #require(await transport.createRequest())
    let createBody = try #require(create.httpBody)
    let createJSON = try JSONDecoder().decode(JSONValue.self, from: createBody)
    guard case .object(let createObject) = createJSON else {
      Issue.record("subset-feed create was not a JSON object")
      return
    }
    #expect(createObject["changesOnly"] == .bool(true))
    #expect(createObject["subscription"] == .string("top-ten"))
    let tailRequest = try #require(await transport.firstTailRequest())
    #expect(tailRequest.url?.query?.contains("offset=100") == true)
    #expect(tailRequest.url?.query?.contains("live=long-poll") == true)
    #expect(session.issues.map(\.id) == expected.map { Int64($0.id) })
    #expect(session.issues.map(\.title) == expected.map(\.title))
    #expect(session.issues.filter { $0.id == 11 }.count == 1)
  }

  @Test @MainActor
  func createdUpdatedAndDeletedEnvelopesMaintainTheTopTenWithoutDuplicatingBoundaryQueries()
    async throws
  {
    let created = ChangeBatch([nativeEnvelope(id: 11, operation: .insert, lsn: "0/101")])
    let updated = ChangeBatch([
      nativeEnvelope(id: 11, operation: .update, title: "Updated issue 11", lsn: "0/102")
    ])
    let deleted = ChangeBatch([nativeEnvelope(id: 11, operation: .delete, lsn: "0/103")])
    let transport = NativeScriptedRouteTransport(
      snapshots: [
        nativePage(Array((1...10).reversed()), lsn: "0/100"),
        nativePage(Array((2...11).reversed()), lsn: "0/101"),
        nativePage(Array((1...10).reversed()), lsn: "0/103"),
      ],
      streamResponses: [
        try nativeStreamResponse(created, nextOffset: "101"),
        try nativeStreamResponse(updated, nextOffset: "102"),
        try nativeStreamResponse(deleted, nextOffset: "103"),
        nativeTerminalResponse(),
      ],
      headOffsets: ["100"],
      blocksBeforeStreamRead: 3)
    let database = try DatabaseQueue()
    let session = session(transport: transport, database: database)

    await session.start()
    try await waitForNativeContract {
      guard await transport.streamRequestCount() == 3 else { return false }
      return session.issues.first(where: { $0.id == 11 })?.title == "Updated issue 11"
    }
    #expect(session.issues.filter { $0.id == 11 }.count == 1)
    #expect(await transport.requestsFor(method: "POST", path: "/v1/subsets/query").count == 2)

    await transport.releaseStreamGate()
    try await waitForNativeContract { session.connectionState == .terminal(.gone) }

    #expect(session.issues.map(\.id) == Array((1...10).reversed()).map(Int64.init))
    #expect(await transport.requestsFor(method: "POST", path: "/v1/subsets/query").count == 3)
    let cursor: Row? = try database.read { db -> Row? in
      try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?",
        arguments: ["recent-subset-top-ten"])
    }
    #expect(cursor?["offset"] as String? == "103")
    #expect(cursor?["lsn"] as String? == "0/103")
  }

  @Test @MainActor func readerResumesFromTheCheckpointCommittedByAReseedSnapshot() async throws {
    let inserted = ChangeBatch([nativeEnvelope(id: 11, operation: .insert, lsn: "0/101")])
    let laterBatch = ChangeBatch([
      nativeEnvelope(id: 11, operation: .update, title: "Later delivery", lsn: "0/102")
    ])
    let transport = NativeScriptedRouteTransport(
      snapshots: [
        nativePage(Array((1...10).reversed()), lsn: "0/100"),
        // The SQL query-back has a newer LSN than the triggering feed batch, while preserving its
        // durable offset. The next reader apply must CAS from this committed checkpoint.
        nativePage(Array((2...11).reversed()), lsn: "0/150"),
      ],
      streamResponses: [
        try nativeStreamResponse(inserted, nextOffset: "101"),
        try nativeStreamResponse(laterBatch, nextOffset: "102"),
        nativeTerminalResponse(),
      ],
      headOffsets: ["100"])
    let database = try DatabaseQueue()
    let session = session(transport: transport, database: database)

    await session.start()
    try await waitForNativeContract {
      guard session.issues.map(\.id) == Array((2...11).reversed()).map(Int64.init) else {
        return false
      }
      let cursor = try? database.read { db in
        try Row.fetchOne(
          db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?",
          arguments: ["recent-subset-top-ten"])
      }
      return cursor?["offset"] as String? == "102"
        && cursor?["lsn"] as String? == "0/102"
    }

    #expect(session.issues.map(\.id) == Array((2...11).reversed()).map(Int64.init))
    let cursor: Row? = try database.read { db -> Row? in
      try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?",
        arguments: ["recent-subset-top-ten"])
    }
    #expect(cursor?["offset"] as String? == "102")
    #expect(cursor?["lsn"] as String? == "0/102")
  }

  @Test @MainActor func boundaryChurnRefillsTheStrictPageFromTheIndependentSubsetSnapshot()
    async throws
  {
    let movedBelowBoundary = ChangeBatch([
      nativeEnvelope(
        id: 1, operation: .update, modified: 0, title: "Moved below boundary", lsn: "0/201")
    ])
    let transport = NativeScriptedRouteTransport(
      snapshots: [
        nativePage(Array((1...10).reversed()), lsn: "0/200"),
        nativePage(Array((2...11).reversed()), lsn: "0/201"),
      ],
      streamResponses: [
        try nativeStreamResponse(movedBelowBoundary, nextOffset: "201"), nativeTerminalResponse(),
      ],
      headOffsets: ["200"])
    let session = session(transport: transport, database: try DatabaseQueue())

    await session.start()
    try await waitForNativeContract { session.connectionState == .terminal(.gone) }

    #expect(session.issues.map(\.id) == Array((2...11).reversed()).map(Int64.init))
    #expect(session.issues.count == 10)
    #expect(session.issues.first(where: { $0.id == 11 })?.title == "Issue 11")
    #expect(session.issues.contains(where: { $0.id == 1 }) == false)
    #expect(await transport.requestsFor(method: "POST", path: "/v1/subsets/query").count == 2)
  }

  @Test @MainActor func replayedChangeBatchHasExactlyOnceMaterializedEffect() async throws {
    let replayed = ChangeBatch([nativeEnvelope(id: 11, operation: .insert, lsn: "0/301")])
    let transport = NativeScriptedRouteTransport(
      snapshots: [
        nativePage(Array((1...10).reversed()), lsn: "0/300"),
        nativePage(Array((2...11).reversed()), lsn: "0/301"),
      ],
      streamResponses: [
        try nativeStreamResponse(replayed, nextOffset: "301"),
        try nativeStreamResponse(replayed, nextOffset: "302"),
        nativeTerminalResponse(),
      ],
      headOffsets: ["300"])
    let database = try DatabaseQueue()
    let session = session(transport: transport, database: database)

    await session.start()
    try await waitForNativeContract { session.connectionState == .terminal(.gone) }

    #expect(session.issues.map(\.id) == Array((2...11).reversed()).map(Int64.init))
    #expect(await transport.requestsFor(method: "POST", path: "/v1/subsets/query").count == 2)
    let cursor: Row? = try database.read { db -> Row? in
      try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?",
        arguments: ["recent-subset-top-ten"])
    }
    #expect(cursor?["offset"] as String? == "302")
    #expect(cursor?["lsn"] as String? == "0/301")
  }

  @Test @MainActor func closedStreamRequiresAnExplicitFreshSubscriptionAndSnapshot() async throws {
    let oldPage = nativePage(Array((1...10).reversed()), lsn: "0/400")
    let newPage = nativePage(Array((11...20).reversed()), lsn: "0/500")
    let transport = NativeScriptedRouteTransport(
      snapshots: [oldPage, newPage],
      streamResponses: [nativeTerminalResponse(closed: true), nativeTerminalResponse()],
      headOffsets: ["400", "500"])
    let session = session(transport: transport, database: try DatabaseQueue())

    await session.run()
    #expect(session.connectionState == .terminal(.closed))
    #expect(await transport.deleteCount() == 1)
    #expect(session.issues.map(\.id) == Array((1...10).reversed()).map(Int64.init))

    await session.run()
    #expect(session.connectionState == .terminal(.gone))
    #expect(await transport.deleteCount() == 2)
    #expect(session.issues.map(\.id) == Array((11...20).reversed()).map(Int64.init))
    #expect(await transport.requestsFor(method: "POST", path: "/v1/subset-feeds").count == 2)
  }

  @Test @MainActor func cancellingTheTopTenRunStopsTheLongPollAndReleasesTheFeed() async throws {
    let transport = NativeScriptedRouteTransport(
      snapshots: [nativePage(Array((1...10).reversed()), lsn: "0/600")],
      streamResponses: [],
      headOffsets: ["600"],
      blocksWhenStreamIsExhausted: true)
    let session = session(transport: transport, database: try DatabaseQueue())

    let run = Task { @MainActor in await session.run() }
    try await waitForNativeContract { await transport.streamRequestCount() == 1 }
    run.cancel()
    await run.value

    #expect(session.connectionState == .stopped)
    #expect(await transport.deleteCount() == 1)
  }

  @Test @MainActor func filteredRecentSubsetCarriesItsPredicateToTheSnapshotAndLiveFeed()
    async throws
  {
    let transport = NativeScriptedRouteTransport(
      snapshots: [nativePage(Array((1...10).reversed()), lsn: "0/700")],
      streamResponses: [nativeTerminalResponse()],
      headOffsets: ["700"])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "top-ten", transport: transport,
      mode: .recentSubset(
        limit: 10,
        where: .leaf(column: "username", op: .eq, value: .string("ada"))))

    await session.start()
    try await waitForNativeContract {
      let queryCount = await transport.requestsFor(method: "POST", path: "/v1/subsets/query").count
      let feedCount = await transport.requestsFor(method: "POST", path: "/v1/subset-feeds").count
      return queryCount == 1 && feedCount == 1
    }

    let query = try #require(
      await transport.requestsFor(method: "POST", path: "/v1/subsets/query").first)
    let feed = try #require(
      await transport.requestsFor(method: "POST", path: "/v1/subset-feeds").first)
    let expected = Predicate.leaf(column: "username", op: .eq, value: .string("ada"))
    let decodedQuery = try JSONDecoder().decode(SubsetQuery.self, from: query.httpBody ?? Data())
    let decodedFeed = try JSONDecoder().decode(ShapeRequest.self, from: feed.httpBody ?? Data())
    #expect(decodedQuery.where == expected)
    #expect(decodedFeed.where == expected)
    await session.stop()
  }
}
