import Combine
import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import LinearLiteGRDB
import Testing

private actor SessionTransport: HTTPTransport {
  enum Mode { case terminal, blocking, blockingFailRelease, failShape, holdShape, holdRelease }

  private let mode: Mode
  private var streamResponses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []
  private var shapeRequestSeen = false
  private var shapeRequestWaiters: [CheckedContinuation<Void, Never>] = []
  private var shapeCreationReleased = false
  private var deleteRequestWaiters: [Int: [CheckedContinuation<Void, Never>]] = [:]
  private var heldDeleteResponses: [CheckedContinuation<Void, Never>] = []

  init(mode: Mode, streamResponses: [HTTPResponse] = []) {
    self.mode = mode
    self.streamResponses = streamResponses
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    if request.httpMethod == "POST", path.hasSuffix("/v1/shapes") {
      if mode == .failShape { throw URLError(.cannotConnectToHost) }
      if mode == .holdShape {
        shapeRequestSeen = true
        if !shapeCreationReleased {
          await withCheckedContinuation { continuation in
            shapeRequestWaiters.append(continuation)
          }
        }
      }
      return response(
        """
        {"shapeId":"shape-ios","table":"public.issues","streamPath":"/v1/streams/shape-ios","streamUrl":"https://engine.test/v1/streams/shape-ios","subscription":"session-1"}
        """
      )
    }
    if request.httpMethod == "DELETE" {
      if mode == .blockingFailRelease { throw URLError(.notConnectedToInternet) }
      if mode == .holdRelease {
        let deleteCount = deleteRequestCount()
        let waiters = deleteRequestWaiters.removeValue(forKey: deleteCount) ?? []
        for waiter in waiters { waiter.resume() }
        await withCheckedContinuation { continuation in
          heldDeleteResponses.append(continuation)
        }
      }
      return response("{}")
    }
    if mode == .blocking || mode == .blockingFailRelease {
      while true {
        try Task.checkCancellation()
        await Task.yield()
      }
    }
    guard !streamResponses.isEmpty else { throw CancellationError() }
    return streamResponses.removeFirst()
  }

  func requestCount(containing suffix: String) -> Int {
    requests.filter { $0.url?.path.hasSuffix(suffix) == true }.count
  }

  func hasDeleteRequest() -> Bool {
    requests.contains { $0.httpMethod == "DELETE" }
  }

  func deleteRequestCount() -> Int {
    requests.filter { $0.httpMethod == "DELETE" }.count
  }

  func waitUntilDeleteRequest(count: Int) async {
    guard deleteRequestCount() < count else { return }
    await withCheckedContinuation { continuation in
      deleteRequestWaiters[count, default: []].append(continuation)
    }
  }

  func releaseNextDeleteResponse() {
    precondition(!heldDeleteResponses.isEmpty, "expected a held delete response")
    heldDeleteResponses.removeFirst().resume()
  }

  func waitUntilShapeRequest() async {
    if shapeRequestSeen { return }
    await withCheckedContinuation { continuation in
      shapeRequestWaiters.append(continuation)
    }
  }

  func releaseShapeCreation() {
    shapeCreationReleased = true
    let waiters = shapeRequestWaiters
    shapeRequestWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
  }

  private func response(_ body: String, status: Int = 200, headers: [String: String] = [:])
    -> HTTPResponse
  {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: URL(string: "https://engine.test/v1/streams/shape-ios")!,
        statusCode: status,
        httpVersion: nil,
        headerFields: headers)!)
  }
}

private actor UnavailableMaterializer: MaterializerAvailabilityProbe {
  func checkAvailability() async throws {
    throw MaterializerAvailabilityError.protectedDataUnavailable
  }
}

@MainActor
private final class ConnectionStateLatch {
  private let predicate: (LinearLiteConnectionState) -> Bool
  private var matched = false
  private var continuation: CheckedContinuation<Void, Never>?
  private var observation: AnyCancellable?

  init(
    session: LinearLiteSession,
    matching predicate: @escaping (LinearLiteConnectionState) -> Bool
  ) {
    self.predicate = predicate
    observation = session.$connectionState.sink { [weak self] state in
      self?.receive(state)
    }
  }

  func wait() async {
    guard !matched else { return }
    await withCheckedContinuation { continuation in
      precondition(self.continuation == nil, "ConnectionStateLatch only supports one waiter")
      self.continuation = continuation
    }
  }

  private func receive(_ state: LinearLiteConnectionState) {
    guard !matched, predicate(state) else { return }
    matched = true
    continuation?.resume()
    continuation = nil
    observation?.cancel()
    observation = nil
  }
}

private func issueEnvelope(
  id: Int64,
  title: String,
  status: String,
  order: Double,
  modified: Int64 = 2,
  lsn: String = "0/10"
) -> ChangeEnvelope {
  ChangeEnvelope(
    type: "public.issues",
    key: String(id),
    value: [
      "id": .int(id), "title": .string(title), "description": .string("details"),
      "status": .string(status), "priority": .string("high"), "username": .string("ada"),
      "project_id": .int(7), "created": .int(1), "modified": .int(modified),
      "kanbanorder": .number(order),
    ],
    headers: EnvelopeHeaders(operation: .upsert, lsn: lsn))
}

private func streamResponse(_ body: String, status: Int = 200, headers: [String: String] = [:])
  -> HTTPResponse
{
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test/v1/streams/shape-ios")!,
      statusCode: status,
      httpVersion: nil,
      headerFields: headers)!)
}

private actor RecentSubsetTransport: HTTPTransport {
  private var queryPayloads: [Data]
  let streamBatch: Data?
  private var streamBatchServed = false
  private(set) var requests: [URLRequest] = []
  private(set) var diagnosticWrites: [URLRequest] = []

  init(rows: [JSONValue], lsn: String = "0/200") throws {
    queryPayloads = [try JSONEncoder().encode(SubsetResponse(rows: rows, lsn: lsn))]
    streamBatch = nil
  }

  init(payloads: [[JSONValue]], streamBatch: ChangeBatch) throws {
    queryPayloads = try payloads.map {
      try JSONEncoder().encode(SubsetResponse(rows: $0, lsn: "0/200"))
    }
    self.streamBatch = try JSONEncoder().encode(streamBatch)
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    if request.httpMethod == "POST",
      request.url?.path.hasSuffix("/table/public.issues/rows") == true
    {
      diagnosticWrites.append(request)
      return HTTPResponse(
        data: Data(#"{"ok":true,"inserted":1}"#.utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
    }
    if request.httpMethod == "POST", request.url?.path.hasSuffix("/v1/subset-feeds") == true {
      let shapeRequest = try JSONDecoder().decode(
        ShapeRequest.self, from: request.httpBody ?? Data())
      let subscription = shapeRequest.subscription ?? ""
      return HTTPResponse(
        data: Data(
          """
          {"shapeId":"feed-ios","table":"public.issues","streamPath":"/v1/streams/feed-ios","streamUrl":"https://engine.test/v1/streams/feed-ios","subscription":"\(subscription)"}
          """.utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
    }
    if request.httpMethod == "HEAD" {
      return HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil,
          headerFields: ["stream-next-offset": "5"])!)
    }
    if request.httpMethod == "GET" {
      if let streamBatch, !streamBatchServed {
        streamBatchServed = true
        return HTTPResponse(
          data: streamBatch,
          response: HTTPURLResponse(
            url: request.url!, statusCode: 200, httpVersion: nil,
            headerFields: ["stream-next-offset": "6"])!)
      }
      return HTTPResponse(
        data: Data("gone".utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 410, httpVersion: nil, headerFields: nil)!)
    }
    if request.httpMethod == "DELETE" {
      return HTTPResponse(
        data: Data("{}".utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
    }
    return HTTPResponse(
      data: queryPayloads.removeFirst(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil, headerFields: nil)!)
  }
}

private func subsetIssueRow(id: Int64, modified: Int64) -> JSONValue {
  .object([
    "id": .int(id), "title": .string("Issue \(id)"), "description": .string("Details"),
    "status": .string("backlog"), "priority": .string("high"), "username": .string("ada"),
    "project_id": .int(7), "created": .int(1), "modified": .int(modified),
    "kanbanorder": .number(Double(id)),
  ])
}

@Suite("LinearLite app session")
struct LinearLiteSessionTests {
  @Test @MainActor
  func hostLifecycleStartReportsTheActualDurableProviderCursor() async throws {
    let database = try DatabaseQueue()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "session-1", generation: "g1")
    let provider = try LinearLiteShapeMaterializer(database: database, scope: scope)
    let durableCursor = StreamCursor(offset: "7", lsn: "0/7")
    try await provider.apply(
      ChangeBatch([issueEnvelope(id: 1, title: "persisted", status: "backlog", order: 1)]),
      expecting: nil, advancingTo: durableCursor)
    let transport = SessionTransport(mode: .blocking)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "session-1", transport: transport,
      materializationScope: scope)

    let receipt = await session.startForHostLifecycle()
    #expect(session.connectionState == .streaming)
    #expect(receipt == .started(cursor: durableCursor))
    #expect(await session.stopForHostLifecycle() == .released)
  }

  @Test @MainActor
  func hostLifecycleStartReportsFailedCreateWithoutDiagnostics() async throws {
    let transport = SessionTransport(mode: .failShape)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    #expect(await session.startForHostLifecycle() == .failed)
    #expect(await transport.requestCount(containing: "/v1/shapes") == 1)
  }

  @Test @MainActor
  func hostLifecycleStartReportsTypedUnavailableBeforeAnyShapeCreate() async throws {
    let transport = SessionTransport(mode: .blocking)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport,
      materializerAvailability: UnavailableMaterializer())

    #expect(
      await session.startForHostLifecycle() == .unavailable(.protectedDataUnavailable))
    #expect(await transport.requestCount(containing: "/v1/shapes") == 0)
  }

  @Test @MainActor
  func hostLifecycleStopReportsFailedReleaseAndRetainsTheClaimForRetry() async throws {
    let transport = SessionTransport(mode: .blockingFailRelease)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    let receipt = await session.startForHostLifecycle()
    guard case .started = receipt else {
      Issue.record("host session did not start")
      return
    }
    #expect(await session.stopForHostLifecycle() == .failed)
    #expect(await transport.deleteRequestCount() == 1)
  }

  @Test @MainActor func freshSessionStartsIdleWithEmptyLocalSnapshot() throws {
    let transport = SessionTransport(mode: .terminal)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    #expect(session.connectionState == .idle)
    #expect(session.issues.isEmpty)
    #expect(session.syncEvents.isEmpty)
  }

  @Test @MainActor func recentSubsetStartRequestsTenColumnsMaterializesOrderedRows() async throws {
    let rows = (1...10).map { subsetIssueRow(id: Int64($0), modified: Int64($0 * 10)) }
      .reversed()
    let transport = try RecentSubsetTransport(rows: Array(rows))
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "recent", transport: transport,
      mode: .recentSubset(limit: 10))

    await session.start()

    try await waitUntil { session.connectionState == .terminal(.gone) }
    #expect(session.issues.count == 10)
    #expect(session.issues.map(\.id) == Array((1...10).reversed()).map(Int64.init))
    #expect(
      session.syncEvents.map(\.kind).prefix(5) == [
        .syncRequested, .feedCreated, .snapshotLoaded, .streamStarted,
      ])
    let request = try #require(
      await transport.requests.first(where: { $0.url?.path == "/v1/subsets/query" }))
    #expect(request.httpMethod == "POST")
    #expect(request.url?.path == "/v1/subsets/query")
    let body = try #require(request.httpBody)
    let encoded = try JSONDecoder().decode(JSONValue.self, from: body)
    #expect(
      encoded
        == .object([
          "table": .string("public.issues"),
          "columns": .array(LinearLiteSession.issueColumns.map(JSONValue.string)),
          "orderBy": .object(["col": .string("modified"), "desc": .bool(true)]),
          "limit": .int(10),
        ]))
  }

  @Test @MainActor func liveSubsetBatchRefillsTopTenByRequeryingBeforeCursorAdvance() async throws {
    let first = (1...10).map { subsetIssueRow(id: Int64($0), modified: Int64($0)) }
    let second = (2...11).map { subsetIssueRow(id: Int64($0), modified: Int64($0)) }.reversed()
    let batch = ChangeBatch([
      issueEnvelope(
        id: 11, title: "Issue 11", status: "backlog", order: 11, modified: 11, lsn: "0/300")
    ])
    let transport = try RecentSubsetTransport(
      payloads: [first, Array(second)], streamBatch: batch)
    let database = try DatabaseQueue()
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "recent-live", transport: transport,
      mode: .recentSubset(limit: 10))

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }

    #expect(session.issues.map(\.id) == Array((2...11).reversed()).map(Int64.init))
    let requests = await transport.requests
    #expect(requests.contains { $0.httpMethod == "HEAD" })
    #expect(requests.filter { $0.url?.path == "/v1/subsets/query" }.count == 2)
    #expect(requests.contains { $0.httpMethod == "GET" })
    let cursor = try database.read { db in
      try Row.fetchOne(
        db,
        sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?",
        arguments: ["recent-subset-recent-live"])
    }
    #expect(cursor?["offset"] as String? == "6")
    #expect(cursor?["lsn"] as String? == "0/200")
  }

  @Test @MainActor func liveSubsetInWindowUpdateUsesTheExistingSnapshot() async throws {
    let first = (1...10).map { subsetIssueRow(id: Int64($0), modified: Int64($0)) }
    let batch = ChangeBatch([
      issueEnvelope(
        id: 5, title: "Updated in place", status: "backlog", order: 5, modified: 5, lsn: "0/300")
    ])
    let transport = try RecentSubsetTransport(payloads: [first], streamBatch: batch)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "recent-direct", transport: transport,
      mode: .recentSubset(limit: 10))

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }

    #expect(session.issues.first(where: { $0.id == 5 })?.title == "Updated in place")
    let requests = await transport.requests
    #expect(requests.filter { $0.url?.path == "/v1/subsets/query" }.count == 1)
  }

  @Test @MainActor func sessionPublishesAnOptimisticOverlayOverTheSnapshot() async throws {
    let row = subsetIssueRow(id: 5, modified: 5)
    let database = try DatabaseQueue()
    let provider = try LinearLiteShapeMaterializer(
      database: database, shapeID: "recent-subset-overlay-session")
    let changeRow: ChangeRow
    guard case .object(let object) = row else {
      Issue.record("fixture row was not an object")
      return
    }
    changeRow = object
    try await provider.apply(
      ChangeBatch([
        ChangeEnvelope(
          type: "public.issues", key: "5", value: changeRow,
          headers: EnvelopeHeaders(operation: .upsert, lsn: "0/100"))
      ]),
      expecting: nil,
      advancingTo: StreamCursor(offset: "1", lsn: "0/100"))
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "optimistic-session", rowKey: "5", operation: .update,
        patch: ["title": .string("Optimistic from session")]))

    let transport = try RecentSubsetTransport(rows: [row])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "overlay-session", transport: transport,
      mode: .recentSubset(limit: 10))

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }

    #expect(session.issues.first?.title == "Optimistic from session")
  }

  @Test @MainActor
  func diagnosticButtonInsertsTwoTimestampedTasksThroughNativeRowEndpoint() async throws {
    let rows = (1...10).map { subsetIssueRow(id: Int64($0), modified: Int64($0)) }
    let transport = try RecentSubsetTransport(rows: rows)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "diagnostic", transport: transport,
      mode: .recentSubset(limit: 10))

    await session.start()
    await session.createTwoTimestampedTasks()

    let writes = await transport.diagnosticWrites
    #expect(writes.count == 2)
    for (index, request) in writes.enumerated() {
      let body = try #require(request.httpBody)
      let value = try JSONDecoder().decode(JSONValue.self, from: body)
      guard case .object(let envelope) = value,
        case .object(let columns) = envelope["columns"],
        case .string(let title) = columns["title"]
      else {
        Issue.record("diagnostic insert body was not a columns object with a title")
        continue
      }
      #expect(title.hasPrefix("Swift feed task "))
      #expect(title.hasSuffix("#\(index + 1)"))
      guard case .string(let clientID) = columns["client_id"],
        let parsed = ClientID(rawValue: clientID)
      else {
        Issue.record("diagnostic insert did not carry a UUIDv4 client_id")
        continue
      }
      #expect(parsed.rawValue == clientID)
    }
    #expect(session.syncEvents.map(\.kind).contains(.diagnosticTasksRequested))
    #expect(session.syncEvents.map(\.kind).contains(.diagnosticTasksCreated))
  }

  @Test @MainActor func shapeAndCompleteStreamPopulatePublishedGRDBSnapshot() async throws {
    let batch = ChangeBatch([
      issueEnvelope(id: 2, title: "Second", status: "done", order: 2),
      issueEnvelope(id: 1, title: "First", status: "backlog", order: 1),
      issueEnvelope(id: 3, title: "Earlier done", status: "done", order: 1),
    ])
    let body = String(data: try JSONEncoder().encode(batch), encoding: .utf8)!
    let transport = SessionTransport(
      mode: .terminal,
      streamResponses: [
        streamResponse(body, headers: ["stream-next-offset": "10"]),
        streamResponse("gone", status: 404),
      ])
    let database = try DatabaseQueue()
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "session-1", transport: transport)

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.notFound) }

    #expect(session.issues.map(\.id) == [1, 2, 3])
    #expect(session.issuesByStatus.map(\.status) == ["backlog", "done"])
    #expect(session.issuesByStatus[1].issues.map(\.id) == [3, 2])
    #expect(
      session.syncEvents.map(\.kind)
        == [.syncRequested, .shapeCreated, .snapshotLoaded, .streamStarted, .liveBatchApplied])
    #expect(session.syncEvents[2].detail == "0 issue(s)")
    #expect(session.syncEvents[4].detail == "3 issue(s)")
    #expect(session.syncEvents.allSatisfy { $0.elapsedMilliseconds >= 0 })
    let persistedCount = try await database.read { db in
      try Int.fetchOne(
        db,
        sql: """
          SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?
          """, arguments: ["shape-ios"])
    }
    #expect(persistedCount == 3)
    let requests = await transport.requests
    let shapeRequest = try #require(requests.first(where: { $0.httpMethod == "POST" }))
    let payload = try #require(shapeRequest.httpBody)
    #expect(String(data: payload, encoding: .utf8)?.contains("public.issues") == true)
    #expect(String(data: payload, encoding: .utf8)?.contains("session-1") == true)
  }

  @Test @MainActor func streamTerminalStateIsSurfaced() async throws {
    let transport = SessionTransport(
      mode: .terminal, streamResponses: [streamResponse("gone", status: 410)])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }
  }

  @Test @MainActor func streamFailureRecordsFailureTimingEvent() async throws {
    let transport = SessionTransport(
      mode: .terminal, streamResponses: [streamResponse("server error", status: 500)])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    await session.start()
    try await waitUntil {
      if case .failed = session.connectionState { return true }
      return false
    }

    #expect(session.syncEvents.last?.kind == .failed)
    #expect(session.syncEvents.last?.detail.contains("500") == true)
  }

  @Test @MainActor func stopCancelsReaderAndReleasesShape() async throws {
    let transport = SessionTransport(mode: .blocking)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    await session.start()
    try await waitUntil { await transport.requestCount(containing: "/v1/streams/shape-ios") == 1 }
    await session.stop()

    #expect(session.connectionState == .stopped)
    #expect(session.syncEvents.last?.kind == .stopped)
    #expect(await transport.hasDeleteRequest())
  }

  @Test @MainActor func syncAgainStartsANewTimingTimeline() async throws {
    let transport = SessionTransport(
      mode: .terminal,
      streamResponses: [streamResponse("gone", status: 410), streamResponse("gone", status: 410)])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }
    let firstTimeline = session.syncEvents
    await session.stop()
    await session.start()
    try await waitUntil { session.connectionState == .terminal(.gone) }

    #expect(firstTimeline.first?.kind == .syncRequested)
    #expect(session.syncEvents.first?.kind == .syncRequested)
    #expect(session.syncEvents.map(\.kind).contains(.shapeCreated))
    #expect(session.syncEvents.allSatisfy { $0.elapsedMilliseconds >= 0 })
  }

  @Test(.timeLimit(.minutes(1))) @MainActor
  func startAfterNaturalTerminalCreatesAFreshShape() async throws {
    let transport = SessionTransport(
      mode: .holdRelease,
      streamResponses: [streamResponse("gone", status: 410), streamResponse("gone", status: 410)])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)
    let firstTerminal = ConnectionStateLatch(session: session) { $0 == .terminal(.gone) }

    await session.start()
    await transport.waitUntilDeleteRequest(count: 1)
    let stateDuringFirstRelease = session.connectionState
    await transport.releaseNextDeleteResponse()
    try #require(stateDuringFirstRelease == .streaming)
    await firstTerminal.wait()

    let secondTerminal = ConnectionStateLatch(session: session) { $0 == .terminal(.gone) }
    await session.start()
    await transport.waitUntilDeleteRequest(count: 2)
    let stateDuringSecondRelease = session.connectionState
    await transport.releaseNextDeleteResponse()
    try #require(stateDuringSecondRelease == .streaming)
    await secondTerminal.wait()

    #expect(await transport.requestCount(containing: "/v1/shapes") == 2)
    #expect(await transport.deleteRequestCount() == 2)
    #expect(session.syncEvents.first?.kind == .syncRequested)
  }

  @Test @MainActor func cancellingRunStopsReaderAndReleasesShapeExactlyOnce() async throws {
    let transport = SessionTransport(mode: .blocking)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    let runTask = Task { @MainActor in await session.run() }
    try await waitUntil { await transport.requestCount(containing: "/v1/streams/shape-ios") == 1 }

    runTask.cancel()
    await runTask.value

    #expect(session.connectionState == .stopped)
    #expect(await transport.requestCount(containing: "/v1/streams/shape-ios") == 1)
    #expect(await transport.deleteRequestCount() == 1)
  }

  @Test @MainActor func runPreservesNaturalTerminalStateAfterReleasingShape() async throws {
    let transport = SessionTransport(
      mode: .terminal, streamResponses: [streamResponse("gone", status: 410)])
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    await session.run()

    #expect(session.connectionState == .terminal(.gone))
    #expect(await transport.deleteRequestCount() == 1)
  }

  @Test @MainActor func setupMigrationFailureReleasesCreatedShape() async throws {
    let database = try DatabaseQueue()
    try await database.write { db in
      try db.execute(sql: "CREATE TABLE issues (id INTEGER PRIMARY KEY NOT NULL)")
      try db.execute(sql: "INSERT INTO issues (id) VALUES (1)")
    }
    let transport = SessionTransport(mode: .terminal)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "session-1", transport: transport)

    await session.start()

    #expect(
      session.connectionState
        == .failed(
          String(describing: LinearLiteShapeMaterializerError.unscopedRowsRequireReseed(count: 1)))
    )
    #expect(await transport.deleteRequestCount() == 1)
    #expect(await transport.requestCount(containing: "/v1/streams/shape-ios") == 0)
  }

  @Test @MainActor func initialSnapshotReadFailureDoesNotStartReader() async throws {
    let database = try DatabaseQueue()
    try LinearLiteShapeMaterializer.migrate(database)
    try await database.write { db in
      try db.execute(sql: "DROP TABLE issues")
    }
    let transport = SessionTransport(mode: .terminal)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: database, subscription: "session-1", transport: transport)

    await session.start()

    guard case .failed = session.connectionState else {
      Issue.record("initial snapshot failure was not published as failed")
      return
    }
    #expect(session.syncEvents.last?.kind == .failed)
    #expect(await transport.requestCount(containing: "/v1/streams/shape-ios") == 0)
    #expect(await transport.deleteRequestCount() == 1)
  }

  @Test @MainActor func stopDuringShapeCreationDoesNotStartReaderAndReleasesOnce() async throws {
    let transport = SessionTransport(mode: .holdShape)
    let session = LinearLiteSession(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      database: try DatabaseQueue(), subscription: "session-1", transport: transport)

    let startTask = Task { @MainActor in await session.start() }
    try await waitUntil { await transport.requestCount(containing: "/v1/shapes") == 1 }
    await session.stop()
    await transport.releaseShapeCreation()
    await startTask.value

    #expect(session.connectionState == .stopped)
    #expect(await transport.requestCount(containing: "/v1/streams/shape-ios") == 0)
    #expect(await transport.deleteRequestCount() == 1)
  }

}

@MainActor
private func waitUntil(
  _ predicate: @escaping @MainActor () async -> Bool
) async throws {
  for _ in 0..<10_000 {
    if await predicate() { return }
    await Task.yield()
  }
  throw CancellationError()
}
