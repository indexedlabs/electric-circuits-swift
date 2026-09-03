import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private struct NativeIssue: Equatable, Sendable {
  let id: Int64
  let title: String

  init(id: Int64, title: String) {
    self.id = id
    self.title = title
  }

  init(row: ChangeRow) throws {
    guard case .int(let id) = row["id"], case .string(let title) = row["title"] else {
      throw DecodeFailure()
    }
    self.id = id
    self.title = title
  }

  struct DecodeFailure: Error {}
}

private actor NativeSubsetTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var streamReads = 0
  private var streamBodies: [String]
  private var headFailures: Int
  private var queryFailures: Int
  private var deleteFailures: Int
  private var deleteTransportFailures: Int

  init(
    streamBodies: [String] = [
      #"[{"type":"public.issues","key":"1","value":{"id":1,"title":"Live"},"headers":{"operation":"upsert","lsn":"0/20"}}]"#
    ],
    headFailures: Int = 0,
    queryFailures: Int = 0,
    deleteFailures: Int = 0,
    deleteTransportFailures: Int = 0
  ) {
    self.streamBodies = streamBodies
    self.headFailures = headFailures
    self.queryFailures = queryFailures
    self.deleteFailures = deleteFailures
    self.deleteTransportFailures = deleteTransportFailures
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subset-feeds"):
      return response(
        #"{"shapeId":"shape-1","table":"public.issues","streamPath":"/streams/shape-1","streamUrl":"https://streams.test/streams/shape-1"}"#,
        request: request)
    case ("HEAD", "/streams/shape-1"):
      if headFailures > 0 {
        headFailures -= 1
        return response("head failed", request: request, status: 503)
      }
      return response("", request: request, headers: ["stream-next-offset": "10"])
    case ("POST", "/v1/subsets/query"):
      if queryFailures > 0 {
        queryFailures -= 1
        return response("query failed", request: request, status: 503)
      }
      return response(
        #"{"rows":[{"id":1,"title":"Snapshot"}],"lsn":"0/10"}"#,
        request: request)
    case ("GET", "/streams/shape-1"):
      streamReads += 1
      if !streamBodies.isEmpty {
        let body = streamBodies.removeFirst()
        return response(
          body, request: request, headers: ["stream-next-offset": "\(10 + streamReads)"])
      }
      try await Task.sleep(for: .seconds(3_600))
      throw CancellationError()
    case ("DELETE", "/v1/shapes/shape-1"):
      if deleteTransportFailures > 0 {
        deleteTransportFailures -= 1
        throw URLError(.networkConnectionLost)
      }
      if deleteFailures > 0 {
        deleteFailures -= 1
        return response("delete failed", request: request, status: 503)
      }
      return response("", request: request, status: 204)
    default:
      return response("unexpected", request: request, status: 500)
    }
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
        url: request.url!,
        statusCode: status,
        httpVersion: nil,
        headerFields: headers)!)
  }
}

/// Answers the first `goneCreates` subset-feed creates with `410` and behaves like
/// `NativeSubsetTransport` afterwards.
private actor GoneSubsetFeedTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var creates = 0
  private let goneCreates: Int

  init(goneCreates: Int) { self.goneCreates = goneCreates }

  var subsetFeedCreateCount: Int {
    requests.filter { $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds" }.count
  }
  var subsetFeedCreateBodies: [Data] {
    requests.filter { $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds" }
      .compactMap(\.httpBody)
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subset-feeds"):
      creates += 1
      if creates <= goneCreates { return goneResponse("gone", request: request, status: 410) }
      return goneResponse(
        #"{"shapeId":"shape-1","table":"public.issues","streamPath":"/streams/shape-1","streamUrl":"https://streams.test/streams/shape-1"}"#,
        request: request)
    case ("HEAD", "/streams/shape-1"):
      return goneResponse("", request: request, headers: ["stream-next-offset": "10"])
    case ("POST", "/v1/subsets/query"):
      return goneResponse(
        #"{"rows":[{"id":1,"title":"Snapshot"}],"lsn":"0/10"}"#, request: request)
    case ("GET", "/streams/shape-1"):
      try await Task.sleep(for: .seconds(3_600))
      throw CancellationError()
    case ("DELETE", "/v1/shapes/shape-1"):
      return goneResponse("", request: request, status: 204)
    default:
      return goneResponse("unexpected", request: request, status: 500)
    }
  }
}

private func goneResponse(
  _ body: String, request: URLRequest, status: Int = 200, headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
}

private actor SubsetRecreateClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  func sleep(for duration: Duration) async throws { delays.append(duration) }
}

private actor AppliedNativeBatches {
  private var batches: [CollectionChangeBatch<NativeIssue, Int64>] = []

  func append(_ batch: CollectionChangeBatch<NativeIssue, Int64>) {
    batches.append(batch)
  }

  func values() -> [CollectionChangeBatch<NativeIssue, Int64>] { batches }
}

@Suite("Native Circuits subset source")
struct CircuitsSubsetSourceTests {
  @Test func snapshotFenceThenAwaitedLiveTailUsesStableSubsetFeedClaim() async throws {
    let transport = NativeSubsetTransport()
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client,
      transport: transport,
      table: "public.issues",
      columns: ["id", "title"],
      retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
      decodeRow: NativeIssue.init(row:),
      decodeKey: { key in
        guard let id = Int64(key) else { throw CircuitsSubsetSourceError.invalidLiveKey(key) }
        return id
      }
    )
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(
      principal: "user-1", authorization: "workspace-1", generation: "generation-1")
    let demand = CollectionDemand<NativeIssue>(
      unsafePredicateIdentity: "status=open",
      sourcePredicate: .leaf(column: "status", op: .eq, value: .string("open")),
      order: [
        CollectionOrder(
          unsafeFieldID: "modified", sourceName: "modified_at", direction: .descending)
      ]
    )
    let identity = demand.identity(for: definition, scope: scope)
    let materializationID = CollectionMaterializationID(rawValue: "issues-open-recent")

    let session = try await source.materialize(
      demand, identity: identity, materializationID: materializationID)

    #expect(session.snapshot.rows == [NativeIssue(id: 1, title: "Snapshot")])
    #expect(session.snapshot.cursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(session.snapshot.fence.rawValue.contains("0/10"))

    let applied = AppliedNativeBatches()
    let run = Task {
      try await session.run { batch in await applied.append(batch) }
    }
    for _ in 0..<10_000 {
      if await applied.values().count == 1 { break }
      await Task.yield()
    }
    let batch = try #require(await applied.values().first)
    #expect(batch.expectedCursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(batch.cursor == StreamCursor(offset: "11", lsn: "0/20"))
    guard case .upsert(let issue, let changeVersion) = try #require(batch.changes.first) else {
      Issue.record("expected an upsert")
      try await session.stop()
      return
    }
    #expect(issue == NativeIssue(id: 1, title: "Live"))
    #expect(changeVersion == .init(rawValue: "0/20", order: 32))

    try await session.stop()
    try await run.value

    let requests = await transport.requests
    let subsetCreates = requests.filter {
      $0.httpMethod == "POST" && $0.url?.path == "/v1/subset-feeds"
    }
    #expect(subsetCreates.count == 2)
    #expect(requests.contains { $0.httpMethod == "HEAD" && $0.url?.path == "/streams/shape-1" })
    #expect(requests.contains { $0.httpMethod == "POST" && $0.url?.path == "/v1/subsets/query" })
    #expect(requests.contains { $0.httpMethod == "DELETE" && $0.url?.path == "/v1/shapes/shape-1" })

    let createBodies = try subsetCreates.map { request in
      try JSONDecoder().decode(ShapeRequest.self, from: #require(request.httpBody))
    }
    #expect(createBodies.allSatisfy { $0.subscription == materializationID.rawValue })
    #expect(createBodies.allSatisfy { $0.changesOnly == true })
  }


  @Test func goneOnTheInitialSubsetFeedRecreatesBeforeSnapshotSetup() async throws {
    let transport = GoneSubsetFeedTransport(goneCreates: 2)
    let clock = SubsetRecreateClock()
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client,
      transport: transport,
      table: "public.issues",
      columns: ["id", "title"],
      retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
      clock: clock,
      decodeRow: NativeIssue.init(row:),
      decodeKey: { key in
        guard let id = Int64(key) else { throw CircuitsSubsetSourceError.invalidLiveKey(key) }
        return id
      }
    )
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(
      unsafePredicateIdentity: "status=open",
      sourcePredicate: .leaf(column: "status", op: .eq, value: .string("open")))
    let materializationID = CollectionMaterializationID(rawValue: "issues-open")

    // A persisted materialization ID joining a dormant shape whose fall-through the engine has
    // exhausted must not fail setup before HEAD/snapshot; it must re-POST the identical claim.
    let session = try await source.materialize(
      demand,
      identity: demand.identity(for: definition, scope: scope),
      materializationID: materializationID)

    #expect(session.snapshot.rows == [NativeIssue(id: 1, title: "Snapshot")])
    #expect(session.snapshot.cursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(await transport.subsetFeedCreateCount == 3)
    #expect(await clock.delays == [.milliseconds(250), .milliseconds(250)])
    let bodies = await transport.subsetFeedCreateBodies
    #expect(bodies.count == 3)
    #expect(Set(bodies).count == 1)
    try await session.stop()
  }


  @Test func aZeroBoundSourceSurfacesTheFirstGoneFromItsInitialFeed() async throws {
    let transport = GoneSubsetFeedTransport(goneCreates: 1)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client,
      transport: transport,
      table: "public.issues",
      retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
      clock: SubsetRecreateClock(),
      recreatePolicy: ShapeSubscriptionRecreatePolicy(maximumRecreates: 0),
      decodeRow: NativeIssue.init(row:),
      decodeKey: { key in
        guard let id = Int64(key) else { throw CircuitsSubsetSourceError.invalidLiveKey(key) }
        return id
      }
    )
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")

    // A collection deployment that would rather run its own scope handoff must be able to say so.
    await #expect(throws: ClientError.http(status: 410, message: "HTTP request failed")) {
      _ = try await source.materialize(
        demand,
        identity: demand.identity(for: definition, scope: scope),
        materializationID: CollectionMaterializationID(rawValue: "issues-all"))
    }
    #expect(await transport.subsetFeedCreateCount == 1)
  }

  @Test func limitedDemandFailsBeforeCreatingAnUnmaintainedLiveFeed() async throws {
    let transport = NativeSubsetTransport()
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client,
      transport: transport,
      table: "public.issues",
      decodeRow: NativeIssue.init(row:),
      decodeKey: { key in
        guard let id = Int64(key) else { throw CircuitsSubsetSourceError.invalidLiveKey(key) }
        return id
      }
    )
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(
      principal: "user-1", authorization: "workspace-1", generation: "generation-1")
    let demand = CollectionDemand<NativeIssue>(
      unsafePredicateIdentity: "recent",
      order: [
        CollectionOrder(
          unsafeFieldID: "modified", sourceName: "modified_at", direction: .descending)
      ],
      limit: 10
    )

    await #expect(throws: CircuitsSubsetSourceError.unsupportedLimitedLiveDemand(10)) {
      _ = try await source.materialize(
        demand,
        identity: demand.identity(for: definition, scope: scope),
        materializationID: CollectionMaterializationID(rawValue: "issues-recent")
      )
    }
    #expect(await transport.requests.isEmpty)
  }

  @Test func overlapBeforeSnapshotAdvancesOffsetWithoutMutatingAndStallsSafely() async throws {
    let transport = NativeSubsetTransport(streamBodies: [
      #"[{"type":"public.issues","key":"1","headers":{"operation":"delete","lsn":"0/F"}},{"type":"public.issues","key":"1","value":{"id":1,"title":"Old"},"headers":{"operation":"update","lsn":"0/F"}}]"#
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      retryPolicy: .init(maxRetries: 0), decodeRow: NativeIssue.init(row:),
      decodeKey: {
        guard let key = Int64($0) else { throw NativeIssue.DecodeFailure() }
        return key
      })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let session = try await source.materialize(
      demand, identity: demand.identity(for: definition, scope: scope),
      materializationID: CollectionMaterializationID(rawValue: "overlap"))
    let applied = AppliedNativeBatches()
    let task = Task { try await session.run { await applied.append($0) } }
    for _ in 0..<10_000 where await applied.values().isEmpty { await Task.yield() }
    let batch = try #require(await applied.values().first)
    #expect(batch.changes.isEmpty)
    #expect(batch.cursor.offset == "11")
    #expect(batch.cursor.lsn == "0/10")
    try await session.stop()
    try await task.value
  }

  @Test func equalSnapshotBoundaryUpdateAndDeleteBothApply() async throws {
    let transport = NativeSubsetTransport(streamBodies: [
      #"[{"type":"public.issues","key":"1","value":{"id":1,"title":"Boundary update"},"headers":{"operation":"update","lsn":"0/10"}},{"type":"public.issues","key":"2","headers":{"operation":"delete","lsn":"0/10"}}]"#
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      retryPolicy: .init(maxRetries: 0),
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let session = try await source.materialize(
      demand, identity: demand.identity(for: definition, scope: scope),
      materializationID: .init(rawValue: "boundary"))
    let applied = AppliedNativeBatches()
    let task = Task { try await session.run { await applied.append($0) } }
    for _ in 0..<10_000 where await applied.values().isEmpty { await Task.yield() }
    let batch = try #require(await applied.values().first)
    #expect(batch.changes.count == 2)
    #expect(batch.sourceVersion == .init(rawValue: "0/10", order: 16))
    #expect(
      batch.changes.allSatisfy { change in
        switch change {
        case .upsert(_, let version), .delete(_, let version):
          version == .init(rawValue: "0/10", order: 16)
        }
      })
    try await session.stop()
    try await task.value
  }

  @Test func allEnvelopesAtOnePostSnapshotLSNApplyTogether() async throws {
    let transport = NativeSubsetTransport(streamBodies: [
      #"[{"type":"public.issues","key":"1","value":{"id":1,"title":"First"},"headers":{"operation":"update","lsn":"0/20"}},{"type":"public.issues","key":"2","value":{"id":2,"title":"Second"},"headers":{"operation":"upsert","lsn":"0/20"}},{"type":"public.issues","key":"3","headers":{"operation":"delete","lsn":"0/20"}}]"#
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      retryPolicy: .init(maxRetries: 0),
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let session = try await source.materialize(
      demand, identity: demand.identity(for: definition, scope: scope),
      materializationID: .init(rawValue: "same-lsn"))
    let applied = AppliedNativeBatches()
    let task = Task { try await session.run { await applied.append($0) } }
    for _ in 0..<10_000 where await applied.values().isEmpty { await Task.yield() }
    let batch = try #require(await applied.values().first)
    #expect(batch.changes.count == 3)
    #expect(batch.sourceVersion == .init(rawValue: "0/20", order: 32))
    #expect(
      batch.changes.allSatisfy { change in
        switch change {
        case .upsert(_, let version), .delete(_, let version):
          version == .init(rawValue: "0/20", order: 32)
        }
      })
    try await session.stop()
    try await task.value
  }

  @Test func failedHeadSetupRetainsTheFeedReleaseHandleUntilTheNextAttemptCleansIt() async throws {
    let transport = NativeSubsetTransport(headFailures: 1, deleteFailures: 1)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let identity = demand.identity(for: definition, scope: scope)
    let materializationID = CollectionMaterializationID(rawValue: "head-cleanup")

    await #expect(throws: CircuitsSubsetSourceError.self) {
      _ = try await source.materialize(
        demand, identity: identity, materializationID: materializationID)
    }
    #expect(await transport.requests.filter { $0.httpMethod == "DELETE" }.count == 1)

    let session = try await source.materialize(
      demand, identity: identity, materializationID: materializationID)
    #expect(await transport.requests.filter { $0.httpMethod == "DELETE" }.count == 2)
    try await session.stop()
  }

  @Test func failedQuerySetupRetainsTheFeedReleaseHandleUntilTheNextAttemptCleansIt() async throws {
    let transport = NativeSubsetTransport(queryFailures: 1, deleteFailures: 1)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let identity = demand.identity(for: definition, scope: scope)
    let materializationID = CollectionMaterializationID(rawValue: "query-cleanup")

    await #expect(throws: CircuitsSubsetSourceError.self) {
      _ = try await source.materialize(
        demand, identity: identity, materializationID: materializationID)
    }
    #expect(await transport.requests.filter { $0.httpMethod == "DELETE" }.count == 1)

    let session = try await source.materialize(
      demand, identity: identity, materializationID: materializationID)
    #expect(await transport.requests.filter { $0.httpMethod == "DELETE" }.count == 2)
    try await session.stop()
  }

  @Test func arbitraryDeleteTransportFailureRetainsSetupCleanupAuthority() async throws {
    let transport = NativeSubsetTransport(headFailures: 1, deleteTransportFailures: 1)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let identity = demand.identity(for: definition, scope: scope)
    let materializationID = CollectionMaterializationID(rawValue: "url-error-cleanup")

    await #expect(throws: URLError.self) {
      _ = try await source.materialize(
        demand, identity: identity, materializationID: materializationID)
    }
    let firstRequests = await transport.requests
    #expect(firstRequests.compactMap(\.httpMethod) == ["POST", "HEAD", "DELETE"])

    let session = try await source.materialize(
      demand, identity: identity, materializationID: materializationID)
    let requests = await transport.requests
    let deleteIndices = requests.indices.filter { requests[$0].httpMethod == "DELETE" }
    let feedPostIndices = requests.indices.filter {
      requests[$0].httpMethod == "POST" && requests[$0].url?.path == "/v1/subset-feeds"
    }
    #expect(deleteIndices.count == 2)
    #expect(feedPostIndices.count == 2)
    let retryDelete = try #require(deleteIndices.last)
    let replacementPost = try #require(feedPostIndices.last)
    #expect(retryDelete < replacementPost)
    try await session.stop()
  }

  @Test func malformedNonNilLiveLSNFailsClosedWithoutRegressingTheStoredSnapshot() async throws {
    let transport = NativeSubsetTransport(streamBodies: [
      #"[{"type":"public.issues","key":"1","value":{"id":1,"title":"Bad"},"headers":{"operation":"upsert","lsn":"not-a-postgres-lsn"}}]"#
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let source = CircuitsSubsetSource<NativeIssue, Int64>(
      client: client, transport: transport, table: "public.issues",
      retryPolicy: .init(maxRetries: 0),
      decodeRow: NativeIssue.init(row:), decodeKey: { Int64($0) ?? 0 })
    let definition = CollectionDefinition<NativeIssue, Int64>(
      id: CollectionID(rawValue: "issues"), key: \.id)
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let demand = CollectionDemand<NativeIssue>(unsafePredicateIdentity: "all")
    let identity = demand.identity(for: definition, scope: scope)
    let store = InMemoryCollectionStore<NativeIssue, Int64>(key: \.id)
    let coordinator = CollectionCoordinator(
      definition: definition, scope: scope, source: source, store: store)
    let lease = await coordinator.acquire(demand)
    for _ in 0..<10_000 {
      if await lease.state() == .failed(.sourceUnavailable) { break }
      await Task.yield()
    }
    #expect(await lease.state() == .failed(.sourceUnavailable))
    #expect(await store.rows() == [1: NativeIssue(id: 1, title: "Snapshot")])
    let record = try #require(await store.materialization(for: identity))
    #expect(record.cursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(record.sourceVersion == CollectionSourceVersion(rawValue: "0/10", order: 16))
    #expect(await transport.requests.contains { $0.httpMethod == "DELETE" })
    try await lease.release()
  }
}
