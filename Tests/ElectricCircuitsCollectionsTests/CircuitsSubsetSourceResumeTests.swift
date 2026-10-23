import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private struct ResumeIssue: Equatable, Sendable {
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

/// A native control plane and durable-stream store. Each subset-feed create answers the next
/// scripted shape ID (the last repeats); a stream read takes the next scripted reply for its
/// shape and otherwise parks, cancellably, until the test scripts one.
private actor ResumeFeedTransport: HTTPTransport {
  enum StreamReply: Sendable {
    case batch(String, next: String)
    /// A `204` with no next offset: the long-poll window elapsed at the tail.
    case idle
    case status(Int)
  }

  private(set) var requests: [URLRequest] = []
  private var feedShapes: [String]
  private var queryBodies: [String]
  private var streams: [String: [StreamReply]] = [:]
  private var parked: [UUID: CheckedContinuation<Void, Never>] = [:]

  init(
    feedShapes: [String] = ["shape-1"],
    queryBodies: [String] = [
      #"{"rows":[{"id":1,"title":"One"},{"id":3,"title":"Three"}],"lsn":"0/10"}"#
    ]
  ) {
    self.feedShapes = feedShapes
    self.queryBodies = queryBodies
  }

  func script(_ shape: String, _ replies: [StreamReply]) {
    streams[shape, default: []] += replies
    let waiting = parked.values
    parked.removeAll()
    for continuation in waiting { continuation.resume() }
  }

  func count(_ method: String, _ path: String) -> Int {
    requests.filter { $0.httpMethod == method && $0.url?.path == path }.count
  }

  /// The `offset` of every stream read of `shape`, in order.
  func streamOffsets(_ shape: String) -> [String] {
    requests.filter { $0.httpMethod == "GET" && $0.url?.path == "/streams/\(shape)" }.compactMap {
      URLComponents(url: $0.url!, resolvingAgainstBaseURL: false)?.queryItems?
        .first { $0.name == "offset" }?.value
    }
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch (request.httpMethod ?? "", path) {
    case ("POST", "/v1/subset-feeds"):
      let shape = feedShapes.count > 1 ? feedShapes.removeFirst() : feedShapes[0]
      return reply(
        #"{"shapeId":"\#(shape)","table":"public.issues","streamPath":"/streams/\#(shape)","streamUrl":"https://streams.test/streams/\#(shape)"}"#,
        to: request)
    case ("HEAD", _) where path.hasPrefix("/streams/"):
      return reply("", to: request, headers: ["stream-next-offset": "10"])
    case ("POST", "/v1/subsets/query"):
      let body = queryBodies.count > 1 ? queryBodies.removeFirst() : queryBodies[0]
      return reply(body, to: request)
    case ("GET", _) where path.hasPrefix("/streams/"):
      let shape = String(path.dropFirst("/streams/".count))
      while streams[shape, default: []].isEmpty { try await park() }
      switch streams[shape, default: []].removeFirst() {
      case .batch(let body, let next):
        return reply(body, to: request, headers: ["stream-next-offset": next])
      case .idle:
        return reply("", to: request, status: 204)
      case .status(let status):
        return reply("", to: request, status: status)
      }
    case ("DELETE", _) where path.hasPrefix("/v1/shapes/"):
      return reply("", to: request, status: 204)
    default:
      return reply("unexpected", to: request, status: 500)
    }
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

  private func reply(
    _ body: String, to request: URLRequest, status: Int = 200, headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

private let resumeIssues = CollectionDefinition<ResumeIssue, Int64>(
  id: CollectionID(rawValue: "issues"), key: \.id)
private let resumeScope = CollectionScope(principal: "u", authorization: "a", generation: "g")
private let openIssues = CollectionDemand<ResumeIssue>(
  unsafePredicateIdentity: "status=open",
  sourcePredicate: .leaf(column: "status", op: .eq, value: .string("open")))
private let openIdentity = openIssues.identity(for: resumeIssues, scope: resumeScope)

private func makeSource(
  _ transport: ResumeFeedTransport, keyForRow: (@Sendable (ResumeIssue) throws -> Int64)? = nil
) -> CircuitsSubsetSource<ResumeIssue, Int64> {
  CircuitsSubsetSource<ResumeIssue, Int64>(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport),
    transport: transport,
    table: "public.issues",
    columns: ["id", "title"],
    retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
    decodeRow: ResumeIssue.init(row:),
    decodeKey: { key in
      guard let id = Int64(key) else { throw CircuitsSubsetSourceError.invalidLiveKey(key) }
      return id
    },
    keyForRow: keyForRow
  )
}

private typealias ResumeCoordinator = CollectionCoordinator<
  ResumeIssue, Int64, CircuitsSubsetSource<ResumeIssue, Int64>,
  InMemoryCollectionStore<ResumeIssue, Int64>
>

/// A fresh coordinator and source over a surviving store: what an app relaunch sees.
private func relaunch(
  _ transport: ResumeFeedTransport, store: InMemoryCollectionStore<ResumeIssue, Int64>
) -> ResumeCoordinator {
  CollectionCoordinator(
    definition: resumeIssues, scope: resumeScope, source: makeSource(transport), store: store)
}

/// Polls with real sleeps rather than yields: the HTTP path crosses several actors per step.
private func eventually(_ condition: @Sendable () async throws -> Bool) async -> Bool {
  for _ in 0..<2_500 {
    if (try? await condition()) == true { return true }
    try? await Task.sleep(for: .milliseconds(2))
  }
  return (try? await condition()) == true
}

/// Materializes `openIssues` once through the snapshot path and releases it, leaving the stored
/// record a later acquire resumes from.
private func seedBySnapshot(
  _ transport: ResumeFeedTransport, store: InMemoryCollectionStore<ResumeIssue, Int64>
) async throws -> CollectionMaterializationRecord {
  let lease = await relaunch(transport, store: store).acquire(openIssues)
  #expect(await eventually { await lease.state() == .live })
  try await lease.release()
  return try #require(try await store.materialization(for: openIdentity))
}

@Suite("Native Circuits subset source resume")
struct CircuitsSubsetSourceResumeTests {
  @Test func resumingTheSameFeedTailsFromTheStoredCursorWithoutASnapshot() async throws {
    let transport = ResumeFeedTransport()
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let seeded = try await seedBySnapshot(transport, store: store)
    #expect(seeded.cursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(seeded.snapshotFence.rawValue == "9:subset-v2|7:shape-1|2:10|4:0/10|0:|0:")
    #expect(await store.rowClaims(for: seeded.id) == [1, 3])

    await transport.script(
      "shape-1",
      [
        .batch(
          #"[{"type":"public.issues","key":"2","value":{"id":2,"title":"Two"},"headers":{"operation":"insert","lsn":"0/20"}},{"type":"public.issues","key":"1","value":{"id":1,"title":"One, edited"},"headers":{"operation":"update","lsn":"0/20"}}]"#,
          next: "11"),
        .batch(
          #"[{"type":"public.issues","key":"3","headers":{"operation":"delete","lsn":"0/30"}}]"#,
          next: "12"),
      ])
    let lease = await relaunch(transport, store: store).acquire(openIssues)

    #expect(await eventually { await lease.state() == .live })
    #expect(
      await eventually {
        try await store.materialization(for: openIdentity)?.cursor?.offset == "12"
      })
    #expect(
      await store.rows() == [
        1: ResumeIssue(id: 1, title: "One, edited"), 2: ResumeIssue(id: 2, title: "Two"),
      ])
    // The feed delete released this materialization's claim; nothing else held row 3.
    #expect(await store.rowClaims(for: seeded.id) == [1, 2])
    let resumed = try #require(try await store.materialization(for: openIdentity))
    #expect(resumed.id == seeded.id)
    #expect(resumed.snapshotFence == seeded.snapshotFence)
    #expect(resumed.cursor == StreamCursor(offset: "12", lsn: "0/30"))
    #expect(resumed.sourceVersion.order == 0x30)

    // One create per materialize and one per resume; the resume reads neither the frontier nor
    // a snapshot, and its first read starts at the stored offset.
    #expect(await transport.count("POST", "/v1/subset-feeds") == 2)
    #expect(await transport.count("HEAD", "/streams/shape-1") == 1)
    #expect(await transport.count("POST", "/v1/subsets/query") == 1)
    #expect(await transport.streamOffsets("shape-1").prefix(4) == ["10", "10", "11", "12"])

    try await lease.release()
    #expect(await transport.count("DELETE", "/v1/shapes/shape-1") == 2)
  }

  @Test func resumedDemandStaysCachedUntilAnIdlePollProvesTheTailIsCurrent() async throws {
    let transport = ResumeFeedTransport()
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let seeded = try await seedBySnapshot(transport, store: store)

    let lease = await relaunch(transport, store: store).acquire(openIssues)
    #expect(await eventually { await transport.streamOffsets("shape-1").count == 2 })
    // The resumed read is parked at the tail: its rows are local, but nothing proves the
    // stored cursor is still current yet.
    #expect(await lease.state() == .cached)

    await transport.script("shape-1", [.idle])
    #expect(await eventually { await lease.state() == .live })
    #expect(try await store.materialization(for: openIdentity) == seeded)
    #expect(await transport.count("POST", "/v1/subsets/query") == 1)
    #expect(await transport.count("POST", "/v1/subset-feeds") == 2)
    try await lease.release()
  }

  @Test func aReplacedFeedSnapshotsUnderTheResumesSingleCreate() async throws {
    let transport = ResumeFeedTransport(
      feedShapes: ["shape-1", "shape-2"],
      queryBodies: [
        #"{"rows":[{"id":1,"title":"One"},{"id":3,"title":"Three"}],"lsn":"0/10"}"#,
        #"{"rows":[{"id":1,"title":"Fresh"}],"lsn":"0/40"}"#,
      ])
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let seeded = try await seedBySnapshot(transport, store: store)

    let lease = await relaunch(transport, store: store).acquire(openIssues)

    #expect(await eventually { await lease.state() == .live })
    // A replacement changes-only feed has no history for the gap, so the demand snapshots. The
    // snapshot adopts the claim the resume's create returned instead of creating it again.
    #expect(await transport.count("POST", "/v1/subset-feeds") == 2)
    #expect(await transport.count("HEAD", "/streams/shape-2") == 1)
    #expect(await transport.count("POST", "/v1/subsets/query") == 2)
    #expect(await transport.streamOffsets("shape-1") == ["10"])
    let replaced = try #require(try await store.materialization(for: openIdentity))
    #expect(replaced.id == seeded.id)
    #expect(replaced.snapshotFence.rawValue == "9:subset-v2|7:shape-2|2:10|4:0/40|0:|0:")
    #expect(await store.rows() == [1: ResumeIssue(id: 1, title: "Fresh")])
    try await lease.release()
  }

  @Test func aTerminalFirstResumedReadMakesTheNextAttemptSnapshot() async throws {
    let transport = ResumeFeedTransport(queryBodies: [
      #"{"rows":[{"id":1,"title":"One"},{"id":3,"title":"Three"}],"lsn":"0/10"}"#,
      #"{"rows":[{"id":1,"title":"Fresh"}],"lsn":"0/40"}"#,
    ])
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let seeded = try await seedBySnapshot(transport, store: store)
    await transport.script("shape-1", [.status(410)])

    let lease = await relaunch(transport, store: store).acquire(openIssues)
    #expect(await eventually { await lease.state() == .failed(.sourceUnavailable) })
    #expect(await transport.count("POST", "/v1/subsets/query") == 1)
    #expect(try await store.materialization(for: openIdentity) == seeded)

    // The failed resume is not retried as one: the coordinator's next attempt, and this
    // source's memory of the fence, both make the retry snapshot rather than re-join a stream
    // the engine will not serve from that cursor.
    await lease.refresh()
    #expect(await eventually { await lease.state() == .live })
    #expect(await transport.count("POST", "/v1/subsets/query") == 2)
    #expect(await transport.count("POST", "/v1/subset-feeds") == 3)
    #expect(await transport.count("HEAD", "/streams/shape-1") == 2)
    let refreshed = try #require(try await store.materialization(for: openIdentity))
    #expect(refreshed.snapshotFence.rawValue == "9:subset-v2|7:shape-1|2:10|4:0/40|0:|0:")
    #expect(await store.rows() == [1: ResumeIssue(id: 1, title: "Fresh")])
    try await lease.release()
  }

  @Test func aVersionOneFenceSnapshotsAndIsRewrittenAsVersionTwo() async throws {
    let transport = ResumeFeedTransport()
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let id = CollectionMaterializationID(rawValue: "issues-open")
    try await store.replaceSnapshot(
      CollectionSnapshot(
        rows: [ResumeIssue(id: 1, title: "Before")],
        fence: SnapshotFence(rawValue: "9:subset-v1|2:10|4:0/10"),
        sourceVersion: CollectionSourceVersion(rawValue: "0/10", order: 0x10),
        cursor: StreamCursor(offset: "10", lsn: "0/10")),
      materializationID: id, demand: openIdentity)

    let lease = await relaunch(transport, store: store).acquire(openIssues)

    #expect(await eventually { await lease.state() == .live })
    // A v1 fence does not name its feed, so it cannot prove the cursor's stream survived.
    #expect(await transport.count("POST", "/v1/subset-feeds") == 1)
    #expect(await transport.count("HEAD", "/streams/shape-1") == 1)
    #expect(await transport.count("POST", "/v1/subsets/query") == 1)
    let rewritten = try #require(try await store.materialization(for: openIdentity))
    #expect(rewritten.id == id)
    #expect(rewritten.snapshotFence.rawValue == "9:subset-v2|7:shape-1|2:10|4:0/10|0:|0:")
    try await lease.release()
  }

  @Test func unresumableRecordsDeclineBeforeAnyRequest() async throws {
    let transport = ResumeFeedTransport()
    let source = makeSource(transport, keyForRow: \.id)
    let id = CollectionMaterializationID(rawValue: "issues-open")
    func record(
      for identity: CollectionDemandIdentity, fence: String, cursor: StreamCursor?
    ) -> CollectionMaterializationRecord {
      CollectionMaterializationRecord(
        id: id, demand: identity, state: .live, snapshotFence: SnapshotFence(rawValue: fence),
        sourceVersion: CollectionSourceVersion(rawValue: "0/10", order: 0x10), cursor: cursor)
    }
    let v2 = "9:subset-v2|7:shape-1|2:10|4:0/10|0:|0:"
    let cursor = StreamCursor(offset: "10", lsn: "0/10")

    let versionOne = try await source.resume(
      openIssues, identity: openIdentity, materializationID: id,
      record: record(for: openIdentity, fence: "9:subset-v1|2:10|4:0/10", cursor: cursor))
    #expect(versionOne == nil)

    // A fence from before visibility was recorded, and one whose visibility does not parse,
    // cannot gate a resumed tail.
    let fourPart = try await source.resume(
      openIssues, identity: openIdentity, materializationID: id,
      record: record(for: openIdentity, fence: "9:subset-v2|7:shape-1|2:10|4:0/10", cursor: cursor))
    #expect(fourPart == nil)
    let badVisibility = try await source.resume(
      openIssues, identity: openIdentity, materializationID: id,
      record: record(
        for: openIdentity, fence: "9:subset-v2|7:shape-1|2:10|4:0/10|3:bad|4:0/30", cursor: cursor))
    #expect(badVisibility == nil)

    let cursorless = try await source.resume(
      openIssues, identity: openIdentity, materializationID: id,
      record: record(for: openIdentity, fence: v2, cursor: nil))
    #expect(cursorless == nil)

    // A limited window's live state is its page, which only a query can produce.
    let window = CollectionDemand<ResumeIssue>(
      unsafePredicateIdentity: "recent",
      order: [
        CollectionOrder(
          unsafeFieldID: "modified", sourceName: "modified_at", direction: .descending)
      ],
      limit: 10)
    let windowIdentity = window.identity(for: resumeIssues, scope: resumeScope)
    let limited = try await source.resume(
      window, identity: windowIdentity, materializationID: id,
      record: record(for: windowIdentity, fence: v2, cursor: cursor))
    #expect(limited == nil)

    #expect(await transport.requests.isEmpty)
  }

  /// A store need not keep the snapshot's transaction visibility with its source version (the
  /// app's GRDB store keeps only the LSN), so the fence carries it. A commit that was still in
  /// progress when the snapshot was taken can sit after the stored offset with an LSN below the
  /// snapshot's; the resumed tail must admit it as the snapshot's own tail would have, and still
  /// skip a commit the snapshot already holds.
  @Test func aResumeRestoresTheSnapshotVisibilityTheStoreDidNotKeep() async throws {
    let transport = ResumeFeedTransport(queryBodies: [
      #"{"rows":[{"id":1,"title":"One"}],"lsn":"0/100","snapshot":"100:110:105","horizon":"0/180"}"#
    ])
    let seeded = try await seedBySnapshot(
      transport, store: InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id))
    #expect(
      seeded.snapshotFence.rawValue
        == "9:subset-v2|7:shape-1|2:10|5:0/100|11:100:110:105|5:0/180")

    // The same record in a store that dropped `sourceVersion.snapshot`.
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    try await store.replaceSnapshot(
      CollectionSnapshot(
        rows: [ResumeIssue(id: 1, title: "One")], fence: seeded.snapshotFence,
        sourceVersion: CollectionSourceVersion(rawValue: "0/100", order: 0x100),
        cursor: try #require(seeded.cursor)),
      materializationID: seeded.id, demand: openIdentity)
    #expect(try await store.materialization(for: openIdentity)?.sourceVersion.snapshot == nil)

    await transport.script(
      "shape-1",
      [
        .batch(
          #"[{"type":"public.issues","key":"1","value":{"id":1,"title":"Already in the snapshot"},"headers":{"operation":"update","lsn":"0/F0","txid":"104"}},{"type":"public.issues","key":"2","value":{"id":2,"title":"Committed during snapshot"},"headers":{"operation":"insert","lsn":"0/FF","txid":"105"}}]"#,
          next: "11")
      ])
    let lease = await relaunch(transport, store: store).acquire(openIssues)

    #expect(await eventually { await lease.state() == .live })
    #expect(
      await eventually {
        try await store.materialization(for: openIdentity)?.cursor?.offset == "11"
      })
    #expect(
      await store.rows() == [
        1: ResumeIssue(id: 1, title: "One"),
        2: ResumeIssue(id: 2, title: "Committed during snapshot"),
      ])
    #expect(await transport.count("POST", "/v1/subsets/query") == 1)
    try await lease.release()
  }

  @Test func aLimitedCachedWindowSnapshotsItsPage() async throws {
    let transport = ResumeFeedTransport(queryBodies: [
      #"{"rows":[{"id":1,"title":"One"}],"lsn":"0/10"}"#
    ])
    let store = InMemoryCollectionStore<ResumeIssue, Int64>(key: \.id)
    let source = makeSource(transport, keyForRow: \.id)
    let window = CollectionDemand<ResumeIssue>(
      unsafePredicateIdentity: "recent",
      order: [
        CollectionOrder(
          unsafeFieldID: "modified", sourceName: "modified_at", direction: .descending)
      ],
      limit: 1)
    for _ in 0..<2 {
      let coordinator = CollectionCoordinator(
        definition: resumeIssues, scope: resumeScope, source: source, store: store)
      let lease = await coordinator.acquire(window)
      #expect(await eventually { await lease.state() == .live })
      try await lease.release()
    }
    #expect(await transport.count("POST", "/v1/subset-feeds") == 2)
    #expect(await transport.count("POST", "/v1/subsets/query") == 2)
    #expect(await transport.count("HEAD", "/streams/shape-1") == 2)
  }
}
