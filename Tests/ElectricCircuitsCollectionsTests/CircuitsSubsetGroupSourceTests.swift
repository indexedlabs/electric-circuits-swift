import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

// Three member tables with distinct models, each keyed by a string id that repeats across tables.

private struct GroupRecord: Equatable, Sendable {
  let id: String
  let title: String
  init(id: String, title: String) {
    self.id = id
    self.title = title
  }
  init(row: ChangeRow) throws {
    guard case .string(let id) = row["id"], case .string(let title) = row["title"] else {
      throw GroupDecodeFailure()
    }
    self.init(id: id, title: title)
  }
}

private struct GroupAssignment: Equatable, Sendable {
  let id: String
  let person: String
  init(id: String, person: String) {
    self.id = id
    self.person = person
  }
  init(row: ChangeRow) throws {
    guard case .string(let id) = row["id"], case .string(let person) = row["person"] else {
      throw GroupDecodeFailure()
    }
    self.init(id: id, person: person)
  }
}

private struct GroupTag: Equatable, Sendable {
  let id: String
  let name: String
  init(id: String, name: String) {
    self.id = id
    self.name = name
  }
  init(row: ChangeRow) throws {
    guard case .string(let id) = row["id"], case .string(let name) = row["name"] else {
      throw GroupDecodeFailure()
    }
    self.init(id: id, name: name)
  }
}

private struct GroupDecodeFailure: Error {}

private let records = CollectionDefinition<GroupRecord, String>(
  id: CollectionID(rawValue: "records"), key: \.id)
private let assignments = CollectionDefinition<GroupAssignment, String>(
  id: CollectionID(rawValue: "assignments"), key: \.id)
private let tags = CollectionDefinition<GroupTag, String>(
  id: CollectionID(rawValue: "tags"), key: \.id)
private let groupScope = CollectionScope(
  principal: "user-1", authorization: "space-1", generation: "generation-1")

private func identityKey(_ key: String) -> String { key }

private func threeTableDemand() -> CollectionGroupDemand {
  CollectionGroupDemand(members: [
    CollectionGroupMember(
      definition: records, table: "public.record", unsafePredicateIdentity: "id=1",
      sourcePredicate: .leaf(column: "id", op: .eq, value: .string("1")),
      decodeRow: GroupRecord.init(row:), decodeKey: identityKey),
    CollectionGroupMember(
      definition: assignments, table: "public.assignment", unsafePredicateIdentity: "record=1",
      sourcePredicate: .leaf(column: "record_id", op: .eq, value: .string("1")),
      decodeRow: GroupAssignment.init(row:), decodeKey: identityKey),
    CollectionGroupMember(
      definition: tags, table: "public.tag", unsafePredicateIdentity: "record=1",
      sourcePredicate: .leaf(column: "record_id", op: .eq, value: .string("1")),
      decodeRow: GroupTag.init(row:), decodeKey: identityKey),
  ])
}

/// The group's snapshot: key "1" is a row in all three tables.
private let groupLoadAnswer = #"""
  {"groupId":"group-1","shapeId":"group-1","subscription":"scope-1","streamPath":"/streams/group-1",
   "streamUrl":"https://engine.test/streams/group-1","offset":"10","lsn":"0/10",
   "snapshot":"100:110:","horizon":"0/15",
   "rows":[{"table":"public.record","value":{"id":"1","title":"Record"}},
           {"table":"public.assignment","value":{"id":"1","person":"Ada"}},
           {"table":"public.assignment","value":{"id":"2","person":"Grace"}},
           {"table":"public.tag","value":{"id":"1","name":"urgent"}}],
   "results":[{"key":"public.record","table":"public.record","shapeId":"m-1"},
              {"key":"public.assignment","table":"public.assignment","shapeId":"m-2"},
              {"key":"public.tag","table":"public.tag","shapeId":"m-3"}]}
  """#

/// One batch touching all three tables, with a delete of key "1" in `assignment` while key "1"
/// stays in `record` and `tag`; then a delete of key "1" in `record` and an update of key "1"
/// in `tag`. The first envelope committed inside the group snapshot (xid 105 below its xmax), so
/// the snapshot gate skips it. The `tag` update carries an LSN below the `record` delete before
/// it, as a deferred query-back can; it is applied only because rows are versioned per table
/// and key, not per key.
private let groupBatches = [
  #"[{"type":"public.assignment","key":"2","value":{"id":"2","person":"Grace (in snapshot)"},"headers":{"operation":"update","lsn":"0/F","txid":"105"}},"#
    + #"{"type":"public.record","key":"1","value":{"id":"1","title":"Record v2"},"headers":{"operation":"update","lsn":"0/20"}},"#
    + #"{"type":"public.assignment","key":"1","headers":{"operation":"delete","lsn":"0/20"}},"#
    + #"{"type":"public.assignment","key":"3","value":{"id":"3","person":"Linus"},"headers":{"operation":"insert","lsn":"0/20"}},"#
    + #"{"type":"public.tag","key":"2","value":{"id":"2","name":"later"},"headers":{"operation":"insert","lsn":"0/20"}}]"#,
  #"[{"type":"public.record","key":"1","headers":{"operation":"delete","lsn":"0/30"}},"#
    + #"{"type":"public.tag","key":"1","value":{"id":"1","name":"urgent v2"},"headers":{"operation":"update","lsn":"0/19"}}]"#,
]

/// A mock of the gateway's group-only `subsets/load` and the group's one stream, answering
/// stream reads by long-poll through `send` and by SSE through `stream`.
private actor GroupStreamServer: HTTPTransport {
  private var batches: [String]
  private var nextOffset = 10
  /// How many stream reads answer `goneStatus` before the scripted batches start.
  private var goneReads: Int
  private let goneStatus: Int
  /// The lease each load's answer names; nil names none.
  private let leaseSeconds: Int?
  /// How long each load takes to answer.
  private let loadDelay: Duration
  /// After the scripted batches, keep answering with one new `tag` row per read.
  private let endless: Bool
  private(set) var loads: [SubsetGroupRequest] = []
  private(set) var streamURLs: [URL] = []
  private(set) var releases: [URL] = []

  init(
    batches: [String], goneReads: Int = 0, goneStatus: Int = 410, endless: Bool = false,
    leaseSeconds: Int? = nil, loadDelay: Duration = .zero
  ) {
    self.leaseSeconds = leaseSeconds
    self.loadDelay = loadDelay
    self.batches = batches
    self.goneReads = goneReads
    self.goneStatus = goneStatus
    self.endless = endless
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subsets/load"):
      loads.append(try JSONDecoder().decode(SubsetGroupRequest.self, from: request.httpBody!))
      try await Task.sleep(for: loadDelay)
      let answer = leaseSeconds.map {
        groupLoadAnswer.replacingOccurrences(
          of: #""streamPath":"#, with: #""leaseSeconds":\#($0),"streamPath":"#)
      }
      return reply(answer ?? groupLoadAnswer, request: request)
    case ("GET", let path) where path.hasPrefix("/streams/"):
      streamURLs.append(request.url!)
      if goneReads > 0 {
        goneReads -= 1
        return reply("gone", request: request, status: goneStatus)
      }
      guard let body = takeBatch() else {
        try await Task.sleep(for: .seconds(3_600))
        throw CancellationError()
      }
      return reply(body, request: request, headers: ["stream-next-offset": "\(nextOffset)"])
    case ("DELETE", let path) where path.hasPrefix("/v1/shapes/"):
      releases.append(request.url!)
      return reply("", request: request, status: 204)
    default:
      return reply("unexpected", request: request, status: 500)
    }
  }

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    streamURLs.append(request.url!)
    if goneReads > 0 {
      goneReads -= 1
      try await onResponse(
        HTTPURLResponse(
          url: request.url!, statusCode: goneStatus, httpVersion: nil, headerFields: [:])!)
      return
    }
    try await onResponse(
      HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!)
    guard let body = takeBatch() else {
      try await Task.sleep(for: .seconds(3_600))
      throw CancellationError()
    }
    // One response carries one batch and ends, so the next read resumes from the committed offset.
    try await onBytes(Data("event: data\ndata: \(body)\n\n".utf8))
    try await onBytes(
      Data("event: control\ndata: {\"streamNextOffset\":\"\(nextOffset)\"}\n\n".utf8))
  }

  private func takeBatch() -> String? {
    if batches.isEmpty, endless {
      let n = nextOffset + 1
      batches.append(
        #"[{"type":"public.tag","key":"t\#(n)","value":{"id":"t\#(n)","name":"live"},"#
          + #""headers":{"operation":"insert","lsn":"1/\#(String(n, radix: 16))"}}]"#)
    }
    guard !batches.isEmpty else { return nil }
    nextOffset += 1
    return batches.removeFirst()
  }

  private func reply(
    _ body: String, request: URLRequest, status: Int = 200, headers: [String: String] = [:]
  ) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }
}

private actor GroupBatchLog {
  private(set) var batches: [CollectionGroupChangeBatch] = []
  func append(_ batch: CollectionGroupChangeBatch) { batches.append(batch) }
}

private func makeSource(
  _ server: GroupStreamServer, mode: LiveReadMode,
  clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock()
) -> CircuitsSubsetGroupSource {
  CircuitsSubsetGroupSource(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: server,
      configuration: .init(liveReadMode: mode)),
    transport: server,
    retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
    clock: clock)
}

/// Records each sleep and holds it until the test wakes it or its task is cancelled.
private actor GroupSourceLeaseClock: ShapeSubscriptionClock {
  private(set) var sleeps: [Duration] = []
  private var sleeping: [UUID: CheckedContinuation<Void, Error>] = [:]
  private var cancelled: Set<UUID> = []

  func sleep(for duration: Duration) async throws {
    sleeps.append(duration)
    let id = UUID()
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
        if cancelled.remove(id) != nil {
          continuation.resume(throwing: CancellationError())
        } else {
          sleeping[id] = continuation
        }
      }
    } onCancel: {
      Task { await self.cancel(id) }
    }
  }

  private func cancel(_ id: UUID) {
    if let sleeper = sleeping.removeValue(forKey: id) {
      sleeper.resume(throwing: CancellationError())
    } else {
      cancelled.insert(id)
    }
  }

  func wake() {
    let woken = sleeping
    sleeping.removeAll()
    for sleeper in woken.values { sleeper.resume() }
  }
}

private func waitUntil(_ condition: @Sendable () async -> Bool) async {
  for _ in 0..<5_000 {
    if await condition() { return }
    try? await Task.sleep(for: .milliseconds(2))
  }
}

private actor LiveDeliveryLog {
  private(set) var values: [LiveDelivery?] = []
  func append(_ delivery: LiveDelivery?) { values.append(delivery) }
}

@Suite("Native Circuits group source")
struct CircuitsSubsetGroupSourceTests {
  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func threeTableGroupCommitsEveryTableWithOneCursor(mode: LiveReadMode) async throws {
    let server = GroupStreamServer(batches: groupBatches)
    let source = makeSource(server, mode: mode)
    let demand = threeTableDemand()
    let identity = demand.identity(scope: groupScope)
    let id = CollectionMaterializationID(rawValue: "scope-1")

    let session = try await source.materialize(demand, materializationID: id)

    #expect(session.snapshot.cursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(session.snapshot.members.map(\.collection) == [records.id, assignments.id, tags.id])
    #expect(
      session.snapshot.rows(in: assignments.id, as: GroupAssignment.self, key: String.self)
        == [GroupAssignment(id: "1", person: "Ada"), GroupAssignment(id: "2", person: "Grace")])

    let store = InMemoryCollectionGroupStore()
    try await store.replaceGroupSnapshot(session.snapshot, materializationID: id, demand: identity)
    let log = GroupBatchLog()
    let deliveries = LiveDeliveryLog()
    let run = Task {
      try await session.run { batch in
        try await store.applyGroup(batch, to: id)
        await deliveries.append(LiveDelivery.current)
        await log.append(batch)
      }
    }
    await waitUntil { await log.batches.count == 2 }
    try await session.stop()
    try await run.value

    let batches = await log.batches
    #expect(batches.count == 2)
    let first = try #require(batches.first)
    // The first batch is one store call that touches all three tables.
    #expect(first.members.map(\.collection) == [records.id, assignments.id, tags.id])
    #expect(first.changes(in: records.id, as: GroupRecord.self, key: String.self)?.count == 1)
    #expect(
      first.changes(in: assignments.id, as: GroupAssignment.self, key: String.self)?.count == 2)
    #expect(first.changes(in: tags.id, as: GroupTag.self, key: String.self)?.count == 1)
    #expect(first.expectedCursor == StreamCursor(offset: "10", lsn: "0/10"))
    #expect(first.cursor == StreamCursor(offset: "11", lsn: "0/20"))
    // The store's apply runs under the batch's delivery, in the mode that read it.
    let applied = await deliveries.values
    #expect(applied.count == 2)
    #expect(applied.allSatisfy { $0?.mode == mode })
    #expect(applied.map { $0?.firstAfterReconnect } == [true, false])
    // The snapshot already held "Grace", so the first batch's delivery lists the four applied.
    #expect(applied.map { $0?.changes.count } == [4, 2])

    // Each row is in its own table: deleting key "1" in one table never touched the others.
    #expect(
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
        == [:])
    #expect(
      await store.rows(
        in: assignments.id, scope: groupScope, as: GroupAssignment.self, key: String.self)
        == [
          "2": GroupAssignment(id: "2", person: "Grace"),
          "3": GroupAssignment(id: "3", person: "Linus"),
        ])
    #expect(
      await store.rows(in: tags.id, scope: groupScope, as: GroupTag.self, key: String.self)
        == ["1": GroupTag(id: "1", name: "urgent v2"), "2": GroupTag(id: "2", name: "later")])
    // One cursor for the whole group.
    let record = try #require(try await store.groupMaterialization(id))
    #expect(record.cursor == StreamCursor(offset: "12", lsn: "0/30"))
    #expect(record.collections == [records.id, assignments.id, tags.id])

    // Setup loads the group once, and the subscription starts on that load.
    let loads = await server.loads
    #expect(loads.count == 1)
    #expect(loads.first?.subscription == "scope-1")
    #expect(
      loads.first?.members.map(\.table) == ["public.record", "public.assignment", "public.tag"])
    let live = await server.streamURLs.compactMap {
      URLComponents(url: $0, resolvingAgainstBaseURL: false)?.queryItems?
        .first { $0.name == "live" }?.value
    }
    #expect(!live.isEmpty && live.allSatisfy { $0 == mode.rawValue })
    #expect(await server.releases.count == 1)
  }

  @Test func equalKeysInTwoTablesInsertUpdateAndDeleteIndependently() async throws {
    let store = InMemoryCollectionGroupStore()
    let demand = threeTableDemand()
    let id = CollectionMaterializationID(rawValue: "equal-keys")
    let v1 = CollectionSourceVersion(rawValue: "0/1", order: 1)
    try await store.replaceGroupSnapshot(
      CollectionGroupSnapshot(
        members: [
          CollectionGroupRows(
            collection: records.id, table: "public.record",
            rows: [GroupRecord(id: "7", title: "Record")], key: \.id),
          CollectionGroupRows(
            collection: tags.id, table: "public.tag", rows: [GroupTag(id: "7", name: "Tag")],
            key: \.id),
        ],
        fence: SnapshotFence(rawValue: "f"), sourceVersion: v1,
        cursor: StreamCursor(offset: "1")),
      materializationID: id, demand: demand.identity(scope: groupScope))

    func commit(
      _ record: [CollectionChange<GroupRecord, String>],
      _ tag: [CollectionChange<GroupTag, String>], from: String, to: String, order: UInt64
    ) async throws {
      try await store.applyGroup(
        CollectionGroupChangeBatch(
          members: [
            CollectionGroupChanges(
              collection: records.id, table: "public.record", changes: record, key: \.id),
            CollectionGroupChanges(
              collection: tags.id, table: "public.tag", changes: tag, key: \.id),
          ],
          expectedCursor: StreamCursor(offset: from), cursor: StreamCursor(offset: to),
          sourceVersion: CollectionSourceVersion(rawValue: "0/\(order)", order: order)),
        to: id)
    }
    func version(_ order: UInt64) -> CollectionSourceVersion {
      CollectionSourceVersion(rawValue: "0/\(order)", order: order)
    }
    func recordRows() async -> [String: GroupRecord] {
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
    }
    func tagRows() async -> [String: GroupTag] {
      await store.rows(in: tags.id, scope: groupScope, as: GroupTag.self, key: String.self)
    }

    try await commit(
      [.upsert(GroupRecord(id: "7", title: "Record v2"), sourceVersion: version(2))], [],
      from: "1", to: "2", order: 2)
    #expect(await recordRows() == ["7": GroupRecord(id: "7", title: "Record v2")])
    #expect(await tagRows() == ["7": GroupTag(id: "7", name: "Tag")])

    try await commit(
      [], [.delete("7", sourceVersion: version(3))], from: "2", to: "3", order: 3)
    #expect(await recordRows() == ["7": GroupRecord(id: "7", title: "Record v2")])
    #expect(await tagRows() == [:])

    try await commit(
      [.delete("7", sourceVersion: version(4))],
      [.upsert(GroupTag(id: "7", name: "Tag again"), sourceVersion: version(4))],
      from: "3", to: "4", order: 4)
    #expect(await recordRows() == [:])
    #expect(await tagRows() == ["7": GroupTag(id: "7", name: "Tag again")])
  }

  @Test func failedGroupCommitLeavesEveryTableAndTheCursorUnchanged() async throws {
    let store = InMemoryCollectionGroupStore()
    let id = CollectionMaterializationID(rawValue: "atomic")
    let v1 = CollectionSourceVersion(rawValue: "0/1", order: 1)
    let v2 = CollectionSourceVersion(rawValue: "0/2", order: 2)
    try await store.replaceGroupSnapshot(
      CollectionGroupSnapshot(
        members: [
          CollectionGroupRows(
            collection: records.id, table: "public.record",
            rows: [GroupRecord(id: "1", title: "Record")], key: \.id),
          CollectionGroupRows(
            collection: tags.id, table: "public.tag", rows: [GroupTag(id: "1", name: "Tag")],
            key: \.id),
        ],
        fence: SnapshotFence(rawValue: "f"), sourceVersion: v1,
        cursor: StreamCursor(offset: "1")),
      materializationID: id, demand: threeTableDemand().identity(scope: groupScope))
    let recordChange = CollectionGroupChanges<GroupRecord, String>(
      collection: records.id, table: "public.record",
      changes: [.upsert(GroupRecord(id: "1", title: "Record v2"), sourceVersion: v2)], key: \.id)

    // The second table of this batch can't be committed, after the first table's change.
    let refused = CollectionGroupChangeBatch(
      members: [
        recordChange,
        CollectionGroupChanges<GroupAssignment, String>(
          collection: assignments.id, table: "public.assignment",
          changes: [.upsert(GroupAssignment(id: "1", person: "Ada"), sourceVersion: v2)],
          key: \.id),
      ],
      expectedCursor: StreamCursor(offset: "1"), cursor: StreamCursor(offset: "2"),
      sourceVersion: v2)
    await #expect(throws: CollectionGroupStoreError.collectionOutsideGroup(assignments.id)) {
      try await store.applyGroup(refused, to: id)
    }
    #expect(
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
        == ["1": GroupRecord(id: "1", title: "Record")])
    #expect(try await store.groupMaterialization(id)?.cursor == StreamCursor(offset: "1"))

    let committed = CollectionGroupChangeBatch(
      members: [
        recordChange,
        CollectionGroupChanges<GroupTag, String>(
          collection: tags.id, table: "public.tag",
          changes: [.upsert(GroupTag(id: "1", name: "Tag v2"), sourceVersion: v2)], key: \.id),
      ],
      expectedCursor: StreamCursor(offset: "1"), cursor: StreamCursor(offset: "2"),
      sourceVersion: v2)
    try await store.applyGroup(committed, to: id)
    #expect(
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
        == ["1": GroupRecord(id: "1", title: "Record v2")])
    #expect(
      await store.rows(in: tags.id, scope: groupScope, as: GroupTag.self, key: String.self)
        == ["1": GroupTag(id: "1", name: "Tag v2")])
    #expect(try await store.groupMaterialization(id)?.cursor == StreamCursor(offset: "2"))
  }

  @Test func refusedStoreCommitFailsTheGroupWithoutAdvancingItsCursor() async throws {
    let server = GroupStreamServer(batches: groupBatches)
    let source = makeSource(server, mode: .longPoll)
    let demand = threeTableDemand()
    let id = CollectionMaterializationID(rawValue: "scope-1")
    let session = try await source.materialize(demand, materializationID: id)
    // The store holds the group without `tags`, so it refuses the first batch at its last
    // member, after the `record` and `assignment` changes are already in its working copy.
    let store = InMemoryCollectionGroupStore()
    try await store.replaceGroupSnapshot(
      CollectionGroupSnapshot(
        members: session.snapshot.members.filter { $0.collection != tags.id },
        fence: session.snapshot.fence, sourceVersion: session.snapshot.sourceVersion,
        cursor: session.snapshot.cursor),
      materializationID: id, demand: demand.identity(scope: groupScope))
    let attempts = GroupBatchLog()

    await #expect(throws: CircuitsSubsetGroupSourceError.subscription(.materializer)) {
      try await session.run { batch in
        await attempts.append(batch)
        try await store.applyGroup(batch, to: id)
      }
    }
    // The refused batch was the only one offered: the source never moved past it.
    let offered = await attempts.batches
    #expect(offered.count == 1)
    #expect(offered.first?.expectedCursor == session.snapshot.cursor)
    #expect(offered.first?.cursor.offset == "11")
    // Neither table nor the cursor changed.
    #expect(try await store.groupMaterialization(id)?.cursor == session.snapshot.cursor)
    #expect(
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
        == ["1": GroupRecord(id: "1", title: "Record")])
    #expect(
      await store.rows(
        in: assignments.id, scope: groupScope, as: GroupAssignment.self, key: String.self)
        == [
          "1": GroupAssignment(id: "1", person: "Ada"),
          "2": GroupAssignment(id: "2", person: "Grace"),
        ])
    #expect(await server.releases.count == 1)
  }

  @Test(arguments: [LiveReadMode.longPoll, .sse], [410, 404])
  func aGroupReleasedBetweenSetupAndItsFirstReadRequiresReseed(
    mode: LiveReadMode, status: Int
  ) async throws {
    // The group was released after setup's load, so its stream is gone. Nothing from it reaches
    // the store, and the caller loads again.
    let server = GroupStreamServer(batches: groupBatches, goneReads: 1, goneStatus: status)
    let session = try await makeSource(server, mode: mode).materialize(
      threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))
    let offered = GroupBatchLog()

    do {
      try await session.run { batch in await offered.append(batch) }
      Issue.record("expected the released group to require a reseed")
    } catch CircuitsSubsetGroupSourceError.subscription(.reseedRequired) {}
    #expect(await offered.batches.isEmpty)
    #expect(await server.loads.count == 1)
    #expect(await server.releases.contains { $0.path == "/v1/shapes/group-1" })
  }

  @Test func cancellingRunWhileTheStreamIsReadTearsDownAndReleases() async throws {
    for _ in 0..<20 {
      let server = GroupStreamServer(batches: [])
      let session = try await makeSource(server, mode: .longPoll).materialize(
        threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))
      let run = Task { try await session.run { _ in } }
      await waitUntil { await !server.streamURLs.isEmpty }
      run.cancel()

      let finished = await withTaskGroup(of: Bool.self) { group in
        group.addTask {
          _ = try? await run.value
          return true
        }
        group.addTask {
          try? await Task.sleep(for: .seconds(10))
          return false
        }
        let first = await group.next() ?? false
        group.cancelAll()
        return first
      }
      #expect(finished, "run did not finish after cancellation")
      guard finished else { return }
      #expect(await server.loads.count == 1)
      // A cancelled run hands its release to the session's stop, which outlives it.
      await waitUntil { await !server.releases.isEmpty }
      #expect(await server.releases.contains { $0.path == "/v1/shapes/group-1" })
    }
  }

  @Test func aSessionRenewsItsSetupLoadCountingFromThatLoad() async throws {
    let server = GroupStreamServer(batches: [], leaseSeconds: 30)
    let clock = GroupSourceLeaseClock()
    let session = try await makeSource(server, mode: .longPoll, clock: clock).materialize(
      threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))
    // The caller commits its snapshot between the load and `run`.
    try await Task.sleep(for: .milliseconds(50))

    let run = Task { try await session.run { _ in } }
    await waitUntil { await !clock.sleeps.isEmpty }
    let first = try #require(await clock.sleeps.first)
    // The source reads a real clock, so the 50 ms above counts against the first interval.
    #expect(first <= .seconds(10) - .milliseconds(50))
    #expect(await server.loads.count == 1)

    await clock.wake()
    await waitUntil { await clock.sleeps.count >= 2 }
    #expect(await server.loads.count == 2)
    #expect(await clock.sleeps[1] == .seconds(10))

    run.cancel()
    _ = await run.result
    await waitUntil { await !server.releases.isEmpty }
    #expect(await server.releases.contains { $0.path == "/v1/shapes/group-1" })
  }

  @Test func aSlowSetupLoadShortensTheFirstRenewal() async throws {
    // A 30-second lease renews every 10 seconds, and the lease may have begun as the request
    // reached the server, so the load's own time counts against the first interval.
    let loadDelay = Duration.milliseconds(50)
    let server = GroupStreamServer(batches: [], leaseSeconds: 30, loadDelay: loadDelay)
    let clock = GroupSourceLeaseClock()
    let session = try await makeSource(server, mode: .longPoll, clock: clock).materialize(
      threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))

    let run = Task { try await session.run { _ in } }
    await waitUntil { await !clock.sleeps.isEmpty }
    let first = try #require(await clock.sleeps.first)
    #expect(first <= .seconds(10) - loadDelay)
    #expect(await server.loads.count == 1)

    // The renewal comes when that sleep ends, inside the lease.
    await clock.wake()
    await waitUntil { await server.loads.count >= 2 }
    #expect(await server.loads.count == 2)

    run.cancel()
    _ = await run.result
    await waitUntil { await !server.releases.isEmpty }
  }

  @Test func aStopDuringSetupReleasesTheGroupAndAppliesNothing() async throws {
    let server = GroupStreamServer(batches: groupBatches)
    let session = try await makeSource(server, mode: .longPoll).materialize(
      threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))

    try await session.stop()

    #expect(await server.loads.count == 1)
    #expect(await server.releases.contains { $0.path == "/v1/shapes/group-1" })
    #expect(await server.streamURLs.isEmpty)
  }

  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func stoppingAStreamingSessionEndsRunWithoutAFailure(mode: LiveReadMode) async throws {
    // The stream never runs dry, so batches keep arriving while the session stops.
    for _ in 0..<20 {
      let server = GroupStreamServer(batches: [], endless: true)
      let demand = threeTableDemand()
      let id = CollectionMaterializationID(rawValue: "scope-1")
      let session = try await makeSource(server, mode: mode).materialize(
        demand, materializationID: id)
      let store = InMemoryCollectionGroupStore()
      try await store.replaceGroupSnapshot(
        session.snapshot, materializationID: id, demand: demand.identity(scope: groupScope))
      let log = GroupBatchLog()
      let run = Task {
        try await session.run { batch in
          try await store.applyGroup(batch, to: id)
          await log.append(batch)
        }
      }
      await waitUntil { await log.batches.count >= 3 }
      #expect(await log.batches.count >= 3)
      try await session.stop()
      do {
        try await run.value
      } catch {
        Issue.record("a clean stop ended run with \(error)")
        return
      }
      #expect(await server.releases.count == 1)
    }
  }

  @Test func envelopeForATableOutsideTheGroupFailsTheGroup() async throws {
    let server = GroupStreamServer(batches: [
      #"[{"type":"public.calendar_event","key":"1","value":{"id":"1"},"headers":{"operation":"insert","lsn":"0/20"}}]"#
    ])
    let session = try await makeSource(server, mode: .longPoll).materialize(
      threeTableDemand(), materializationID: CollectionMaterializationID(rawValue: "scope-1"))
    await #expect(throws: CircuitsSubsetGroupSourceError.tableOutsideGroup("public.calendar_event"))
    {
      try await session.run { _ in }
    }
    #expect(await server.releases.count == 1)
  }
}
