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
/// in `tag`.
private let groupBatches = [
  #"[{"type":"public.record","key":"1","value":{"id":"1","title":"Record v2"},"headers":{"operation":"update","lsn":"0/20"}},"#
    + #"{"type":"public.assignment","key":"1","headers":{"operation":"delete","lsn":"0/20"}},"#
    + #"{"type":"public.assignment","key":"3","value":{"id":"3","person":"Linus"},"headers":{"operation":"insert","lsn":"0/20"}},"#
    + #"{"type":"public.tag","key":"2","value":{"id":"2","name":"later"},"headers":{"operation":"insert","lsn":"0/20"}}]"#,
  #"[{"type":"public.record","key":"1","headers":{"operation":"delete","lsn":"0/30"}},"#
    + #"{"type":"public.tag","key":"1","value":{"id":"1","name":"urgent v2"},"headers":{"operation":"update","lsn":"0/30"}}]"#,
]

/// A mock of the gateway's group-only `subsets/load` and the group's one stream, answering
/// stream reads by long-poll through `send` and by SSE through `stream`.
private actor GroupStreamServer: HTTPTransport {
  private var batches: [String]
  private var nextOffset = 10
  private(set) var loads: [SubsetGroupRequest] = []
  private(set) var streamURLs: [URL] = []
  private(set) var releases: [URL] = []

  init(batches: [String]) { self.batches = batches }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch (request.httpMethod, path) {
    case ("POST", "/v1/subsets/load"):
      loads.append(try JSONDecoder().decode(SubsetGroupRequest.self, from: request.httpBody!))
      return reply(groupLoadAnswer, request: request)
    case ("GET", "/streams/group-1"):
      streamURLs.append(request.url!)
      guard let body = takeBatch() else {
        try await Task.sleep(for: .seconds(3_600))
        throw CancellationError()
      }
      return reply(body, request: request, headers: ["stream-next-offset": "\(nextOffset)"])
    case ("DELETE", "/v1/shapes/group-1"):
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

private struct StoreRefusal: Error {}

private func makeSource(_ server: GroupStreamServer, mode: LiveReadMode)
  -> CircuitsSubsetGroupSource
{
  CircuitsSubsetGroupSource(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: server,
      configuration: .init(liveReadMode: mode)),
    transport: server,
    retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0))
}

private func waitUntil(_ condition: @Sendable () async -> Bool) async {
  for _ in 0..<20_000 {
    if await condition() { return }
    await Task.yield()
  }
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
    let run = Task {
      try await session.run { batch in
        try await store.applyGroup(batch, to: id)
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

    // Setup loads the group and the subscription renews it under the same claim and members.
    let loads = await server.loads
    #expect(loads.count == 2)
    #expect(loads.allSatisfy { $0.subscription == "scope-1" })
    #expect(
      loads.first?.members.map(\.table) == ["public.record", "public.assignment", "public.tag"])
    #expect(loads.first == loads.last)
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
    let store = InMemoryCollectionGroupStore()
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: id, demand: demand.identity(scope: groupScope))

    await #expect(throws: CircuitsSubsetGroupSourceError.self) {
      try await session.run { _ in throw StoreRefusal() }
    }
    #expect(try await store.groupMaterialization(id)?.cursor == session.snapshot.cursor)
    #expect(
      await store.rows(in: records.id, scope: groupScope, as: GroupRecord.self, key: String.self)
        == ["1": GroupRecord(id: "1", title: "Record")])
    #expect(await server.releases.count == 1)
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
