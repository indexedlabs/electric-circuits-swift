import ElectricCircuitsSwift
import Foundation
import GRDB
import Testing

@testable import LinearLiteGRDB

extension LinearLiteShapeMaterializer {
  fileprivate func apply(_ batch: ChangeBatch, advancingTo cursor: StreamCursor) async throws {
    try await apply(batch, expecting: try await currentCursor(), advancingTo: cursor)
  }

  fileprivate func replaceSnapshot(_ rows: [ChangeRow], advancingTo cursor: StreamCursor)
    async throws
  {
    try await replaceSnapshot(rows, expecting: try await currentCursor(), advancingTo: cursor)
  }
}

@Suite("LinearLite GRDB shape materializer")
struct LinearLiteShapeMaterializerTests {
  private func database() throws -> DatabaseQueue {
    try DatabaseQueue()
  }

  private func issueRow(
    id: Int64,
    clientID: String? = nil,
    title: String = "First",
    description: String = "Details",
    status: String = "backlog",
    priority: String = "high",
    username: String = "ada",
    projectID: Int64 = 7,
    created: Int64 = 100,
    modified: Int64 = 101,
    kanbanOrder: Double = 1.5
  ) -> ChangeRow {
    var row: ChangeRow = [
      "id": .int(id), "title": .string(title), "description": .string(description),
      "status": .string(status), "priority": .string(priority), "username": .string(username),
      "project_id": .int(projectID), "created": .int(created), "modified": .int(modified),
      "kanbanorder": .number(kanbanOrder),
    ]
    if let clientID { row["client_id"] = .string(clientID) }
    return row
  }

  private func envelope(
    key: String,
    value: ChangeRow? = nil,
    operation: ChangeOperation = .upsert,
    lsn: String? = nil
  ) -> ChangeEnvelope {
    ChangeEnvelope(
      type: "public.issues", key: key, value: value,
      headers: EnvelopeHeaders(operation: operation, lsn: lsn))
  }

  /// The core protocol deliberately exposes a cursor but not a common row-store API. This fixture
  /// shares only public `ShapeMaterializer` transitions; each provider supplies its own read path.
  private func assertPublicMaterializerContract(
    _ materializer: any ShapeMaterializer,
    rows: () async throws -> [String],
    batch: ChangeBatch
  ) async throws {
    let committed = StreamCursor(offset: "contract-1", lsn: "0/1")
    try await materializer.apply(batch, expecting: nil, advancingTo: committed)
    #expect(try await rows() == ["one"])
    #expect(try await materializer.currentCursor() == committed)

    try await materializer.apply(ChangeBatch(), expecting: committed, advancingTo: committed)
    #expect(try await rows() == ["one"])
    await #expect(
      throws: StreamError.cursorConflict(
        expected: StreamCursor(offset: "wrong"), actual: committed,
        advancingTo: StreamCursor(offset: "contract-2"))
    ) {
      try await materializer.apply(
        ChangeBatch(), expecting: StreamCursor(offset: "wrong"),
        advancingTo: StreamCursor(offset: "contract-2"))
    }
    #expect(try await materializer.currentCursor() == committed)
  }

  @Test func initialCursorIsNilAndMigrationsCreateTables() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")

    #expect(try await provider.currentCursor() == nil)
    let tables = try await db.read { db in
      try String.fetchAll(
        db, sql: "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    }
    #expect(tables.contains("issues"))
    #expect(tables.contains("shape_cursors"))
    let issueColumns = try db.read { db in
      try Row.fetchAll(db, sql: "PRAGMA table_info(issues)")
    }
    #expect(issueColumns.first?["name"] as String? == "principal_id")
    #expect(issueColumns.first?["pk"] as Int? == 1)
    #expect(issueColumns.dropFirst().first?["name"] as String? == "id")
    #expect(issueColumns.dropFirst().first?["pk"] as Int? == 2)
    #expect(issueColumns.contains { $0["name"] as String? == "client_id" })
    #expect(tables.contains("subset_view_members"))
    #expect(tables.contains("issue_overlays"))
  }

  @Test func seedsAndUpsertsTypedIssuesInOneBatch() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let issue = issueRow(id: 42)
    try await provider.apply(
      ChangeBatch([envelope(key: "42", value: issue)]),
      advancingTo: StreamCursor(offset: "10", lsn: "0/10"))

    let expected = try Issue(changeRow: issue)
    #expect(try await provider.allIssues() == [expected])
    #expect(try await provider.currentCursor() == StreamCursor(offset: "10", lsn: "0/10"))
  }

  @Test func publicMaterializerContractRunsAgainstInMemoryAndGRDBProviders() async throws {
    let batch = ChangeBatch([
      envelope(key: "1", value: issueRow(id: 1, title: "one")),
      envelope(key: "2", value: issueRow(id: 2, title: "two")),
      envelope(key: "2", operation: .delete),
    ])

    let memory = InMemoryShapeMaterializer()
    try await assertPublicMaterializerContract(
      memory,
      rows: {
        await memory.snapshot().compactMap { _, row in
          guard case .string(let title) = row["title"] else { return nil }
          return title
        }
      },
      batch: batch)

    let db = try database()
    let grdb = try LinearLiteShapeMaterializer(database: db, shapeID: "contract")
    try await assertPublicMaterializerContract(
      grdb, rows: { try await grdb.allIssues().map(\.title) }, batch: batch)
  }

  @Test func clientIDRoundTripsThroughCanonicalRowsAndChangeRows() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-client-id")
    let row = issueRow(id: 42, clientID: "550e8400-e29b-41d4-a716-446655440000")
    try await provider.apply(
      ChangeBatch([envelope(key: "42", value: row)]),
      advancingTo: StreamCursor(offset: "1", lsn: "0/10"))

    let issue = try #require(try await provider.allIssues().first)
    #expect(issue.clientID == "550e8400-e29b-41d4-a716-446655440000")
    #expect(issue.changeRow["client_id"] == .string("550e8400-e29b-41d4-a716-446655440000"))
  }

  @Test func deleteUsesBarePrimaryKeyAndPersistsCursor() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    try await provider.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42))]),
      advancingTo: StreamCursor(offset: "10"))
    try await provider.apply(
      ChangeBatch([envelope(key: "42", operation: .delete)]),
      advancingTo: StreamCursor(offset: "11"))

    #expect(try await provider.allIssues().isEmpty)
    let reopened = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    #expect(try await reopened.currentCursor() == StreamCursor(offset: "11"))
  }

  @Test func cursorDurabilitySurvivesNewProviderInstance() async throws {
    let db = try database()
    let first = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    try await first.apply(
      ChangeBatch(), advancingTo: StreamCursor(offset: "99", lsn: "0/99"))

    let second = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    #expect(try await second.currentCursor() == StreamCursor(offset: "99", lsn: "0/99"))
  }

  @Test func malformedLaterEnvelopeRollsBackEarlierRowsAndCursor() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let malformed: ChangeRow = ["id": .int(43), "title": .string("missing required fields")]
    await #expect(throws: LinearLiteShapeMaterializerError.self) {
      try await provider.apply(
        ChangeBatch([
          envelope(key: "42", value: issueRow(id: 42)),
          envelope(key: "43", value: malformed),
        ]),
        advancingTo: StreamCursor(offset: "12"))
    }

    #expect(try await provider.allIssues().isEmpty)
    #expect(try await provider.currentCursor() == nil)
  }

  @Test func changeBatchReadsBackMultipleTypedIssues() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let expected = [
      try Issue(changeRow: issueRow(id: 1)), try Issue(changeRow: issueRow(id: 2, title: "Second")),
    ]
    try await provider.apply(
      ChangeBatch(expected.map { envelope(key: String($0.id), value: $0.changeRow) }),
      advancingTo: StreamCursor(offset: "20"))

    #expect(try await provider.allIssues() == expected)
  }

  @Test func twoMaterializersSharingDatabaseRemainShapeIsolated() async throws {
    let db = try database()
    let first = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let second = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-2")
    let sharedIssue = issueRow(id: 42, title: "Shared issue")
    let firstIssue = issueRow(id: 1, title: "First view")
    let secondIssue = issueRow(id: 2, title: "Second view")

    try await first.apply(
      ChangeBatch([envelope(key: "42", value: sharedIssue), envelope(key: "1", value: firstIssue)]),
      advancingTo: StreamCursor(offset: "1"))
    try await second.apply(
      ChangeBatch([envelope(key: "42", value: sharedIssue), envelope(key: "2", value: secondIssue)]
      ),
      advancingTo: StreamCursor(offset: "2"))

    #expect(try await first.allIssues().map(\.id) == [1, 42])
    #expect(try await second.allIssues().map(\.id) == [2, 42])

    try await second.apply(
      ChangeBatch([envelope(key: "42", operation: .delete)]),
      advancingTo: StreamCursor(offset: "3"))

    #expect(try await second.allIssues().map(\.id) == [2])
    #expect(try await first.allIssues().map(\.id) == [1, 42])
  }

  @Test func staleOverlappingFeedRowsCannotRegressCanonicalDataOrMembership() async throws {
    let db = try database()
    let first = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let second = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-2")

    try await first.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Newest"), lsn: "0/20")]),
      advancingTo: StreamCursor(offset: "20", lsn: "0/20"))
    try await second.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Newest"), lsn: "0/20")]),
      advancingTo: StreamCursor(offset: "20", lsn: "0/20"))

    try await second.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Stale"), lsn: "0/10")]),
      advancingTo: StreamCursor(offset: "21", lsn: "0/10"))

    #expect(try await first.allIssues().first?.title == "Newest")
    #expect(try await second.allIssues().first?.title == "Newest")

    try await second.apply(
      ChangeBatch([envelope(key: "42", operation: .delete, lsn: "0/15")]),
      advancingTo: StreamCursor(offset: "15", lsn: "0/15"))
    #expect(try await second.allIssues().map(\.id) == [42])

    try await second.apply(
      ChangeBatch([envelope(key: "42", operation: .delete, lsn: "0/25")]),
      advancingTo: StreamCursor(offset: "25", lsn: "0/25"))
    #expect(try await second.allIssues().isEmpty)
    #expect(try await first.allIssues().map(\.id) == [42])
  }

  @Test func oldUnscopedRowsFailMigrationInsteadOfAssigningShape() throws {
    let db = try database()
    try db.write { db in
      try db.execute(
        sql: """
          CREATE TABLE issues (
            id INTEGER PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            username TEXT NOT NULL,
            project_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            kanbanorder REAL NOT NULL
          )
          """)
      try db.execute(
        sql: """
          CREATE TABLE shape_cursors (
            shape_id TEXT PRIMARY KEY NOT NULL,
            offset TEXT NOT NULL,
            lsn TEXT
          )
          """)
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified, kanbanorder)
          VALUES (42, 'legacy', 'legacy', 'backlog', 'high', 'ada', 7, 100, 101, 1.5)
          """)
    }

    // The manually-created schema represents a database that reached the old v1 schema. GRDB's
    // migration table is intentionally absent, so v1 is recorded and v2 sees the legacy row.
    // The provider must fail closed rather than silently choosing a shape for that row.
    #expect(throws: LinearLiteShapeMaterializerError.unscopedRowsRequireReseed(count: 1)) {
      try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    }
  }

  @Test func shapeScopedRowsMigrateToCanonicalRowsAndMemberships() throws {
    let db = try database()
    try db.write { db in
      try db.execute(
        sql: """
          CREATE TABLE issues (
            shape_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            username TEXT NOT NULL,
            project_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            kanbanorder REAL NOT NULL,
            PRIMARY KEY (shape_id, id)
          )
          """)
      try db.execute(
        sql: """
          CREATE TABLE shape_cursors (
            shape_id TEXT PRIMARY KEY NOT NULL,
            offset TEXT NOT NULL,
            lsn TEXT
          )
          """)
      for shapeID in ["shape-1", "shape-2"] {
        try db.execute(
          sql: """
            INSERT INTO issues
              (shape_id, id, title, description, status, priority, username, project_id,
               created, modified, kanbanorder)
            VALUES (?, 42, 'First', 'Details', 'backlog', 'high', 'ada', 7, 100, 101, 1.5)
            """,
          arguments: [shapeID])
      }
    }

    _ = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    let tables = try db.read { db in
      try String.fetchAll(
        db, sql: "SELECT name FROM sqlite_master WHERE type = 'table' ORDER BY name")
    }
    #expect(tables.contains("issues_shape_scoped_legacy"))
    #expect(tables.contains("issues"))
    #expect(
      try db.read { db in
        try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM issues")
      } == 1)
    #expect(
      try db.read { db in
        try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM subset_view_members")
      } == 2)
  }

  @Test func conflictingShapeScopedRowsFailClosedDuringMigration() throws {
    let db = try database()
    try db.write { db in
      try db.execute(
        sql: """
          CREATE TABLE issues (
            shape_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            username TEXT NOT NULL,
            project_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            kanbanorder REAL NOT NULL,
            PRIMARY KEY (shape_id, id)
          )
          """)
      try db.execute(
        sql: """
          CREATE TABLE shape_cursors (
            shape_id TEXT PRIMARY KEY NOT NULL,
            offset TEXT NOT NULL,
            lsn TEXT
          )
          """)
      for (shapeID, title) in [("shape-1", "first"), ("shape-2", "different")] {
        try db.execute(
          sql: """
            INSERT INTO issues
              (shape_id, id, title, description, status, priority, username, project_id,
               created, modified, kanbanorder)
            VALUES (?, 42, ?, 'Details', 'backlog', 'high', 'ada', 7, 100, 101, 1.5)
            """,
          arguments: [shapeID, title])
      }
    }

    #expect(
      throws: LinearLiteShapeMaterializerError.conflictingShapeRowsRequireReseed(ids: [42])
    ) {
      try LinearLiteShapeMaterializer(database: db, shapeID: "shape-1")
    }
    #expect(
      try db.read { db in
        try Bool.fetchOne(
          db,
          sql: "SELECT EXISTS(SELECT 1 FROM pragma_table_info('issues') WHERE name = 'shape_id')")
      } == true)
  }

  @Test func completeJSONEnvelopeDecodesAndMaterializes() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-json")
    let fixtureURL = Bundle.module.url(
      forResource: "complete-shape-upsert", withExtension: "json", subdirectory: "Fixtures")!
    let batch = try JSONDecoder().decode(ChangeBatch.self, from: Data(contentsOf: fixtureURL))
    try await provider.apply(batch, advancingTo: StreamCursor(offset: "json-1"))

    #expect(
      try await provider.allIssues() == [
        try Issue(
          changeRow: issueRow(
            id: 42,
            title: "From JSON",
            description: "Decoded from the provider fixture",
            kanbanOrder: 2.25))
      ])
  }

  @Test func replacingSnapshotRemovesStaleRowsAndCommitsCursorAtomically() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "subset")
    try await provider.apply(
      ChangeBatch([envelope(key: "99", value: issueRow(id: 99))]),
      advancingTo: StreamCursor(offset: "old"))

    try await provider.replaceSnapshot(
      [issueRow(id: 2, modified: 200), issueRow(id: 1, modified: 100)],
      advancingTo: StreamCursor(offset: "subset:0/20", lsn: "0/20"))

    #expect(try await provider.allIssues(order: .modifiedDescending).map(\.id) == [2, 1])
    #expect(try await provider.currentCursor() == StreamCursor(offset: "subset:0/20", lsn: "0/20"))
    #expect(
      try await db.read { db in
        try Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?",
          arguments: ["subset"])
      } == 2)
  }

  @Test func optimisticOverlayPersistsAndCanBeRetired() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "overlay-view")
    let overlay = LinearLiteIssueOverlay(
      mutationID: "mutation-1", rowKey: "42", operation: .update,
      patch: ["title": .string("Optimistic title")], requestID: "request-1",
      createdAt: Date(timeIntervalSince1970: 123))

    try await provider.saveOverlay(overlay)
    let reopened = try LinearLiteShapeMaterializer(database: db, shapeID: "overlay-view")
    #expect(try await reopened.overlays(for: "42") == [overlay])

    try await reopened.removeOverlay(mutationID: "mutation-1")
    #expect(try await reopened.overlays(for: "42").isEmpty)
  }

  @Test func optimisticOverlayProjectsOverCanonicalIssueWithoutChangingAuthoritativeRow()
    async throws
  {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "overlay-view")
    let row = issueRow(id: 42, title: "Authoritative")
    try await provider.apply(
      ChangeBatch([envelope(key: "42", value: row)]), advancingTo: StreamCursor(offset: "1"))
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "mutation-update", rowKey: "42", operation: .update,
        patch: ["title": .string("Optimistic")]))

    #expect(try await provider.allIssues().first?.title == "Authoritative")
    #expect(try await provider.allIssuesIncludingOverlays().first?.title == "Optimistic")

    try await provider.removeOverlay(mutationID: "mutation-update")
    #expect(try await provider.allIssuesIncludingOverlays().first?.title == "Authoritative")

    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "mutation-delete", rowKey: "42", operation: .delete, patch: [:]))
    #expect(try await provider.allIssuesIncludingOverlays().isEmpty)
  }

  @Test func clientIDReconcilesAnOptimisticInsertWhenTheFeedAssignsNumericID() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-client-id")
    let clientID = "550e8400-e29b-41d4-a716-446655440000"
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "insert-client-id", rowKey: clientID, operation: .insert,
        patch: ["client_id": .string(clientID), "title": .string("Optimistic")]))

    try await provider.apply(
      ChangeBatch([
        envelope(key: "42", value: issueRow(id: 42, clientID: clientID, title: "Optimistic"))
      ]), advancingTo: StreamCursor(offset: "2", lsn: "0/20"))

    #expect(try await provider.overlays(for: clientID).isEmpty)
    #expect(try await provider.allIssues().first?.clientID == clientID)
  }

  @Test func clientIDReconcilesAnOptimisticDeleteFromReplicaIdentityOldRow() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "shape-client-delete")
    let clientID = "550e8400-e29b-41d4-a716-446655440000"
    try await provider.apply(
      ChangeBatch([
        envelope(
          key: "42", value: issueRow(id: 42, clientID: clientID), lsn: "0/10")
      ]), advancingTo: StreamCursor(offset: "1", lsn: "0/10"))
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "delete-client-id", rowKey: clientID, operation: .delete, patch: [:]))

    try await provider.apply(
      ChangeBatch([
        ChangeEnvelope(
          type: "public.issues", key: "42", old: ["client_id": .string(clientID)],
          headers: EnvelopeHeaders(operation: .delete, lsn: "0/20"))
      ]), advancingTo: StreamCursor(offset: "2", lsn: "0/20"))

    #expect(try await provider.overlays(for: clientID).isEmpty)
    #expect(try await provider.allIssues().isEmpty)
  }

  @Test func rejectedOverlayDoesNotProjectAndInvalidPatchFailsClosed() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "overlay-view")
    try await provider.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Authoritative"))]),
      advancingTo: StreamCursor(offset: "1"))
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "mutation-rejected", rowKey: "42", operation: .update,
        patch: ["title": .string("Rejected")], status: .rejected))
    #expect(try await provider.allIssuesIncludingOverlays().first?.title == "Authoritative")

    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "mutation-invalid", rowKey: "42", operation: .update,
        patch: ["unknown": .string("bad")]))
    await #expect(throws: LinearLiteShapeMaterializerError.self) {
      try await provider.allIssuesIncludingOverlays()
    }
  }

  @Test func injectedProviderFailureRollsBackRowsMembershipAndCursor() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(
      database: db,
      shapeID: "faulted",
      faultInjector: { phase in
        if phase == .afterFirstRow { throw MaterializationFault.injected(phase: phase) }
      })

    await #expect(throws: MaterializationFault.injected(phase: .afterFirstRow)) {
      try await provider.apply(
        ChangeBatch([
          envelope(key: "1", value: issueRow(id: 1)),
          envelope(key: "2", value: issueRow(id: 2)),
        ]), advancingTo: StreamCursor(offset: "fault-1"))
    }
    #expect(try await provider.allIssues().isEmpty)
    #expect(try await provider.currentCursor() == nil)
  }

  @Test func committedBatchCanBeReplayedAfterProviderRestart() async throws {
    let db = try database()
    let cursor = StreamCursor(offset: "restart-1", lsn: "0/10")
    let batch = ChangeBatch([envelope(key: "1", value: issueRow(id: 1), lsn: "0/10")])
    let first = try LinearLiteShapeMaterializer(database: db, shapeID: "restart")
    try await first.apply(batch, advancingTo: cursor)

    let reopened = try LinearLiteShapeMaterializer(database: db, shapeID: "restart")
    try await reopened.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1, title: "replay-invalid"))]),
      advancingTo: cursor)
    #expect(try await reopened.currentCursor() == cursor)
    #expect(try await reopened.allIssues().first?.title == "First")
  }

  @Test func explicitScopesIsolateOverlaysAndPurgeWithoutTouchingSibling() async throws {
    let db = try database()
    let firstScope = MaterializationScope(
      principal: "account-a", template: "issues:open", subscription: "sub-a", generation: "g1")
    let secondScope = MaterializationScope(
      principal: "account-a", template: "issues:open", subscription: "sub-b", generation: "g1")
    let first = try LinearLiteShapeMaterializer(database: db, scope: firstScope)
    let second = try LinearLiteShapeMaterializer(database: db, scope: secondScope)
    let row = issueRow(id: 7, title: "Shared")
    try await first.apply(
      ChangeBatch([envelope(key: "7", value: row)]), advancingTo: StreamCursor(offset: "a"))
    try await second.apply(
      ChangeBatch([envelope(key: "7", value: row)]), advancingTo: StreamCursor(offset: "b"))
    try await first.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "only-a", rowKey: "7", operation: .update, patch: ["title": .string("A")]))

    #expect(try await first.allIssuesIncludingOverlays().first?.title == "A")
    #expect(try await second.allIssuesIncludingOverlays().first?.title == "Shared")
    try await first.purgeScope()
    #expect(try await first.currentCursor() == nil)
    #expect(try await first.allIssues().isEmpty)
    #expect(
      try await db.read { db in
        try Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM materialization_scopes WHERE scope_key = ?",
          arguments: [firstScope.storageKey])
      } == 0)
    #expect(try await second.currentCursor() == StreamCursor(offset: "b"))
    #expect(try await second.allIssues().first?.title == "Shared")
  }

  @Test func malformedEmptySchemaFailsClosedAndRemainsRecoverable() throws {
    let db = try database()
    try db.write { db in
      try db.execute(sql: "CREATE TABLE issues (id INTEGER PRIMARY KEY NOT NULL)")
    }
    #expect(
      throws: LinearLiteShapeMaterializerError.incompatibleSchema(
        missingColumns: [
          "title", "description", "status", "priority", "username", "project_id", "created",
          "modified", "kanbanorder",
        ])
    ) {
      try LinearLiteShapeMaterializer(database: db, shapeID: "bad-schema")
    }
    let columns = try db.read { db in try db.columns(in: "issues").map(\.name) }
    #expect(columns == ["id"])
    #expect(
      throws: LinearLiteShapeMaterializerError.incompatibleSchema(
        missingColumns: [
          "title", "description", "status", "priority", "username", "project_id", "created",
          "modified", "kanbanorder",
        ])
    ) {
      try LinearLiteShapeMaterializer(database: db, shapeID: "bad-schema-retry")
    }
  }

  @Test func staleCASCursorIsRejectedWithoutRegressingCommittedRows() async throws {
    let db = try database()
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "cas")
    let committed = StreamCursor(offset: "10")
    try await provider.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1))]),
      expecting: nil,
      advancingTo: committed)

    await #expect(
      throws: StreamError.cursorConflict(
        expected: StreamCursor(offset: "9"), actual: committed,
        advancingTo: StreamCursor(offset: "11"))
    ) {
      try await provider.apply(
        ChangeBatch([envelope(key: "2", value: issueRow(id: 2))]),
        expecting: StreamCursor(offset: "9"),
        advancingTo: StreamCursor(offset: "11"))
    }
    #expect(try await provider.currentCursor() == committed)
    #expect(try await provider.allIssues().map(\.id) == [1])
  }

  @Test func sameMutationIDIsIndependentAcrossSiblingScopes() async throws {
    let db = try database()
    let first = try LinearLiteShapeMaterializer(
      database: db,
      scope: MaterializationScope(
        principal: "account", template: "issues", subscription: "first", generation: "g1"))
    let second = try LinearLiteShapeMaterializer(
      database: db,
      scope: MaterializationScope(
        principal: "account", template: "issues", subscription: "second", generation: "g1"))
    let firstOverlay = LinearLiteIssueOverlay(
      mutationID: "same", rowKey: "1", operation: .update, patch: ["title": .string("first")])
    let secondOverlay = LinearLiteIssueOverlay(
      mutationID: "same", rowKey: "1", operation: .update, patch: ["title": .string("second")])
    try await first.saveOverlay(firstOverlay)
    try await second.saveOverlay(secondOverlay)

    let firstStored = try await first.overlays(for: "1")
    let secondStored = try await second.overlays(for: "1")
    #expect(firstStored.map(\.mutationID) == ["same"])
    #expect(secondStored.map(\.mutationID) == ["same"])
    #expect(firstStored.first?.patch == firstOverlay.patch)
    #expect(secondStored.first?.patch == secondOverlay.patch)
  }

  @Test func sameIssueIDCannotLeakAcrossPrincipalPartitionsAndLogoutPurgesAllScopes() async throws {
    let db = try database()
    let firstScope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "sub", generation: "g1")
    let secondScope = MaterializationScope(
      principal: "account-b", template: "issues", subscription: "sub", generation: "g1")
    let first = try LinearLiteShapeMaterializer(database: db, scope: firstScope)
    let second = try LinearLiteShapeMaterializer(database: db, scope: secondScope)
    try await first.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Private A"))]),
      expecting: nil,
      advancingTo: StreamCursor(offset: "a1"))
    try await second.apply(
      ChangeBatch([envelope(key: "42", value: issueRow(id: 42, title: "Private B"))]),
      expecting: nil,
      advancingTo: StreamCursor(offset: "b1"))

    #expect(try await first.allIssues().first?.title == "Private A")
    #expect(try await second.allIssues().first?.title == "Private B")
    try await LinearLiteShapeMaterializer.purgePrincipal("account-a", from: db)
    #expect(try await first.allIssues().isEmpty)
    #expect(try await first.currentCursor() == nil)
    #expect(try await second.allIssues().first?.title == "Private B")
    #expect(
      try await db.read { db in
        try Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM materialization_scopes WHERE principal = 'account-a'")
      } == 0)
  }

  @Test func committedReplaySurvivesClosingAndReopeningFileBackedQueue() async throws {
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(
      UUID().uuidString + ".sqlite")
    defer { try? FileManager.default.removeItem(at: url) }
    let cursor = StreamCursor(offset: "file-1")
    var queue: DatabaseQueue? = try DatabaseQueue(path: url.path)
    var provider: LinearLiteShapeMaterializer? = try LinearLiteShapeMaterializer(
      database: try #require(queue), shapeID: "file")
    try await provider?.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1))]), expecting: nil, advancingTo: cursor
    )
    provider = nil
    queue = nil

    let reopenedQueue = try DatabaseQueue(path: url.path)
    let reopened = try LinearLiteShapeMaterializer(database: reopenedQueue, shapeID: "file")
    try await reopened.apply(
      ChangeBatch([envelope(key: "1", operation: .delete)]), expecting: cursor, advancingTo: cursor)
    #expect(try await reopened.currentCursor() == cursor)
    #expect(try await reopened.allIssues().map(\.id) == [1])
  }

  @Test func v5UpgradeRollsBackOnLateV8FailureThenRetriesCleanly() async throws {
    let db = try database()
    try LinearLiteShapeMaterializer.migrate(db, through: .v5)
    try await db.write { db in
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified,
             kanbanorder, last_lsn, client_id)
          VALUES (1, 'v5', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, NULL, NULL)
          """)
      try db.execute(sql: "INSERT INTO shape_cursors VALUES ('legacy-v5', '5', NULL)")
      try db.execute(sql: "INSERT INTO subset_view_members VALUES ('legacy-v5', 1, NULL)")
      // This collision occurs late in v8 after it has rebuilt issues and memberships. GRDB must
      // roll that work back, leaving the v5 schema retryable.
      try db.execute(sql: "CREATE TABLE issue_overlays_partitioned (sentinel TEXT)")
    }

    #expect(throws: (any Error).self) {
      try LinearLiteShapeMaterializer(database: db, shapeID: "legacy-v5")
    }
    #expect(
      try await db.read { db in
        try Bool.fetchOne(
          db,
          sql:
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('issues') WHERE name = 'principal_id')")
      } == false)
    try await db.write { db in try db.execute(sql: "DROP TABLE issue_overlays_partitioned") }
    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: "legacy-v5")
    #expect(try await provider.currentCursor() == StreamCursor(offset: "5"))
    #expect(try await provider.allIssues().first?.title == "v5")
  }

  @Test func populatedV7RegisteredScopeKeepsRowsAndCursorThroughV8Reopen() async throws {
    let db = try database()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "sub-a", generation: "g1")
    try LinearLiteShapeMaterializer.migrate(db, through: .v7)
    try await db.write { db in
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified,
             kanbanorder, last_lsn, client_id)
          VALUES (7, 'kept', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, NULL, NULL)
          """
      )
      try db.execute(
        sql: "INSERT INTO shape_cursors VALUES (?, '7', NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO subset_view_members VALUES (?, 7, NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO materialization_scopes VALUES (?, ?, ?, ?, ?)",
        arguments: [
          scope.storageKey, scope.principal, scope.template, scope.subscription, scope.generation,
        ])
    }

    let reopened = try LinearLiteShapeMaterializer(database: db, scope: scope)
    #expect(try await reopened.currentCursor() == StreamCursor(offset: "7"))
    #expect(try await reopened.allIssues().map(\.id) == [7])
    #expect(try await reopened.allIssues().first?.title == "kept")
  }

  @Test func v7CompatibleCallerIssueColumnAndIndexSurviveV8Rebuild() async throws {
    let db = try database()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "caller-v7", generation: "g1")
    try LinearLiteShapeMaterializer.migrate(db, through: .v7)
    try await db.write { db in
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified,
             kanbanorder, last_lsn, client_id)
          VALUES (7, 'kept', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, NULL, NULL)
          """)
      try db.execute(
        sql: "INSERT INTO shape_cursors VALUES (?, '7', NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO subset_view_members VALUES (?, 7, NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO materialization_scopes VALUES (?, ?, ?, ?, ?)",
        arguments: [
          scope.storageKey, scope.principal, scope.template, scope.subscription, scope.generation,
        ])
      try db.execute(
        sql: """
          INSERT INTO issue_overlays
            (mutation_id, row_key, operation, patch_json, status, request_id, created_at, scope_id)
          VALUES ('caller-overlay', '7', 'update', X'7B7D', 'pending', NULL, 1, ?)
          """, arguments: [scope.storageKey])
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN caller_note TEXT")
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN caller_rank INTEGER NOT NULL DEFAULT 0")
      try db.execute(sql: "UPDATE issues SET caller_note = 'preserve me' WHERE id = 7")
      try db.execute(sql: "UPDATE issues SET caller_rank = 9 WHERE id = 7")
      try db.execute(sql: "CREATE INDEX caller_owned_issue_note ON issues(caller_note)")
    }

    let reopened = try LinearLiteShapeMaterializer(database: db, scope: scope)
    #expect(try await reopened.currentCursor() == StreamCursor(offset: "7"))
    #expect(try await reopened.allIssues().map(\.id) == [7])
    #expect(try await reopened.overlays(for: "7").map(\.mutationID) == ["caller-overlay"])
    #expect(
      try await db.read { db in try String.fetchOne(db, sql: "SELECT caller_note FROM issues") }
        == "preserve me")
    #expect(
      try await db.read { db in try Int.fetchOne(db, sql: "SELECT caller_rank FROM issues") } == 9)
    #expect(
      try await db.read { db in
        try Int.fetchOne(
          db, sql: "SELECT \"notnull\" FROM pragma_table_info('issues') WHERE name = 'caller_rank'")
      } == 1)
    #expect(
      try await db.read { db in
        try Bool.fetchOne(
          db,
          sql:
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'caller_owned_issue_note')"
        )
      } == true)
  }

  @Test func incompatibleV7CallerIndexFailsClosedAndCanBeCorrectedThenRetried() async throws {
    let db = try database()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "caller-v7-retry", generation: "g1")
    try LinearLiteShapeMaterializer.migrate(db, through: .v7)
    try await db.write { db in
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified,
             kanbanorder, last_lsn, client_id)
          VALUES (8, 'retry', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, NULL, NULL)
          """)
      try db.execute(
        sql: "INSERT INTO shape_cursors VALUES (?, '8', NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO subset_view_members VALUES (?, 8, NULL)", arguments: [scope.storageKey])
      try db.execute(
        sql: "INSERT INTO materialization_scopes VALUES (?, ?, ?, ?, ?)",
        arguments: [
          scope.storageKey, scope.principal, scope.template, scope.subscription, scope.generation,
        ])
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN caller_state TEXT")
      try db.execute(sql: "UPDATE issues SET caller_state = 'retry me' WHERE id = 8")
      try db.execute(sql: "CREATE UNIQUE INDEX caller_unique_state ON issues(caller_state)")
    }

    #expect(
      throws: LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(
        name: "caller_unique_state")
    ) {
      try LinearLiteShapeMaterializer(database: db, scope: scope)
    }
    #expect(
      try await db.read { db in
        try Bool.fetchOne(
          db,
          sql:
            "SELECT EXISTS(SELECT 1 FROM pragma_table_info('issues') WHERE name = 'principal_id')"
        )
      } == false)
    #expect(
      try await db.read { db in
        try String.fetchOne(db, sql: "SELECT caller_state FROM issues WHERE id = 8")
      } == "retry me")
    #expect(
      try await db.read { db in
        try String.fetchOne(
          db, sql: "SELECT offset FROM shape_cursors WHERE shape_id = ?",
          arguments: [scope.storageKey])
      } == "8")

    try await db.write { db in try db.execute(sql: "DROP INDEX caller_unique_state") }
    let reopened = try LinearLiteShapeMaterializer(database: db, scope: scope)
    #expect(try await reopened.currentCursor() == StreamCursor(offset: "8"))
    #expect(try await reopened.allIssues().map(\.id) == [8])
    #expect(
      try await db.read { db in
        try String.fetchOne(db, sql: "SELECT caller_state FROM issues WHERE id = 8")
      } == "retry me")
  }

  @Test(
    "every supported populated schema upgrades through the production migration history",
    arguments: LinearLiteShapeMaterializer.SchemaVersion.allCases.filter { $0 != .v1 })
  private func supportedPopulatedHistoryPreservesRowsMembershipsAndCursor(
    _ version: LinearLiteShapeMaterializer.SchemaVersion
  ) async throws {
    let db = try database()
    let shapeID = "historical-\(version.rawValue)"
    try LinearLiteShapeMaterializer.migrate(db, through: version)
    try await db.write { db in
      try db.execute(
        sql: "INSERT INTO shape_cursors (shape_id, offset, lsn) VALUES (?, ?, ?)",
        arguments: [shapeID, "cursor-\(version.rawValue)", "0/\(version.rawValue)"])
      if version == .v2 {
        try db.execute(
          sql: """
            INSERT INTO issues
              (shape_id, id, title, description, status, priority, username, project_id, created,
               modified, kanbanorder)
            VALUES (?, 1, 'historical', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5)
            """, arguments: [shapeID])
      } else if version == .v8 {
        try db.execute(
          sql: """
            INSERT INTO issues
              (principal_id, id, client_id, title, description, status, priority, username, project_id,
               created, modified, kanbanorder, last_lsn)
            VALUES ('legacy', 1, NULL, 'historical', 'details', 'backlog', 'high', 'ada', 7, 1, 2,
                    1.5, '0/1')
            """)
        try db.execute(
          sql: """
            INSERT INTO subset_view_members (view_id, principal_id, issue_id, row_lsn)
            VALUES (?, 'legacy', 1, '0/1')
            """, arguments: [shapeID])
      } else if version.rawValue < LinearLiteShapeMaterializer.SchemaVersion.v5.rawValue {
        try db.execute(
          sql: """
            INSERT INTO issues
              (id, title, description, status, priority, username, project_id, created, modified,
               kanbanorder, last_lsn)
            VALUES (1, 'historical', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, '0/1')
            """)
        try db.execute(
          sql: "INSERT INTO subset_view_members (view_id, issue_id, row_lsn) VALUES (?, 1, '0/1')",
          arguments: [shapeID])
      } else {
        try db.execute(
          sql: """
            INSERT INTO issues
              (id, client_id, title, description, status, priority, username, project_id, created,
               modified, kanbanorder, last_lsn)
            VALUES (1, NULL, 'historical', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5, '0/1')
            """)
        try db.execute(
          sql: "INSERT INTO subset_view_members (view_id, issue_id, row_lsn) VALUES (?, 1, '0/1')",
          arguments: [shapeID])
      }
      if version == .v7 {
        try db.execute(
          sql: """
            INSERT INTO issue_overlays
              (mutation_id, row_key, operation, patch_json, status, request_id, created_at, scope_id)
            VALUES ('historical-overlay', '1', 'update', X'7B7D', 'pending', NULL, 1, ?)
            """, arguments: [shapeID])
      }
    }

    let provider = try LinearLiteShapeMaterializer(database: db, shapeID: shapeID)
    #expect(
      try await provider.currentCursor()
        == StreamCursor(
          offset: "cursor-\(version.rawValue)", lsn: "0/\(version.rawValue)"))
    #expect(try await provider.allIssues().map(\.title) == ["historical"])
    #expect(
      try await db.read { db in
        try Int.fetchOne(
          db,
          sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ? AND issue_id = 1",
          arguments: [shapeID])
      } == 1)
    if version == .v7 {
      #expect(try await provider.overlays(for: "1").map(\.mutationID) == ["historical-overlay"])
    }
  }

  @Test func populatedV1FailsClosedAndThePriorDatabaseRemainsRecoverable() throws {
    let db = try database()
    try LinearLiteShapeMaterializer.migrate(db, through: .v1)
    try db.write { db in
      try db.execute(
        sql: """
          INSERT INTO issues
            (id, title, description, status, priority, username, project_id, created, modified, kanbanorder)
          VALUES (1, 'unscoped', 'details', 'backlog', 'high', 'ada', 7, 1, 2, 1.5)
          """)
    }

    for shapeID in ["first-reseed-attempt", "retry-reseed-attempt"] {
      #expect(throws: LinearLiteShapeMaterializerError.unscopedRowsRequireReseed(count: 1)) {
        try LinearLiteShapeMaterializer(database: db, shapeID: shapeID)
      }
    }
    #expect(
      try db.read { db in try String.fetchOne(db, sql: "SELECT title FROM issues") } == "unscoped")
    #expect(try db.read { db in try db.columns(in: "issues").map(\.name) }.contains("id"))
  }

  @Test func compatibleCallerColumnAndIndexSurviveProviderReopenWithoutResettingState() async throws
  {
    let db = try database()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "caller-compatible",
      generation: "g1")
    let provider = try LinearLiteShapeMaterializer(database: db, scope: scope)
    let cursor = StreamCursor(offset: "caller-compatible-1", lsn: "0/1")
    try await provider.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1, title: "preserved"))]), expecting: nil,
      advancingTo: cursor)
    try await provider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "caller-overlay", rowKey: "1", operation: .update,
        patch: ["title": .string("overlay preserved")]))
    try await db.write { db in
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN caller_note TEXT")
      try db.execute(sql: "CREATE INDEX caller_owned_issue_title ON issues(title)")
      try db.execute(
        sql: "UPDATE issues SET caller_note = 'owned' WHERE principal_id = 'account-a'")
    }

    let reopened = try LinearLiteShapeMaterializer(database: db, scope: scope)
    #expect(try await reopened.currentCursor() == cursor)
    #expect(try await reopened.allIssues().map(\.title) == ["preserved"])
    #expect(try await reopened.overlays(for: "1").map(\.mutationID) == ["caller-overlay"])
    #expect(
      try await db.read { db in
        try String.fetchOne(
          db, sql: "SELECT caller_note FROM issues WHERE principal_id = 'account-a'")
      } == "owned")
    #expect(
      try await db.read { db in
        try Bool.fetchOne(
          db,
          sql:
            "SELECT EXISTS(SELECT 1 FROM sqlite_master WHERE type = 'index' AND name = 'caller_owned_issue_title')"
        )
      } == true)
  }

  @Test func currentSchemaExposesTheProviderIndexesAndKeepsForeignKeysAsConnectionPolicy() throws {
    let db = try database()
    _ = try LinearLiteShapeMaterializer(database: db, shapeID: "index-check")
    let indexes = try db.read { db in
      try String.fetchAll(
        db, sql: "SELECT name FROM sqlite_master WHERE type = 'index' ORDER BY name")
    }
    #expect(indexes.contains("idx_issues_principal_client_id"))
    #expect(indexes.contains("idx_subset_view_members_principal_issue"))
    #expect(indexes.contains("idx_issue_overlays_scope_row"))
    #expect(LinearLiteShapeMaterializer.databaseConfiguration.foreignKeysEnabled)
  }

  @Test func walSnapshotReaderKeepsCommittedPrefixWhileProviderCommitsTheNextBatch() async throws {
    let url = FileManager.default.temporaryDirectory.appendingPathComponent(
      "linearlite-wal-reader-\(UUID().uuidString).sqlite")
    defer {
      for suffix in ["", "-wal", "-shm"] {
        try? FileManager.default.removeItem(atPath: url.path + suffix)
      }
    }
    let writer = try DatabaseQueue(
      path: url.path, configuration: LinearLiteShapeMaterializer.databaseConfiguration)
    let provider = try LinearLiteShapeMaterializer(database: writer, shapeID: "wal-reader")
    let firstCursor = StreamCursor(offset: "wal-1", lsn: "0/1")
    try await provider.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1, title: "before"))]), expecting: nil,
      advancingTo: firstCursor)

    let reader = try DatabasePool(
      path: url.path, configuration: LinearLiteShapeMaterializer.databaseConfiguration)
    let committedPrefix = try reader.makeSnapshot()
    let prefix = try await committedPrefix.read { db in
      (
        try String.fetchOne(
          db, sql: "SELECT title FROM issues WHERE principal_id = 'legacy' AND id = 1"),
        try String.fetchOne(
          db, sql: "SELECT offset FROM shape_cursors WHERE shape_id = 'wal-reader'")
      )
    }
    #expect(prefix.0 == "before")
    #expect(prefix.1 == firstCursor.offset)

    let secondCursor = StreamCursor(offset: "wal-2", lsn: "0/2")
    try await provider.apply(
      ChangeBatch([envelope(key: "1", value: issueRow(id: 1, title: "after"), lsn: "0/2")]),
      expecting: firstCursor, advancingTo: secondCursor)

    let heldPrefix = try await committedPrefix.read { db in
      (
        try String.fetchOne(
          db, sql: "SELECT title FROM issues WHERE principal_id = 'legacy' AND id = 1"),
        try String.fetchOne(
          db, sql: "SELECT offset FROM shape_cursors WHERE shape_id = 'wal-reader'")
      )
    }
    let current = try await reader.read { db in
      (
        try String.fetchOne(
          db, sql: "SELECT title FROM issues WHERE principal_id = 'legacy' AND id = 1"),
        try String.fetchOne(
          db, sql: "SELECT offset FROM shape_cursors WHERE shape_id = 'wal-reader'")
      )
    }
    #expect(heldPrefix.0 == "before")
    #expect(heldPrefix.1 == firstCursor.offset)
    #expect(current.0 == "after")
    #expect(current.1 == secondCursor.offset)
  }
}
