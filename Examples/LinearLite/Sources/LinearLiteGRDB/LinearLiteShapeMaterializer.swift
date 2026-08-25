import ElectricCircuitsSwift
import Foundation
import GRDB

public struct Issue: Codable, FetchableRecord, PersistableRecord, Equatable, Sendable {
  public var id: Int64
  /// Stable client-generated UUID carried alongside the server-assigned numeric primary key.
  /// Existing databases/rows may be nil until the server schema is migrated.
  public var clientID: String?
  public var title: String
  public var description: String
  public var status: String
  public var priority: String
  public var username: String
  public var projectID: Int64
  public var created: Int64
  public var modified: Int64
  public var kanbanOrder: Double

  public static let databaseTableName = "issues"

  public init(
    id: Int64,
    clientID: String? = nil,
    title: String,
    description: String,
    status: String,
    priority: String,
    username: String, projectID: Int64, created: Int64, modified: Int64, kanbanOrder: Double
  ) {
    self.id = id
    self.clientID = clientID
    self.title = title
    self.description = description
    self.status = status
    self.priority = priority
    self.username = username
    self.projectID = projectID
    self.created = created
    self.modified = modified
    self.kanbanOrder = kanbanOrder
  }

  enum CodingKeys: String, CodingKey {
    case id
    case clientID = "client_id"
    case title, description, status, priority, username
    case projectID = "project_id"
    case created, modified
    case kanbanOrder = "kanbanorder"
  }
}

/// A client-owned write rendered over authoritative rows until the server confirms or rejects it.
/// The row key is a string so an optimistic insert can use its UUIDv4 `client_id` before Postgres
/// assigns its numeric primary key.
public struct LinearLiteIssueOverlay: Codable, Equatable, Sendable {
  public enum Operation: String, Codable, Sendable {
    case insert
    case update
    case delete
  }

  public enum Status: String, Codable, Sendable {
    case pending
    case acknowledged
    case rejected
  }

  public let mutationID: String
  public let rowKey: String
  public let operation: Operation
  public let patch: [String: JSONValue]
  public let status: Status
  public let requestID: String?
  public let createdAt: Date

  public init(
    mutationID: String,
    rowKey: String,
    operation: Operation,
    patch: [String: JSONValue],
    status: Status = .pending,
    requestID: String? = nil,
    createdAt: Date = .now
  ) {
    self.mutationID = mutationID
    self.rowKey = rowKey
    self.operation = operation
    self.patch = patch
    self.status = status
    self.requestID = requestID
    self.createdAt = createdAt
  }
}

public enum LinearLiteShapeMaterializerError: Error, Equatable, Sendable {
  case missingValue(key: String)
  case malformedValue(key: String, detail: String)
  case invalidDeleteKey(key: String)
  case keyMismatch(key: String, id: Int64)
  case unscopedRowsRequireReseed(count: Int)
  case conflictingShapeRowsRequireReseed(ids: [Int64])
  case malformedSnapshotRow(index: Int, detail: String)
  case malformedOverlay(mutationID: String, detail: String)
  case incompatibleSchema(missingColumns: [String])
  case incompatibleCallerIssueExtension(name: String)
  case scopeCollision(key: String)
}

public enum MaterializationFault: Error, Equatable, Sendable {
  case injected(phase: Phase)

  public enum Phase: String, Equatable, Sendable {
    case afterFirstRow
    case beforeCursorCommit
  }
}

/// A durable materializer for one subscribed view. Canonical issue rows are shared by every view;
/// `subset_view_members` records which views currently include each row. Actor isolation
/// serializes materialization requests so concurrent callers cannot commit rows and cursors out of
/// order.
public actor LinearLiteShapeMaterializer: ShapeMaterializer {
  /// File-backed application databases use WAL so an independent reader can observe the last
  /// committed materialization while the writer commits the next batch. Foreign keys remain
  /// enabled as a connection policy; this versioned schema deliberately has no declared foreign
  /// keys because legacy/unregistered scopes must remain recoverable during v1...v8 upgrades.
  public static var databaseConfiguration: Configuration {
    var configuration = Configuration()
    configuration.journalMode = .wal
    configuration.foreignKeysEnabled = true
    return configuration
  }

  /// Internal migration cut points let the compatibility corpus construct exact predecessor
  /// databases from these same registrations. The runtime always migrates through v8.
  enum SchemaVersion: Int, CaseIterable, Sendable {
    case v1 = 1
    case v2, v3, v4, v5, v6, v7, v8

    fileprivate var migrationIdentifier: String {
      switch self {
      case .v1: "linear_lite_v1"
      case .v2: "linear_lite_v2_shape_scoped_issues"
      case .v3: "linear_lite_v3_canonical_issue_views"
      case .v4: "linear_lite_v4_issue_lsn"
      case .v5: "linear_lite_v5_issue_client_id"
      case .v6: "linear_lite_v6_materialization_scopes"
      case .v7: "linear_lite_v7_overlay_scope"
      case .v8: "linear_lite_v8_principal_partitions"
      }
    }
  }

  private struct CompatibleIssueColumn {
    let name: String
    let declaration: String
  }

  private struct CompatibleIssueIndex {
    let name: String
    let columns: [String]
  }

  private let database: DatabaseQueue
  private let faultInjector: (@Sendable (MaterializationFault.Phase) throws -> Void)?
  private let availability: any MaterializerAvailabilityProbe
  public let shapeID: String
  private let principalID: String
  public let materializationScope: MaterializationScope?

  public init(
    database: DatabaseQueue,
    shapeID: String,
    faultInjector: (@Sendable (MaterializationFault.Phase) throws -> Void)? = nil,
    availability: any MaterializerAvailabilityProbe = AlwaysAvailableMaterializerAvailability()
  ) throws {
    self.database = database
    self.shapeID = shapeID
    self.principalID = "legacy"
    self.materializationScope = nil
    self.faultInjector = faultInjector
    self.availability = availability
    try Self.migrate(database)
  }

  /// Initializes a provider with an explicit principal/query/subscription/generation scope. The
  /// encoded scope key is used for all view membership and cursor rows, preventing an account
  /// switch or feed reset from reusing an old materialization accidentally.
  public init(
    database: DatabaseQueue,
    scope: MaterializationScope,
    faultInjector: (@Sendable (MaterializationFault.Phase) throws -> Void)? = nil,
    availability: any MaterializerAvailabilityProbe = AlwaysAvailableMaterializerAvailability()
  ) throws {
    self.database = database
    self.shapeID = scope.storageKey
    self.principalID = scope.principal
    self.materializationScope = scope
    self.faultInjector = faultInjector
    self.availability = availability
    try Self.migrate(database)
    try database.write { db in
      let existing = try Row.fetchOne(
        db,
        sql:
          "SELECT principal, template, subscription, generation FROM materialization_scopes WHERE scope_key = ?",
        arguments: [scope.storageKey])
      if let existing,
        String(existing["principal"]) != scope.principal
          || String(existing["template"]) != scope.template
          || String(existing["subscription"]) != scope.subscription
          || String(existing["generation"]) != scope.generation
      {
        throw LinearLiteShapeMaterializerError.scopeCollision(key: scope.storageKey)
      }
      try db.execute(
        sql: """
          INSERT INTO materialization_scopes (scope_key, principal, template, subscription, generation)
          VALUES (?, ?, ?, ?, ?)
          ON CONFLICT(scope_key) DO NOTHING
          """,
        arguments: [
          scope.storageKey, scope.principal, scope.template, scope.subscription, scope.generation,
        ])
    }
  }

  /// Runs the versioned schema migration used by the example provider.
  public static func migrate(_ database: DatabaseQueue) throws {
    try migrate(database, through: .v8)
  }

  /// Internal only: compatibility tests use the production migration registrations, not copied
  /// SQL layouts or fabricated migration records.
  static func migrate(_ database: DatabaseQueue, through version: SchemaVersion) throws {
    var migrator = DatabaseMigrator()
    migrator.registerMigration("linear_lite_v1") { db in
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS issues (
            id INTEGER PRIMARY KEY NOT NULL,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            username TEXT NOT NULL,
            project_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            kanbanorder REAL NOT NULL,
            last_lsn TEXT
          )
          """)
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS shape_cursors (
            shape_id TEXT PRIMARY KEY NOT NULL,
            offset TEXT NOT NULL,
            lsn TEXT
          )
          """)
    }
    migrator.registerMigration("linear_lite_v2_shape_scoped_issues") { db in
      let columns = try db.columns(in: "issues")
      if columns.contains(where: { $0.name == "shape_id" }) {
        // This is already the scoped schema (for example, a freshly created database after v1
        // has been revised). Keep the migration idempotent.
        return
      }

      let rowCount = try Int.fetchOne(db, sql: "SELECT COUNT(*) FROM issues") ?? 0
      guard rowCount == 0 else {
        // Old rows have no shape identity. Never guess which shape owns them: the example must be
        // reseeded (or the old database replaced) before the scoped schema can be installed.
        throw LinearLiteShapeMaterializerError.unscopedRowsRequireReseed(count: rowCount)
      }

      let requiredLegacyColumns = [
        "id", "title", "description", "status", "priority", "username", "project_id",
        "created", "modified", "kanbanorder",
      ]
      let present = Set(columns.map(\.name))
      let missing = requiredLegacyColumns.filter { !present.contains($0) }
      guard missing.isEmpty else {
        throw LinearLiteShapeMaterializerError.incompatibleSchema(missingColumns: missing)
      }

      try db.execute(sql: "DROP TABLE issues")
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
    }
    migrator.registerMigration("linear_lite_v3_canonical_issue_views") { db in
      let columns = try db.columns(in: "issues")
      let hasShapeID = columns.contains { $0.name == "shape_id" }
      var migratedMemberships: [(viewID: String, issueID: Int64)] = []

      if hasShapeID {
        // v2 duplicated rows per shape. Preserve the old table while moving equal copies into a
        // canonical row table and recording view membership separately.
        let rows = try Row.fetchAll(
          db,
          sql: """
            SELECT shape_id, id, title, description, status, priority, username, project_id,
                   created, modified, kanbanorder
            FROM issues
            ORDER BY shape_id, id
            """)
        migratedMemberships = rows.map { (viewID: $0["shape_id"], issueID: $0["id"]) }
        var canonical: [Int64: Row] = [:]
        var conflicts = Set<Int64>()
        for row in rows {
          let id: Int64 = row["id"]
          if let previous = canonical[id] {
            let fields = [
              "title", "description", "status", "priority", "username", "project_id",
              "created", "modified", "kanbanorder",
            ]
            if fields.contains(where: {
              String(describing: previous[$0]) != String(describing: row[$0])
            }) {
              conflicts.insert(id)
            }
          } else {
            canonical[id] = row
          }
        }
        guard conflicts.isEmpty else {
          throw
            LinearLiteShapeMaterializerError
            .conflictingShapeRowsRequireReseed(ids: conflicts.sorted())
        }

        try db.execute(
          sql: """
            CREATE TABLE issues_canonical (
              id INTEGER PRIMARY KEY NOT NULL,
              title TEXT NOT NULL,
              description TEXT NOT NULL,
              status TEXT NOT NULL,
              priority TEXT NOT NULL,
              username TEXT NOT NULL,
              project_id INTEGER NOT NULL,
              created INTEGER NOT NULL,
              modified INTEGER NOT NULL,
              kanbanorder REAL NOT NULL,
              last_lsn TEXT
            )
            """)
        for row in canonical.values {
          try db.execute(
            sql: """
              INSERT INTO issues_canonical
                (id, title, description, status, priority, username, project_id, created, modified,
                 kanbanorder, last_lsn)
              VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, NULL)
              """,
            arguments: [
              row["id"], row["title"], row["description"], row["status"], row["priority"],
              row["username"], row["project_id"], row["created"], row["modified"],
              row["kanbanorder"],
            ])
        }
        try db.execute(sql: "ALTER TABLE issues RENAME TO issues_shape_scoped_legacy")
        try db.execute(sql: "ALTER TABLE issues_canonical RENAME TO issues")
      }

      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS subset_view_members (
            view_id TEXT NOT NULL,
            issue_id INTEGER NOT NULL,
            row_lsn TEXT,
            PRIMARY KEY (view_id, issue_id)
          )
          """)
      try db.execute(
        sql:
          "CREATE INDEX IF NOT EXISTS idx_subset_view_members_issue ON subset_view_members(issue_id)"
      )
      for membership in migratedMemberships {
        try db.execute(
          sql:
            "INSERT OR IGNORE INTO subset_view_members (view_id, issue_id, row_lsn) VALUES (?, ?, NULL)",
          arguments: [membership.viewID, membership.issueID])
      }
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS issue_overlays (
            mutation_id TEXT PRIMARY KEY NOT NULL,
            row_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            patch_json BLOB NOT NULL,
            status TEXT NOT NULL,
            request_id TEXT,
            created_at REAL NOT NULL
          )
          """)
      try db.execute(
        sql: "CREATE INDEX IF NOT EXISTS idx_issue_overlays_row ON issue_overlays(row_key)")
    }
    migrator.registerMigration("linear_lite_v4_issue_lsn") { db in
      let columns = try db.columns(in: "issues")
      guard !columns.contains(where: { $0.name == "last_lsn" }) else { return }
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN last_lsn TEXT")
    }
    migrator.registerMigration("linear_lite_v5_issue_client_id") { db in
      let columns = try db.columns(in: "issues")
      guard !columns.contains(where: { $0.name == "client_id" }) else { return }
      try db.execute(sql: "ALTER TABLE issues ADD COLUMN client_id TEXT")
      try db.execute(
        sql: "CREATE UNIQUE INDEX IF NOT EXISTS idx_issues_client_id ON issues(client_id)")
    }
    migrator.registerMigration("linear_lite_v6_materialization_scopes") { db in
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS materialization_scopes (
            scope_key TEXT PRIMARY KEY NOT NULL,
            principal TEXT NOT NULL,
            template TEXT NOT NULL,
            subscription TEXT NOT NULL,
            generation TEXT NOT NULL
          )
          """)
    }
    migrator.registerMigration("linear_lite_v7_overlay_scope") { db in
      let columns = try db.columns(in: "issue_overlays")
      guard !columns.contains(where: { $0.name == "scope_id" }) else { return }
      try db.execute(
        sql: "ALTER TABLE issue_overlays ADD COLUMN scope_id TEXT NOT NULL DEFAULT 'legacy'")
      try db.execute(
        sql:
          "CREATE INDEX IF NOT EXISTS idx_issue_overlays_scope_row ON issue_overlays(scope_id, row_key)"
      )
    }
    migrator.registerMigration("linear_lite_v8_principal_partitions") { db in
      // Rebuild instead of altering primary keys: SQLite cannot change a table's primary key in
      // place. Existing unregistered materializations are retained under the explicit legacy
      // partition; registered scope rows carry their own principal. A caller may extend the v7
      // table with additive nullable/defaulted columns and ordinary indexes. Preserve only that
      // narrow, data-compatible subset; table constraints, generated columns, triggers, and
      // provider-reserved names are deliberately not copied into a schema rebuild.
      let extensions = try compatibleIssueExtensions(in: db)
      let extensionDefinitions = extensions.columns.map { column in
        "            \(quotedIdentifier(column.name)) \(column.declaration)"
      }.joined(separator: ",\n")
      let extensionTableSuffix = extensionDefinitions.isEmpty ? "" : ",\n\(extensionDefinitions)"
      let extensionNames = extensions.columns.map(\.name)
      let extensionInsertColumns = extensionNames.map(quotedIdentifier).joined(separator: ", ")
      let extensionSelectColumns = extensionNames.map { "issues.\(quotedIdentifier($0))" }.joined(
        separator: ", ")
      let extensionInsertSuffix =
        extensionInsertColumns.isEmpty ? "" : ", \(extensionInsertColumns)"
      let extensionSelectSuffix =
        extensionSelectColumns.isEmpty ? "" : ", \(extensionSelectColumns)"
      try db.execute(
        sql: """
          CREATE TABLE issues_partitioned (
            principal_id TEXT NOT NULL,
            id INTEGER NOT NULL,
            client_id TEXT,
            title TEXT NOT NULL,
            description TEXT NOT NULL,
            status TEXT NOT NULL,
            priority TEXT NOT NULL,
            username TEXT NOT NULL,
            project_id INTEGER NOT NULL,
            created INTEGER NOT NULL,
            modified INTEGER NOT NULL,
            kanbanorder REAL NOT NULL,
            last_lsn TEXT\(extensionTableSuffix),
            PRIMARY KEY (principal_id, id)
          )
          """)
      try db.execute(
        sql: """
          INSERT INTO issues_partitioned
            (principal_id, id, client_id, title, description, status, priority, username, project_id,
             created, modified, kanbanorder, last_lsn\(extensionInsertSuffix))
          SELECT DISTINCT COALESCE(scopes.principal, 'legacy'), issues.id, issues.client_id,
                 issues.title, issues.description, issues.status, issues.priority, issues.username,
                 issues.project_id, issues.created, issues.modified, issues.kanbanorder, issues.last_lsn\(extensionSelectSuffix)
          FROM issues
          LEFT JOIN subset_view_members members ON members.issue_id = issues.id
          LEFT JOIN materialization_scopes scopes ON scopes.scope_key = members.view_id
          """)
      try db.execute(sql: "DROP TABLE issues")
      try db.execute(sql: "ALTER TABLE issues_partitioned RENAME TO issues")
      try db.execute(
        sql: "CREATE UNIQUE INDEX idx_issues_principal_client_id ON issues(principal_id, client_id)"
      )
      for index in extensions.indexes {
        try db.execute(
          sql:
            "CREATE INDEX \(quotedIdentifier(index.name)) ON issues (\(index.columns.map(quotedIdentifier).joined(separator: ", ")))"
        )
      }

      try db.execute(
        sql: """
          CREATE TABLE subset_view_members_partitioned (
            view_id TEXT NOT NULL,
            principal_id TEXT NOT NULL,
            issue_id INTEGER NOT NULL,
            row_lsn TEXT,
            PRIMARY KEY (view_id, issue_id)
          )
          """)
      try db.execute(
        sql: """
          INSERT INTO subset_view_members_partitioned (view_id, principal_id, issue_id, row_lsn)
          SELECT members.view_id, COALESCE(scopes.principal, 'legacy'), members.issue_id, members.row_lsn
          FROM subset_view_members members
          LEFT JOIN materialization_scopes scopes ON scopes.scope_key = members.view_id
          """)
      try db.execute(sql: "DROP TABLE subset_view_members")
      try db.execute(
        sql: "ALTER TABLE subset_view_members_partitioned RENAME TO subset_view_members")
      try db.execute(
        sql:
          "CREATE INDEX idx_subset_view_members_principal_issue ON subset_view_members(principal_id, issue_id)"
      )

      try db.execute(
        sql: """
          CREATE TABLE issue_overlays_partitioned (
            scope_id TEXT NOT NULL,
            mutation_id TEXT NOT NULL,
            row_key TEXT NOT NULL,
            operation TEXT NOT NULL,
            patch_json BLOB NOT NULL,
            status TEXT NOT NULL,
            request_id TEXT,
            created_at REAL NOT NULL,
            PRIMARY KEY (scope_id, mutation_id)
          )
          """)
      try db.execute(
        sql: """
          INSERT INTO issue_overlays_partitioned
            (scope_id, mutation_id, row_key, operation, patch_json, status, request_id, created_at)
          SELECT scope_id, mutation_id, row_key, operation, patch_json, status, request_id, created_at
          FROM issue_overlays
          """)
      try db.execute(sql: "DROP TABLE issue_overlays")
      try db.execute(sql: "ALTER TABLE issue_overlays_partitioned RENAME TO issue_overlays")
      try db.execute(
        sql: "CREATE INDEX idx_issue_overlays_scope_row ON issue_overlays(scope_id, row_key)")
    }
    try migrator.migrate(database, upTo: version.migrationIdentifier)
  }

  /// Returns the small caller-owned extension surface that can survive the v8 table rebuild.
  ///
  /// The contract intentionally admits only additive, non-generated nullable columns or `NOT
  /// NULL` columns with a SQLite default, plus non-unique/non-partial ascending BINARY
  /// simple-column indexes over retained columns. Unsupported additions fail closed so the v7
  /// database can be corrected and retried; recreating arbitrary constraints, expressions,
  /// collations, or triggers would make this provider an unsafe general SQLite schema migrator.
  private static func compatibleIssueExtensions(
    in db: Database
  ) throws -> (columns: [CompatibleIssueColumn], indexes: [CompatibleIssueIndex]) {
    let reservedColumns: Set<String> = [
      "principal_id", "id", "client_id", "title", "description", "status", "priority", "username",
      "project_id", "created", "modified", "kanbanorder", "last_lsn",
    ]
    if let trigger = try String.fetchOne(
      db,
      sql: "SELECT name FROM sqlite_master WHERE type = 'trigger' AND tbl_name = 'issues' LIMIT 1")
    {
      throw LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(name: trigger)
    }
    let definition =
      try String.fetchOne(
        db, sql: "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = 'issues'") ?? ""
    for unsupportedKeyword in [
      "CHECK", "COLLATE", "CONSTRAINT", "GENERATED", "REFERENCES", "UNIQUE",
    ] {
      guard definition.range(of: unsupportedKeyword, options: .caseInsensitive) == nil else {
        throw LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(
          name: "issues:\(unsupportedKeyword.lowercased())")
      }
    }

    let hiddenColumns = try Row.fetchAll(db, sql: "PRAGMA table_xinfo(issues)").reduce(
      into: [String: Int]()
    ) {
      result, row in
      let name: String = row["name"]
      let hidden: Int = row["hidden"]
      result[name] = hidden
    }
    let columns = try db.columns(in: "issues").compactMap {
      column throws -> CompatibleIssueColumn? in
      guard !reservedColumns.contains(column.name) else { return nil }
      guard
        isCompatibleIdentifier(column.name),
        hiddenColumns[column.name] == 0,
        column.primaryKeyIndex == 0,
        !column.type.isEmpty,
        !column.isNotNull || column.defaultValueSQL != nil
      else {
        throw LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(name: column.name)
      }
      let notNullClause = column.isNotNull ? " NOT NULL" : ""
      let defaultClause = column.defaultValueSQL.map { " DEFAULT \($0)" } ?? ""
      return CompatibleIssueColumn(
        name: column.name, declaration: "\(column.type)\(notNullClause)\(defaultClause)")
    }

    let retainedColumns = reservedColumns.union(columns.map(\.name))
    let indexes = try Row.fetchAll(db, sql: "PRAGMA index_list(issues)").compactMap {
      row -> CompatibleIssueIndex? in
      let name: String = row["name"]
      let isUnique: Int = row["unique"]
      let origin: String = row["origin"]
      let isPartial: Int = row["partial"]
      guard name != "idx_issues_client_id" else { return nil }
      guard
        isCompatibleIdentifier(name),
        !name.hasPrefix("idx_issues_"),
        isUnique == 0,
        origin == "c",
        isPartial == 0
      else {
        throw LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(name: name)
      }
      let keyColumns = try Row.fetchAll(db, sql: "PRAGMA index_xinfo(\(quotedIdentifier(name)))")
        .filter { row in
          let isKeyColumn: Int = row["key"]
          return isKeyColumn != 0
        }
      let indexColumns = keyColumns.compactMap { row -> String? in
        let columnID: Int = row["cid"]
        let isDescending: Int = row["desc"]
        let collation: String = row["coll"]
        guard columnID >= 0, isDescending == 0, collation == "BINARY" else { return nil }
        return row["name"]
      }
      guard
        !indexColumns.isEmpty,
        indexColumns.count == keyColumns.count,
        indexColumns.allSatisfy(retainedColumns.contains)
      else {
        throw LinearLiteShapeMaterializerError.incompatibleCallerIssueExtension(name: name)
      }
      return CompatibleIssueIndex(name: name, columns: indexColumns)
    }
    return (columns, indexes)
  }

  private static func isCompatibleIdentifier(_ identifier: String) -> Bool {
    let bytes = Array(identifier.utf8)
    guard let first = bytes.first, isASCIILetter(first) || first == 95 else { return false }
    return bytes.dropFirst().allSatisfy { isASCIILetter($0) || (48...57).contains($0) || $0 == 95 }
  }

  private static func isASCIILetter(_ byte: UInt8) -> Bool {
    (65...90).contains(byte) || (97...122).contains(byte)
  }

  private static func quotedIdentifier(_ identifier: String) -> String {
    precondition(isCompatibleIdentifier(identifier))
    return "\"\(identifier)\""
  }

  public func currentCursor() async throws -> StreamCursor? {
    try await availability.checkAvailability()
    return try await database.read { [shapeID] db in
      guard
        let row = try Row.fetchOne(
          db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?", arguments: [shapeID])
      else { return nil }
      return StreamCursor(offset: row["offset"], lsn: row["lsn"])
    }
  }

  public func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    try await availability.checkAvailability()
    try await database.write { [shapeID, principalID] db in
      let persisted = try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?", arguments: [shapeID])
      let actual = persisted.map { StreamCursor(offset: $0["offset"], lsn: $0["lsn"]) }
      if actual == cursor { return }
      guard actual == expectedCursor else {
        throw StreamError.cursorConflict(
          expected: expectedCursor, actual: actual, advancingTo: cursor)
      }
      for (index, envelope) in batch.envelopes.enumerated() {
        let incomingLSN = envelope.headers.lsn
        let membershipKey = Int64(envelope.key) ?? -1
        let currentMembershipLSN: String? = try String.fetchOne(
          db,
          sql:
            "SELECT row_lsn FROM subset_view_members WHERE view_id = ? AND principal_id = ? AND issue_id = ?",
          arguments: [shapeID, principalID, membershipKey])
        guard Self.accepts(incomingLSN, relativeTo: currentMembershipLSN) else { continue }

        switch envelope.headers.operation {
        case .delete:
          guard let id = Int64(envelope.key) else {
            throw LinearLiteShapeMaterializerError.invalidDeleteKey(key: envelope.key)
          }
          let knownClientID: String? = try String.fetchOne(
            db, sql: "SELECT client_id FROM issues WHERE principal_id = ? AND id = ?",
            arguments: [principalID, id])
          try db.execute(
            sql:
              "DELETE FROM subset_view_members WHERE view_id = ? AND principal_id = ? AND issue_id = ?",
            arguments: [shapeID, principalID, id])
          if case .string(let clientID) = envelope.old?["client_id"] {
            try Self.removeOverlays(db, clientID: clientID, scopeID: shapeID)
          } else if let knownClientID {
            // Replica identity normally carries the old row, but the local canonical row is a
            // safe fallback for transports that omit it.
            try Self.removeOverlays(db, clientID: knownClientID, scopeID: shapeID)
          }
        case .insert, .update, .upsert:
          guard let value = envelope.value else {
            throw LinearLiteShapeMaterializerError.missingValue(key: envelope.key)
          }
          let issue: Issue
          do {
            issue = try Issue(changeRow: value)
          } catch {
            throw LinearLiteShapeMaterializerError.malformedValue(
              key: envelope.key, detail: String(describing: error))
          }
          guard String(issue.id) == envelope.key else {
            throw LinearLiteShapeMaterializerError.keyMismatch(key: envelope.key, id: issue.id)
          }
          let currentIssueLSN: String? = try String.fetchOne(
            db, sql: "SELECT last_lsn FROM issues WHERE principal_id = ? AND id = ?",
            arguments: [principalID, issue.id])
          guard Self.accepts(incomingLSN, relativeTo: currentIssueLSN) else { continue }
          try Self.upsertIssue(
            db, issue: issue, principalID: principalID,
            lsn: Self.newerLSN(incomingLSN, than: currentIssueLSN))
          try Self.retireMatchingOverlays(db, issue: issue, scopeID: shapeID)
          try db.execute(
            sql: """
              INSERT INTO subset_view_members (view_id, principal_id, issue_id, row_lsn)
              VALUES (?, ?, ?, ?)
              ON CONFLICT(view_id, issue_id) DO UPDATE SET row_lsn = excluded.row_lsn
              """,
            arguments: [
              shapeID, principalID, issue.id,
              Self.newerLSN(incomingLSN, than: currentMembershipLSN),
            ])
        }
        if index == 0 { try faultInjector?(.afterFirstRow) }
      }
      try faultInjector?(.beforeCursorCommit)
      try Self.pruneUnownedIssues(db)
      try db.execute(
        sql: """
          INSERT INTO shape_cursors (shape_id, offset, lsn)
          VALUES (?, ?, ?)
          ON CONFLICT(shape_id) DO UPDATE SET offset = excluded.offset, lsn = excluded.lsn
          """, arguments: [shapeID, cursor.offset, cursor.lsn])
    }
  }

  /// Deletes this scope's memberships, cursor, overlays, and now-unowned canonical rows in one
  /// transaction. Other scopes remain readable and retain any canonical rows they still own.
  public func purgeScope() async throws {
    try await database.write { [shapeID] db in
      try db.execute(sql: "DELETE FROM issue_overlays WHERE scope_id = ?", arguments: [shapeID])
      try db.execute(sql: "DELETE FROM subset_view_members WHERE view_id = ?", arguments: [shapeID])
      try db.execute(sql: "DELETE FROM shape_cursors WHERE shape_id = ?", arguments: [shapeID])
      try db.execute(
        sql: "DELETE FROM materialization_scopes WHERE scope_key = ?", arguments: [shapeID])
      try Self.pruneUnownedIssues(db)
    }
  }

  /// Deletes every scope and private materialized row for one principal in one transaction. This
  /// is the account-switch/logout boundary; no metadata, cursor, overlay, membership, or canonical
  /// row from the principal remains visible afterwards.
  public static func purgePrincipal(_ principal: String, from database: DatabaseQueue) async throws
  {
    try await database.write { db in
      let scopeKeys = try String.fetchAll(
        db, sql: "SELECT scope_key FROM materialization_scopes WHERE principal = ?",
        arguments: [principal])
      for scopeKey in scopeKeys {
        try db.execute(sql: "DELETE FROM issue_overlays WHERE scope_id = ?", arguments: [scopeKey])
        try db.execute(
          sql: "DELETE FROM subset_view_members WHERE view_id = ?", arguments: [scopeKey])
        try db.execute(sql: "DELETE FROM shape_cursors WHERE shape_id = ?", arguments: [scopeKey])
      }
      try db.execute(
        sql: "DELETE FROM materialization_scopes WHERE principal = ?", arguments: [principal])
      try db.execute(sql: "DELETE FROM issues WHERE principal_id = ?", arguments: [principal])
    }
  }

  /// Atomically replaces this shape's complete snapshot and cursor.
  ///
  /// This is intentionally separate from `apply`: a subset query is a one-shot snapshot, not a
  /// stream of changes, so stale rows must be removed before the new ordered page is published.
  public func replaceSnapshot(
    _ rows: [ChangeRow],
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    let issues: [Issue] = try rows.enumerated().map { index, row in
      do { return try Issue(changeRow: row) } catch {
        throw LinearLiteShapeMaterializerError.malformedSnapshotRow(
          index: index, detail: String(describing: error))
      }
    }
    try await database.write { [shapeID, principalID] db in
      let persisted = try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM shape_cursors WHERE shape_id = ?", arguments: [shapeID])
      let actual = persisted.map { StreamCursor(offset: $0["offset"], lsn: $0["lsn"]) }
      if actual == cursor { return }
      guard actual == expectedCursor else {
        throw StreamError.cursorConflict(
          expected: expectedCursor, actual: actual, advancingTo: cursor)
      }
      for issue in issues {
        let currentIssueLSN: String? = try String.fetchOne(
          db, sql: "SELECT last_lsn FROM issues WHERE principal_id = ? AND id = ?",
          arguments: [principalID, issue.id])
        guard Self.accepts(cursor.lsn, relativeTo: currentIssueLSN) else { continue }
        try Self.upsertIssue(
          db, issue: issue, principalID: principalID,
          lsn: Self.newerLSN(cursor.lsn, than: currentIssueLSN))
      }
      try db.execute(
        sql: "DELETE FROM subset_view_members WHERE view_id = ? AND principal_id = ?",
        arguments: [shapeID, principalID])
      for issue in issues {
        try db.execute(
          sql:
            "INSERT INTO subset_view_members (view_id, principal_id, issue_id, row_lsn) VALUES (?, ?, ?, ?)",
          arguments: [shapeID, principalID, issue.id, cursor.lsn])
      }
      try Self.pruneUnownedIssues(db)
      try db.execute(
        sql: """
          INSERT INTO shape_cursors (shape_id, offset, lsn)
          VALUES (?, ?, ?)
          ON CONFLICT(shape_id) DO UPDATE SET offset = excluded.offset, lsn = excluded.lsn
          """, arguments: [shapeID, cursor.offset, cursor.lsn])
    }
  }

  public func allIssues() async throws -> [Issue] {
    try await allIssues(order: .id)
  }

  public enum IssueOrder: Equatable, Sendable {
    case id
    case modifiedDescending
  }

  public func allIssues(order: IssueOrder) async throws -> [Issue] {
    try await database.read { [shapeID, principalID] db in
      try Issue.fetchAll(
        db,
        sql: """
          SELECT issues.id, issues.client_id, issues.title, issues.description, issues.status, issues.priority,
                 issues.username, issues.project_id, issues.created, issues.modified, issues.kanbanorder
          FROM issues
          JOIN subset_view_members ON subset_view_members.issue_id = issues.id
            AND subset_view_members.principal_id = issues.principal_id
          WHERE subset_view_members.view_id = ? AND subset_view_members.principal_id = ?
          ORDER BY \(order == .modifiedDescending ? "modified DESC, id DESC" : "id")
          """,
        arguments: [shapeID, principalID])
    }
  }

  /// Reads the current view after applying durable client-owned overlays. The authoritative
  /// `allIssues` query remains available for reconciliation and diagnostics; this projection is the
  /// UI-facing read path while a mutation is pending or acknowledged but not yet present in the
  /// feed.
  public func allIssuesIncludingOverlays(order: IssueOrder = .id) async throws -> [Issue] {
    try await database.read { [shapeID, principalID] db in
      let issues = try Issue.fetchAll(
        db,
        sql: """
          SELECT issues.id, issues.client_id, issues.title, issues.description, issues.status, issues.priority,
                 issues.username, issues.project_id, issues.created, issues.modified, issues.kanbanorder
          FROM issues
          JOIN subset_view_members ON subset_view_members.issue_id = issues.id
            AND subset_view_members.principal_id = issues.principal_id
          WHERE subset_view_members.view_id = ? AND subset_view_members.principal_id = ?
          ORDER BY \(order == .modifiedDescending ? "modified DESC, id DESC" : "id")
          """,
        arguments: [shapeID, principalID])
      let overlayRows = try Row.fetchAll(
        db,
        sql: """
          SELECT mutation_id, row_key, operation, patch_json, status, request_id, created_at
          FROM issue_overlays
          WHERE scope_id = ?
          ORDER BY created_at, mutation_id
          """, arguments: [shapeID])
      let overlays = try overlayRows.map(Self.decodeOverlay)
      let overlaysByRow = Dictionary(grouping: overlays, by: { $0.rowKey })
      return try issues.compactMap { issue in
        var projected: Issue? = issue
        var matchingOverlays = overlaysByRow[String(issue.id)] ?? []
        if let clientID = issue.clientID {
          matchingOverlays.append(contentsOf: overlaysByRow[clientID] ?? [])
        }
        for overlay in matchingOverlays.sorted(by: Self.overlayPrecedes) {
          projected = try Self.apply(overlay, to: projected)
          if projected == nil { break }
        }
        return projected
      }
    }
  }

  /// Persists a client-owned optimistic mutation separately from the authoritative issue row.
  /// The feed can later retire this record after an exact server acknowledgement.
  public func saveOverlay(_ overlay: LinearLiteIssueOverlay) async throws {
    let patch = try JSONEncoder().encode(overlay.patch)
    try await database.write { db in
      try db.execute(
        sql: """
          INSERT INTO issue_overlays
            (mutation_id, row_key, operation, patch_json, status, request_id, created_at, scope_id)
          VALUES (?, ?, ?, ?, ?, ?, ?, ?)
          ON CONFLICT(scope_id, mutation_id) DO UPDATE SET
            row_key = excluded.row_key,
            operation = excluded.operation,
            patch_json = excluded.patch_json,
            status = excluded.status,
            request_id = excluded.request_id,
            created_at = excluded.created_at
          """,
        arguments: [
          overlay.mutationID, overlay.rowKey, overlay.operation.rawValue, patch,
          overlay.status.rawValue, overlay.requestID, overlay.createdAt.timeIntervalSince1970,
          shapeID,
        ])
    }
  }

  public func overlays(for rowKey: String) async throws -> [LinearLiteIssueOverlay] {
    try await database.read { [shapeID] db in
      try Row.fetchAll(
        db,
        sql: """
            SELECT mutation_id, row_key, operation, patch_json, status, request_id, created_at
            FROM issue_overlays
            WHERE scope_id = ? AND row_key = ?
            ORDER BY created_at, mutation_id
          """,
        arguments: [shapeID, rowKey]
      ).map(Self.decodeOverlay)
    }
  }

  public func removeOverlay(mutationID: String) async throws {
    try await database.write { [shapeID] db in
      try db.execute(
        sql: "DELETE FROM issue_overlays WHERE scope_id = ? AND mutation_id = ?",
        arguments: [shapeID, mutationID])
    }
  }

  private static func decodeOverlay(_ row: Row) throws -> LinearLiteIssueOverlay {
    guard
      let operation = LinearLiteIssueOverlay.Operation(rawValue: row["operation"]),
      let status = LinearLiteIssueOverlay.Status(rawValue: row["status"])
    else {
      throw DatabaseError(message: "invalid issue overlay enum")
    }
    let patchData: Data = row["patch_json"]
    return LinearLiteIssueOverlay(
      mutationID: row["mutation_id"], rowKey: row["row_key"], operation: operation,
      patch: try JSONDecoder().decode([String: JSONValue].self, from: patchData), status: status,
      requestID: row["request_id"], createdAt: Date(timeIntervalSince1970: row["created_at"]))
  }

  private static func overlayPrecedes(
    _ lhs: LinearLiteIssueOverlay, _ rhs: LinearLiteIssueOverlay
  ) -> Bool {
    if lhs.createdAt != rhs.createdAt { return lhs.createdAt < rhs.createdAt }
    return lhs.mutationID < rhs.mutationID
  }

  /// Removes an optimistic insert/update once the feed contains its client identity. Updates
  /// additionally require every patched field to match the authoritative row; an insert only needs
  /// the immutable client ID because the server may fill defaults or normalize other fields.
  private static func retireMatchingOverlays(_ db: Database, issue: Issue, scopeID: String) throws {
    guard let clientID = issue.clientID else { return }
    let rows = try Row.fetchAll(
      db,
      sql: """
        SELECT mutation_id, row_key, operation, patch_json, status, request_id, created_at
        FROM issue_overlays
        WHERE scope_id = ? AND row_key = ?
        """, arguments: [scopeID, clientID])
    for row in rows {
      let overlay = try Self.decodeOverlay(row)
      guard overlay.status != .rejected else { continue }
      let matches: Bool
      switch overlay.operation {
      case .insert:
        matches = true
      case .update:
        matches = try Self.apply(overlay, to: issue) == issue
      case .delete:
        matches = false
      }
      if matches {
        try db.execute(
          sql: "DELETE FROM issue_overlays WHERE scope_id = ? AND mutation_id = ?",
          arguments: [scopeID, overlay.mutationID])
      }
    }
  }

  private static func removeOverlays(_ db: Database, clientID: String, scopeID: String) throws {
    try db.execute(
      sql: "DELETE FROM issue_overlays WHERE scope_id = ? AND row_key = ?",
      arguments: [scopeID, clientID])
  }

  private static func apply(
    _ overlay: LinearLiteIssueOverlay,
    to issue: Issue?
  ) throws -> Issue? {
    guard overlay.status != .rejected else { return issue }
    guard var issue else {
      guard overlay.operation == .insert else { return nil }
      throw LinearLiteShapeMaterializerError.malformedOverlay(
        mutationID: overlay.mutationID, detail: "insert overlays require a canonical row key")
    }
    if overlay.operation == .delete { return nil }
    for (field, value) in overlay.patch {
      do {
        switch field {
        case "id":
          let id = try overlayInt(value)
          guard id == issue.id else { throw OverlayPatchError.invalidValue }
        case "client_id":
          guard case .string(let clientID) = value, clientID == issue.clientID else {
            throw OverlayPatchError.invalidValue
          }
        case "title": issue.title = try overlayString(value)
        case "description": issue.description = try overlayString(value)
        case "status": issue.status = try overlayString(value)
        case "priority": issue.priority = try overlayString(value)
        case "username": issue.username = try overlayString(value)
        case "project_id": issue.projectID = try overlayInt(value)
        case "created": issue.created = try overlayInt(value)
        case "modified": issue.modified = try overlayInt(value)
        case "kanbanorder": issue.kanbanOrder = try overlayDouble(value)
        default: throw OverlayPatchError.unknownField
        }
      } catch {
        throw LinearLiteShapeMaterializerError.malformedOverlay(
          mutationID: overlay.mutationID, detail: "invalid \(field): \(error)")
      }
    }
    return issue
  }

  private enum OverlayPatchError: Error {
    case invalidValue
    case unknownField
  }

  private static func overlayString(_ value: JSONValue) throws -> String {
    guard case .string(let value) = value else { throw OverlayPatchError.invalidValue }
    return value
  }

  private static func overlayInt(_ value: JSONValue) throws -> Int64 {
    switch value {
    case .int(let value): return value
    case .decimal(let value): return NSDecimalNumber(decimal: value).int64Value
    case .number(let value): return Int64(value)
    default: throw OverlayPatchError.invalidValue
    }
  }

  private static func overlayDouble(_ value: JSONValue) throws -> Double {
    switch value {
    case .int(let value): return Double(value)
    case .decimal(let value): return NSDecimalNumber(decimal: value).doubleValue
    case .number(let value): return value
    default: throw OverlayPatchError.invalidValue
    }
  }

  /// Applies an LSN watermark without making SQLite responsible for parsing Postgres LSNs. A
  /// malformed or absent LSN is treated as incomparable and therefore accepted, while a missing
  /// incoming LSN never erases a valid watermark recorded by another feed.
  private static func accepts(_ incoming: String?, relativeTo stored: String?) -> Bool {
    guard let incoming, let stored else { return true }
    guard let incomingValue = parseLSN(incoming), let storedValue = parseLSN(stored) else {
      return true
    }
    return incomingValue >= storedValue
  }

  private static func newerLSN(_ candidate: String?, than stored: String?) -> String? {
    guard let candidate else { return stored }
    guard let candidateValue = parseLSN(candidate) else { return stored ?? candidate }
    guard let stored, let storedValue = parseLSN(stored) else { return candidate }
    return candidateValue >= storedValue ? candidate : stored
  }

  private static func parseLSN(_ value: String) -> UInt64? {
    let parts = value.split(separator: "/", omittingEmptySubsequences: false)
    guard parts.count == 2,
      let high = UInt64(parts[0], radix: 16),
      let low = UInt64(parts[1], radix: 16)
    else { return nil }
    return (high << 32) | low
  }

  private static func upsertIssue(
    _ db: Database,
    issue: Issue,
    principalID: String,
    lsn: String?
  ) throws {
    try db.execute(
      sql: """
        INSERT INTO issues
          (principal_id, id, client_id, title, description, status, priority, username, project_id, created,
           modified, kanbanorder, last_lsn)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ON CONFLICT(principal_id, id) DO UPDATE SET
          client_id = COALESCE(excluded.client_id, issues.client_id),
          title = excluded.title,
          description = excluded.description,
          status = excluded.status,
          priority = excluded.priority,
          username = excluded.username,
          project_id = excluded.project_id,
          created = excluded.created,
          modified = excluded.modified,
          kanbanorder = excluded.kanbanorder,
          last_lsn = excluded.last_lsn
        """,
      arguments: [
        principalID, issue.id, issue.clientID, issue.title, issue.description, issue.status,
        issue.priority,
        issue.username, issue.projectID, issue.created, issue.modified, issue.kanbanOrder, lsn,
      ])
  }

  private static func pruneUnownedIssues(_ db: Database) throws {
    try db.execute(
      sql: """
        DELETE FROM issues
        WHERE NOT EXISTS (
          SELECT 1 FROM subset_view_members
          WHERE subset_view_members.principal_id = issues.principal_id
            AND subset_view_members.issue_id = issues.id
        )
        AND NOT EXISTS (
          SELECT 1 FROM issue_overlays
          LEFT JOIN materialization_scopes ON materialization_scopes.scope_key = issue_overlays.scope_id
          WHERE (materialization_scopes.principal = issues.principal_id
                 OR (issues.principal_id = 'legacy' AND issue_overlays.scope_id = 'legacy'))
            AND (issue_overlays.row_key = CAST(issues.id AS TEXT)
                 OR issue_overlays.row_key = issues.client_id)
        )
        """)
  }
}

extension Issue {
  public init(changeRow: ChangeRow) throws {
    guard case .int(let id) = changeRow["id"] else { throw IssueRowError.missingOrInvalid("id") }
    let clientID: String?
    switch changeRow["client_id"] {
    case nil: clientID = nil
    case .string(let value):
      guard ClientID(rawValue: value) != nil else {
        throw IssueRowError.missingOrInvalid("client_id")
      }
      clientID = value.lowercased()
    default: throw IssueRowError.missingOrInvalid("client_id")
    }
    guard case .string(let title) = changeRow["title"] else {
      throw IssueRowError.missingOrInvalid("title")
    }
    guard case .string(let description) = changeRow["description"] else {
      throw IssueRowError.missingOrInvalid("description")
    }
    guard case .string(let status) = changeRow["status"] else {
      throw IssueRowError.missingOrInvalid("status")
    }
    guard case .string(let priority) = changeRow["priority"] else {
      throw IssueRowError.missingOrInvalid("priority")
    }
    guard case .string(let username) = changeRow["username"] else {
      throw IssueRowError.missingOrInvalid("username")
    }
    guard case .int(let projectID) = changeRow["project_id"] else {
      throw IssueRowError.missingOrInvalid("project_id")
    }
    guard case .int(let created) = changeRow["created"] else {
      throw IssueRowError.missingOrInvalid("created")
    }
    guard case .int(let modified) = changeRow["modified"] else {
      throw IssueRowError.missingOrInvalid("modified")
    }
    let kanbanOrder: Double
    switch changeRow["kanbanorder"] {
    case .number(let value): kanbanOrder = value
    case .int(let value): kanbanOrder = Double(value)
    case .decimal(let value): kanbanOrder = NSDecimalNumber(decimal: value).doubleValue
    default: throw IssueRowError.missingOrInvalid("kanbanorder")
    }
    self.init(
      id: id,
      clientID: clientID,
      title: title,
      description: description,
      status: status,
      priority: priority,
      username: username, projectID: projectID, created: created, modified: modified,
      kanbanOrder: kanbanOrder)
  }

  public var changeRow: ChangeRow {
    var row: ChangeRow = [
      "id": .int(id), "title": .string(title), "description": .string(description),
      "status": .string(status), "priority": .string(priority), "username": .string(username),
      "project_id": .int(projectID), "created": .int(created), "modified": .int(modified),
      "kanbanorder": .number(kanbanOrder),
    ]
    if let clientID { row["client_id"] = .string(clientID) }
    return row
  }
}

private enum IssueRowError: Error {
  case missingOrInvalid(String)
}
