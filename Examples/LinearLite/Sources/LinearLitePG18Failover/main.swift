import ElectricCircuitsSwift
import Foundation
import GRDB

private enum FailoverError: Error {
  case missingEnvironment(String)
  case deadline(String)
  case assertion(String)
}

private struct Item: Codable, Equatable, Sendable {
  let id: Int64
  let title: String
}

private struct ExpectedState: Codable, Sendable {
  let baseline: [Item]
  let promoted: [Item]
}

private struct HeaderFacts: Codable, Sendable {
  let control: Bool
  let stream: Bool
  let successfulReleases: Int
}

private struct Result: Codable, Sendable {
  let oldSubscription: String
  let newSubscription: String
  let oldHandle: String
  let newHandle: String
  let oldScope: String
  let newScope: String
  let oldCursor: StreamCursor
  let reopenedCursor: StreamCursor
  let baseline: [Item]
  let oldAfterPromotion: [Item]
  let recovered: [Item]
  let reopened: [Item]
  let visibleObservations: [VisibleObservation]
  let headers: HeaderFacts
}

private struct VisibleObservation: Codable, Equatable, Sendable {
  let stage: String
  let scope: String
  let rows: [Item]
}

private actor RequestFacts {
  private var control = false
  private var stream = false
  private var successfulReleases = 0

  func record(_ request: URLRequest) throws {
    guard request.value(forHTTPHeaderField: "X-ECS-Qualification") == "pg18-failover-p1-1" else {
      throw FailoverError.assertion(
        "qualification header missing on \(request.url?.absoluteString ?? "request")")
    }
    if request.httpMethod == "GET", request.url?.query?.contains("live=long-poll") == true {
      stream = true
    } else if request.url?.path.contains("/v1/shapes") == true {
      control = true
    }
  }

  func record(_ response: HTTPResponse, for request: URLRequest) {
    guard request.httpMethod == "DELETE", request.url?.path.contains("/v1/shapes/") == true else {
      return
    }
    if (200..<300).contains(response.response.statusCode) || response.response.statusCode == 404 {
      successfulReleases += 1
    }
  }

  func snapshot() -> HeaderFacts {
    HeaderFacts(control: control, stream: stream, successfulReleases: successfulReleases)
  }
}

private struct RecordingTransport: HTTPTransport {
  let base: URLSessionTransport
  let facts: RequestFacts

  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let prepared = base.prepare(request)
    try await facts.record(prepared)
    let response = try await base.send(prepared)
    await facts.record(response, for: prepared)
    return response
  }
}

/// A tiny file-backed provider used only by this qualification. Its transaction is the public
/// `ShapeMaterializer` boundary: rows and the durable-stream cursor move together, or not at all.
private actor FileMaterializer: ShapeMaterializer {
  let materializationScope: MaterializationScope?
  private let database: DatabaseQueue
  private let key: String

  init(database: DatabaseQueue, scope: MaterializationScope) throws {
    self.database = database
    materializationScope = scope
    key = scope.storageKey
    try database.write { db in
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS pg18_failover_rows (
            scope_key TEXT NOT NULL,
            id INTEGER NOT NULL,
            title TEXT NOT NULL,
            PRIMARY KEY (scope_key, id)
          )
          """)
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS pg18_failover_cursors (
            scope_key TEXT PRIMARY KEY NOT NULL,
            offset TEXT NOT NULL,
            lsn TEXT
          )
          """)
    }
  }

  func currentCursor() async throws -> StreamCursor? {
    try await database.read { [key] db in
      guard
        let row = try Row.fetchOne(
          db, sql: "SELECT offset, lsn FROM pg18_failover_cursors WHERE scope_key = ?",
          arguments: [key])
      else { return nil }
      return StreamCursor(offset: row["offset"], lsn: row["lsn"])
    }
  }

  func apply(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    try await database.write { [key] db in
      let persisted = try Row.fetchOne(
        db, sql: "SELECT offset, lsn FROM pg18_failover_cursors WHERE scope_key = ?",
        arguments: [key])
      let actual = persisted.map { StreamCursor(offset: $0["offset"], lsn: $0["lsn"]) }
      if actual == cursor { return }
      guard actual == expectedCursor else {
        throw StreamError.cursorConflict(
          expected: expectedCursor, actual: actual, advancingTo: cursor)
      }
      for envelope in batch.envelopes {
        guard let id = Int64(envelope.key) else {
          throw FailoverError.assertion("non-integer item key \(envelope.key)")
        }
        switch envelope.headers.operation {
        case .delete:
          try db.execute(
            sql: "DELETE FROM pg18_failover_rows WHERE scope_key = ? AND id = ?",
            arguments: [key, id])
        case .insert, .update, .upsert:
          guard let row = envelope.value, let title = row["title"].flatMap(stringValue) else {
            throw StreamError.missingValue(key: envelope.key)
          }
          try db.execute(
            sql: """
              INSERT INTO pg18_failover_rows (scope_key, id, title) VALUES (?, ?, ?)
              ON CONFLICT(scope_key, id) DO UPDATE SET title = excluded.title
              """, arguments: [key, id, title])
        }
      }
      try db.execute(
        sql: """
          INSERT INTO pg18_failover_cursors (scope_key, offset, lsn) VALUES (?, ?, ?)
          ON CONFLICT(scope_key) DO UPDATE SET offset = excluded.offset, lsn = excluded.lsn
          """, arguments: [key, cursor.offset, cursor.lsn])
    }
  }

  func rows() async throws -> [Item] {
    try await database.read { [key] db in
      try Item.fetchAll(
        db, sql: "SELECT id, title FROM pg18_failover_rows WHERE scope_key = ? ORDER BY id",
        arguments: [key])
    }
  }

  func rowCount() async throws -> Int {
    try await database.read { [key] db in
      try Int.fetchOne(
        db, sql: "SELECT COUNT(*) FROM pg18_failover_rows WHERE scope_key = ?", arguments: [key])
        ?? 0
    }
  }

  /// Deliberately incomplete recovery used only by the red execution: it loses the old stream
  /// cursor while retaining the old rows. A new full feed then proves why a new generation scope
  /// (rather than cursor-only recovery) is required after unsynchronised-slot promotion.
  func resetCursorRetainingRowsForRed() async throws {
    try await database.write { [key] db in
      try db.execute(sql: "DELETE FROM pg18_failover_cursors WHERE scope_key = ?", arguments: [key])
    }
  }
}

/// The public reader selects exactly one scope from the same GRDB file. Recovery builds the new
/// generation off-screen, then swaps this selector after its complete snapshot is committed. It
/// cannot accidentally union stale old rows with partial new rows.
private actor VisibleScopeStore {
  private let database: DatabaseQueue

  init(database: DatabaseQueue) throws {
    self.database = database
    try database.write { db in
      try db.execute(
        sql: """
          CREATE TABLE IF NOT EXISTS pg18_failover_visible_scope (
            singleton INTEGER PRIMARY KEY CHECK (singleton = 1),
            scope_key TEXT NOT NULL
          )
          """)
    }
  }

  func activate(_ scope: MaterializationScope) async throws {
    try await database.write { db in
      try db.execute(
        sql: """
          INSERT INTO pg18_failover_visible_scope (singleton, scope_key) VALUES (1, ?)
          ON CONFLICT(singleton) DO UPDATE SET scope_key = excluded.scope_key
          """, arguments: [scope.storageKey])
    }
  }

  func read() async throws -> (scope: String, rows: [Item]) {
    try await database.read { db in
      guard
        let scope = try String.fetchOne(
          db, sql: "SELECT scope_key FROM pg18_failover_visible_scope WHERE singleton = 1")
      else { throw FailoverError.assertion("visible scope is unset") }
      let rows = try Item.fetchAll(
        db, sql: "SELECT id, title FROM pg18_failover_rows WHERE scope_key = ? ORDER BY id",
        arguments: [scope])
      return (scope, rows)
    }
  }
}

extension Item: FetchableRecord {}

private func stringValue(_ value: JSONValue) -> String? {
  switch value {
  case .string(let value): value
  case .int(let value): String(value)
  case .decimal(let value): String(describing: value)
  case .number(let value): String(value)
  default: nil
  }
}

@main
struct LinearLitePG18Failover {
  static func main() async {
    do {
      try await run()
    } catch {
      FileHandle.standardError.write(Data("linear-lite-pg18-failover failure: \(error)\n".utf8))
      exit(1)
    }
  }

  private static func run() async throws {
    let environment = ProcessInfo.processInfo.environment
    guard let baseURL = environment["ECS_PG18_FAILOVER_BASE_URL"].flatMap(URL.init(string:)),
      let phaseDirectory = environment["ECS_PG18_FAILOVER_DIR"].map(URL.init(fileURLWithPath:)),
      let databaseURL = environment["ECS_PG18_FAILOVER_DATABASE"].map(URL.init(fileURLWithPath:))
    else {
      throw FailoverError.missingEnvironment(
        "ECS_PG18_FAILOVER_BASE_URL/ECS_PG18_FAILOVER_DIR/ECS_PG18_FAILOVER_DATABASE")
    }

    let expected = try read(
      ExpectedState.self, from: phaseDirectory.appendingPathComponent("expected.json"))
    let database = try DatabaseQueue(path: databaseURL.path)
    let facts = RequestFacts()
    let transport = RecordingTransport(
      base: URLSessionTransport(headers: ["X-ECS-Qualification": "pg18-failover-p1-1"]),
      facts: facts)
    let client = ElectricCircuitsClient(baseURL: baseURL, transport: transport)
    let oldSubscription = "swift-pg18-failover-old"
    let newSubscription = "swift-pg18-failover-new"
    let oldScope = MaterializationScope(
      principal: "qualification", template: "items", subscription: oldSubscription,
      generation: "old")
    let oldProvider = try FileMaterializer(database: database, scope: oldScope)
    let visible = try VisibleScopeStore(database: database)
    let oldCoordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport,
      request: ShapeRequest(
        table: "public.items", where: .leaf(column: "id", op: .gte, value: .int(0)),
        subscription: oldSubscription),
      materializer: oldProvider, retryPolicy: retryPolicy())
    let oldHandle = try await oldCoordinator.start()
    try await waitFor("old baseline materialization") {
      guard let rows = try? await oldProvider.rows(),
        let cursor = try? await oldProvider.currentCursor()
      else { return false }
      _ = cursor
      return rows == expected.baseline
    }
    let oldCursor = try require(await oldProvider.currentCursor(), "old cursor")
    try await visible.activate(oldScope)
    let oldVisible = try await visible.read()
    guard oldVisible.scope == oldScope.storageKey, oldVisible.rows == expected.baseline else {
      throw FailoverError.assertion("old scope was not the complete visible baseline")
    }
    try write(
      ["shapeID": oldHandle.id, "subscription": oldSubscription, "cursor": oldCursor.offset],
      to: phaseDirectory.appendingPathComponent("swift-baseline-ready.json"))
    try Data().write(to: phaseDirectory.appendingPathComponent("swift-baseline-ready"))

    try await waitForFile(
      phaseDirectory.appendingPathComponent("engine-repointed"), name: "engine repointed")
    try await waitFor("typed old-generation terminal") {
      if case .reseedRequired(let outcome) = await oldCoordinator.state,
        case .terminal = outcome.reason
      {
        return true
      }
      return false
    }
    let oldAfterTerminal = try await oldProvider.rows()
    guard oldAfterTerminal == expected.baseline else {
      throw FailoverError.assertion("old generation changed before recovery")
    }
    try Data().write(to: phaseDirectory.appendingPathComponent("old-terminal"))
    try await waitForFile(
      phaseDirectory.appendingPathComponent("promoted-mutation-drained"),
      name: "promoted marker receipt")
    let oldAfterPromotion = try await oldProvider.rows()
    guard oldAfterPromotion == expected.baseline else {
      throw FailoverError.assertion("old reader applied a promoted-generation row")
    }
    let beforeActivation = try await visible.read()
    guard beforeActivation.scope == oldScope.storageKey, beforeActivation.rows == expected.baseline
    else {
      throw FailoverError.assertion("visible reader exposed a hybrid before recovery activation")
    }

    let reuseOldScope = environment["ECS_PG18_FAILOVER_RED_REUSE_OLD_SCOPE"] == "1"
    let newScope =
      reuseOldScope
      ? oldScope
      : MaterializationScope(
        principal: "qualification", template: "items", subscription: newSubscription,
        generation: "promoted")
    let recoveredProvider =
      reuseOldScope
      ? oldProvider
      : try FileMaterializer(database: database, scope: newScope)
    if reuseOldScope { try await oldProvider.resetCursorRetainingRowsForRed() }
    let recoveredCoordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport,
      request: ShapeRequest(
        table: "public.items", where: .leaf(column: "id", op: .gte, value: .int(0)),
        subscription: newSubscription),
      materializer: recoveredProvider, retryPolicy: retryPolicy())
    let newHandle = try await recoveredCoordinator.start()
    try await waitForRecovered(
      provider: recoveredProvider, expected: expected.promoted, previous: expected.baseline)
    let recovered = try await recoveredProvider.rows()
    // `waitForRecovered` returned only after the new provider's transaction had committed the
    // exact promoted snapshot. The selector change is its own one-row GRDB transaction, so every
    // reader observes one complete scope on either side of this named cut.
    try await visible.activate(newScope)
    let afterActivation = try await visible.read()
    guard afterActivation.scope == newScope.storageKey, afterActivation.rows == expected.promoted
    else {
      throw FailoverError.assertion(
        "visible reader did not switch atomically to the complete new scope")
    }
    try write(
      [
        VisibleObservation(
          stage: "old_complete_before_activation", scope: beforeActivation.scope,
          rows: beforeActivation.rows),
        VisibleObservation(
          stage: "new_complete_after_activation", scope: afterActivation.scope,
          rows: afterActivation.rows),
      ], to: phaseDirectory.appendingPathComponent("visible-scope-transition.json"))
    let reopenedDatabase = try DatabaseQueue(path: databaseURL.path)
    let reopened = try FileMaterializer(database: reopenedDatabase, scope: newScope)
    let reopenedRows = try await reopened.rows()
    let reopenedCursor = try require(await reopened.currentCursor(), "reopened cursor")
    guard recovered == expected.promoted, reopenedRows == recovered,
      try await reopened.rowCount() == Set(recovered.map(\.id)).count
    else {
      throw FailoverError.assertion("recovered GRDB state is not exact and duplicate-free")
    }
    // The UI owns exactly one generation at a time. A correct observation is therefore a complete
    // old state before explicit recovery or a complete promoted state afterwards, never their union.
    let visibleObservations = [
      VisibleObservation(
        stage: "old_complete_before_activation", scope: beforeActivation.scope,
        rows: beforeActivation.rows),
      VisibleObservation(
        stage: "new_complete_after_activation", scope: afterActivation.scope,
        rows: afterActivation.rows),
    ]
    guard
      visibleObservations.allSatisfy({
        $0.rows == expected.baseline || $0.rows == expected.promoted
      })
    else {
      throw FailoverError.assertion("mixed-generation rows became visible")
    }
    try await recoveredCoordinator.stop()
    let headers = await facts.snapshot()
    guard headers.control, headers.stream, headers.successfulReleases >= 2 else {
      throw FailoverError.assertion("control/stream header or named release proof is missing")
    }
    guard oldHandle.id != newHandle.id, oldSubscription != newSubscription, oldScope != newScope
    else {
      throw FailoverError.assertion("recovery reused a retired handle, subscription, or generation")
    }
    try write(
      Result(
        oldSubscription: oldSubscription, newSubscription: newSubscription, oldHandle: oldHandle.id,
        newHandle: newHandle.id, oldScope: oldScope.storageKey, newScope: newScope.storageKey,
        oldCursor: oldCursor, reopenedCursor: reopenedCursor, baseline: expected.baseline,
        oldAfterPromotion: oldAfterPromotion, recovered: recovered, reopened: reopenedRows,
        visibleObservations: visibleObservations, headers: headers),
      to: phaseDirectory.appendingPathComponent("swift-result.json"))
  }

  private static func retryPolicy() -> ShapeSubscriptionRetryPolicy {
    ShapeSubscriptionRetryPolicy(
      maxRetries: 20, baseDelay: .milliseconds(25), maximumDelay: .milliseconds(100), jitterRatio: 0
    )
  }
}

private func require<T>(_ value: T?, _ name: String) throws -> T {
  guard let value else { throw FailoverError.assertion("missing \(name)") }
  return value
}

private func read<T: Decodable>(_ type: T.Type, from url: URL) throws -> T {
  try JSONDecoder().decode(T.self, from: Data(contentsOf: url))
}

private func write<T: Encodable>(_ value: T, to url: URL) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.sortedKeys]
  try encoder.encode(value).write(to: url, options: .atomic)
}

private func waitForFile(_ url: URL, name: String) async throws {
  try await waitFor(name) { FileManager.default.fileExists(atPath: url.path) }
}

private func waitFor(_ name: String, condition: @escaping @Sendable () async -> Bool) async throws {
  let deadline = ContinuousClock.now + .seconds(45)
  while ContinuousClock.now < deadline {
    if await condition() { return }
    try await Task.sleep(for: .milliseconds(25))
  }
  throw FailoverError.deadline(name)
}

private func waitForRecovered(
  provider: FileMaterializer, expected: [Item], previous: [Item]
) async throws {
  let deadline = ContinuousClock.now + .seconds(45)
  while ContinuousClock.now < deadline {
    let rows = try await provider.rows()
    if rows == expected { return }
    // A fresh private generation is empty while its first backfill batch is in flight. It is not
    // public yet because `VisibleScopeStore` still selects the complete old scope. Any nonempty
    // third state is a true hybrid and must fail immediately.
    if !rows.isEmpty && rows != previous {
      throw FailoverError.assertion("mixed-generation recovered rows: \(rows)")
    }
    try await Task.sleep(for: .milliseconds(25))
  }
  throw FailoverError.deadline("recovered generation materialization")
}
