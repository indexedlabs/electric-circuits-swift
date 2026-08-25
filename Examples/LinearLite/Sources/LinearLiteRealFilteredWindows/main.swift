import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import LinearLiteGRDB

private enum QualificationError: Error {
  case missingEnvironment(String)
  case deadline(String)
  case assertion(String)
}

private actor RequestFacts {
  private let expectedHeader: String
  private var feeds = 0
  private var queries = 0
  private var streams = 0
  private var releases = 0
  init(expectedHeader: String) { self.expectedHeader = expectedHeader }
  func record(_ request: URLRequest) throws {
    guard request.value(forHTTPHeaderField: "X-ECS-Qualification") == expectedHeader else {
      throw QualificationError.assertion(
        "missing qualification header on \(request.url?.absoluteString ?? "request")")
    }
    let path = request.url?.path ?? ""
    if request.httpMethod == "POST", path.contains("/v1/subset-feeds") { feeds += 1 }
    if request.httpMethod == "POST", path.contains("/v1/subsets/query") { queries += 1 }
    if request.httpMethod == "GET", request.url?.query?.contains("live=long-poll") == true {
      streams += 1
    }
    if request.httpMethod == "DELETE", path.contains("/v1/shapes/") { releases += 1 }
  }
  func snapshot() -> HeaderFacts {
    HeaderFacts(feeds: feeds, queries: queries, streams: streams, releases: releases)
  }
}

private struct HeaderFacts: Codable, Sendable {
  let feeds: Int
  let queries: Int
  let streams: Int
  let releases: Int
}

/// Holds both initial snapshots until each feed has exposed its durable head. The harness commits
/// its same-transaction source marker only after this named gate, then releases the two queries.
private actor InitialPairGate {
  private let directory: URL
  private var heads = 0
  private var initialQueries = 0
  private var released = false
  init(directory: URL) { self.directory = directory }
  func observe(_ request: URLRequest) async throws {
    if request.httpMethod == "HEAD" {
      heads += 1
      return
    }
    guard request.httpMethod == "POST", request.url?.path.contains("/v1/subsets/query") == true,
      initialQueries < 2
    else { return }
    initialQueries += 1
    if initialQueries == 2 {
      try write(
        GateFacts(heads: heads, initialQueries: initialQueries),
        to: directory.appendingPathComponent("initial-pair-gate.json"))
      try Data().write(to: directory.appendingPathComponent("initial-pair-queries-entered"))
    }
    while !released {
      if FileManager.default.fileExists(
        atPath: directory.appendingPathComponent("initial-pair-query-release").path)
      {
        released = true
      } else {
        try await Task.sleep(for: .milliseconds(10))
      }
    }
  }
}
private struct GateFacts: Codable {
  let heads: Int
  let initialQueries: Int
}

private struct GateTransport: HTTPTransport {
  let base: URLSessionTransport
  let facts: RequestFacts
  let gate: InitialPairGate
  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let prepared = base.prepare(request)
    try await facts.record(prepared)
    try await gate.observe(prepared)
    return try await base.send(prepared)
  }
}

private struct IssueView: Codable, Equatable, Sendable {
  let id: Int64
  let modified: Int64
  let username: String
  let title: String
  init(_ issue: Issue) {
    id = issue.id
    modified = issue.modified
    username = issue.username
    title = issue.title
  }
}
private struct Window: Codable, Sendable {
  let rows: [IssueView]
  let sessionRows: [IssueView]
  let cursor: StreamCursor
  let membershipCount: Int
}
private struct Observation: Codable, Sendable {
  let name: String
  let a: Window
  let b: Window
  let canonicalIssueCount: Int
  let canonicalIssueIDs: [Int64]
}
private struct Result: Codable, Sendable {
  let scopes: [String: String]
  let baseline: Observation
  let aInsert: Observation
  let reassignment: Observation
  let neutralUpdate: Observation
  let bAfterAClose: Observation
  let reopened: Observation
  let headers: HeaderFacts
}

@main
struct LinearLiteRealFilteredWindows {
  static func main() async {
    do { try await run() } catch {
      FileHandle.standardError.write(
        Data("linear-lite-real-filtered-windows failure: \(error)\n".utf8))
      exit(1)
    }
  }

  @MainActor
  private static func run() async throws {
    let env = ProcessInfo.processInfo.environment
    guard let baseURL = env["ECS_FILTERED_WINDOWS_BASE_URL"].flatMap(URL.init(string:)),
      let directory = env["ECS_FILTERED_WINDOWS_DIR"].map(URL.init(fileURLWithPath:)),
      let databaseURL = env["ECS_FILTERED_WINDOWS_DATABASE"].map(URL.init(fileURLWithPath:))
    else {
      throw QualificationError.missingEnvironment("ECS_FILTERED_WINDOWS_BASE_URL/DIR/DATABASE")
    }
    let database = try DatabaseQueue(path: databaseURL.path)
    let facts = RequestFacts(expectedHeader: "filtered-windows-v1")
    let transport = GateTransport(
      base: URLSessionTransport(headers: ["X-ECS-Qualification": "filtered-windows-v1"]),
      facts: facts, gate: InitialPairGate(directory: directory))
    let client = ElectricCircuitsClient(baseURL: baseURL, transport: transport)
    let principal = "filtered-windows-principal"
    let scopeA = MaterializationScope(
      principal: principal, template: "recent-subset:username=ada", subscription: "assignee-ada",
      generation: "v1")
    let scopeB = MaterializationScope(
      principal: principal, template: "recent-subset:username=bob", subscription: "assignee-bob",
      generation: "v1")
    let providerA = try LinearLiteShapeMaterializer(database: database, scope: scopeA)
    let providerB = try LinearLiteShapeMaterializer(database: database, scope: scopeB)
    let a = LinearLiteSession(
      client: client, database: database, subscription: scopeA.subscription, transport: transport,
      mode: .recentSubset(
        limit: 10, where: .leaf(column: "username", op: .eq, value: .string("ada"))),
      materializationScope: scopeA)
    let b = LinearLiteSession(
      client: client, database: database, subscription: scopeB.subscription, transport: transport,
      mode: .recentSubset(
        limit: 10, where: .leaf(column: "username", op: .eq, value: .string("bob"))),
      materializationScope: scopeB)

    let startA = Task { @MainActor in await a.start() }
    let startB = Task { @MainActor in await b.start() }
    try await waitForFile(
      directory.appendingPathComponent("initial-pair-queries-entered"),
      name: "two feed-before-snapshot queries")
    try await waitForFile(
      directory.appendingPathComponent("initial-pair-query-release"),
      name: "initial snapshot release")
    _ = await (startA.value, startB.value)
    try await waitFor("initial windows") {
      let aCursor = try? await providerA.currentCursor()
      let bCursor = try? await providerB.currentCursor()
      return a.connectionState == .streaming && b.connectionState == .streaming && aCursor != nil
        && bCursor != nil
    }
    let baseline = try await capture(
      "baseline", a: a, b: b, providerA: providerA, providerB: providerB, database: database,
      principal: principal)
    try recordReady("baseline", observation: baseline, directory: directory)

    let beforeAInsertA = try require(await providerA.currentCursor(), "A baseline cursor")
    let beforeAInsertB = try require(await providerB.currentCursor(), "B baseline cursor")
    try recordReady("a-insert", observation: baseline, directory: directory)
    try await waitForFile(
      directory.appendingPathComponent("a-insert-server-drained"), name: "A insert receipt")
    try await waitFor("A insert converged") {
      let aCursor = try await providerA.currentCursor()
      let bCursor = try await providerB.currentCursor()
      return aCursor != beforeAInsertA && bCursor == beforeAInsertB
    }
    let aInsert = try await capture(
      "a-insert", a: a, b: b, providerA: providerA, providerB: providerB, database: database,
      principal: principal)
    try recordObserved("a-insert", observation: aInsert, directory: directory)

    let beforeMoveA = try require(await providerA.currentCursor(), "A move cursor")
    let beforeMoveB = try require(await providerB.currentCursor(), "B move cursor")
    try recordReady("reassignment", observation: aInsert, directory: directory)
    try await waitForFile(
      directory.appendingPathComponent("reassignment-server-drained"), name: "reassignment receipt")
    try await waitFor("reassignment converged") {
      let aCursor = try await providerA.currentCursor()
      let bCursor = try await providerB.currentCursor()
      return aCursor != beforeMoveA && bCursor != beforeMoveB
    }
    let reassignment = try await capture(
      "reassignment", a: a, b: b, providerA: providerA, providerB: providerB, database: database,
      principal: principal)
    try recordObserved("reassignment", observation: reassignment, directory: directory)

    let beforeNeutralA = try require(await providerA.currentCursor(), "A neutral cursor")
    let beforeNeutralB = try require(await providerB.currentCursor(), "B neutral cursor")
    try recordReady("neutral-update", observation: reassignment, directory: directory)
    try await waitForFile(
      directory.appendingPathComponent("neutral-update-server-drained"),
      name: "neutral update receipt")
    try await waitFor("neutral update converged") {
      let aCursor = try await providerA.currentCursor()
      let bCursor = try await providerB.currentCursor()
      return aCursor == beforeNeutralA && bCursor != beforeNeutralB
    }
    let neutralUpdate = try await capture(
      "neutral-update", a: a, b: b, providerA: providerA, providerB: providerB, database: database,
      principal: principal)
    try recordObserved("neutral-update", observation: neutralUpdate, directory: directory)

    let cursorABeforeClose = try require(await providerA.currentCursor(), "A pre-close cursor")
    let cursorBBeforeClose = try require(await providerB.currentCursor(), "B pre-close cursor")
    await a.stop()
    guard a.connectionState == .stopped, b.connectionState == .streaming else {
      throw QualificationError.assertion("closing A did not preserve B's active session")
    }
    try Data().write(to: directory.appendingPathComponent("a-closed"))
    try recordReady("b-after-a-close", observation: neutralUpdate, directory: directory)
    try await waitForFile(
      directory.appendingPathComponent("b-after-a-close-server-drained"),
      name: "B after A close receipt")
    try await waitFor("B survives A close") {
      let aCursor = try await providerA.currentCursor()
      let bCursor = try await providerB.currentCursor()
      return aCursor == cursorABeforeClose && bCursor != cursorBBeforeClose
    }
    let bAfterAClose = try await capture(
      "b-after-a-close", a: a, b: b, providerA: providerA, providerB: providerB, database: database,
      principal: principal)
    try recordObserved("b-after-a-close", observation: bAfterAClose, directory: directory)

    await b.stop()
    let reopenedDatabase = try DatabaseQueue(path: databaseURL.path)
    let reopenedA = try LinearLiteShapeMaterializer(database: reopenedDatabase, scope: scopeA)
    let reopenedB = try LinearLiteShapeMaterializer(database: reopenedDatabase, scope: scopeB)
    let reopened = try await capture(
      "reopened", a: a, b: b, providerA: reopenedA, providerB: reopenedB,
      database: reopenedDatabase, principal: principal)
    guard reopened.a.rows == bAfterAClose.a.rows, reopened.b.rows == bAfterAClose.b.rows,
      reopened.a.cursor == bAfterAClose.a.cursor, reopened.b.cursor == bAfterAClose.b.cursor
    else {
      throw QualificationError.assertion("reopened providers did not retain isolated scope state")
    }
    let headers = await facts.snapshot()
    guard headers.feeds >= 2, headers.queries >= 2, headers.streams >= 2, headers.releases == 2
    else {
      throw QualificationError.assertion("missing header-bearing native route or release evidence")
    }
    try write(
      Result(
        scopes: ["a": scopeA.storageKey, "b": scopeB.storageKey], baseline: baseline,
        aInsert: aInsert, reassignment: reassignment, neutralUpdate: neutralUpdate,
        bAfterAClose: bAfterAClose, reopened: reopened, headers: headers),
      to: directory.appendingPathComponent("swift-filtered-windows-result.json"))
  }

  @MainActor
  private static func capture(
    _ name: String, a: LinearLiteSession, b: LinearLiteSession,
    providerA: LinearLiteShapeMaterializer, providerB: LinearLiteShapeMaterializer,
    database: DatabaseQueue, principal: String
  ) async throws -> Observation {
    let aRows = try await providerA.allIssues(order: .modifiedDescending).map(IssueView.init)
    let bRows = try await providerB.allIssues(order: .modifiedDescending).map(IssueView.init)
    let aCursor = try require(await providerA.currentCursor(), "\(name) A cursor")
    let bCursor = try require(await providerB.currentCursor(), "\(name) B cursor")
    let aShapeID = await providerA.shapeID
    let bShapeID = await providerB.shapeID
    let counts = try await database.read { db -> (Int, Int, Int, [Int64]) in
      let aCount =
        try Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?",
          arguments: [aShapeID]) ?? 0
      let bCount =
        try Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?",
          arguments: [bShapeID]) ?? 0
      let ids = try Int64.fetchAll(
        db, sql: "SELECT id FROM issues WHERE principal_id = ? ORDER BY id", arguments: [principal])
      return (aCount, bCount, ids.count, ids)
    }
    let aWindow = Window(
      rows: aRows, sessionRows: a.issues.map(IssueView.init), cursor: aCursor,
      membershipCount: counts.0)
    let bWindow = Window(
      rows: bRows, sessionRows: b.issues.map(IssueView.init), cursor: bCursor,
      membershipCount: counts.1)
    try assertWindow(aWindow, name: "\(name) A")
    try assertWindow(bWindow, name: "\(name) B")
    return Observation(
      name: name, a: aWindow, b: bWindow, canonicalIssueCount: counts.2, canonicalIssueIDs: counts.3
    )
  }
}

private func assertWindow(_ window: Window, name: String) throws {
  guard window.rows == window.sessionRows, window.rows.count == 10, window.membershipCount == 10,
    Set(window.rows.map(\.id)).count == 10
  else {
    throw QualificationError.assertion(
      "\(name) did not expose exactly one isolated recent-10 window")
  }
}
private func recordReady(_ name: String, observation: Observation, directory: URL) throws {
  try write(observation, to: directory.appendingPathComponent("\(name)-client-ready.json"))
  try Data().write(to: directory.appendingPathComponent("\(name)-client-ready"))
}
private func recordObserved(_ name: String, observation: Observation, directory: URL) throws {
  try write(observation, to: directory.appendingPathComponent("\(name)-observed.json"))
  try Data().write(to: directory.appendingPathComponent("\(name)-observed"))
}
private func write<T: Encodable>(_ value: T, to url: URL) throws {
  let encoder = JSONEncoder()
  encoder.outputFormatting = [.sortedKeys]
  try encoder.encode(value).write(to: url, options: .atomic)
}
private func require<T>(_ value: T?, _ name: String) throws -> T {
  guard let value else { throw QualificationError.assertion("missing \(name)") }
  return value
}
@MainActor private func waitForFile(_ url: URL, name: String) async throws {
  try await waitFor(name) { FileManager.default.fileExists(atPath: url.path) }
}
@MainActor private func waitFor(
  _ name: String, condition: @escaping @MainActor @Sendable () async throws -> Bool
) async throws {
  let deadline = ContinuousClock.now + .seconds(45)
  while ContinuousClock.now < deadline {
    if try await condition() { return }
    try await Task.sleep(for: .milliseconds(25))
  }
  throw QualificationError.deadline(name)
}
