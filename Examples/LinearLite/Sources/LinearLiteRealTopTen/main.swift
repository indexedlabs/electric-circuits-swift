import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import LinearLiteGRDB

// This executable is intentionally a qualification adapter, not application infrastructure. It
// drives the public `LinearLiteSession.recentSubset(limit: 10)` path over the real native routes.

private enum QualificationError: Error {
  case missingEnvironment(String)
  case deadline(String)
  case assertion(String)
}

private actor RequestFacts {
  private var nativeControl = false
  private var subsetQuery = false
  private var durableStream = false
  private var release = false
  private var streamResponses: [String] = []

  func record(_ request: URLRequest) throws {
    guard request.value(forHTTPHeaderField: "X-ECS-Qualification") == "real-top10" else {
      throw QualificationError.assertion(
        "qualification header missing from \(request.url?.absoluteString ?? "request")")
    }
    let path = request.url?.path ?? ""
    if path.contains("/v1/subset-feeds") { nativeControl = true }
    if path.contains("/v1/subsets/query") { subsetQuery = true }
    if request.httpMethod == "DELETE", path.contains("/v1/shapes/") { release = true }
    if request.url?.query?.contains("live=long-poll") == true { durableStream = true }
  }

  func snapshot() -> HeaderFacts {
    HeaderFacts(
      nativeControl: nativeControl, subsetQuery: subsetQuery, durableStream: durableStream,
      release: release)
  }

  func record(_ response: HTTPResponse, for request: URLRequest) {
    guard request.url?.query?.contains("live=long-poll") == true else { return }
    // Qualification artifacts must not capture application rows or authentication material. Keep
    // only the public progress facts needed to diagnose a stream/cursor transition.
    let batch = try? JSONDecoder().decode(ChangeBatch.self, from: response.data)
    let operations =
      batch?.envelopes
      .map { $0.headers.operation.rawValue }
      .joined(separator: ",") ?? "none"
    let lsns = batch?.envelopes.compactMap(\.headers.lsn).joined(separator: ",") ?? "none"
    let requestedOffset =
      URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
      .queryItems?
      .first(where: { $0.name == "offset" })?
      .value ?? "nil"
    streamResponses.append(
      "path=\(request.url?.path ?? "nil") requested=\(requestedOffset) "
        + "next=\(response.response.value(forHTTPHeaderField: "stream-next-offset") ?? "nil") "
        + "status=\(response.response.statusCode) count=\(batch?.envelopes.count ?? 0) "
        + "operations=\(operations) lsns=\(lsns)")
  }

  func streamTrace() -> String { streamResponses.joined(separator: "\n") }
}

private struct FeedTrace: Codable {
  let id: String
  let streamPath: String
}

private actor InitialSnapshotGate {
  private let directory: URL
  private var used = false

  init(directory: URL) { self.directory = directory }

  func observe(_ request: URLRequest) async throws {
    guard !used else { return }
    let path = request.url?.path ?? ""
    // `streamCursor(for:)` is the only HEAD issued by this executable. The returned durable URL
    // may carry server-owned query parameters, so the method (not URL shape) is the stable gate.
    if request.httpMethod == "HEAD" {
      try Data().write(to: directory.appendingPathComponent("feed-cursor-captured"))
      return
    }
    guard request.httpMethod == "POST", path.contains("/v1/subsets/query") else { return }
    used = true
    try Data().write(to: directory.appendingPathComponent("subset-query-entered"))
    try await waitForFile(
      directory.appendingPathComponent("subset-query-release"), name: "subset query release")
  }

  func observe(_ response: HTTPResponse, for request: URLRequest) throws {
    guard request.httpMethod == "POST", request.url?.path.contains("/v1/subset-feeds") == true
    else { return }
    let handle = try JSONDecoder().decode(ShapeResponse.self, from: response.data)
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    try encoder.encode(FeedTrace(id: handle.shapeId, streamPath: handle.streamPath)).write(
      to: directory.appendingPathComponent("feed-handle.json"), options: .atomic)
  }
}

private struct GateTransport: HTTPTransport {
  let base: URLSessionTransport
  let facts: RequestFacts
  let gate: InitialSnapshotGate

  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let prepared = base.prepare(request)
    try await facts.record(prepared)
    try await gate.observe(prepared)
    let response = try await base.send(prepared)
    try await gate.observe(response, for: prepared)
    await facts.record(response, for: prepared)
    return response
  }
}

private struct HeaderFacts: Codable, Sendable {
  let nativeControl: Bool
  let subsetQuery: Bool
  let durableStream: Bool
  let release: Bool
}

private struct IssueView: Codable, Equatable, Sendable {
  let id: Int64
  let modified: Int64
  let title: String

  init(_ issue: Issue) {
    id = issue.id
    modified = issue.modified
    title = issue.title
  }
}

private struct PhaseResult: Codable, Sendable {
  let name: String
  let sessionRows: [IssueView]
  let grdbRows: [IssueView]
  let cursor: StreamCursor
  let membershipCount: Int
  let liveBatchCount: Int
}

private struct PhaseClientReady: Codable, Sendable {
  let cursor: StreamCursor
  let liveBatchCount: Int
}

private struct Result: Codable, Sendable {
  let baseline: PhaseResult
  let phases: [PhaseResult]
  let reopenedRows: [IssueView]
  let reopenedCursor: StreamCursor
  let reopenedMembershipCount: Int
  let headers: HeaderFacts
}

@main
struct LinearLiteRealTopTen {
  static func main() async {
    do {
      try await run()
    } catch {
      FileHandle.standardError.write(Data("linear-lite-real-top10 failure: \(error)\n".utf8))
      exit(1)
    }
  }

  @MainActor
  private static func run() async throws {
    let environment = ProcessInfo.processInfo.environment
    guard let baseURL = environment["ECS_REAL_TOP10_BASE_URL"].flatMap(URL.init(string:)),
      let phaseDirectory = environment["ECS_REAL_TOP10_DIR"].map(URL.init(fileURLWithPath:)),
      let databaseURL = environment["ECS_REAL_TOP10_DATABASE"].map(URL.init(fileURLWithPath:))
    else {
      throw QualificationError.missingEnvironment(
        "ECS_REAL_TOP10_BASE_URL/ECS_REAL_TOP10_DIR/ECS_REAL_TOP10_DATABASE")
    }

    let subscription = "swift-real-top10"
    let database = try DatabaseQueue(path: databaseURL.path)
    let facts = RequestFacts()
    let gate = InitialSnapshotGate(directory: phaseDirectory)
    let transport = GateTransport(
      base: URLSessionTransport(headers: ["X-ECS-Qualification": "real-top10"]), facts: facts,
      gate: gate)
    let client = ElectricCircuitsClient(baseURL: baseURL, transport: transport)
    let session = LinearLiteSession(
      client: client, database: database, subscription: subscription, transport: transport,
      mode: .recentSubset(limit: 10))
    let provider = try LinearLiteShapeMaterializer(
      database: database, shapeID: "recent-subset-\(subscription)")

    await session.start()
    do {
      try await waitFor(
        "initial snapshot after overlapping feed boundary",
        condition: {
          guard session.connectionState == .streaming,
            (try? await provider.currentCursor()) != nil
          else { return false }
          // The overlap mutation is intentionally allowed to be represented by the real snapshot
          // rather than an extra feed delivery. Exact SQL membership plus the GRDB unique view key
          // below establish one converged copy without depending on delivery timing.
          return session.issues.count == 10
        })
    } catch {
      let debug = [
        "state": String(describing: session.connectionState),
        "issues": String(session.issues.count),
        "events": session.syncEvents.map { "\($0.kind.rawValue):\($0.detail)" }.joined(
          separator: " | "),
        "cursor": String(describing: try? await provider.currentCursor()),
      ]
      try write(debug, to: phaseDirectory.appendingPathComponent("initial-debug.json"))
      throw error
    }
    let baseline = try await capture(
      name: "snapshot-boundary", session: session, provider: provider, database: database)
    try write(baseline, to: phaseDirectory.appendingPathComponent("baseline-observed.json"))
    try Data().write(to: phaseDirectory.appendingPathComponent("baseline-observed"))

    var phases: [PhaseResult] = []
    for name in ["newest-insert", "outside-update", "move-below-boundary", "delete-promotes"] {
      let beforeBatches = session.syncEvents.filter({ $0.kind == .liveBatchApplied }).count
      let beforeCursor = try require(await provider.currentCursor(), "\(name) pre-commit cursor")
      try write(
        PhaseClientReady(cursor: beforeCursor, liveBatchCount: beforeBatches),
        to: phaseDirectory.appendingPathComponent("\(name)-client-ready.json"))
      try Data().write(to: phaseDirectory.appendingPathComponent("\(name)-client-ready"))
      try await waitForFile(
        phaseDirectory.appendingPathComponent("\(name)-server-drained"),
        name: "\(name) drain receipt")
      do {
        try await waitFor(
          "\(name) live materialization",
          condition: {
            let current = try? await provider.currentCursor()
            return current != beforeCursor
              && session.syncEvents.filter({ $0.kind == .liveBatchApplied }).count > beforeBatches
          })
      } catch {
        let debug = [
          "state": String(describing: session.connectionState),
          "issues": String(session.issues.count),
          "events": session.syncEvents.map { "\($0.kind.rawValue):\($0.detail)" }.joined(
            separator: " | "),
          "beforeCursor": String(describing: beforeCursor),
          "cursor": String(describing: try? await provider.currentCursor()),
          "streamTrace": await facts.streamTrace(),
        ]
        try write(debug, to: phaseDirectory.appendingPathComponent("\(name)-debug.json"))
        throw error
      }
      let result = try await capture(
        name: name, session: session, provider: provider, database: database)
      phases.append(result)
      try write(result, to: phaseDirectory.appendingPathComponent("\(name)-observed.json"))
      try Data().write(to: phaseDirectory.appendingPathComponent("\(name)-observed"))
    }

    let finalCursor = try require(await provider.currentCursor(), "final GRDB cursor")
    let finalRows = try await provider.allIssues(order: .modifiedDescending).map(IssueView.init)
    await session.stop()
    let headers = await facts.snapshot()
    guard headers.nativeControl, headers.subsetQuery, headers.durableStream, headers.release else {
      throw QualificationError.assertion(
        "missing header-bearing native control, subset, stream, or release request")
    }

    // A new queue/provider sees the exact committed rows and checkpoint from the file-backed GRDB
    // transaction. `membershipCount` catches duplicate view memberships independently of the UI.
    let reopenedDatabase = try DatabaseQueue(path: databaseURL.path)
    let reopened = try LinearLiteShapeMaterializer(
      database: reopenedDatabase, shapeID: "recent-subset-\(subscription)")
    let reopenedCursor = try require(await reopened.currentCursor(), "reopened GRDB cursor")
    let reopenedRows = try await reopened.allIssues(order: .modifiedDescending).map(IssueView.init)
    let reopenedMembershipCount = try await reopenedDatabase.read { db in
      try Int.fetchOne(
        db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?",
        arguments: ["recent-subset-\(subscription)"]) ?? 0
    }
    guard reopenedRows == finalRows, reopenedCursor == finalCursor,
      reopenedMembershipCount == reopenedRows.count
    else {
      throw QualificationError.assertion(
        "reopened GRDB state was not an exact, duplicate-free final view")
    }

    try write(
      Result(
        baseline: baseline, phases: phases, reopenedRows: reopenedRows,
        reopenedCursor: reopenedCursor,
        reopenedMembershipCount: reopenedMembershipCount, headers: headers),
      to: phaseDirectory.appendingPathComponent("swift-result.json"))
  }

  @MainActor
  private static func capture(
    name: String,
    session: LinearLiteSession,
    provider: LinearLiteShapeMaterializer,
    database: DatabaseQueue
  ) async throws -> PhaseResult {
    let cursor = try require(await provider.currentCursor(), "\(name) cursor")
    let grdbRows = try await provider.allIssues(order: .modifiedDescending).map(IssueView.init)
    let membershipCount = try await database.read { db in
      try Int.fetchOne(
        db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE view_id = ?",
        arguments: ["recent-subset-swift-real-top10"]) ?? 0
    }
    return PhaseResult(
      name: name, sessionRows: session.issues.map(IssueView.init), grdbRows: grdbRows,
      cursor: cursor,
      membershipCount: membershipCount,
      liveBatchCount: session.syncEvents.filter({ $0.kind == .liveBatchApplied }).count)
  }

  private static func write<T: Encodable>(_ value: T, to url: URL) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    try encoder.encode(value).write(to: url, options: .atomic)
  }
}

private func require<T>(_ value: T?, _ name: String) throws -> T {
  guard let value else { throw QualificationError.assertion("missing \(name)") }
  return value
}

@MainActor
private func waitForFile(_ url: URL, name: String) async throws {
  try await waitFor(name, condition: { FileManager.default.fileExists(atPath: url.path) })
}

@MainActor
private func waitFor(
  _ name: String, condition: @escaping @MainActor @Sendable () async -> Bool
) async throws {
  let deadline = ContinuousClock.now + .seconds(30)
  while ContinuousClock.now < deadline {
    if await condition() { return }
    try await Task.sleep(for: .milliseconds(25))
  }
  throw QualificationError.deadline(name)
}
