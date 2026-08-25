import ElectricCircuitsSwift
import Foundation

private struct PersistedStore: Codable {
  var rows: [String: ChangeRow]
  var cursor: StreamCursor?
}

/// A deliberately small file-backed provider used only by the qualification executable. Its one
/// atomic replacement is the concrete store transaction for rows plus cursor; it is not core API.
private actor FileMaterializer: ShapeMaterializer {
  private let url: URL
  private var state: PersistedStore
  private var appliedBatches = 0
  private var appliedEvents = 0

  init(url: URL) throws {
    self.url = url
    if let data = try? Data(contentsOf: url) {
      state = try JSONDecoder().decode(PersistedStore.self, from: data)
    } else {
      state = PersistedStore(rows: [:], cursor: nil)
    }
  }

  func currentCursor() async throws -> StreamCursor? { state.cursor }
  func rows() -> [String: ChangeRow] { state.rows }
  func applicationCounts() -> (batches: Int, events: Int) { (appliedBatches, appliedEvents) }

  func apply(
    _ batch: ChangeBatch, expecting expected: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    if state.cursor == cursor { return }
    guard state.cursor == expected else {
      throw StreamError.cursorConflict(
        expected: expected, actual: state.cursor, advancingTo: cursor)
    }
    var next = state.rows
    for event in batch.envelopes {
      switch event.headers.operation {
      case .delete: next.removeValue(forKey: event.key)
      case .insert, .update, .upsert:
        guard let value = event.value else { throw StreamError.missingValue(key: event.key) }
        next[event.key] = value
      }
    }
    let candidate = PersistedStore(rows: next, cursor: cursor)
    // `Data.write(.atomic)` makes the completed row map and cursor visible together.
    try JSONEncoder().encode(candidate).write(to: url, options: .atomic)
    state = candidate
    appliedBatches += 1
    appliedEvents += batch.envelopes.count
  }
}

private actor RequestAudit {
  private var control = false
  private var stream = false
  private var firstStreamOffset: String?
  private var streamRequestCount = 0
  func record(_ request: URLRequest) {
    guard request.value(forHTTPHeaderField: "X-ECS-Qualification") == "present" else { return }
    if request.url?.path.contains("/v1/shapes") == true { control = true }
    if request.url?.query?.contains("live=long-poll") == true {
      stream = true
      streamRequestCount += 1
      if firstStreamOffset == nil {
        firstStreamOffset =
          URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
          .queryItems?.first(where: { $0.name == "offset" })?.value
      }
    }
  }
  func facts() -> (Bool, Bool, String?, Int) {
    (control, stream, firstStreamOffset, streamRequestCount)
  }
}

private struct AuditingTransport: HTTPTransport {
  let base: URLSessionTransport
  let audit: RequestAudit
  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let prepared = base.prepare(request)
    await audit.record(prepared)
    return try await base.send(prepared)
  }
}

private struct Result: Codable {
  let rows: [String: ChangeRow]
  let cursor: StreamCursor
  let controlHeaderInjected: Bool
  let streamHeaderInjected: Bool
  let replayRequestedAt: String
  let preTerminalCursor: StreamCursor
  let replayAppliedBatches: Int
  let replayAppliedEvents: Int
}

private enum HarnessError: Error {
  case missingEnvironment(String)
  case deadline(String)
  case assertion(String)
}

@main
struct ElectricCircuitsSwiftRealStack {
  static func main() async {
    do { try await run() } catch {
      FileHandle.standardError.write(Data("swift-real-stack failure: \(error)\n".utf8))
      exit(1)
    }
  }

  private static func run() async throws {
    let env = ProcessInfo.processInfo.environment
    guard let base = env["ECS_REAL_STACK_BASE_URL"].flatMap(URL.init(string:)),
      let dir = env["ECS_REAL_STACK_DIR"].map(
        URL.init(fileURLWithPath:)
      )
    else { throw HarnessError.missingEnvironment("ECS_REAL_STACK_BASE_URL/ECS_REAL_STACK_DIR") }
    let mutationGate = dir.appendingPathComponent("mutation-committed")
    let serverDrained = dir.appendingPathComponent("server-drained")
    let baselineReady = dir.appendingPathComponent("baseline-ready")
    let resultURL = dir.appendingPathComponent("swift-result.json")
    let storeURL = dir.appendingPathComponent("materialized-store.json")
    let preTerminalStoreURL = dir.appendingPathComponent("pre-terminal-materialized-store.json")
    let audit = RequestAudit()
    let transport = AuditingTransport(
      base: URLSessionTransport(headers: ["X-ECS-Qualification": "present"]), audit: audit)
    let materializer = try FileMaterializer(url: storeURL)
    let client = ElectricCircuitsClient(baseURL: base, transport: transport)
    let handle = try await client.createShape(
      ShapeRequest(table: "items", subscription: "swift-real-stack-v2"))
    let reader = ShapeStreamReader(
      streamURL: handle.stream.url, transport: transport, materializer: materializer)
    let readerTask = Task { try await reader.run() }
    defer { readerTask.cancel() }

    try await eventually("baseline materialization") {
      let rows = await materializer.rows()
      return rows["1"]?["title"] == .string("before") && rows["2"]?["title"] == .string("delete-me")
    }
    try Data().write(to: baselineReady)
    guard let preTerminalCursor = try await materializer.currentCursor() else {
      throw HarnessError.assertion("baseline did not durably commit a cursor")
    }
    try FileManager.default.copyItem(at: storeURL, to: preTerminalStoreURL)
    try await eventually("source mutation gate") {
      FileManager.default.fileExists(atPath: mutationGate.path)
    }
    try await eventually("server drain receipt") {
      FileManager.default.fileExists(atPath: serverDrained.path)
    }
    if env["ECS_REAL_STACK_INJECT_HANG"] == "1" {
      try await Task.sleep(for: .seconds(3_600))
    }
    try await eventually("final materialization") {
      let rows = await materializer.rows()
      return rows.count == 2 && rows["1"]?["title"] == .string("after")
        && rows["3"]?["title"] == .string("created")
    }
    guard let cursor = try await materializer.currentCursor() else {
      throw HarnessError.assertion("no durable cursor")
    }
    readerTask.cancel()
    _ = await readerTask.result

    // Reopen the persisted baseline, not the terminal state. The real stream must replay the
    // committed insert/update/delete tail from its older durable cursor and converge again.
    let reopened = try FileMaterializer(url: preTerminalStoreURL)
    let replayAudit = RequestAudit()
    let replayReader = ShapeStreamReader(
      streamURL: handle.stream.url,
      transport: AuditingTransport(
        base: URLSessionTransport(headers: ["X-ECS-Qualification": "present"]), audit: replayAudit),
      materializer: reopened)
    let replayTask = Task {
      do { try await replayReader.run() } catch {
        FileHandle.standardError.write(
          Data("swift-real-stack replay reader failure: \(error)\n".utf8))
        throw error
      }
    }
    try await eventually("older-cursor replay request") {
      (await replayAudit.facts()).2 == preTerminalCursor.offset
    }
    try await eventually("older-cursor replay application") {
      let rows = await reopened.rows()
      let counts = await reopened.applicationCounts()
      let replayCursor = try? await reopened.currentCursor()
      return counts.batches > 0 && counts.events > 0 && replayCursor == cursor
        && rows.count == 2 && rows["1"]?["title"] == .string("after")
        && rows["3"]?["title"] == .string("created")
    }
    replayTask.cancel()
    _ = await replayTask.result
    let reopenedRows = await reopened.rows()
    let materializedRows = await materializer.rows()
    guard reopenedRows == materializedRows else {
      throw HarnessError.assertion("replay did not converge to final key map")
    }
    guard try await reopened.currentCursor() == cursor else {
      throw HarnessError.assertion("replay did not converge to final cursor")
    }
    let replayCounts = await reopened.applicationCounts()
    let facts = await audit.facts()
    guard facts.0 && facts.1 else {
      throw HarnessError.assertion("auth/header injection missing on control or stream request")
    }
    try JSONEncoder().encode(
      Result(
        rows: reopenedRows, cursor: cursor, controlHeaderInjected: facts.0,
        streamHeaderInjected: facts.1,
        replayRequestedAt: preTerminalCursor.offset, preTerminalCursor: preTerminalCursor,
        replayAppliedBatches: replayCounts.batches, replayAppliedEvents: replayCounts.events
      )
    ).write(to: resultURL, options: .atomic)
    try await client.releaseShape(handle)
  }

  private static func eventually(_ name: String, _ predicate: @escaping @Sendable () async -> Bool)
    async throws
  {
    let deadline = ContinuousClock.now + .seconds(30)
    while ContinuousClock.now < deadline {
      if await predicate() { return }
      try await Task.sleep(for: .milliseconds(25))
    }
    throw HarnessError.deadline(name)
  }
}
