import ElectricCircuitsSwift
import Foundation

private struct PersistedStore: Codable {
  var rows: [String: ChangeRow]
  var cursor: StreamCursor?
  /// A durable history of successfully committed checkpoints. This lets the qualification prove
  /// that replay/retry did not apply one checkpoint more than once, including after reopening.
  var appliedCursors: [String]
}

/// A qualification-only provider. Its single atomic replacement is the durable transaction seam
/// promised by `ShapeMaterializer`: rows and the cursor become visible together.
private actor FileMaterializer: ShapeMaterializer {
  private let url: URL
  private var state: PersistedStore

  init(url: URL) throws {
    self.url = url
    if let data = try? Data(contentsOf: url) {
      state = try JSONDecoder().decode(PersistedStore.self, from: data)
    } else {
      state = PersistedStore(rows: [:], cursor: nil, appliedCursors: [])
    }
  }

  func currentCursor() async throws -> StreamCursor? { state.cursor }
  func rows() -> [String: ChangeRow] { state.rows }
  func appliedCursors() -> [String] { state.appliedCursors }

  func apply(
    _ batch: ChangeBatch, expecting expected: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    // A delivered replay of an already committed checkpoint is not applied again.
    if state.cursor == cursor { return }
    guard state.cursor == expected else {
      throw StreamError.cursorConflict(
        expected: expected, actual: state.cursor, advancingTo: cursor)
    }
    var nextRows = state.rows
    for envelope in batch.envelopes {
      switch envelope.headers.operation {
      case .delete:
        nextRows.removeValue(forKey: envelope.key)
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        nextRows[envelope.key] = value
      }
    }
    var next = state
    next.rows = nextRows
    next.cursor = cursor
    next.appliedCursors.append(cursor.offset)
    try JSONEncoder().encode(next).write(to: url, options: .atomic)
    state = next
  }
}

private actor RequestAudit {
  private var control = false
  private var stream = false
  private var release = false

  func record(_ request: URLRequest) {
    guard request.value(forHTTPHeaderField: "X-ECS-Qualification") == "engine-ds-outage",
      request.value(forHTTPHeaderField: "X-ECS-Principal") == "swift-layer-b"
    else { return }
    if request.url?.path.contains("/v1/shapes") == true {
      if request.httpMethod == "DELETE" { release = true } else { control = true }
    }
    if request.url?.query?.contains("live=long-poll") == true { stream = true }
  }

  func facts() -> (control: Bool, stream: Bool, release: Bool) { (control, stream, release) }
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

private struct PhaseReceipt: Codable {
  let name: String
  let rows: [String: ChangeRow]
  let cursor: StreamCursor
}

private struct Result: Codable {
  let rows: [String: ChangeRow]
  let baselineCursor: StreamCursor
  let engineRestartCursor: StreamCursor
  let durableStreamsCursor: StreamCursor
  let appliedCursors: [String]
  let controlHeaderForwarded: Bool
  let streamHeaderForwarded: Bool
  let releaseHeaderForwarded: Bool
  let durableStreamsRetryObserved: Bool
  let reopenedRowsMatch: Bool
  let reopenedCursorMatch: Bool
}

private enum HarnessError: Error {
  case missingEnvironment(String)
  case deadline(String)
  case assertion(String)
}

@main
struct ElectricCircuitsSwiftEngineDSOutage {
  static func main() async {
    do { try await run() } catch {
      FileHandle.standardError.write(Data("swift-engine-ds-outage failure: \(error)\n".utf8))
      exit(1)
    }
  }

  private static func run() async throws {
    let environment = ProcessInfo.processInfo.environment
    guard let baseURL = environment["ECS_OUTAGE_BASE_URL"].flatMap(URL.init(string:)),
      let phaseDirectory = environment["ECS_OUTAGE_PHASE_DIR"].map(URL.init(fileURLWithPath:))
    else { throw HarnessError.missingEnvironment("ECS_OUTAGE_BASE_URL/ECS_OUTAGE_PHASE_DIR") }

    func path(_ name: String) -> URL { phaseDirectory.appendingPathComponent(name) }
    let materializerURL = path("swift-materialized.json")
    let resultURL = path("swift-result.json")
    let retryReceiptURL = path("durable-streams-client-retrying")
    let audit = RequestAudit()
    let transport = AuditingTransport(
      base: URLSessionTransport(headers: [
        "X-ECS-Qualification": "engine-ds-outage", "X-ECS-Principal": "swift-layer-b",
      ]),
      audit: audit)
    let materializer = try FileMaterializer(url: materializerURL)
    let request = ShapeRequest(table: "items", subscription: "swift-engine-ds-outage-v1")
    let client = ElectricCircuitsClient(baseURL: baseURL, transport: transport)
    let coordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport, request: request, materializer: materializer,
      retryPolicy: .init(
        maxRetries: 12, baseDelay: .milliseconds(80), maximumDelay: .seconds(1), jitterRatio: 0)
    )
    let stateUpdates = await coordinator.stateUpdates
    let stateReceiptTask = Task {
      for await state in stateUpdates {
        if case .waitingToRetry(operation: "stream", attempt: _, delay: _) = state {
          try? Data().write(to: retryReceiptURL, options: .atomic)
        }
      }
    }
    defer { stateReceiptTask.cancel() }

    _ = try await coordinator.start()
    try await eventually("baseline client/provider receipt") {
      let rows = await materializer.rows()
      return rows["1"]?["title"] == .string("before")
        && rows["2"]?["title"] == .string("delete-me")
    }
    guard let baselineCursor = try await materializer.currentCursor() else {
      throw HarnessError.assertion("baseline did not durably commit a cursor")
    }
    try writeReceipt(
      PhaseReceipt(name: "baseline", rows: await materializer.rows(), cursor: baselineCursor),
      to: path("baseline-client-provider-receipt.json"))

    try await waitForFile(
      path("engine-restart-mutation-committed"), name: "engine restart source marker")
    try await waitForFile(
      path("engine-restart-server-receipt"), name: "engine restart server receipt")
    try await eventually("engine restart client/provider receipt") {
      let rows = await materializer.rows()
      return rows["1"]?["title"] == .string("after-engine")
        && rows["3"]?["title"] == .string("engine-created")
    }
    guard let engineRestartCursor = try await materializer.currentCursor(),
      engineRestartCursor != baselineCursor
    else {
      throw HarnessError.assertion("engine restart did not advance the durable client cursor")
    }
    try writeReceipt(
      PhaseReceipt(
        name: "engine-restart", rows: await materializer.rows(), cursor: engineRestartCursor),
      to: path("engine-restart-client-provider-receipt.json"))

    try await waitForFile(path("durable-streams-outage-started"), name: "durable-streams outage")
    try await waitForFile(retryReceiptURL, name: "Swift durable-streams retry receipt")
    try await waitForFile(
      path("durable-streams-mutation-committed"), name: "durable-streams source marker")
    try await waitForFile(
      path("durable-streams-server-receipt"), name: "durable-streams server receipt")
    try await eventually("durable-streams client/provider receipt") {
      let rows = await materializer.rows()
      return rows.count == 3 && rows["1"]?["title"] == .string("after-ds")
        && rows["3"]?["title"] == .string("engine-created")
        && rows["4"]?["title"] == .string("ds-created") && rows["2"] == nil
    }
    guard let durableStreamsCursor = try await materializer.currentCursor(),
      durableStreamsCursor != engineRestartCursor
    else {
      throw HarnessError.assertion("durable-streams recovery did not advance the durable cursor")
    }
    try writeReceipt(
      PhaseReceipt(
        name: "durable-streams", rows: await materializer.rows(), cursor: durableStreamsCursor),
      to: path("durable-streams-client-provider-receipt.json"))

    try await coordinator.stop()
    let reopened = try FileMaterializer(url: materializerURL)
    let finalRows = await materializer.rows()
    let reopenedRows = await reopened.rows()
    let appliedCursors = await reopened.appliedCursors()
    let facts = await audit.facts()
    guard Set(appliedCursors).count == appliedCursors.count else {
      throw HarnessError.assertion("provider durably recorded duplicate checkpoint application")
    }
    guard facts.control && facts.stream && facts.release else {
      throw HarnessError.assertion(
        "custom auth/header forwarding missing on control, stream, or release")
    }
    let reopenedCursor = try await reopened.currentCursor()
    let result = Result(
      rows: finalRows, baselineCursor: baselineCursor, engineRestartCursor: engineRestartCursor,
      durableStreamsCursor: durableStreamsCursor, appliedCursors: appliedCursors,
      controlHeaderForwarded: facts.control, streamHeaderForwarded: facts.stream,
      releaseHeaderForwarded: facts.release,
      durableStreamsRetryObserved: FileManager.default.fileExists(atPath: retryReceiptURL.path),
      reopenedRowsMatch: reopenedRows == finalRows,
      reopenedCursorMatch: reopenedCursor == durableStreamsCursor)
    guard result.reopenedRowsMatch && result.reopenedCursorMatch else {
      throw HarnessError.assertion(
        "reopened file provider did not preserve rows and cursor atomically")
    }
    try JSONEncoder().encode(result).write(to: resultURL, options: .atomic)
  }

  private static func writeReceipt(_ receipt: PhaseReceipt, to url: URL) throws {
    try JSONEncoder().encode(receipt).write(to: url, options: .atomic)
  }

  private static func waitForFile(_ url: URL, name: String) async throws {
    try await eventually(name) { FileManager.default.fileExists(atPath: url.path) }
  }

  private static func eventually(_ name: String, _ predicate: @escaping @Sendable () async -> Bool)
    async throws
  {
    let deadline = ContinuousClock.now + .seconds(45)
    while ContinuousClock.now < deadline {
      if await predicate() { return }
      try await Task.sleep(for: .milliseconds(25))
    }
    throw HarnessError.deadline(name)
  }
}
