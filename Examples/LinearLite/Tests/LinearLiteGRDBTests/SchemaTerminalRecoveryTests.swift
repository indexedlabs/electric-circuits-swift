import ElectricCircuitsSwift
import Foundation
import GRDB
import Testing

@testable import LinearLiteGRDB

private actor SchemaRecoveryTransport: HTTPTransport {
  private var events: Set<String> = []
  private var eventWaiters: [String: [CheckedContinuation<Void, Never>]] = [:]
  private var parkedPolls: [String: CheckedContinuation<Void, Never>] = [:]
  private var cancelledParkedPolls: Set<String> = []
  private var requests: [URLRequest] = []
  private var creates = 0

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let id = request.url?.path.split(separator: "/").last.map(String.init) ?? "unknown"
    switch request.httpMethod {
    case "POST":
      creates += 1
      let shapeID = creates == 1 ? "shape-old" : "shape-new"
      record("claim-created:\(shapeID)")
      return schemaRecoveryResponse(
        url: request.url!,
        body: """
          {
            "shapeId": "\(shapeID)",
            "table": "public.issues",
            "streamPath": "/v1/streams/\(shapeID)",
            "streamUrl": "https://streams.test/v1/streams/\(shapeID)"
          }
          """)
    case "GET" where id == "shape-old":
      // Native durable-stream retirement is 409 with stream-closed, 404, or 410. This is the
      // close-before-delete signal documented for schema drift; it is not an invented schema API.
      record("terminal:shape-old")
      return schemaRecoveryResponse(
        url: request.url!, status: 409, headers: ["stream-closed": "true"])
    case "GET" where id == "shape-new":
      record("poll:shape-new")
      await waitForParkedPoll(id)
      try Task.checkCancellation()
      throw CancellationError()
    case "DELETE":
      record("claim-released:\(id)")
      return schemaRecoveryResponse(url: request.url!, status: 404)
    default:
      throw CancellationError()
    }
  }

  func wait(for event: String) async {
    guard !events.contains(event) else { return }
    await withCheckedContinuation { eventWaiters[event, default: []].append($0) }
  }

  func snapshot() -> [URLRequest] { requests }

  private func cancelParkedPoll(_ id: String) {
    guard cancelledParkedPolls.insert(id).inserted else { return }
    parkedPolls.removeValue(forKey: id)?.resume()
  }

  private func waitForParkedPoll(_ id: String) async {
    await withTaskCancellationHandler(
      operation: {
        await withCheckedContinuation { continuation in
          if cancelledParkedPolls.remove(id) != nil {
            continuation.resume()
          } else {
            parkedPolls[id] = continuation
          }
        }
      },
      onCancel: { Task { await self.cancelParkedPoll(id) } })
    cancelledParkedPolls.remove(id)
  }

  private func record(_ event: String) {
    guard events.insert(event).inserted else { return }
    for waiter in eventWaiters.removeValue(forKey: event) ?? [] { waiter.resume() }
  }
}

private func schemaRecoveryResponse(
  url: URL,
  body: String = "",
  status: Int = 200,
  headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: url, statusCode: status, httpVersion: nil, headerFields: headers)!)
}

private func schemaRecoveryIssue(id: Int64, title: String) -> ChangeRow {
  [
    "id": .int(id), "title": .string(title), "description": .string("details"),
    "status": .string("backlog"), "priority": .string("high"),
    "username": .string("ada"), "project_id": .int(7), "created": .int(1),
    "modified": .int(2), "kanbanorder": .number(1),
  ]
}

/// The application-owned selected generation. Snapshot writes remain provider-owned; this tiny
/// test seam models the one atomic UI selection change between two isolated materialized scopes.
private actor VisibleGenerationSelection {
  private var scope: MaterializationScope

  init(_ scope: MaterializationScope) { self.scope = scope }

  func selectedScope() -> MaterializationScope { scope }

  func select(_ scope: MaterializationScope) {
    self.scope = scope
  }
}

private func visibleIssueTitles(
  selection: VisibleGenerationSelection,
  oldScope: MaterializationScope,
  oldProvider: LinearLiteShapeMaterializer,
  newScope: MaterializationScope,
  newProvider: LinearLiteShapeMaterializer
) async throws -> [String] {
  let selectedScope = await selection.selectedScope()
  if selectedScope == oldScope {
    return try await oldProvider.allIssues().map(\.title)
  }
  if selectedScope == newScope {
    return try await newProvider.allIssues().map(\.title)
  }
  Issue.record("visible generation must be one of the explicitly isolated recovery scopes")
  return []
}

@Suite("LinearLite schema-terminal recovery")
struct SchemaTerminalRecoveryTests {
  @Test func closedNativeStreamKeepsOldVisibleUntilFreshScopeSnapshotIsSelectedAndPurged()
    async throws
  {
    let database = try DatabaseQueue()
    let oldScope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-old", generation: "g1")
    let oldProvider = try LinearLiteShapeMaterializer(database: database, scope: oldScope)
    let visibleGeneration = VisibleGenerationSelection(oldScope)
    let oldCursor = StreamCursor(offset: "old-7", lsn: "0/7")
    try await oldProvider.replaceSnapshot(
      [schemaRecoveryIssue(id: 7, title: "old generation")], expecting: nil, advancingTo: oldCursor)
    let transport = SchemaRecoveryTransport()
    let oldCoordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.issues", subscription: oldScope.subscription),
      materializer: oldProvider,
      retryPolicy: .init(maxRetries: 0))
    let updates = await oldCoordinator.stateUpdates
    let terminalReceipt = Task { () throws -> ShapeSubscriptionState in
      for await state in updates {
        if case .reseedRequired = state { return state }
      }
      throw CancellationError()
    }

    _ = try await oldCoordinator.start()
    await transport.wait(for: "terminal:shape-old")
    await transport.wait(for: "claim-released:shape-old")
    guard case .reseedRequired(let outcome) = try await terminalReceipt.value else {
      Issue.record("terminal close must publish a reseed receipt after release")
      return
    }
    #expect(outcome.reason == .terminal(.closed))
    #expect(outcome.previous.id == "shape-old")
    #expect(try await oldProvider.currentCursor() == oldCursor)
    #expect(try await oldProvider.allIssues().map(\.title) == ["old generation"])
    #expect((await transport.snapshot()).filter { $0.httpMethod == "POST" }.count == 1)

    // The terminal receipt never turns the old provider into a new generation. Keep the old
    // visible selection intact while a fully isolated replacement snapshot is staged.
    let newScope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-new", generation: "g2")
    let newProvider = try LinearLiteShapeMaterializer(database: database, scope: newScope)
    let newCursor = StreamCursor(offset: "new-snapshot", lsn: "0/9")
    try await newProvider.replaceSnapshot(
      [schemaRecoveryIssue(id: 9, title: "fresh generation")], expecting: nil,
      advancingTo: newCursor)
    #expect(try await oldProvider.currentCursor() == oldCursor)
    #expect(try await oldProvider.allIssues().map(\.title) == ["old generation"])
    #expect(try await newProvider.currentCursor() == newCursor)
    #expect(try await newProvider.allIssues().map(\.title) == ["fresh generation"])
    #expect(
      try await visibleIssueTitles(
        selection: visibleGeneration, oldScope: oldScope, oldProvider: oldProvider,
        newScope: newScope, newProvider: newProvider) == ["old generation"])
    let newCoordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.issues", subscription: newScope.subscription),
      materializer: newProvider,
      retryPolicy: .init(maxRetries: 0))

    let newHandle = try await newCoordinator.start()
    #expect(newHandle.id == "shape-new")
    await transport.wait(for: "poll:shape-new")
    let requests = await transport.snapshot()
    #expect(requests.filter { $0.httpMethod == "POST" }.count == 2)
    let newPoll = try #require(requests.last(where: { $0.httpMethod == "GET" }))
    #expect(
      URLComponents(url: newPoll.url!, resolvingAgainstBaseURL: false)?
        .queryItems?.first(where: { $0.name == "offset" })?.value == newCursor.offset)
    #expect(try await newProvider.currentCursor() == newCursor)
    #expect(try await newProvider.allIssues().map(\.title) == ["fresh generation"])

    // Switching the application-visible pointer is a single actor turn and occurs only after the
    // replacement snapshot has committed. The old scope is then safe to purge without exposing a
    // mixed generation.
    await visibleGeneration.select(newScope)
    #expect(await visibleGeneration.selectedScope() == newScope)
    #expect(
      try await visibleIssueTitles(
        selection: visibleGeneration, oldScope: oldScope, oldProvider: oldProvider,
        newScope: newScope, newProvider: newProvider) == ["fresh generation"])
    try await oldProvider.purgeScope()
    #expect(try await oldProvider.currentCursor() == nil)
    #expect(try await oldProvider.allIssues().isEmpty)
    #expect(
      try await visibleIssueTitles(
        selection: visibleGeneration, oldScope: oldScope, oldProvider: oldProvider,
        newScope: newScope, newProvider: newProvider) == ["fresh generation"])

    try await newCoordinator.stop()
    await transport.wait(for: "claim-released:shape-new")
    #expect(await newCoordinator.state == .stopped)
  }
}
