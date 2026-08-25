import ElectricCircuitsSwift
import Foundation
import GRDB
import Testing

@testable import LinearLiteGRDB

private actor LifecycleTransport: HTTPTransport {
  private var shapeCount = 0
  private var events: [String] = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]
  private var polls: [String: CheckedContinuation<Void, Never>] = [:]

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch request.httpMethod {
    case "POST":
      shapeCount += 1
      let id = "s\(shapeCount)"
      record("create:\(id)")
      return response(
        """
        {"shapeId":"\(id)","table":"public.issues","streamPath":"/v1/streams/\(id)","streamUrl":"https://streams.test/v1/streams/\(id)"}
        """
      )
    case "GET":
      let id = path.split(separator: "/").last.map(String.init) ?? "unknown"
      let offset =
        URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?
        .queryItems?.first(where: { $0.name == "offset" })?.value ?? "missing"
      record("poll:\(id):\(offset)")
      await withTaskCancellationHandler(
        operation: { await withCheckedContinuation { polls[id] = $0 } },
        onCancel: { Task { await self.cancelPoll(id) } })
      throw CancellationError()
    case "DELETE":
      let id = path.split(separator: "/").last.map(String.init) ?? "unknown"
      record("release:\(id)")
      return response("", status: 404)
    default:
      throw CancellationError()
    }
  }

  func wait(for event: String) async {
    guard !events.contains(event) else { return }
    await withCheckedContinuation { waiters[event, default: []].append($0) }
  }

  func snapshot() -> [String] { events }

  private func cancelPoll(_ id: String) {
    guard let continuation = polls.removeValue(forKey: id) else { return }
    record("cancelled:\(id)")
    continuation.resume()
  }

  private func record(_ event: String) {
    events.append(event)
    let matched = waiters.removeValue(forKey: event) ?? []
    for waiter in matched { waiter.resume() }
  }

  private func response(_ body: String, status: Int = 200) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
        headerFields: nil)!)
  }
}

private actor HeldAvailability: MaterializerAvailabilityProbe {
  private enum Outcome {
    case held
    case available
    case unavailable(MaterializerAvailabilityError)
  }

  private var outcome: Outcome = .held
  private var entered = false
  private var enteredWaiters: [CheckedContinuation<Void, Never>] = []
  private var outcomeWaiters: [CheckedContinuation<Void, Never>] = []

  func checkAvailability() async throws {
    entered = true
    let waiters = enteredWaiters
    enteredWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
    if case .held = outcome {
      await withCheckedContinuation { outcomeWaiters.append($0) }
    }
    switch outcome {
    case .available:
      return
    case .unavailable(let error):
      throw error
    case .held:
      preconditionFailure("availability gate resumed without an outcome")
    }
  }

  func waitForCheck() async {
    guard !entered else { return }
    await withCheckedContinuation { enteredWaiters.append($0) }
  }

  func makeAvailable() {
    outcome = .available
    releaseOutcomeWaiters()
  }

  func makeUnavailable(_ error: MaterializerAvailabilityError) {
    outcome = .unavailable(error)
    releaseOutcomeWaiters()
  }

  private func releaseOutcomeWaiters() {
    let waiters = outcomeWaiters
    outcomeWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
  }
}

private func lifecycleIssueRow(id: Int64, title: String) -> ChangeRow {
  [
    "id": .int(id),
    "title": .string(title),
    "description": .string("details"),
    "status": .string("backlog"),
    "priority": .string("high"),
    "username": .string("ada"),
    "project_id": .int(7),
    "created": .int(100),
    "modified": .int(101),
    "kanbanorder": .number(1),
  ]
}

private func lifecycleEnvelope(id: Int64, title: String) -> ChangeEnvelope {
  ChangeEnvelope(
    type: "public.issues", key: String(id), value: lifecycleIssueRow(id: id, title: title),
    headers: EnvelopeHeaders(operation: .upsert))
}

private func lifecycleCoordinator(
  transport: any HTTPTransport,
  provider: any ShapeMaterializer,
  subscription: String
) -> ShapeSubscriptionCoordinator {
  ShapeSubscriptionCoordinator(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport),
    transport: transport,
    request: ShapeRequest(table: "public.issues", subscription: subscription),
    materializer: provider,
    retryPolicy: .init(maxRetries: 0))
}

@Suite("LinearLite mobile lifecycle and privacy")
struct MobileLifecyclePrivacyTests {
  @Test func backgroundTeardownReleasesOnceAndForegroundUsesTheDurableCursor() async throws {
    let database = try DatabaseQueue()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-a", generation: "g1")
    let provider = try LinearLiteShapeMaterializer(database: database, scope: scope)
    let durableCursor = StreamCursor(offset: "7", lsn: "0/7")
    try await provider.apply(
      ChangeBatch([lifecycleEnvelope(id: 1, title: "persisted")]), expecting: nil,
      advancingTo: durableCursor)
    let transport = LifecycleTransport()

    let backgrounded = lifecycleCoordinator(
      transport: transport, provider: provider, subscription: scope.subscription)
    _ = try await backgrounded.start()
    await transport.wait(for: "poll:s1:7")
    try await backgrounded.stop()
    await transport.wait(for: "release:s1")

    let foregrounded = lifecycleCoordinator(
      transport: transport, provider: provider, subscription: scope.subscription)
    _ = try await foregrounded.start()
    await transport.wait(for: "poll:s2:7")
    try await foregrounded.stop()
    await transport.wait(for: "release:s2")

    let events = await transport.snapshot()
    let firstPoll = try #require(events.firstIndex(of: "poll:s1:7"))
    let firstCancelled = try #require(events.firstIndex(of: "cancelled:s1"))
    let firstRelease = try #require(events.firstIndex(of: "release:s1"))
    #expect(firstPoll < firstCancelled && firstCancelled < firstRelease)
    #expect(events.filter { $0 == "release:s1" }.count == 1)
    #expect(events.filter { $0 == "release:s2" }.count == 1)
    #expect(try await provider.currentCursor() == durableCursor)
    #expect(try await provider.allIssues().map(\.title) == ["persisted"])
  }

  @Test func protectedDataLaunchFailureLeaksNoClaimAndTheSameCoordinatorRecovers() async throws {
    let database = try DatabaseQueue()
    let availability = HeldAvailability()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-a", generation: "g1")
    let provider = try LinearLiteShapeMaterializer(
      database: database, scope: scope, availability: availability)
    let transport = LifecycleTransport()
    let coordinator = lifecycleCoordinator(
      transport: transport, provider: provider, subscription: scope.subscription)

    let unavailableStart = Task { try await coordinator.start() }
    await availability.waitForCheck()
    #expect(await transport.snapshot().isEmpty)
    await availability.makeUnavailable(.protectedDataUnavailable)
    await #expect(
      throws: ShapeSubscriptionFailure.materializerUnavailable(.protectedDataUnavailable)
    ) {
      _ = try await unavailableStart.value
    }
    #expect(await transport.snapshot().isEmpty)

    await availability.makeAvailable()
    _ = try await coordinator.start()
    await transport.wait(for: "poll:s1:-1")
    try await coordinator.stop()
    await transport.wait(for: "release:s1")

    #expect(try await provider.currentCursor() == nil)
    #expect(try await provider.allIssues().isEmpty)
    #expect(await transport.snapshot().filter { $0 == "create:s1" }.count == 1)
    #expect(await transport.snapshot().filter { $0 == "release:s1" }.count == 1)
  }

  @Test func accountSwitchReleasesAndPurgesOldPrincipalBeforeNewScopeIsVisible() async throws {
    let database = try DatabaseQueue()
    let oldScope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-a", generation: "g1")
    let newScope = MaterializationScope(
      principal: "account-b", template: "issues", subscription: "claim-b", generation: "g1")
    let oldProvider = try LinearLiteShapeMaterializer(database: database, scope: oldScope)
    try await oldProvider.apply(
      ChangeBatch([lifecycleEnvelope(id: 42, title: "private account a")]), expecting: nil,
      advancingTo: StreamCursor(offset: "a-7"))
    try await oldProvider.saveOverlay(
      LinearLiteIssueOverlay(
        mutationID: "a-overlay", rowKey: "42", operation: .update,
        patch: ["title": .string("private overlay a")]))
    let transport = LifecycleTransport()
    let oldCoordinator = lifecycleCoordinator(
      transport: transport, provider: oldProvider, subscription: oldScope.subscription)
    _ = try await oldCoordinator.start()
    await transport.wait(for: "poll:s1:a-7")
    try await oldCoordinator.stop()
    await transport.wait(for: "release:s1")

    try await LinearLiteShapeMaterializer.purgePrincipal("account-a", from: database)
    let oldArtifacts = try await database.read { db in
      try [
        Int.fetchOne(db, sql: "SELECT COUNT(*) FROM issues WHERE principal_id = 'account-a'") ?? -1,
        Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM subset_view_members WHERE principal_id = 'account-a'")
          ?? -1,
        Int.fetchOne(
          db,
          sql: "SELECT COUNT(*) FROM shape_cursors WHERE shape_id = ?",
          arguments: [oldScope.storageKey]) ?? -1,
        Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM issue_overlays WHERE scope_id = ?",
          arguments: [oldScope.storageKey]) ?? -1,
        Int.fetchOne(
          db, sql: "SELECT COUNT(*) FROM materialization_scopes WHERE principal = 'account-a'")
          ?? -1,
      ]
    }
    #expect(oldArtifacts == [0, 0, 0, 0, 0])

    let newProvider = try LinearLiteShapeMaterializer(database: database, scope: newScope)
    try await newProvider.apply(
      ChangeBatch([lifecycleEnvelope(id: 42, title: "private account b")]), expecting: nil,
      advancingTo: StreamCursor(offset: "b-7"))
    let newCoordinator = lifecycleCoordinator(
      transport: transport, provider: newProvider, subscription: newScope.subscription)
    _ = try await newCoordinator.start()
    await transport.wait(for: "poll:s2:b-7")
    #expect(try await newProvider.allIssues().map(\.title) == ["private account b"])
    #expect(try await oldProvider.allIssues().isEmpty)
    try await newCoordinator.stop()
    await transport.wait(for: "release:s2")

    let events = await transport.snapshot()
    #expect(
      try #require(events.firstIndex(of: "release:s1"))
        < #require(events.firstIndex(of: "create:s2")))
  }

  @Test func responseCancellationBeforeProviderApplicationLeavesGRDBRowsAndCursorUntouched()
    async throws
  {
    let database = try DatabaseQueue()
    let scope = MaterializationScope(
      principal: "account-a", template: "issues", subscription: "claim-a", generation: "g1")
    let provider = try LinearLiteShapeMaterializer(database: database, scope: scope)
    let transport = LifecycleTransport()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/v1/streams/cancel")!, transport: transport,
      materializer: provider)

    let read = Task { try await reader.run() }
    await transport.wait(for: "poll:cancel:-1")
    read.cancel()
    await #expect(throws: CancellationError.self) { try await read.value }
    #expect(try await provider.currentCursor() == nil)
    #expect(try await provider.allIssues().isEmpty)
  }
}
