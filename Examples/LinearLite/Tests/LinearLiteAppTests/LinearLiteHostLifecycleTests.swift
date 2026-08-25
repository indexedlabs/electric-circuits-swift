import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import LinearLiteGRDB
import Testing

private actor LifecycleAudit {
  private var values: [String] = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]

  func record(_ value: String) {
    values.append(value)
    for waiter in waiters.removeValue(forKey: value) ?? [] { waiter.resume() }
  }

  func wait(for value: String) async {
    guard !values.contains(value) else { return }
    await withCheckedContinuation { waiters[value, default: []].append($0) }
  }

  func snapshot() -> [String] { values }
}

private enum TestPurgeError: Error { case refused }

private actor HostBridgeTransport: HTTPTransport {
  private var sequence = 0
  private var values: [String] = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]
  private var polls: [String: CheckedContinuation<Void, Never>] = [:]

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch request.httpMethod {
    case "POST":
      sequence += 1
      let id = "host-\(sequence)"
      record("create:\(id)")
      return response(
        """
        {"shapeId":"\(id)","table":"public.issues","streamPath":"/v1/streams/\(id)","streamUrl":"https://engine.test/v1/streams/\(id)"}
        """)
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
      return response("{}")
    default:
      throw CancellationError()
    }
  }

  func record(_ value: String) {
    values.append(value)
    for waiter in waiters.removeValue(forKey: value) ?? [] { waiter.resume() }
  }

  func wait(for value: String) async {
    guard !values.contains(value) else { return }
    await withCheckedContinuation { waiters[value, default: []].append($0) }
  }

  func snapshot() -> [String] { values }

  private func cancelPoll(_ id: String) {
    polls.removeValue(forKey: id)?.resume()
  }

  private func response(_ body: String) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: URL(string: "https://engine.test")!, statusCode: 200, httpVersion: nil,
        headerFields: nil)!)
  }
}

@MainActor
private final class CoordinatorHostSession: LinearLiteHostLifecycleSession {
  private let coordinator: ShapeSubscriptionCoordinator
  private let provider: LinearLiteShapeMaterializer

  init(coordinator: ShapeSubscriptionCoordinator, provider: LinearLiteShapeMaterializer) {
    self.coordinator = coordinator
    self.provider = provider
  }

  func start() async -> LinearLiteHostStartReceipt {
    do {
      let cursor = try await provider.currentCursor()
      _ = try await coordinator.start()
      return .started(cursor: cursor)
    } catch let failure as ShapeSubscriptionFailure {
      if case .materializerUnavailable(let availability) = failure {
        return .unavailable(availability)
      }
      return .cancelled
    } catch {
      return .cancelled
    }
  }

  func stop() async -> LinearLiteHostReleaseReceipt {
    do {
      try await coordinator.stop()
      return .released(name: "coordinator-release")
    } catch {
      return .failed
    }
  }
}

@MainActor
private final class CoordinatorHostSessionFactory {
  private let providers: [LinearLiteHostPrincipal: LinearLiteShapeMaterializer]
  private let transport: HostBridgeTransport

  init(
    providers: [LinearLiteHostPrincipal: LinearLiteShapeMaterializer],
    transport: HostBridgeTransport
  ) {
    self.providers = providers
    self.transport = transport
  }

  func make(_ principal: LinearLiteHostPrincipal) -> any LinearLiteHostLifecycleSession {
    let provider = providers[principal]!
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.issues", subscription: principal.value),
      materializer: provider,
      retryPolicy: .init(maxRetries: 0))
    return CoordinatorHostSession(coordinator: coordinator, provider: provider)
  }
}

private func scopedIssue(_ id: Int64, title: String) -> ChangeEnvelope {
  ChangeEnvelope(
    type: "public.issues", key: String(id),
    value: [
      "id": .int(id), "title": .string(title), "description": .string("details"),
      "status": .string("backlog"), "priority": .string("high"), "username": .string("ada"),
      "project_id": .int(7), "created": .int(1), "modified": .int(2), "kanbanorder": .number(1),
    ], headers: EnvelopeHeaders(operation: .upsert))
}

private actor LifecycleGate {
  private var isOpen = false
  private var entered = false
  private var entrants: [CheckedContinuation<Void, Never>] = []
  private var waiters: [CheckedContinuation<Void, Never>] = []

  func wait() async {
    entered = true
    for entrant in entrants { entrant.resume() }
    entrants.removeAll()
    guard !isOpen else { return }
    await withCheckedContinuation { waiters.append($0) }
  }

  func waitUntilEntered() async {
    guard !entered else { return }
    await withCheckedContinuation { entrants.append($0) }
  }

  func unblock() {
    isOpen = true
    for waiter in waiters { waiter.resume() }
    waiters.removeAll()
  }
}

@MainActor
private final class ScriptedHostSession: LinearLiteHostLifecycleSession {
  let principal: LinearLiteHostPrincipal
  private var startResults: [LinearLiteHostStartReceipt]
  let audit: LifecycleAudit
  let startGate: LifecycleGate?
  let stopGate: LifecycleGate?

  init(
    principal: LinearLiteHostPrincipal,
    startResults: [LinearLiteHostStartReceipt],
    audit: LifecycleAudit,
    startGate: LifecycleGate? = nil,
    stopGate: LifecycleGate? = nil
  ) {
    self.principal = principal
    self.startResults = startResults
    self.audit = audit
    self.startGate = startGate
    self.stopGate = stopGate
  }

  func start() async -> LinearLiteHostStartReceipt {
    await audit.record("start:\(principal.value)")
    if let startGate { await startGate.wait() }
    precondition(!startResults.isEmpty, "scripted start result missing")
    return startResults.count == 1 ? startResults[0] : startResults.removeFirst()
  }

  func stop() async -> LinearLiteHostReleaseReceipt {
    await audit.record("release-begin:\(principal.value)")
    if let stopGate { await stopGate.wait() }
    await audit.record("release:\(principal.value)")
    return .released(name: "host-stop")
  }
}

@Suite("LinearLite iOS host lifecycle bridge")
struct LinearLiteHostLifecycleTests {
  @Test @MainActor
  func protectedDataNotificationStartsTheSameSessionOnlyAfterTypedUnavailableReceipt() async {
    let audit = LifecycleAudit()
    let principal = LinearLiteHostPrincipal("account-a")
    let session = ScriptedHostSession(
      principal: principal, startResults: [.started(cursor: StreamCursor(offset: "7"))],
      audit: audit)
    let bridge = LinearLiteHostLifecycle(
      principal: principal, protectedDataAvailable: false,
      makeSession: { _ in session }, purgePrincipal: { _ in })

    #expect(await bridge.launch() == .protectedDataUnavailable)
    #expect(await audit.snapshot().isEmpty)
    #expect(
      await bridge.protectedDataDidBecomeAvailable()
        == .started(principal: principal, cursor: StreamCursor(offset: "7")))
    #expect(await audit.snapshot() == ["start:account-a"])
  }

  @Test @MainActor
  func inactiveJoinsOneReleaseBeforeForegroundStartsAndRepeatedEventsAreIdempotent() async {
    let audit = LifecycleAudit()
    let stopGate = LifecycleGate()
    let principal = LinearLiteHostPrincipal("account-a")
    let session = ScriptedHostSession(
      principal: principal, startResults: [.started(cursor: StreamCursor(offset: "7"))],
      audit: audit,
      stopGate: stopGate)
    let bridge = LinearLiteHostLifecycle(
      principal: principal, protectedDataAvailable: true,
      makeSession: { _ in session }, purgePrincipal: { _ in })

    let initial = await bridge.launch()
    guard initial == .started(principal: principal, cursor: StreamCursor(offset: "7")) else {
      Issue.record("expected durable-cursor start receipt, got \(initial)")
      return
    }
    let inactive = Task { await bridge.sceneDidBecomeInactive() }
    await audit.wait(for: "release-begin:account-a")
    let repeatedInactive = Task { await bridge.sceneDidBecomeInactive() }
    let foreground = Task { await bridge.sceneDidBecomeActive() }
    #expect(await audit.snapshot() == ["start:account-a", "release-begin:account-a"])
    await stopGate.unblock()
    #expect(await inactive.value == .released(principal: principal, name: "host-stop"))
    #expect(await repeatedInactive.value == .released(principal: principal, name: "host-stop"))
    #expect(
      await foreground.value == .started(principal: principal, cursor: StreamCursor(offset: "7")))
    #expect(await audit.snapshot().filter { $0 == "release:account-a" }.count == 1)
    #expect(
      await bridge.sceneDidBecomeInactive() == .released(principal: principal, name: "host-stop"))
    #expect(await audit.snapshot().filter { $0 == "release:account-a" }.count == 2)
  }

  @Test @MainActor
  func inactiveFencesEveryCallerJoinedToAHeldStartGeneration() async {
    let audit = LifecycleAudit()
    let startGate = LifecycleGate()
    let principal = LinearLiteHostPrincipal("account-a")
    let session = ScriptedHostSession(
      principal: principal, startResults: [.started(cursor: StreamCursor(offset: "7"))],
      audit: audit, startGate: startGate)
    let bridge = LinearLiteHostLifecycle(
      principal: principal, protectedDataAvailable: true,
      makeSession: { _ in session }, purgePrincipal: { _ in })

    let firstCaller = Task { await bridge.launch() }
    await audit.wait(for: "start:account-a")
    let secondCaller = Task { await bridge.launch() }
    await Task.yield()
    let inactive = Task { await bridge.sceneDidBecomeInactive() }
    await Task.yield()
    await startGate.unblock()

    #expect(await firstCaller.value == .ignored)
    #expect(await secondCaller.value == .ignored)
    #expect(await inactive.value == .released(principal: principal, name: "host-stop"))
    #expect(
      await audit.snapshot() == ["start:account-a", "release-begin:account-a", "release:account-a"])
  }

  @Test @MainActor
  func accountSwitchReleasesPurgesThenStartsASeparatedPrincipalWithTheSameNumericRowID() async {
    let audit = LifecycleAudit()
    let old = LinearLiteHostPrincipal("account-a")
    let next = LinearLiteHostPrincipal("account-b")
    let oldSession = ScriptedHostSession(
      principal: old, startResults: [.started(cursor: StreamCursor(offset: "a-7"))], audit: audit)
    let nextSession = ScriptedHostSession(
      principal: next, startResults: [.started(cursor: StreamCursor(offset: "b-7"))], audit: audit)
    let bridge = LinearLiteHostLifecycle(
      principal: old, protectedDataAvailable: true,
      makeSession: { $0 == old ? oldSession : nextSession },
      purgePrincipal: { principal in await audit.record("purge:\(principal.value):row-42") })

    _ = await bridge.launch()
    #expect(await bridge.switchAccount(to: next) == .accountSwitched(from: old, to: next))
    #expect(
      await audit.snapshot() == [
        "start:account-a", "release-begin:account-a", "release:account-a",
        "purge:account-a:row-42", "start:account-b",
      ])
  }

  @Test @MainActor
  func staleStartCannotPublishIntoTheGenerationThatStoppedIt() async {
    let audit = LifecycleAudit()
    let startGate = LifecycleGate()
    let principal = LinearLiteHostPrincipal("account-a")
    let session = ScriptedHostSession(
      principal: principal, startResults: [.started(cursor: StreamCursor(offset: "9"))],
      audit: audit,
      startGate: startGate)
    let bridge = LinearLiteHostLifecycle(
      principal: principal, protectedDataAvailable: true,
      makeSession: { _ in session }, purgePrincipal: { _ in })

    let starting = Task { await bridge.launch() }
    await startGate.waitUntilEntered()
    let stopping = Task { await bridge.sceneDidBecomeInactive() }
    await startGate.unblock()
    #expect(await starting.value == .ignored)
    #expect(await stopping.value == .released(principal: principal, name: "host-stop"))
    #expect(
      await audit.snapshot() == ["start:account-a", "release-begin:account-a", "release:account-a"])
  }

  @Test @MainActor
  func accountSwitchDoesNotClaimSuccessWhenTheNewPrincipalCannotStart() async {
    let audit = LifecycleAudit()
    let old = LinearLiteHostPrincipal("account-a")
    let next = LinearLiteHostPrincipal("account-b")
    let oldSession = ScriptedHostSession(
      principal: old, startResults: [.started(cursor: StreamCursor(offset: "a-7"))], audit: audit)
    let unavailableSession = ScriptedHostSession(
      principal: next, startResults: [.unavailable(.databaseUnavailable)], audit: audit)
    let bridge = LinearLiteHostLifecycle(
      principal: old, protectedDataAvailable: true,
      makeSession: { $0 == old ? oldSession : unavailableSession },
      purgePrincipal: { principal in await audit.record("purge:\(principal.value)") })

    _ = await bridge.launch()
    #expect(await bridge.switchAccount(to: next) == .accountSwitchFailed(from: old, to: next))
    #expect(
      await audit.snapshot() == [
        "start:account-a", "release-begin:account-a", "release:account-a",
        "purge:account-a", "start:account-b",
      ])
  }

  @Test @MainActor
  func purgeFailureKeepsTheOldPrincipalIdleAndNeverConstructsTheNewSession() async {
    let audit = LifecycleAudit()
    let old = LinearLiteHostPrincipal("account-a")
    let next = LinearLiteHostPrincipal("account-b")
    let oldSession = ScriptedHostSession(
      principal: old, startResults: [.started(cursor: StreamCursor(offset: "a-7"))], audit: audit)
    let newSession = ScriptedHostSession(
      principal: next, startResults: [.started(cursor: StreamCursor(offset: "b-7"))], audit: audit)
    let bridge = LinearLiteHostLifecycle(
      principal: old, protectedDataAvailable: true,
      makeSession: { $0 == old ? oldSession : newSession },
      purgePrincipal: { principal in
        await audit.record("purge:\(principal.value)")
        throw TestPurgeError.refused
      })

    _ = await bridge.launch()
    #expect(await bridge.switchAccount(to: next) == .accountSwitchFailed(from: old, to: next))
    #expect(
      await audit.snapshot() == [
        "start:account-a", "release-begin:account-a", "release:account-a", "purge:account-a",
      ])
  }

  @Test @MainActor
  func cancelledWaitingSwitchCannotStarveTheNextAccountTransition() async {
    let audit = LifecycleAudit()
    let purgeGate = LifecycleGate()
    let a = LinearLiteHostPrincipal("account-a")
    let b = LinearLiteHostPrincipal("account-b")
    let c = LinearLiteHostPrincipal("account-c")
    let sessions = [
      a: ScriptedHostSession(
        principal: a, startResults: [.started(cursor: StreamCursor(offset: "a-7"))], audit: audit),
      b: ScriptedHostSession(
        principal: b, startResults: [.started(cursor: StreamCursor(offset: "b-7"))], audit: audit),
      c: ScriptedHostSession(
        principal: c, startResults: [.started(cursor: StreamCursor(offset: "c-7"))], audit: audit),
    ]
    let bridge = LinearLiteHostLifecycle(
      principal: a, protectedDataAvailable: true,
      makeSession: { sessions[$0]! },
      purgePrincipal: { principal in
        await audit.record("purge:\(principal.value)")
        await purgeGate.wait()
      })

    _ = await bridge.launch()
    let first = Task { await bridge.switchAccount(to: b) }
    await purgeGate.waitUntilEntered()
    let cancelled = Task { await bridge.switchAccount(to: c) }
    cancelled.cancel()
    let later = Task { await bridge.switchAccount(to: c) }
    await purgeGate.unblock()

    #expect(await first.value == .accountSwitched(from: a, to: b))
    _ = await cancelled.value
    #expect(await later.value == .accountSwitched(from: b, to: c))
    #expect(await audit.snapshot().filter { $0 == "start:account-c" }.count == 1)
  }

  @Test @MainActor
  func realCoordinatorProviderBridgeFencesAvailabilityResumesCursorAndPurgesSameIDAccountRows()
    async throws
  {
    let database = try DatabaseQueue()
    let old = LinearLiteHostPrincipal("account-a")
    let next = LinearLiteHostPrincipal("account-b")
    let oldScope = MaterializationScope(
      principal: old.value, template: "issues", subscription: old.value, generation: "g1")
    let nextScope = MaterializationScope(
      principal: next.value, template: "issues", subscription: next.value, generation: "g1")
    let oldProvider = try LinearLiteShapeMaterializer(database: database, scope: oldScope)
    let nextProvider = try LinearLiteShapeMaterializer(database: database, scope: nextScope)
    try await oldProvider.apply(
      ChangeBatch([scopedIssue(42, title: "old private row")]), expecting: nil,
      advancingTo: StreamCursor(offset: "a-7"))
    try await nextProvider.apply(
      ChangeBatch([scopedIssue(42, title: "new private row")]), expecting: nil,
      advancingTo: StreamCursor(offset: "b-7"))
    let transport = HostBridgeTransport()
    let factory = CoordinatorHostSessionFactory(
      providers: [old: oldProvider, next: nextProvider], transport: transport)
    let bridge = LinearLiteHostLifecycle(
      principal: old, protectedDataAvailable: false,
      makeSession: { factory.make($0) },
      purgePrincipal: { principal in
        try await LinearLiteShapeMaterializer.purgePrincipal(principal.value, from: database)
        await transport.record("purge:\(principal.value)")
      })

    #expect(await bridge.launch() == .protectedDataUnavailable)
    #expect(await transport.snapshot().isEmpty)
    #expect(
      await bridge.protectedDataDidBecomeAvailable()
        == .started(principal: old, cursor: StreamCursor(offset: "a-7")))
    await transport.wait(for: "poll:host-1:a-7")
    #expect(
      await bridge.sceneDidBecomeInactive()
        == .released(principal: old, name: "coordinator-release"))
    await transport.wait(for: "release:host-1")
    #expect(
      await bridge.sceneDidBecomeActive()
        == .started(principal: old, cursor: StreamCursor(offset: "a-7")))
    await transport.wait(for: "poll:host-2:a-7")

    #expect(await bridge.switchAccount(to: next) == .accountSwitched(from: old, to: next))
    await transport.wait(for: "poll:host-3:b-7")
    #expect(try await oldProvider.allIssues().isEmpty)
    #expect(try await oldProvider.currentCursor() == nil)
    #expect(try await nextProvider.allIssues().map(\.title) == ["new private row"])
    let events = await transport.snapshot()
    #expect(
      try #require(events.firstIndex(of: "release:host-2"))
        < #require(events.firstIndex(of: "purge:account-a")))
    #expect(
      try #require(events.firstIndex(of: "purge:account-a"))
        < #require(events.firstIndex(of: "create:host-3")))
    _ = await bridge.sceneDidBecomeInactive()
  }
}
