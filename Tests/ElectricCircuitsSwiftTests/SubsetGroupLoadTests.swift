import Foundation
import Testing

@testable import ElectricCircuitsSwift

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private actor GroupLoadTransport: HTTPTransport {
  private let body: String
  private(set) var requests: [URLRequest] = []

  init(body: String) { self.body = body }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "application/json"])!)
  }
}

/// Answers each group load with the next scripted group id, holds stream reads open and records
/// releases, so a coordinator can create, renew and release a group.
private actor GroupRenewalTransport: HTTPTransport {
  private var shapeIDs: [String]
  private(set) var loads = 0
  private(set) var releasedPaths: [String] = []

  init(shapeIDs: [String]) { self.shapeIDs = shapeIDs }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let path = request.url?.path ?? ""
    switch request.httpMethod {
    case "POST":
      loads += 1
      let id = shapeIDs.count > 1 ? shapeIDs.removeFirst() : shapeIDs[0]
      let body = groupLoadAnswer.replacingOccurrences(of: #""g-1""#, with: "\"\(id)\"")
      return reply(body, request: request)
    case "DELETE":
      releasedPaths.append(path)
      return reply("", request: request, status: 204)
    default:
      try await Task.sleep(for: .seconds(3_600))
      throw CancellationError()
    }
  }

  private func reply(_ body: String, request: URLRequest, status: Int = 200) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil,
        headerFields: ["Content-Type": "application/json"])!)
  }
}

/// Records each sleep and holds it until the test wakes it.
private actor GroupLeaseClock: ShapeSubscriptionClock {
  private(set) var sleeps: [Duration] = []
  private var sleeping: [UUID: CheckedContinuation<Void, Error>] = [:]
  private var cancelled: Set<UUID> = []

  func sleep(for duration: Duration) async throws {
    sleeps.append(duration)
    let id = UUID()
    try await withTaskCancellationHandler {
      try await withCheckedThrowingContinuation { (continuation: CheckedContinuation<Void, Error>) in
        if cancelled.remove(id) != nil {
          continuation.resume(throwing: CancellationError())
        } else {
          sleeping[id] = continuation
        }
      }
    } onCancel: {
      Task { await self.cancel(id) }
    }
  }

  private func cancel(_ id: UUID) {
    if let sleeper = sleeping.removeValue(forKey: id) {
      sleeper.resume(throwing: CancellationError())
    } else {
      cancelled.insert(id)
    }
  }

  func wake() {
    let woken = sleeping
    sleeping.removeAll()
    for sleeper in woken.values { sleeper.resume() }
  }

  func waitForSleeps(_ count: Int) async {
    for _ in 0..<5_000 where sleeps.count < count {
      try? await Task.sleep(for: .milliseconds(2))
    }
  }
}

private let groupLoadAnswer = #"""
  {"groupId":"g-1","shapeId":"g-1","subscription":"scope-1","leaseSeconds":30,
   "streamPath":"/sync/v1/streams/tok","streamUrl":"https://gateway.test/sync/v1/streams/tok",
   "offset":"0000_0010","lsn":"0/10","snapshot":"100:110:105","horizon":"0/20",
   "rows":[{"table":"public.record","value":{"id":"1"}},
           {"table":"public.assignment","value":{"id":"1","record_id":"1"}}],
   "results":[{"key":"public.record","table":"public.record","shapeId":"s-1","subscription":"scope-1"},
              {"key":"public.assignment","table":"public.assignment","shapeId":"s-2","subscription":"scope-1:member:1"}]}
  """#

private func groupRequest() -> SubsetGroupRequest {
  SubsetGroupRequest(
    members: [
      SubsetGroupMember(
        table: "public.record", where: .leaf(column: "id", op: .eq, value: .string("1")),
        columns: ["id"], key: "public.record"),
      SubsetGroupMember(
        table: "public.assignment",
        where: .leaf(column: "record_id", op: .eq, value: .string("1")),
        key: "public.assignment"),
    ],
    subscription: "scope-1")
}

@Suite("Shape group load")
struct SubsetGroupLoadTests {
  @Test func groupLoadPostsEveryMemberAndMapsOneHandle() async throws {
    let transport = GroupLoadTransport(body: groupLoadAnswer)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)

    let load = try await client.loadSubsetGroup(groupRequest())

    let requests = await transport.requests
    #expect(requests.count == 1)
    let request = try #require(requests.first)
    #expect(request.httpMethod == "POST")
    #expect(request.url?.path == "/sync/v1/subsets/load")
    let body = try #require(request.httpBody)
    let sent = try JSONDecoder().decode(SubsetGroupRequest.self, from: body)
    #expect(sent == groupRequest())

    #expect(load.handle.id == "g-1")
    #expect(load.groupId == load.shapeId)
    #expect(load.handle.subscription == "scope-1")
    #expect(load.handle.stream.url.absoluteString == "https://gateway.test/sync/v1/streams/tok")
    #expect(load.handle.response.leaseSeconds == 30)
    #expect(load.cursor == StreamCursor(offset: "0000_0010", lsn: "0/10"))
    #expect(load.snapshot == "100:110:105")
    #expect(load.horizon == "0/20")
    #expect(load.rows.map(\.table) == ["public.record", "public.assignment"])
    #expect(
      load.rows.map(\.value) == [
        .object(["id": .string("1")]),
        .object(["id": .string("1"), "record_id": .string("1")]),
      ])
    #expect(load.results.map(\.key) == ["public.record", "public.assignment"])
  }

  @Test func groupLoadRejectsAnAnswerThatDoesNotMatchTheRequest() async throws {
    let otherMembers = groupLoadAnswer.replacingOccurrences(
      of: #""key":"public.assignment""#, with: #""key":"public.tag""#)
    let foreignRow = groupLoadAnswer.replacingOccurrences(
      of: #"{"table":"public.assignment","value""#, with: #"{"table":"public.tag","value""#)
    let otherSubscription = groupLoadAnswer.replacingOccurrences(
      of: #""subscription":"scope-1","leaseSeconds""#,
      with: #""subscription":"scope-2","leaseSeconds""#)
    for body in [otherMembers, foreignRow, otherSubscription] {
      let client = ElectricCircuitsClient(
        baseURL: URL(string: "https://gateway.test/sync")!,
        transport: GroupLoadTransport(body: body))
      await #expect(throws: ClientError.self) {
        _ = try await client.loadSubsetGroup(groupRequest())
      }
    }
  }

  @Test func coordinatorStartsOnASetupClaimAndRenewsIt() async throws {
    let transport = GroupRenewalTransport(shapeIDs: ["g-1", "g-1", "g-2"])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)
    let setup = try await client.loadSubsetGroup(groupRequest())
    let coordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport, groupRequest: groupRequest(),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      setupClaim: setup.handle)

    // The setup's load is the only one, and its lease is the coordinator's to renew.
    let handle = try await coordinator.start()
    #expect(handle == setup.handle)
    #expect(handle.response.leaseSeconds == 30)
    #expect(await transport.loads == 1)

    #expect(try await coordinator.renew() == handle)
    #expect(await transport.loads == 2)

    await #expect(throws: ShapeSubscriptionFailure.self) {
      _ = try await coordinator.renew()
    }
    guard case .reseedRequired(let outcome) = await coordinator.state else {
      Issue.record("expected reseedRequired after a replaced group")
      try await coordinator.stop()
      return
    }
    #expect(outcome.reason == .replacement(previousShapeID: "g-1", replacementShapeID: "g-2"))
    try await coordinator.stop()
  }

  @Test(arguments: [
    (age: Duration.seconds(8), first: Duration.seconds(2)),
    (age: Duration.seconds(12), first: Duration.zero),
  ])
  func aSetupClaimsFirstRenewalCountsFromItsLoad(age: Duration, first: Duration) async throws {
    // A 30-second lease renews every 10 seconds, counted from the load that began it.
    let transport = GroupRenewalTransport(shapeIDs: ["g-1"])
    let clock = GroupLeaseClock()
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)
    let setup = try await client.loadSubsetGroup(groupRequest())
    let coordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport, groupRequest: groupRequest(),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0), clock: clock,
      setupClaim: setup.handle, setupClaimAge: age)

    _ = try await coordinator.start()
    if first > .zero {
      await clock.waitForSleeps(1)
      #expect(await clock.sleeps == [first])
      #expect(await transport.loads == 1)
      await clock.wake()
    }

    await clock.waitForSleeps(first > .zero ? 2 : 1)
    #expect(await transport.loads == 2)
    #expect(await clock.sleeps.last == .seconds(10))
    try await coordinator.stop()
  }

  @Test func stoppingBeforeStartReleasesTheSetupClaim() async throws {
    let transport = GroupRenewalTransport(shapeIDs: ["g-1"])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)
    let setup = try await client.loadSubsetGroup(groupRequest())
    let coordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport, groupRequest: groupRequest(),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      setupClaim: setup.handle)

    try await coordinator.stop()

    #expect(await transport.loads == 1)
    #expect(await transport.releasedPaths.count == 1)
  }

  @Test func aRefusedStartStillLeavesTheSetupClaimForStopToRelease() async throws {
    let transport = GroupRenewalTransport(shapeIDs: ["g-1"])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)
    let setup = try await client.loadSubsetGroup(groupRequest())
    let capacity = ShapeSubscriptionCapacity(maximumActiveSubscriptions: 1)
    _ = try await capacity.acquire()
    let coordinator = ShapeSubscriptionCoordinator(
      client: client, transport: transport, groupRequest: groupRequest(),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
      capacity: capacity, setupClaim: setup.handle)

    await #expect(throws: (any Error).self) { _ = try await coordinator.start() }
    try await coordinator.stop()

    #expect(await transport.loads == 1)
    #expect(await transport.releasedPaths.count == 1)
  }

  @Test func groupCoordinatorRenewsTheSameGroupAndReseedsOnReplacement() async throws {
    let transport = GroupRenewalTransport(shapeIDs: ["g-1", "g-1", "g-2"])
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://gateway.test/sync")!, transport: transport),
      transport: transport, groupRequest: groupRequest(),
      materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0))

    let handle = try await coordinator.start()
    #expect(handle.id == "g-1")
    #expect(handle.subscription == "scope-1")
    #expect(handle.response.leaseSeconds == 30)

    // A renewal loads the same subscription and members and keeps the same group.
    #expect(try await coordinator.renew() == handle)
    guard case .streaming = await coordinator.state else {
      Issue.record("expected the renewed group to keep streaming")
      try await coordinator.stop()
      return
    }

    // A renewal answered by another group is a replacement: both claims are released and the
    // caller must reseed rather than read the new group from the old cursor.
    await #expect(throws: ShapeSubscriptionFailure.self) {
      _ = try await coordinator.renew()
    }
    guard case .reseedRequired(let outcome) = await coordinator.state else {
      Issue.record("expected reseedRequired after a replaced group")
      try await coordinator.stop()
      return
    }
    #expect(outcome.reason == .replacement(previousShapeID: "g-1", replacementShapeID: "g-2"))
    #expect(await transport.loads == 3)
    #expect(
      Set(await transport.releasedPaths) == ["/sync/v1/shapes/g-1", "/sync/v1/shapes/g-2"])
    try await coordinator.stop()
  }

  @Test func groupClaimRejectsAnAnswerThatDoesNotMatchTheRequest() async throws {
    let otherMembers = groupLoadAnswer.replacingOccurrences(
      of: #""key":"public.assignment""#, with: #""key":"public.tag""#)
    var answer = try #require(
      try JSONSerialization.jsonObject(with: Data(groupLoadAnswer.utf8)) as? [String: Any])
    answer["results"] = (answer["results"] as? [Any])?.dropLast().map { $0 }
    let missingMember = String(
      decoding: try JSONSerialization.data(withJSONObject: answer), as: UTF8.self)
    let otherSubscription = groupLoadAnswer.replacingOccurrences(
      of: #""subscription":"scope-1","leaseSeconds""#,
      with: #""subscription":"scope-2","leaseSeconds""#)
    for body in [otherMembers, missingMember, otherSubscription] {
      let client = ElectricCircuitsClient(
        baseURL: URL(string: "https://gateway.test/sync")!,
        transport: GroupLoadTransport(body: body))
      await #expect(throws: ClientError.self) {
        _ = try await client.claimSubsetGroup(groupRequest())
      }
    }
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!,
      transport: GroupLoadTransport(body: groupLoadAnswer))
    #expect(try await client.claimSubsetGroup(groupRequest()).id == "g-1")
  }
}
