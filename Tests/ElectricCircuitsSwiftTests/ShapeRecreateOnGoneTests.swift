import Foundation
import Testing

@testable import ElectricCircuitsSwift

/// The engine answers a create/join whose fall-through is exhausted, or whose reactivation join
/// timed out, with `410 Gone` (electric-circuits ADR-0011, "Dormant reactivation is bounded").
/// These tests pin the client half of that vocabulary: the coordinator performs the fall-through
/// the engine could not, bounded, and only on the two native routes that mint a shape.
private actor RecreateClock: ShapeSubscriptionClock {
  private(set) var delays: [Duration] = []
  func sleep(for duration: Duration) async throws { delays.append(duration) }
}

/// Answers the first `goneCreates` POSTs with `410` and every later POST with a fresh shape id.
private actor GoneCreateTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var posts = 0
  private let goneCreates: Int

  init(goneCreates: Int) { self.goneCreates = goneCreates }

  var postCount: Int { posts }
  var postBodies: [Data] { requests.filter { $0.httpMethod == "POST" }.compactMap(\.httpBody) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    switch request.httpMethod {
    case "POST":
      posts += 1
      return posts <= goneCreates
        ? recreateResponse("gone", status: 410) : recreateShapeResponse(id: "s1")
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE":
      return recreateResponse("", status: 404)
    default:
      throw CancellationError()
    }
  }
}

/// Answers every POST with one fixed non-2xx status.
private actor FixedCreateStatusTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private let status: Int
  init(status: Int) { self.status = status }
  var postCount: Int { requests.filter { $0.httpMethod == "POST" }.count }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    switch request.httpMethod {
    case "POST": return recreateResponse("fault", status: status)
    case "DELETE": return recreateResponse("", status: 404)
    default: throw CancellationError()
    }
  }
}

/// Creates `s1`, answers the join with `410`, then falls through to `s2`.
private actor GoneJoinTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  private var posts = 0

  var postBodies: [Data] { requests.filter { $0.httpMethod == "POST" }.compactMap(\.httpBody) }
  var deleteURLs: [URL] { requests.filter { $0.httpMethod == "DELETE" }.compactMap(\.url) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    switch request.httpMethod {
    case "POST":
      posts += 1
      switch posts {
      case 1: return recreateShapeResponse(id: "s1")
      case 2: return recreateResponse("gone", status: 410)
      default: return recreateShapeResponse(id: "s2")
      }
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE":
      return recreateResponse("", status: 404)
    default:
      throw CancellationError()
    }
  }
}

/// Creates `s1` and answers every release with `410`.
private actor GoneReleaseTransport: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  var deleteCount: Int { requests.filter { $0.httpMethod == "DELETE" }.count }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST": return recreateShapeResponse(id: "s1")
    case "GET":
      while !Task.isCancelled { await Task.yield() }
      throw CancellationError()
    case "DELETE": return recreateResponse("gone", status: 410)
    default: throw CancellationError()
    }
  }
}

private func recreateResponse(_ body: String, status: Int = 200) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
      headerFields: [:])!)
}

private func recreateShapeResponse(id: String) -> HTTPResponse {
  recreateResponse(
    "{\"shapeId\":\"\(id)\",\"table\":\"public.items\",\"streamPath\":\"/v1/streams/\(id)\",\"streamUrl\":\"https://streams.test/\(id)\"}"
  )
}

private let recreateRequest = ShapeRequest(
  table: "public.items",
  where: .leaf(column: "status", op: .eq, value: .string("open")),
  columns: ["id", "status"],
  subscription: "stable-claim")

private func recreateCoordinator(
  transport: any HTTPTransport,
  clock: any ShapeSubscriptionClock,
  maxRetries: Int = 0,
  kind: ShapeSubscriptionKind = .shape,
  recreatePolicy: ShapeSubscriptionRecreatePolicy = .init()
) -> ShapeSubscriptionCoordinator {
  ShapeSubscriptionCoordinator(
    client: ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport),
    transport: transport,
    request: recreateRequest,
    materializer: InMemoryShapeMaterializer(),
    retryPolicy: .init(maxRetries: maxRetries),
    clock: clock,
    kind: kind,
    recreatePolicy: recreatePolicy)
}

@Suite("Shape recreate on gone")
struct ShapeRecreateOnGoneTests {
  @Test func goneCreateFallsThroughToAFreshShapeWithinTheBound() async throws {
    let transport = GoneCreateTransport(goneCreates: 2)
    let clock = RecreateClock()
    let coordinator = recreateCoordinator(transport: transport, clock: clock)

    let handle = try await coordinator.start()

    #expect(handle.id == "s1")
    #expect(handle.subscription == "stable-claim")
    #expect(await transport.postCount == 3)
    #expect(await clock.delays == [.milliseconds(250), .milliseconds(250)])
    // The recreate is a byte-identical re-POST: the retired shape id only ever lived on the
    // server, so the table, predicate, columns, and stable claim survive untouched.
    let bodies = await transport.postBodies
    #expect(bodies.count == 3)
    #expect(Set(bodies).count == 1)
    try await coordinator.stop()
  }

  @Test func goneSubsetFeedCreateFallsThroughOnItsOwnNativeRoute() async throws {
    let transport = GoneCreateTransport(goneCreates: 1)
    let coordinator = recreateCoordinator(
      transport: transport, clock: RecreateClock(), kind: .subsetFeed)

    let handle = try await coordinator.start()

    #expect(handle.id == "s1")
    #expect(await transport.postCount == 2)
    let paths = await transport.requests.filter { $0.httpMethod == "POST" }.compactMap {
      $0.url?.path
    }
    #expect(paths == ["/v1/subset-feeds", "/v1/subset-feeds"])
    try await coordinator.stop()
  }

  @Test func goneCreateBeyondTheBoundIsTerminal() async throws {
    let transport = FixedCreateStatusTransport(status: 410)
    let coordinator = recreateCoordinator(transport: transport, clock: RecreateClock())

    await #expect(
      throws: ShapeSubscriptionFailure.client(.http(status: 410, message: "HTTP request failed"))
    ) { _ = try await coordinator.start() }

    #expect(await transport.postCount == 3)
    #expect(
      await coordinator.state
        == .failed(.client(.http(status: 410, message: "HTTP request failed"))))
  }

  @Test func notFoundOnCreateStaysTerminalWithoutRecreating() async throws {
    let transport = FixedCreateStatusTransport(status: 404)
    let coordinator = recreateCoordinator(transport: transport, clock: RecreateClock())

    await #expect(
      throws: ShapeSubscriptionFailure.client(.http(status: 404, message: "HTTP request failed"))
    ) { _ = try await coordinator.start() }

    #expect(await transport.postCount == 1)
  }

  @Test func serviceUnavailableOnCreateStaysRetryableNotRecreated() async throws {
    let transport = FixedCreateStatusTransport(status: 503)
    let coordinator = recreateCoordinator(
      transport: transport, clock: RecreateClock(), maxRetries: 1)

    await #expect(
      throws: ShapeSubscriptionFailure.retryExhausted(
        operation: "create", attempts: 2, cause: .http(status: 503))
    ) { _ = try await coordinator.start() }

    #expect(await transport.postCount == 2)
  }

  @Test func goneJoinReseedsThroughTheReplacementPathAndReleasesBothClaims() async throws {
    let transport = GoneJoinTransport()
    let coordinator = recreateCoordinator(transport: transport, clock: RecreateClock())

    let created = try await coordinator.start()
    #expect(created.id == "s1")

    await #expect(throws: ShapeSubscriptionFailure.self) { _ = try await coordinator.renew() }

    guard case .reseedRequired(let outcome) = await coordinator.state else {
      Issue.record("expected a reseed publication, got \(await coordinator.state)")
      return
    }
    // A recreated join reaches the application through exactly the vocabulary the engine's own
    // fall-through produces, which is the same `requireReseed` reconciliation the stream-read
    // gone receipt uses. Materialization is reseeded once, not duplicated across two generations.
    #expect(outcome.reason == .replacement(previousShapeID: "s1", replacementShapeID: "s2"))
    #expect(outcome.previous.id == "s1")
    #expect(outcome.previous.subscription == "stable-claim")
    #expect(outcome.replacement?.id == "s2")
    #expect(outcome.replacement?.subscription == "stable-claim")

    // The recreate is a byte-identical re-POST under the same stable claim.
    let bodies = await transport.postBodies
    #expect(bodies.count == 3)
    #expect(Set(bodies).count == 1)

    // Both the retired identity and the replacement are released under that claim, so the
    // application starts one explicit fresh scope instead of inheriting two live server claims.
    let deletes = await transport.deleteURLs
    #expect(Set(deletes.map(\.path)) == ["/v1/shapes/s1", "/v1/shapes/s2"])
    #expect(
      deletes.allSatisfy {
        URLComponents(url: $0, resolvingAgainstBaseURL: false)?.queryItems
          == [URLQueryItem(name: "subscription", value: "stable-claim")]
      })
  }

  @Test func goneOnANonCreateControlRouteStaysTerminal() async throws {
    let transport = GoneReleaseTransport()
    let coordinator = recreateCoordinator(transport: transport, clock: RecreateClock())

    _ = try await coordinator.start()
    await #expect(
      throws: ShapeSubscriptionFailure.client(.http(status: 410, message: "HTTP request failed"))
    ) { try await coordinator.stop() }

    #expect(await transport.deleteCount == 1)
  }

  @Test func recreatePolicyDefaultsToTwoRecreatesAndAShortBackoff() {
    let policy = ShapeSubscriptionRecreatePolicy()
    #expect(policy.maximumRecreates == 2)
    #expect(policy.backoff == .milliseconds(250))
  }

  @Test func aZeroRecreateBoundSurfacesTheFirstGoneAsTerminal() async throws {
    let transport = FixedCreateStatusTransport(status: 410)
    let coordinator = recreateCoordinator(
      transport: transport, clock: RecreateClock(),
      recreatePolicy: .init(maximumRecreates: 0))

    await #expect(
      throws: ShapeSubscriptionFailure.client(.http(status: 410, message: "HTTP request failed"))
    ) { _ = try await coordinator.start() }

    #expect(await transport.postCount == 1)
  }

  @Test func aWidenedRecreateBoundAndBackoffAreHonoured() async throws {
    let transport = GoneCreateTransport(goneCreates: 3)
    let clock = RecreateClock()
    let coordinator = recreateCoordinator(
      transport: transport, clock: clock,
      recreatePolicy: .init(maximumRecreates: 3, backoff: .milliseconds(10)))

    let handle = try await coordinator.start()

    #expect(handle.id == "s1")
    #expect(await transport.postCount == 4)
    #expect(await clock.delays == [.milliseconds(10), .milliseconds(10), .milliseconds(10)])
    try await coordinator.stop()
  }
}
