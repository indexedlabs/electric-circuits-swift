import Foundation
import Testing

@testable import ElectricCircuitsSwift

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// The engine's claim rules as the client sees them (`Contracts/native-v1-contract-v1.md`,
/// "Claim identity"), for one shape or one group:
///
/// - a create, a load or a re-join takes or renews the claim its subscription holds and answers
///   the claim's identity. A request that names that identity (`?claim=…`) keeps it; any other is
///   answered a new one, `claim-1`, `claim-2`, and so on;
/// - a release that names a claim (`?subscription=…&claim=…`) releases it only while it is the
///   one held; a release that names none releases whatever claim the subscription holds.
///
/// `passesClaimNames: false` is a server on the way that drops the query parameter, as a gateway
/// from before claim identities does: the engine then sees no name on any request.
private actor ClaimEngine: HTTPTransport {
  private(set) var requests: [URLRequest] = []
  /// The identity of the claim each subscription holds.
  private(set) var held: [String: String] = [:]
  private var granted = 0
  private let passesClaimNames: Bool
  private let rejoinRoute: Bool

  init(passesClaimNames: Bool = true, rejoinRoute: Bool = true) {
    self.passesClaimNames = passesClaimNames
    self.rejoinRoute = rejoinRoute
  }

  private struct Body: Decodable {
    let subscription: String?
  }

  static func query(_ request: URLRequest, _ name: String) -> String? {
    request.url
      .flatMap { URLComponents(url: $0, resolvingAgainstBaseURL: false) }?
      .queryItems?.first { $0.name == name }?.value
  }

  /// The `claim` each request of `method` named, in order; nil for one that named none.
  func claims(named method: String, path suffix: String = "") -> [String?] {
    requests
      .filter { $0.httpMethod == method && ($0.url?.path ?? "").hasSuffix(suffix) }
      .map { Self.query($0, "claim") }
  }

  private func grant(_ request: URLRequest) -> (subscription: String, claim: String) {
    let subscription =
      request.httpBody.flatMap { try? JSONDecoder().decode(Body.self, from: $0) }?.subscription
      ?? ""
    let named = passesClaimNames ? Self.query(request, "claim") : nil
    if let current = held[subscription], current == named { return (subscription, current) }
    granted += 1
    held[subscription] = "claim-\(granted)"
    return (subscription, "claim-\(granted)")
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    let path = request.url?.path ?? ""
    switch request.httpMethod {
    case "POST" where path.hasSuffix("/v1/subsets/rejoin") && !rejoinRoute:
      return answer("", status: 404)
    case "POST" where path.contains("/v1/subsets/"):
      let (subscription, claim) = grant(request)
      let rows = path.hasSuffix("/load") ? #""lsn":"0/1","rows":[],"# : ""
      return answer(
        #"{"groupId":"shape/g1","shapeId":"shape/g1","subscription":"\#(subscription)","#
          + #""leaseSeconds":300,"claim":"\#(claim)","streamPath":"shape/g1","#
          + #""streamUrl":"https://streams.test/g1","offset":"0_0",\#(rows)"#
          + #""results":[{"key":"items","table":"public.items","shapeId":"s1"}]}"#)
    case "POST":
      let (subscription, claim) = grant(request)
      return answer(
        #"{"shapeId":"s1","table":"public.items","streamPath":"shape/s1","#
          + #""streamUrl":"https://streams.test/s1","subscription":"\#(subscription)","#
          + #""leaseSeconds":300,"claim":"\#(claim)"}"#)
    case "DELETE":
      if let subscription = Self.query(request, "subscription") {
        let named = passesClaimNames ? Self.query(request, "claim") : nil
        if named == nil || held[subscription] == named { held.removeValue(forKey: subscription) }
      }
      return answer(#"{"ok":true}"#)
    default:
      // The stream's long poll: held open until the reader is cancelled.
      try await Task.sleep(for: .seconds(3_600))
      throw CancellationError()
    }
  }

  private func answer(_ body: String, status: Int = 200) -> HTTPResponse {
    HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
        headerFields: [:])!)
  }
}

/// A clock whose sleeps never end, so a lease loop renews only when a test asks.
private actor ParkedClock: ShapeSubscriptionClock {
  func sleep(for duration: Duration) async throws {
    try await Task.sleep(for: .seconds(3_600))
  }
}

private func client(_ engine: ClaimEngine) -> ElectricCircuitsClient {
  ElectricCircuitsClient(baseURL: URL(string: "https://engine.test")!, transport: engine)
}

private func coordinator(
  _ engine: ClaimEngine, kind: ShapeSubscriptionKind = .shape, subscription: String = "device"
) -> ShapeSubscriptionCoordinator {
  ShapeSubscriptionCoordinator(
    client: client(engine), transport: engine,
    request: ShapeRequest(table: "public.items", subscription: subscription),
    materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
    clock: ParkedClock(), kind: kind)
}

private func groupCoordinator(_ engine: ClaimEngine) -> ShapeSubscriptionCoordinator {
  ShapeSubscriptionCoordinator(
    client: client(engine), transport: engine,
    groupRequest: SubsetGroupRequest(
      members: [SubsetGroupMember(table: "items", key: "items")], subscription: "scope"),
    materializer: InMemoryShapeMaterializer(), retryPolicy: .init(maxRetries: 0),
    clock: ParkedClock())
}

/// A claim has an identity, and a session names the one it holds: on every renewal, so the
/// engine keeps it, and on its release, so the release cannot drop a claim a later session took
/// under the same subscription.
@Suite("Claim identity")
struct ClaimIdentityTests {
  @Test func aReleaseNamesTheClaimItsHandleHolds() async throws {
    let engine = ClaimEngine()
    let client = client(engine)
    let request = ShapeRequest(table: "public.items", subscription: "device")
    let handle = try await client.createShape(request)

    try await client.releaseShape(handle)

    let release = try #require(await engine.requests.last)
    #expect(release.httpMethod == "DELETE")
    #expect(ClaimEngine.query(release, "subscription") == "device")
    #expect(ClaimEngine.query(release, "claim") == "claim-1")
    #expect(await engine.held.isEmpty)
  }

  /// The race the identity closes, seen from the client: session A's handle, released again
  /// after session B took the claim under the same subscription, releases nothing of B's.
  @Test func aRetriedReleaseDoesNotReleaseTheClaimALaterSessionHolds() async throws {
    let engine = ClaimEngine()
    let request = ShapeRequest(table: "public.items", subscription: "device")
    let sessionA = client(engine)
    let handleA = try await sessionA.createShape(request)
    try await sessionA.releaseShape(handleA)
    // Session B: another client, the same subscription, the same shape.
    let sessionB = client(engine)
    let handleB = try await sessionB.createShape(request)

    try await sessionA.releaseShape(handleA)

    #expect(await engine.held == ["device": "claim-2"], "B's claim is still held")
    try await sessionB.releaseShape(handleB)
    #expect(await engine.held.isEmpty, "B's own release names B's claim")
    #expect(await engine.claims(named: "DELETE") == ["claim-1", "claim-1", "claim-2"])
  }

  @Test(arguments: [ShapeSubscriptionKind.shape, .subsetFeed])
  func aRenewalNamesTheHeldClaimAndTheReleaseAfterItStillWorks(kind: ShapeSubscriptionKind)
    async throws
  {
    let engine = ClaimEngine()
    let coordinator = coordinator(engine, kind: kind)
    try await coordinator.start()

    try await coordinator.renew()
    try await coordinator.renew()

    // The create named none (the session held none); each renewal named the claim it holds.
    #expect(await engine.claims(named: "POST") == [nil, "claim-1", "claim-1"])
    #expect(await engine.held == ["device": "claim-1"], "a renewal keeps the claim's identity")
    try await coordinator.stop()
    #expect(await engine.claims(named: "DELETE") == ["claim-1"])
    #expect(await engine.held.isEmpty, "the release after the renewals drops the claim")
  }

  /// A server on the way that drops the claim's name (a gateway from before claim identities)
  /// makes every renewal look like a new session's to the engine, which answers a new identity
  /// each time. The session holds the latest answer, and that is the one its release names.
  @Test func aRenewalAnsweredAnotherClaimIsReleasedByThatOne() async throws {
    let engine = ClaimEngine(passesClaimNames: false)
    let coordinator = coordinator(engine)
    try await coordinator.start()

    let renewed = try await coordinator.renew()

    #expect(await engine.held == ["device": "claim-2"])
    #expect(await engine.claims(named: "POST") == [nil, "claim-1"])
    #expect(try await coordinator.start() == renewed, "the session holds the renewal's handle")
    try await coordinator.stop()
    #expect(await engine.claims(named: "DELETE") == ["claim-2"], "the release names the latest")
  }

  @Test func aGroupRenewalReJoinsNamingItsClaimAndItsReleaseNamesIt() async throws {
    let engine = ClaimEngine()
    let coordinator = groupCoordinator(engine)
    try await coordinator.start()

    try await coordinator.renew()
    try await coordinator.renew()

    #expect(await engine.claims(named: "POST", path: "/v1/subsets/load") == [nil])
    #expect(
      await engine.claims(named: "POST", path: "/v1/subsets/rejoin") == ["claim-1", "claim-1"])
    #expect(await engine.held == ["scope": "claim-1"])
    try await coordinator.stop()
    #expect(await engine.claims(named: "DELETE") == ["claim-1"])
    #expect(await engine.held.isEmpty)
  }

  /// Without the re-join route a group renews by load, and the load names the claim too.
  @Test func aGroupRenewalByLoadNamesItsClaim() async throws {
    let engine = ClaimEngine(rejoinRoute: false)
    let coordinator = groupCoordinator(engine)
    try await coordinator.start()

    try await coordinator.renew()

    #expect(await engine.claims(named: "POST", path: "/v1/subsets/load") == [nil, "claim-1"])
    #expect(await engine.held == ["scope": "claim-1"])
    try await coordinator.stop()
    #expect(await engine.claims(named: "DELETE") == ["claim-1"])
  }
}
