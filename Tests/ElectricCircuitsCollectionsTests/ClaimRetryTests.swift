import ElectricCircuitsSwift
import Foundation
import Testing

@testable import ElectricCircuitsCollections

/// Records each sleep and returns at once, so a retry's delay is read without waiting it out.
private actor RetryClock: ShapeSubscriptionClock {
  private(set) var sleeps: [Duration] = []
  func sleep(for duration: Duration) async throws { sleeps.append(duration) }
}

private func retryPolicy(_ maxRetries: Int) -> ShapeSubscriptionRetryPolicy {
  ShapeSubscriptionRetryPolicy(
    maxRetries: maxRetries, baseDelay: .milliseconds(250), maximumDelay: .seconds(30),
    jitterRatio: 0)
}

private func feedSource(_ engine: ContractEngine, clock: RetryClock, maxRetries: Int)
  -> CircuitsSubsetSource<ContractItem, Int64>
{
  CircuitsSubsetSource<ContractItem, Int64>(
    client: ElectricCircuitsClient(baseURL: URL(string: "https://engine.test")!, transport: engine),
    transport: engine, table: "items", retryPolicy: retryPolicy(maxRetries), clock: clock,
    decodeRow: ContractItem.init(row:), decodeKey: contractKey)
}

private func groupSource(_ engine: ContractEngine, clock: RetryClock, maxRetries: Int)
  -> CircuitsSubsetGroupSource
{
  CircuitsSubsetGroupSource(
    client: ElectricCircuitsClient(baseURL: URL(string: "https://engine.test")!, transport: engine),
    transport: engine, retryPolicy: retryPolicy(maxRetries), clock: clock)
}

private let copyID = CollectionMaterializationID(rawValue: "retry-copy")

/// A source claims its feed or its group before a subscription coordinator exists, so the
/// coordinator's retries do not cover that request. An engine at its shape cap refuses a create
/// with `503` and `Retry-After` until a subscription is released or lapses; the request is fine
/// and succeeds when repeated. The source repeats it on its retry policy, waiting at least what
/// the server asked for, where it used to fail the scope on the first refusal.
@Suite("A claim request refused as retryable")
struct ClaimRetryTests {
  @Test func aFeedCreateRefused503IsRepeatedAfterTheServersRetryAfter() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    await engine.refuseClaims(.feedCreate, status: 503, retryAfter: "2", count: 2)
    let clock = RetryClock()

    let session = try await feedSource(engine, clock: clock, maxRetries: 3).materialize(
      listOneItems, identity: contractIdentity("launch-1"), materializationID: copyID)

    // Two refusals, each waited out for the server's two seconds (the policy's own backoff is
    // shorter), then the create that succeeded and the snapshot behind it.
    #expect(await engine.count(.feedCreate) == 3)
    #expect(await clock.sleeps == [.seconds(2), .seconds(2)])
    #expect(await engine.count(.snapshotQuery) == 1)
    #expect(session.snapshot.rows.count == 2)
    try await session.stop()
  }

  @Test func aFeedCreateStillRefusedWhenTheBudgetIsSpentFailsWithTheRefusal() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    await engine.refuseClaims(.feedCreate, status: 503, retryAfter: "1", count: 5)
    let clock = RetryClock()

    await #expect(throws: ClientError.retryableHTTP(status: 503, retryAfter: .seconds(1))) {
      _ = try await feedSource(engine, clock: clock, maxRetries: 2).materialize(
        listOneItems, identity: contractIdentity("launch-1"), materializationID: copyID)
    }

    #expect(await engine.count(.feedCreate) == 3)
    #expect(await clock.sleeps.count == 2)
    #expect(await engine.count(.snapshotQuery) == 0)
  }

  @Test func aFeedCreateRefusedAsInvalidIsNotRepeated() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    await engine.refuseClaims(.feedCreate, status: 400)
    let clock = RetryClock()

    await #expect(throws: ClientError.http(status: 400, message: "HTTP request failed")) {
      _ = try await feedSource(engine, clock: clock, maxRetries: 3).materialize(
        listOneItems, identity: contractIdentity("launch-1"), materializationID: copyID)
    }

    #expect(await engine.count(.feedCreate) == 1)
    #expect(await clock.sleeps.isEmpty)
  }

  @Test func aGroupLoadRefused503IsRepeatedAfterTheServersRetryAfter() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    await engine.refuseClaims(.groupLoad, status: 503, retryAfter: "1")
    let clock = RetryClock()
    let demand = contractGroupDemand()

    let session = try await groupSource(engine, clock: clock, maxRetries: 3).materialize(
      demand, identity: demand.identity(scope: contractScope("launch-1")),
      materializationID: copyID)

    #expect(await engine.count(.groupLoad) == 2)
    #expect(await clock.sleeps == [.seconds(1)])
    try await session.stop()
  }

  /// A re-join is refused `503` while a load of the same subscription is still in flight on the
  /// engine, and by an engine that cannot serve. Either way it is repeated, not turned into a
  /// snapshot: only "no such group" says the stored group is gone.
  @Test func aGroupRejoinRefused503IsRepeatedAndThenResumes() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = contractGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let first = demand.identity(scope: contractScope("launch-1"))
    let loaded = try await groupSource(engine, clock: RetryClock(), maxRetries: 0).materialize(
      demand, identity: first, materializationID: copyID)
    try await store.replaceGroupSnapshot(loaded.snapshot, materializationID: copyID, demand: first)
    let stored = try #require(try await store.groupMaterialization(copyID))
    await engine.refuseClaims(.groupRejoin, status: 503, retryAfter: "1")
    let clock = RetryClock()

    let resumed = try await groupSource(engine, clock: clock, maxRetries: 3).resume(
      demand, identity: demand.identity(scope: contractScope("launch-2")),
      materializationID: copyID, record: stored)

    #expect(resumed != nil)
    #expect(await engine.count(.groupRejoin) == 2)
    #expect(await engine.count(.groupLoad) == 1)
    #expect(await clock.sleeps == [.seconds(1)])
    try await resumed?.stop()
  }
}
