import ElectricCircuitsSwift
import Foundation
import Testing

@testable import ElectricCircuitsCollections

/// The fixture's tables as a group whose members both have plain predicates, so the engine
/// positions every envelope on its stream.
private func plainGroupDemand() -> CollectionGroupDemand {
  CollectionGroupDemand(members: [
    CollectionGroupMember(
      definition: contractLists, table: "lists", unsafePredicateIdentity: "owner=u1",
      sourcePredicate: .leaf(column: "owner", op: .eq, value: .string("u1")),
      decodeRow: ContractList.init(row:), decodeKey: contractKey),
    CollectionGroupMember(
      definition: contractItems, table: "items", unsafePredicateIdentity: "list_id=1",
      sourcePredicate: .leaf(column: "list_id", op: .eq, value: .int(1)),
      decodeRow: ContractItem.init(row:), decodeKey: contractKey),
  ])
}

private let groupID = CollectionMaterializationID(rawValue: "group-copy")

private func groupIdentity(_ demand: CollectionGroupDemand, _ generation: String)
  -> CollectionGroupDemandIdentity
{
  demand.identity(scope: contractScope(generation))
}

private actor GroupResumeLog {
  enum Event: Equatable, Sendable {
    case applied
    case caughtUp
  }

  private(set) var events: [Event] = []
  var batches: Int { events.filter { $0 == .applied }.count }
  var caughtUp: Int { events.filter { $0 == .caughtUp }.count }
  func applied() { events.append(.applied) }
  func idle() { events.append(.caughtUp) }
}

private func items(_ store: InMemoryCollectionGroupStore) async -> [Int64: ContractItem] {
  await store.rows(
    in: contractItems.id, scope: contractScope("any"), as: ContractItem.self, key: Int64.self)
}

private func lists(_ store: InMemoryCollectionGroupStore) async -> [Int64: ContractList] {
  await store.rows(
    in: contractLists.id, scope: contractScope("any"), as: ContractList.self, key: Int64.self)
}

/// A group belongs to its subscription on the engine. A stored group resumes by re-joining it
/// under that subscription (`POST /v1/subsets/rejoin`), which reads no rows on the engine, and
/// tailing the stream the fence names from the stored cursor. When the engine says there is no
/// such group, the group is loaded once, as a snapshot. An engine without the re-join route is
/// asked with a load instead, as before the route existed.
@Suite("Native Circuits group source resume")
struct CircuitsSubsetGroupSourceResumeTests {
  /// Launch 1 loads the group and stores its snapshot, then ends without releasing its claim, as
  /// a killed process does.
  private func firstLaunch(
    _ engine: ContractEngine, demand: CollectionGroupDemand, store: InMemoryCollectionGroupStore
  ) async throws -> CollectionGroupMaterializationRecord {
    let identity = groupIdentity(demand, "launch-1")
    let session = try await contractGroupSource(engine).materialize(
      demand, identity: identity, materializationID: groupID)
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: groupID, demand: identity)
    return try #require(try await store.groupMaterialization(groupID))
  }

  @Test func theFenceNamesTheStreamAndTheSubscriptionThatHoldsIt() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let stored = try await firstLaunch(
      engine, demand: plainGroupDemand(), store: InMemoryCollectionGroupStore())
    let subscription = groupID.subscription(generation: "launch-1")

    let fence = try #require(GroupSnapshotFence(rawValue: stored.snapshotFence.rawValue))
    guard case .v2(let stream, let named, let offset, let lsn, let visibility) = fence else {
      Issue.record("expected a group-v2 fence")
      return
    }
    #expect(stream == ContractEngine.streamURL("group-1"))
    #expect(named == subscription)
    #expect(offset == ContractEngine.start)
    #expect(lsn == "0/1BF2C98")
    #expect(visibility?.snapshotText == "767:767:")
    #expect(GroupSnapshotFence(rawValue: fence.rawValue) == fence)
    // A fence written before resume still reads, and names nothing to resume.
    let old = "8:group-v1|2:10|4:0/10"
    #expect(GroupSnapshotFence(rawValue: old) == .v1(offset: "10", sourceVersion: "0/10"))
    #expect(GroupSnapshotFence(rawValue: old)?.rawValue == old)
  }

  @Test func aLaterLaunchResumesAStoredGroupWhoseClaimIsStillHeld() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    // Changes the engine wrote to the group's stream while no process was reading it: the
    // fixture's plain `items` changes and its `lists` insert, each with its position.
    let next = await engine.append(
      "group-1", EngineEnvelopeFixture.body(fixture.plain.envelopes + [fixture.group.envelopes[3]]))
    // Rows the engine would answer a new snapshot with; a resume must not use them.
    await engine.setGroupRows("[]")

    let source = contractGroupSource(engine)
    let resumed = try #require(
      try await source.resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    #expect(resumed.cursor == stored.cursor)
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }

    #expect(await contractEventually { await log.batches == 1 })
    #expect(
      await items(store) == [
        1: ContractItem(id: 1, title: "one-b"), 2: ContractItem(id: 2, title: "two"),
      ])
    #expect(
      await lists(store) == [
        1: ContractList(id: 1, owner: "u1"), 3: ContractList(id: 3, owner: "u1"),
      ])
    let record = try #require(try await store.groupMaterialization(groupID))
    #expect(record.cursor == StreamCursor(offset: next, lsn: "0/1BF3080"))
    #expect(record.snapshotFence == stored.snapshotFence)
    // The first launch loaded the group. This one re-joined it under the subscription that
    // holds it, for which the engine reads no rows, and read on from the stored offset.
    let subscription = groupID.subscription(generation: "launch-1")
    #expect(await engine.requests(of: .groupLoad).map(\.subscription) == [subscription])
    #expect(await engine.requests(of: .groupRejoin).map(\.subscription) == [subscription])
    #expect(await engine.requests(of: .streamRead).first?.offset == ContractEngine.start)
    #expect(await engine.requests(of: .streamRead).first?.stream == "group-1")

    try await resumed.stop()
    try await run.value
    #expect(await engine.requests(of: .release).map(\.subscription) == [subscription])
  }

  /// A resumed session shares its subscription with the launch that stored the group, and
  /// nothing else. It holds no claim to name when it re-joins (none is ever stored), is answered
  /// a claim of its own, and releases only that one: the earlier launch's release, arriving
  /// late, changes nothing, and this launch's stop releases this launch's claim.
  @Test func aResumedSessionInANewProcessReleasesOnlyItsOwnClaim() async throws {
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, releasesGroups: false)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    // Launch 1 loads the group and is killed holding its claim.
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    let subscription = groupID.subscription(generation: "launch-1")
    let firstClaim = try #require(await engine.heldClaim(subscription))
    #expect(!stored.snapshotFence.rawValue.contains(firstClaim), "a claim is never stored")

    // Launch 2: a new process, so a new source, resuming from the stored record.
    let resumed = try #require(
      try await contractGroupSource(engine).resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }
    #expect(await contractEventually { await log.caughtUp == 1 })
    // It held no claim to name, and the engine answered it one of its own.
    #expect(await engine.requests(of: .groupRejoin).map(\.claim) == [nil])
    let ownClaim = try #require(await engine.heldClaim(subscription))
    #expect(ownClaim != firstClaim)

    // Launch 1's release arrives late. It names launch 1's claim, which nobody holds any more.
    var late = URLComponents(string: "https://engine.test/v1/shapes/shape%2Fgroup-1")!
    late.queryItems = [
      URLQueryItem(name: "subscription", value: subscription),
      URLQueryItem(name: "claim", value: firstClaim),
    ]
    var lateRelease = URLRequest(url: late.url!)
    lateRelease.httpMethod = "DELETE"
    #expect(try await engine.send(lateRelease).response.statusCode == 200)
    #expect(await engine.heldClaim(subscription) == ownClaim, "launch 2's claim is still held")

    try await resumed.stop()
    try await run.value
    #expect(await engine.requests(of: .release).last?.claim == ownClaim)
    #expect(await engine.heldClaim(subscription) == nil, "its own release drops its own claim")
  }

  /// A launch that stopped cleanly released its group. The engine keeps a released group for its
  /// subscription to come back to, so the next launch re-joins it and reads what it missed from
  /// the stored offset: no load, and no rows read on the engine.
  @Test func aLaterLaunchResumesAGroupTheEarlierLaunchReleased() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, releasesGroups: false)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let first = groupIdentity(demand, "launch-1")
    let session = try await contractGroupSource(engine).materialize(
      demand, identity: first, materializationID: groupID)
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: groupID, demand: first)
    try await session.stop()
    let stored = try #require(try await store.groupMaterialization(groupID))
    let subscription = groupID.subscription(generation: "launch-1")
    #expect(await engine.requests(of: .release).map(\.subscription) == [subscription])
    // A change the engine wrote to the group's stream while nothing held the group.
    let next = await engine.append(
      "group-1", EngineEnvelopeFixture.body([fixture.plain.envelopes[0]]))
    await engine.setGroupRows("[]")

    let resumed = try #require(
      try await contractGroupSource(engine).resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }

    #expect(await contractEventually { await log.caughtUp == 1 })
    #expect(await log.events == [.applied, .caughtUp])
    #expect(await items(store)[1] == ContractItem(id: 1, title: "one-b"))
    #expect(try await store.groupMaterialization(groupID)?.cursor?.offset == next)
    #expect(await engine.count(.groupLoad) == 1)
    #expect(await engine.requests(of: .groupRejoin).map(\.subscription) == [subscription])
    try await resumed.stop()
    try await run.value
  }

  /// An engine or gateway without the re-join route answers it `404`. The source then asks as
  /// it did before the route existed: a load under the fenced subscription, whose rows it
  /// ignores when the answer names the fenced stream.
  @Test func anEngineWithoutTheRejoinRouteIsAskedWithALoad() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, rejoinRoute: false)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.append("group-1", EngineEnvelopeFixture.body([fixture.plain.envelopes[0]]))
    await engine.setGroupRows("[]")

    let resumed = try #require(
      try await contractGroupSource(engine).resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }

    #expect(await contractEventually { await log.caughtUp == 1 })
    #expect(await items(store)[1] == ContractItem(id: 1, title: "one-b"))
    let subscription = groupID.subscription(generation: "launch-1")
    #expect(await engine.requests(of: .groupRejoin).map(\.subscription) == [subscription])
    #expect(
      await engine.requests(of: .groupLoad).map(\.subscription) == [subscription, subscription])
    try await resumed.stop()
    try await run.value
  }

  /// With nothing new on the group's stream, the stored rows are current as soon as the stream
  /// says the stored offset is its head: at once, under long-poll and SSE alike, and not when a
  /// long-poll window at the head runs out (this engine never answers one unasked).
  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func aResumedGroupWithNothingNewReportsCaughtUpWithoutWaitingOutAPoll(mode: LiveReadMode)
    async throws
  {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)

    let resumed = try #require(
      try await contractGroupSource(engine, liveReadMode: mode).resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }

    #expect(await contractEventually { await log.caughtUp == 1 })
    #expect(await log.events == [.caughtUp])
    #expect(try await store.groupMaterialization(groupID) == stored)
    if mode == .longPoll {
      // The catch-up read, then the live tail from the same offset.
      #expect(await contractEventually { await engine.count(.streamRead) == 2 })
      #expect(await engine.requests(of: .streamRead).map(\.live) == [nil, "long-poll"])
    }
    try await resumed.stop()
    try await run.value
  }

  /// A resumed group that is two reads behind reports caught up once, after the batch that ends
  /// at the stream's head is committed, and never between the two.
  @Test(arguments: [LiveReadMode.longPoll, .sse])
  func aResumedGroupReportsCaughtUpOnlyAfterEveryChangeUpToTheHead(mode: LiveReadMode)
    async throws
  {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.append("group-1", EngineEnvelopeFixture.body([fixture.plain.envelopes[0]]))
    let head = await engine.append(
      "group-1", EngineEnvelopeFixture.body([fixture.group.envelopes[3]]))

    let resumed = try #require(
      try await contractGroupSource(engine, liveReadMode: mode).resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored))
    let log = GroupResumeLog()
    let run = Task {
      try await resumed.run(
        { batch in
          try await store.applyGroup(batch, to: groupID)
          await log.applied()
        }, { await log.idle() })
    }

    #expect(await contractEventually { await log.caughtUp == 1 })
    #expect(await log.events == [.applied, .applied, .caughtUp])
    #expect(try await store.groupMaterialization(groupID)?.cursor?.offset == head)
    #expect(await items(store)[1] == ContractItem(id: 1, title: "one-b"))
    #expect(await lists(store)[3] == ContractList(id: 3, owner: "u1"))
    try await resumed.stop()
    try await run.value
  }

  /// The engine retired the group, so the re-join under the stored subscription answers that
  /// there is no such group, and creates none. The group is then loaded once, as a snapshot,
  /// under this launch's own subscription.
  @Test(arguments: [true, false])
  func aStoredGroupTheEngineRetiredLoadsOnceAsASnapshot(released: Bool) async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let subscription = groupID.subscription(generation: "launch-1")
    let stored: CollectionGroupMaterializationRecord
    if released {
      // The earlier launch stopped its session, which released the claim.
      let identity = groupIdentity(demand, "launch-1")
      let session = try await contractGroupSource(engine).materialize(
        demand, identity: identity, materializationID: groupID)
      try await store.replaceGroupSnapshot(
        session.snapshot, materializationID: groupID, demand: identity)
      try await session.stop()
      stored = try #require(try await store.groupMaterialization(groupID))
    } else {
      stored = try await firstLaunch(engine, demand: demand, store: store)
      await engine.lapseGroup(subscription: subscription)
    }
    await engine.setGroupRows(
      #"[{"table":"public.lists","value":{"id":1,"owner":"u1"}},"#
        + #"{"table":"public.items","value":{"id":1,"list_id":1,"title":"one-b"}}]"#)

    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored) == nil)
    let session = try await source.materialize(
      demand, identity: identity, materializationID: groupID)
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: groupID, demand: identity)

    // One re-join, which found nothing, and one load: the first launch's and this snapshot's.
    let launched = groupID.subscription(generation: "launch-2")
    #expect(await engine.requests(of: .groupRejoin).map(\.subscription) == [subscription])
    #expect(
      await engine.requests(of: .groupLoad).map(\.subscription) == [subscription, launched])
    #expect(await items(store) == [1: ContractItem(id: 1, title: "one-b")])
    let record = try #require(try await store.groupMaterialization(groupID))
    #expect(record.demand == identity)
    #expect(
      GroupSnapshotFence(rawValue: record.snapshotFence.rawValue)
        == .v2(
          stream: ContractEngine.streamURL("group-2"), subscription: launched,
          offset: ContractEngine.start, sourceVersion: "0/1BF2C98",
          visibility: record.sourceVersion.snapshot))
    try await session.stop()
    #expect(await engine.requests(of: .release).last?.subscription == launched)
  }

  /// The same on an engine without the re-join route: the load that stands in for the re-join
  /// answers with another stream, and that one load is the snapshot.
  @Test func withoutTheRejoinRouteARetiredGroupStillLoadsOnce() async throws {
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, rejoinRoute: false)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let subscription = groupID.subscription(generation: "launch-1")
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.lapseGroup(subscription: subscription)

    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored) == nil)
    let session = try await source.materialize(
      demand, identity: identity, materializationID: groupID)
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: groupID, demand: identity)

    #expect(await engine.count(.groupRejoin) == 1)
    #expect(
      await engine.requests(of: .groupLoad).map(\.subscription) == [subscription, subscription])
    let record = try #require(try await store.groupMaterialization(groupID))
    #expect(
      GroupSnapshotFence(rawValue: record.snapshotFence.rawValue)
        == .v2(
          stream: ContractEngine.streamURL("group-2"), subscription: subscription,
          offset: ContractEngine.start, sourceVersion: "0/1BF2C98",
          visibility: record.sourceVersion.snapshot))
    try await session.stop()
  }

  /// A caller that wants the group read again says so with the mode it passes for a single
  /// table: `resume` then declines without a request, and `materialize` loads the group under
  /// this launch's subscription even though the stored group could have been continued.
  @Test func aSnapshotModeResumeDeclinesAGroupItCouldContinue() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.setGroupRows(
      #"[{"table":"public.lists","value":{"id":1,"owner":"u1"}},"#
        + #"{"table":"public.items","value":{"id":1,"list_id":1,"title":"read again"}}]"#)
    let before = await engine.requests.count

    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored,
        mode: .snapshot) == nil)
    #expect(await engine.requests.count == before)

    let session = try await source.materialize(
      demand, identity: identity, materializationID: groupID)
    try await store.replaceGroupSnapshot(
      session.snapshot, materializationID: groupID, demand: identity)
    #expect(await items(store) == [1: ContractItem(id: 1, title: "read again")])
    #expect(
      await engine.requests(of: .groupLoad).last?.subscription
        == groupID.subscription(generation: "launch-2"))
    try await session.stop()

    // With `.resume`, the same record continues: the default.
    let resumed = try #require(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored, mode: .resume))
    try await resumed.stop()
  }

  /// A load an earlier declined resume kept is as old as that resume. A caller that asks for a
  /// snapshot gets a new load, not that one. (A resume keeps a load only where a load stood in
  /// for the re-join: an engine without the route.)
  @Test func aSnapshotModeResumeReleasesALoadAnEarlierResumeKept() async throws {
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, rejoinRoute: false)
    let demand = plainGroupDemand()
    let stored = try await firstLaunch(
      engine, demand: demand, store: InMemoryCollectionGroupStore())
    await engine.lapseGroup(subscription: groupID.subscription(generation: "launch-1"))
    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored) == nil)
    #expect(await engine.count(.groupLoad) == 2)
    #expect(await engine.count(.release) == 0)

    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored,
        mode: .snapshot) == nil)
    #expect(await engine.count(.groupLoad) == 2)
    #expect(await engine.count(.release) == 1)

    let session = try await source.materialize(
      demand, identity: identity, materializationID: groupID)
    #expect(await engine.count(.groupLoad) == 3)
    try await session.stop()
  }

  /// A declined resume's load is released when no snapshot follows it.
  @Test func cleanupReleasesALoadADeclinedResumeKept() async throws {
    let engine = ContractEngine(
      snapshots: [contractSnapshot], groupRows: contractGroupRows, rejoinRoute: false)
    let demand = plainGroupDemand()
    let stored = try await firstLaunch(
      engine, demand: demand, store: InMemoryCollectionGroupStore())
    await engine.lapseGroup(subscription: groupID.subscription(generation: "launch-1"))

    let source = contractGroupSource(engine)
    #expect(
      try await source.resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored) == nil)
    #expect(await engine.count(.release) == 0)
    try await source.cleanupAbandonedMaterialization(groupID)
    #expect(await engine.count(.release) == 1)
    try await source.cleanupAbandonedMaterialization(groupID)
    #expect(await engine.count(.release) == 1)
  }

  /// A batch the source refuses waits at the stored cursor. The resumed session ends with
  /// `CollectionResumeInvalidated`, and the source declines that fence from then on.
  @Test func aResumedGroupThatMeetsARefusedEnvelopeIsInvalidated() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = contractGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.append("group-1", fixture.group.body)

    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    let resumed = try #require(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored))
    do {
      try await resumed.run({ batch in try await store.applyGroup(batch, to: groupID) }, {})
      Issue.record("the resumed tail must end at the refused batch")
    } catch let invalidated as CollectionResumeInvalidated {
      #expect(
        invalidated.cause as? CircuitsSubsetGroupSourceError == .invalidLiveSourceVersion(nil))
    }
    #expect(try await store.groupMaterialization(groupID) == stored)
    #expect(await items(store)[1] == ContractItem(id: 1, title: "one"))

    // No request: the fence is known to lead into the refused batch.
    let before = await engine.requests.count
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored) == nil)
    #expect(await engine.requests.count == before)
    // The snapshot that follows is a new group under this launch's own subscription.
    let session = try await source.materialize(
      demand, identity: identity, materializationID: groupID)
    #expect(
      await engine.requests(of: .groupLoad).last?.subscription
        == groupID.subscription(generation: "launch-2"))
    try await session.stop()
  }

  /// The engine dropped output on the stream and says so with a must-refetch marker, which a
  /// resumed read from the stored cursor reaches like any other entry.
  @Test func aMustRefetchMarkerInvalidatesAResumedGroup() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let store = InMemoryCollectionGroupStore()
    let stored = try await firstLaunch(engine, demand: demand, store: store)
    await engine.append(
      "group-1",
      #"[{"type":"__circuits.control","key":"must-refetch","value":{"control":"must-refetch","#
        + #""reason":"group-detached","groupId":"shape/group-1","resumeLsn":"0/1BF2D00","#
        + #""throughLsn":"0/1BF3080"},"headers":{"operation":"control","last":true}}]"#)

    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    let resumed = try #require(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored))
    do {
      try await resumed.run({ batch in try await store.applyGroup(batch, to: groupID) }, {})
      Issue.record("the resumed tail must end at the marker")
    } catch let invalidated as CollectionResumeInvalidated {
      #expect(
        invalidated.cause as? CircuitsSubsetGroupSourceError
          == .mustRefetch(groupID: "shape/group-1", resumeLSN: "0/1BF2D00"))
    }
    #expect(try await store.groupMaterialization(groupID) == stored)
    #expect(
      try await source.resume(
        demand, identity: identity, materializationID: groupID, record: stored) == nil)
  }

  @Test func recordsThatCannotResumeDeclineBeforeAnyRequest() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let stored = try await firstLaunch(
      engine, demand: demand, store: InMemoryCollectionGroupStore())
    let source = contractGroupSource(engine)
    let identity = groupIdentity(demand, "launch-2")
    func record(
      id: CollectionMaterializationID = groupID,
      demand recorded: CollectionGroupDemandIdentity = stored.demand,
      fence: String = stored.snapshotFence.rawValue, cursor: StreamCursor? = stored.cursor
    ) -> CollectionGroupMaterializationRecord {
      CollectionGroupMaterializationRecord(
        id: id, demand: recorded, collections: stored.collections,
        snapshotFence: SnapshotFence(rawValue: fence), sourceVersion: stored.sourceVersion,
        cursor: cursor)
    }
    let unresumable = [
      // A fence written before resume names neither stream nor subscription.
      record(fence: "8:group-v1|33:\(ContractEngine.start)|9:0/1BF2C98"),
      record(fence: "not a fence"),
      record(cursor: nil),
      record(id: CollectionMaterializationID(rawValue: "another-copy")),
      // Another group's record under this ID.
      record(demand: groupIdentity(contractGroupDemand(), "launch-1")),
    ]
    for record in unresumable {
      #expect(
        try await source.resume(
          demand, identity: identity, materializationID: groupID, record: record) == nil)
    }
    #expect(await engine.count(.groupLoad) == 1)
    #expect(await engine.count(.groupRejoin) == 0)
  }

  /// A resume that found no group left nothing behind on the engine: there is no load to
  /// release, and cleanup has nothing to do.
  @Test func aResumeThatFoundNoGroupKeepsNothingToRelease() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot], groupRows: contractGroupRows)
    let demand = plainGroupDemand()
    let stored = try await firstLaunch(
      engine, demand: demand, store: InMemoryCollectionGroupStore())
    await engine.lapseGroup(subscription: groupID.subscription(generation: "launch-1"))

    let source = contractGroupSource(engine)
    #expect(
      try await source.resume(
        demand, identity: groupIdentity(demand, "launch-2"), materializationID: groupID,
        record: stored) == nil)
    try await source.cleanupAbandonedMaterialization(groupID)
    #expect(await engine.count(.groupLoad) == 1)
    #expect(await engine.count(.release) == 0)
  }
}
