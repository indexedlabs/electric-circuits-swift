import ElectricCircuitsSwift
import Foundation

public enum CircuitsSubsetGroupSourceError: Error, Equatable, Sendable {
  /// A snapshot row or live envelope named a table that is not a member of the group.
  case tableOutsideGroup(String)
  case invalidSnapshotRow(table: String, index: Int)
  case invalidLiveKey(table: String, key: String)
  case invalidSnapshotSourceVersion(String)
  case invalidLiveSourceVersion(String?)
  case release(ClientError)
  case subscription(ShapeSubscriptionFailure)
  /// The group detached on the server and dropped output, then reattached: its stream carries a
  /// must-refetch marker. The session ends without applying the batch that carries it or moving
  /// its cursor; load the group again (a new `materialize`) and replace its rows with the new
  /// snapshot.
  case mustRefetch(groupID: String?, resumeLSN: String?)
}

/// The group's live session: its snapshot, and a tail that hands every stream batch to the
/// caller's store as one `CollectionGroupChangeBatch`. `stop` releases the group's claim.
public struct CollectionGroupSourceSession: Sendable {
  public let snapshot: CollectionGroupSnapshot
  public let run:
    @Sendable (
      @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void
    ) async throws -> Void
  public let stop: @Sendable () async throws -> Void

  public init(
    snapshot: CollectionGroupSnapshot,
    run:
      @escaping @Sendable (
        @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void
      ) async throws -> Void,
    stop: @escaping @Sendable () async throws -> Void
  ) {
    self.snapshot = snapshot
    self.run = run
    self.stop = stop
  }
}

/// A group's live tail continued from a stored cursor, without a new snapshot: a
/// `CollectionGroupSourceSession` minus the snapshot, as `CollectionResumeSession` is for one
/// table. The store already holds every member's rows, so the caller does not call
/// `replaceGroupSnapshot`.
///
/// `cursor` and `sourceVersion` are the stored position the tail starts from; the first batch's
/// `expectedCursor` is `cursor`. `run` hands batches to `apply` as a snapshot session does, and
/// calls `caughtUp` once, when the tail has read to the stream's head: every change the stream
/// held after `cursor` has been applied, or it held none. Until then the stored rows are not
/// known to be current, however many batches `apply` has committed. The source asks the stream
/// where its head is rather than waiting for it, so with nothing new `caughtUp` comes at once
/// (`Contracts/native-v1-contract-v1.md`, "Live after a resume"). A group whose stream will not
/// serve `cursor`, or whose next batch the source refuses, throws `CollectionResumeInvalidated`
/// from `run`: load the group again with `materialize` and replace its rows. `stop` releases the
/// group's claim.
public struct CollectionGroupResumeSession: Sendable {
  public let cursor: StreamCursor
  public let sourceVersion: CollectionSourceVersion
  public let run:
    @Sendable (
      _ apply: @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void,
      _ caughtUp: @escaping @Sendable () async -> Void
    ) async throws -> Void
  public let stop: @Sendable () async throws -> Void

  public init(
    cursor: StreamCursor,
    sourceVersion: CollectionSourceVersion,
    run:
      @escaping @Sendable (
        _ apply: @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void,
        _ caughtUp: @escaping @Sendable () async -> Void
      ) async throws -> Void,
    stop: @escaping @Sendable () async throws -> Void
  ) {
    self.cursor = cursor
    self.sourceVersion = sourceVersion
    self.run = run
    self.stop = stop
  }
}

/// Native Electric Circuits source for a shape group: one `loadSubsetGroup(_:)` call returns the
/// group's handle, its snapshot of every member and the stream frontier, and one subscription
/// then reads the group's one stream. Each envelope goes to its member by its `type`, rows are
/// keyed by table and key, and every stream batch reaches the store as one transaction with the
/// group's one cursor. A refused member, a `503` or a broken stream fails the whole group. The
/// server subscription claim a new group is loaded under is the materialization ID plus the
/// identity's generation, as in `CircuitsSubsetSource`.
///
/// The snapshot fence names the group's stream, the subscription that holds it and the
/// snapshot's transaction visibility (`group-v2`), so a later launch can `resume` the stored
/// group. The engine gives every group its own stream, and the group belongs to the subscription
/// its load named: that subscription re-joins it, held, released or lapsed, for as long as the
/// engine keeps it, and is answered with the same stream URL. So `resume` re-joins under the
/// fenced subscription (`ElectricCircuitsClient.rejoinSubsetGroup(_:)`, for which the engine
/// reads no rows), and the same stream URL proves the stored offset is still a position in the
/// stream the stored rows were read from. The engine marks output it dropped on that stream with
/// a must-refetch control, which ends the session for a reload.
public struct CircuitsSubsetGroupSource: Sendable {
  private let client: ElectricCircuitsClient
  private let transport: any HTTPTransport
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let capacity: ShapeSubscriptionCapacity?
  private let responseDecodingLimits: ResponseDecodingLimits
  private let telemetry: TelemetryReporter
  private let recreatePolicy: ShapeSubscriptionRecreatePolicy
  private let pendingCleanup: PendingGroupCleanup
  private let resumeFailures = FailedSubsetResumes()

  public init(
    client: ElectricCircuitsClient,
    transport: any HTTPTransport,
    retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    capacity: ShapeSubscriptionCapacity? = nil,
    responseDecodingLimits: ResponseDecodingLimits = .default,
    telemetry: TelemetryReporter = .noop,
    recreatePolicy: ShapeSubscriptionRecreatePolicy = .init()
  ) {
    self.client = client
    self.transport = transport
    self.retryPolicy = retryPolicy
    self.clock = clock
    self.capacity = capacity
    self.responseDecodingLimits = responseDecodingLimits
    self.telemetry = telemetry
    self.recreatePolicy = recreatePolicy
    pendingCleanup = PendingGroupCleanup(client: client)
  }

  public func materialize(
    _ demand: CollectionGroupDemand,
    identity: CollectionGroupDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionGroupSourceSession {
    try await pendingCleanup.retry(materializationID)
    let members = Self.members(of: demand)
    let request: SubsetGroupRequest
    let loadStarted: ContinuousClock.Instant
    let load: SubsetGroupResponse
    if let kept = await pendingCleanup.takeAdoptable(materializationID, members: members) {
      // A resume that found the group on another stream loaded it moments ago; that answer is
      // this snapshot, and its claim is the one the tail renews.
      (request, loadStarted, load) = (kept.request, kept.loadStarted, kept.load)
    } else {
      try await pendingCleanup.releaseAdoptable(materializationID)
      request = SubsetGroupRequest(
        members: members,
        subscription: materializationID.subscription(generation: identity.scope.generation))
      // The lease begins somewhere inside the load, so its age counts from before the request.
      loadStarted = ContinuousClock.now
      load = try await recreatePolicy.recreatingOnGone(clock: clock) {
        try await retryPolicy.retryingRefusedClaim(clock: clock) {
          try await client.loadSubsetGroup(request)
        }
      }
    }
    let handle = load.handle

    do {
      let routes = try GroupRoutes(demand: demand, load: load)
      guard
        let snapshotVersion = CircuitsSubsetSource<Never, Int>.pageVersion(
          SubsetResponse(rows: [], lsn: load.lsn, snapshot: load.snapshot, horizon: load.horizon))
      else {
        throw CircuitsSubsetGroupSourceError.invalidSnapshotSourceVersion(load.lsn)
      }
      var memberRows = Array(repeating: [ChangeRow](), count: demand.members.count)
      for (index, row) in load.rows.enumerated() {
        guard let member = routes.member(for: row.table) else {
          throw CircuitsSubsetGroupSourceError.tableOutsideGroup(row.table)
        }
        guard case .object(let value) = row.value else {
          throw CircuitsSubsetGroupSourceError.invalidSnapshotRow(table: row.table, index: index)
        }
        memberRows[member].append(value)
      }
      let snapshotMembers = try zip(demand.members, memberRows).map { member, rows in
        try member.codec.snapshot(rows)
      }
      let cursor = StreamCursor(offset: load.offset, lsn: snapshotVersion.rawValue)
      let lifecycle = CircuitsGroupSessionLifecycle(client: client, initialHandle: handle)
      let snapshotFence = SnapshotFence(
        rawValue: GroupSnapshotFence.v2(
          stream: load.streamURL.absoluteString, subscription: request.subscription ?? "",
          offset: load.offset, sourceVersion: snapshotVersion.rawValue,
          visibility: snapshotVersion.snapshot
        ).rawValue)
      let start = GroupTailStart(
        request: request, handle: handle, loadStarted: loadStarted, members: demand.members,
        routes: routes, cursor: cursor, gate: snapshotVersion, sourceVersion: snapshotVersion)
      let transport = transport
      let resumeFailures = resumeFailures

      return CollectionGroupSourceSession(
        snapshot: CollectionGroupSnapshot(
          members: snapshotMembers, fence: snapshotFence, sourceVersion: snapshotVersion,
          cursor: cursor),
        run: { apply in
          try await runTail(
            start, lifecycle: lifecycle, transport: transport, apply: apply,
            onFailure: { failure, refusedBatch in
              // The record this snapshot writes names this stream; a later resume from it would
              // meet the same outcome.
              if refusedBatch || subsetFailureInvalidatesResume(failure) {
                await resumeFailures.record(materializationID, fence: snapshotFence)
              }
            })
        },
        stop: { try await lifecycle.stop() }
      )
    } catch {
      try await release(handle, for: materializationID)
      throw error
    }
  }

  /// Continues a stored group from `record`'s cursor when the engine still serves the stream its
  /// fence names. Returns nil, so the caller loads the group with `materialize`, when:
  /// - the fence predates `group-v2` and so names neither its stream nor its subscription, or the
  ///   record has no cursor or belongs to another group demand;
  /// - the last session on this same fence ended in an outcome reading on from its cursor would
  ///   repeat: a terminal stream answer, a must-refetch marker, or a batch this source refused,
  ///   such as a live envelope without `headers.lsn`;
  /// - the engine answers the re-join under the fenced subscription with "no such group": it
  ///   retired the stored group, or that subscription now holds other members. The re-join
  ///   created nothing, so nothing is kept, and the `materialize` that follows loads the group
  ///   once, under this launch's own subscription;
  /// - the engine answers with another stream URL (the subscription holds a newer group), or
  ///   says the subscription belongs to something else (`409`). A new group's stream has nothing
  ///   of the gap since the stored cursor.
  ///
  /// Otherwise the session tails the same stream from the stored cursor and source version. A
  /// resume costs one `POST /v1/subsets/rejoin`, which takes the claim again and for which the
  /// engine reads no rows, and the caller replaces nothing in the store.
  ///
  /// A server without the re-join route (`404` or `405`: an engine or gateway from before it
  /// existed) is asked with the load that route replaced. That load renews the claim and the
  /// engine reads the group's snapshot for it; the source uses none of its rows when the answer
  /// names the fenced stream. When it names another, the load is kept for the `materialize` call
  /// that follows on this source instance, which uses it as its snapshot, so the fallback loads
  /// once; a caller that does not follow with `materialize` calls
  /// `cleanupAbandonedMaterialization`, which releases it. A re-join answered with another
  /// stream is followed by that same load, for the rows the new group holds.
  ///
  /// The stored group is held by the subscription the earlier load named, so the resumed session
  /// renews and releases under that name. It shares that name with the session that stored the
  /// group, and nothing else: the fence stores no claim identity, the re-join names none, and
  /// the engine answers this session a claim of its own, which is the one it renews and the only
  /// one it releases. A release the earlier session sent, arriving after this re-join, names
  /// that session's claim and changes nothing (`Contracts/native-v1-contract-v1.md`, "Claim
  /// identity"). Against a server that answers no claim identity the caller keeps the
  /// precondition `CollectionCoordinator.init` states for single tables: whatever held the group
  /// before has stopped, since a release it sent later would release this session's claim.
  ///
  /// `mode` is the caller's policy, as for a single table (`CollectionLoadMode`). With `.resume`,
  /// the default, the above applies. With `.snapshot` the group is to be read again: this makes
  /// no load, releases a load an earlier declined `resume` kept (it is as old as that call), and
  /// returns nil, so the `materialize` that follows loads the group anew under this launch's
  /// subscription. Calling `materialize` without `resume` does the same when no load was kept.
  public func resume(
    _ demand: CollectionGroupDemand,
    identity: CollectionGroupDemandIdentity,
    materializationID: CollectionMaterializationID,
    record: CollectionGroupMaterializationRecord,
    mode: CollectionLoadMode = .resume
  ) async throws -> CollectionGroupResumeSession? {
    guard mode == .resume else {
      try await pendingCleanup.releaseAdoptable(materializationID)
      return nil
    }
    let fence = GroupSnapshotFence(rawValue: record.snapshotFence.rawValue)
    guard record.id == materializationID, record.demand.cacheKey == identity.cacheKey,
      let cursor = record.cursor,
      case .v2(let fencedStream, let subscription, let snapshotOffset, _, let visibility)? = fence,
      !subscription.isEmpty
    else { return nil }
    guard await resumeFailures.allowsResume(materializationID, from: record.snapshotFence) else {
      return nil
    }
    // Drain any earlier failed setup's release, and drop a kept load no `materialize` consumed:
    // this request re-establishes the claim.
    try await pendingCleanup.retry(materializationID)
    try await pendingCleanup.releaseAdoptable(materializationID)
    let request = SubsetGroupRequest(members: Self.members(of: demand), subscription: subscription)
    // The lease begins somewhere inside the request that takes the claim.
    var claimStarted = ContinuousClock.now
    let claimed: (handle: ShapeHandle, results: [SubsetGroupMemberResult])
    do {
      let rejoin = try await recreatePolicy.recreatingOnGone(clock: clock) {
        try await retryPolicy.retryingRefusedClaim(clock: clock) {
          try await client.rejoinSubsetGroup(request)
        }
      }
      // The claim a re-join just took, which a load that follows it renews.
      let held: String? = if case .joined(let claim) = rejoin { claim.claim } else { nil }
      switch rejoin {
      case .joined(let claim) where claim.streamURL.absoluteString == fencedStream:
        claimed = (claim.handle, claim.results)
      case .noSuchGroup:
        return nil
      case .joined, .unavailable:
        // No re-join route, or the subscription now holds a group on another stream: ask with
        // the load, whose rows are the snapshot when the stored cursor cannot continue.
        claimStarted = ContinuousClock.now
        let load = try await recreatePolicy.recreatingOnGone(clock: clock) {
          try await retryPolicy.retryingRefusedClaim(clock: clock) {
            try await client.loadSubsetGroup(request, holding: held)
          }
        }
        guard load.streamURL.absoluteString == fencedStream else {
          await pendingCleanup.keepAdoptable(
            PendingGroupCleanup.KeptLoad(request: request, loadStarted: claimStarted, load: load),
            for: materializationID)
          return nil
        }
        claimed = (load.handle, load.results)
      }
    } catch ClientError.http(status: 409, message: _) {
      return nil
    }

    let routes: GroupRoutes
    do {
      routes = try GroupRoutes(demand: demand, results: claimed.results)
    } catch {
      try await release(claimed.handle, for: materializationID)
      throw error
    }
    let lifecycle = CircuitsGroupSessionLifecycle(client: client, initialHandle: claimed.handle)
    let start = GroupTailStart(
      request: request, handle: claimed.handle, loadStarted: claimStarted,
      members: demand.members, routes: routes, cursor: cursor,
      gate: CircuitsSubsetSource<Never, Int>.resumedGate(
        record.sourceVersion, fencedVisibility: visibility,
        atSnapshotOffset: cursor.offset == snapshotOffset),
      sourceVersion: record.sourceVersion)
    let transport = transport
    let resumeFailures = resumeFailures
    let storedFence = record.snapshotFence
    return CollectionGroupResumeSession(
      cursor: cursor,
      sourceVersion: record.sourceVersion,
      run: { apply, caughtUp in
        do {
          try await runTail(
            start, lifecycle: lifecycle, transport: transport, apply: apply,
            // The stored rows are current once the tail has read from the stored cursor to the
            // stream's head (`Contracts/native-v1-contract-v1.md`, "Live after a resume").
            caughtUp: caughtUp,
            onFailure: { failure, refusedBatch in
              if refusedBatch || subsetFailureInvalidatesResume(failure) {
                await resumeFailures.record(materializationID, fence: storedFence)
              }
            })
        } catch {
          if await !resumeFailures.allowsResume(materializationID, from: storedFence) {
            throw CollectionResumeInvalidated(cause: error)
          }
          throw error
        }
      },
      stop: { try await lifecycle.stop() }
    )
  }

  /// Each member is named by its table, so the answer's keys and the stream's `type` route back
  /// to the member whatever schema spelling the server uses for the table.
  private static func members(of demand: CollectionGroupDemand) -> [SubsetGroupMember] {
    demand.members.map {
      SubsetGroupMember(
        table: $0.table, where: $0.sourcePredicate, columns: $0.columns, key: $0.table)
    }
  }

  /// Releases a group a failed setup created, keeping its handle for a later retry when the
  /// release itself fails.
  private func release(
    _ handle: ShapeHandle, for materializationID: CollectionMaterializationID
  ) async throws {
    do {
      try await client.releaseShape(handle)
    } catch {
      await pendingCleanup.record(handle, for: materializationID)
      if let release = error as? ClientError {
        throw CircuitsSubsetGroupSourceError.release(release)
      }
      throw error
    }
  }

  /// Where a session's live tail starts: the claim a load or a re-join took and when that request
  /// was sent, the identical request the subscription coordinator renews with, the snapshot's or
  /// the stored position, and the version the tail gates snapshot overlap at.
  private struct GroupTailStart: Sendable {
    let request: SubsetGroupRequest
    let handle: ShapeHandle
    let loadStarted: ContinuousClock.Instant
    let members: [CollectionGroupMember]
    let routes: GroupRoutes
    let cursor: StreamCursor
    let gate: CollectionSourceVersion
    let sourceVersion: CollectionSourceVersion
  }

  /// Reads the group's stream from `start.cursor` through the awaited tail materializer. The
  /// coordinator starts on the group `start.handle` claimed. If the group was released in between
  /// (a retired table, a reset or a lapsed lease), its first read ends the stream and the caller
  /// reseeds.
  ///
  /// With `caughtUp`, a resumed tail's, the stream is first read to its head without waiting
  /// there, and `caughtUp` is called once every change up to that head has been applied.
  private func runTail(
    _ start: GroupTailStart,
    lifecycle: CircuitsGroupSessionLifecycle,
    transport: any HTTPTransport,
    apply: @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void,
    caughtUp: (@Sendable () async -> Void)? = nil,
    onFailure: @escaping @Sendable (ShapeSubscriptionFailure, _ refusedBatch: Bool) async -> Void
  ) async throws {
    let materializer = CircuitsGroupTailMaterializer(
      members: start.members, routes: start.routes, cursor: start.cursor, gate: start.gate,
      sourceVersion: start.sourceVersion, apply: apply)
    let onCaughtUp: (@Sendable (StreamCursor) async -> Void)? =
      if let caughtUp { { _ in await caughtUp() } } else { nil }
    let coordinator = ShapeSubscriptionCoordinator(
      client: client,
      transport: transport,
      groupRequest: start.request,
      materializer: materializer,
      retryPolicy: retryPolicy,
      clock: clock,
      capacity: capacity,
      responseDecodingLimits: responseDecodingLimits,
      telemetry: telemetry,
      recreatePolicy: recreatePolicy,
      setupClaim: start.handle,
      setupClaimAge: ContinuousClock.now - start.loadStarted,
      onCaughtUp: onCaughtUp
    )
    try await lifecycle.install(coordinator, materializer: materializer)
    let states = await coordinator.stateUpdates
    do {
      try await withTaskCancellationHandler {
        try await coordinator.start()
        await materializer.open()
        await lifecycle.markCoordinatorStarted()
        for await state in states {
          try Task.checkCancellation()
          switch state {
          case .failed(let failure):
            // A stop refuses the tail before it stops the coordinator, so a batch caught
            // between the two fails the coordinator; that failure is the stop. Any other
            // failure, including one that came before the stop, is reported.
            if await materializer.failedForStop() { return }
            // A batch the materializer could not route or decode: the stream serves it again
            // at the same cursor.
            let refused = await materializer.takeTerminalFailure()
            await onFailure(failure, refused != nil)
            if let refused { throw refused }
            // The reader refused the batch carrying a must-refetch marker without applying
            // it or moving the cursor: the caller reloads the group from a snapshot.
            if case .stream(.mustRefetch(_, let control)) = failure {
              throw CircuitsSubsetGroupSourceError.mustRefetch(
                groupID: control.groupID, resumeLSN: control.resumeLSN)
            }
            throw CircuitsSubsetGroupSourceError.subscription(failure)
          case .reseedRequired(let outcome):
            await onFailure(.reseedRequired(outcome), false)
            throw CircuitsSubsetGroupSourceError.subscription(.reseedRequired(outcome))
          case .stopped:
            return
          default:
            continue
          }
        }
      } onCancel: {
        Task { _ = try? await lifecycle.stop() }
      }
    } catch {
      try await lifecycle.stop()
      throw error
    }
  }

  /// Retries the release of a group whose setup failed after the load created it, and releases
  /// a load a declined `resume` kept that no `materialize` used.
  public func cleanupAbandonedMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    try await pendingCleanup.retry(materializationID)
    try await pendingCleanup.releaseAdoptable(materializationID)
  }
}

/// The group source's snapshot fence, framed like `SubsetSnapshotFence`.
///
/// `group-v1|<offset>|<lsn>` predates resume: it names neither the stream its cursor belongs to
/// nor the subscription that holds the group, so it still parses but is never resumable.
/// `group-v2|<streamUrl>|<subscription>|<offset>|<lsn>|<snapshot>|<horizon>` adds both, and the
/// snapshot's transaction visibility, which a resumed tail gates at.
enum GroupSnapshotFence: Equatable, Sendable {
  case v1(offset: String, sourceVersion: String)
  case v2(
    stream: String, subscription: String, offset: String, sourceVersion: String,
    visibility: CollectionPageSnapshot?)

  init?(rawValue: String) {
    switch SubsetSnapshotFence.components(of: rawValue) {
    case let parts? where parts.count == 3 && parts[0] == "group-v1":
      self = .v1(offset: parts[1], sourceVersion: parts[2])
    case let parts? where parts.count == 7 && parts[0] == "group-v2":
      let visibility: CollectionPageSnapshot?
      if parts[5].isEmpty, parts[6].isEmpty {
        visibility = nil
      } else {
        // Visibility the fence names but that does not parse cannot gate a resumed tail.
        guard let order = CollectionPageSnapshot.postgresLSN(parts[4]),
          let parsed = CollectionPageSnapshot(lsn: order, snapshot: parts[5], horizon: parts[6])
        else { return nil }
        visibility = parsed
      }
      self = .v2(
        stream: parts[1], subscription: parts[2], offset: parts[3], sourceVersion: parts[4],
        visibility: visibility)
    default:
      return nil
    }
  }

  var rawValue: String {
    switch self {
    case .v1(let offset, let sourceVersion):
      SubsetSnapshotFence.framed(["group-v1", offset, sourceVersion])
    case .v2(let stream, let subscription, let offset, let sourceVersion, let visibility):
      SubsetSnapshotFence.framed([
        "group-v2", stream, subscription, offset, sourceVersion, visibility?.snapshotText ?? "",
        visibility?.horizonText ?? "",
      ])
    }
  }
}

/// Maps a table name, in the request's or the server's spelling, to its member's index.
struct GroupRoutes: Sendable {
  private let indexByTable: [String: Int]

  init(demand: CollectionGroupDemand, load: SubsetGroupResponse) throws {
    try self.init(demand: demand, results: load.results)
  }

  /// `results` are the members a load or a re-join answered, which the client has already
  /// checked are the requested members in order.
  init(demand: CollectionGroupDemand, results: [SubsetGroupMemberResult]) throws {
    var indexByTable: [String: Int] = [:]
    for (index, member) in demand.members.enumerated() {
      indexByTable[member.table] = index
    }
    for (index, result) in results.enumerated() {
      if let existing = indexByTable[result.table], existing != index {
        throw CircuitsSubsetGroupSourceError.tableOutsideGroup(result.table)
      }
      indexByTable[result.table] = index
    }
    self.indexByTable = indexByTable
  }

  func member(for table: String) -> Int? { indexByTable[table] }
}

/// A row's identity in a group stream: its table and its key. Equal keys in two tables are two
/// rows.
private struct GroupRowKey: Hashable, Sendable {
  let table: Int
  let key: String
}

/// A failed setup has no session to return, but it can already have created the group. Keep its
/// handle until the idempotent release succeeds, as `CircuitsSubsetSource` does for a feed.
///
/// It also holds the load a resume made and declined because the engine answered with another
/// stream. The `materialize` that follows uses that load as its snapshot instead of loading
/// again; abandoned-materialization cleanup releases one no `materialize` used.
private actor PendingGroupCleanup {
  struct KeptLoad: Sendable {
    let request: SubsetGroupRequest
    let loadStarted: ContinuousClock.Instant
    let load: SubsetGroupResponse
  }

  private let client: ElectricCircuitsClient
  private var handles: [CollectionMaterializationID: ShapeHandle] = [:]
  private var adoptable: [CollectionMaterializationID: KeptLoad] = [:]

  func keepAdoptable(_ load: KeptLoad, for materializationID: CollectionMaterializationID) {
    adoptable[materializationID] = load
  }

  /// The kept load, when it claimed exactly `members`.
  func takeAdoptable(
    _ materializationID: CollectionMaterializationID, members: [SubsetGroupMember]
  ) -> KeptLoad? {
    guard adoptable[materializationID]?.request.members == members else { return nil }
    return adoptable.removeValue(forKey: materializationID)
  }

  func releaseAdoptable(_ materializationID: CollectionMaterializationID) async throws {
    guard let handle = adoptable[materializationID]?.load.handle else { return }
    try await client.releaseShape(handle)
    if adoptable[materializationID]?.load.handle == handle {
      adoptable.removeValue(forKey: materializationID)
    }
  }

  init(client: ElectricCircuitsClient) {
    self.client = client
  }

  func record(_ handle: ShapeHandle, for materializationID: CollectionMaterializationID) {
    handles[materializationID] = handle
  }

  func retry(_ materializationID: CollectionMaterializationID) async throws {
    guard let handle = handles[materializationID] else { return }
    try await client.releaseShape(handle)
    handles.removeValue(forKey: materializationID)
  }
}

private actor CircuitsGroupSessionLifecycle {
  private let client: ElectricCircuitsClient
  private let initialHandle: ShapeHandle
  private var coordinator: ShapeSubscriptionCoordinator?
  private var materializer: CircuitsGroupTailMaterializer?
  private var coordinatorStarted = false
  private var stopped = false
  private var stopTask: Task<Void, Error>?

  init(client: ElectricCircuitsClient, initialHandle: ShapeHandle) {
    self.client = client
    self.initialHandle = initialHandle
  }

  func install(
    _ coordinator: ShapeSubscriptionCoordinator, materializer: CircuitsGroupTailMaterializer
  ) async throws {
    guard !stopped else {
      await materializer.stopBeforeOpen()
      try await coordinator.stop()
      throw CancellationError()
    }
    self.coordinator = coordinator
    self.materializer = materializer
  }

  func markCoordinatorStarted() {
    coordinatorStarted = true
  }

  func stop() async throws {
    if let stopTask {
      do {
        try await stopTask.value
      } catch {
        self.stopTask = nil
        throw error
      }
      return
    }
    stopped = true
    let coordinator = coordinator
    let materializer = materializer
    let coordinatorStarted = coordinatorStarted
    let client = client
    let initialHandle = initialHandle
    let task = Task<Void, Error> {
      // A stop can come before `run` finished starting, while the coordinator's reader waits
      // on the setup permit; the coordinator's stop waits for that reader, so refuse it first.
      // Once setup has opened the tail, the coordinator's stop alone ends the reader.
      await materializer?.stopBeforeOpen()
      if let coordinator {
        try await coordinator.stop()
      }
      if !coordinatorStarted {
        try await client.releaseShape(initialHandle)
      }
    }
    stopTask = task
    try await task.value
  }
}

/// Turns each group stream batch into one `CollectionGroupChangeBatch`. Envelopes are routed by
/// `type`, deduplicated against the snapshot per (table, key), and the cursor moves only after
/// the caller's store has committed every member's changes with it.
private actor CircuitsGroupTailMaterializer: ShapeMaterializer {
  private let members: [CollectionGroupMember]
  private let routes: GroupRoutes
  private var cursor: StreamCursor?
  private var snapshotSourceVersion: CollectionSourceVersion
  private var passedSnapshotHorizon = false
  private var rowVersions: [GroupRowKey: CollectionSourceVersion] = [:]
  // Actors are reentrant across store application; hold this FIFO permit across it so a second
  // application cannot commit against the same cursor. Setup holds it until `run` has finished
  // starting its subscription.
  private var applying = true
  private var setupHoldsPermit = true
  private var refused = false
  private var refusingForStop = false
  /// Set when a batch was refused because the session was stopping, so the coordinator failure
  /// that refusal causes is reported as the stop rather than as a materializer failure.
  private var rejectedForStop = false
  private var applicationWaiters: [CheckedContinuation<Void, Never>] = []
  private var sourceVersion: CollectionSourceVersion
  private let applyBatch: @Sendable (CollectionGroupChangeBatch) async throws -> Void
  /// The routing or decoding error that ended this materializer. The coordinator reports any
  /// materializer error as `.materializer`, so the source rethrows the typed cause.
  private var terminalFailure: (any Error)?

  init(
    members: [CollectionGroupMember],
    routes: GroupRoutes,
    cursor: StreamCursor,
    gate: CollectionSourceVersion,
    sourceVersion: CollectionSourceVersion,
    apply: @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void
  ) {
    self.members = members
    self.routes = routes
    self.cursor = cursor
    snapshotSourceVersion = gate
    self.sourceVersion = sourceVersion
    applyBatch = apply
  }

  /// Lets batches through once `run` has finished starting its subscription.
  func open() { releaseSetupPermit() }

  /// Refuses every later batch: the session is stopping before it opened. A batch already
  /// applying finishes.
  func refuse() {
    refused = true
    releaseSetupPermit()
  }

  /// Refuses batches only while setup still holds the permit, so a stop before `run` finished
  /// starting frees a reader waiting on it. After `open()` it does nothing: a batch arriving
  /// while the session stops is then an ordinary batch, not a failure.
  func stopBeforeOpen() {
    guard setupHoldsPermit else { return }
    refusingForStop = true
    refuse()
  }

  /// Whether the materializer's last failure was a batch refused because the session stopped.
  func failedForStop() -> Bool { rejectedForStop }

  private func releaseSetupPermit() {
    guard setupHoldsPermit else { return }
    setupHoldsPermit = false
    releasePermit()
  }

  private func releasePermit() {
    if applicationWaiters.isEmpty {
      applying = false
    } else {
      applicationWaiters.removeFirst().resume()
    }
  }

  func takeTerminalFailure() -> (any Error)? {
    defer { terminalFailure = nil }
    return terminalFailure
  }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo nextCursor: StreamCursor
  ) async throws {
    if applying {
      await withCheckedContinuation { applicationWaiters.append($0) }
    } else {
      applying = true
    }
    defer { releasePermit() }
    try Task.checkCancellation()
    if refused {
      if refusingForStop { rejectedForStop = true }
      throw CancellationError()
    }
    if cursor == nextCursor { return }
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: cursor, advancingTo: nextCursor)
    }
    let groupBatch: CollectionGroupChangeBatch
    let nextRowVersions: [GroupRowKey: CollectionSourceVersion]
    let passedHorizon: Bool
    let latest: CollectionSourceVersion
    let accepted: [Int]
    do {
      (groupBatch, nextRowVersions, passedHorizon, latest, accepted) = try decode(
        batch, expecting: expectedCursor, advancingTo: nextCursor)
    } catch {
      terminalFailure = error
      throw error
    }
    try await LiveDelivery.applying(changesAt: accepted) { try await applyBatch(groupBatch) }
    cursor = groupBatch.cursor
    sourceVersion = latest
    rowVersions = nextRowVersions
    passedSnapshotHorizon = passedHorizon
    if passedHorizon {
      // Release the gate's xip; persisted canonical row versions retain their own visibility.
      snapshotSourceVersion = CollectionSourceVersion(
        rawValue: snapshotSourceVersion.rawValue, order: snapshotSourceVersion.order)
    }
  }

  /// The same rules for an envelope's position as the single-table source
  /// (`CircuitsCollectionTailMaterializer.decode`), with every row identified by its member and
  /// key rather than its key alone.
  private func decode(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?,
    advancingTo nextCursor: StreamCursor
  ) throws -> (
    CollectionGroupChangeBatch, [GroupRowKey: CollectionSourceVersion], Bool,
    CollectionSourceVersion, [Int]
  ) {
    var wire = Array(repeating: [CollectionGroupMemberCodec.WireChange](), count: members.count)
    var nextRowVersions = rowVersions
    var passedHorizon = passedSnapshotHorizon
    var latest = sourceVersion
    // The envelopes the snapshot gate lets through, for `LiveDelivery.applying(changesAt:_:)`.
    var accepted: [Int] = []
    for (index, envelope) in batch.envelopes.enumerated() {
      guard let member = routes.member(for: envelope.type) else {
        throw CircuitsSubsetGroupSourceError.tableOutsideGroup(envelope.type)
      }
      guard let rawVersion = envelope.headers.lsn,
        let lsn = CircuitsSubsetSource<Never, Int>.postgresLSN(rawVersion)
      else {
        throw CircuitsSubsetGroupSourceError.invalidLiveSourceVersion(envelope.headers.lsn)
      }
      let version = CollectionSourceVersion(
        rawValue: lsn.rawValue, order: lsn.order,
        transactionID: envelope.headers.txid.flatMap(CollectionPageSnapshot.xid32))
      if let snapshot = snapshotSourceVersion.snapshot, version.order >= snapshot.horizon {
        passedHorizon = true
      }
      let rowKey = GroupRowKey(table: member, key: envelope.key)
      if let rowVersion = nextRowVersions[rowKey] {
        guard version.supersedes(rowVersion) else { continue }
      } else if passedHorizon {
        // The ordered tail has retired the snapshot gate permanently.
      } else if let snapshot = snapshotSourceVersion.snapshot {
        if snapshot.includes(lsn: version.order, transactionID: version.transactionID) {
          continue
        }
      } else {
        guard version >= snapshotSourceVersion else { continue }
      }
      latest = max(latest, version)
      accepted.append(index)
      nextRowVersions[rowKey] = version
      switch envelope.headers.operation {
      case .delete:
        wire[member].append(.delete(envelope.key, version))
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        wire[member].append(.upsert(value, version))
      }
    }
    let changes = try zip(members, wire).map { member, wire in try member.codec.changes(wire) }
    return (
      CollectionGroupChangeBatch(
        members: changes, expectedCursor: expectedCursor,
        cursor: StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue),
        sourceVersion: latest),
      nextRowVersions, passedHorizon, latest, accepted
    )
  }
}
