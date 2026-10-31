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

/// Native Electric Circuits source for a shape group: one `loadSubsetGroup(_:)` call returns the
/// group's handle, its snapshot of every member and the stream frontier, and one subscription
/// then reads the group's one stream. Each envelope goes to its member by its `type`, rows are
/// keyed by table and key, and every stream batch reaches the store as one transaction with the
/// group's one cursor. A refused member, a `503` or a broken stream fails the whole group. A stable
/// materialization ID is also the server subscription claim.
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
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionGroupSourceSession {
    try await pendingCleanup.retry(materializationID)
    // Each member is named by its table, so the answer's keys and the stream's `type` route back
    // to the member whatever schema spelling the server uses for the table.
    let request = SubsetGroupRequest(
      members: demand.members.map {
        SubsetGroupMember(
          table: $0.table, where: $0.sourcePredicate, columns: $0.columns, key: $0.table)
      },
      subscription: materializationID.rawValue)
    let load = try await recreatePolicy.recreatingOnGone(clock: clock) {
      try await client.loadSubsetGroup(request)
    }
    let handle = load.handle
    let loadedAt = ContinuousClock.now

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
      let client = client
      let transport = transport
      let retryPolicy = retryPolicy
      let clock = clock
      let capacity = capacity
      let responseDecodingLimits = responseDecodingLimits
      let telemetry = telemetry
      let recreatePolicy = recreatePolicy

      return CollectionGroupSourceSession(
        snapshot: CollectionGroupSnapshot(
          members: snapshotMembers,
          fence: SnapshotFence(
            rawValue: Self.snapshotFence(
              offset: load.offset, sourceVersion: snapshotVersion.rawValue)),
          sourceVersion: snapshotVersion, cursor: cursor),
        run: { apply in
          let materializer = CircuitsGroupTailMaterializer(
            members: demand.members, routes: routes, cursor: cursor,
            sourceVersion: snapshotVersion, apply: apply)
          // The coordinator starts on the group this setup loaded and reads its stream from the
          // snapshot's cursor. If the group was released in between (a retired table, a reset or
          // a lapsed lease), its first read ends the stream and the caller reseeds.
          let coordinator = ShapeSubscriptionCoordinator(
            client: client,
            transport: transport,
            groupRequest: request,
            materializer: materializer,
            retryPolicy: retryPolicy,
            clock: clock,
            capacity: capacity,
            responseDecodingLimits: responseDecodingLimits,
            telemetry: telemetry,
            recreatePolicy: recreatePolicy,
            setupClaim: handle,
            setupClaimAge: ContinuousClock.now - loadedAt
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
                  if let cause = await materializer.takeTerminalFailure() { throw cause }
                  throw CircuitsSubsetGroupSourceError.subscription(failure)
                case .reseedRequired(let outcome):
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
        },
        stop: { try await lifecycle.stop() }
      )
    } catch {
      do {
        try await client.releaseShape(handle)
      } catch {
        await pendingCleanup.record(handle, for: materializationID)
        if let release = error as? ClientError {
          throw CircuitsSubsetGroupSourceError.release(release)
        }
        throw error
      }
      throw error
    }
  }

  /// Retries the release of a group whose setup failed after the load created it.
  public func cleanupAbandonedMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    try await pendingCleanup.retry(materializationID)
  }

  private static func snapshotFence(offset: String, sourceVersion: String) -> String {
    ["group-v1", offset, sourceVersion]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
  }
}

/// Maps a table name, in the request's or the server's spelling, to its member's index.
struct GroupRoutes: Sendable {
  private let indexByTable: [String: Int]

  init(demand: CollectionGroupDemand, load: SubsetGroupResponse) throws {
    var indexByTable: [String: Int] = [:]
    for (index, member) in demand.members.enumerated() {
      indexByTable[member.table] = index
    }
    // `loadSubsetGroup(_:)` has already checked that the results are the members in order.
    for (index, result) in load.results.enumerated() {
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
private actor PendingGroupCleanup {
  private let client: ElectricCircuitsClient
  private var handles: [CollectionMaterializationID: ShapeHandle] = [:]

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
    sourceVersion: CollectionSourceVersion,
    apply: @escaping @Sendable (CollectionGroupChangeBatch) async throws -> Void
  ) {
    self.members = members
    self.routes = routes
    self.cursor = cursor
    snapshotSourceVersion = sourceVersion
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
    do {
      (groupBatch, nextRowVersions, passedHorizon, latest) = try decode(
        batch, expecting: expectedCursor, advancingTo: nextCursor)
    } catch {
      terminalFailure = error
      throw error
    }
    try await applyBatch(groupBatch)
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

  /// The same snapshot gate as the single-table source, with every row identified by its
  /// member and key rather than its key alone.
  private func decode(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?,
    advancingTo nextCursor: StreamCursor
  ) throws -> (
    CollectionGroupChangeBatch, [GroupRowKey: CollectionSourceVersion], Bool,
    CollectionSourceVersion
  ) {
    var wire = Array(repeating: [CollectionGroupMemberCodec.WireChange](), count: members.count)
    var nextRowVersions = rowVersions
    var passedHorizon = passedSnapshotHorizon
    var latest = sourceVersion
    for envelope in batch.envelopes {
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
      if snapshotSourceVersion.snapshot != nil || passedHorizon {
        nextRowVersions[rowKey] = version
      }
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
      nextRowVersions, passedHorizon, latest
    )
  }
}
