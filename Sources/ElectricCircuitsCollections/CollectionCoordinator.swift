import ElectricCircuitsSwift
import Foundation

public enum CollectionLoadFailure: Equatable, Sendable {
  case sourceUnavailable
  case storeUnavailable
}

public enum CollectionLoadState: Equatable, Sendable {
  case unavailable
  case cached
  case refreshing
  case live
  case failed(CollectionLoadFailure)
}

/// An eviction cannot invalidate a materialization while it has an active or transitioning lease.
public enum CollectionEvictionError: Error, Equatable, Sendable {
  case activeDemand
}

/// One consumer's cancellable interest in a collection demand. Copying is intentionally impossible:
/// each acquired lease has one independently idempotent `release()` lifecycle.
public actor CollectionLease {
  public nonisolated let stateUpdates: AsyncStream<CollectionLoadState>

  fileprivate nonisolated let id: UUID
  private let stateAction: @Sendable (UUID) async -> CollectionLoadState
  private let refreshAction: @Sendable (UUID) async -> Void
  private let releaseAction: @Sendable (UUID) async throws -> Void
  private var released = false

  init(
    id: UUID,
    stateUpdates: AsyncStream<CollectionLoadState>,
    stateAction: @escaping @Sendable (UUID) async -> CollectionLoadState,
    refreshAction: @escaping @Sendable (UUID) async -> Void,
    releaseAction: @escaping @Sendable (UUID) async throws -> Void
  ) {
    self.id = id
    self.stateUpdates = stateUpdates
    self.stateAction = stateAction
    self.refreshAction = refreshAction
    self.releaseAction = releaseAction
  }

  public func state() async -> CollectionLoadState {
    guard !released else { return .unavailable }
    return await stateAction(id)
  }

  public func refresh() async {
    guard !released else { return }
    await refreshAction(id)
  }

  public func release() async throws {
    guard !released else { return }
    try await releaseAction(id)
    released = true
  }
}

private actor AtMostOnceStop {
  private let action: @Sendable () async throws -> Void
  private var task: Task<Void, Error>?

  init(_ action: @escaping @Sendable () async throws -> Void) {
    self.action = action
  }

  func call() async throws {
    if let task {
      do {
        try await task.value
      } catch {
        // A remote DELETE failed; retain the caller's lease authority but allow a later explicit
        // release to issue the retry rather than pinning this at-most-once wrapper forever.
        self.task = nil
        throw error
      }
      return
    }
    let action = action
    let task = Task<Void, Error> { try await action() }
    self.task = task
    do {
      try await task.value
    } catch {
      self.task = nil
      throw error
    }
  }
}

private struct CollectionStoreApplyFailure: Error, Sendable {}

/// Coordinates exact-demand sharing and store-backed lifecycle for one collection definition and
/// principal/generation scope. Coverage proofs intentionally begin with exact identity only.
public actor CollectionCoordinator<
  Model: Sendable,
  Key: Hashable & Sendable,
  Source: CollectionSourceAdapter<Model, Key>,
  Store: CollectionStore<Model, Key>
> {
  private struct Entry {
    var materializationID: CollectionMaterializationID
    let demand: CollectionDemand<Model>
    var state: CollectionLoadState
    var leases: [UUID: AsyncStream<CollectionLoadState>.Continuation]
    var attempt: UUID
    var task: Task<Void, Never>?
    var stop: AtMostOnceStop?
    var refreshToken: UUID?
    var refreshTask: Task<Void, Never>?
    var releaseToken: UUID?
    var releaseTask: Task<Void, Error>?
    var awaitsEviction: Bool
    var snapshotRowCount: Int?
  }

  private let definition: CollectionDefinition<Model, Key>
  private let scope: CollectionScope
  private let source: Source
  private let store: Store
  private var entries: [CollectionDemandIdentity: Entry] = [:]
  private var demandByLease: [UUID: CollectionDemandIdentity] = [:]
  private var evictionTasks: [CollectionDemandIdentity: Task<Void, Error>] = [:]

  private struct StaleObservation {
    let position: CollectionSourceVersion
    let firstSeen: ContinuousClock.Instant
  }

  private var staleGateOpen = false
  private var staleAdmissionOpen = false
  private var stalePassToken: UUID?
  private var stalePassClock: (any ShapeSubscriptionClock)?
  private var staleGateRevision = UUID()
  private var staleDrainToken: UUID?
  private var staleDrainWaiters: [UUID: AsyncStream<Void>.Continuation] = [:]
  private var stalePassStopping = false
  private var stalePassWake: AsyncStream<Void>.Continuation?
  private var staleRuns: [CollectionMaterializationID: Task<Void, Never>] = [:]
  private var staleRetries: [CollectionMaterializationID: Task<Void, Never>] = [:]
  private var staleFailures: [CollectionMaterializationID: Int] = [:]
  private var staleObservations: [CollectionMaterializationID: StaleObservation] = [:]
  // Keep release authority after a refused release without retaining a lease actor that captures
  // this coordinator. These IDs are excluded when deciding whether a screen holds the demand.
  private var staleLeaseIDs: [CollectionMaterializationID: UUID] = [:]

  public init(
    definition: CollectionDefinition<Model, Key>,
    scope: CollectionScope,
    source: Source,
    store: Store
  ) {
    self.definition = definition
    self.scope = scope
    self.source = source
    self.store = store
  }

  /// Runs the stale pass for this collection and scope until the calling task is cancelled.
  /// The app owns that task and cancels and awaits it when retiring a scope or generation.
  /// The gate starts closed; the app opens it after first paint while foreground and online.
  /// At most two unheld demands are re-run concurrently, smallest claim count first.
  ///
  /// Each run emits `sync.revalidate`: `electric.table` (the definition ID),
  /// `sync.subscription_kind`, `sync.outcome`, `sync.rows_returned`,
  /// `sync.claims_released`, `sync.seconds_since_mark`, and `sync.duration_seconds`.
  /// Seconds since the mark measures monotonic elapsed time since this coordinator first
  /// listed that mark in this process, not elapsed time since the store wrote it.
  /// Claims released is the net decrease in the materialization's claims: for `refreshed`,
  /// max(0, claimCount read with the mark before the run - rows the snapshot returned).
  /// A run that both drops and gains rows reports only the net. For `unrebuildable`, it is
  /// the claimCount read with the mark; for `held` and `failed`, it is zero.
  public func startStaleRevalidation(
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    telemetry: TelemetryReporter = .noop
  ) async {
    // One app-owned loop per coordinator, even if a second caller tries to start it.
    guard stalePassToken == nil, !Task.isCancelled else { return }
    let token = UUID()
    stalePassToken = token
    stalePassClock = clock
    stalePassStopping = false
    let wake = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    stalePassWake = wake.continuation
    wake.continuation.yield(())
    await withTaskCancellationHandler {
      var listingFailures = 0
      var idle: Task<Void, Never>?
      for await _ in wake.stream {
        idle?.cancel()
        guard stalePassToken == token, !stalePassStopping, !Task.isCancelled else { break }
        guard staleGateOpen && staleAdmissionOpen else { continue }
        do {
          try await scheduleStaleRuns(token: token, clock: clock, telemetry: telemetry)
          listingFailures = 0
        } catch is CancellationError {
          break
        } catch {
          listingFailures = min(listingFailures + 1, 64)
          // Local notifications cannot bypass a failed listing's bounded backoff.
          let delay = ShapeSubscriptionRetryPolicy(jitterRatio: 0).delay(forRetry: listingFailures)
          do { try await clock.sleep(for: delay) } catch { break }
          wake.continuation.yield(())
          continue
        }
        guard staleGateOpen && staleAdmissionOpen else { continue }
        // External writers have no notification seam. Poll slowly, while local writes,
        // completed runs, retry expiry and opening the gate wake this same serialized loop.
        idle = Task {
          do {
            try await clock.sleep(for: .seconds(5))
            try Task.checkCancellation()
            wake.continuation.yield(())
          } catch {}
        }
      }
      idle?.cancel()
      if let idle { await idle.value }
      cancelStalePass(token)
      let pending = Array(staleRuns.values) + Array(staleRetries.values)
      for task in pending { await task.value }
      // The issuing generation is ending. Make one final best-effort release per retained
      // authority, without keeping retirement alive on a retry timer or clearing its mark.
      for id in Array(staleLeaseIDs.keys) {
        try? await releaseStaleLease(id)
      }
      if stalePassToken == token {
        stalePassToken = nil
        stalePassClock = nil
        stalePassStopping = false
        stalePassWake = nil
      }
    } onCancel: {
      Task { await self.cancelStalePass(token) }
    }
  }

  /// Updates the app's foreground/online/first-paint gate. No runs proceed while it is closed.
  public func setStaleRevalidationGate(isOpen: Bool) async {
    if isOpen {
      if !staleGateOpen || !staleAdmissionOpen {
        staleGateOpen = true
        staleAdmissionOpen = true
        staleDrainToken = nil
        wakeStaleDrains()
        stalePassWake?.yield(())
      }
      return
    }
    let running = closeStaleRevalidationGate()
    for task in running { await task.value }
  }

  /// Pauses background admission and waits for already admitted work to release its leases.
  /// Long snapshots continue, and foreground leases are never released by this operation.
  /// Ordinary request-retry timers are not awaited. A refused lease release retains its
  /// admitted slot and retries cleanup with capped backoff while the lifecycle gate is open.
  /// Lifecycle closure or pass retirement cancels snapshots and cleanup backoff, taking
  /// precedence over drain. Closure retains failed-release authority for the next opening;
  /// retirement makes one final best-effort release. Admission stays closed until an explicit
  /// `setStaleRevalidationGate(isOpen: true)` call.
  public func drainStaleRevalidation() async {
    staleAdmissionOpen = false
    wakeStaleDrains()
    stalePassWake?.yield(())
    let admitted = Array(staleRuns.values)
    for task in admitted { await task.value }
    if let token = stalePassToken, let clock = stalePassClock {
      _ = await drainRetainedStaleLeases(token: token, clock: clock, revision: staleGateRevision)
    }
    // No state changes here: a lifecycle close or explicit reopen during the await wins.
  }

  /// Reopens lifecycle permission with background admission closed, then drains retained leases.
  /// Use this before handing the app's repair budget to another collection or an explicit read
  /// after a foreground/online transition. Cleanup uses retained release authority even when a
  /// snapshot takeover has removed its old materialization from the store's stale listing.
  ///
  /// Returns true only after all admitted work and retained release authority are gone, with
  /// admission still closed. Returns false if lifecycle closure, pass retirement, a concurrent
  /// reopen, or caller cancellation interrupts the handoff; the caller must not admit repair work
  /// then. Caller cancellation closes the lifecycle gate and cancels retry backoff only while
  /// this invocation still owns it; a superseded caller cannot close a newer operation's gate.
  /// With no pass and no retained work, returns true; retained work without a live pass returns
  /// false. This operation never starts a stale read or releases a foreground consumer's lease.
  public func resumeAndDrainStaleRevalidation() async -> Bool {
    guard !Task.isCancelled, !stalePassStopping else { return false }
    let drainToken = UUID()
    staleDrainToken = drainToken
    let revision = UUID()
    staleGateRevision = revision
    staleGateOpen = true
    staleAdmissionOpen = false
    wakeStaleDrains()
    stalePassWake?.yield(())
    defer {
      if staleDrainToken == drainToken { staleDrainToken = nil }
    }
    return await withTaskCancellationHandler {
      guard let token = stalePassToken, let clock = stalePassClock else {
        return staleRuns.isEmpty && staleLeaseIDs.isEmpty && !Task.isCancelled
      }
      let drained = await drainRetainedStaleLeases(token: token, clock: clock, revision: revision)
      // AsyncStream cancellation can win before the cancellation handler reaches this actor.
      if Task.isCancelled { cancelStaleDrain(drainToken) }
      return drained
    } onCancel: {
      Task { await self.cancelStaleDrain(drainToken) }
    }
  }

  private func cancelStaleDrain(_ token: UUID) {
    guard staleDrainToken == token else { return }
    _ = closeStaleRevalidationGate()
  }

  private func drainRetainedStaleLeases(
    token: UUID, clock: any ShapeSubscriptionClock, revision: UUID
  ) async -> Bool {
    let waiter = UUID()
    let wake = AsyncStream<Void>.makeStream(bufferingPolicy: .bufferingNewest(1))
    staleDrainWaiters[waiter] = wake.continuation
    wake.continuation.yield(())
    defer {
      staleDrainWaiters.removeValue(forKey: waiter)
      wake.continuation.finish()
    }
    for await _ in wake.stream {
      guard stalePassMayRun(token), !staleAdmissionOpen, staleGateRevision == revision else {
        return false
      }
      resumeRetainedStaleLeases(token: token, clock: clock)
      if staleRuns.isEmpty { return staleLeaseIDs.isEmpty }
    }
    return false
  }

  private func wakeStaleDrains() {
    for waiter in staleDrainWaiters.values { waiter.yield(()) }
  }

  // Keep cancellation in one actor turn, separate from awaiting release. In particular, do
  // not cancel the shared entry's attempt: final release owns cancellation and restarts an
  // attempt if a foreground lease joins during cleanup.
  func closeStaleRevalidationGate() -> [Task<Void, Never>] {
    staleGateRevision = UUID()
    staleDrainToken = nil
    staleGateOpen = false
    staleAdmissionOpen = false
    wakeStaleDrains()
    stalePassWake?.yield(())
    let running = Array(staleRuns.values)
    for task in running { task.cancel() }
    return running
  }

  private func stalePassMayRun(_ token: UUID) -> Bool {
    stalePassToken == token && !stalePassStopping && staleGateOpen && !Task.isCancelled
  }

  private func cancelStalePass(_ token: UUID) {
    guard stalePassToken == token else { return }
    stalePassStopping = true
    wakeStaleDrains()
    for task in staleRuns.values { task.cancel() }
    for task in staleRetries.values { task.cancel() }
    stalePassWake?.finish()
  }

  private func scheduleStaleRuns(
    token: UUID, clock: any ShapeSubscriptionClock, telemetry: TelemetryReporter
  ) async throws {
    // Retained authority is independent of the store listing: a snapshot can replace its
    // predecessor materialization before its remote lease release is accepted.
    resumeRetainedStaleLeases(token: token, clock: clock)
    // A job can finish while the store is listing. Do not re-admit its old list entry after
    // completion removes it from staleRuns; the next listing will see its conditional clear.
    let occupiedAtRead = Set(staleRuns.keys).union(staleRetries.keys).union(staleLeaseIDs.keys)
    let marks = try await store.staleMaterializations().filter {
      $0.record.demand.collection == definition.id
        && $0.record.demand.scope.principal == scope.principal
        && $0.record.demand.scope.authorization == scope.authorization
    }
    guard stalePassMayRun(token), staleAdmissionOpen else { return }
    let markedIDs = Set(marks.map { $0.record.id })
    staleObservations = staleObservations.filter { markedIDs.contains($0.key) }
    staleFailures = staleFailures.filter { markedIDs.contains($0.key) }
    for mark in marks {
      let id = mark.record.id
      if staleObservations[id]?.position != mark.markedAt {
        staleObservations[id] = .init(position: mark.markedAt, firstSeen: ContinuousClock.now)
      }
    }
    for mark in marks.sorted(by: { $0.claimCount < $1.claimCount }) {
      guard Set(staleRuns.keys).union(staleLeaseIDs.keys).count < 2 else { break }
      let id = mark.record.id
      guard !occupiedAtRead.contains(id), staleRuns[id] == nil, staleRetries[id] == nil,
        evictionTasks[mark.record.demand] == nil, let observation = staleObservations[id]
      else { continue }
      staleRuns[id] = Task {
        await self.revalidate(
          mark, firstSeen: observation.firstSeen, token: token,
          clock: clock, telemetry: telemetry)
      }
    }
  }

  private func resumeRetainedStaleLeases(token: UUID, clock: any ShapeSubscriptionClock) {
    guard stalePassMayRun(token) else { return }
    for id in staleLeaseIDs.keys where staleRuns[id] == nil {
      staleRuns[id] = Task {
        defer {
          self.staleRuns.removeValue(forKey: id)
          self.wakeStaleDrains()
          if self.stalePassToken == token { self.stalePassWake?.yield(()) }
        }
        await self.finishStaleLeaseRelease(id, token: token, clock: clock)
      }
    }
  }

  private func releaseStaleLease(_ id: CollectionMaterializationID) async throws {
    guard let leaseID = staleLeaseIDs[id] else { return }
    try await release(leaseID: leaseID)
    if staleLeaseIDs[id] == leaseID { staleLeaseIDs.removeValue(forKey: id) }
  }

  private func finishStaleLeaseRelease(
    _ id: CollectionMaterializationID, token: UUID, clock: any ShapeSubscriptionClock,
    retrying: Bool = false
  ) async {
    guard staleLeaseIDs[id] != nil else { return }
    // A cancelled snapshot still owes its first release attempt. A release already refused
    // goes straight to backoff; closing the lifecycle gate must not start another retry.
    if !retrying { try? await releaseStaleLease(id) }
    var failures = 1
    // This sleep belongs to the admitted run, so both gate-close cancellation and pass
    // retirement wake it immediately. Drain only pauses admission and leaves it running.
    while staleLeaseIDs[id] != nil && stalePassMayRun(token) {
      let delay = ShapeSubscriptionRetryPolicy(jitterRatio: 0).delay(forRetry: failures)
      do {
        try await clock.sleep(for: delay)
      } catch {
        return
      }
      // Recheck after suspension, including the gate, pass token/stopping fence and this
      // run's cancellation. A quick reopen cannot revive an already cancelled cleanup.
      guard stalePassMayRun(token) else { return }
      do {
        try await releaseStaleLease(id)
      } catch {
        failures = min(failures + 1, 64)
      }
    }
  }

  private struct StaleSnapshotFailure: Error {}

  private func staleSnapshotRowCount(
    for lease: CollectionLease, identity: CollectionDemandIdentity
  ) async throws -> Int {
    for await state in lease.stateUpdates {
      try Task.checkCancellation()
      switch state {
      case .live, .cached:
        // Initial cached readiness is not the new snapshot. The count is set only after
        // replaceSnapshot commits, and before that attempt publishes live readiness.
        if let count = entries[identity]?.snapshotRowCount { return count }
      case .failed:
        throw StaleSnapshotFailure()
      case .unavailable, .refreshing:
        continue
      }
    }
    try Task.checkCancellation()
    throw StaleSnapshotFailure()
  }

  private func revalidate(
    _ mark: CollectionStaleMaterialization, firstSeen: ContinuousClock.Instant,
    token: UUID, clock: any ShapeSubscriptionClock, telemetry: TelemetryReporter
  ) async {
    let id = mark.record.id
    defer {
      staleRuns.removeValue(forKey: id)
      wakeStaleDrains()
      if stalePassToken == token { stalePassWake?.yield(()) }
    }
    guard stalePassMayRun(token) else { return }
    let started = ContinuousClock.now
    let span = telemetry.beginSpan(name: "sync.revalidate", kind: .internalSpan)
    var outcome = "failed"
    var rowsReturned = 0
    var claimsReleased = 0
    var attemptedRelease = false
    defer {
      var attributes = [
        "electric.table": definition.id.rawValue,
        "sync.outcome": outcome,
        "sync.rows_returned": String(rowsReturned),
        "sync.claims_released": String(claimsReleased),
        "sync.seconds_since_mark": String(staleSeconds(firstSeen.duration(to: started))),
        "sync.duration_seconds": String(staleSeconds(started.duration(to: ContinuousClock.now))),
      ]
      if let kind = definition.subscriptionKindForDemand?(mark.record.demand)
        ?? definition.subscriptionKind
      {
        attributes["sync.subscription_kind"] = kind
      }
      telemetry.endSpan(span, attributes: attributes)
    }
    do {
      // Retry a refused release with the same authority before attempting a new acquisition.
      if staleLeaseIDs[id] != nil {
        attemptedRelease = true
        try await releaseStaleLease(id)
        attemptedRelease = false
      }
      guard stalePassMayRun(token) else { throw CancellationError() }
      let storedIdentity = mark.record.demand
      let activeIdentity = CollectionDemandIdentity(
        collection: storedIdentity.collection, scope: scope,
        canonicalDemand: storedIdentity.canonicalDemand)
      if let entry = entries[activeIdentity], !entry.leases.isEmpty, entry.state == .live {
        if storedIdentity != activeIdentity {
          // The current live copy owns its own claims; an earlier launch's claims must
          // actually leave the store, not merely lose their stale flag.
          try await removeStaleMaterialization(storedIdentity)
        } else {
          try await store.clearStale(id, ifMarkedAt: mark.markedAt)
        }
        outcome = "held"
      } else if let demand = definition.rebuildDemand?(storedIdentity),
        demand.identity(for: definition, scope: scope) == activeIdentity
      {
        let lease = acquire(demand)
        staleLeaseIDs[id] = lease.id
        rowsReturned = try await staleSnapshotRowCount(for: lease, identity: activeIdentity)
        guard stalePassMayRun(token) else { throw CancellationError() }
        attemptedRelease = true
        try await lease.release()
        staleLeaseIDs.removeValue(forKey: id)
        guard stalePassMayRun(token) else { throw CancellationError() }
        if storedIdentity != activeIdentity {
          // Retire the superseded generation only after the current snapshot and release
          // succeed. Its replacement retains its own marks, including concurrent drops.
          try await removeStaleMaterialization(storedIdentity)
        } else {
          try await store.clearStale(id, ifMarkedAt: mark.markedAt)
        }
        outcome = "refreshed"
        claimsReleased = max(0, mark.claimCount - rowsReturned)
      } else {
        try await removeStaleMaterialization(storedIdentity)
        outcome = "unrebuildable"
        claimsReleased = mark.claimCount
      }
      staleFailures.removeValue(forKey: id)
    } catch {
      // Drain retains this run's slot through refused-release retries, but lifecycle closure
      // or retirement ends the retry loop without losing authority in a still-live generation.
      await finishStaleLeaseRelease(id, token: token, clock: clock, retrying: attemptedRelease)
      guard stalePassMayRun(token) else { return }
      let failures = min((staleFailures[id] ?? 0) + 1, 64)
      staleFailures[id] = failures
      let delay = ShapeSubscriptionRetryPolicy(jitterRatio: 0).delay(forRetry: failures)
      staleRetries[id] = Task {
        defer {
          self.staleRetries.removeValue(forKey: id)
          if self.stalePassToken == token { self.stalePassWake?.yield(()) }
        }
        // Backoff occupies no request slot. Failures have no attempt limit; marks survive
        // process restart, while the process-local backoff starts afresh after restart.
        try? await clock.sleep(for: delay)
      }
    }
  }

  private func removeStaleMaterialization(_ identity: CollectionDemandIdentity) async throws {
    // Fence removal using the stored identity, including its original generation. Acquiring
    // the replacement must never cause eviction to target that replacement's materialization.
    let removal: Task<Void, Error>
    if let existing = evictionTasks[identity] {
      removal = existing
    } else {
      removal = Task { try await self.performEviction(identity) }
      evictionTasks[identity] = removal
    }
    try await removal.value
  }

  private func staleSeconds(_ duration: Duration) -> Double {
    let components = duration.components
    return max(0, Double(components.seconds) + Double(components.attoseconds) / 1e18)
  }

  public func acquire(_ demand: CollectionDemand<Model>) -> CollectionLease {
    let identity = demand.identity(for: definition, scope: scope)
    let leaseID = UUID()
    let updates = AsyncStream<CollectionLoadState>.makeStream(
      bufferingPolicy: .bufferingNewest(16))

    if var entry = entries[identity] {
      entry.leases[leaseID] = updates.continuation
      let shouldRetry = {
        guard case .failed = entry.state else { return false }
        return entry.task == nil && entry.stop == nil && entry.releaseToken == nil
          && !entry.awaitsEviction
      }()
      if shouldRetry {
        entry.attempt = UUID()
        entry.state = .unavailable
      }
      entries[identity] = entry
      for continuation in entry.leases.values { continuation.yield(entry.state) }
      if shouldRetry {
        startAttempt(for: identity, attempt: entry.attempt)
      }
    } else {
      let attempt = UUID()
      let materializationID = CollectionMaterializationID(rawValue: UUID().uuidString.lowercased())
      entries[identity] = Entry(
        materializationID: materializationID,
        demand: demand,
        state: .unavailable,
        leases: [leaseID: updates.continuation],
        attempt: attempt,
        task: nil,
        stop: nil,
        refreshToken: nil,
        refreshTask: nil,
        releaseToken: nil,
        releaseTask: nil,
        awaitsEviction: evictionTasks[identity] != nil,
        snapshotRowCount: nil
      )
      updates.continuation.yield(.unavailable)
      if evictionTasks[identity] == nil {
        startAttempt(for: identity, attempt: attempt)
      }
    }
    demandByLease[leaseID] = identity

    return CollectionLease(
      id: leaseID,
      stateUpdates: updates.stream,
      stateAction: { await self.state(for: $0) },
      refreshAction: { await self.refresh(leaseID: $0) },
      releaseAction: { try await self.release(leaseID: $0) }
    )
  }

  deinit {
    for entry in entries.values {
      entry.task?.cancel()
      for continuation in entry.leases.values {
        continuation.finish()
      }
    }
  }

  private func state(for leaseID: UUID) -> CollectionLoadState {
    guard let identity = demandByLease[leaseID], let entry = entries[identity] else {
      return .unavailable
    }
    return entry.state
  }

  private func startAttempt(for identity: CollectionDemandIdentity, attempt: UUID) {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty,
      entry.releaseToken == nil, !entry.awaitsEviction
    else { return }
    let task = Task { [weak self] in
      guard let self else { return }
      await self.run(identity: identity, attempt: attempt)
    }
    entry.task = task
    entry.snapshotRowCount = nil
    entries[identity] = entry
  }

  private func run(identity: CollectionDemandIdentity, attempt: UUID) async {
    let cached: CollectionMaterializationRecord?
    do {
      cached = try await store.materialization(for: identity)
    } catch is CancellationError {
      return
    } catch {
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return
    }

    if let cached {
      guard updateEntry(identity: identity, attempt: attempt, state: .cached) else { return }
      guard var entry = entries[identity], entry.attempt == attempt else { return }
      entry.materializationID = cached.id
      entries[identity] = entry
    }
    guard updateEntry(identity: identity, attempt: attempt, state: .refreshing),
      let entry = entries[identity]
    else { return }

    let session: CollectionSourceSession<Model, Key>
    do {
      session = try await source.materialize(
        entry.demand,
        identity: identity,
        materializationID: entry.materializationID
      )
    } catch is CancellationError {
      return
    } catch {
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return
    }

    let stop = AtMostOnceStop(session.stop)
    guard install(stop: stop, identity: identity, attempt: attempt) else {
      do {
        try await stop.call()
      } catch {
        fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      }
      return
    }
    do {
      try Task.checkCancellation()
      guard let current = entries[identity], current.attempt == attempt else {
        try await stop.call()
        return
      }
      try await store.replaceSnapshot(
        session.snapshot,
        materializationID: current.materializationID,
        demand: identity
      )
    } catch is CancellationError {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return
    } catch {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return
    }

    guard var snapshotted = entries[identity], snapshotted.attempt == attempt,
      !Task.isCancelled
    else {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return
    }
    stalePassWake?.yield(())
    snapshotted.snapshotRowCount = session.snapshot.rows.count
    entries[identity] = snapshotted
    guard updateEntry(identity: identity, attempt: attempt, state: .live) else {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return
    }

    do {
      try await session.run { batch in
        try Task.checkCancellation()
        try await self.apply(batch, identity: identity, attempt: attempt)
      }
    } catch is CancellationError {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return
    } catch is CollectionStoreApplyFailure {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return
    } catch {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return
    }

    if await cleanup(stop, identity: identity, attempt: attempt) {
      finishStream(identity: identity, attempt: attempt)
    }
  }

  private func apply(
    _ batch: CollectionChangeBatch<Model, Key>,
    identity: CollectionDemandIdentity,
    attempt: UUID
  ) async throws {
    guard let current = entries[identity], current.attempt == attempt, !current.leases.isEmpty,
      current.releaseToken == nil
    else { throw CancellationError() }
    do {
      try await store.apply(batch, to: current.materializationID)
      stalePassWake?.yield(())
    } catch is CancellationError {
      throw CancellationError()
    } catch {
      throw CollectionStoreApplyFailure()
    }
  }

  private func cleanup(
    _ stop: AtMostOnceStop, identity: CollectionDemandIdentity, attempt: UUID
  ) async -> Bool {
    do {
      try await stop.call()
      clearInstalledStop(stop, identity: identity, attempt: attempt)
      return true
    } catch {
      // Keep the failed stop installed so a lease release can retry the same server authority.
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return false
    }
  }

  private func clearInstalledStop(
    _ stop: AtMostOnceStop, identity: CollectionDemandIdentity, attempt: UUID
  ) {
    guard var entry = entries[identity], entry.attempt == attempt, entry.stop === stop else {
      return
    }
    entry.stop = nil
    entries[identity] = entry
  }

  private func install(
    stop: AtMostOnceStop,
    identity: CollectionDemandIdentity,
    attempt: UUID
  ) -> Bool {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return false
    }
    entry.stop = stop
    entries[identity] = entry
    return true
  }

  @discardableResult
  private func updateEntry(
    identity: CollectionDemandIdentity,
    attempt: UUID,
    state: CollectionLoadState
  ) -> Bool {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return false
    }
    entry.state = state
    entries[identity] = entry
    for continuation in entry.leases.values {
      continuation.yield(state)
    }
    return true
  }

  private func fail(
    identity: CollectionDemandIdentity,
    attempt: UUID,
    with failure: CollectionLoadFailure,
    retainTask: Bool = false
  ) {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return
    }
    if !retainTask { entry.task = nil }
    // A failed cleanup retains the exact remote-release authority for an explicit lease retry.
    entry.state = .failed(failure)
    entries[identity] = entry
    for continuation in entry.leases.values {
      continuation.yield(.failed(failure))
    }
  }

  private func finishStream(identity: CollectionDemandIdentity, attempt: UUID) {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return
    }
    entry.task = nil
    entry.stop = nil
    entry.state = .cached
    entries[identity] = entry
    for continuation in entry.leases.values {
      continuation.yield(.cached)
    }
  }

  private func refresh(leaseID: UUID) async {
    guard let identity = demandByLease[leaseID], var entry = entries[identity],
      entry.releaseToken == nil
    else { return }
    if let task = entry.refreshTask {
      await task.value
      return
    }
    let token = UUID()
    let task = Task { [weak self] in
      guard let self else { return }
      await self.performRefresh(identity: identity, token: token)
    }
    entry.refreshToken = token
    entry.refreshTask = task
    entry.state = .refreshing
    entries[identity] = entry
    for continuation in entry.leases.values { continuation.yield(.refreshing) }
    await task.value
  }

  private func release(leaseID: UUID) async throws {
    guard let identity = demandByLease[leaseID], var entry = entries[identity]
    else { return }
    guard entry.leases.count == 1 else {
      demandByLease.removeValue(forKey: leaseID)
      entry.leases.removeValue(forKey: leaseID)?.finish()
      entries[identity] = entry
      return
    }
    if let task = entry.releaseTask {
      try await task.value
      return
    }
    let token = UUID()
    let task = Task<Void, Error> { [weak self] in
      guard let self else { return }
      try await self.performFinalRelease(identity: identity, leaseID: leaseID, token: token)
    }
    entry.releaseToken = token
    entry.releaseTask = task
    entries[identity] = entry
    try await task.value
  }

  private func performRefresh(identity: CollectionDemandIdentity, token: UUID) async {
    guard let initial = entries[identity], initial.refreshToken == token,
      initial.releaseToken == nil, !initial.leases.isEmpty
    else {
      clearRefresh(identity: identity, token: token)
      return
    }
    let oldAttempt = initial.attempt
    let oldTask = initial.task
    let oldStop = initial.stop
    do {
      try await oldStop?.call()
      guard let afterStop = entries[identity], afterStop.refreshToken == token,
        afterStop.releaseToken == nil, afterStop.attempt == oldAttempt, !afterStop.leases.isEmpty
      else {
        clearRefresh(identity: identity, token: token)
        return
      }
      try await source.cleanupAbandonedMaterialization(afterStop.materializationID)
    } catch {
      fail(identity: identity, attempt: oldAttempt, with: .sourceUnavailable, retainTask: true)
      clearRefresh(identity: identity, token: token)
      return
    }
    guard let afterCleanup = entries[identity], afterCleanup.refreshToken == token,
      afterCleanup.releaseToken == nil, afterCleanup.attempt == oldAttempt,
      !afterCleanup.leases.isEmpty
    else {
      clearRefresh(identity: identity, token: token)
      return
    }
    oldTask?.cancel()
    if let oldTask { await oldTask.value }
    guard let current = entries[identity], current.refreshToken == token,
      current.releaseToken == nil, current.attempt == oldAttempt, !current.leases.isEmpty
    else {
      clearRefresh(identity: identity, token: token)
      return
    }
    guard var entry = entries[identity], entry.refreshToken == token,
      entry.releaseToken == nil, entry.attempt == oldAttempt, !entry.leases.isEmpty
    else {
      clearRefresh(identity: identity, token: token)
      return
    }
    let nextAttempt = UUID()
    entry.attempt = nextAttempt
    entry.task = nil
    entry.stop = nil
    entry.refreshToken = nil
    entry.refreshTask = nil
    entry.state = .unavailable
    entries[identity] = entry
    for continuation in entry.leases.values { continuation.yield(.unavailable) }
    startAttempt(for: identity, attempt: nextAttempt)
  }

  private func clearRefresh(identity: CollectionDemandIdentity, token: UUID) {
    guard var entry = entries[identity], entry.refreshToken == token else { return }
    entry.refreshToken = nil
    entry.refreshTask = nil
    entries[identity] = entry
  }

  private func performFinalRelease(
    identity: CollectionDemandIdentity,
    leaseID: UUID,
    token: UUID
  ) async throws {
    guard let initial = entries[identity], initial.releaseToken == token,
      initial.leases[leaseID] != nil
    else { return }
    if let refresh = initial.refreshTask { await refresh.value }
    guard let afterRefresh = entries[identity], afterRefresh.releaseToken == token,
      afterRefresh.leases[leaseID] != nil
    else { return }
    do {
      try await afterRefresh.stop?.call()
    } catch {
      failRelease(identity: identity, attempt: afterRefresh.attempt, token: token)
      throw error
    }
    guard let afterStop = entries[identity], afterStop.releaseToken == token,
      afterStop.leases[leaseID] != nil
    else { return }
    afterStop.task?.cancel()
    if let task = afterStop.task { await task.value }
    guard let current = entries[identity], current.releaseToken == token,
      current.leases[leaseID] != nil
    else { return }
    do {
      guard let afterCleanup = entries[identity], afterCleanup.releaseToken == token,
        afterCleanup.leases[leaseID] != nil
      else { return }
      try await source.cleanupAbandonedMaterialization(afterCleanup.materializationID)
    } catch {
      failRelease(identity: identity, attempt: current.attempt, token: token)
      throw error
    }
    guard var entry = entries[identity], entry.releaseToken == token,
      entry.leases.removeValue(forKey: leaseID) != nil
    else { return }
    demandByLease.removeValue(forKey: leaseID)
    initial.leases[leaseID]?.finish()
    if entry.leases.isEmpty {
      entries.removeValue(forKey: identity)
      return
    }
    let nextAttempt = UUID()
    entry.attempt = nextAttempt
    entry.task = nil
    entry.stop = nil
    entry.releaseToken = nil
    entry.releaseTask = nil
    entry.state = .unavailable
    entries[identity] = entry
    for continuation in entry.leases.values { continuation.yield(.unavailable) }
    startAttempt(for: identity, attempt: nextAttempt)
  }

  private func failRelease(identity: CollectionDemandIdentity, attempt: UUID, token: UUID) {
    guard var entry = entries[identity], entry.releaseToken == token, entry.attempt == attempt,
      !entry.leases.isEmpty
    else { return }
    entry.releaseToken = nil
    entry.releaseTask = nil
    entry.state = .failed(.sourceUnavailable)
    entries[identity] = entry
    for continuation in entry.leases.values { continuation.yield(.failed(.sourceUnavailable)) }
  }

  /// Removes an inactive cached materialization. Acquires that arrive while the store operation is
  /// in flight are fenced behind it and receive a fresh source attempt after it completes.
  public func evict(_ demand: CollectionDemand<Model>) async throws {
    let identity = demand.identity(for: definition, scope: scope)
    if let task = evictionTasks[identity] {
      try await task.value
      return
    }
    guard entries[identity] == nil else { throw CollectionEvictionError.activeDemand }
    let task = Task<Void, Error> { [weak self] in
      guard let self else { return }
      try await self.performEviction(identity)
    }
    evictionTasks[identity] = task
    try await task.value
  }

  private func performEviction(_ identity: CollectionDemandIdentity) async throws {
    do {
      if let record = try await store.materialization(for: identity) {
        try await store.removeMaterialization(record.id)
      }
      finishEviction(identity)
    } catch {
      finishEviction(identity)
      throw error
    }
  }

  private func finishEviction(_ identity: CollectionDemandIdentity) {
    evictionTasks.removeValue(forKey: identity)
    guard var entry = entries[identity], entry.awaitsEviction, !entry.leases.isEmpty else { return }
    entry.awaitsEviction = false
    entries[identity] = entry
    startAttempt(for: identity, attempt: entry.attempt)
  }
}
