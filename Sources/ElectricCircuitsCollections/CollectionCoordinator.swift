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

/// What an attempt may start from when the store already holds a copy of its demand.
///
/// The caller owns the policy. `.resume` fits wherever continuing from the stored cursor is good
/// enough: a first load in a launch, a retry after a failure. `.snapshot` fits wherever the
/// stored rows themselves are in doubt (the scope was marked stale, its authority was read
/// again, another holder handed it over), because a resumed tail applies only what the feed
/// carries after the stored cursor and never reads the source again.
public enum CollectionLoadMode: Equatable, Sendable {
  /// Continue the stored copy from its cursor when the source can
  /// (`CollectionSourceAdapter.resume`), and take a snapshot when it cannot. What
  /// `CollectionCoordinator.acquire(_:)` and `CollectionLease.refresh(.resume)` do.
  case resume
  /// Take a new snapshot and replace the stored copy's rows with it, without asking the source
  /// to resume, as a plain `CollectionLease.refresh()` does. The demand owes that snapshot until
  /// one commits: an attempt that fails first is followed by another snapshot, whatever mode
  /// asks for it. The debt ends with the demand's last lease.
  case snapshot
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
  private let refreshAction: @Sendable (UUID, CollectionLoadMode) async -> Void
  private let releaseAction: @Sendable (UUID) async throws -> Void
  private var released = false

  init(
    id: UUID,
    stateUpdates: AsyncStream<CollectionLoadState>,
    stateAction: @escaping @Sendable (UUID) async -> CollectionLoadState,
    refreshAction: @escaping @Sendable (UUID, CollectionLoadMode) async -> Void,
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

  /// Always re-reads the source by replacing the demand's current attempt with a new snapshot,
  /// as `refresh(.snapshot)` does, for every lease that shares it. To continue from the stored
  /// cursor instead, call `refresh(.resume)`.
  public func refresh() async {
    await refresh(.snapshot)
  }

  /// Replaces the demand's current attempt with a new one, for every lease that shares it.
  ///
  /// With `.resume` the new attempt continues the stored copy from its cursor when the source
  /// can and snapshots otherwise: the way to retry after a failure. With `.snapshot` it takes a
  /// new snapshot, which replaces the stored rows: the way to read the source again when the
  /// stored rows are in doubt, and the default for plain `refresh()`. A snapshot the demand
  /// still owes (`CollectionLoadMode.snapshot`) is taken in either mode.
  public func refresh(_ mode: CollectionLoadMode) async {
    guard !released else { return }
    await refreshAction(id, mode)
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
/// Leases are keyed by `storageKey`, which includes the generation; stored copies are found by
/// `cacheKey`, so a later generation's coordinator finds and refreshes an earlier one's copy.
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
    /// The attempt must take the snapshot path even when the store holds a copy it could
    /// resume. Set by a lease that asks for a snapshot (`CollectionLoadMode.snapshot`), which is
    /// also how the stale pass revalidates; cleared once a snapshot commits.
    var requiresSnapshot = false
    /// A snapshot is owed by the attempt that starts next, not by the one in place: a
    /// `refresh(.snapshot)`, which replaces the attempt in place, or a lease that asked for a
    /// snapshot while that attempt was already on its way out. The outgoing attempt may still
    /// commit a snapshot it read before the request, and that commit clears `requiresSnapshot`;
    /// so the request waits here, and `startAttempt` turns it into `requiresSnapshot` for the
    /// attempt it starts.
    var nextAttemptRequiresSnapshot = false
    /// The attempt currently continuing a stored cursor through `source.resume`, if any.
    var resumingAttempt: UUID?
  }

  private let definition: CollectionDefinition<Model, Key>
  private let scope: CollectionScope
  private let source: Source
  private let store: Store
  private var entries: [CollectionDemandIdentity: Entry] = [:]
  private var demandByLease: [UUID: CollectionDemandIdentity] = [:]
  /// Keyed by `cacheKey`: a removal fences every generation's demand for the same stored copy.
  private var evictionTasks: [String: Task<Void, Error>] = [:]

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

  /// Precondition: before a coordinator for a newer generation of the same principal and
  /// authorization is created, every coordinator of the retiring generation has released its
  /// leases and its stale pass has been cancelled and awaited. Generations share stored copies by
  /// `cacheKey` and cannot be ordered, and a copy has one cursor: two generations live on one copy
  /// at once would fail the older lease on its next batch, and a retiring generation's stale pass
  /// could re-read, rebind or remove the copy the newer generation is live on. This precondition
  /// rules both out. The coordinator does not enforce it; the app does.
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
  /// Precondition (see `init`): a retiring generation releases its leases and cancels and awaits
  /// this pass before a newer generation's coordinator is created. Generations of one principal
  /// and authorization share stored copies by `cacheKey` and cannot be ordered, so a retiring
  /// generation's pass could otherwise re-read, rebind or remove a copy a newer generation is live
  /// on, and two generations live on one copy at once would fail the older lease on its next batch.
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
  /// the claimCount read with the mark; for `held`, zero; for `failed`, zero. When a `held` or
  /// `refreshed` run finds the marked copy is not the copy it held or re-read (a second copy
  /// under the key) and removes it, it is the claimCount read with the mark.
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
      openStaleRevalidationGate()
      return
    }
    let running = closeStaleRevalidationGate()
    for task in running { await task.value }
  }

  func openStaleRevalidationGate() {
    if !staleGateOpen || !staleAdmissionOpen {
      staleGateOpen = true
      staleAdmissionOpen = true
      staleDrainToken = nil
      wakeStaleDrains()
      stalePassWake?.yield(())
    }
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
    if let token = stalePassToken {
      _ = await drainRetainedStaleLeases(token: token, revision: staleGateRevision)
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
      guard let token = stalePassToken else {
        return staleRuns.isEmpty && staleLeaseIDs.isEmpty && !Task.isCancelled
      }
      let drained = await drainRetainedStaleLeases(token: token, revision: revision)
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

  private func drainRetainedStaleLeases(token: UUID, revision: UUID) async -> Bool {
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
      resumeRetainedStaleLeases()
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
    resumeRetainedStaleLeases()
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
        evictionTasks[mark.record.demand.cacheKey] == nil,
        let observation = staleObservations[id]
      else { continue }
      staleRuns[id] = Task {
        await self.revalidate(
          mark, firstSeen: observation.firstSeen, token: token,
          clock: clock, telemetry: telemetry)
      }
    }
  }

  func resumeRetainedStaleLeases() {
    guard let token = stalePassToken, let clock = stalePassClock, stalePassMayRun(token) else {
      return
    }
    for id in staleLeaseIDs.keys where staleRuns[id] == nil {
      staleRuns[id] = Task {
        defer {
          self.staleRuns.removeValue(forKey: id)
          self.wakeStaleDrains()
          if self.stalePassToken == token { self.stalePassWake?.yield(()) }
        }
        guard self.stalePassMayRun(token) else { return }
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
      // The copy may have been stored by an earlier generation. This generation's demand for it
      // has the same `cacheKey`, so it finds the same copy and its snapshot replaces that copy's
      // claims in place; the pass then clears the mark and never removes the copy it refreshed.
      // A mark clears only when the copy held or re-read is the marked copy. A second copy under
      // the same key, which a provider should never keep, is removed instead.
      let storedIdentity = mark.record.demand
      let activeIdentity = CollectionDemandIdentity(
        collection: storedIdentity.collection, scope: scope,
        canonicalDemand: storedIdentity.canonicalDemand, columns: source.columns)
      // A held live copy maintains its own claims from the feed, once its snapshot established
      // them. A resumed tail does not count: it never re-read the source, which is what the mark
      // asks for, so it revalidates through a snapshot like an unheld copy, below.
      let leased = entries[activeIdentity].flatMap { entry in
        !entry.leases.isEmpty && activeIdentity.cacheKey == storedIdentity.cacheKey ? entry : nil
      }
      let held = leased.flatMap { $0.state == .live ? $0 : nil }
      // A resumed attempt tails this copy without a snapshot, live or not yet: the pass joins it
      // and restarts it as a snapshot rather than removing the copy under its tail.
      let resuming = leased.flatMap { $0.resumingAttempt == $0.attempt ? $0 : nil }
      if let entry = held, entry.resumingAttempt != entry.attempt {
        if entry.materializationID == id {
          try await store.clearStale(id, ifMarkedAt: mark.markedAt)
        } else {
          try await removeStaleMaterialization(storedIdentity, copy: id)
          claimsReleased = mark.claimCount
        }
        outcome = "held"
      } else if let demand = resuming?.demand
        ?? rebuiltDemand(storedIdentity, active: activeIdentity),
        activeIdentity.cacheKey == storedIdentity.cacheKey
      {
        // Revalidation is a fresh snapshot: a resumed tail neither reports a row count nor
        // proves, once this lease releases it, that it reached the drop position. So the pass
        // asks as any caller does, and a resumed attempt its lease joins is replaced.
        let lease = acquire(demand, mode: .snapshot)
        staleLeaseIDs[id] = lease.id
        await entries[activeIdentity]?.refreshTask?.value
        rowsReturned = try await staleSnapshotRowCount(for: lease, identity: activeIdentity)
        let refreshedID = entries[activeIdentity]?.materializationID
        guard stalePassMayRun(token) else { throw CancellationError() }
        attemptedRelease = true
        try await lease.release()
        staleLeaseIDs.removeValue(forKey: id)
        guard stalePassMayRun(token) else { throw CancellationError() }
        if refreshedID == id {
          try await store.clearStale(id, ifMarkedAt: mark.markedAt)
          claimsReleased = max(0, mark.claimCount - rowsReturned)
        } else {
          try await removeStaleMaterialization(storedIdentity, copy: id)
          claimsReleased = mark.claimCount
        }
        outcome = "refreshed"
      } else {
        try await removeStaleMaterialization(storedIdentity, copy: id)
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

  /// The demand a stale mark's stored identity rebuilds to, when it is the active identity.
  private func rebuiltDemand(
    _ stored: CollectionDemandIdentity, active: CollectionDemandIdentity
  ) -> CollectionDemand<Model>? {
    guard let demand = definition.rebuildDemand?(stored),
      demand.identity(for: definition, scope: scope, columns: source.columns) == active
    else { return nil }
    return demand
  }

  private func removeStaleMaterialization(
    _ identity: CollectionDemandIdentity, copy: CollectionMaterializationID
  ) async throws {
    // Fence removal by the stored copy's key, so a demand acquired meanwhile waits for it and
    // then loads afresh rather than binding the copy being removed.
    let removal: Task<Void, Error>
    if let existing = evictionTasks[identity.cacheKey] {
      removal = existing
    } else {
      removal = Task { try await self.performEviction(identity, copy: copy) }
      evictionTasks[identity.cacheKey] = removal
    }
    try await removal.value
  }

  private func staleSeconds(_ duration: Duration) -> Double {
    let components = duration.components
    return max(0, Double(components.seconds) + Double(components.attoseconds) / 1e18)
  }

  /// Takes a lease on `demand`, as `acquire(_:mode:)` does with `.resume`.
  public func acquire(_ demand: CollectionDemand<Model>) -> CollectionLease {
    acquire(demand, mode: .resume)
  }

  /// Takes a lease on `demand`, sharing the attempt of every other lease on the same demand.
  ///
  /// With `.resume`, a demand the store already holds continues from its stored cursor when the
  /// source can, and snapshots otherwise. With `.snapshot` it is loaded by a new snapshot, which
  /// replaces the stored rows, as every acquire was before a source could resume. A lease that
  /// joins a demand another lease already holds shares that demand's attempt: `.snapshot`
  /// replaces an attempt that resumed, and leaves one that took its own snapshot, however long
  /// ago, as it is. To read the source again regardless, call `refresh()` on the lease.
  public func acquire(_ demand: CollectionDemand<Model>, mode: CollectionLoadMode)
    -> CollectionLease
  {
    let lease = acquire(demand, requiringSnapshot: mode == .snapshot)
    if mode == .snapshot, let identity = demandByLease[lease.id], let joined = entries[identity],
      joined.resumingAttempt == joined.attempt
    {
      // The attempt this lease joined chose to resume before the lease asked for a snapshot,
      // so it will never take one. Replace it with one that does.
      _ = beginRefresh(identity, requiringSnapshot: true)
    }
    return lease
  }

  private func acquire(
    _ demand: CollectionDemand<Model>, requiringSnapshot: Bool
  ) -> CollectionLease {
    let identity = demand.identity(for: definition, scope: scope, columns: source.columns)
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
      if requiringSnapshot {
        if entry.releaseToken != nil || entry.refreshTask != nil {
          // The attempt in place is on its way out; the one that follows serves this lease.
          entry.nextAttemptRequiresSnapshot = true
        } else if shouldRetry || entry.snapshotRowCount == nil {
          // The attempt this lease joins settles the request once it has committed a snapshot
          // of its own; only an attempt still to start, or still to commit, owes one.
          entry.requiresSnapshot = true
        }
      }
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
        awaitsEviction: evictionTasks[identity.cacheKey] != nil,
        snapshotRowCount: nil,
        requiresSnapshot: requiringSnapshot
      )
      updates.continuation.yield(.unavailable)
      if evictionTasks[identity.cacheKey] == nil {
        startAttempt(for: identity, attempt: attempt)
      }
    }
    demandByLease[leaseID] = identity

    return CollectionLease(
      id: leaseID,
      stateUpdates: updates.stream,
      stateAction: { await self.state(for: $0) },
      refreshAction: { await self.refresh(leaseID: $0, mode: $1) },
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
    entry.resumingAttempt = nil
    if entry.nextAttemptRequiresSnapshot {
      entry.requiresSnapshot = true
      entry.nextAttemptRequiresSnapshot = false
    }
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
      // The copy under this `cacheKey` resumes whichever generation stored it. `evict(_:)` and
      // the stale pass key on the copy's binding, so a copy an earlier generation stored is
      // bound to this demand first: by the store (`rebindMaterialization`, in `runResumed`)
      // when it resumes, or by `replaceSnapshot` when it snapshots.
      let resumes = !entry.requiresSnapshot && cached.demand.cacheKey == identity.cacheKey
      if resumes { entry.resumingAttempt = attempt }
      entries[identity] = entry
      if resumes, await runResumed(from: cached, identity: identity, attempt: attempt) { return }
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
    snapshotted.requiresSnapshot = false
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

  /// Continues the stored copy for this demand from its committed cursor, whichever generation
  /// stored it. Returns false when the store cannot bind an earlier generation's copy without a
  /// snapshot, the source declines, the stored record moved while it was asked, or the feed
  /// refuses the stored cursor (`CollectionResumeInvalidated`) before the tail went live, and the
  /// attempt must take the snapshot path; true when this attempt has finished, failed or been
  /// superseded. A transient failure fails the attempt and leaves the cursor resumable by the
  /// next one.
  ///
  /// The snapshot path's per-acquire invariants, and how a resumed attempt keeps them:
  /// - Binding: a copy an earlier generation stored is bound to this demand by
  ///   `rebindMaterialization` before the tail runs, as `replaceSnapshot` would have bound it, so
  ///   `evict(_:)` and the stale pass treat it as this generation's. The precondition on `init`
  ///   (the earlier generation has released its leases) is what makes that safe.
  /// - Rows and claims: there is no `replaceSnapshot`, so no omission pass marks other holders
  ///   and no provider claim takeover runs. Neither is owed: the stored claims are exactly what
  ///   this materialization's snapshot and feed established up to the stored cursor (any takeover
  ///   happened at that snapshot), and the tail is gap-free from it, so each feed delete still
  ///   releases its claim and marks other holders through the store's ordinary awaited `apply`.
  ///   Stored marks are untouched. Retiring an earlier generation's stale copy stays with the
  ///   stale pass, which always snapshots before it removes that copy.
  /// - Cursor CAS: the session must start at the cursor the store still holds. The record is read
  ///   again after the source call, and the first batch expects that cursor, so the store's
  ///   compare-and-set rejects an interleaved writer instead of the tail skipping or replaying.
  /// - `snapshotRowCount` stays nil because no snapshot was taken. The stale pass, which needs
  ///   one, acquires in snapshot mode, which replaces a resumed attempt it joins.
  /// - Readiness: the demand stays `.cached` (its rows are already local) and reports `.live` only
  ///   when the session says the tail has read to the feed's head (`caughtUp`). A batch the tail
  ///   commits on the way there changes the store, not the state.
  private func runResumed(
    from record: CollectionMaterializationRecord,
    identity: CollectionDemandIdentity,
    attempt: UUID
  ) async -> Bool {
    guard let entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return true
    }
    if record.demand != identity {
      // An earlier generation stored this copy. Bind it to this demand before asking the source,
      // so a store that binds only by snapshot costs no request before the snapshot path.
      let rebound: Bool
      do {
        rebound = try await store.rebindMaterialization(record.id, to: identity)
      } catch is CancellationError {
        return true
      } catch {
        fail(identity: identity, attempt: attempt, with: .storeUnavailable)
        return true
      }
      guard let current = entries[identity], current.attempt == attempt, !current.leases.isEmpty
      else { return true }
      guard rebound else {
        clearResumingAttempt(identity: identity, attempt: attempt)
        return false
      }
    }
    let resumed: CollectionResumeSession<Model, Key>?
    do {
      resumed = try await source.resume(
        entry.demand, identity: identity, materializationID: record.id, record: record)
    } catch is CancellationError {
      return true
    } catch {
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return true
    }
    guard let session = resumed else {
      clearResumingAttempt(identity: identity, attempt: attempt)
      return false
    }

    let stop = AtMostOnceStop(session.stop)
    guard install(stop: stop, identity: identity, attempt: attempt) else {
      do {
        try await stop.call()
      } catch {
        fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      }
      return true
    }
    do {
      try Task.checkCancellation()
      let current = try await store.materialization(for: identity)
      guard let current, current.id == record.id, current.cursor == session.cursor,
        current.snapshotFence == record.snapshotFence
      else {
        // Another writer replaced or advanced the record while the source was asked. Release
        // the resumed claim and snapshot rather than tail from a position the store left.
        guard await cleanup(stop, identity: identity, attempt: attempt) else { return true }
        clearResumingAttempt(identity: identity, attempt: attempt)
        return false
      }
    } catch is CancellationError {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return true
    } catch {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return true
    }
    guard let current = entries[identity], current.attempt == attempt, !Task.isCancelled else {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return true
    }

    do {
      try await session.run(
        { batch in
          // A committed batch is not yet live: the feed may hold more after it.
          try Task.checkCancellation()
          try await self.apply(batch, identity: identity, attempt: attempt)
        },
        { await self.markResumedLive(identity: identity, attempt: attempt) }
      )
    } catch is CancellationError {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      return true
    } catch is CollectionStoreApplyFailure {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return true
    } catch is CollectionResumeInvalidated {
      // The feed will not serve the stored cursor. Until the tail has committed or caught up,
      // nothing has been reported live from it, so this attempt reseeds by snapshot.
      guard await cleanup(stop, identity: identity, attempt: attempt) else { return true }
      if let entry = entries[identity], entry.attempt == attempt, entry.state == .cached,
        !Task.isCancelled
      {
        clearResumingAttempt(identity: identity, attempt: attempt)
        return false
      }
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return true
    } catch {
      _ = await cleanup(stop, identity: identity, attempt: attempt)
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return true
    }

    if await cleanup(stop, identity: identity, attempt: attempt) {
      finishStream(identity: identity, attempt: attempt)
    }
    return true
  }

  /// The resumed tail has read to its feed's head: report the cached demand live. A refresh,
  /// failure or replacement attempt already owns the state, so only `.cached` advances.
  private func markResumedLive(identity: CollectionDemandIdentity, attempt: UUID) {
    guard let entry = entries[identity], entry.attempt == attempt, entry.state == .cached else {
      return
    }
    updateEntry(identity: identity, attempt: attempt, state: .live)
    stalePassWake?.yield(())
  }

  private func clearResumingAttempt(identity: CollectionDemandIdentity, attempt: UUID) {
    guard var entry = entries[identity], entry.resumingAttempt == attempt else { return }
    entry.resumingAttempt = nil
    entries[identity] = entry
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

  private func refresh(leaseID: UUID, mode: CollectionLoadMode) async {
    guard let identity = demandByLease[leaseID] else { return }
    await beginRefresh(identity, requiringSnapshot: mode == .snapshot)?.value
  }

  /// Starts replacing the demand's current attempt and returns the task that does, or the one
  /// already doing so; nil while the demand's last lease is being released. With
  /// `requiringSnapshot` the attempt that starts next snapshots, whichever caller began the
  /// replacement and whatever the attempt being replaced commits on its way out.
  private func beginRefresh(
    _ identity: CollectionDemandIdentity, requiringSnapshot: Bool = false
  ) -> Task<Void, Never>? {
    guard var entry = entries[identity] else { return nil }
    if requiringSnapshot {
      // Kept even when nothing is started here: the attempt that follows a release in flight,
      // for a lease that joined during it, takes the snapshot.
      entry.nextAttemptRequiresSnapshot = true
      entries[identity] = entry
    }
    guard entry.releaseToken == nil else { return nil }
    if let task = entry.refreshTask { return task }
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
    return task
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
  /// in flight are fenced behind it and receive a fresh source attempt after it completes. Only a
  /// copy still bound to this coordinator's demand (its generation included) is removed: once
  /// another generation's snapshot has bound the copy, it is that generation's, and a retired
  /// generation's eviction leaves it. A copy an earlier launch stored and this launch has neither
  /// loaded nor resumed is bound to the earlier launch's demand, so this does nothing for it;
  /// remove such copies through the store, as an app's age-based eviction does.
  public func evict(_ demand: CollectionDemand<Model>) async throws {
    let identity = demand.identity(for: definition, scope: scope, columns: source.columns)
    if let task = evictionTasks[identity.cacheKey] {
      try await task.value
      return
    }
    guard entries[identity] == nil else { throw CollectionEvictionError.activeDemand }
    let task = Task<Void, Error> { [weak self] in
      guard let self else { return }
      try await self.performEviction(identity)
    }
    evictionTasks[identity.cacheKey] = task
    try await task.value
  }

  /// Removes the copy `identity` names, only while the store still binds it to exactly `identity`
  /// (generation included). With `copy`, only that materialization is removed: when the key finds
  /// another copy or none, `copy` is a copy under the key that no lookup reaches.
  private func performEviction(
    _ identity: CollectionDemandIdentity, copy: CollectionMaterializationID? = nil
  ) async throws {
    do {
      let record = try await store.materialization(for: identity)
      if let copy, record?.id != copy {
        try await store.removeMaterialization(copy)
      } else if let record, record.demand == identity {
        try await store.removeMaterialization(record.id)
      }
      finishEviction(identity)
    } catch {
      finishEviction(identity)
      throw error
    }
  }

  private func finishEviction(_ identity: CollectionDemandIdentity) {
    evictionTasks.removeValue(forKey: identity.cacheKey)
    // The fenced entry is this generation's demand for the same key, whatever generation the
    // removed copy was stored under.
    for (waiting, var entry) in entries
    where waiting.cacheKey == identity.cacheKey && entry.awaitsEviction && !entry.leases.isEmpty {
      entry.awaitsEviction = false
      entries[waiting] = entry
      startAttempt(for: waiting, attempt: entry.attempt)
    }
  }
}
