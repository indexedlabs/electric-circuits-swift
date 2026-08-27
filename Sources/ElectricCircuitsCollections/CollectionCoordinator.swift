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

/// One consumer's cancellable interest in a collection demand. Copying is intentionally impossible:
/// each acquired lease has one independently idempotent `release()` lifecycle.
public actor CollectionLease {
  public nonisolated let stateUpdates: AsyncStream<CollectionLoadState>

  private let id: UUID
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
  }

  private let definition: CollectionDefinition<Model, Key>
  private let scope: CollectionScope
  private let source: Source
  private let store: Store
  private var entries: [CollectionDemandIdentity: Entry] = [:]
  private var demandByLease: [UUID: CollectionDemandIdentity] = [:]

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

  public func acquire(_ demand: CollectionDemand<Model>) -> CollectionLease {
    let identity = demand.identity(for: definition, scope: scope)
    let leaseID = UUID()
    let updates = AsyncStream<CollectionLoadState>.makeStream(
      bufferingPolicy: .bufferingNewest(16))

    if var entry = entries[identity] {
      entry.leases[leaseID] = updates.continuation
      entries[identity] = entry
      updates.continuation.yield(entry.state)
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
        stop: nil
      )
      updates.continuation.yield(.unavailable)
      startAttempt(for: identity, attempt: attempt)
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
    guard var entry = entries[identity], entry.attempt == attempt else { return }
    let task = Task { [weak self] in
      guard let self else { return }
      await self.run(identity: identity, attempt: attempt)
    }
    entry.task = task
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
    guard let current = entries[identity], current.attempt == attempt, !current.leases.isEmpty
    else { throw CancellationError() }
    do {
      try await store.apply(batch, to: current.materializationID)
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
      return true
    } catch {
      // Keep the failed stop installed so a lease release can retry the same server authority.
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return false
    }
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
    with failure: CollectionLoadFailure
  ) {
    guard var entry = entries[identity], entry.attempt == attempt, !entry.leases.isEmpty else {
      return
    }
    entry.task = nil
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
    guard let identity = demandByLease[leaseID], var entry = entries[identity] else { return }
    entry.task?.cancel()
    let previousTask = entry.task
    let previousStop = entry.stop
    let attempt = UUID()
    entry.attempt = attempt
    entry.task = nil
    entry.stop = nil
    entry.state = .refreshing
    entries[identity] = entry
    for continuation in entry.leases.values {
      continuation.yield(.refreshing)
    }
    // A cancellation-resistant source may still be applying or holding its server claim. Join it
    // before installing a replacement so its delayed cleanup cannot release the new attempt.
    if let previousTask { await previousTask.value }
    do { try await previousStop?.call() } catch {
      fail(identity: identity, attempt: entry.attempt, with: .sourceUnavailable)
      return
    }
    startAttempt(for: identity, attempt: attempt)
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

    let task = entry.task
    task?.cancel()
    if let stop = entry.stop {
      try await stop.call()
    } else if let task {
      // A source can have accepted a remote claim yet still be returning its local session when the
      // final lease is released. Keep this lease's authority installed until that cancelled attempt
      // has either installed and released its stop action or failed before creating a claim.
      await task.value
      guard let current = entries[identity] else { return }
      if let stop = current.stop { try await stop.call() }
    }
    demandByLease.removeValue(forKey: leaseID)
    entry.leases[leaseID]?.finish()
    entries.removeValue(forKey: identity)
  }
}
