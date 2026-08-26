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
  private let releaseAction: @Sendable (UUID) async -> Void
  private var released = false

  init(
    id: UUID,
    stateUpdates: AsyncStream<CollectionLoadState>,
    stateAction: @escaping @Sendable (UUID) async -> CollectionLoadState,
    refreshAction: @escaping @Sendable (UUID) async -> Void,
    releaseAction: @escaping @Sendable (UUID) async -> Void
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

  public func release() async {
    guard !released else { return }
    released = true
    await releaseAction(id)
  }
}

private actor AtMostOnceStop {
  private let action: @Sendable () async -> Void
  private var task: Task<Void, Never>?

  init(_ action: @escaping @Sendable () async -> Void) {
    self.action = action
  }

  func call() async {
    if let task {
      await task.value
      return
    }
    let action = action
    let task = Task { await action() }
    self.task = task
    await task.value
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
      releaseAction: { await self.release(leaseID: $0) }
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
      await stop.call()
      return
    }
    do {
      try Task.checkCancellation()
      guard let current = entries[identity], current.attempt == attempt else {
        await stop.call()
        return
      }
      try await store.replaceSnapshot(
        session.snapshot,
        materializationID: current.materializationID,
        demand: identity
      )
    } catch is CancellationError {
      await stop.call()
      return
    } catch {
      await stop.call()
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return
    }

    guard updateEntry(identity: identity, attempt: attempt, state: .live) else {
      await stop.call()
      return
    }

    do {
      try await session.run { batch in
        try Task.checkCancellation()
        try await self.apply(batch, identity: identity, attempt: attempt)
      }
    } catch is CancellationError {
      await stop.call()
      return
    } catch is CollectionStoreApplyFailure {
      await stop.call()
      fail(identity: identity, attempt: attempt, with: .storeUnavailable)
      return
    } catch {
      await stop.call()
      fail(identity: identity, attempt: attempt, with: .sourceUnavailable)
      return
    }

    await stop.call()
    finishStream(identity: identity, attempt: attempt)
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
    entry.stop = nil
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
    await previousStop?.call()
    startAttempt(for: identity, attempt: attempt)
  }

  private func release(leaseID: UUID) async {
    guard let identity = demandByLease.removeValue(forKey: leaseID), var entry = entries[identity]
    else { return }
    entry.leases.removeValue(forKey: leaseID)?.finish()
    guard entry.leases.isEmpty else {
      entries[identity] = entry
      return
    }

    entry.task?.cancel()
    let stop = entry.stop
    entries.removeValue(forKey: identity)
    await stop?.call()
  }
}
