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
    var refreshToken: UUID?
    var refreshTask: Task<Void, Never>?
    var releaseToken: UUID?
    var releaseTask: Task<Void, Error>?
    var awaitsEviction: Bool
  }

  private let definition: CollectionDefinition<Model, Key>
  private let scope: CollectionScope
  private let source: Source
  private let store: Store
  private var entries: [CollectionDemandIdentity: Entry] = [:]
  private var demandByLease: [UUID: CollectionDemandIdentity] = [:]
  private var evictionTasks: [CollectionDemandIdentity: Task<Void, Error>] = [:]

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
        awaitsEviction: evictionTasks[identity] != nil
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
    guard let current = entries[identity], current.attempt == attempt, !current.leases.isEmpty,
      current.releaseToken == nil
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
