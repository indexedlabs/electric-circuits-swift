import Foundation

public protocol ShapeSubscriptionClock: Sendable {
  func sleep(for duration: Duration) async throws
}

public struct ContinuousShapeSubscriptionClock: ShapeSubscriptionClock, Sendable {
  private let clock = ContinuousClock()
  public init() {}
  public func sleep(for duration: Duration) async throws { try await clock.sleep(for: duration) }
}

public struct ShapeSubscriptionRetryPolicy: Sendable {
  public let maxRetries: Int
  public let baseDelay: Duration
  public let maximumDelay: Duration
  public let jitterRatio: Double
  private let randomUnit: @Sendable () -> Double

  public init(
    maxRetries: Int = 8, baseDelay: Duration = .milliseconds(250),
    maximumDelay: Duration = .seconds(30), jitterRatio: Double = 0.2,
    randomUnit: @escaping @Sendable () -> Double = { Double.random(in: 0...1) }
  ) {
    precondition(maxRetries >= 0)
    precondition(baseDelay >= .zero && maximumDelay >= baseDelay)
    precondition((0...1).contains(jitterRatio))
    self.maxRetries = maxRetries
    self.baseDelay = baseDelay
    self.maximumDelay = maximumDelay
    self.jitterRatio = jitterRatio
    self.randomUnit = randomUnit
  }

  public func delay(forRetry retry: Int) -> Duration {
    precondition(retry > 0)
    func seconds(_ duration: Duration) -> Double {
      let c = duration.components
      return Double(c.seconds) + Double(c.attoseconds) / 1e18
    }
    let ceiling = seconds(maximumDelay)
    let raw = min(seconds(baseDelay) * pow(2, Double(min(retry - 1, 62))), ceiling)
    let sample = min(max(randomUnit(), 0), 1)
    return .seconds(min(max(raw * (1 + ((sample * 2) - 1) * jitterRatio), 0), ceiling))
  }

  /// The client backoff and a server `Retry-After` directive are both lower bounds. The policy's
  /// maximum remains an absolute upper bound so a malicious or stale server header cannot pin a
  /// subscription forever.
  public func delay(forRetry retry: Int, retryAfter: Duration?) -> Duration {
    let clientDelay = delay(forRetry: retry)
    guard let retryAfter else { return clientDelay }
    return min(max(clientDelay, retryAfter), maximumDelay)
  }
}

/// Bounds the client-side fall-through for a create/join the engine answered `410 Gone`.
///
/// The engine retires a dormant shape whose replay is over budget, and abandons a reactivation
/// join past its own timeout, as a typed recreate outcome rather than a server fault
/// (electric-circuits ADR-0011, "Dormant reactivation is bounded"). It normally answers that by
/// falling through to a fresh create in the same round trip; when that fall-through is exhausted
/// it answers `410` and expects the client to recreate.
///
/// The bound matters because a join that timed out leaves a detached replay running on the
/// engine: each attempt can cost its full join timeout, and only a later attempt finds the shape
/// active. A create answered `410` past the bound is a standing condition, not a reactivation
/// race, and reaches the caller as the terminal `ClientError.http(status: 410, ...)` it already
/// was. The backoff exists to avoid a hot loop, not to pace the engine's replay.
public struct ShapeSubscriptionRecreatePolicy: Equatable, Sendable {
  /// Recreates attempted after the first `410`. `0` surfaces the first `410` as terminal.
  public let maximumRecreates: Int
  public let backoff: Duration

  public init(maximumRecreates: Int = 2, backoff: Duration = .milliseconds(250)) {
    precondition(maximumRecreates >= 0)
    precondition(backoff >= .zero)
    self.maximumRecreates = maximumRecreates
    self.backoff = backoff
  }
}

extension ShapeSubscriptionRecreatePolicy {
  /// The engine's "recreate" answer on a create/join whose fall-through it could not perform.
  public static let goneStatus = 410

  /// True when `error` is that answer, in either the raw `ClientError` form the client throws or
  /// the `ShapeSubscriptionFailure` form the coordinator's retry wrapper produces.
  public static func isGone(_ error: Error) -> Bool {
    if case ClientError.http(let status, _) = error { return status == goneStatus }
    if case ShapeSubscriptionFailure.client(let client) = error,
      case .http(let status, _) = client
    {
      return status == goneStatus
    }
    return false
  }

  /// Runs `body`, answering a `410 Gone` with a bounded, backed-off re-run of the identical
  /// request. This is the one implementation of the client-side fall-through: both
  /// `ShapeSubscriptionCoordinator`'s create/join and a source's initial feed create use it, so a
  /// shape minted before a coordinator exists gets the same recreate vocabulary as one minted by
  /// it.
  ///
  /// `body` must be the same request every time — the two native routes that mint a shape carry no
  /// shape id, so re-running them re-sends the same table, predicate, columns, and stable claim.
  /// Every other outcome, `404` and the transient statuses included, propagates unchanged.
  /// `willRecreate` receives the 1-based attempt number before its backoff, for state or telemetry.
  public func recreatingOnGone<T: Sendable>(
    clock: any ShapeSubscriptionClock,
    willRecreate: @Sendable (Int) async -> Void = { _ in },
    _ body: @Sendable () async throws -> T
  ) async throws -> T {
    var recreates = 0
    while true {
      do { return try await body() } catch {
        guard Self.isGone(error), recreates < maximumRecreates else { throw error }
        recreates += 1
        await willRecreate(recreates)
        try await clock.sleep(for: backoff)
        try Task.checkCancellation()
      }
    }
  }
}

/// A caller-owned, shared admission boundary for shape subscription coordinators. Sharing one
/// instance across coordinators bounds active claims and the stream/retry tasks they own; it does
/// not queue pending starts.
public actor ShapeSubscriptionCapacity {
  public struct Snapshot: Equatable, Sendable {
    public let limit: Int
    public let active: Int
    public let admitted: Int
    public let rejected: Int

    public init(limit: Int, active: Int, admitted: Int, rejected: Int) {
      self.limit = limit
      self.active = active
      self.admitted = admitted
      self.rejected = rejected
    }
  }

  struct Permit: Hashable, Sendable {
    fileprivate let id: UInt64
  }

  public let maximumActiveSubscriptions: Int
  private var nextPermitID: UInt64 = 0
  private var permits: Set<Permit> = []
  private var admitted = 0
  private var rejected = 0

  public init(maximumActiveSubscriptions: Int) {
    self.maximumActiveSubscriptions = max(1, maximumActiveSubscriptions)
  }

  func acquire() throws -> Permit {
    guard permits.count < maximumActiveSubscriptions else {
      rejected += 1
      throw ShapeSubscriptionFailure.capacityExceeded(limit: maximumActiveSubscriptions)
    }
    nextPermitID &+= 1
    let permit = Permit(id: nextPermitID)
    permits.insert(permit)
    admitted += 1
    return permit
  }

  func release(_ permit: Permit) {
    permits.remove(permit)
  }

  public func snapshot() -> Snapshot {
    Snapshot(
      limit: maximumActiveSubscriptions, active: permits.count, admitted: admitted,
      rejected: rejected)
  }
}

public enum ShapeSubscriptionReseedReason: Equatable, Sendable {
  case terminal(StreamTerminalReason)
  case replacement(previousShapeID: String, replacementShapeID: String)
}

public struct ShapeSubscriptionReseedRequired: Equatable, Sendable {
  public let reason: ShapeSubscriptionReseedReason
  public let previous: ShapeHandle
  public let replacement: ShapeHandle?
  public init(
    reason: ShapeSubscriptionReseedReason, previous: ShapeHandle, replacement: ShapeHandle? = nil
  ) {
    self.reason = reason
    self.previous = previous
    self.replacement = replacement
  }
}

public enum ShapeSubscriptionTransportFailure: Equatable, Sendable {
  case url(code: Int)
  case unknown
}

public enum ShapeSubscriptionRetryCause: Equatable, Sendable {
  case http(status: Int)
  case transport(ShapeSubscriptionTransportFailure)
}

public enum ShapeSubscriptionFailure: Error, Equatable, Sendable {
  case stream(StreamError)
  case client(ClientError)
  case transport(ShapeSubscriptionTransportFailure)
  case materializerUnavailable(MaterializerAvailabilityError)
  case materializer
  case retryExhausted(operation: String, attempts: Int, cause: ShapeSubscriptionRetryCause)
  case reseedRequired(ShapeSubscriptionReseedRequired)
  case capacityExceeded(limit: Int)
}

public enum ShapeSubscriptionState: Equatable, Sendable {
  case idle, starting, renewing, stopping, stopped
  case streaming(StreamCursor)
  case waitingToRetry(operation: String, attempt: Int, delay: Duration)
  case reseedRequired(ShapeSubscriptionReseedRequired)
  case failed(ShapeSubscriptionFailure)
}

/// Native claim route used for initial creation and stable-subscription renewal.
public enum ShapeSubscriptionKind: Equatable, Sendable {
  case shape
  case subsetFeed
}

private struct MaterializerFailure: Error, Sendable {}
private struct CoordinatedMaterializer: ShapeMaterializer {
  let base: any ShapeMaterializer
  func currentCursor() async throws -> StreamCursor? {
    do { return try await base.currentCursor() } catch let availability
      as MaterializerAvailabilityError
    {
      throw availability
    } catch {
      throw MaterializerFailure()
    }
  }
  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    do { try await base.apply(batch, expecting: expectedCursor, advancingTo: cursor) } catch let
      availability as MaterializerAvailabilityError
    {
      throw availability
    } catch {
      throw MaterializerFailure()
    }
  }
}

/// One shape claim, reader, and lease lifecycle. Reconnects always read the cursor from the
/// materializer, never from process memory, preserving the provider's atomic apply/cursor boundary.
public actor ShapeSubscriptionCoordinator {
  public let stateUpdates: AsyncStream<ShapeSubscriptionState>
  public private(set) var state: ShapeSubscriptionState = .idle

  private let client: ElectricCircuitsClient
  private let transport: any HTTPTransport
  private let request: ShapeRequest
  private let materializer: any ShapeMaterializer
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let capacity: ShapeSubscriptionCapacity?
  private let responseDecodingLimits: ResponseDecodingLimits
  private let telemetry: TelemetryReporter
  private let kind: ShapeSubscriptionKind
  private let recreatePolicy: ShapeSubscriptionRecreatePolicy
  private var continuation: AsyncStream<ShapeSubscriptionState>.Continuation?
  private var handle: ShapeHandle?
  private var pendingRelease: [ShapeHandle] = []
  private var generation: UInt64 = 0
  private var stopping = false
  private var startTask: Task<ShapeHandle, Error>?
  private var renewTask: Task<ShapeHandle, Error>?
  private var streamTask: Task<Void, Never>?
  private var leaseTask: Task<Void, Never>?
  private var stopTask: Task<Void, Error>?
  private var capacityPermit: ShapeSubscriptionCapacity.Permit?
  // A newly installed claim has no external owner until a `start()` caller successfully receives
  // it. A cancelled sibling must compensate an unacknowledged claim, but must not stop a claim
  // that another caller already received.
  private var currentClaimAcknowledged = false
  private var nextStartWaiterID: UInt64 = 0
  private var startWaiters: Set<UInt64> = []
  // A caller that joined an initial create owns cancellation compensation until `start()` returns.
  // A later caller that merely observes an already-installed handle must never tear that claim down.
  private var initialStartWaiters: Set<UInt64> = []
  private var cancelledStartWaiters: Set<UInt64> = []
  private var cancellationAcknowledgements: [UInt64: [CheckedContinuation<Void, Never>]] = [:]
  private var startCancellationCleanupTask: Task<Void, Error>?
  #if DEBUG
    private var initialStartWaiterCountWaiters:
      [(count: Int, continuation: CheckedContinuation<Void, Never>)] = []
    private var heldStartReturnWaiters: Set<UInt64> = []
    private var reachedStartReturnWaiters: Set<UInt64> = []
    private var startReturnReachWaiters: [UInt64: [CheckedContinuation<Void, Never>]] = [:]
    private var startReturnGates: [UInt64: CheckedContinuation<Void, Never>] = [:]
  #endif

  public init(
    client: ElectricCircuitsClient, transport: any HTTPTransport, request: ShapeRequest,
    materializer: any ShapeMaterializer, retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    capacity: ShapeSubscriptionCapacity? = nil,
    responseDecodingLimits: ResponseDecodingLimits = .default,
    telemetry: TelemetryReporter = .noop,
    kind: ShapeSubscriptionKind = .shape,
    recreatePolicy: ShapeSubscriptionRecreatePolicy = .init()
  ) {
    self.client = client
    self.transport = transport
    self.request = request
    self.materializer = materializer
    self.retryPolicy = retryPolicy
    self.clock = clock
    self.capacity = capacity
    self.responseDecodingLimits = responseDecodingLimits
    self.telemetry = telemetry
    self.kind = kind
    self.recreatePolicy = recreatePolicy
    let updates = AsyncStream<ShapeSubscriptionState>.makeStream()
    stateUpdates = updates.stream
    continuation = updates.continuation
  }

  deinit {
    continuation?.finish()
    startTask?.cancel()
    renewTask?.cancel()
    streamTask?.cancel()
    leaseTask?.cancel()
    stopTask?.cancel()
  }

  @discardableResult
  public func start() async throws -> ShapeHandle {
    let waiterID = registerStartWaiter()
    return try await withTaskCancellationHandler(
      operation: { try await self.startForWaiter(waiterID) },
      onCancel: { Task { await self.cancelStartWaiter(waiterID) } })
  }

  private func startForWaiter(_ waiterID: UInt64) async throws -> ShapeHandle {
    registerInitialStartParticipationIfNeeded(waiterID)
    defer { finishStartWaiter(waiterID) }
    if Task.isCancelled {
      await awaitCancellationAcknowledgement(for: waiterID)
      try await awaitCancelledStartCleanup()
      throw CancellationError()
    }
    do {
      let created = try await startImpl()
      #if DEBUG
        await waitAtStartReturnGateForTesting(waiterID)
      #endif
      if Task.isCancelled {
        await awaitCancellationAcknowledgement(for: waiterID)
        try await awaitCancelledStartCleanup()
        throw CancellationError()
      }
      acknowledgeCurrentClaim(created)
      return created
    } catch {
      if Task.isCancelled {
        await awaitCancellationAcknowledgement(for: waiterID)
        try await awaitCancelledStartCleanup()
        throw CancellationError()
      }
      throw error
    }
  }

  private func startImpl() async throws -> ShapeHandle {
    if let handle, isAcceptingOperations { return handle }
    if let task = startTask {
      let created = try await task.value
      guard isAcceptingOperations else { throw CancellationError() }
      return created
    }
    guard isAcceptingOperations else { throw CancellationError() }
    transition(.starting)
    let task = Task { [weak self] in
      guard let self else { throw CancellationError() }
      return try await self.createWithAdmissionAndRetry()
    }
    startTask = task
    do {
      let created = try await task.value
      startTask = nil
      guard isAcceptingOperations else { throw CancellationError() }
      handle = created
      currentClaimAcknowledged = false
      generation &+= 1
      launchStream(for: created, generation: generation)
      return created
    } catch {
      startTask = nil
      if !isAcceptingOperations || error is CancellationError { throw error }
      let failure = failure(for: error)
      transition(.failed(failure))
      throw failure
    }
  }

  private func registerStartWaiter() -> UInt64 {
    nextStartWaiterID &+= 1
    let id = nextStartWaiterID
    startWaiters.insert(id)
    return id
  }

  private func registerInitialStartParticipationIfNeeded(_ waiterID: UInt64) {
    guard handle == nil else { return }
    initialStartWaiters.insert(waiterID)
    #if DEBUG
      resumeInitialStartWaiterCountWaiters()
    #endif
  }

  private func acknowledgeCurrentClaim(_ candidate: ShapeHandle) {
    guard handle == candidate, isAcceptingOperations else { return }
    currentClaimAcknowledged = true
  }

  private func finishStartWaiter(_ waiterID: UInt64) {
    startWaiters.remove(waiterID)
    initialStartWaiters.remove(waiterID)
    cancelledStartWaiters.remove(waiterID)
    let acknowledgements = cancellationAcknowledgements.removeValue(forKey: waiterID) ?? []
    for acknowledgement in acknowledgements { acknowledgement.resume() }
  }

  private func cancelStartWaiter(_ waiterID: UInt64) {
    guard startWaiters.contains(waiterID), cancelledStartWaiters.insert(waiterID).inserted else {
      return
    }
    let acknowledgements = cancellationAcknowledgements.removeValue(forKey: waiterID) ?? []
    for acknowledgement in acknowledgements { acknowledgement.resume() }

    // A cancelled waiter detaches from a shared start. Only the last live initial-start caller
    // owns teardown; cancelling one of several callers must leave their shared claim/start task
    // intact. The `handle != nil` arm is intentional: create may have landed after cancellation
    // but before this caller observes cancellation. Its public cancellation result must wait for
    // that landed claim's DELETE and permit return.
    guard
      initialStartWaiters.contains(waiterID), activeInitialStartWaiterCount == 0,
      !currentClaimAcknowledged,
      startTask != nil || handle != nil
    else { return }
    startTask?.cancel()
    guard startCancellationCleanupTask == nil else { return }
    startCancellationCleanupTask = Task { [weak self] in
      guard let self else { return }
      try await self.stop()
    }
  }

  private var activeInitialStartWaiterCount: Int {
    initialStartWaiters.filter { !cancelledStartWaiters.contains($0) }.count
  }

  private func awaitCancellationAcknowledgement(for waiterID: UInt64) async {
    guard !cancelledStartWaiters.contains(waiterID) else { return }
    await withCheckedContinuation {
      cancellationAcknowledgements[waiterID, default: []].append($0)
    }
  }

  private func awaitCancelledStartCleanup() async throws {
    guard let cleanup = startCancellationCleanupTask else { return }
    try await cleanup.value
  }

  #if DEBUG
    func holdStartReturnForTesting(_ waiterID: UInt64) {
      heldStartReturnWaiters.insert(waiterID)
    }

    func waitForStartReturnGateForTesting(_ waiterID: UInt64) async {
      guard !reachedStartReturnWaiters.contains(waiterID) else { return }
      await withCheckedContinuation { startReturnReachWaiters[waiterID, default: []].append($0) }
    }

    func releaseStartReturnGateForTesting(_ waiterID: UInt64) {
      startReturnGates.removeValue(forKey: waiterID)?.resume()
    }

    private func waitAtStartReturnGateForTesting(_ waiterID: UInt64) async {
      guard heldStartReturnWaiters.contains(waiterID) else { return }
      reachedStartReturnWaiters.insert(waiterID)
      let waiters = startReturnReachWaiters.removeValue(forKey: waiterID) ?? []
      for waiter in waiters { waiter.resume() }
      await withCheckedContinuation { startReturnGates[waiterID] = $0 }
    }

    func waitForInitialStartCallersForTesting(_ count: Int) async {
      guard initialStartWaiters.count < count else { return }
      await withCheckedContinuation { initialStartWaiterCountWaiters.append((count, $0)) }
    }

    private func resumeInitialStartWaiterCountWaiters() {
      let ready = initialStartWaiterCountWaiters.filter { initialStartWaiters.count >= $0.count }
      initialStartWaiterCountWaiters.removeAll { initialStartWaiters.count >= $0.count }
      for waiter in ready { waiter.continuation.resume() }
    }
  #endif

  @discardableResult
  public func renew() async throws -> ShapeHandle {
    guard let previous = handle, isAcceptingOperations else { throw CancellationError() }
    let expectedGeneration = generation
    if let task = renewTask {
      let renewed = try await task.value
      guard isCurrent(expectedGeneration) else {
        appendRelease(renewed)
        throw CancellationError()
      }
      return renewed
    }
    transition(.renewing)
    let task = Task { [weak self] in
      guard let self else { throw CancellationError() }
      return try await self.renewWithRetry()
    }
    renewTask = task
    do {
      let renewed = try await task.value
      renewTask = nil
      // A terminal stream result or stop can advance the generation while the HTTP response is in
      // flight. Never publish a handle from that retired generation; retain it for compensation.
      guard isCurrent(expectedGeneration) else {
        appendRelease(renewed)
        throw CancellationError()
      }
      guard renewed.id == previous.id else {
        let outcome = ShapeSubscriptionReseedRequired(
          reason: .replacement(previousShapeID: previous.id, replacementShapeID: renewed.id),
          previous: previous, replacement: renewed)
        // A replacement cannot inherit an old cursor or an in-flight old-generation apply.
        let oldStream = streamTask
        oldStream?.cancel()
        if let oldStream { await oldStream.value }
        streamTask = nil
        try await requireReseed(outcome, extraRelease: renewed)
        throw ShapeSubscriptionFailure.reseedRequired(outcome)
      }
      handle = renewed
      transition(.streaming(try await committedCursor()))
      return renewed
    } catch {
      renewTask = nil
      if !isAcceptingOperations || error is CancellationError { throw error }
      let failure = error as? ShapeSubscriptionFailure ?? failure(for: error)
      if case .reseedRequired = failure { throw failure }
      transition(.failed(failure))
      throw failure
    }
  }

  /// Joined close. It cancels I/O, drains every accepted tail (including materialization), then
  /// retries release of every handle that could have landed before publishing `.stopped`.
  public func stop() async throws {
    if let task = stopTask { return try await task.value }
    let task = Task { [weak self] in
      guard let self else { return }
      try await self.finishStop()
    }
    stopTask = task
    do {
      try await task.value
      stopTask = nil
    } catch {
      stopTask = nil
      throw error
    }
  }

  private func finishStop() async throws {
    if !stopping {
      stopping = true
      generation &+= 1
      transition(.stopping)
      if let handle { appendRelease(handle) }
      handle = nil
    }
    let create = startTask
    let renew = renewTask
    let stream = streamTask
    let lease = leaseTask
    startTask?.cancel()
    renewTask?.cancel()
    streamTask?.cancel()
    leaseTask?.cancel()
    if let stream { await stream.value }
    if let lease { await lease.value }
    if let create, case .success(let created) = await create.result { appendRelease(created) }
    if let renew, case .success(let renewed) = await renew.result { appendRelease(renewed) }
    startTask = nil
    renewTask = nil
    streamTask = nil
    leaseTask = nil
    do {
      try await releasePending()
      await releaseCapacityPermit()
      transition(.stopped)
      continuation?.finish()
      continuation = nil
    } catch {
      let failure = error as? ShapeSubscriptionFailure ?? failure(for: error)
      transition(.failed(failure))
      throw failure
    }
  }

  private func launchStream(for handle: ShapeHandle, generation: UInt64) {
    streamTask = Task { [weak self] in await self?.consume(handle: handle, generation: generation) }
    if let seconds = handle.response.leaseSeconds, seconds > 0 {
      let interval = max(Duration.seconds(Double(seconds) / 3), .seconds(1))
      leaseTask = Task { [weak self] in
        await self?.renewOnLease(interval: interval, generation: generation)
      }
    }
  }

  private func consume(handle streamHandle: ShapeHandle, generation: UInt64) async {
    var retries = 0
    while isCurrent(generation) {
      do {
        transition(.streaming(try await committedCursor()))
        let reader = ShapeStreamReader(
          streamURL: streamHandle.stream.url, transport: transport,
          materializer: CoordinatedMaterializer(base: materializer), telemetry: telemetry,
          responseDecodingLimits: responseDecodingLimits)
        try await reader.run()
        return
      } catch {
        guard isCurrent(generation), !(error is CancellationError) else { return }
        if let stream = error as? StreamError, case .terminal(_, _, let reason) = stream {
          let outcome = ShapeSubscriptionReseedRequired(
            reason: .terminal(reason), previous: streamHandle)
          do { try await requireReseed(outcome) } catch { transition(.failed(failure(for: error))) }
          return
        }
        guard isRetryable(error), retries < retryPolicy.maxRetries else {
          transition(.failed(exhaustedOrFailure(error, retries: retries, operation: "stream")))
          return
        }
        retries += 1
        let delay = retryPolicy.delay(forRetry: retries, retryAfter: retryAfter(for: error))
        transition(.waitingToRetry(operation: "stream", attempt: retries, delay: delay))
        do { try await clock.sleep(for: delay) } catch { return }
      }
    }
  }

  private func renewOnLease(interval: Duration, generation: UInt64) async {
    while isCurrent(generation) {
      do {
        try await clock.sleep(for: interval)
        guard isCurrent(generation) else { return }
        _ = try await renew()
      } catch { return }
    }
  }

  private func createWithRetry() async throws -> ShapeHandle {
    // Read the durable checkpoint before creating any server-owned claim. This is both the resume
    // authority and the earliest safe provider availability boundary: a locked or unavailable
    // store must not leave a claim that the client cannot materialize or release reliably.
    do {
      _ = try await materializer.currentCursor()
    } catch is CancellationError {
      throw CancellationError()
    } catch let availability as MaterializerAvailabilityError {
      throw availability
    } catch {
      throw MaterializerFailure()
    }
    try Task.checkCancellation()
    return try await recreatingOnGone(operation: "create") {
      switch self.kind {
      case .shape:
        try await self.client.createShape(self.request)
      case .subsetFeed:
        try await self.client.createSubsetFeed(self.request)
      }
    }
  }

  /// `POST /v1/shapes` and `POST /v1/subset-feeds` are the only native routes that mint a shape,
  /// and they are the only routes with a recreate vocabulary. The engine retires a dormant shape
  /// whose replay is over budget, and abandons a reactivation join past its bound, as a typed
  /// recreate outcome rather than a server fault (electric-circuits ADR-0011). It normally answers
  /// that by falling through to a fresh create in the same round trip, returning `2xx` with a NEW
  /// shape id — the replacement this coordinator already reseeds from. When the fall-through is
  /// exhausted, or the join timed out, it answers `410 Gone` instead, because `410` is what the
  /// durable-stream reader already maps to "gone, get a fresh one".
  ///
  /// This performs the fall-through the engine could not: a byte-identical re-POST. The request
  /// carries no shape id — the retired id only ever lived on the server — so the same table,
  /// predicate, columns, and stable subscription claim are re-sent and the caller's demand
  /// identity survives. The bound matters because a join that timed out leaves a detached replay
  /// running: each attempt can cost the engine's full join timeout, and only a later attempt finds
  /// the shape active. The backoff exists to avoid a hot loop, not to pace that replay.
  ///
  /// `404` is deliberately excluded. ADR-0011 keeps it out of the create vocabulary because it is
  /// ambiguous with an unknown shape id, and these two routes are collection endpoints with no
  /// shape id at all, so a `404` there is a routing or deploy fault that retrying would mask.
  /// `408`/`425`/`429`/`5xx` keep their transient `retryableHTTP` classification and the retry
  /// policy's backoff. Once the recreate bound is spent, the standing `410` reaches the caller as
  /// the terminal `ClientError.http(status: 410, ...)` it already was. The bound and its backoff
  /// are `ShapeSubscriptionRecreatePolicy`.
  private func recreatingOnGone(
    operation: String, _ body: @escaping @Sendable () async throws -> ShapeHandle
  ) async throws -> ShapeHandle {
    try await recreatePolicy.recreatingOnGone(
      clock: clock,
      willRecreate: { attempt in await self.noteRecreateAttempt(attempt) },
      { try await self.retrying(operation: operation, body) })
  }

  private func noteRecreateAttempt(_ attempt: Int) {
    transition(
      .waitingToRetry(operation: "recreate", attempt: attempt, delay: recreatePolicy.backoff))
  }

  /// Admission occurs inside the shared `startTask`, before materializer preflight or the create
  /// POST. Consequently concurrent callers join one permit and an over-cap caller cannot create an
  /// unbounded pending-start queue or launch retry/stream work.
  private func createWithAdmissionAndRetry() async throws -> ShapeHandle {
    try await acquireCapacityPermit()
    do {
      return try await createWithRetry()
    } catch {
      await releaseCapacityPermit()
      throw error
    }
  }

  private func acquireCapacityPermit() async throws {
    guard capacityPermit == nil, let capacity else { return }
    let permit = try await capacity.acquire()
    guard isAcceptingOperations, !Task.isCancelled else {
      await capacity.release(permit)
      throw CancellationError()
    }
    capacityPermit = permit
  }

  private func releaseCapacityPermit() async {
    guard let capacity, let permit = capacityPermit else { return }
    // Clear before the await so a joined stop/reseed cannot double-return the opaque permit.
    capacityPermit = nil
    await capacity.release(permit)
  }
  /// A join repeats the create under the same stable claim, so it carries the same recreate
  /// vocabulary. A recreated join answers with a fresh shape id, which is exactly the replacement
  /// the engine's own fall-through produces: `renew()` routes it through `requireReseed`, the same
  /// reconciliation the durable-stream gone receipt uses, so the application reseeds one explicit
  /// fresh scope instead of inheriting two live server claims.
  private func renewWithRetry() async throws -> ShapeHandle {
    try await recreatingOnGone(operation: "renew") {
      switch self.kind {
      case .shape:
        try await self.client.renewShape(self.request)
      case .subsetFeed:
        try await self.client.createSubsetFeed(self.request)
      }
    }
  }

  private func releasePending() async throws {
    // Keep the handle in the durable local retry queue until its DELETE actually succeeds (404 is
    // normalized to success by the client). A failed stop can therefore be retried without losing
    // the only claim identity that authorizes release.
    while let next = pendingRelease.last {
      try await retrying(operation: "release") { try await self.client.releaseShape(next) }
      if let index = pendingRelease.lastIndex(where: {
        $0.id == next.id && $0.subscription == next.subscription
      }) {
        pendingRelease.remove(at: index)
      }
    }
  }

  private func retrying<T>(operation: String, _ body: @escaping @Sendable () async throws -> T)
    async throws -> T
  {
    var retries = 0
    while true {
      try Task.checkCancellation()
      do { return try await body() } catch {
        guard isRetryable(error), retries < retryPolicy.maxRetries else {
          throw exhaustedOrFailure(error, retries: retries, operation: operation)
        }
        retries += 1
        let delay = retryPolicy.delay(forRetry: retries, retryAfter: retryAfter(for: error))
        transition(.waitingToRetry(operation: operation, attempt: retries, delay: delay))
        try await clock.sleep(for: delay)
      }
    }
  }

  private func requireReseed(
    _ outcome: ShapeSubscriptionReseedRequired, extraRelease: ShapeHandle? = nil
  ) async throws {
    generation &+= 1
    leaseTask?.cancel()
    // A renew may already have been accepted with the old claim while the reader learned that its
    // epoch is terminal. Join it before reseed is visible and compensate any returned handle.
    let renewing = renewTask
    renewing?.cancel()
    if let renewing, case .success(let renewed) = await renewing.result {
      appendRelease(renewed)
    }
    renewTask = nil
    if let handle { appendRelease(handle) }
    if let extraRelease { appendRelease(extraRelease) }
    handle = nil
    try await releasePending()
    await releaseCapacityPermit()
    transition(.reseedRequired(outcome))
  }

  private var isAcceptingOperations: Bool { !stopping && stopTask == nil }
  private func isCurrent(_ expected: UInt64) -> Bool {
    isAcceptingOperations && generation == expected
  }
  private func committedCursor() async throws -> StreamCursor {
    try await materializer.currentCursor() ?? .beginning
  }
  private func appendRelease(_ candidate: ShapeHandle) {
    if !pendingRelease.contains(where: {
      $0.id == candidate.id && $0.subscription == candidate.subscription
    }) {
      pendingRelease.append(candidate)
    }
  }
  private func transition(_ next: ShapeSubscriptionState) {
    state = next
    continuation?.yield(next)
  }
  private func exhaustedOrFailure(_ error: Error, retries: Int, operation: String)
    -> ShapeSubscriptionFailure
  {
    if isRetryable(error), retries >= retryPolicy.maxRetries {
      return .retryExhausted(
        operation: operation, attempts: retries + 1, cause: retryCause(for: error))
    }
    return failure(for: error)
  }
  private func isRetryable(_ error: Error) -> Bool {
    if error is CancellationError || error is StreamError || error is MaterializerFailure
      || error is MaterializerAvailabilityError || error is ShapeSubscriptionFailure
    {
      return false
    }
    if case ClientError.retryableHTTP = error {
      return true
    }
    if error is ClientError { return false }
    if let url = error as? URLError {
      return url.code != .cancelled && url.code != .badURL && url.code != .unsupportedURL
    }
    return true
  }
  private func failure(for error: Error) -> ShapeSubscriptionFailure {
    if let failure = error as? ShapeSubscriptionFailure { return failure }
    if let stream = error as? StreamError { return .stream(stream) }
    if let unavailable = error as? MaterializerAvailabilityError {
      return .materializerUnavailable(unavailable)
    }
    if error is MaterializerFailure { return .materializer }
    if let client = error as? ClientError { return .client(client) }
    return .transport(transportFailure(for: error))
  }

  private func retryAfter(for error: Error) -> Duration? {
    guard case ClientError.retryableHTTP(_, let retryAfter) = error else { return nil }
    return retryAfter
  }

  private func retryCause(for error: Error) -> ShapeSubscriptionRetryCause {
    if case ClientError.retryableHTTP(let status, _) = error { return .http(status: status) }
    return .transport(transportFailure(for: error))
  }

  private func transportFailure(for error: Error) -> ShapeSubscriptionTransportFailure {
    if let url = error as? URLError { return .url(code: url.code.rawValue) }
    return .unknown
  }
}
