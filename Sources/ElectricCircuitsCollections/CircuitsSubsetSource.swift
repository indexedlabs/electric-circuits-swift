import ElectricCircuitsSwift
import Foundation

public enum CircuitsSubsetSourceError: Error, Equatable, Sendable {
  case unsupportedOrderCount(Int)
  case unsupportedLimitedLiveDemand(Int)
  /// A limited window could not obtain a page at least as fresh as the tail changes it must
  /// answer within the retry budget; applying an older page would acknowledge those changes
  /// without reflecting them.
  case stalePage(required: CollectionSourceVersion, observed: CollectionSourceVersion)
  case invalidSnapshotRow(index: Int)
  case invalidLiveKey(String)
  case invalidSnapshotSourceVersion(String)
  case invalidLiveSourceVersion(String?)
  case release(ClientError)
  case subscription(ShapeSubscriptionFailure)
}

/// How a subset session is set up.
public enum SubsetSetupRoute: Sendable, Equatable {
  /// One `POST v1/subset-loads` (feed, frontier and snapshot in one round trip). A server without
  /// the route (404 or 405) is remembered and set up with the three requests instead.
  case automatic
  /// Feed create, stream `HEAD`, then snapshot query, as three requests.
  case threeRequests
}

/// Native Electric Circuits source strategy for an on-demand demand: establish the changes-only
/// feed, fence its durable frontier, query the subset snapshot, then consume the feed with awaited
/// store application. A stable materialization ID is also the server subscription claim. The
/// three steps are one round trip where the server offers `v1/subset-loads`, and the session's
/// tail starts on the loaded feed rather than POSTing its claim again.
public struct CircuitsSubsetSource<Model: Sendable, Key: Hashable & Sendable>:
  CollectionSourceAdapter
{
  private let client: ElectricCircuitsClient
  private let transport: any HTTPTransport
  private let table: String
  private let columns: [String]?
  private let decodeRow: @Sendable (ChangeRow) throws -> Model
  private let decodeKey: @Sendable (String) throws -> Key
  private let keyForRow: (@Sendable (Model) throws -> Key)?
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let capacity: ShapeSubscriptionCapacity?
  private let responseDecodingLimits: ResponseDecodingLimits
  private let telemetry: TelemetryReporter
  private let recreatePolicy: ShapeSubscriptionRecreatePolicy
  private let pendingCleanup: PendingSubsetFeedCleanup
  private let setupRoute: SubsetSetupRoute
  private let loadSupport = SubsetLoadSupport()

  public init(
    client: ElectricCircuitsClient,
    transport: any HTTPTransport,
    table: String,
    columns: [String]? = nil,
    retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    capacity: ShapeSubscriptionCapacity? = nil,
    responseDecodingLimits: ResponseDecodingLimits = .default,
    telemetry: TelemetryReporter = .noop,
    recreatePolicy: ShapeSubscriptionRecreatePolicy = .init(),
    setupRoute: SubsetSetupRoute = .automatic,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key,
    keyForRow: (@Sendable (Model) throws -> Key)? = nil
  ) {
    precondition(!table.isEmpty)
    self.client = client
    self.transport = transport
    self.table = table
    self.columns = columns
    self.retryPolicy = retryPolicy
    self.clock = clock
    self.capacity = capacity
    self.responseDecodingLimits = responseDecodingLimits
    self.telemetry = telemetry
    self.recreatePolicy = recreatePolicy
    self.setupRoute = setupRoute
    self.decodeRow = decodeRow
    self.decodeKey = decodeKey
    self.keyForRow = keyForRow
    pendingCleanup = PendingSubsetFeedCleanup(client: client)
  }

  public func materialize(
    _ demand: CollectionDemand<Model>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<Model, Key> {
    // A limited live demand is a window: the snapshot is the ordered page and every accepted tail
    // change re-queries that page so membership follows the source. The window needs exactly one
    // order column to be well defined and a row key to diff the page against the held keys.
    if let limit = demand.limit, demand.order.count != 1 || keyForRow == nil {
      throw CircuitsSubsetSourceError.unsupportedLimitedLiveDemand(limit)
    }
    guard demand.order.count <= 1 else {
      throw CircuitsSubsetSourceError.unsupportedOrderCount(demand.order.count)
    }
    try await pendingCleanup.retry(materializationID)
    let subscription = materializationID.rawValue
    let feedRequest = ShapeRequest(
      table: table,
      where: demand.sourcePredicate,
      columns: columns,
      changesOnly: true,
      subscription: subscription
    )
    let orderBy = demand.order.first.map {
      SubsetOrderBy(column: $0.sourceName, descending: $0.direction == .descending)
    }
    // A window is positioned by its order column, so a row without one cannot be placed in
    // the page: SQL sorts NULL sort keys first under DESC and would fill the page with them.
    // The page therefore requires the sort key while the feed keeps the demand's predicate, so
    // a row that gains its sort key later still arrives on the tail and re-queries the page.
    let pagePredicate: ElectricCircuitsSwift.Predicate? =
      if demand.limit != nil, let orderBy {
        .and(
          [demand.sourcePredicate, .isNull(column: orderBy.column, isNull: false)]
            .compactMap { $0 })
      } else {
        demand.sourcePredicate
      }
    let pageQuery = SubsetQuery(
      table: table,
      where: pagePredicate,
      columns: columns,
      orderBy: orderBy,
      limit: demand.limit
    )
    // One round trip where the server offers it: the load creates or renews the feed, reads its
    // frontier, then queries the page, in the same order as the three requests below. Like the
    // feed create, it carries the recreate vocabulary: a persisted materialization ID joining a
    // dormant shape whose fall-through the engine has exhausted re-POSTs the identical claim.
    let loaded = try await loadIfSupported(
      feedRequest: feedRequest, pageQuery: pageQuery, materializationID: materializationID)
    let feed: ShapeHandle
    if let loaded {
      feed = loaded.handle
    } else {
      feed = try await recreatePolicy.recreatingOnGone(clock: clock) {
        try await client.createSubsetFeed(feedRequest)
      }
    }
    let adopted = AdoptedShapeHandle(handle: feed)

    do {
      let frontier: StreamCursor
      if let loaded {
        frontier = loaded.frontier
      } else {
        frontier = try await client.streamCursor(for: feed)
      }
      let decodeRow = decodeRow
      let decodePage: @Sendable (SubsetResponse) throws -> ([Model], CollectionSourceVersion) = {
        response in
        let rows = try response.rows.enumerated().map { index, value in
          guard case .object(let row) = value else {
            throw CircuitsSubsetSourceError.invalidSnapshotRow(index: index)
          }
          return try decodeRow(row)
        }
        guard let version = Self.pageVersion(response) else {
          throw CircuitsSubsetSourceError.invalidSnapshotSourceVersion(response.lsn)
        }
        return (rows, version)
      }
      let firstPage: SubsetResponse
      if let loaded {
        firstPage = loaded.page
      } else {
        firstPage = try await client.querySubset(pageQuery)
      }
      let (rows, snapshotVersion) = try decodePage(firstPage)
      let window: LimitedWindow<Model, Key>? =
        if demand.limit != nil, let keyForRow {
          LimitedWindow(
            keys: Set(try rows.map(keyForRow)),
            keyForRow: keyForRow,
            fetchPage: { try await client.querySubset(pageQuery) },
            decodePage: decodePage
          )
        } else {
          nil
        }
      let cursor = StreamCursor(offset: frontier.offset, lsn: snapshotVersion.rawValue)
      let lifecycle = CircuitsSubsetSessionLifecycle(client: client, initialHandle: feed)
      let snapshotFence = SnapshotFence(
        rawValue: Self.snapshotFence(
          offset: frontier.offset, sourceVersion: snapshotVersion.rawValue))

      return CollectionSourceSession(
        snapshot: CollectionSnapshot(
          rows: rows, fence: snapshotFence, sourceVersion: snapshotVersion, cursor: cursor),
        run: { apply in
          let materializer = CircuitsCollectionTailMaterializer(
            cursor: cursor,
            sourceVersion: snapshotVersion,
            decodeRow: decodeRow,
            decodeKey: decodeKey,
            window: window,
            retryPolicy: retryPolicy,
            clock: clock,
            apply: apply
          )
          let coordinator = ShapeSubscriptionCoordinator(
            client: client,
            transport: transport,
            request: feedRequest,
            materializer: materializer,
            retryPolicy: retryPolicy,
            clock: clock,
            capacity: capacity,
            responseDecodingLimits: responseDecodingLimits,
            telemetry: telemetry,
            kind: .subsetFeed,
            recreatePolicy: recreatePolicy,
            // The tail starts on the feed setup created rather than POSTing its claim again.
            adopting: adopted
          )
          try await lifecycle.install(coordinator)
          let states = await coordinator.stateUpdates
          do {
            try await withTaskCancellationHandler {
              _ = try await coordinator.start()
              await lifecycle.markCoordinatorStarted()
              for await state in states {
                try Task.checkCancellation()
                switch state {
                case .failed(let failure):
                  if let cause = await materializer.takeTerminalFailure() { throw cause }
                  throw CircuitsSubsetSourceError.subscription(failure)
                case .reseedRequired(let outcome):
                  throw CircuitsSubsetSourceError.subscription(.reseedRequired(outcome))
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
        try await client.releaseShape(feed)
      } catch {
        await pendingCleanup.record(feed, for: materializationID)
        if let release = error as? ClientError {
          throw CircuitsSubsetSourceError.release(release)
        }
        throw error
      }
      throw error
    }
  }

  /// The one-round-trip setup, or nil when the server has no `v1/subset-loads` route (remembered
  /// for this source) or the caller asked for three requests.
  private func loadIfSupported(
    feedRequest: ShapeRequest, pageQuery: SubsetQuery,
    materializationID: CollectionMaterializationID
  ) async throws -> SubsetLoad? {
    guard setupRoute == .automatic, await loadSupport.shouldLoad() else { return nil }
    let request = SubsetLoadRequest(
      table: table,
      where: feedRequest.where,
      columns: columns,
      subscription: feedRequest.subscription,
      query: SubsetLoadPage(
        where: pageQuery.where, orderBy: pageQuery.orderBy, limit: pageQuery.limit))
    do {
      let loaded = try await recreatePolicy.recreatingOnGone(clock: clock) {
        try await client.loadSubset(request)
      }
      await loadSupport.settle(available: true)
      return loaded
    } catch {
      let status = Self.httpStatus(error)
      if let status, status == 404 || status == 405 {
        await loadSupport.settle(available: false)
        return nil
      }
      if let status, (400..<500).contains(status) {
        // A refused request (400, 409, an exhausted 410) leaves no claim to release.
        await loadSupport.settle(available: true)
        throw error
      }
      await loadSupport.settle(available: nil)
      // The server may have created or renewed the claim before the client rejected the response
      // or lost the connection. Keep the claim so this attempt, or the next setup or abandonment
      // cleanup of this materialization, recovers its handle and releases it.
      await pendingCleanup.recordClaim(feedRequest, for: materializationID)
      if !Task.isCancelled { try? await pendingCleanup.retry(materializationID) }
      throw error
    }
  }

  private static func httpStatus(_ error: any Error) -> Int? {
    guard let error = error as? ClientError, case .http(let status, _) = error else { return nil }
    return status
  }

  public func cleanupAbandonedMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    try await pendingCleanup.retry(materializationID)
  }

  private static func snapshotFence(offset: String, sourceVersion: String) -> String {
    ["subset-v1", offset, sourceVersion]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
  }

  fileprivate static func pageVersion(_ response: SubsetResponse) -> CollectionSourceVersion? {
    guard let lsn = postgresLSN(response.lsn) else { return nil }
    return CollectionSourceVersion(
      rawValue: lsn.rawValue, order: lsn.order,
      snapshot: CollectionPageSnapshot(
        lsn: lsn.order, snapshot: response.snapshot, horizon: response.horizon))
  }

  /// PostgreSQL LSNs are two unsigned hexadecimal 32-bit words. A `UInt64` sort key is portable
  /// to Indexed/GRDB stores and avoids trusting lexicographic wire strings.
  fileprivate static func postgresLSN(_ value: String) -> CollectionSourceVersion? {
    let words = value.split(separator: "/", omittingEmptySubsequences: false)
    guard words.count == 2, !words[0].isEmpty, !words[1].isEmpty,
      let high = UInt64(words[0], radix: 16), let low = UInt64(words[1], radix: 16),
      high <= UInt64(UInt32.max), low <= UInt64(UInt32.max)
    else { return nil }
    return CollectionSourceVersion(rawValue: value, order: (high << 32) | low)
  }
}

/// Whether the server offers `v1/subset-loads`, learnt per source. The first load probes;
/// concurrent setups wait for its answer instead of each sending their own probe. A 404 or 405
/// settles it as unavailable for five minutes, so an old instance answering during a rolling
/// deploy doesn't pin the source to three requests; a successful load settles it as available;
/// any other failure leaves it unknown for the next setup to probe again.
private actor SubsetLoadSupport {
  private enum State {
    case unknown
    case probing
    case available
    case unavailable(until: ContinuousClock.Instant)
  }
  private var state = State.unknown
  private var waiters: [CheckedContinuation<Void, Never>] = []

  /// True when this setup should send the load: the route is known, or this setup is the probe.
  func shouldLoad() async -> Bool {
    while true {
      switch state {
      case .available: return true
      case .unavailable(let until):
        if ContinuousClock.now < until { return false }
        state = .probing
        return true
      case .unknown:
        state = .probing
        return true
      case .probing:
        await withCheckedContinuation { waiters.append($0) }
      }
    }
  }

  func settle(available: Bool?) {
    guard case .probing = state else { return }
    state =
      switch available {
      case true?: .available
      case false?: .unavailable(until: .now + .seconds(300))
      case nil: .unknown
      }
    let resumed = waiters
    waiters = []
    for waiter in resumed { waiter.resume() }
  }
}

/// A failed setup has no session to return, but it can already have created a server feed. Keep
/// the one release handle per materialization until the idempotent DELETE succeeds (or receives
/// 404, which the client normalizes to success). The registry is bounded by active failed setup
/// attempts and is drained before another feed with that stable subscription can be created.
///
/// A rejected subset load (an over-limit or malformed 2xx, a dropped connection, a cancellation)
/// can leave a claim the client never received a handle for. Its request is kept instead: the
/// retry repeats the identical feed create, which renews the same claim and returns its handle,
/// then releases that.
private actor PendingSubsetFeedCleanup {
  private enum Pending {
    case handle(ShapeHandle)
    case claim(ShapeRequest)
  }
  private let client: ElectricCircuitsClient
  private var pending: [CollectionMaterializationID: Pending] = [:]

  init(client: ElectricCircuitsClient) {
    self.client = client
  }

  func record(_ handle: ShapeHandle, for materializationID: CollectionMaterializationID) {
    pending[materializationID] = .handle(handle)
  }

  func recordClaim(_ request: ShapeRequest, for materializationID: CollectionMaterializationID) {
    pending[materializationID] = .claim(request)
  }

  func retry(_ materializationID: CollectionMaterializationID) async throws {
    switch pending[materializationID] {
    case nil:
      return
    case .handle(let handle):
      try await client.releaseShape(handle)
    case .claim(let request):
      let handle: ShapeHandle
      do {
        handle = try await client.createSubsetFeed(request)
      } catch ClientError.http(status: let status, message: _) where (400..<500).contains(status) {
        // The server refuses this claim outright, so the load cannot have left one behind.
        pending.removeValue(forKey: materializationID)
        return
      }
      try await client.releaseShape(handle)
    }
    pending.removeValue(forKey: materializationID)
  }
}

private actor CircuitsSubsetSessionLifecycle {
  private let client: ElectricCircuitsClient
  private let initialHandle: ShapeHandle
  private var coordinator: ShapeSubscriptionCoordinator?
  private var coordinatorStarted = false
  private var stopped = false
  private var stopTask: Task<Void, Error>?

  init(client: ElectricCircuitsClient, initialHandle: ShapeHandle) {
    self.client = client
    self.initialHandle = initialHandle
  }

  func install(_ coordinator: ShapeSubscriptionCoordinator) async throws {
    guard !stopped else {
      try await coordinator.stop()
      throw CancellationError()
    }
    self.coordinator = coordinator
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
    let coordinatorStarted = coordinatorStarted
    let client = client
    let initialHandle = initialHandle
    let task = Task<Void, Error> {
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

/// The held page of a limited live demand.
///
/// The feed carries every change under the base predicate, but a bounded page cannot apply them
/// positionally: an update can move a row across the page boundary, an insert can push the last
/// row out, and a delete leaves a slot the feed cannot refill. So the window never applies tail
/// rows directly — it does not even decode them. Each tail batch that carries an accepted change
/// re-queries the page and emits the difference: the new page rows as upserts and the held keys
/// that left the page as deletes, versioned by the page snapshot (or its LSN on an old engine).
/// Only changes already visible to the snapshot are skipped.
///
/// The page must reflect every accepted transaction, including commits below its LSN that were
/// still invisible during the query. Older pages are retried, never applied. Membership is committed
/// only after the store has applied the diff, so a failed application leaves the held keys unchanged.
private struct LimitedWindow<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  var keys: Set<Key>
  let keyForRow: @Sendable (Model) throws -> Key
  /// The page fetch alone, so the retry loop never re-fetches a page whose only problem is that
  /// the model cannot decode it.
  let fetchPage: @Sendable () async throws -> SubsetResponse
  let decodePage: @Sendable (SubsetResponse) throws -> ([Model], CollectionSourceVersion)
}

private actor CircuitsCollectionTailMaterializer<Model: Sendable, Key: Hashable & Sendable>:
  ShapeMaterializer
{
  private var cursor: StreamCursor?
  private var snapshotSourceVersion: CollectionSourceVersion
  private var passedSnapshotHorizon = false
  private var rowVersions: [String: CollectionSourceVersion] = [:]
  // Actors are reentrant across page fetch and store application. Hold this FIFO permit across
  // both awaits so a second application cannot fetch/commit against the same window or cursor.
  private var applying = false
  private var applicationWaiters: [CheckedContinuation<Void, Never>] = []
  private var sourceVersion: CollectionSourceVersion
  private let decodeRow: @Sendable (ChangeRow) throws -> Model
  private let decodeKey: @Sendable (String) throws -> Key
  private var window: LimitedWindow<Model, Key>?
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let applyBatch: @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
  /// The window error that ended this materializer. The subscription coordinator reports any
  /// materializer error as `.materializer`, so the source keeps the typed cause here and rethrows
  /// it from the session instead of the coordinator's summary.
  private var terminalFailure: (any Error)?

  func takeTerminalFailure() -> (any Error)? {
    defer { terminalFailure = nil }
    return terminalFailure
  }

  init(
    cursor: StreamCursor,
    sourceVersion: CollectionSourceVersion,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key,
    window: LimitedWindow<Model, Key>? = nil,
    retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
  ) {
    self.cursor = cursor
    snapshotSourceVersion = sourceVersion
    self.sourceVersion = sourceVersion
    self.decodeRow = decodeRow
    self.decodeKey = decodeKey
    self.window = window
    self.retryPolicy = retryPolicy
    self.clock = clock
    applyBatch = apply
  }

  /// A page that reflects every accepted change, or the LSN floor on an older engine. Transient
  /// failures and pages still behind the tail are retried on the subscription's policy; cancellation,
  /// decoding and
  /// non-retryable client errors surface at once, and exhausting the budget on a stale page
  /// throws `stalePage` rather than acknowledging changes the page does not reflect.
  private func freshPage(
    _ window: LimitedWindow<Model, Key>, notBefore: CollectionSourceVersion,
    reflecting changes: [CollectionSourceVersion]
  ) async throws -> ([Model], CollectionSourceVersion) {
    var retries = 0
    while true {
      try Task.checkCancellation()
      let response: SubsetResponse
      do {
        response = try await window.fetchPage()
      } catch {
        guard Self.isRetryablePageFailure(error), retries < retryPolicy.maxRetries else {
          throw error
        }
        retries += 1
        try await clock.sleep(
          for: retryPolicy.delay(forRetry: retries, retryAfter: Self.retryAfter(for: error)))
        continue
      }
      guard let version = CircuitsSubsetSource<Model, Key>.pageVersion(response) else {
        throw CircuitsSubsetSourceError.invalidSnapshotSourceVersion(response.lsn)
      }
      // Decoding sits outside the retry path: a page the model rejects is terminal, and
      // fetching it again could not change that.
      let fresh =
        version.snapshot.map { snapshot in
          changes.allSatisfy { snapshot.includes(lsn: $0.order, transactionID: $0.transactionID) }
        } ?? (version >= notBefore)
      if fresh { return try window.decodePage(response) }
      let stale = version
      guard retries < retryPolicy.maxRetries else {
        throw CircuitsSubsetSourceError.stalePage(required: notBefore, observed: stale)
      }
      retries += 1
      try await clock.sleep(for: retryPolicy.delay(forRetry: retries))
    }
  }

  private static func retryAfter(for error: any Error) -> Duration? {
    guard case ClientError.retryableHTTP(_, let retryAfter) = error else { return nil }
    return retryAfter
  }

  private static func isRetryablePageFailure(_ error: any Error) -> Bool {
    if error is CancellationError { return false }
    if case ClientError.retryableHTTP = error { return true }
    if error is ClientError || error is CircuitsSubsetSourceError { return false }
    if let url = error as? URLError {
      return url.code != .cancelled && url.code != .badURL && url.code != .unsupportedURL
    }
    return true
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
    defer {
      if applicationWaiters.isEmpty {
        applying = false
      } else {
        applicationWaiters.removeFirst().resume()
      }
    }
    try Task.checkCancellation()
    if cursor == nextCursor { return }
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor,
        actual: cursor,
        advancingTo: nextCursor
      )
    }
    var changes: [CollectionChange<Model, Key>] = []
    var acceptedVersions: [CollectionSourceVersion] = []
    var nextRowVersions = rowVersions
    var passedHorizon = passedSnapshotHorizon
    var latest = sourceVersion
    for envelope in batch.envelopes {
      guard let rawVersion = envelope.headers.lsn,
        let lsn = CircuitsSubsetSource<Model, Key>.postgresLSN(rawVersion)
      else {
        throw CircuitsSubsetSourceError.invalidLiveSourceVersion(envelope.headers.lsn)
      }
      let version = CollectionSourceVersion(
        rawValue: lsn.rawValue, order: lsn.order,
        transactionID: envelope.headers.txid.flatMap(CollectionPageSnapshot.xid32))
      if let snapshot = snapshotSourceVersion.snapshot, version.order >= snapshot.horizon {
        passedHorizon = true
      }
      if let rowVersion = nextRowVersions[envelope.key] {
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
      acceptedVersions.append(version)
      // A window never applies tail rows, so it does not decode them either: the feed covers the
      // whole predicate, including rows the page excludes (NULL sort keys among them), and a
      // model decoder is entitled to reject those. The page is the only source of window rows.
      guard window == nil else { continue }
      if snapshotSourceVersion.snapshot != nil || passedHorizon {
        nextRowVersions[envelope.key] = version
      }
      switch envelope.headers.operation {
      case .delete:
        do { changes.append(.delete(try decodeKey(envelope.key), sourceVersion: version)) } catch {
          throw CircuitsSubsetSourceError.invalidLiveKey(envelope.key)
        }
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        changes.append(.upsert(try decodeRow(value), sourceVersion: version))
      }
    }
    var committedWindowKeys: Set<Key>?
    var committedPageVersion: CollectionSourceVersion?
    if let window, !acceptedVersions.isEmpty {
      let (rows, pageVersion): ([Model], CollectionSourceVersion)
      do {
        (rows, pageVersion) = try await freshPage(
          window, notBefore: latest, reflecting: acceptedVersions)
      } catch {
        if !(error is CancellationError) { terminalFailure = error }
        throw error
      }
      latest = max(latest, pageVersion)
      committedPageVersion = pageVersion
      var pageKeys = Set<Key>()
      pageKeys.reserveCapacity(rows.count)
      changes = try rows.map { row in
        pageKeys.insert(try window.keyForRow(row))
        return .upsert(row, sourceVersion: pageVersion)
      }
      for key in window.keys.subtracting(pageKeys) {
        changes.append(.delete(key, sourceVersion: pageVersion))
      }
      committedWindowKeys = pageKeys
    }
    try await applyBatch(
      CollectionChangeBatch(
        changes: changes,
        expectedCursor: expectedCursor,
        cursor: StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue),
        sourceVersion: latest
      ))
    // Window membership, cursor and source version move together, only once the store has
    // applied the diff: a failed application leaves the held keys describing the store's rows.
    if let committedWindowKeys {
      window?.keys = committedWindowKeys
      if let committedPageVersion {
        snapshotSourceVersion = committedPageVersion
        passedHorizon = false
      }
    }
    cursor = StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue)
    sourceVersion = latest
    rowVersions = nextRowVersions
    passedSnapshotHorizon = committedPageVersion == nil ? passedHorizon : false
    if passedSnapshotHorizon {
      // Release the gate's xip; persisted canonical row versions retain their own visibility.
      snapshotSourceVersion = CollectionSourceVersion(
        rawValue: snapshotSourceVersion.rawValue, order: snapshotSourceVersion.order)
    }
  }
}
