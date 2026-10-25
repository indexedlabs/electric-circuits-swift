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

/// Native Electric Circuits source strategy for an on-demand demand: establish the changes-only
/// feed, fence its durable frontier, query the subset snapshot, then consume the feed with awaited
/// store application. A stable materialization ID is also the server subscription claim.
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
    // The initial feed create is the same native route the coordinator later joins, so it carries
    // the same recreate vocabulary. A persisted materialization ID joining a dormant shape whose
    // fall-through the engine has exhausted must re-POST the identical claim, not fail setup
    // before the frontier HEAD and snapshot query.
    let feed = try await recreatePolicy.recreatingOnGone(clock: clock) {
      try await client.createSubsetFeed(feedRequest)
    }

    do {
      let frontier = try await client.streamCursor(for: feed)
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
      let (rows, snapshotVersion) = try decodePage(try await client.querySubset(pageQuery))
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
            recreatePolicy: recreatePolicy
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

  public func cleanupAbandonedMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    try await pendingCleanup.retry(materializationID)
  }

  /// Builds the additive keyed-window mode for this source. The returned controller is intended
  /// for a host that mounts and releases a set of secondary keys independently of the collection
  /// coordinator's exact-demand leases. It shares this source's decoder, transport and retry
  /// policy, and therefore uses the same native subset-feed contract as `materialize`.
  public func makeKeyedWindow(
    keyColumn: String,
    basePredicate: ElectricCircuitsSwift.Predicate? = nil,
    secondaryKeyForRow: @escaping @Sendable (Model) throws -> JSONValue,
    onChanges: (@Sendable ([CollectionChange<Model, Key>]) async throws -> Void)? = nil
  ) -> KeyedWindow<Model, Key> {
    let primaryKeyForRow: @Sendable (Model) throws -> Key = keyForRow ?? { _ in
      // A keyed window must be able to associate a page row with the opaque primary key in the
      // stream. Callers using this convenience method therefore need to provide `keyForRow`.
      throw CircuitsSubsetSourceError.invalidLiveKey("keyForRow is required for KeyedWindow")
    }
    KeyedWindow(
      client: client,
      transport: transport,
      table: table,
      columns: columns,
      basePredicate: basePredicate,
      keyColumn: keyColumn,
      decodeRow: decodeRow,
      decodePrimaryKey: decodeKey,
      primaryKeyForRow: primaryKeyForRow,
      secondaryKeyForRow: secondaryKeyForRow,
      retryPolicy: retryPolicy,
      clock: clock,
      recreatePolicy: recreatePolicy,
      onChanges: onChanges
    )
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

/// A failed setup has no session to return, but it can already have created a server feed. Keep
/// the one release handle per materialization until the idempotent DELETE succeeds (or receives
/// 404, which the client normalizes to success). The registry is bounded by active failed setup
/// attempts and is drained before another feed with that stable subscription can be created.
private actor PendingSubsetFeedCleanup {
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

/// Additive, explicitly claimed keyed pages over one changes-only feed.
///
/// `LimitedWindow` follows a moving ordered page. A `KeyedWindow` instead keeps the keys a host has
/// claimed and fetches those keys in one-shot pages. Pages never replace store membership: callers
/// release claims explicitly, and an omitted key on a restart is released by the window itself.
/// The controller is deliberately small and host-facing so the keyed-window design can be tested
/// before it is threaded through `CollectionCoordinator`.
public actor KeyedWindow<Model: Sendable, PrimaryKey: Hashable & Sendable>:
  Sendable, ShapeMaterializer
{
  public static let pageSize = 100

  public enum State: Equatable, Sendable {
    case idle
    case live
    case ended
  }

  public struct DebugState: Equatable, Sendable {
    public let state: State
    public let claimedKeyCount: Int
    public let heldPrimaryKeyCount: Int
    public let inFlightPageCount: Int
    public let feedCreated: Bool

    public init(
      state: State, claimedKeyCount: Int, heldPrimaryKeyCount: Int,
      inFlightPageCount: Int, feedCreated: Bool
    ) {
      self.state = state
      self.claimedKeyCount = claimedKeyCount
      self.heldPrimaryKeyCount = heldPrimaryKeyCount
      self.inFlightPageCount = inFlightPageCount
      self.feedCreated = feedCreated
    }
  }

  private struct PageResult: Sendable {
    let rows: [(model: Model, primary: PrimaryKey, secondary: JSONValue, token: String)]
    let version: CollectionSourceVersion
  }

  private struct InFlightPage {
    let id: UUID
    let keys: [JSONValue]
    let tokens: Set<String>
    let task: Task<SubsetResponse, Error>
  }

  private let client: ElectricCircuitsClient
  private let transport: any HTTPTransport
  private let table: String
  private let columns: [String]?
  private let basePredicate: ElectricCircuitsSwift.Predicate?
  private let keyColumn: String
  private let decodeRow: @Sendable (ChangeRow) throws -> Model
  private let decodePrimaryKey: @Sendable (String) throws -> PrimaryKey
  private let primaryKeyForRow: @Sendable (Model) throws -> PrimaryKey
  private let secondaryKeyForRow: @Sendable (Model) throws -> JSONValue
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let recreatePolicy: ShapeSubscriptionRecreatePolicy
  private let onChanges: (@Sendable ([CollectionChange<Model, PrimaryKey>]) async throws -> Void)?

  private var state: State = .idle
  private var feed: ShapeHandle?
  private var frontier: StreamCursor?
  private var claims: [String: JSONValue] = [:]
  private var rows: [PrimaryKey: Model] = [:]
  private var secondaryByPrimary: [PrimaryKey: String] = [:]
  private var primariesBySecondary: [String: Set<PrimaryKey>] = [:]
  /// The version map includes delete tombstones. It is intentionally retained while a page is in
  /// flight: a page snapshot that predates a tail change must never overwrite that change.
  private var primaryVersions: [PrimaryKey: CollectionSourceVersion] = [:]
  private var inFlight: [UUID: InFlightPage] = [:]
  private var pageForToken: [String: UUID] = [:]
  private var applying = false
  private var applicationWaiters: [CheckedContinuation<Void, Never>] = []

  public init(
    client: ElectricCircuitsClient,
    transport: any HTTPTransport,
    table: String,
    columns: [String]? = nil,
    basePredicate: ElectricCircuitsSwift.Predicate? = nil,
    keyColumn: String,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodePrimaryKey: @escaping @Sendable (String) throws -> PrimaryKey,
    primaryKeyForRow: @escaping @Sendable (Model) throws -> PrimaryKey,
    secondaryKeyForRow: @escaping @Sendable (Model) throws -> JSONValue,
    retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    recreatePolicy: ShapeSubscriptionRecreatePolicy = .init(),
    onChanges: (@Sendable ([CollectionChange<Model, PrimaryKey>]) async throws -> Void)? = nil
  ) {
    precondition(!table.isEmpty && !keyColumn.isEmpty)
    self.client = client
    self.transport = transport
    self.table = table
    self.columns = columns
    self.basePredicate = basePredicate
    self.keyColumn = keyColumn
    self.decodeRow = decodeRow
    self.decodePrimaryKey = decodePrimaryKey
    self.primaryKeyForRow = primaryKeyForRow
    self.secondaryKeyForRow = secondaryKeyForRow
    self.retryPolicy = retryPolicy
    self.clock = clock
    self.recreatePolicy = recreatePolicy
    self.onChanges = onChanges
  }

  public func debugState() -> DebugState {
    DebugState(
      state: state, claimedKeyCount: claims.count, heldPrimaryKeyCount: rows.count,
      inFlightPageCount: inFlight.count, feedCreated: feed != nil)
  }

  public func currentCursor() async throws -> StreamCursor? { frontier }

  /// Polls the one feed until cancellation or a terminal authority response. A 410/closed stream
  /// retires the current feed but preserves claims, allowing `restart()` to re-page them.
  public func run() async throws {
    guard let feed, let frontier else { throw KeyedWindowError.notLive }
    let reader = ShapeStreamReader(
      streamURL: feed.stream.url, transport: transport, materializer: self,
      startingAt: frontier)
    do {
      try await reader.run()
    } catch is CancellationError {
      throw CancellationError()
    } catch {
      try? await end()
      throw error
    }
  }

  public func apply(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    guard frontier == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: frontier, advancingTo: cursor)
    }
    _ = try await applyTail(batch)
    frontier = cursor
  }

  /// Creates the single changes-only feed and fences its HEAD before any page query is issued.
  /// The controller does not start a poller itself; a host can hand stream batches to `applyTail`,
  /// which keeps this prototype independent of an app's coordinator and storage transaction.
  public func start(subscription: String = UUID().uuidString) async throws {
    guard state != .live else { return }
    let request = ShapeRequest(
      table: table, where: basePredicate, columns: columns, changesOnly: true,
      subscription: subscription)
    let handle = try await recreatePolicy.recreatingOnGone(clock: clock) {
      try await client.createSubsetFeed(request)
    }
    do {
      frontier = try await client.streamCursor(for: handle)
      feed = handle
      state = .live
    } catch {
      try? await client.releaseShape(handle)
      throw error
    }
  }

  /// Reopens a terminated authority and re-pages exactly the claims that remain mounted. A key
  /// absent from a replacement page loses its claim and any old row for that key is deleted from
  /// the emitted additive diff.
  public func restart(subscription: String = UUID().uuidString) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    let retainedClaims = Array(claims.values)
    let oldRows = rows
    try await stop()
    try await start(subscription: subscription)
    guard !retainedClaims.isEmpty else { return [] }

    // Rebuild the local page state from the new authority. Claims remain until each page has
    // answered, so a cancellation or failed restart does not silently release mounted keys.
    rows.removeAll(keepingCapacity: true)
    secondaryByPrimary.removeAll(keepingCapacity: true)
    primariesBySecondary.removeAll(keepingCapacity: true)
    let oldVersions = primaryVersions
    primaryVersions.removeAll(keepingCapacity: true)
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    var returned = Set<String>()
    for chunk in chunks(retainedClaims) {
      let result = try await fetchPage(chunk)
      let requested = Set(try chunk.map { try token(for: $0) })
      for row in result.rows where requested.contains(row.token) {
        returned.insert(row.token)
        install(row, version: result.version)
        changes.append(.upsert(row.model, sourceVersion: result.version))
      }
    }
    for token in claims.keys where !returned.contains(token) {
      claims.removeValue(forKey: token)
    }
    for (primary, oldRow) in oldRows {
      if rows[primary] == nil {
        // The page omission is a membership release. The row's version is retained by the
        // materializer that consumes this diff; the window has no newer source version to invent.
        let version = oldVersions[primary]
          ?? .init(rawValue: "restart", order: 0)
        changes.append(.delete(primary, sourceVersion: version))
        _ = oldRow
      }
    }
    try await emit(changes)
    return changes
  }

  /// Adds secondary keys. At most 100 values appear in each query. Existing claims return their
  /// currently held rows immediately, and a key already covered by an in-flight page joins that
  /// page rather than creating a second query.
  @discardableResult
  public func addKeys(_ keys: [JSONValue]) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    guard state == .live else { throw KeyedWindowError.notLive }
    let unique = uniqueValues(keys)
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    var toFetch: [JSONValue] = []
    var addedTokens = Set<String>()
    do {
      for key in unique {
        let token = try token(for: key)
        if let primaryKeys = primariesBySecondary[token] {
          for primary in primaryKeys {
            if let model = rows[primary], let version = primaryVersions[primary] {
              changes.append(.upsert(model, sourceVersion: version))
            }
          }
        } else if let pageID = pageForToken[token], let page = inFlight[pageID] {
          // The owner commits this page. A joiner only reads the resulting held rows; it never
          // starts a second query for the same key.
          _ = try await page.task.value
          if let primaryKeys = primariesBySecondary[token] {
            for primary in primaryKeys {
              if let model = rows[primary], let version = primaryVersions[primary] {
                changes.append(.upsert(model, sourceVersion: version))
              }
            }
          }
        } else if claims[token] == nil {
          claims[token] = key
          addedTokens.insert(token)
          toFetch.append(key)
        }
      }
      for chunk in chunks(toFetch) {
        let result = try await fetchPageAndCommit(chunk)
        changes.append(contentsOf: result)
      }
      try await emit(changes)
      return changes
    } catch {
      if error is CancellationError || Task.isCancelled {
        for token in addedTokens { claims.removeValue(forKey: token) }
      }
      throw error
    }
  }

  /// Releases claims without replacing the rows in the canonical store. A later restart will page
  /// only the keys still claimed by the host.
  @discardableResult
  public func releaseKeys(_ keys: [JSONValue]) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    for key in uniqueValues(keys) {
      let token = try token(for: key)
      claims.removeValue(forKey: token)
      for primary in primariesBySecondary[token] ?? [] {
        if rows.removeValue(forKey: primary) != nil {
          removeReverse(primary: primary)
          changes.append(
            .delete(primary, sourceVersion: primaryVersions[primary] ?? .init(rawValue: "release", order: 0)))
        }
      }
    }
    try await emit(changes)
    return changes
  }

  /// Applies a durable-stream batch. Changes for primary keys outside the claimed secondary-key
  /// set are skipped before model decoding. For an update that moves a foreign key into the set,
  /// the raw key column is inspected first and the model is decoded only when it enters a claim.
  @discardableResult
  public func applyTail(_ batch: ChangeBatch) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    guard state == .live else { throw KeyedWindowError.notLive }
    await acquireApplicationPermit()
    defer { releaseApplicationPermit() }
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    for envelope in batch.envelopes {
      guard let lsn = envelope.headers.lsn,
        let parsed = CircuitsSubsetSource<Model, PrimaryKey>.postgresLSN(lsn)
      else { throw CircuitsSubsetSourceError.invalidLiveSourceVersion(envelope.headers.lsn) }
      let version = CollectionSourceVersion(
        rawValue: parsed.rawValue, order: parsed.order,
        transactionID: envelope.headers.txid.flatMap(CollectionPageSnapshot.xid32))
      let primary = try decodePrimaryKey(envelope.key)
      if let previous = primaryVersions[primary], !version.supersedes(previous) { continue }

      let existingSecondary = secondaryByPrimary[primary]
      if envelope.headers.operation == .delete {
        // A delete carries only the primary key. While a page is in flight there is no reverse
        // map yet, so retain the tombstone anyway; a matching page row will be rejected during
        // reconciliation. Outside a page, an unheld delete is ignored without decoding.
        if existingSecondary == nil && inFlight.isEmpty { continue }
        primaryVersions[primary] = version
        if rows.removeValue(forKey: primary) != nil {
          removeReverse(primary: primary)
          changes.append(.delete(primary, sourceVersion: version))
        }
        continue
      }
      let valueSecondary = try rawSecondaryToken(from: envelope.value ?? envelope.old)
      let claimedSecondary = valueSecondary.map { claims[$0] != nil } ?? false
      let held = existingSecondary != nil || claimedSecondary
      guard held else { continue }

      primaryVersions[primary] = version
      switch envelope.headers.operation {
      case .delete:
        // Deletes are handled before secondary-key inspection because the envelope may carry no
        // value or old row at all.
        break
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        let model = try decodeRow(value)
        let secondary = try secondaryKeyForRow(model)
        let token = try token(for: secondary)
        guard claims[token] != nil else {
          if rows.removeValue(forKey: primary) != nil {
            removeReverse(primary: primary)
            changes.append(.delete(primary, sourceVersion: version))
          }
          continue
        }
        if existingSecondary != nil, existingSecondary != token {
          removeReverse(primary: primary)
          changes.append(.delete(primary, sourceVersion: version))
        }
        install(model: model, primary: primary, secondary: secondary, version: version)
        changes.append(.upsert(model, sourceVersion: version))
      }
    }
    try await emit(changes)
    return changes
  }

  /// Marks a 410/closed stream as ended while retaining claims for `restart`.
  public func end() async throws {
    guard state != .idle else { return }
    if let feed { try await client.releaseShape(feed) }
    feed = nil
    state = .ended
  }

  public func stop() async throws {
    for page in inFlight.values { page.task.cancel() }
    inFlight.removeAll()
    pageForToken.removeAll()
    if let feed { try await client.releaseShape(feed) }
    feed = nil
    frontier = nil
    state = .idle
  }

  private func fetchPageAndCommit(_ keys: [JSONValue]) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    let pageID = UUID()
    let tokens = Set(try keys.map { try token(for: $0) })
    let query = pageQuery(for: keys)
    let client = client
    let task = Task { try await client.querySubset(query) }
    let page = InFlightPage(id: pageID, keys: keys, tokens: tokens, task: task)
    inFlight[pageID] = page
    for token in tokens { pageForToken[token] = pageID }
    defer {
      inFlight.removeValue(forKey: pageID)
      for token in tokens where pageForToken[token] == pageID { pageForToken.removeValue(forKey: token) }
    }
    do {
      try Task.checkCancellation()
      let response: SubsetResponse
      do {
        response = try await task.value
      } catch {
        task.cancel()
        throw error
      }
      let result = try decodePage(response)
      var changes: [CollectionChange<Model, PrimaryKey>] = []
      var returnedTokens = Set<String>()
      for row in result.rows where tokens.contains(row.token) {
        returnedTokens.insert(row.token)
        // A tail may have passed this primary key while the page was in flight. Its version map
        // also stores delete tombstones, so a stale page row is ignored instead of resurrecting it.
        if let tailVersion = primaryVersions[row.primary], !result.version.supersedes(tailVersion) {
          continue
        }
        claims[row.token] = row.secondary
        install(row, version: result.version)
        changes.append(.upsert(row.model, sourceVersion: result.version))
      }
      // A missing row is not a durable claim. This is especially important for a first page:
      // otherwise an id that was absent at mount would unexpectedly be re-paged forever.
      for token in tokens where !returnedTokens.contains(token) {
        claims.removeValue(forKey: token)
      }
      return changes
    } catch is CancellationError {
      for token in tokens { claims.removeValue(forKey: token) }
      throw CancellationError()
    } catch {
      for token in tokens { claims.removeValue(forKey: token) }
      throw error
    }
  }

  private func fetchPage(_ keys: [JSONValue]) async throws -> PageResult {
    let response = try await client.querySubset(pageQuery(for: keys))
    return try decodePage(response)
  }

  private func decodePage(_ response: SubsetResponse) throws -> PageResult {
    guard let version = CircuitsSubsetSource<Model, PrimaryKey>.pageVersion(response) else {
      throw CircuitsSubsetSourceError.invalidSnapshotSourceVersion(response.lsn)
    }
    var decoded: [(model: Model, primary: PrimaryKey, secondary: JSONValue, token: String)] = []
    decoded.reserveCapacity(response.rows.count)
    for (index, value) in response.rows.enumerated() {
      guard case .object(let row) = value else {
        throw CircuitsSubsetSourceError.invalidSnapshotRow(index: index)
      }
      let model = try decodeRow(row)
      let primary = try primaryKeyForRow(model)
      let secondary = try secondaryKeyForRow(model)
      decoded.append((model, primary, secondary, try token(for: secondary)))
    }
    return PageResult(rows: decoded, version: version)
  }

  private func pageQuery(for keys: [JSONValue]) -> SubsetQuery {
    let leaves = keys.map { .leaf(column: keyColumn, op: .eq, value: $0) }
    let keyPredicate: ElectricCircuitsSwift.Predicate = .or(leaves)
    let predicate: ElectricCircuitsSwift.Predicate? =
      if let basePredicate { .and([basePredicate, keyPredicate]) } else { keyPredicate }
    return SubsetQuery(table: table, where: predicate, columns: columns)
  }

  private func chunks(_ values: [JSONValue]) -> [[JSONValue]] {
    stride(from: 0, to: values.count, by: Self.pageSize).map {
      Array(values[$0..<min($0 + Self.pageSize, values.count)])
    }
  }

  private func uniqueValues(_ values: [JSONValue]) -> [JSONValue] {
    var seen = Set<String>()
    return values.compactMap { value in
      guard let key = try? token(for: value), seen.insert(key).inserted else { return nil }
      return value
    }
  }

  private func token(for value: JSONValue) throws -> String {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return String(decoding: try encoder.encode(value), as: UTF8.self)
  }

  private func rawSecondaryToken(from row: ChangeRow?) throws -> String? {
    guard let row, let value = row[keyColumn] else { return nil }
    return try token(for: value)
  }

  private func install(
    _ row: (model: Model, primary: PrimaryKey, secondary: JSONValue, token: String),
    version: CollectionSourceVersion
  ) {
    install(model: row.model, primary: row.primary, secondary: row.secondary, version: version)
  }

  private func install(
    model: Model, primary: PrimaryKey, secondary: JSONValue, version: CollectionSourceVersion
  ) {
    if secondaryByPrimary[primary] != nil { removeReverse(primary: primary) }
    rows[primary] = model
    let token = try! token(for: secondary)
    secondaryByPrimary[primary] = token
    primariesBySecondary[token, default: []].insert(primary)
    primaryVersions[primary] = version
  }

  private func removeReverse(primary: PrimaryKey) {
    guard let token = secondaryByPrimary.removeValue(forKey: primary) else { return }
    primariesBySecondary[token]?.remove(primary)
    if primariesBySecondary[token]?.isEmpty == true { primariesBySecondary.removeValue(forKey: token) }
  }

  private func emit(_ changes: [CollectionChange<Model, PrimaryKey>]) async throws {
    guard !changes.isEmpty, let onChanges else { return }
    try await onChanges(changes)
  }

  private func acquireApplicationPermit() async {
    if applying {
      await withCheckedContinuation { applicationWaiters.append($0) }
    } else {
      applying = true
    }
  }

  private func releaseApplicationPermit() {
    if applicationWaiters.isEmpty {
      applying = false
    } else {
      applicationWaiters.removeFirst().resume()
    }
  }
}

public enum KeyedWindowError: Error, Equatable, Sendable {
  case notLive
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
