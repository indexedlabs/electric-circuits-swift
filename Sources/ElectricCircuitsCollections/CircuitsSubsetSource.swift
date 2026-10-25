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
    return KeyedWindow(
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
/// release claims explicitly, and a membership release is reported separately from a source delete.
public actor KeyedWindow<Model: Sendable, PrimaryKey: Hashable & Sendable>:
  Sendable, ShapeMaterializer
{
  public static var pageSize: Int { 100 }

  public enum State: Equatable, Sendable { case idle, live, ended }

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

  private struct Claim {
    let value: JSONValue
    var holders: Set<String>
  }

  private struct PageResult: Sendable {
    let rows: [(raw: ChangeRow, model: Model, primary: PrimaryKey, secondary: JSONValue, token: String)]
    let version: CollectionSourceVersion
  }

  private struct PageDraft: Sendable {
    let tokens: Set<String>
    let result: PageResult
    let completion: KeyedPageCompletion<[CollectionChange<Model, PrimaryKey>]>
  }

  private struct InFlightPage {
    let id: UUID
    let tokens: Set<String>
    let completion: KeyedPageCompletion<[CollectionChange<Model, PrimaryKey>]>
    let task: Task<SubsetResponse, Error>
  }

  private struct Snapshot {
    var claims: [String: Claim]
    var primaryToSecondary: [PrimaryKey: String]
    var secondaryToPrimaries: [String: Set<PrimaryKey>]
    var versions: [PrimaryKey: CollectionSourceVersion]
    var rawRows: [PrimaryKey: ChangeRow]
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
  private let onMembershipRelease: (@Sendable ([PrimaryKey]) async throws -> Void)?
  private let defaultHolder = "default"

  private var state: State = .idle
  private var feed: ShapeHandle?
  private var frontier: StreamCursor?
  private var claims: [String: Claim] = [:]
  private var primaryToSecondary: [PrimaryKey: String] = [:]
  private var secondaryToPrimaries: [String: Set<PrimaryKey>] = [:]
  private var versions: [PrimaryKey: CollectionSourceVersion] = [:]
  private var rawRows: [PrimaryKey: ChangeRow] = [:]
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
    onChanges: (@Sendable ([CollectionChange<Model, PrimaryKey>]) async throws -> Void)? = nil,
    onMembershipRelease: (@Sendable ([PrimaryKey]) async throws -> Void)? = nil
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
    self.onMembershipRelease = onMembershipRelease
  }

  public func debugState() -> DebugState {
    DebugState(
      state: state, claimedKeyCount: claims.values.reduce(0) { $0 + $1.holders.count },
      heldPrimaryKeyCount: primaryToSecondary.count, inFlightPageCount: inFlight.count,
      feedCreated: feed != nil)
  }

  public func currentCursor() async throws -> StreamCursor? { frontier }

  public static func compositePrimaryKey(_ components: [String]) -> String {
    components.enumerated().map { index, component in
      let escaped = component.reduce(into: "") { output, character in
        switch character {
        case "\\": output.append("\\\\")
        case "\u{1f}": output.append("\\x1f")
        default: output.append(character)
        }
      }
      return index == 0 ? escaped : "\u{1f}\(escaped)"
    }.joined()
  }

  /// The registry/coordinator integration is kept at the feed boundary: the controller is a
  /// `ShapeMaterializer`, so a host can hand it to `ShapeSubscriptionCoordinator` and get renewal,
  /// retry, recreate-on-410 and telemetry from the SDK. `run()` remains a small compatibility
  /// helper for the prototype's scripted transport tests.
  public func run() async throws {
    guard let feed, let frontier else { throw KeyedWindowError.notLive }
    let reader = ShapeStreamReader(
      streamURL: feed.stream.url, transport: transport, materializer: self,
      startingAt: frontier)
    try await reader.run()
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

  public func restart(subscription: String = UUID().uuidString)
    async throws -> [CollectionChange<Model, PrimaryKey>]
  {
    let retained = claims
    try await stop()
    try await start(subscription: subscription)
    guard !retained.isEmpty else { return [] }
    var drafts: [PageResult] = []
    for chunk in chunks(retained.values.map(\.value)) {
      drafts.append(try await fetchPage(chunk))
    }
    await acquireApplicationPermit()
    defer { releaseApplicationPermit() }
    var next = Snapshot(
      claims: retained, primaryToSecondary: [:], secondaryToPrimaries: [:], versions: [:], rawRows: [:])
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    for result in drafts {
      for row in result.rows where claimIsActive(row.token, in: retained) {
        nextInstall(row, version: result.version, into: &next)
        changes.append(.upsert(row.model, sourceVersion: result.version))
      }
    }
    let released = Set(primaryToSecondary.keys).subtracting(next.primaryToSecondary.keys)
    try await emit(changes, membershipReleases: Array(released))
    commit(next)
    return changes
  }

  @discardableResult
  public func addKeys(_ keys: [JSONValue]) async throws -> [CollectionChange<Model, PrimaryKey>] {
    try await addKeys(keys, holder: defaultHolder)
  }

  @discardableResult
  public func addKeys(_ keys: [JSONValue], holder: String) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    guard state == .live else { throw KeyedWindowError.notLive }
    let unique = uniqueValues(keys)
    var ownedTokens = Set<String>()
    var joiners: [KeyedPageCompletion<[CollectionChange<Model, PrimaryKey>]>] = []
    var pages: [PageDraft] = []
    do {
      for key in unique {
        let token = try canonicalToken(key)
        if var claim = claims[token] {
          claim.holders.insert(holder)
          claims[token] = claim
          if let pageID = pageForToken[token], let page = inFlight[pageID] {
            joiners.append(page.completion)
          }
        } else {
          claims[token] = Claim(value: key, holders: [holder])
          ownedTokens.insert(token)
        }
      }
      let pending = ownedTokens.map { claims[$0]!.value }
      for chunk in chunks(pending) {
        pages.append(try await fetchPageDraft(chunk))
      }
      await acquireApplicationPermit()
      defer { releaseApplicationPermit() }
      var next = snapshot()
      var changes: [CollectionChange<Model, PrimaryKey>] = []
      var releases: [PrimaryKey] = []
      for page in pages {
        for row in page.result.rows where claimIsActive(row.token, in: next.claims) {
          guard next.versions[row.primary].map({ page.result.version.supersedes($0) }) ?? true else { continue }
          if let old = next.primaryToSecondary[row.primary], old != row.token {
            removePrimary(row.primary, from: &next)
            releases.append(row.primary)
          }
          nextInstall(row, version: page.result.version, into: &next)
          changes.append(.upsert(row.model, sourceVersion: page.result.version))
        }
      }
      try await emit(changes, membershipReleases: releases)
      commit(next)
      for page in pages { page.completion.finish(.success(changes)) }
      for joiner in joiners { _ = try await joiner.value() }
      return changes
    } catch {
      for token in ownedTokens {
        if var claim = claims[token] {
          claim.holders.remove(holder)
          if claim.holders.isEmpty { claims.removeValue(forKey: token) } else { claims[token] = claim }
        }
      }
      for page in pages { page.completion.finish(.failure(error)) }
      throw error
    }
  }

  public func addKeys(_ keys: [String]) async throws -> [CollectionChange<Model, PrimaryKey>] {
    try await addKeys(keys.map { .string($0) })
  }

  @discardableResult
  public func releaseKeys(_ keys: [JSONValue]) async throws -> [CollectionChange<Model, PrimaryKey>] {
    try await releaseKeys(keys, holder: defaultHolder)
  }

  @discardableResult
  public func releaseKeys(_ keys: [JSONValue], holder: String) async throws
    -> [CollectionChange<Model, PrimaryKey>]
  {
    var next = snapshot()
    var releases: [PrimaryKey] = []
    for key in uniqueValues(keys) {
      let token = try canonicalToken(key)
      guard var claim = next.claims[token] else { continue }
      claim.holders.remove(holder)
      if claim.holders.isEmpty {
        next.claims.removeValue(forKey: token)
        for primary in next.secondaryToPrimaries[token] ?? [] {
          removePrimary(primary, from: &next)
          releases.append(primary)
        }
      } else {
        next.claims[token] = claim
      }
    }
    await acquireApplicationPermit()
    defer { releaseApplicationPermit() }
    try await emit([], membershipReleases: releases)
    commit(next)
    return []
  }

  @discardableResult
  public func releaseKeys(_ keys: [String]) async throws -> [CollectionChange<Model, PrimaryKey>] {
    try await releaseKeys(keys.map { .string($0) })
  }

  @discardableResult
  public func applyTail(_ batch: ChangeBatch) async throws -> [CollectionChange<Model, PrimaryKey>] {
    guard state == .live else { throw KeyedWindowError.notLive }
    await acquireApplicationPermit()
    defer { releaseApplicationPermit() }
    var next = snapshot()
    var changes: [CollectionChange<Model, PrimaryKey>] = []
    var releases: [PrimaryKey] = []
    for envelope in batch.envelopes {
      guard let rawLSN = envelope.headers.lsn,
        let parsed = CircuitsSubsetSource<Model, PrimaryKey>.postgresLSN(rawLSN)
      else { throw CircuitsSubsetSourceError.invalidLiveSourceVersion(envelope.headers.lsn) }
      let version = CollectionSourceVersion(
        rawValue: parsed.rawValue, order: parsed.order,
        transactionID: envelope.headers.txid.flatMap(CollectionPageSnapshot.xid32))
      let primary = try decodePrimaryKey(envelope.key)
      if let previous = next.versions[primary], !version.supersedes(previous) { continue }
      let pageInFlight = !inFlight.isEmpty
      if pageInFlight { next.versions[primary] = version }
      if envelope.headers.operation == .delete {
        if next.primaryToSecondary[primary] != nil {
          removePrimary(primary, from: &next)
          changes.append(.delete(primary, sourceVersion: version))
        }
        continue
      }
      let heldSecondary = next.primaryToSecondary[primary]
      let oldRaw = next.rawRows[primary]
      let merged = mergeRow(old: oldRaw, oldImage: envelope.old, value: envelope.value)
      let rawSecondary: String?
      if let merged, let value = merged[keyColumn] {
        rawSecondary = try canonicalToken(value)
      } else {
        rawSecondary = nil
      }
      let claimed = rawSecondary.map { claimIsActive($0, in: next.claims) } ?? false
      guard heldSecondary != nil || claimed else { continue }
      guard let merged else { throw StreamError.missingValue(key: envelope.key) }
      let model = try decodeRow(merged)
      let secondary = try secondaryKeyForRow(model)
      let token = try canonicalToken(secondary)
      guard claimIsActive(token, in: next.claims) else {
        if next.primaryToSecondary[primary] != nil {
          removePrimary(primary, from: &next)
          releases.append(primary)
        }
        continue
      }
      if heldSecondary != nil, heldSecondary != token {
        removePrimary(primary, from: &next)
        releases.append(primary)
      }
      nextInstall(
        (raw: merged, model: model, primary: primary, secondary: secondary, token: token),
        version: version, into: &next)
      changes.append(.upsert(model, sourceVersion: version))
    }
    gcUnheldVersions(&next)
    try await emit(changes, membershipReleases: releases)
    commit(next)
    return changes
  }

  public func end() async throws {
    guard state != .idle else { return }
    let handle = feed
    feed = nil
    state = .ended
    if let handle { try await client.releaseShape(handle) }
  }

  public func stop() async throws {
    for page in inFlight.values { page.task.cancel(); page.completion.finish(.failure(CancellationError())) }
    inFlight.removeAll()
    pageForToken.removeAll()
    if let feed { try await client.releaseShape(feed) }
    feed = nil
    frontier = nil
    state = .idle
  }

  private func fetchPageDraft(_ keys: [JSONValue]) async throws -> PageDraft {
    let tokens = Set(try keys.map(canonicalToken))
    let id = UUID()
    let completion = KeyedPageCompletion<[CollectionChange<Model, PrimaryKey>]>()
    let query = pageQuery(for: keys)
    let client = client
    let task = Task { try await client.querySubset(query) }
    inFlight[id] = InFlightPage(id: id, tokens: tokens, completion: completion, task: task)
    for token in tokens { pageForToken[token] = id }
    defer {
      inFlight.removeValue(forKey: id)
      for token in tokens where pageForToken[token] == id { pageForToken.removeValue(forKey: token) }
    }
    do {
      let response = try await withTaskCancellationHandler(
        operation: { try await task.value },
        onCancel: { task.cancel() })
      try Task.checkCancellation()
      let result = try decodePage(response)
      return PageDraft(tokens: tokens, result: result, completion: completion)
    } catch {
      task.cancel()
      completion.finish(.failure(error))
      throw error
    }
  }

  private func fetchPage(_ keys: [JSONValue]) async throws -> PageResult {
    try Task.checkCancellation()
    let response = try await client.querySubset(pageQuery(for: keys))
    try Task.checkCancellation()
    return try decodePage(response)
  }

  private func decodePage(_ response: SubsetResponse) throws -> PageResult {
    guard let version = CircuitsSubsetSource<Model, PrimaryKey>.pageVersion(response) else {
      throw CircuitsSubsetSourceError.invalidSnapshotSourceVersion(response.lsn)
    }
    var decoded: [(raw: ChangeRow, model: Model, primary: PrimaryKey, secondary: JSONValue, token: String)] = []
    for (index, value) in response.rows.enumerated() {
      guard case .object(let row) = value else {
        throw CircuitsSubsetSourceError.invalidSnapshotRow(index: index)
      }
      let model = try decodeRow(row)
      let primary = try primaryKeyForRow(model)
      let secondary = try secondaryKeyForRow(model)
      decoded.append((row, model, primary, secondary, try canonicalToken(secondary)))
    }
    return PageResult(rows: decoded, version: version)
  }

  private func pageQuery(for keys: [JSONValue]) -> SubsetQuery {
    let leaves: [ElectricCircuitsSwift.Predicate] = keys.map {
      .leaf(column: keyColumn, op: .eq, value: $0)
    }
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
      guard let key = try? canonicalToken(value), seen.insert(key).inserted else { return nil }
      return value
    }
  }

  private func canonicalToken(_ value: JSONValue) throws -> String {
    switch value {
    case .null: return "null"
    case .bool(let value): return value ? "bool:1" : "bool:0"
    case .int(let value): return "int:\(value)"
    case .decimal(let value): return "decimal:\(value)"
    case .number(let value): return "number:\(value)"
    case .string(let value): return "string:\(value.utf8.count):\(value)"
    case .array(let values): return "array:[\(try values.map(canonicalToken).joined(separator: ","))]"
    case .object(let values):
      return "object:{\(try values.keys.sorted().map { "\($0)=\(try canonicalToken(values[$0]!))" }.joined(separator: ","))}"
    }
  }

  private func mergeRow(old: ChangeRow?, oldImage: ChangeRow?, value: ChangeRow?) -> ChangeRow? {
    var merged = old ?? oldImage ?? [:]
    if let oldImage { for (key, value) in oldImage { merged[key] = value } }
    if let value { for (key, value) in value { merged[key] = value } }
    return merged.isEmpty ? nil : merged
  }

  private func snapshot() -> Snapshot {
    Snapshot(
      claims: claims, primaryToSecondary: primaryToSecondary,
      secondaryToPrimaries: secondaryToPrimaries, versions: versions, rawRows: rawRows)
  }

  private func commit(_ next: Snapshot) {
    claims = next.claims
    primaryToSecondary = next.primaryToSecondary
    secondaryToPrimaries = next.secondaryToPrimaries
    versions = next.versions
    rawRows = next.rawRows
  }

  private func claimIsActive(_ token: String, in claims: [String: Claim]) -> Bool {
    !(claims[token]?.holders.isEmpty ?? true)
  }

  private func nextInstall(
    _ row: (raw: ChangeRow, model: Model, primary: PrimaryKey, secondary: JSONValue, token: String),
    version: CollectionSourceVersion,
    into next: inout Snapshot
  ) {
    if let old = next.primaryToSecondary[row.primary], old != row.token {
      next.secondaryToPrimaries[old]?.remove(row.primary)
    }
    next.primaryToSecondary[row.primary] = row.token
    next.secondaryToPrimaries[row.token, default: []].insert(row.primary)
    next.versions[row.primary] = version
    next.rawRows[row.primary] = row.raw
  }

  private func removePrimary(_ primary: PrimaryKey, from next: inout Snapshot) {
    if let token = next.primaryToSecondary.removeValue(forKey: primary) {
      next.secondaryToPrimaries[token]?.remove(primary)
      if next.secondaryToPrimaries[token]?.isEmpty == true {
        next.secondaryToPrimaries.removeValue(forKey: token)
      }
    }
    next.rawRows.removeValue(forKey: primary)
  }

  private func gcUnheldVersions(_ next: inout Snapshot) {
    guard inFlight.isEmpty else { return }
    next.versions = next.versions.filter { next.primaryToSecondary[$0.key] != nil }
  }

  private func emit(
    _ changes: [CollectionChange<Model, PrimaryKey>], membershipReleases: [PrimaryKey]
  ) async throws {
    if !changes.isEmpty, let onChanges { try await onChanges(changes) }
    if !membershipReleases.isEmpty, let onMembershipRelease {
      try await onMembershipRelease(membershipReleases)
    }
  }

  private func acquireApplicationPermit() async {
    if applying {
      await withCheckedContinuation { applicationWaiters.append($0) }
    } else {
      applying = true
    }
  }

  private func releaseApplicationPermit() {
    if applicationWaiters.isEmpty { applying = false }
    else { applicationWaiters.removeFirst().resume() }
  }
}

private final class KeyedPageCompletion<Value: Sendable>: @unchecked Sendable {
  private let continuation: AsyncThrowingStream<Value, Error>.Continuation
  private let task: Task<Value, Error>
  init() {
    let pair = AsyncThrowingStream<Value, Error>.makeStream()
    continuation = pair.continuation
    task = Task { try await pair.stream.first(where: { _ in true })! }
  }
  func finish(_ result: Result<Value, Error>) {
    switch result {
    case .success(let value): continuation.yield(value); continuation.finish()
    case .failure(let error): continuation.finish(throwing: error)
    }
  }
  func value() async throws -> Value { try await task.value }
}

public enum KeyedWindowError: Error, Equatable, Sendable { case notLive }
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
