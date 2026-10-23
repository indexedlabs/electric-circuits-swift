import ElectricCircuitsSwift
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

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
///
/// The snapshot fence names the stream it was taken from and the snapshot's transaction
/// visibility (`subset-v2`), so a later acquire of the same cached demand can `resume`: re-join
/// the identical feed request and, when the engine answers with the same stream URL, continue the
/// tail from the stored cursor with no frontier read or snapshot query. Circuits shares shapes by
/// `(table, canonical predicate, sorted columns, changes_only)`, a changes-only feed's stream is
/// append-only until the engine retires it, and within one catalog a shape ID is never minted
/// twice; the stream URL also names the catalog's namespace and generations, where shape IDs
/// restart. So the same stream URL proves the stored offset is still a gap-free position in that
/// stream (`Contracts/native-v1-contract-v1.md`, "Feed continuity").
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
  private let resumeFailures = FailedSubsetResumes()

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
    let feedRequest = feedRequest(for: demand, materializationID: materializationID)
    let feed: ShapeHandle
    let feedCreatedAt: ContinuousClock.Instant
    if let adopted = await pendingCleanup.takeAdoptable(materializationID) {
      // A resume that found its feed replaced already established this claim moments ago; the
      // frontier read and snapshot below fence against it exactly as against a fresh create.
      (feed, feedCreatedAt) = (adopted.handle, adopted.createdAt)
    } else {
      // The initial feed create is the same native route the coordinator later renews, so it
      // carries the same recreate vocabulary. A persisted materialization ID joining a dormant
      // shape whose fall-through the engine has exhausted must re-POST the identical claim, not
      // fail setup before the frontier HEAD and snapshot query.
      feedCreatedAt = .now
      feed = try await createFeed(feedRequest)
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
        rawValue: SubsetSnapshotFence.v2(
          stream: feed.response.streamURL.absoluteString, offset: frontier.offset,
          sourceVersion: snapshotVersion.rawValue,
          visibility: snapshotVersion.snapshot
        ).rawValue)

      let resumeFailures = resumeFailures
      return CollectionSourceSession(
        snapshot: CollectionSnapshot(
          rows: rows, fence: snapshotFence, sourceVersion: snapshotVersion, cursor: cursor),
        run: { apply in
          try await runTail(
            TailStart(
              feed: feed, createdAt: feedCreatedAt, request: feedRequest, cursor: cursor,
              sourceVersion: snapshotVersion, window: window),
            lifecycle: lifecycle, transport: transport, apply: apply,
            onFailure: { failure in
              // The record this snapshot writes names this feed; a later resume from it would
              // meet the same outcome.
              if Self.failureInvalidatesResume(failure) {
                await resumeFailures.record(materializationID, fence: snapshotFence)
              }
            })
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

  /// Continues an exact cached materialization when its fence names the feed the engine still
  /// serves for this request. Returns nil, so the coordinator snapshots, when:
  /// - the demand is a limited window (its live state is the page, which needs a query anyway);
  /// - the fence predates `subset-v2` and so does not name its feed, or the record has no cursor;
  /// - the last resume from this same fence ended in a terminal or non-retryable stream outcome;
  /// - the re-joined feed answers with a different stream URL. A replacement changes-only feed has
  ///   no history for the gap since the stored cursor, so only a new snapshot can close it. That
  ///   create is kept for the `materialize` call that follows on this source instance, so the
  ///   fallback POSTs only once; a caller that materializes through another instance should first
  ///   call `cleanupAbandonedMaterialization` here, which releases it.
  ///
  /// Otherwise the session tails the same feed from the stored cursor and source version. Its
  /// subscription coordinator adopts this create's handle, so a resume POSTs `/subset-feeds`
  /// once and issues no frontier `HEAD` and no `/subsets/query`.
  public func resume(
    _ demand: CollectionDemand<Model>,
    identity _: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID,
    record: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<Model, Key>? {
    let fence = SubsetSnapshotFence(rawValue: record.snapshotFence.rawValue)
    guard demand.limit == nil, demand.order.count <= 1, record.id == materializationID,
      let cursor = record.cursor, case .v2(let fencedStream, _, _, let visibility)? = fence
    else { return nil }
    guard await resumeFailures.allowsResume(materializationID, from: record.snapshotFence) else {
      return nil
    }
    // Drain any earlier failed setup's release before the same stable claim is created again, and
    // drop a replaced-feed handle no `materialize` consumed: this create re-establishes the claim.
    try await pendingCleanup.retry(materializationID)
    try await pendingCleanup.releaseAdoptable(materializationID)
    let feedRequest = feedRequest(for: demand, materializationID: materializationID)
    let createdAt = ContinuousClock.Instant.now
    let feed = try await createFeed(feedRequest)
    guard feed.response.streamURL.absoluteString == fencedStream else {
      await pendingCleanup.keepAdoptable(feed, createdAt: createdAt, for: materializationID)
      return nil
    }

    let lifecycle = CircuitsSubsetSessionLifecycle(client: client, initialHandle: feed)
    let resumeFailures = resumeFailures
    let storedFence = record.snapshotFence
    let start = TailStart(
      feed: feed, createdAt: createdAt, request: feedRequest, cursor: cursor,
      sourceVersion: Self.resumedGate(record.sourceVersion, fencedVisibility: visibility),
      window: nil)
    return CollectionResumeSession(
      cursor: cursor,
      sourceVersion: record.sourceVersion,
      run: { apply, caughtUp in
        do {
          try await runTail(
            start, lifecycle: lifecycle,
            // The subscription coordinator uses this transport only for stream reads from the
            // stored cursor onward, so an idle answer there is the resumed tail's up-to-date proof.
            transport: CaughtUpObservingTransport(base: transport, caughtUp: caughtUp),
            apply: apply,
            onFailure: { failure in
              // Re-joining the same feed would repeat this outcome. Remember it so this source
              // declines the fence rather than re-join a cursor the engine will not serve.
              if Self.failureInvalidatesResume(failure) {
                await resumeFailures.record(materializationID, fence: storedFence)
              }
            })
        } catch {
          // The coordinator reseeds by snapshot on a cursor the feed refused, and fails the
          // attempt on anything else.
          if await !resumeFailures.allowsResume(materializationID, from: storedFence) {
            throw CollectionResumeInvalidated(cause: error)
          }
          throw error
        }
      },
      stop: { try await lifecycle.stop() }
    )
  }

  public func cleanupAbandonedMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    try await pendingCleanup.retry(materializationID)
    try await pendingCleanup.releaseAdoptable(materializationID)
  }

  private func feedRequest(
    for demand: CollectionDemand<Model>, materializationID: CollectionMaterializationID
  ) -> ShapeRequest {
    ShapeRequest(
      table: table,
      where: demand.sourcePredicate,
      columns: columns,
      changesOnly: true,
      subscription: materializationID.rawValue
    )
  }

  private func createFeed(_ request: ShapeRequest) async throws -> ShapeHandle {
    try await recreatePolicy.recreatingOnGone(clock: clock) {
      try await client.createSubsetFeed(request)
    }
  }

  /// Where a session's live tail starts: the feed handle its create returned and when that create
  /// was sent, the identical request the subscription coordinator renews with, and the stored or
  /// snapshot position.
  private struct TailStart: Sendable {
    let feed: ShapeHandle
    let createdAt: ContinuousClock.Instant
    let request: ShapeRequest
    let cursor: StreamCursor
    let sourceVersion: CollectionSourceVersion
    let window: LimitedWindow<Model, Key>?
  }

  /// Streams the feed from `start.cursor` through the awaited tail materializer. The subscription
  /// coordinator adopts `start.feed`, the handle this session's create already returned, so its
  /// `start()` does not POST `/subset-feeds` again; it renews on that handle's lease schedule with
  /// the identical request. From `install` on, the coordinator owns that claim's release.
  private func runTail(
    _ start: TailStart,
    lifecycle: CircuitsSubsetSessionLifecycle,
    transport: any HTTPTransport,
    apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void,
    onFailure: @escaping @Sendable (ShapeSubscriptionFailure) async -> Void = { _ in }
  ) async throws {
    let materializer = CircuitsCollectionTailMaterializer(
      cursor: start.cursor,
      sourceVersion: start.sourceVersion,
      decodeRow: decodeRow,
      decodeKey: decodeKey,
      window: start.window,
      retryPolicy: retryPolicy,
      clock: clock,
      apply: apply
    )
    let coordinator = ShapeSubscriptionCoordinator(
      client: client,
      transport: transport,
      request: start.request,
      materializer: materializer,
      retryPolicy: retryPolicy,
      clock: clock,
      capacity: capacity,
      responseDecodingLimits: responseDecodingLimits,
      telemetry: telemetry,
      kind: .subsetFeed,
      recreatePolicy: recreatePolicy,
      adopting: start.feed,
      createdAt: start.createdAt
    )
    try await lifecycle.install(coordinator)
    let states = await coordinator.stateUpdates
    do {
      try await withTaskCancellationHandler {
        _ = try await coordinator.start()
        for await state in states {
          try Task.checkCancellation()
          switch state {
          case .failed(let failure):
            await onFailure(failure)
            if let cause = await materializer.takeTerminalFailure() { throw cause }
            throw CircuitsSubsetSourceError.subscription(failure)
          case .reseedRequired(let outcome):
            await onFailure(.reseedRequired(outcome))
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
  }

  /// The version a resumed tail gates snapshot overlap at, so it admits exactly what the
  /// snapshot's own tail would have admitted from the stored cursor.
  ///
  /// While no change has been accepted past the snapshot, the stored version is the snapshot's
  /// page version, and only its transaction visibility tells a commit the snapshot already holds
  /// from one that was still in progress when it was taken: such a commit can sit after the
  /// stored offset with an LSN below the snapshot's, and an LSN comparison would drop it. A store
  /// need not keep that visibility with the version (`sourceVersion.snapshot`), so the fence
  /// carries it and it is restored here. Once a change has been accepted, the stored version is
  /// that change's LSN; the stream after the stored offset commits at or above it, so the LSN gate
  /// can re-admit only snapshot-visible commits ordered after an invisible one below the
  /// horizon, and the store's `supersedes` keeps their bytes from regressing a newer row. A
  /// snapshot with no visibility (an older engine) was tailed by LSN from the start.
  static func resumedGate(
    _ stored: CollectionSourceVersion, fencedVisibility: CollectionPageSnapshot?
  ) -> CollectionSourceVersion {
    guard stored.snapshot == nil, let fencedVisibility, stored.order <= fencedVisibility.lsn
    else { return stored }
    return CollectionSourceVersion(
      rawValue: stored.rawValue, order: stored.order, snapshot: fencedVisibility,
      transactionID: stored.transactionID)
  }

  /// Outcomes a re-join of the same feed from the same cursor would deterministically repeat:
  /// terminal stream receipts (404/410/`stream-closed`), a replacement shape on renewal, and
  /// non-retryable stream or HTTP failures. Transport loss, exhausted transient retries, an
  /// authentication refusal the app clears by refreshing its credentials (401, 403, 407), and local
  /// store failures say nothing about the stored cursor.
  private static func failureInvalidatesResume(_ failure: ShapeSubscriptionFailure) -> Bool {
    switch failure {
    case .client(.http(let status, _)) where [401, 403, 407].contains(status):
      false
    case .reseedRequired, .stream, .client:
      true
    case .transport, .materializerUnavailable, .materializer, .retryExhausted, .capacityExceeded:
      false
    }
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

/// The subset source's snapshot fence. Components are UTF-8 length-prefixed (`<bytes>:<value>`)
/// and joined by `|`, so application-controlled values cannot alias one another.
///
/// `subset-v1|<offset>|<lsn>` predates resume: it does not name the feed its cursor belongs to,
/// so it still parses but is never resumable. `subset-v2|<streamUrl>|<offset>|<lsn>|<snapshot>|
/// <horizon>` adds the URL of the changes-only feed's stream that the stored cursor positions,
/// and the snapshot's transaction visibility (`xmin:xmax:xip,…` and the horizon LSN,
/// both empty when the engine sent none), which a resumed tail gates at (`resumedGate`). The
/// fence stays opaque to the collection core and to stores; only this source interprets it.
enum SubsetSnapshotFence: Equatable, Sendable {
  case v1(offset: String, sourceVersion: String)
  case v2(
    stream: String, offset: String, sourceVersion: String, visibility: CollectionPageSnapshot?)

  init?(rawValue: String) {
    switch Self.components(of: rawValue) {
    case let parts? where parts.count == 3 && parts[0] == "subset-v1":
      self = .v1(offset: parts[1], sourceVersion: parts[2])
    case let parts? where parts.count == 6 && parts[0] == "subset-v2":
      let visibility: CollectionPageSnapshot?
      if parts[4].isEmpty, parts[5].isEmpty {
        visibility = nil
      } else {
        // Visibility the fence names but that does not parse cannot gate a resumed tail.
        guard let order = CollectionPageSnapshot.postgresLSN(parts[3]),
          let parsed = CollectionPageSnapshot(lsn: order, snapshot: parts[4], horizon: parts[5])
        else { return nil }
        visibility = parsed
      }
      self = .v2(
        stream: parts[1], offset: parts[2], sourceVersion: parts[3], visibility: visibility)
    default:
      return nil
    }
  }

  var rawValue: String {
    let parts =
      switch self {
      case .v1(let offset, let sourceVersion): ["subset-v1", offset, sourceVersion]
      case .v2(let stream, let offset, let sourceVersion, let visibility):
        [
          "subset-v2", stream, offset, sourceVersion, visibility?.snapshotText ?? "",
          visibility?.horizonText ?? "",
        ]
      }
    return parts.map { "\($0.utf8.count):\($0)" }.joined(separator: "|")
  }

  private static func components(of rawValue: String) -> [String]? {
    let bytes = Array(rawValue.utf8)
    var index = 0
    var parts: [String] = []
    while index < bytes.count {
      if !parts.isEmpty {
        guard bytes[index] == UInt8(ascii: "|") else { return nil }
        index += 1
      }
      var length = 0
      var digits = 0
      while index < bytes.count, bytes[index] != UInt8(ascii: ":") {
        let byte = bytes[index]
        guard byte >= UInt8(ascii: "0"), byte <= UInt8(ascii: "9"), digits < 9 else { return nil }
        length = length * 10 + Int(byte - UInt8(ascii: "0"))
        digits += 1
        index += 1
      }
      guard digits > 0, index < bytes.count else { return nil }
      index += 1
      guard length <= bytes.count - index,
        let part = String(bytes: bytes[index..<(index + length)], encoding: .utf8)
      else { return nil }
      parts.append(part)
      index += length
    }
    return parts.isEmpty ? nil : parts
  }
}

/// A failed setup has no session to return, but it can already have created a server feed. Keep
/// the one release handle per materialization until the idempotent DELETE succeeds (or receives
/// 404, which the client normalizes to success). The registry is bounded by active failed setup
/// attempts and is drained before another feed with that stable subscription can be created.
///
/// It also holds the feed a resume created and declined because the engine answered with a
/// replacement shape. The `materialize` that follows adopts that claim instead of creating it
/// again; abandoned-materialization cleanup releases one no `materialize` consumed.
private actor PendingSubsetFeedCleanup {
  private let client: ElectricCircuitsClient
  private var handles: [CollectionMaterializationID: ShapeHandle] = [:]
  private var adoptable:
    [CollectionMaterializationID: (handle: ShapeHandle, createdAt: ContinuousClock.Instant)] = [:]

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

  func keepAdoptable(
    _ handle: ShapeHandle, createdAt: ContinuousClock.Instant,
    for materializationID: CollectionMaterializationID
  ) {
    adoptable[materializationID] = (handle, createdAt)
  }

  func takeAdoptable(
    _ materializationID: CollectionMaterializationID
  ) -> (handle: ShapeHandle, createdAt: ContinuousClock.Instant)? {
    adoptable.removeValue(forKey: materializationID)
  }

  func releaseAdoptable(_ materializationID: CollectionMaterializationID) async throws {
    guard let handle = adoptable[materializationID]?.handle else { return }
    try await client.releaseShape(handle)
    if adoptable[materializationID]?.handle == handle {
      adoptable.removeValue(forKey: materializationID)
    }
  }
}

/// Resumes that ended in an outcome a re-join of the same feed would repeat, keyed by
/// materialization and the fence they resumed from. In memory only: a relaunch may try once more.
/// A record with a different fence comes from a newer snapshot and is resumable again, so the
/// entry is dropped then and the registry stays bounded by materializations with a failed resume.
private actor FailedSubsetResumes {
  private var fences: [CollectionMaterializationID: SnapshotFence] = [:]

  func record(_ materializationID: CollectionMaterializationID, fence: SnapshotFence) {
    fences[materializationID] = fence
  }

  func allowsResume(_ materializationID: CollectionMaterializationID, from fence: SnapshotFence)
    -> Bool
  {
    guard let failed = fences[materializationID] else { return true }
    guard failed != fence else { return false }
    fences.removeValue(forKey: materializationID)
    return true
  }
}

/// Owns a session's feed claim until the tail's subscription coordinator takes it over. The
/// coordinator adopts the handle at construction, and `install` is the handoff: after it, the
/// coordinator's stop releases the claim whether or not its start ran. A session stopped before
/// the handoff releases the handle itself and never installs a coordinator.
private actor CircuitsSubsetSessionLifecycle {
  private let client: ElectricCircuitsClient
  private let initialHandle: ShapeHandle
  private var coordinator: ShapeSubscriptionCoordinator?
  private var stopped = false
  private var stopTask: Task<Void, Error>?

  init(client: ElectricCircuitsClient, initialHandle: ShapeHandle) {
    self.client = client
    self.initialHandle = initialHandle
  }

  func install(_ coordinator: ShapeSubscriptionCoordinator) throws {
    // A stop that ran first has released the handle. The coordinator never started, so dropping
    // it leaves nothing to compensate; stopping it would DELETE the same claim a second time.
    guard !stopped else { throw CancellationError() }
    self.coordinator = coordinator
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
    let client = client
    let initialHandle = initialHandle
    let task = Task<Void, Error> {
      if let coordinator {
        try await coordinator.stop()
      } else {
        try await client.releaseShape(initialHandle)
      }
    }
    stopTask = task
    try await task.value
  }
}

/// Reports idle answers on a resumed tail's stream reads: a `204` (the long-poll window elapsed
/// at the tail) or an empty successful batch, neither closed. A response carrying changes proves
/// progress through the committed `apply` instead, and terminal answers stay with the reader.
private struct CaughtUpObservingTransport: HTTPTransport {
  let base: any HTTPTransport
  let caughtUp: @Sendable () async -> Void

  func prepare(_ request: URLRequest) -> URLRequest { base.prepare(request) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    let response = try await base.send(request)
    await observe(response)
    return response
  }

  func send(_ request: URLRequest, maximumResponseBytes: Int) async throws -> HTTPResponse {
    let response = try await base.send(request, maximumResponseBytes: maximumResponseBytes)
    await observe(response)
    return response
  }

  private func observe(_ response: HTTPResponse) async {
    let headers = response.response
    guard headers.value(forHTTPHeaderField: "stream-closed")?.lowercased() != "true" else {
      return
    }
    let status = headers.statusCode
    if status == 204 || ((200..<300).contains(status) && Self.isEmptyBatch(response.data)) {
      await caughtUp()
    }
  }

  /// An empty body or a JSON `[]`, the same idle answers the stream reader skips.
  private static func isEmptyBatch(_ data: Data) -> Bool {
    guard data.count <= 64 else { return false }
    let significant = data.filter { $0 != 0x20 && $0 != 0x09 && $0 != 0x0A && $0 != 0x0D }
    return significant.isEmpty || significant.elementsEqual("[]".utf8)
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
