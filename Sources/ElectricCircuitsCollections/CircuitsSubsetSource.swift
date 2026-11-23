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
/// store application. The server subscription claim is the materialization ID plus the scope's
/// generation (`CollectionMaterializationID.subscription(generation:)`), so the stored copy's ID is
/// stable across launches while each launch opens its own subscription.
///
/// The snapshot fence names the stream it was taken from and the snapshot's transaction
/// visibility (`subset-v2`), so a later acquire of the same cached demand can `resume`: re-join
/// the identical feed request and, when the engine answers with the same stream URL, continue the
/// tail from the stored cursor with no frontier read or snapshot query. Circuits shares shapes by
/// `(table, canonical predicate, sorted columns, changes_only)`, not by subscription, a
/// changes-only feed's stream is append-only until the engine retires it, and within one catalog a
/// shape ID is never minted twice; the stream URL also names the catalog's namespace and
/// generations, where shape IDs restart. So the same stream URL proves the stored offset is still a
/// gap-free position in that stream (`Contracts/native-v1-contract-v1.md`, "Feed continuity").
public struct CircuitsSubsetSource<Model: Sendable, Key: Hashable & Sendable>:
  CollectionSourceAdapter
{
  private let client: ElectricCircuitsClient
  private let transport: any HTTPTransport
  private let table: String
  public let columns: [String]?
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
    let feedRequest = feedRequest(
      for: demand, identity: identity, materializationID: materializationID)
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
              gate: snapshotVersion, sourceVersion: snapshotVersion, window: window),
            lifecycle: lifecycle, transport: transport, apply: apply,
            onFailure: { failure, refusedBatch in
              // The record this snapshot writes names this feed; a later resume from it would
              // meet the same outcome.
              if refusedBatch || subsetFailureInvalidatesResume(failure) {
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
  /// - the last session on this same fence ended in a terminal stream outcome, a non-retryable
  ///   failure other than an authentication refusal (`subsetFailureInvalidatesResume`), or a
  ///   batch this source refused, such as a live envelope without `headers.lsn`: the stream serves
  ///   that batch again at the stored cursor, and only a snapshot's frontier is past it;
  /// - the re-joined feed answers with a different stream URL. A replacement changes-only feed has
  ///   no history for the gap since the stored cursor, so only a new snapshot can close it. That
  ///   create is kept for the `materialize` call that follows on this source instance, so the
  ///   fallback POSTs only once; a caller that materializes through another instance should first
  ///   call `cleanupAbandonedMaterialization` here, which releases it.
  ///
  /// Otherwise the session tails the same feed from the stored cursor and source version. Its
  /// subscription coordinator adopts this create's handle, so a resume POSTs `/subset-feeds`
  /// once and issues no frontier `HEAD` and no `/subsets/query`.
  ///
  /// The session calls `caughtUp` once it has read from the stored cursor to the stream's head,
  /// having applied everything on the way. It asks the stream where the head is rather than
  /// waiting at it: a long-poll tail starts with reads that carry no `live` parameter, which the
  /// stream answers at once, and an SSE tail is told by the stream's control event
  /// (`Contracts/native-v1-contract-v1.md`, "Live after a resume").
  public func resume(
    _ demand: CollectionDemand<Model>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID,
    record: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<Model, Key>? {
    let fence = SubsetSnapshotFence(rawValue: record.snapshotFence.rawValue)
    guard demand.limit == nil, demand.order.count <= 1, record.id == materializationID,
      let cursor = record.cursor,
      case .v2(let fencedStream, let snapshotOffset, _, let visibility)? = fence
    else { return nil }
    guard await resumeFailures.allowsResume(materializationID, from: record.snapshotFence) else {
      return nil
    }
    // Drain any earlier failed setup's release before the same stable claim is created again, and
    // drop a replaced-feed handle no `materialize` consumed: this create re-establishes the claim.
    try await pendingCleanup.retry(materializationID)
    try await pendingCleanup.releaseAdoptable(materializationID)
    let feedRequest = feedRequest(
      for: demand, identity: identity, materializationID: materializationID)
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
      gate: Self.resumedGate(
        record.sourceVersion, fencedVisibility: visibility,
        atSnapshotOffset: cursor.offset == snapshotOffset),
      sourceVersion: record.sourceVersion, window: nil)
    return CollectionResumeSession(
      cursor: cursor,
      sourceVersion: record.sourceVersion,
      run: { apply, caughtUp in
        do {
          try await runTail(
            start, lifecycle: lifecycle, transport: transport, apply: apply,
            // The stored rows are current once the tail has read from the stored cursor to the
            // stream's head (`Contracts/native-v1-contract-v1.md`, "Live after a resume").
            caughtUp: caughtUp,
            onFailure: { failure, refusedBatch in
              // Re-joining the same feed would repeat this outcome. Remember it so this source
              // declines the fence rather than re-join a cursor the engine will not serve, or
              // one whose next batch it refuses.
              if refusedBatch || subsetFailureInvalidatesResume(failure) {
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

  /// The feed request for a demand: its predicate and this source's columns, claimed under this
  /// launch's subscription for the materialization.
  private func feedRequest(
    for demand: CollectionDemand<Model>, identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) -> ShapeRequest {
    ShapeRequest(
      table: table,
      where: demand.sourcePredicate,
      columns: columns,
      changesOnly: true,
      subscription: materializationID.subscription(generation: identity.scope.generation)
    )
  }

  private func createFeed(_ request: ShapeRequest) async throws -> ShapeHandle {
    try await recreatePolicy.recreatingOnGone(clock: clock) {
      try await retryPolicy.retryingRefusedClaim(clock: clock) {
        try await client.createSubsetFeed(request)
      }
    }
  }

  /// Where a session's live tail starts: the feed handle its create returned and when that create
  /// was sent, the identical request the subscription coordinator renews with, the stored or
  /// snapshot position, and the version the tail gates snapshot overlap at (the snapshot's own,
  /// or a resumed tail's `resumedGate`).
  private struct TailStart: Sendable {
    let feed: ShapeHandle
    let createdAt: ContinuousClock.Instant
    let request: ShapeRequest
    let cursor: StreamCursor
    let gate: CollectionSourceVersion
    let sourceVersion: CollectionSourceVersion
    let window: LimitedWindow<Model, Key>?
  }

  /// Streams the feed from `start.cursor` through the awaited tail materializer. The subscription
  /// coordinator adopts `start.feed`, the handle this session's create already returned, so its
  /// `start()` does not POST `/subset-feeds` again; it renews on that handle's lease schedule with
  /// the identical request. From `install` on, the coordinator owns that claim's release.
  ///
  /// With `caughtUp`, a resumed tail's, the stream is first read to its head without waiting
  /// there, and `caughtUp` is called once every change up to that head has been applied.
  private func runTail(
    _ start: TailStart,
    lifecycle: CircuitsSubsetSessionLifecycle,
    transport: any HTTPTransport,
    apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void,
    caughtUp: (@Sendable () async -> Void)? = nil,
    onFailure:
      @escaping @Sendable (ShapeSubscriptionFailure, _ refusedBatch: Bool) async -> Void = {
        _, _ in
      }
  ) async throws {
    let materializer = CircuitsCollectionTailMaterializer(
      cursor: start.cursor,
      gate: start.gate,
      sourceVersion: start.sourceVersion,
      decodeRow: decodeRow,
      decodeKey: decodeKey,
      window: start.window,
      retryPolicy: retryPolicy,
      clock: clock,
      apply: apply
    )
    let onCaughtUp: (@Sendable (StreamCursor) async -> Void)? =
      if let caughtUp { { _ in await caughtUp() } } else { nil }
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
      setupClaim: start.feed,
      setupClaimAge: start.createdAt.duration(to: .now),
      onCaughtUp: onCaughtUp
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
            let terminal = await materializer.takeTerminalFailure()
            await onFailure(failure, terminal?.refusedBatch == true)
            if let terminal { throw terminal.cause }
            throw CircuitsSubsetSourceError.subscription(failure)
          case .reseedRequired(let outcome):
            await onFailure(.reseedRequired(outcome), false)
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

  /// The gate a resumed tail reads its stream through, by the engine's rules for an envelope's
  /// position (electric-circuits `docs/ARCHITECTURE.md`, "Native envelope positions").
  ///
  /// A snapshot may be consulted only for a key no stream envelope has been applied to since it
  /// was taken (rule 2). A resumed tail does not know which keys those are, so it consults the
  /// snapshot only while the stored cursor is still the snapshot's own offset: nothing has been
  /// applied from the stream yet, and the snapshot's transaction visibility tells a commit the
  /// snapshot already holds from one that was in progress when it was taken, which can sit after
  /// the stored offset with an LSN below the snapshot's. A store need not keep that visibility
  /// with the version (`sourceVersion.snapshot`), so the fence carries it and it is restored here.
  ///
  /// Once the cursor has moved, some key may have had an envelope applied, and a later envelope
  /// for it is the stream's newer word on that key whatever the snapshot saw: a flip's verdict
  /// names the transaction that caused it, which the snapshot may include, and still follows a
  /// newer change to the same row. So the tail then skips nothing on the snapshot's account (rule
  /// 3: when in doubt, apply), and the store's `supersedes` keeps an envelope the snapshot covers
  /// from replacing a snapshot row. It keeps an LSN floor only when an applied change raised the
  /// stored version above the snapshot's LSN: positions never decrease along a stream, so
  /// anything below that floor is a replay. A snapshot with no visibility (an older engine) was
  /// tailed by LSN from the start.
  static func resumedGate(
    _ stored: CollectionSourceVersion, fencedVisibility: CollectionPageSnapshot?,
    atSnapshotOffset: Bool
  ) -> CollectionSourceVersion {
    guard let visibility = stored.snapshot ?? fencedVisibility else { return stored }
    if atSnapshotOffset {
      return CollectionSourceVersion(
        rawValue: stored.rawValue, order: stored.order, snapshot: visibility,
        transactionID: stored.transactionID)
    }
    return CollectionSourceVersion(
      rawValue: stored.rawValue, order: stored.order > visibility.lsn ? stored.order : 0)
  }

  static func pageVersion(_ response: SubsetResponse) -> CollectionSourceVersion? {
    guard let lsn = postgresLSN(response.lsn) else { return nil }
    return CollectionSourceVersion(
      rawValue: lsn.rawValue, order: lsn.order,
      snapshot: CollectionPageSnapshot(
        lsn: lsn.order, snapshot: response.snapshot, horizon: response.horizon))
  }

  /// PostgreSQL LSNs are two unsigned hexadecimal 32-bit words. A `UInt64` sort key is portable
  /// to Indexed/GRDB stores and avoids trusting lexicographic wire strings.
  static func postgresLSN(_ value: String) -> CollectionSourceVersion? {
    let words = value.split(separator: "/", omittingEmptySubsequences: false)
    guard words.count == 2, !words[0].isEmpty, !words[1].isEmpty,
      let high = UInt64(words[0], radix: 16), let low = UInt64(words[1], radix: 16),
      high <= UInt64(UInt32.max), low <= UInt64(UInt32.max)
    else { return nil }
    return CollectionSourceVersion(rawValue: value, order: (high << 32) | low)
  }
}

extension ShapeSubscriptionRetryPolicy {
  /// Runs the feed-create request a source makes itself, before a subscription coordinator exists
  /// to retry it. A transient refusal (`408`,
  /// `425`, `429` or `5xx`, `ClientError.retryableHTTP`) is answered by repeating the identical
  /// request, up to `maxRetries` times, each after this policy's backoff or the server's
  /// `Retry-After`, whichever is longer, and never longer than `maximumDelay`. An engine at its
  /// shape cap refuses a create this way (`503` with `Retry-After`) until a subscription is
  /// released or lapses; the same request then succeeds. Past the budget the refusal is thrown
  /// as it was received. Every other outcome, transport errors included, propagates unchanged:
  /// the request names a stable subscription, so repeating it is a renewal, but only a server
  /// that asked for a retry is given one here.
  func retryingRefusedClaim<T: Sendable>(
    clock: any ShapeSubscriptionClock, _ body: @Sendable () async throws -> T
  ) async throws -> T {
    var retries = 0
    while true {
      do {
        return try await body()
      } catch ClientError.retryableHTTP(let status, let retryAfter) {
        guard retries < maxRetries else {
          throw ClientError.retryableHTTP(status: status, retryAfter: retryAfter)
        }
        retries += 1
        try await clock.sleep(for: delay(forRetry: retries, retryAfter: retryAfter))
        try Task.checkCancellation()
      }
    }
  }
}

/// Outcomes a retry of the same feed from the same cursor would deterministically repeat: terminal
/// stream receipts (404/410/`stream-closed`), a must-refetch marker, a
/// replacement shape on renewal, and non-retryable stream or HTTP failures. Transport loss,
/// exhausted transient retries, an authentication refusal the app clears by refreshing its
/// credentials (401, 403, 407), and local store failures say nothing about the stored cursor. A
/// batch the source itself refused is reported apart from the failure (`refusedBatch`).
func subsetFailureInvalidatesResume(_ failure: ShapeSubscriptionFailure) -> Bool {
  switch failure {
  case .client(.http(let status, _)) where [401, 403, 407].contains(status):
    false
  case .reseedRequired, .stream, .client:
    true
  case .transport, .materializerUnavailable, .materializer, .retryExhausted, .capacityExceeded:
    false
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
    return Self.framed(parts)
  }

  static func framed(_ parts: [String]) -> String {
    parts.map { "\($0.utf8.count):\($0)" }.joined(separator: "|")
  }

  static func components(of rawValue: String) -> [String]? {
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

/// Sessions that ended in an outcome a retry of the same feed would repeat, keyed by
/// materialization and the fence they ran on. In memory only: a relaunch may try once more.
/// A record with a different fence comes from a newer snapshot and is resumable again, so the
/// entry is dropped then and the registry stays bounded by materializations with a failed resume.
actor FailedSubsetResumes {
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
  /// The error that ended this materializer, other than a store's refusal to apply. The
  /// subscription coordinator reports any materializer error as `.materializer`, so the source
  /// keeps the typed cause here and rethrows it from the session instead of the coordinator's
  /// summary. `refusedBatch` marks a batch this materializer could not turn into changes (a live
  /// envelope without a parseable `headers.lsn`, a key or row that does not decode, a missing
  /// value): the stream serves the same batch at the same cursor again, so reading on from the
  /// stored cursor repeats the failure. A window's page failure says nothing about the batch.
  private var terminalFailure: (cause: any Error, refusedBatch: Bool)?

  func takeTerminalFailure() -> (cause: any Error, refusedBatch: Bool)? {
    defer { terminalFailure = nil }
    return terminalFailure
  }

  init(
    cursor: StreamCursor,
    gate: CollectionSourceVersion,
    sourceVersion: CollectionSourceVersion,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key,
    window: LimitedWindow<Model, Key>? = nil,
    retryPolicy: ShapeSubscriptionRetryPolicy = .init(),
    clock: any ShapeSubscriptionClock = ContinuousShapeSubscriptionClock(),
    apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
  ) {
    self.cursor = cursor
    snapshotSourceVersion = gate
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

  /// Turns one stream batch into the changes to apply, or throws when the batch cannot be read.
  /// It follows the engine's rules for an envelope's position (electric-circuits
  /// `docs/ARCHITECTURE.md`, "Native envelope positions"):
  /// - Every live envelope carries `headers.lsn`. One without a parseable position is refused,
  ///   never applied as if it were ordered.
  /// - Between envelopes a position only rejects a replay: an envelope for a key is applied when
  ///   its `lsn` is at or above the last one applied to that key. Equal positions are ordinary
  ///   (a transaction's second envelope for a key, a flip's raised verdict), and offset order
  ///   decides among them.
  /// - A snapshot is consulted only for a key no envelope has been applied to since, and never
  ///   after the first envelope at or past its horizon: such an envelope is skipped when its
  ///   `lsn` is below the horizon and its `txid` was visible to the snapshot (without a `txid`,
  ///   when its `lsn` is below the snapshot's).
  private func decode(
    _ batch: ChangeBatch, changes: inout [CollectionChange<Model, Key>],
    acceptedVersions: inout [CollectionSourceVersion], accepted: inout [Int],
    rowVersions nextRowVersions: inout [String: CollectionSourceVersion],
    passedHorizon: inout Bool, latest: inout CollectionSourceVersion
  ) throws {
    for (index, envelope) in batch.envelopes.enumerated() {
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
      accepted.append(index)
      // A window never applies tail rows, so it does not decode them either: the feed covers the
      // whole predicate, including rows the page excludes (NULL sort keys among them), and a
      // model decoder is entitled to reject those. The page is the only source of window rows.
      guard window == nil else { continue }
      // The last position applied to each key: the next envelope for it is applied when its
      // position is at or above this one, and the snapshot is not consulted for it again.
      nextRowVersions[envelope.key] = version
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
    // The envelopes the snapshot gate lets through, for `LiveDelivery.applying(changesAt:_:)`.
    var accepted: [Int] = []
    var nextRowVersions = rowVersions
    var passedHorizon = passedSnapshotHorizon
    var latest = sourceVersion
    do {
      try decode(
        batch, changes: &changes, acceptedVersions: &acceptedVersions, accepted: &accepted,
        rowVersions: &nextRowVersions, passedHorizon: &passedHorizon, latest: &latest)
    } catch {
      terminalFailure = (error, true)
      throw error
    }
    var committedWindowKeys: Set<Key>?
    var committedPageVersion: CollectionSourceVersion?
    if let window, !acceptedVersions.isEmpty {
      let (rows, pageVersion): ([Model], CollectionSourceVersion)
      do {
        (rows, pageVersion) = try await freshPage(
          window, notBefore: latest, reflecting: acceptedVersions)
      } catch {
        if !(error is CancellationError) { terminalFailure = (error, false) }
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
    let changeBatch = CollectionChangeBatch(
      changes: changes,
      expectedCursor: expectedCursor,
      cursor: StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue),
      sourceVersion: latest
    )
    try await LiveDelivery.applying(changesAt: accepted) { try await applyBatch(changeBatch) }
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
