import ElectricCircuitsSwift
import Foundation

public enum CircuitsSubsetSourceError: Error, Equatable, Sendable {
  case unsupportedOrderCount(Int)
  case unsupportedLimitedLiveDemand(Int)
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
  private let retryPolicy: ShapeSubscriptionRetryPolicy
  private let clock: any ShapeSubscriptionClock
  private let capacity: ShapeSubscriptionCapacity?
  private let responseDecodingLimits: ResponseDecodingLimits
  private let telemetry: TelemetryReporter
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
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key
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
    self.decodeRow = decodeRow
    self.decodeKey = decodeKey
    pendingCleanup = PendingSubsetFeedCleanup(client: client)
  }

  public func materialize(
    _ demand: CollectionDemand<Model>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<Model, Key> {
    if let limit = demand.limit {
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
    let feed = try await client.createSubsetFeed(feedRequest)

    do {
      let frontier = try await client.streamCursor(for: feed)
      let orderBy = demand.order.first.map {
        SubsetOrderBy(column: $0.sourceName, descending: $0.direction == .descending)
      }
      let response = try await client.querySubset(
        SubsetQuery(
          table: table,
          where: demand.sourcePredicate,
          columns: columns,
          orderBy: orderBy,
          limit: demand.limit
        ))
      let rows = try response.rows.enumerated().map { index, value in
        guard case .object(let row) = value else {
          throw CircuitsSubsetSourceError.invalidSnapshotRow(index: index)
        }
        return try decodeRow(row)
      }
      guard let snapshotVersion = Self.postgresLSN(response.lsn) else {
        throw CircuitsSubsetSourceError.invalidSnapshotSourceVersion(response.lsn)
      }
      let cursor = StreamCursor(offset: frontier.offset, lsn: snapshotVersion.rawValue)
      let lifecycle = CircuitsSubsetSessionLifecycle(client: client, initialHandle: feed)
      let snapshotFence = SnapshotFence(
        rawValue: Self.snapshotFence(offset: frontier.offset, sourceVersion: response.lsn))

      return CollectionSourceSession(
        snapshot: CollectionSnapshot(
          rows: rows, fence: snapshotFence, sourceVersion: snapshotVersion, cursor: cursor),
        run: { apply in
          let materializer = CircuitsCollectionTailMaterializer(
            cursor: cursor,
            sourceVersion: snapshotVersion,
            decodeRow: decodeRow,
            decodeKey: decodeKey,
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
            kind: .subsetFeed
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

  private static func snapshotFence(offset: String, sourceVersion: String) -> String {
    ["subset-v1", offset, sourceVersion]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
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

private actor CircuitsCollectionTailMaterializer<Model: Sendable, Key: Hashable & Sendable>:
  ShapeMaterializer
{
  private var cursor: StreamCursor?
  private let snapshotSourceVersion: CollectionSourceVersion
  private var sourceVersion: CollectionSourceVersion
  private let decodeRow: @Sendable (ChangeRow) throws -> Model
  private let decodeKey: @Sendable (String) throws -> Key
  private let applyBatch: @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void

  init(
    cursor: StreamCursor,
    sourceVersion: CollectionSourceVersion,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key,
    apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
  ) {
    self.cursor = cursor
    snapshotSourceVersion = sourceVersion
    self.sourceVersion = sourceVersion
    self.decodeRow = decodeRow
    self.decodeKey = decodeKey
    applyBatch = apply
  }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo nextCursor: StreamCursor
  ) async throws {
    if cursor == nextCursor { return }
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor,
        actual: cursor,
        advancingTo: nextCursor
      )
    }
    var accepted: [CollectionChange<Model, Key>] = []
    var latest = sourceVersion
    for envelope in batch.envelopes {
      guard let rawVersion = envelope.headers.lsn,
        let version = CircuitsSubsetSource<Model, Key>.postgresLSN(rawVersion)
      else {
        throw CircuitsSubsetSourceError.invalidLiveSourceVersion(envelope.headers.lsn)
      }
      // The feed starts before the subset snapshot. The snapshot LSN is an immutable lower bound:
      // changes at that LSN are part of the same source transaction and must all apply. Do not use
      // the moving high-water mark as a per-envelope filter or sibling changes sharing one LSN
      // disappear. We still advance the durable offset for dropped pre-snapshot overlap.
      guard version >= snapshotSourceVersion else { continue }
      latest = max(latest, version)
      switch envelope.headers.operation {
      case .delete:
        do { accepted.append(.delete(try decodeKey(envelope.key))) } catch {
          throw CircuitsSubsetSourceError.invalidLiveKey(envelope.key)
        }
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        accepted.append(.upsert(try decodeRow(value)))
      }
    }
    try await applyBatch(
      CollectionChangeBatch(
        changes: accepted,
        expectedCursor: expectedCursor,
        cursor: StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue),
        sourceVersion: latest
      ))
    cursor = StreamCursor(offset: nextCursor.offset, lsn: latest.rawValue)
    sourceVersion = latest
  }
}
