import ElectricCircuitsSwift
import Foundation

/// Snapshot plus gap-free live tail selected by a source adapter for one collection demand.
/// `stop` must release the remote resource; the coordinator guarantees at-most-once invocation.
public struct CollectionSourceSession<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  public let snapshot: CollectionSnapshot<Model>
  public let run:
    @Sendable (
      @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
    ) async throws -> Void
  public let stop: @Sendable () async throws -> Void

  public init(
    snapshot: CollectionSnapshot<Model>,
    run:
      @escaping @Sendable (
        @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
      ) async throws -> Void,
    stop: @escaping @Sendable () async throws -> Void
  ) {
    self.snapshot = snapshot
    self.run = run
    self.stop = stop
  }
}

/// A gap-free live tail that continues a stored materialization from its committed cursor,
/// without a new snapshot: a `CollectionSourceSession` minus the snapshot. The store already holds
/// the rows, so the coordinator never calls `replaceSnapshot` for it.
///
/// `cursor` and `sourceVersion` are the stored position the tail starts from; the first batch's
/// `expectedCursor` is `cursor`. `run` delivers batches through `apply` exactly as a
/// `CollectionSourceSession` does. It calls `caughtUp` when a read from the resumed position
/// returned without changes (an idle or up-to-date poll); a committed `apply` already proves the
/// tail made progress, so the session need not call both. Until one of them happens, the
/// coordinator reports the demand as `.cached`. A session whose feed will not serve `cursor` throws
/// `CollectionResumeInvalidated` from `run`; any other error fails the attempt like a snapshot
/// session's. `stop` must release the remote resource; the coordinator guarantees at-most-once
/// invocation.
public struct CollectionResumeSession<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  public let cursor: StreamCursor
  public let sourceVersion: CollectionSourceVersion
  public let run:
    @Sendable (
      _ apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void,
      _ caughtUp: @escaping @Sendable () async -> Void
    ) async throws -> Void
  public let stop: @Sendable () async throws -> Void

  public init(
    cursor: StreamCursor,
    sourceVersion: CollectionSourceVersion,
    run:
      @escaping @Sendable (
        _ apply: @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void,
        _ caughtUp: @escaping @Sendable () async -> Void
      ) async throws -> Void,
    stop: @escaping @Sendable () async throws -> Void
  ) {
    self.cursor = cursor
    self.sourceVersion = sourceVersion
    self.run = run
    self.stop = stop
  }
}

/// Thrown from `CollectionResumeSession.run` when the feed will not serve the stored cursor: a
/// terminal stream outcome (`404`, `410`, `stream-closed`) or another answer a re-join from the
/// same cursor would repeat. A stale cursor is an ordinary reseed, so before the resumed tail has
/// committed or caught up the coordinator stops the session and snapshots in the same attempt.
/// After that, the demand has been reported live and the attempt fails as any tail would.
/// Transient failures (transport loss, exhausted retries, capacity) throw their own error: they
/// say nothing about the cursor, and the next attempt resumes again.
public struct CollectionResumeInvalidated: Error, Sendable {
  public let cause: any Error

  public init(cause: any Error) {
    self.cause = cause
  }
}

/// Provider seam that hides whether a demand is backed by a shape, subset snapshot/feed pair, or
/// another server-native materialization strategy.
public protocol CollectionSourceAdapter<Model, Key>: Sendable {
  associatedtype Model: Sendable
  associatedtype Key: Hashable & Sendable

  func materialize(
    _ demand: CollectionDemand<Model>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<Model, Key>

  /// Continues an exact cached materialization from `record`'s committed cursor, or returns nil
  /// when this source cannot prove the stored cursor still names a gap-free position in the same
  /// live feed. The coordinator asks this before `materialize` whenever the store holds an exact
  /// record for the demand, and takes the snapshot path on nil. A thrown error fails the attempt
  /// as a source failure, and the next attempt asks again. The default returns nil, so every
  /// acquire snapshots.
  func resume(
    _ demand: CollectionDemand<Model>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID,
    record: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<Model, Key>?

  /// Retries cleanup for a remote feed that was created before local source setup failed. The
  /// coordinator calls this before replacing or finally releasing an attempt; implementations
  /// retain this authority until a successful or already-retired release.
  func cleanupAbandonedMaterialization(_ materializationID: CollectionMaterializationID)
    async throws
}

extension CollectionSourceAdapter {
  public func resume(
    _: CollectionDemand<Model>,
    identity _: CollectionDemandIdentity,
    materializationID _: CollectionMaterializationID,
    record _: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<Model, Key>? {
    nil
  }

  public func cleanupAbandonedMaterialization(
    _: CollectionMaterializationID
  ) async throws {
    // Most adapters only acquire a remote resource after they can return a session.
  }
}
