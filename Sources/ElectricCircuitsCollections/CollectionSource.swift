import Foundation

/// Snapshot plus gap-free live tail selected by a source adapter for one collection demand.
/// `stop` must release the remote resource; the coordinator guarantees at-most-once invocation.
public struct CollectionSourceSession<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  public let snapshot: CollectionSnapshot<Model>
  public let run:
    @Sendable (
      @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
    ) async throws -> Void
  public let stop: @Sendable () async -> Void

  public init(
    snapshot: CollectionSnapshot<Model>,
    run:
      @escaping @Sendable (
        @escaping @Sendable (CollectionChangeBatch<Model, Key>) async throws -> Void
      ) async throws -> Void,
    stop: @escaping @Sendable () async -> Void
  ) {
    self.snapshot = snapshot
    self.run = run
    self.stop = stop
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
}
