import ElectricCircuitsSwift
import Foundation

/// Atomic persistence boundary for canonical rows, materialization row claims, snapshot fences and
/// live cursors. A conforming durable provider must commit each mutating method as one transaction.
public protocol CollectionStore<Model, Key>: Sendable {
  associatedtype Model: Sendable
  associatedtype Key: Hashable & Sendable

  func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?

  func replaceSnapshot(
    _ snapshot: CollectionSnapshot<Model>,
    materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws

  func apply(
    _ batch: CollectionChangeBatch<Model, Key>,
    to materializationID: CollectionMaterializationID
  ) async throws
}

public enum CollectionStoreError: Error, Equatable, Sendable {
  case missingMaterialization(CollectionMaterializationID)
  case cursorConflict(expected: StreamCursor?, actual: StreamCursor?, advancingTo: StreamCursor)
}

/// Reference store for contract tests, previews and applications that do not require disk-backed
/// offline state. It models the same one-domain-table plus shared row-claim topology required of a
/// durable provider.
public actor InMemoryCollectionStore<Model: Sendable, Key: Hashable & Sendable>:
  CollectionStore
{
  private let key: @Sendable (Model) -> Key
  private var canonicalRows: [Key: Model] = [:]
  private var recordsByDemand: [CollectionDemandIdentity: CollectionMaterializationRecord] = [:]
  private var demandByMaterialization: [CollectionMaterializationID: CollectionDemandIdentity] = [:]
  private var claims: [CollectionMaterializationID: Set<Key>] = [:]

  public init(key: @escaping @Sendable (Model) -> Key) {
    self.key = key
  }

  public func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?
  {
    recordsByDemand[demand]
  }

  public func replaceSnapshot(
    _ snapshot: CollectionSnapshot<Model>,
    materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    let oldClaims = claims[materializationID] ?? []
    let newClaims = Set(snapshot.rows.map(key))
    var nextRows = canonicalRows
    var nextClaims = claims
    nextClaims[materializationID] = newClaims

    for oldKey in oldClaims.subtracting(newClaims) {
      if !Self.isClaimed(oldKey, in: nextClaims) {
        nextRows.removeValue(forKey: oldKey)
      }
    }
    for row in snapshot.rows {
      nextRows[key(row)] = row
    }

    let record = CollectionMaterializationRecord(
      id: materializationID,
      demand: demand,
      state: .live,
      snapshotFence: snapshot.fence,
      cursor: snapshot.cursor
    )
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand[demand] = record
    demandByMaterialization[materializationID] = demand
  }

  public func apply(
    _ batch: CollectionChangeBatch<Model, Key>,
    to materializationID: CollectionMaterializationID
  ) async throws {
    guard let demand = demandByMaterialization[materializationID],
      let record = recordsByDemand[demand]
    else {
      throw CollectionStoreError.missingMaterialization(materializationID)
    }
    if record.cursor == batch.cursor { return }
    guard record.cursor == batch.expectedCursor else {
      throw CollectionStoreError.cursorConflict(
        expected: batch.expectedCursor,
        actual: record.cursor,
        advancingTo: batch.cursor
      )
    }

    var nextRows = canonicalRows
    var nextClaims = claims
    var materializationClaims = nextClaims[materializationID] ?? []
    for change in batch.changes {
      switch change {
      case .upsert(let row):
        let rowKey = key(row)
        nextRows[rowKey] = row
        materializationClaims.insert(rowKey)
      case .delete(let rowKey):
        materializationClaims.remove(rowKey)
      }
    }
    nextClaims[materializationID] = materializationClaims
    for change in batch.changes {
      guard case .delete(let rowKey) = change else { continue }
      if !Self.isClaimed(rowKey, in: nextClaims) {
        nextRows.removeValue(forKey: rowKey)
      }
    }

    let nextRecord = CollectionMaterializationRecord(
      id: record.id,
      demand: record.demand,
      state: .live,
      snapshotFence: record.snapshotFence,
      cursor: batch.cursor
    )
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand[demand] = nextRecord
  }

  public func rows() -> [Key: Model] { canonicalRows }

  public func rowClaims(for materializationID: CollectionMaterializationID) -> Set<Key> {
    claims[materializationID] ?? []
  }

  private static func isClaimed(
    _ rowKey: Key,
    in claims: [CollectionMaterializationID: Set<Key>]
  ) -> Bool {
    claims.values.contains { $0.contains(rowKey) }
  }
}
