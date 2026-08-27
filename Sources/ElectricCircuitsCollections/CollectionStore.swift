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

  /// Explicit bounded-cache lifecycle. Providers remove the materialization's claims and any
  /// now-unclaimed canonical rows in the same transaction.
  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws
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
  private struct Domain: Hashable, Sendable {
    let collection: CollectionID
    let scope: CollectionScope
  }
  private struct CanonicalKey: Hashable, Sendable {
    let domain: Domain
    let key: Key
  }
  private var canonicalRows: [CanonicalKey: (row: Model, version: CollectionSourceVersion)] = [:]
  private var recordsByDemand: [CollectionDemandIdentity: CollectionMaterializationRecord] = [:]
  private var demandByMaterialization: [CollectionMaterializationID: CollectionDemandIdentity] = [:]
  private var claims: [CollectionMaterializationID: Set<Key>] = [:]

  private func domain(for demand: CollectionDemandIdentity) -> Domain {
    Domain(collection: demand.collection, scope: demand.scope)
  }

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
    // A stale overlapping snapshot may advance no local state and must never replace newer live
    // data already committed by this materialization.
    if let current = recordsByDemand[demand], snapshot.sourceVersion < current.sourceVersion {
      return
    }
    let domain = domain(for: demand)
    var nextRows = canonicalRows
    var nextClaims = claims
    nextClaims[materializationID] = newClaims

    for oldKey in oldClaims.subtracting(newClaims) {
      if !isClaimed(oldKey, in: nextClaims, domain: domain, current: materializationID) {
        nextRows.removeValue(forKey: CanonicalKey(domain: domain, key: oldKey))
      }
    }
    for row in snapshot.rows {
      let rowKey = CanonicalKey(domain: domain, key: key(row))
      if let existing = nextRows[rowKey], existing.version > snapshot.sourceVersion {
        continue
      }
      nextRows[rowKey] = (row, snapshot.sourceVersion)
    }

    let record = CollectionMaterializationRecord(
      id: materializationID,
      demand: demand,
      state: .live,
      snapshotFence: snapshot.fence,
      sourceVersion: snapshot.sourceVersion,
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

    let domain = domain(for: demand)
    var nextRows = canonicalRows
    var nextClaims = claims
    var materializationClaims = nextClaims[materializationID] ?? []
    // Claims are membership facts, not a row-byte version. An older overlapping feed cannot
    // replace newer canonical bytes, but it still proves that its materialization owns the key.
    // Apply every claim delta while comparing source versions only at the canonical byte layer.
    for change in batch.changes {
      switch change {
      case .upsert(let row):
        let rowKey = key(row)
        let canonicalKey = CanonicalKey(domain: domain, key: rowKey)
        if nextRows[canonicalKey].map({ $0.version <= batch.sourceVersion }) ?? true {
          nextRows[canonicalKey] = (row, batch.sourceVersion)
        }
        materializationClaims.insert(rowKey)
      case .delete(let rowKey):
        materializationClaims.remove(rowKey)
      }
    }
    nextClaims[materializationID] = materializationClaims
    for change in batch.changes {
      guard case .delete(let rowKey) = change else { continue }
      if !isClaimed(rowKey, in: nextClaims, domain: domain, current: materializationID) {
        nextRows.removeValue(forKey: CanonicalKey(domain: domain, key: rowKey))
      }
    }

    let nextRecord = CollectionMaterializationRecord(
      id: record.id,
      demand: record.demand,
      state: .live,
      snapshotFence: record.snapshotFence,
      sourceVersion: max(record.sourceVersion, batch.sourceVersion),
      cursor: batch.cursor
    )
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand[demand] = nextRecord
  }

  /// Convenience inspection for one-domain test stores. Production providers should expose their
  /// own scoped query API rather than flattening identities from different principals.
  public func rows(for demand: CollectionDemandIdentity) -> [Key: Model] {
    let domain = domain(for: demand)
    return canonicalRows.reduce(into: [:]) { result, pair in
      if pair.key.domain == domain { result[pair.key.key] = pair.value.row }
    }
  }

  public func rows() -> [Key: Model] {
    canonicalRows.reduce(into: [:]) { result, pair in
      result[pair.key.key] = pair.value.row
    }
  }

  public func rowClaims(for materializationID: CollectionMaterializationID) -> Set<Key> {
    claims[materializationID] ?? []
  }

  public func removeMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    guard let demand = demandByMaterialization.removeValue(forKey: materializationID) else {
      return
    }
    let domain = domain(for: demand)
    let oldClaims = claims.removeValue(forKey: materializationID) ?? []
    recordsByDemand.removeValue(forKey: demand)
    for rowKey in oldClaims where !isClaimed(rowKey, in: claims, domain: domain, current: nil) {
      canonicalRows.removeValue(forKey: CanonicalKey(domain: domain, key: rowKey))
    }
  }

  private func isClaimed(
    _ rowKey: Key,
    in claims: [CollectionMaterializationID: Set<Key>],
    domain: Domain,
    current: CollectionMaterializationID?
  ) -> Bool {
    claims.contains { id, keys in
      guard keys.contains(rowKey) else { return false }
      if id == current { return true }
      return demandByMaterialization[id].map { self.domain(for: $0) == domain } ?? false
    }
  }
}
