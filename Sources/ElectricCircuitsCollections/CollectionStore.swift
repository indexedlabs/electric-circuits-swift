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
  case materializationBoundToDifferentDemand(
    materializationID: CollectionMaterializationID,
    existing: CollectionDemandIdentity,
    requested: CollectionDemandIdentity
  )
  case demandBoundToDifferentMaterialization(
    demand: CollectionDemandIdentity,
    existing: CollectionMaterializationID,
    requested: CollectionMaterializationID
  )
  case staleSnapshot(current: CollectionSourceVersion, received: CollectionSourceVersion)
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
  /// A nil row is a versioned tombstone. It prevents an older overlapping feed from resurrecting a
  /// key after a later delete while retaining no application-visible row.
  private var canonicalRows: [CanonicalKey: (row: Model?, version: CollectionSourceVersion)] = [:]
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
    if let existingDemand = demandByMaterialization[materializationID], existingDemand != demand {
      throw CollectionStoreError.materializationBoundToDifferentDemand(
        materializationID: materializationID, existing: existingDemand, requested: demand)
    }
    if let existing = recordsByDemand[demand], existing.id != materializationID {
      throw CollectionStoreError.demandBoundToDifferentMaterialization(
        demand: demand, existing: existing.id, requested: materializationID)
    }
    if let current = recordsByDemand[demand], snapshot.sourceVersion < current.sourceVersion {
      throw CollectionStoreError.staleSnapshot(
        current: current.sourceVersion, received: snapshot.sourceVersion)
    }
    let oldClaims = claims[materializationID] ?? []
    let newClaims = Set(snapshot.rows.map(key))
    let domain = domain(for: demand)
    var nextRows = canonicalRows
    var nextClaims = claims
    var nextRecords = recordsByDemand
    var nextDemandByMaterialization = demandByMaterialization
    nextDemandByMaterialization[materializationID] = demand
    nextClaims[materializationID] = newClaims

    for oldKey in oldClaims.subtracting(newClaims) {
      if !isClaimed(oldKey, in: nextClaims, domain: domain, bindings: nextDemandByMaterialization),
        nextRows[CanonicalKey(domain: domain, key: oldKey)]?.row != nil
      {
        nextRows.removeValue(forKey: CanonicalKey(domain: domain, key: oldKey))
      }
    }
    for row in snapshot.rows {
      let rowKey = CanonicalKey(domain: domain, key: key(row))
      if let existing = nextRows[rowKey], existing.version.order > snapshot.sourceVersion.order {
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
    nextRecords[demand] = record
    pruneTombstones(&nextRows, using: nextRecords)
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand = nextRecords
    demandByMaterialization = nextDemandByMaterialization
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
    var nextRecords = recordsByDemand
    var materializationClaims = nextClaims[materializationID] ?? []
    // Claims are membership facts, not a row-byte version. An older overlapping feed cannot
    // replace newer canonical bytes, but it still proves that its materialization owns the key.
    // Apply every claim delta while comparing source versions only at the canonical byte layer.
    for change in batch.changes {
      switch change {
      case .upsert(let row, let sourceVersion):
        let rowKey = key(row)
        let canonicalKey = CanonicalKey(domain: domain, key: rowKey)
        if nextRows[canonicalKey].map({ $0.version.order <= sourceVersion.order }) ?? true {
          nextRows[canonicalKey] = (row, sourceVersion)
        }
        materializationClaims.insert(rowKey)
      case .delete(let rowKey, _):
        materializationClaims.remove(rowKey)
      }
    }
    nextClaims[materializationID] = materializationClaims
    for change in batch.changes {
      guard case .delete(let rowKey, let sourceVersion) = change else { continue }
      if !isClaimed(rowKey, in: nextClaims, domain: domain, bindings: demandByMaterialization) {
        let canonicalKey = CanonicalKey(domain: domain, key: rowKey)
        if nextRows[canonicalKey].map({ $0.version.order <= sourceVersion.order }) ?? true {
          nextRows[canonicalKey] = (nil, sourceVersion)
        }
      }
    }

    let nextRecord = CollectionMaterializationRecord(
      id: record.id,
      demand: record.demand,
      state: .live,
      snapshotFence: record.snapshotFence,
      sourceVersion: later(of: record.sourceVersion, and: batch.sourceVersion),
      cursor: batch.cursor
    )
    nextRecords[demand] = nextRecord
    pruneTombstones(&nextRows, using: nextRecords)
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand = nextRecords
  }

  /// Convenience inspection for one-domain test stores. Production providers should expose their
  /// own scoped query API rather than flattening identities from different principals.
  public func rows(for demand: CollectionDemandIdentity) -> [Key: Model] {
    let domain = domain(for: demand)
    return canonicalRows.reduce(into: [:]) { result, pair in
      if pair.key.domain == domain, let row = pair.value.row { result[pair.key.key] = row }
    }
  }

  public func rows() -> [Key: Model] {
    canonicalRows.reduce(into: [:]) { result, pair in
      if let row = pair.value.row { result[pair.key.key] = row }
    }
  }

  public func rowClaims(for materializationID: CollectionMaterializationID) -> Set<Key> {
    claims[materializationID] ?? []
  }

  /// Reference-store diagnostic for testing bounded tombstone retention in one collection domain.
  public func tombstoneCount(for demand: CollectionDemandIdentity) -> Int {
    let domain = domain(for: demand)
    return canonicalRows.reduce(into: 0) { count, pair in
      if pair.key.domain == domain, pair.value.row == nil { count += 1 }
    }
  }

  public func removeMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    guard let demand = demandByMaterialization[materializationID] else {
      return
    }
    let domain = domain(for: demand)
    var nextRows = canonicalRows
    var nextClaims = claims
    var nextRecords = recordsByDemand
    var nextDemandByMaterialization = demandByMaterialization
    let oldClaims = nextClaims.removeValue(forKey: materializationID) ?? []
    nextRecords.removeValue(forKey: demand)
    nextDemandByMaterialization.removeValue(forKey: materializationID)
    for rowKey in oldClaims
    where !isClaimed(rowKey, in: nextClaims, domain: domain, bindings: nextDemandByMaterialization)
      && nextRows[CanonicalKey(domain: domain, key: rowKey)]?.row != nil
    {
      nextRows.removeValue(forKey: CanonicalKey(domain: domain, key: rowKey))
    }
    pruneTombstones(&nextRows, using: nextRecords)
    canonicalRows = nextRows
    claims = nextClaims
    recordsByDemand = nextRecords
    demandByMaterialization = nextDemandByMaterialization
  }

  /// Retains a tombstone until every materialization in its domain has durably applied the delete's
  /// source frontier. Once that holds, no remaining stream can lawfully deliver an older row.
  private func pruneTombstones(
    _ rows: inout [CanonicalKey: (row: Model?, version: CollectionSourceVersion)],
    using records: [CollectionDemandIdentity: CollectionMaterializationRecord]
  ) {
    rows = rows.filter { canonicalKey, value in
      guard value.row == nil else { return true }
      return records.contains { demand, record in
        domain(for: demand) == canonicalKey.domain && record.sourceVersion < value.version
      }
    }
  }

  private func later(
    of current: CollectionSourceVersion, and received: CollectionSourceVersion
  ) -> CollectionSourceVersion {
    received.order >= current.order ? received : current
  }

  private func isClaimed(
    _ rowKey: Key,
    in claims: [CollectionMaterializationID: Set<Key>],
    domain: Domain,
    bindings: [CollectionMaterializationID: CollectionDemandIdentity]
  ) -> Bool {
    claims.contains { id, keys in
      guard keys.contains(rowKey) else { return false }
      return bindings[id].map { self.domain(for: $0) == domain } ?? false
    }
  }
}
