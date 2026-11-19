import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation

enum ProbeStoreError: Error, Equatable, Sendable {
  case unknownCollection(String)
  case unsupportedGroupMember(String)
}

/// A file-backed `CollectionStore` and `CollectionGroupStore` for the probe: canonical rows, row
/// claims, each materialization's fence, source version and cursor, and stale marks, all in one
/// JSON file. Every mutating method builds the next state, replaces the file atomically and only
/// then adopts it, so rows and the cursor that positions them become visible together, and a new
/// process that opens the file continues from exactly what the last one committed.
///
/// It follows `InMemoryCollectionStore` and `InMemoryCollectionGroupStore` rule for rule. Like
/// them, it reports a materialization with the demand that last bound it, generation included.
actor ProbeStore: CollectionStore, CollectionGroupStore {
  typealias Model = ChangeRow
  typealias Key = String

  /// A nil row is a versioned tombstone, as in the reference store.
  struct StoredRow: Codable, Sendable {
    var row: ChangeRow?
    var version: CollectionSourceVersion
  }

  struct State: Codable, Sendable {
    /// Domain (collection, principal, authorization) → key → row.
    var rows: [String: [String: StoredRow]] = [:]
    /// `cacheKey` → the one stored copy for it.
    var records: [String: CollectionMaterializationRecord] = [:]
    /// Materialization ID → the demand it is bound to.
    var bindings: [String: CollectionDemandIdentity] = [:]
    /// Materialization ID → the keys it claims.
    var claims: [String: Set<String>] = [:]
    var staleMarks: [String: CollectionSourceVersion] = [:]
    /// Group materialization ID → its record.
    var groupRecords: [String: CollectionGroupMaterializationRecord] = [:]
    /// Group materialization ID → domain → the keys it claims there.
    var groupClaims: [String: [String: Set<String>]] = [:]
  }

  private let url: URL
  private let output: ProbeOutput
  /// Collection → the column whose value keys its rows.
  private let keyColumns: [String: String]
  /// Group materialization ID → the scope it belongs to, for the report.
  private let groupNames: [String: String]
  private var state: State

  init(
    url: URL, keyColumns: [String: String], groupNames: [String: String], output: ProbeOutput
  ) throws {
    self.url = url
    self.keyColumns = keyColumns
    self.groupNames = groupNames
    self.output = output
    if let data = try? Data(contentsOf: url) {
      state = try JSONDecoder().decode(State.self, from: data)
    } else {
      state = State()
    }
  }

  /// Whether this process opened a store an earlier process wrote.
  var isEmpty: Bool { state.records.isEmpty && state.groupRecords.isEmpty }

  private func commit(_ next: State) throws {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    try encoder.encode(next).write(to: url, options: .atomic)
    state = next
  }

  private static func frame(_ parts: [String]) -> String {
    parts.map { "\($0.utf8.count):\($0)" }.joined(separator: "|")
  }

  private static func domain(_ demand: CollectionDemandIdentity) -> String {
    frame([demand.collection.rawValue, demand.scope.principal, demand.scope.authorization])
  }

  private static func domain(_ collection: CollectionID, _ scope: CollectionScope) -> String {
    frame([collection.rawValue, scope.principal, scope.authorization])
  }

  private func key(_ row: ChangeRow, in collection: CollectionID) throws -> String {
    guard let column = keyColumns[collection.rawValue] else {
      throw ProbeStoreError.unknownCollection(collection.rawValue)
    }
    return ProbeJSON.key(row[column])
  }

  private static func isClaimed(_ key: String, domain: String, in state: State) -> Bool {
    let single = state.claims.contains { id, keys in
      keys.contains(key) && state.bindings[id].map(Self.domain) == domain
    }
    return single || state.groupClaims.values.contains { $0[domain]?.contains(key) == true }
  }

  private static func later(
    _ current: CollectionSourceVersion, _ received: CollectionSourceVersion
  ) -> CollectionSourceVersion {
    received.order >= current.order ? received : current
  }

  private static func markOtherHolders(
    of key: String, dropping id: String, domain: String, at position: CollectionSourceVersion,
    in state: inout State
  ) {
    for record in state.records.values
    where record.id.rawValue != id && Self.domain(record.demand) == domain
      && state.claims[record.id.rawValue]?.contains(key) == true
    {
      let existing = state.staleMarks[record.id.rawValue]
      state.staleMarks[record.id.rawValue] = existing.map { later($0, position) } ?? position
    }
  }

  /// Keeps a tombstone until every materialization holding its domain has applied the delete's
  /// source version.
  private static func pruneTombstones(_ state: inout State) {
    for (domain, rows) in state.rows {
      state.rows[domain] = rows.filter { _, stored in
        guard stored.row == nil else { return true }
        let single = state.records.values.contains { record in
          Self.domain(record.demand) == domain && !record.sourceVersion.supersedes(stored.version)
        }
        let group = state.groupRecords.values.contains { record in
          record.collections.contains { Self.domain($0, record.demand.scope) == domain }
            && !record.sourceVersion.supersedes(stored.version)
        }
        return single || group
      }
    }
  }

  private static func version(_ version: CollectionSourceVersion) -> [String: JSONValue] {
    var fields: [String: JSONValue] = ["lsn": .string(version.rawValue)]
    if let txid = version.transactionID { fields["txid"] = .int(Int64(txid)) }
    return fields
  }

  // MARK: - CollectionStore

  func materialization(for demand: CollectionDemandIdentity) async throws
    -> CollectionMaterializationRecord?
  {
    state.records[demand.cacheKey]
  }

  func staleMaterializations() async throws -> [CollectionStaleMaterialization] {
    state.records.values.compactMap { record in
      guard let position = state.staleMarks[record.id.rawValue] else { return nil }
      return CollectionStaleMaterialization(
        record: record, markedAt: position,
        claimCount: state.claims[record.id.rawValue]?.count ?? 0)
    }
  }

  func clearStale(
    _ materializationID: CollectionMaterializationID, ifMarkedAt position: CollectionSourceVersion
  ) async throws {
    guard state.staleMarks[materializationID.rawValue] == position else { return }
    var next = state
    next.staleMarks.removeValue(forKey: materializationID.rawValue)
    try commit(next)
  }

  func replaceSnapshot(
    _ snapshot: CollectionSnapshot<ChangeRow>,
    materializationID: CollectionMaterializationID,
    demand: CollectionDemandIdentity
  ) async throws {
    let id = materializationID.rawValue
    if let existing = state.bindings[id], existing.cacheKey != demand.cacheKey {
      throw CollectionStoreError.materializationBoundToDifferentDemand(
        materializationID: materializationID, existing: existing, requested: demand)
    }
    if let existing = state.records[demand.cacheKey], existing.id != materializationID {
      throw CollectionStoreError.demandBoundToDifferentMaterialization(
        demand: demand, existing: existing.id, requested: materializationID)
    }
    if let current = state.records[demand.cacheKey], snapshot.sourceVersion < current.sourceVersion
    {
      throw CollectionStoreError.staleSnapshot(
        current: current.sourceVersion, received: snapshot.sourceVersion)
    }
    let domain = Self.domain(demand)
    var next = state
    let oldClaims = next.claims[id] ?? []
    let keyed = try snapshot.rows.map { (try key($0, in: demand.collection), $0) }
    let newClaims = Set(keyed.map(\.0))
    next.bindings[id] = demand
    next.claims[id] = newClaims
    for oldKey in oldClaims.subtracting(newClaims) {
      Self.markOtherHolders(
        of: oldKey, dropping: id, domain: domain, at: snapshot.sourceVersion, in: &next)
      if !Self.isClaimed(oldKey, domain: domain, in: next), next.rows[domain]?[oldKey]?.row != nil {
        next.rows[domain]?.removeValue(forKey: oldKey)
      }
    }
    for (rowKey, row) in keyed {
      if let existing = next.rows[domain]?[rowKey],
        !snapshot.sourceVersion.supersedes(existing.version)
      {
        continue
      }
      next.rows[domain, default: [:]][rowKey] = StoredRow(row: row, version: snapshot.sourceVersion)
    }
    next.records[demand.cacheKey] = CollectionMaterializationRecord(
      id: materializationID, demand: demand, state: .live, snapshotFence: snapshot.fence,
      sourceVersion: snapshot.sourceVersion, cursor: snapshot.cursor)
    Self.pruneTombstones(&next)
    try commit(next)
    output.emit(
      "snapshot",
      [
        "scope": .string(demand.collection.rawValue), "materialization": .string(id),
        "generation": .string(demand.scope.generation),
        "rows": .int(Int64(snapshot.rows.count)),
        "keys": .array(newClaims.sorted().map(JSONValue.string)),
        "fence": .string(snapshot.fence.rawValue),
        "cursor": snapshot.cursor.map { .string($0.offset) } ?? .null,
        "lsn": .string(snapshot.sourceVersion.rawValue),
      ])
  }

  func apply(
    _ batch: CollectionChangeBatch<ChangeRow, String>,
    to materializationID: CollectionMaterializationID
  ) async throws {
    let id = materializationID.rawValue
    guard let demand = state.bindings[id], let record = state.records[demand.cacheKey] else {
      throw CollectionStoreError.missingMaterialization(materializationID)
    }
    if record.cursor == batch.cursor { return }
    guard record.cursor == batch.expectedCursor else {
      throw CollectionStoreError.cursorConflict(
        expected: batch.expectedCursor, actual: record.cursor, advancingTo: batch.cursor)
    }
    let domain = Self.domain(demand)
    var next = state
    var claims = next.claims[id] ?? []
    var events: [[String: JSONValue]] = []
    for change in batch.changes {
      switch change {
      case .upsert(let row, let version):
        let rowKey = try key(row, in: demand.collection)
        if next.rows[domain]?[rowKey].map({ version.supersedes($0.version) }) ?? true {
          next.rows[domain, default: [:]][rowKey] = StoredRow(row: row, version: version)
        }
        claims.insert(rowKey)
        events.append(
          Self.version(version).merging([
            "op": .string("upsert"), "key": .string(rowKey), "row": .object(row),
          ]) { $1 })
      case .delete(let rowKey, let version):
        if claims.remove(rowKey) != nil {
          Self.markOtherHolders(of: rowKey, dropping: id, domain: domain, at: version, in: &next)
        }
        events.append(
          Self.version(version).merging(["op": .string("delete"), "key": .string(rowKey)]) { $1 })
      }
    }
    next.claims[id] = claims
    for change in batch.changes {
      guard case .delete(let rowKey, let version) = change,
        !Self.isClaimed(rowKey, domain: domain, in: next)
      else { continue }
      if next.rows[domain]?[rowKey].map({ version.supersedes($0.version) }) ?? true {
        next.rows[domain, default: [:]][rowKey] = StoredRow(row: nil, version: version)
      }
    }
    next.records[demand.cacheKey] = CollectionMaterializationRecord(
      id: record.id, demand: record.demand, state: .live, snapshotFence: record.snapshotFence,
      sourceVersion: Self.later(record.sourceVersion, batch.sourceVersion), cursor: batch.cursor)
    Self.pruneTombstones(&next)
    try commit(next)
    let scope = demand.collection.rawValue
    for event in events {
      output.emit("change", event.merging(["scope": .string(scope)]) { $1 })
    }
    output.emit(
      "batch",
      [
        "scope": .string(scope), "changes": .int(Int64(batch.changes.count)),
        "from": batch.expectedCursor.map { .string($0.offset) } ?? .null,
        "cursor": .string(batch.cursor.offset),
      ])
  }

  func removeMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    let id = materializationID.rawValue
    var next = state
    next.staleMarks.removeValue(forKey: id)
    guard let demand = next.bindings[id] else {
      try commit(next)
      return
    }
    let domain = Self.domain(demand)
    let oldClaims = next.claims.removeValue(forKey: id) ?? []
    if next.records[demand.cacheKey]?.id == materializationID {
      next.records.removeValue(forKey: demand.cacheKey)
    }
    next.bindings.removeValue(forKey: id)
    for rowKey in oldClaims
    where !Self.isClaimed(rowKey, domain: domain, in: next)
      && next.rows[domain]?[rowKey]?.row != nil
    {
      next.rows[domain]?.removeValue(forKey: rowKey)
    }
    Self.pruneTombstones(&next)
    try commit(next)
  }

  func rebindMaterialization(
    _ materializationID: CollectionMaterializationID, to demand: CollectionDemandIdentity
  ) async throws -> Bool {
    let id = materializationID.rawValue
    guard let record = state.records[demand.cacheKey], record.id == materializationID,
      state.bindings[id]?.cacheKey == demand.cacheKey
    else { return false }
    var next = state
    next.records[demand.cacheKey] = CollectionMaterializationRecord(
      id: record.id, demand: demand, state: record.state, snapshotFence: record.snapshotFence,
      sourceVersion: record.sourceVersion, cursor: record.cursor)
    next.bindings[id] = demand
    try commit(next)
    output.emit(
      "rebound",
      [
        "scope": .string(demand.collection.rawValue), "materialization": .string(id),
        "from": .string(record.demand.scope.generation),
        "generation": .string(demand.scope.generation),
      ])
    return true
  }

  // MARK: - CollectionGroupStore

  func groupMaterialization(_ materializationID: CollectionMaterializationID) async throws
    -> CollectionGroupMaterializationRecord?
  {
    state.groupRecords[materializationID.rawValue]
  }

  func replaceGroupSnapshot(
    _ snapshot: CollectionGroupSnapshot,
    materializationID: CollectionMaterializationID,
    demand: CollectionGroupDemandIdentity
  ) async throws {
    let id = materializationID.rawValue
    if let existing = state.groupRecords[id] {
      // One copy per group key: a later generation's snapshot replaces the copy in place.
      guard existing.demand.cacheKey == demand.cacheKey else {
        throw CollectionGroupStoreError.materializationBoundToDifferentDemand(
          materializationID: materializationID, existing: existing.demand, requested: demand)
      }
      if snapshot.sourceVersion < existing.sourceVersion {
        throw CollectionStoreError.staleSnapshot(
          current: existing.sourceVersion, received: snapshot.sourceVersion)
      }
    }
    var next = state
    var newClaims: [String: Set<String>] = [:]
    var counts: [String: JSONValue] = [:]
    for member in snapshot.members {
      guard let typed = member as? CollectionGroupRows<ChangeRow, String> else {
        throw ProbeStoreError.unsupportedGroupMember(member.table)
      }
      let domain = Self.domain(typed.collection, demand.scope)
      var keys = Set<String>()
      for row in typed.rows {
        let rowKey = typed.key(row)
        keys.insert(rowKey)
        if let existing = next.rows[domain]?[rowKey],
          !snapshot.sourceVersion.supersedes(existing.version)
        {
          continue
        }
        next.rows[domain, default: [:]][rowKey] = StoredRow(
          row: row, version: snapshot.sourceVersion)
      }
      newClaims[domain] = keys
      counts[typed.table] = .array(keys.sorted().map(JSONValue.string))
    }
    let oldClaims = next.groupClaims[id] ?? [:]
    next.groupClaims[id] = newClaims
    for (domain, keys) in oldClaims {
      for dropped in keys.subtracting(newClaims[domain] ?? [])
      where !Self.isClaimed(dropped, domain: domain, in: next)
        && next.rows[domain]?[dropped]?.row != nil
      {
        next.rows[domain]?.removeValue(forKey: dropped)
      }
    }
    next.groupRecords[id] = CollectionGroupMaterializationRecord(
      id: materializationID, demand: demand, collections: snapshot.members.map(\.collection),
      snapshotFence: snapshot.fence, sourceVersion: snapshot.sourceVersion,
      cursor: snapshot.cursor)
    Self.pruneTombstones(&next)
    try commit(next)
    output.emit(
      "snapshot",
      [
        "scope": .string(groupNames[id] ?? id), "materialization": .string(id),
        "generation": .string(demand.scope.generation), "keys": .object(counts),
        "rows": .int(Int64(newClaims.values.reduce(0) { $0 + $1.count })),
        "fence": .string(snapshot.fence.rawValue),
        "cursor": snapshot.cursor.map { .string($0.offset) } ?? .null,
        "lsn": .string(snapshot.sourceVersion.rawValue),
      ])
  }

  func applyGroup(
    _ batch: CollectionGroupChangeBatch,
    to materializationID: CollectionMaterializationID
  ) async throws {
    let id = materializationID.rawValue
    guard let record = state.groupRecords[id] else {
      throw CollectionStoreError.missingMaterialization(materializationID)
    }
    if record.cursor == batch.cursor { return }
    guard record.cursor == batch.expectedCursor else {
      throw CollectionStoreError.cursorConflict(
        expected: batch.expectedCursor, actual: record.cursor, advancingTo: batch.cursor)
    }
    var next = state
    var claims = next.groupClaims[id] ?? [:]
    var deleted: [(domain: String, key: String, version: CollectionSourceVersion)] = []
    var events: [[String: JSONValue]] = []
    for member in batch.members {
      guard record.collections.contains(member.collection) else {
        throw CollectionGroupStoreError.collectionOutsideGroup(member.collection)
      }
      guard let typed = member as? CollectionGroupChanges<ChangeRow, String> else {
        throw ProbeStoreError.unsupportedGroupMember(member.table)
      }
      let domain = Self.domain(typed.collection, record.demand.scope)
      for change in typed.changes {
        switch change {
        case .upsert(let row, let version):
          let rowKey = typed.key(row)
          if next.rows[domain]?[rowKey].map({ version.supersedes($0.version) }) ?? true {
            next.rows[domain, default: [:]][rowKey] = StoredRow(row: row, version: version)
          }
          claims[domain, default: []].insert(rowKey)
          events.append(
            Self.version(version).merging([
              "op": .string("upsert"), "key": .string(rowKey), "row": .object(row),
              "table": .string(typed.table),
            ]) { $1 })
        case .delete(let rowKey, let version):
          claims[domain]?.remove(rowKey)
          deleted.append((domain, rowKey, version))
          events.append(
            Self.version(version).merging([
              "op": .string("delete"), "key": .string(rowKey), "table": .string(typed.table),
            ]) { $1 })
        }
      }
    }
    next.groupClaims[id] = claims
    for (domain, rowKey, version) in deleted
    where !Self.isClaimed(rowKey, domain: domain, in: next) {
      if next.rows[domain]?[rowKey].map({ version.supersedes($0.version) }) ?? true {
        next.rows[domain, default: [:]][rowKey] = StoredRow(row: nil, version: version)
      }
    }
    next.groupRecords[id] = CollectionGroupMaterializationRecord(
      id: record.id, demand: record.demand, collections: record.collections,
      snapshotFence: record.snapshotFence,
      sourceVersion: Self.later(record.sourceVersion, batch.sourceVersion), cursor: batch.cursor)
    Self.pruneTombstones(&next)
    try commit(next)
    let scope = groupNames[id] ?? id
    for event in events {
      output.emit("change", event.merging(["scope": .string(scope)]) { $1 })
    }
    output.emit(
      "batch",
      [
        "scope": .string(scope), "changes": .int(Int64(events.count)),
        "from": batch.expectedCursor.map { .string($0.offset) } ?? .null,
        "cursor": .string(batch.cursor.offset),
      ])
  }

  func removeGroupMaterialization(_ materializationID: CollectionMaterializationID) async throws {
    let id = materializationID.rawValue
    var next = state
    let oldClaims = next.groupClaims.removeValue(forKey: id) ?? [:]
    next.groupRecords.removeValue(forKey: id)
    for (domain, keys) in oldClaims {
      for rowKey in keys
      where !Self.isClaimed(rowKey, domain: domain, in: next)
        && next.rows[domain]?[rowKey]?.row != nil
      {
        next.rows[domain]?.removeValue(forKey: rowKey)
      }
    }
    Self.pruneTombstones(&next)
    try commit(next)
  }

  // MARK: - Reports

  /// The rows the single-table copy for `demand` claims, by key.
  func rows(for demand: CollectionDemandIdentity) -> [String: ChangeRow] {
    guard let record = state.records[demand.cacheKey] else { return [:] }
    let domain = Self.domain(demand)
    var result: [String: ChangeRow] = [:]
    for rowKey in state.claims[record.id.rawValue] ?? [] {
      if let row = state.rows[domain]?[rowKey]?.row { result[rowKey] = row }
    }
    return result
  }

  /// The rows a group copy claims in one member collection, by key.
  func groupRows(
    _ materializationID: CollectionMaterializationID, collection: CollectionID,
    scope: CollectionScope
  ) -> [String: ChangeRow] {
    let domain = Self.domain(collection, scope)
    var result: [String: ChangeRow] = [:]
    for rowKey in state.groupClaims[materializationID.rawValue]?[domain] ?? [] {
      if let row = state.rows[domain]?[rowKey]?.row { result[rowKey] = row }
    }
    return result
  }

  /// The stored position of a single-table copy, for the report.
  func position(for demand: CollectionDemandIdentity) -> [String: JSONValue] {
    guard let record = state.records[demand.cacheKey] else { return ["stored": .bool(false)] }
    return [
      "stored": .bool(true), "materialization": .string(record.id.rawValue),
      "generation": .string(record.demand.scope.generation),
      "fence": .string(record.snapshotFence.rawValue),
      "cursor": record.cursor.map { .string($0.offset) } ?? .null,
      "lsn": .string(record.sourceVersion.rawValue),
    ]
  }

  func groupPosition(_ materializationID: CollectionMaterializationID) -> [String: JSONValue] {
    guard let record = state.groupRecords[materializationID.rawValue] else {
      return ["stored": .bool(false)]
    }
    return [
      "stored": .bool(true), "materialization": .string(record.id.rawValue),
      "generation": .string(record.demand.scope.generation),
      "fence": .string(record.snapshotFence.rawValue),
      "cursor": record.cursor.map { .string($0.offset) } ?? .null,
      "lsn": .string(record.sourceVersion.rawValue),
    ]
  }
}
