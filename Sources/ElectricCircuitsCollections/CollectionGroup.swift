import ElectricCircuitsSwift
import Foundation

// MARK: - Group demands

/// One table of a group demand: the collection its rows land in, the table and predicate the
/// server evaluates, and the typed decoders for its rows and live keys.
public struct CollectionGroupMember: Sendable {
  public let collection: CollectionID
  public let table: String
  public let predicateIdentity: String
  public let sourcePredicate: ElectricCircuitsSwift.Predicate?
  public let columns: [String]?
  let codec: CollectionGroupMemberCodec

  /// Escape hatch for provider adapters, as `CollectionDemand(unsafePredicateIdentity:)` is: the
  /// caller keeps the identity and the wire predicate describing the same expression.
  public init<Model: Sendable, Key: Hashable & Sendable>(
    definition: CollectionDefinition<Model, Key>,
    table: String,
    unsafePredicateIdentity predicateIdentity: String,
    sourcePredicate: ElectricCircuitsSwift.Predicate? = nil,
    columns: [String]? = nil,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key
  ) {
    precondition(!table.isEmpty && !predicateIdentity.isEmpty)
    self.collection = definition.id
    self.table = table
    self.predicateIdentity = predicateIdentity
    self.sourcePredicate = sourcePredicate
    self.columns = columns
    codec = CollectionGroupMemberCodec(
      definition: definition, table: table, decodeRow: decodeRow, decodeKey: decodeKey)
  }

  public init<Model: Sendable, Key: Hashable & Sendable>(
    definition: CollectionDefinition<Model, Key>,
    table: String,
    predicate: CollectionPredicate<Model>,
    columns: [String]? = nil,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key
  ) {
    self.init(
      definition: definition, table: table,
      unsafePredicateIdentity: predicate.canonicalIdentity,
      sourcePredicate: predicate.circuitsPredicate, columns: columns, decodeRow: decodeRow,
      decodeKey: decodeKey)
  }
}

/// Several tables loaded as one shape group: one subscription, one stream and one cursor. A
/// group holds one to eight members, each with its own table and collection.
public struct CollectionGroupDemand: Sendable {
  public static let maximumMembers = 8
  public let members: [CollectionGroupMember]

  public init(members: [CollectionGroupMember]) {
    precondition((1...Self.maximumMembers).contains(members.count))
    precondition(Set(members.map(\.table)).count == members.count, "a group lists a table once")
    precondition(
      Set(members.map(\.collection)).count == members.count, "a group fills a collection once")
    self.members = members
  }

  public func identity(scope: CollectionScope) -> CollectionGroupDemandIdentity {
    func component(_ tag: String, _ value: String) -> String {
      "\(tag)\(value.utf8.count):\(value)"
    }
    let members = members.map { member in
      component("c", member.collection.rawValue) + component("t", member.table)
        + component("p", member.predicateIdentity)
        + component("k", member.columns.map { $0.map { component("", $0) }.joined() } ?? "*")
    }
    return CollectionGroupDemandIdentity(
      scope: scope,
      canonicalDemand: "collection-group-demand-v1|"
        + members.map { component("m", $0) }.joined())
  }
}

/// Exact identity of a group demand in one principal and authorization scope.
public struct CollectionGroupDemandIdentity: Codable, Equatable, Hashable, Sendable {
  public let scope: CollectionScope
  public let canonicalDemand: String

  public init(scope: CollectionScope, canonicalDemand: String) {
    precondition(!canonicalDemand.isEmpty)
    self.scope = scope
    self.canonicalDemand = canonicalDemand
  }

  /// Length-prefixed, like `CollectionDemandIdentity.storageKey`.
  public var storageKey: String {
    [scope.principal, scope.authorization, scope.generation, canonicalDemand]
      .map { "\($0.utf8.count):\($0)" }
      .joined(separator: "|")
  }
}

// MARK: - Group snapshots and batches

/// One member's typed rows in a group snapshot. `key` is the member definition's row key.
public protocol CollectionGroupMemberRows: Sendable {
  associatedtype Model: Sendable
  associatedtype Key: Hashable & Sendable
  var collection: CollectionID { get }
  var table: String { get }
  var rows: [Model] { get }
  var key: @Sendable (Model) -> Key { get }
}

/// One member's typed changes in a group batch. `key` is the member definition's row key.
public protocol CollectionGroupMemberChanges: Sendable {
  associatedtype Model: Sendable
  associatedtype Key: Hashable & Sendable
  var collection: CollectionID { get }
  var table: String { get }
  var changes: [CollectionChange<Model, Key>] { get }
  var key: @Sendable (Model) -> Key { get }
}

public struct CollectionGroupRows<Model: Sendable, Key: Hashable & Sendable>:
  CollectionGroupMemberRows
{
  public let collection: CollectionID
  public let table: String
  public let rows: [Model]
  public let key: @Sendable (Model) -> Key

  public init(
    collection: CollectionID, table: String, rows: [Model], key: @escaping @Sendable (Model) -> Key
  ) {
    self.collection = collection
    self.table = table
    self.rows = rows
    self.key = key
  }
}

public struct CollectionGroupChanges<Model: Sendable, Key: Hashable & Sendable>:
  CollectionGroupMemberChanges
{
  public let collection: CollectionID
  public let table: String
  public let changes: [CollectionChange<Model, Key>]
  public let key: @Sendable (Model) -> Key

  public init(
    collection: CollectionID, table: String, changes: [CollectionChange<Model, Key>],
    key: @escaping @Sendable (Model) -> Key
  ) {
    self.collection = collection
    self.table = table
    self.changes = changes
    self.key = key
  }
}

/// A group's snapshot: every member's rows, read at one PostgreSQL snapshot, with the group's one
/// fence, source version and cursor.
public struct CollectionGroupSnapshot: Sendable {
  /// Every member, in member order, including members with no rows.
  public let members: [any CollectionGroupMemberRows]
  public let fence: SnapshotFence
  public let sourceVersion: CollectionSourceVersion
  public let cursor: StreamCursor?

  public init(
    members: [any CollectionGroupMemberRows], fence: SnapshotFence,
    sourceVersion: CollectionSourceVersion, cursor: StreamCursor?
  ) {
    self.members = members
    self.fence = fence
    self.sourceVersion = sourceVersion
    self.cursor = cursor
  }

  /// One member's rows, when `collection` is a member decoded as `Model`.
  public func rows<Model: Sendable, Key: Hashable & Sendable>(
    in collection: CollectionID, as _: Model.Type = Model.self, key _: Key.Type = Key.self
  ) -> [Model]? {
    (members.first { $0.collection == collection } as? CollectionGroupRows<Model, Key>)?.rows
  }
}

/// One group stream batch: every member's changes and the group's one cursor. A store commits
/// all of it, every table and the cursor, in one transaction or none of it.
public struct CollectionGroupChangeBatch: Sendable {
  /// Every member, in member order, including members this batch does not touch.
  public let members: [any CollectionGroupMemberChanges]
  public let expectedCursor: StreamCursor?
  public let cursor: StreamCursor
  /// The greatest accepted source version in the batch, across every member.
  public let sourceVersion: CollectionSourceVersion

  public init(
    members: [any CollectionGroupMemberChanges], expectedCursor: StreamCursor?,
    cursor: StreamCursor, sourceVersion: CollectionSourceVersion
  ) {
    self.members = members
    self.expectedCursor = expectedCursor
    self.cursor = cursor
    self.sourceVersion = sourceVersion
  }

  /// One member's changes, when `collection` is a member decoded as `Model`.
  public func changes<Model: Sendable, Key: Hashable & Sendable>(
    in collection: CollectionID, as _: Model.Type = Model.self, key _: Key.Type = Key.self
  ) -> [CollectionChange<Model, Key>]? {
    (members.first { $0.collection == collection } as? CollectionGroupChanges<Model, Key>)?
      .changes
  }
}

/// A live group materialization: the demand it holds and the one fence, source version and
/// cursor every member shares.
public struct CollectionGroupMaterializationRecord: Codable, Equatable, Sendable {
  public let id: CollectionMaterializationID
  public let demand: CollectionGroupDemandIdentity
  public let collections: [CollectionID]
  public let snapshotFence: SnapshotFence
  public let sourceVersion: CollectionSourceVersion
  public let cursor: StreamCursor?

  public init(
    id: CollectionMaterializationID, demand: CollectionGroupDemandIdentity,
    collections: [CollectionID], snapshotFence: SnapshotFence,
    sourceVersion: CollectionSourceVersion, cursor: StreamCursor?
  ) {
    self.id = id
    self.demand = demand
    self.collections = collections
    self.snapshotFence = snapshotFence
    self.sourceVersion = sourceVersion
    self.cursor = cursor
  }
}

// MARK: - Group store

public enum CollectionGroupStoreError: Error, Equatable, Sendable {
  case materializationBoundToDifferentDemand(
    materializationID: CollectionMaterializationID,
    existing: CollectionGroupDemandIdentity,
    requested: CollectionGroupDemandIdentity
  )
  /// A batch named a collection the materialization's snapshot did not hold.
  case collectionOutsideGroup(CollectionID)
}

/// Atomic persistence boundary for a shape group. Each member's rows live in its own collection,
/// keyed by that collection's key, so equal keys in two tables never collide; the group's one
/// cursor commits with them. A conforming durable provider must commit each mutating method as
/// one transaction across every member's table and the cursor.
public protocol CollectionGroupStore: Sendable {
  func groupMaterialization(_ materializationID: CollectionMaterializationID) async throws
    -> CollectionGroupMaterializationRecord?

  /// Replaces this materialization's rows in every member collection and records its fence,
  /// source version and cursor. Rows another materialization still claims stay.
  func replaceGroupSnapshot(
    _ snapshot: CollectionGroupSnapshot,
    materializationID: CollectionMaterializationID,
    demand: CollectionGroupDemandIdentity
  ) async throws

  /// Applies every member's changes and compare-and-sets the group cursor from
  /// `batch.expectedCursor` to `batch.cursor`, all in one transaction. An already-current cursor
  /// is an idempotent replay; any other cursor is a `CollectionStoreError.cursorConflict`.
  func applyGroup(
    _ batch: CollectionGroupChangeBatch,
    to materializationID: CollectionMaterializationID
  ) async throws

  /// Removes the materialization, its claims and any rows no other materialization claims.
  func removeGroupMaterialization(_ materializationID: CollectionMaterializationID) async throws
}

/// Reference group store for contract tests and previews. Rows are canonical per collection and
/// scope, claimed per materialization, and versioned like `InMemoryCollectionStore`'s.
public actor InMemoryCollectionGroupStore: CollectionGroupStore {
  private struct RowKey: Hashable {
    let collection: CollectionID
    let scope: CollectionScope
    let key: AnyHashable
  }
  /// A nil value is a versioned tombstone, as in `InMemoryCollectionStore`.
  private struct Row {
    let value: (any Sendable)?
    let version: CollectionSourceVersion
  }
  private struct State {
    var rows: [RowKey: Row] = [:]
    var records: [CollectionMaterializationID: CollectionGroupMaterializationRecord] = [:]
    var claims: [CollectionMaterializationID: Set<RowKey>] = [:]
  }
  private var state = State()

  public init() {}

  public func groupMaterialization(_ materializationID: CollectionMaterializationID) async throws
    -> CollectionGroupMaterializationRecord?
  {
    state.records[materializationID]
  }

  public func replaceGroupSnapshot(
    _ snapshot: CollectionGroupSnapshot,
    materializationID: CollectionMaterializationID,
    demand: CollectionGroupDemandIdentity
  ) async throws {
    if let existing = state.records[materializationID] {
      guard existing.demand == demand else {
        throw CollectionGroupStoreError.materializationBoundToDifferentDemand(
          materializationID: materializationID, existing: existing.demand, requested: demand)
      }
      if snapshot.sourceVersion < existing.sourceVersion {
        throw CollectionStoreError.staleSnapshot(
          current: existing.sourceVersion, received: snapshot.sourceVersion)
      }
    }
    var next = state
    var newClaims = Set<RowKey>()
    for member in snapshot.members {
      Self.replace(
        member, scope: demand.scope, at: snapshot.sourceVersion, rows: &next.rows,
        claims: &newClaims)
    }
    let oldClaims = next.claims[materializationID] ?? []
    next.claims[materializationID] = newClaims
    for dropped in oldClaims.subtracting(newClaims) where !Self.isClaimed(dropped, in: next.claims)
    {
      next.rows.removeValue(forKey: dropped)
    }
    next.records[materializationID] = CollectionGroupMaterializationRecord(
      id: materializationID, demand: demand, collections: snapshot.members.map(\.collection),
      snapshotFence: snapshot.fence, sourceVersion: snapshot.sourceVersion,
      cursor: snapshot.cursor)
    Self.pruneTombstones(&next)
    state = next
  }

  public func applyGroup(
    _ batch: CollectionGroupChangeBatch,
    to materializationID: CollectionMaterializationID
  ) async throws {
    guard let record = state.records[materializationID] else {
      throw CollectionStoreError.missingMaterialization(materializationID)
    }
    if record.cursor == batch.cursor { return }
    guard record.cursor == batch.expectedCursor else {
      throw CollectionStoreError.cursorConflict(
        expected: batch.expectedCursor, actual: record.cursor, advancingTo: batch.cursor)
    }
    // Every member writes into a copy; the copy replaces the store only after the last member
    // and the cursor are in it, so a failure partway leaves every table and the cursor unchanged.
    var next = state
    var claims = next.claims[materializationID] ?? []
    var deleted: [(RowKey, CollectionSourceVersion)] = []
    for member in batch.members {
      guard record.collections.contains(member.collection) else {
        throw CollectionGroupStoreError.collectionOutsideGroup(member.collection)
      }
      Self.apply(
        member, scope: record.demand.scope, rows: &next.rows, claims: &claims,
        deleted: &deleted)
    }
    next.claims[materializationID] = claims
    for (rowKey, version) in deleted where !Self.isClaimed(rowKey, in: next.claims) {
      if next.rows[rowKey].map({ version.supersedes($0.version) }) ?? true {
        next.rows[rowKey] = Row(value: nil, version: version)
      }
    }
    next.records[materializationID] = CollectionGroupMaterializationRecord(
      id: record.id, demand: record.demand, collections: record.collections,
      snapshotFence: record.snapshotFence,
      sourceVersion: batch.sourceVersion.order >= record.sourceVersion.order
        ? batch.sourceVersion : record.sourceVersion,
      cursor: batch.cursor)
    Self.pruneTombstones(&next)
    state = next
  }

  public func removeGroupMaterialization(
    _ materializationID: CollectionMaterializationID
  ) async throws {
    var next = state
    let oldClaims = next.claims.removeValue(forKey: materializationID) ?? []
    next.records.removeValue(forKey: materializationID)
    for rowKey in oldClaims
    where !Self.isClaimed(rowKey, in: next.claims) && next.rows[rowKey]?.value != nil {
      next.rows.removeValue(forKey: rowKey)
    }
    Self.pruneTombstones(&next)
    state = next
  }

  /// One collection's live rows in a scope, decoded as the member's model and key.
  public func rows<Model: Sendable, Key: Hashable & Sendable>(
    in collection: CollectionID, scope: CollectionScope, as _: Model.Type = Model.self,
    key _: Key.Type = Key.self
  ) -> [Key: Model] {
    state.rows.reduce(into: [:]) { result, pair in
      guard pair.key.collection == collection, pair.key.scope == scope,
        let key = pair.key.key.base as? Key, let row = pair.value.value as? Model
      else { return }
      result[key] = row
    }
  }

  private static func replace<Member: CollectionGroupMemberRows>(
    _ member: Member, scope: CollectionScope, at version: CollectionSourceVersion,
    rows: inout [RowKey: Row], claims: inout Set<RowKey>
  ) {
    for row in member.rows {
      let rowKey = RowKey(
        collection: member.collection, scope: scope, key: AnyHashable(member.key(row)))
      claims.insert(rowKey)
      if let existing = rows[rowKey], !version.supersedes(existing.version) { continue }
      rows[rowKey] = Row(value: row, version: version)
    }
  }

  private static func apply<Member: CollectionGroupMemberChanges>(
    _ member: Member, scope: CollectionScope, rows: inout [RowKey: Row],
    claims: inout Set<RowKey>, deleted: inout [(RowKey, CollectionSourceVersion)]
  ) {
    for change in member.changes {
      switch change {
      case .upsert(let row, let version):
        let rowKey = RowKey(
          collection: member.collection, scope: scope, key: AnyHashable(member.key(row)))
        if rows[rowKey].map({ version.supersedes($0.version) }) ?? true {
          rows[rowKey] = Row(value: row, version: version)
        }
        claims.insert(rowKey)
      case .delete(let key, let version):
        let rowKey = RowKey(collection: member.collection, scope: scope, key: AnyHashable(key))
        claims.remove(rowKey)
        deleted.append((rowKey, version))
      }
    }
  }

  private static func isClaimed(
    _ rowKey: RowKey, in claims: [CollectionMaterializationID: Set<RowKey>]
  ) -> Bool {
    claims.values.contains { $0.contains(rowKey) }
  }

  /// Keeps a tombstone until every materialization holding its collection in its scope has
  /// applied the delete's source version, as `InMemoryCollectionStore` does.
  private static func pruneTombstones(_ state: inout State) {
    let records = state.records.values
    state.rows = state.rows.filter { rowKey, row in
      guard row.value == nil else { return true }
      return records.contains { record in
        record.demand.scope == rowKey.scope && record.collections.contains(rowKey.collection)
          && !record.sourceVersion.supersedes(row.version)
      }
    }
  }
}

// MARK: - Erased member decoding

/// A member's typed decoders behind one concrete type, so a group can hold members of different
/// models. Each closure returns that member's typed rows or changes as its existential.
struct CollectionGroupMemberCodec: Sendable {
  enum WireChange: Sendable {
    case upsert(ChangeRow, CollectionSourceVersion)
    case delete(String, CollectionSourceVersion)
  }

  let snapshot: @Sendable ([ChangeRow]) throws -> any CollectionGroupMemberRows
  let changes: @Sendable ([WireChange]) throws -> any CollectionGroupMemberChanges

  init<Model: Sendable, Key: Hashable & Sendable>(
    definition: CollectionDefinition<Model, Key>, table: String,
    decodeRow: @escaping @Sendable (ChangeRow) throws -> Model,
    decodeKey: @escaping @Sendable (String) throws -> Key
  ) {
    let collection = definition.id
    let key = definition.key
    snapshot = { rows in
      CollectionGroupRows(
        collection: collection, table: table, rows: try rows.map(decodeRow), key: key)
    }
    changes = { wire in
      let changes: [CollectionChange<Model, Key>] = try wire.map { change in
        switch change {
        case .upsert(let row, let version):
          return .upsert(try decodeRow(row), sourceVersion: version)
        case .delete(let rawKey, let version):
          let key: Key
          do { key = try decodeKey(rawKey) } catch {
            throw CircuitsSubsetGroupSourceError.invalidLiveKey(table: table, key: rawKey)
          }
          return .delete(key, sourceVersion: version)
        }
      }
      return CollectionGroupChanges(
        collection: collection, table: table, changes: changes, key: key)
    }
  }
}
