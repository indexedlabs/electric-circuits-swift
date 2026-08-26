import ElectricCircuitsSwift
import Foundation

/// Stable application identity for one normalized entity set.
public struct CollectionID: RawRepresentable, Codable, Equatable, Hashable, Sendable {
  public let rawValue: String

  public init(rawValue: String) {
    precondition(!rawValue.isEmpty)
    self.rawValue = rawValue
  }
}

/// Principal and authorization epoch in which cached collection data is valid.
public struct CollectionScope: Codable, Equatable, Hashable, Sendable {
  public let principal: String
  public let authorization: String
  public let generation: String

  public init(principal: String, authorization: String, generation: String) {
    precondition(!principal.isEmpty && !authorization.isEmpty && !generation.isEmpty)
    self.principal = principal
    self.authorization = authorization
    self.generation = generation
  }
}

/// A stable, provider-neutral sort component. `fieldID` is a logical schema field ID, not a
/// PostgreSQL or SQLite column spelling.
public struct CollectionOrder<Model: Sendable>: Equatable, Hashable, Sendable {
  public enum Direction: String, Codable, Equatable, Hashable, Sendable {
    case ascending
    case descending
  }

  public let fieldID: String
  public let sourceName: String
  public let direction: Direction

  public init(
    fieldID: String,
    sourceName: String? = nil,
    direction: Direction = .ascending
  ) {
    let sourceName = sourceName ?? fieldID
    precondition(!fieldID.isEmpty && !sourceName.isEmpty)
    self.fieldID = fieldID
    self.sourceName = sourceName
    self.direction = direction
  }
}

/// Rows requested by one consumer. `predicateIdentity` is produced by the typed collection
/// predicate AST and deliberately contains no provider-specific column names.
public struct CollectionDemand<Model: Sendable>: Sendable {
  public let predicateIdentity: String
  public let sourcePredicate: ElectricCircuitsSwift.Predicate?
  public let order: [CollectionOrder<Model>]
  public let limit: Int?

  public init(
    predicateIdentity: String,
    sourcePredicate: ElectricCircuitsSwift.Predicate? = nil,
    order: [CollectionOrder<Model>] = [],
    limit: Int? = nil
  ) {
    precondition(!predicateIdentity.isEmpty)
    precondition(limit.map { $0 > 0 } ?? true)
    self.predicateIdentity = predicateIdentity
    self.sourcePredicate = sourcePredicate
    self.order = order
    self.limit = limit
  }

  public init(
    predicate: CollectionPredicate<Model>,
    order: [CollectionOrder<Model>] = [],
    limit: Int? = nil
  ) {
    self.init(
      predicateIdentity: predicate.canonicalDescription,
      sourcePredicate: predicate.circuitsPredicate,
      order: order,
      limit: limit
    )
  }
}

/// Stable definition of one canonical application table/entity set.
public struct CollectionDefinition<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  public let id: CollectionID
  public let key: @Sendable (Model) -> Key

  public init(id: CollectionID, key: @escaping @Sendable (Model) -> Key) {
    self.id = id
    self.key = key
  }
}

/// Exact identity used for conservative demand de-duplication and persisted materializations.
public struct CollectionDemandIdentity: Codable, Equatable, Hashable, Sendable {
  public let collection: CollectionID
  public let scope: CollectionScope
  public let canonicalDemand: String

  public init(collection: CollectionID, scope: CollectionScope, canonicalDemand: String) {
    precondition(!canonicalDemand.isEmpty)
    self.collection = collection
    self.scope = scope
    self.canonicalDemand = canonicalDemand
  }

  /// Length-prefixing prevents ambiguous identities when application-controlled values contain
  /// separators.
  public var storageKey: String {
    [
      collection.rawValue,
      scope.principal,
      scope.authorization,
      scope.generation,
      canonicalDemand,
    ]
    .map { "\($0.utf8.count):\($0)" }
    .joined(separator: "|")
  }
}

extension CollectionDemand {
  public func identity<Key: Hashable & Sendable>(
    for definition: CollectionDefinition<Model, Key>,
    scope: CollectionScope
  ) -> CollectionDemandIdentity {
    let orderIdentity = order.map { "\($0.fieldID):\($0.direction.rawValue)" }.joined(
      separator: ",")
    let limitIdentity = limit.map(String.init) ?? "none"
    return CollectionDemandIdentity(
      collection: definition.id,
      scope: scope,
      canonicalDemand:
        "predicate=\(predicateIdentity);order=\(orderIdentity);limit=\(limitIdentity)"
    )
  }
}

/// Internal server-backed resource identity. Applications persist it opaquely.
public struct CollectionMaterializationID: RawRepresentable, Codable, Equatable, Hashable, Sendable
{
  public let rawValue: String

  public init(rawValue: String) {
    precondition(!rawValue.isEmpty)
    self.rawValue = rawValue
  }
}

/// Opaque snapshot-to-live handoff value. The collection core never interprets it as an LSN.
public struct SnapshotFence: RawRepresentable, Codable, Equatable, Hashable, Sendable {
  public let rawValue: String

  public init(rawValue: String) {
    precondition(!rawValue.isEmpty)
    self.rawValue = rawValue
  }
}

public enum CollectionMaterializationState: String, Codable, Equatable, Sendable {
  case cached
  case live
  case stale
}

public struct CollectionMaterializationRecord: Codable, Equatable, Sendable {
  public let id: CollectionMaterializationID
  public let demand: CollectionDemandIdentity
  public let state: CollectionMaterializationState
  public let snapshotFence: SnapshotFence
  public let cursor: StreamCursor?

  public init(
    id: CollectionMaterializationID,
    demand: CollectionDemandIdentity,
    state: CollectionMaterializationState,
    snapshotFence: SnapshotFence,
    cursor: StreamCursor?
  ) {
    self.id = id
    self.demand = demand
    self.state = state
    self.snapshotFence = snapshotFence
    self.cursor = cursor
  }
}

public struct CollectionSnapshot<Model: Sendable>: Sendable {
  public let rows: [Model]
  public let fence: SnapshotFence
  public let cursor: StreamCursor?

  public init(rows: [Model], fence: SnapshotFence, cursor: StreamCursor? = nil) {
    self.rows = rows
    self.fence = fence
    self.cursor = cursor
  }
}

public enum CollectionChange<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  case upsert(Model)
  case delete(Key)
}

public struct CollectionChangeBatch<Model: Sendable, Key: Hashable & Sendable>: Sendable {
  public let changes: [CollectionChange<Model, Key>]
  public let expectedCursor: StreamCursor?
  public let cursor: StreamCursor

  public init(
    changes: [CollectionChange<Model, Key>],
    expectedCursor: StreamCursor?,
    cursor: StreamCursor
  ) {
    self.changes = changes
    self.expectedCursor = expectedCursor
    self.cursor = cursor
  }
}
