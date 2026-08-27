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

  /// Escape hatch for providers that do not have typed schema fields. It makes no aliasing
  /// guarantee: callers own keeping this source spelling aligned with the wire predicate.
  public init(
    unsafeFieldID fieldID: String,
    sourceName: String? = nil,
    direction: Direction = .ascending
  ) {
    let sourceName = sourceName ?? fieldID
    precondition(!fieldID.isEmpty && !sourceName.isEmpty)
    self.fieldID = fieldID
    self.sourceName = sourceName
    self.direction = direction
  }

  public init<Value>(field: CollectionField<Model, Value>, direction: Direction = .ascending) {
    self.init(unsafeFieldID: field.id, sourceName: field.sourceName, direction: direction)
  }
}

/// Rows requested by one consumer. Typed predicate identity includes the provider-neutral schema
/// mapping that determines its wire fields, so distinct mappings cannot share a materialization.
public struct CollectionDemand<Model: Sendable>: Sendable {
  public let predicateIdentity: String
  public let sourcePredicate: ElectricCircuitsSwift.Predicate?
  public let order: [CollectionOrder<Model>]
  public let limit: Int?

  /// Escape hatch for provider adapters. Typed `CollectionPredicate` construction is the only
  /// public form that guarantees the identity and wire predicate describe the same expression.
  public init(
    unsafePredicateIdentity predicateIdentity: String,
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
      unsafePredicateIdentity: predicate.canonicalIdentity,
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
    return CollectionDemandIdentity(
      collection: definition.id,
      scope: scope,
      canonicalDemand: canonicalIdentity()
    )
  }

  /// A typed, length-prefixed encoding rather than a delimiter protocol. Unsafe demand inputs are
  /// application controlled, so every component is framed independently and provider source names
  /// participate in identity as well as in the outbound query.
  private func canonicalIdentity() -> String {
    func component(_ tag: String, _ value: String) -> String {
      "\(tag)\(value.utf8.count):\(value)"
    }
    let orderIdentity = order.map { order in
      component("f", order.fieldID)
        + component("s", order.sourceName)
        + component("d", order.direction.rawValue)
    }.joined()
    let limitIdentity: String
    if let limit {
      limitIdentity = component("some", String(limit))
    } else {
      limitIdentity = "none"
    }
    return "collection-demand-v1|" + component("p", predicateIdentity)
      + component("o", orderIdentity)
      + component("l", limitIdentity)
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

/// A source-provided, totally ordered row-state version. Stores persist both values and compare
/// `order`; they never infer an order from a transport cursor. Sources must reject values they
/// cannot translate to this contract before mutating a collection.
public struct CollectionSourceVersion: Codable, Equatable, Hashable, Comparable, Sendable {
  public let rawValue: String
  public let order: UInt64

  public init(rawValue: String, order: UInt64) {
    precondition(!rawValue.isEmpty)
    self.rawValue = rawValue
    self.order = order
  }

  public static func < (lhs: Self, rhs: Self) -> Bool { lhs.order < rhs.order }
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
  public let sourceVersion: CollectionSourceVersion
  public let cursor: StreamCursor?

  public init(
    id: CollectionMaterializationID,
    demand: CollectionDemandIdentity,
    state: CollectionMaterializationState,
    snapshotFence: SnapshotFence,
    sourceVersion: CollectionSourceVersion,
    cursor: StreamCursor?
  ) {
    self.id = id
    self.demand = demand
    self.state = state
    self.snapshotFence = snapshotFence
    self.sourceVersion = sourceVersion
    self.cursor = cursor
  }
}

public struct CollectionSnapshot<Model: Sendable>: Sendable {
  public let rows: [Model]
  public let fence: SnapshotFence
  public let sourceVersion: CollectionSourceVersion
  public let cursor: StreamCursor?

  public init(
    rows: [Model], fence: SnapshotFence,
    sourceVersion: CollectionSourceVersion = .init(rawValue: "initial", order: 0),
    cursor: StreamCursor? = nil
  ) {
    self.rows = rows
    self.fence = fence
    self.sourceVersion = sourceVersion
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
  /// The greatest accepted source version represented by this batch. A provider must atomically
  /// persist it with the rows and cursor, and ignore row mutations older than its current value.
  public let sourceVersion: CollectionSourceVersion

  public init(
    changes: [CollectionChange<Model, Key>],
    expectedCursor: StreamCursor?,
    cursor: StreamCursor,
    sourceVersion: CollectionSourceVersion = .init(rawValue: "initial", order: 0)
  ) {
    self.changes = changes
    self.expectedCursor = expectedCursor
    self.cursor = cursor
    self.sourceVersion = sourceVersion
  }
}
