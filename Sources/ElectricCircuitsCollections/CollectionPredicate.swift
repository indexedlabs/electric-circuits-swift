import ElectricCircuitsSwift
import Foundation

/// A typed field in a sync collection. Logical, source and storage names are deliberately distinct.
public struct CollectionField<Model: Sendable, Value>: Hashable, Sendable {
  public let id: String
  public let sourceName: String
  public let storageName: String

  public init(id: String, sourceName: String, storageName: String) {
    precondition(!id.isEmpty, "A collection field needs a stable logical id")
    precondition(!sourceName.isEmpty, "A collection field needs a source column name")
    precondition(!storageName.isEmpty, "A collection field needs a storage column name")
    self.id = id
    self.sourceName = sourceName
    self.storageName = storageName
  }

  private var column: CollectionPredicateColumn {
    CollectionPredicateColumn(id: id, sourceName: sourceName, storageName: storageName)
  }

  public var isNull: CollectionPredicate<Model> {
    CollectionPredicate(expression: .isNull(column: column, isNull: true))
  }

  public var isNotNull: CollectionPredicate<Model> {
    CollectionPredicate(expression: .isNull(column: column, isNull: false))
  }
}

public struct CollectionPredicateColumn: Hashable, Sendable {
  public let id: String
  public let sourceName: String
  public let storageName: String

  public init(id: String, sourceName: String, storageName: String) {
    precondition(!id.isEmpty, "A predicate column needs a stable logical id")
    precondition(!sourceName.isEmpty, "A predicate column needs a source column name")
    precondition(!storageName.isEmpty, "A predicate column needs a storage column name")
    self.id = id
    self.sourceName = sourceName
    self.storageName = storageName
  }
}

public enum CollectionPredicateScalar: Hashable, Sendable {
  case string(String)
  case int(Int64)
  case double(Double)
  case bool(Bool)

  public var canonicalDescription: String {
    switch self {
    case .string(let value):
      "'\(value.replacingOccurrences(of: "'", with: "''"))'"
    case .int(let value):
      String(value)
    case .double(let value):
      String(value)
    case .bool(let value):
      value ? "TRUE" : "FALSE"
    }
  }

  public var circuitsValue: JSONValue {
    switch self {
    case .string(let value): .string(value)
    case .int(let value): .int(value)
    case .double(let value): .number(value)
    case .bool(let value): .bool(value)
    }
  }
}

public protocol CollectionPredicateValue: Sendable {
  var collectionPredicateScalar: CollectionPredicateScalar { get }
}

extension String: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar { .string(self) }
}

extension UUID: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar {
    .string(uuidString.lowercased())
  }
}

extension Int: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar { .int(Int64(self)) }
}

extension Int64: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar { .int(self) }
}

extension Double: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar {
    precondition(isFinite, "Collection predicates require a finite Double")
    return .double(self)
  }
}

extension Bool: CollectionPredicateValue {
  public var collectionPredicateScalar: CollectionPredicateScalar { .bool(self) }
}

public enum CollectionPredicateComparison: String, Hashable, Sendable {
  case equal = "="
  case greaterThan = ">"
  case greaterThanOrEqual = ">="
  case lessThan = "<"
  case lessThanOrEqual = "<="

  public var circuitsOperator: LeafOperator {
    switch self {
    case .equal: .eq
    case .greaterThan: .gt
    case .greaterThanOrEqual: .gte
    case .lessThan: .lt
    case .lessThanOrEqual: .lte
    }
  }
}

public indirect enum CollectionPredicateExpression: Hashable, Sendable {
  case comparison(
    column: CollectionPredicateColumn,
    operation: CollectionPredicateComparison,
    value: CollectionPredicateScalar
  )
  case isNull(column: CollectionPredicateColumn, isNull: Bool)
  case and([CollectionPredicateExpression])
  case or([CollectionPredicateExpression])
  case not(CollectionPredicateExpression)

  public var circuitsPredicate: ElectricCircuitsSwift.Predicate {
    switch self {
    case .comparison(let column, let operation, let value):
      .leaf(column: column.sourceName, op: operation.circuitsOperator, value: value.circuitsValue)
    case .isNull(let column, let isNull):
      .isNull(column: column.sourceName, isNull: isNull)
    case .and(let expressions):
      .and(expressions.map(\.circuitsPredicate))
    case .or(let expressions):
      .or(expressions.map(\.circuitsPredicate))
    case .not(let expression):
      .not(expression.circuitsPredicate)
    }
  }

  public var canonicalDescription: String {
    switch self {
    case .comparison(let column, let operation, let value):
      "\(column.id) \(operation.rawValue) \(value.canonicalDescription)"
    case .isNull(let column, let isNull):
      "\(column.id) IS \(isNull ? "" : "NOT ")NULL"
    case .and(let expressions):
      "(\(expressions.map(\.canonicalDescription).joined(separator: " AND ")))"
    case .or(let expressions):
      "(\(expressions.map(\.canonicalDescription).joined(separator: " OR ")))"
    case .not(let expression):
      "NOT (\(expression.canonicalDescription))"
    }
  }

  fileprivate var flatteningAnd: [Self] {
    if case .and(let expressions) = self { return expressions }
    return [self]
  }

  fileprivate var flatteningOr: [Self] {
    if case .or(let expressions) = self { return expressions }
    return [self]
  }
}

/// Provider-neutral, typed predicate syntax. `Model` is a phantom type that prevents fields from
/// unrelated collections from being composed through the public operators.
public struct CollectionPredicate<Model: Sendable>: Hashable, Sendable {
  public let expression: CollectionPredicateExpression

  init(expression: CollectionPredicateExpression) {
    self.expression = expression
  }

  public var circuitsPredicate: ElectricCircuitsSwift.Predicate {
    expression.circuitsPredicate
  }

  public var canonicalDescription: String {
    expression.canonicalDescription
  }
}

public func == <Model: Sendable, Value: CollectionPredicateValue>(
  lhs: CollectionField<Model, Value>, rhs: Value
) -> CollectionPredicate<Model> {
  comparison(lhs, .equal, rhs)
}

public func > <Model: Sendable, Value: CollectionPredicateValue>(
  lhs: CollectionField<Model, Value>, rhs: Value
) -> CollectionPredicate<Model> {
  comparison(lhs, .greaterThan, rhs)
}

public func >= <Model: Sendable, Value: CollectionPredicateValue>(
  lhs: CollectionField<Model, Value>, rhs: Value
) -> CollectionPredicate<Model> {
  comparison(lhs, .greaterThanOrEqual, rhs)
}

public func < <Model: Sendable, Value: CollectionPredicateValue>(
  lhs: CollectionField<Model, Value>, rhs: Value
) -> CollectionPredicate<Model> {
  comparison(lhs, .lessThan, rhs)
}

public func <= <Model: Sendable, Value: CollectionPredicateValue>(
  lhs: CollectionField<Model, Value>, rhs: Value
) -> CollectionPredicate<Model> {
  comparison(lhs, .lessThanOrEqual, rhs)
}

public func && <Model: Sendable>(
  lhs: CollectionPredicate<Model>, rhs: CollectionPredicate<Model>
) -> CollectionPredicate<Model> {
  CollectionPredicate(
    expression: .and(lhs.expression.flatteningAnd + rhs.expression.flatteningAnd))
}

public func || <Model: Sendable>(
  lhs: CollectionPredicate<Model>, rhs: CollectionPredicate<Model>
) -> CollectionPredicate<Model> {
  CollectionPredicate(
    expression: .or(lhs.expression.flatteningOr + rhs.expression.flatteningOr))
}

prefix public func ! <Model: Sendable>(
  predicate: CollectionPredicate<Model>
) -> CollectionPredicate<Model> {
  CollectionPredicate(expression: .not(predicate.expression))
}

private func comparison<Model: Sendable, Value: CollectionPredicateValue>(
  _ field: CollectionField<Model, Value>,
  _ operation: CollectionPredicateComparison,
  _ value: Value
) -> CollectionPredicate<Model> {
  CollectionPredicate(
    expression: .comparison(
      column: CollectionPredicateColumn(
        id: field.id,
        sourceName: field.sourceName,
        storageName: field.storageName
      ),
      operation: operation,
      value: value.collectionPredicateScalar
    ))
}
