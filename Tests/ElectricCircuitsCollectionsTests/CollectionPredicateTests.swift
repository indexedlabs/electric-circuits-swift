import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

private struct PredicateIssue: Sendable {}

private enum PredicateIssueFields {
  static let modifiedAt = CollectionField<PredicateIssue, String>(
    id: "modifiedAt", sourceName: "modified_at", storageName: "modifiedAt")
  static let assigneeID = CollectionField<PredicateIssue, UUID>(
    id: "assigneeID", sourceName: "assignee_id", storageName: "assigneeId")
  static let priority = CollectionField<PredicateIssue, Int>(
    id: "priority", sourceName: "priority", storageName: "priority")
  static let score = CollectionField<PredicateIssue, Double>(
    id: "score", sourceName: "score", storageName: "score")
  static let archived = CollectionField<PredicateIssue, Bool>(
    id: "archived", sourceName: "archived", storageName: "archived")
}

@Suite("Typed collection predicates")
struct CollectionPredicateTests {
  @Test func oneASTUsesSourceNamesForCircuitsAndLogicalNamesForIdentity() {
    let assigneeID = UUID(uuidString: "00000000-0000-4000-8000-000000000001")!
    let predicate =
      PredicateIssueFields.modifiedAt >= "2026-08-26T00:00:00Z"
      && (PredicateIssueFields.assigneeID == assigneeID
        || PredicateIssueFields.assigneeID.isNull)

    #expect(
      predicate.circuitsPredicate
        == .and([
          .leaf(
            column: "modified_at", op: .gte,
            value: .string("2026-08-26T00:00:00Z")),
          .or([
            .leaf(
              column: "assignee_id", op: .eq,
              value: .string(assigneeID.uuidString.lowercased())),
            .isNull(column: "assignee_id", isNull: true),
          ]),
        ]))
    #expect(predicate.canonicalDescription.contains("modifiedAt"))
    #expect(predicate.canonicalDescription.contains("assigneeID"))
    #expect(!predicate.canonicalDescription.contains("modified_at"))
    #expect(!predicate.canonicalDescription.contains("assignee_id"))
  }

  @Test func canonicalIdentityPreservesGroupingAndEscapesStrings() {
    let a = PredicateIssueFields.modifiedAt >= "a'b"
    let b = PredicateIssueFields.modifiedAt < "b"
    let c = PredicateIssueFields.modifiedAt > "c"

    #expect(a.canonicalDescription == "modifiedAt >= 'a''b'")
    #expect(((a || b) && c).canonicalDescription != (a || (b && c)).canonicalDescription)
  }

  @Test func scalarAndNullSemanticsRenderWithoutApproximation() {
    let predicate =
      PredicateIssueFields.priority >= 2
      && PredicateIssueFields.score < 4.5
      && PredicateIssueFields.archived == false
      && PredicateIssueFields.assigneeID.isNotNull

    #expect(
      predicate.circuitsPredicate
        == .and([
          .leaf(column: "priority", op: .gte, value: .int(2)),
          .leaf(column: "score", op: .lt, value: .number(4.5)),
          .leaf(column: "archived", op: .eq, value: .bool(false)),
          .isNull(column: "assignee_id", isNull: false),
        ]))
  }

  @Test func demandDerivesWirePredicateAndCanonicalIdentityFromSameTypedPredicate() {
    let predicate = PredicateIssueFields.priority >= 2
    let demand = CollectionDemand(predicate: predicate, limit: 10)

    #expect(demand.predicateIdentity == predicate.canonicalDescription)
    #expect(demand.sourcePredicate == predicate.circuitsPredicate)
    #expect(demand.limit == 10)
  }

  @Test func demandIdentityCannotAliasDelimiterBearingPredicateAndOrderValues() {
    let definition = CollectionDefinition<PredicateIssue, Int>(
      id: CollectionID(rawValue: "issues"), key: { _ in 0 })
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let first = CollectionDemand<PredicateIssue>(
      unsafePredicateIdentity: "x",
      order: [
        .init(
          unsafeFieldID: "a;limit=none;order=c",
          sourceName: "source-a;limit=none;order=c"
        )
      ],
      limit: 7
    )
    let second = CollectionDemand<PredicateIssue>(
      unsafePredicateIdentity: "x;order=a;limit=none",
      order: [.init(unsafeFieldID: "c", sourceName: "source-c")],
      limit: 7
    )

    let firstIdentity = first.identity(for: definition, scope: scope)
    let secondIdentity = second.identity(for: definition, scope: scope)
    #expect(firstIdentity != secondIdentity)
    #expect(firstIdentity.storageKey != secondIdentity.storageKey)
  }

  @Test func demandIdentityIncludesProviderOrderSourceName() {
    let definition = CollectionDefinition<PredicateIssue, Int>(
      id: CollectionID(rawValue: "issues"), key: { _ in 0 })
    let scope = CollectionScope(principal: "u", authorization: "a", generation: "g")
    let first = CollectionDemand<PredicateIssue>(
      unsafePredicateIdentity: "all",
      order: [.init(unsafeFieldID: "modified", sourceName: "modified_at")]
    )
    let second = CollectionDemand<PredicateIssue>(
      unsafePredicateIdentity: "all",
      order: [.init(unsafeFieldID: "modified", sourceName: "updated_at")]
    )

    #expect(
      first.identity(for: definition, scope: scope)
        != second.identity(for: definition, scope: scope))
  }
}
