import ElectricCircuitsSwift
import Foundation
import LinearLiteApp
import LinearLiteGRDB
import Testing

private func windowIssue(
  id: Int64,
  title: String = "Issue",
  modified: Int64
) -> LinearLiteGRDB.Issue {
  LinearLiteGRDB.Issue(
    id: id, title: "\(title) \(id)", description: "Details", status: "backlog", priority: "high",
    username: "ada", projectID: 7, created: 1, modified: modified, kanbanOrder: Double(id))
}

private func windowEnvelope(
  _ issue: LinearLiteGRDB.Issue,
  lsn: String? = nil,
  operation: ChangeOperation = .upsert
) -> ChangeEnvelope {
  ChangeEnvelope(
    type: "public.issues", key: String(issue.id),
    value: [
      "id": .int(issue.id), "title": .string(issue.title),
      "description": .string(issue.description),
      "status": .string(issue.status), "priority": .string(issue.priority),
      "username": .string(issue.username), "project_id": .int(issue.projectID),
      "created": .int(issue.created), "modified": .int(issue.modified),
      "kanbanorder": .number(issue.kanbanOrder),
    ], headers: EnvelopeHeaders(operation: operation, lsn: lsn))
}

@Suite("LinearLite recent subset window")
struct LinearLiteRecentSubsetWindowTests {
  @Test func inWindowUpdatesMaterializeWithoutReseed() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 10)
    window.seed(
      (1...10).map { windowIssue(id: Int64($0), modified: Int64($0)) }, snapshotLSN: "0/100")
    let delta = windowEnvelope(windowIssue(id: 5, title: "Updated", modified: 5), lsn: "0/120")

    #expect(try window.merge(delta) == .materialize(delta))
    #expect(window.issues.first(where: { $0.id == 5 })?.title == "Updated 5")
  }

  @Test func staleDeltaIsIgnoredAfterSnapshotOrNewerFeedValue() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 10)
    window.seed([windowIssue(id: 1, title: "Current", modified: 1)], snapshotLSN: "0/100")
    let stale = windowEnvelope(windowIssue(id: 1, title: "Stale", modified: 1), lsn: "0/80")

    #expect(try window.merge(stale) == .ignore)
    #expect(window.issues.first?.title == "Current 1")
  }

  @Test func newRowEnteringFullPageRequestsReseed() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 10)
    window.seed(
      (1...10).map { windowIssue(id: Int64($0), modified: Int64($0)) }, snapshotLSN: "0/100")
    let entering = windowEnvelope(windowIssue(id: 11, modified: 11), lsn: "0/120")

    #expect(try window.merge(entering) == .reseed)
  }

  @Test func inWindowRowMovingBelowBoundaryRequestsReseed() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 2)
    let rows = [windowIssue(id: 1, modified: 1), windowIssue(id: 2, modified: 2)]
    window.seed(rows, snapshotLSN: "0/100")
    let movedOut = windowEnvelope(windowIssue(id: 1, title: "Older", modified: 0), lsn: "0/120")

    #expect(try window.merge(movedOut) == .reseed)
  }

  @Test func deletingFromFullPageRequestsReseedButShortPageCanApplyDirectly() throws {
    var full = LinearLiteRecentSubsetWindow(limit: 2)
    let fullRows = [windowIssue(id: 1, modified: 1), windowIssue(id: 2, modified: 2)]
    full.seed(fullRows, snapshotLSN: "0/100")
    let fullDelete = windowEnvelope(fullRows[0], lsn: "0/120", operation: .delete)
    #expect(try full.merge(fullDelete) == .reseed)

    var short = LinearLiteRecentSubsetWindow(limit: 2)
    let shortRows = [windowIssue(id: 1, modified: 1)]
    short.seed(shortRows, snapshotLSN: "0/100")
    let shortDelete = windowEnvelope(shortRows[0], lsn: "0/120", operation: .delete)
    #expect(try short.merge(shortDelete) == .materialize(shortDelete))
    #expect(short.issues.isEmpty)
  }

  @Test func unseenPreSnapshotDeleteDoesNotCreateAReseed() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 10)
    window.seed([windowIssue(id: 1, modified: 1)], snapshotLSN: "0/100")
    let delete = windowEnvelope(windowIssue(id: 99, modified: 99), lsn: "0/80", operation: .delete)

    #expect(try window.merge(delete) == .ignore)
  }

  @Test func libraryModeWithoutLSNsUsesPrimaryKeyIdempotency() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 2)
    let first = windowIssue(id: 1, title: "First", modified: 1)
    window.seed([first], snapshotLSN: nil)
    let update = windowEnvelope(windowIssue(id: 1, title: "Updated", modified: 1))

    #expect(try window.merge(update) == .materialize(update))
    #expect(window.issues.first?.title == "Updated 1")
  }

  @Test func malformedUpsertFailsBeforeMaterialization() throws {
    var window = LinearLiteRecentSubsetWindow(limit: 2)
    window.seed([], snapshotLSN: nil)
    let malformed = ChangeEnvelope(
      type: "public.issues", key: "1", value: ["id": .int(1)],
      headers: EnvelopeHeaders(operation: .upsert))

    #expect(throws: LinearLiteShapeMaterializerError.self) {
      try window.merge(malformed)
    }
  }
}
