import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation

/// One table of a scope, as the probe is given it: the table, the column that keys its rows and
/// the engine's predicate JSON.
struct ProbeMemberSpec: Decodable, Sendable {
  let table: String
  let key: String
  let `where`: ElectricCircuitsSwift.Predicate?
  let columns: [String]?

  /// The predicate's canonical JSON, which names the demand.
  var predicateIdentity: String {
    guard let predicate = `where` else { return "all" }
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    return (try? encoder.encode(predicate)).flatMap { String(data: $0, encoding: .utf8) } ?? "all"
  }
}

/// A scope: either one table (`table`, `key`, `where`) or a group of tables (`group`).
struct ProbeScopeSpec: Decodable, Sendable {
  let name: String
  let table: String?
  let key: String?
  let `where`: ElectricCircuitsSwift.Predicate?
  let columns: [String]?
  let group: [ProbeMemberSpec]?

  var single: ProbeMemberSpec? {
    guard group == nil, let table, let key else { return nil }
    return ProbeMemberSpec(table: table, key: key, where: `where`, columns: columns)
  }

  /// The collection a group member's rows land in. A single-table scope's collection is its name.
  func collection(of member: ProbeMemberSpec) -> CollectionID {
    CollectionID(rawValue: "\(name).\(member.table)")
  }
}

/// Wraps the SDK's single-table source so the report says which path each attempt took and what
/// error ended it. It changes nothing the coordinator sees.
struct ProbeSource: CollectionSourceAdapter {
  typealias Model = ChangeRow
  typealias Key = String

  let base: CircuitsSubsetSource<ChangeRow, String>
  let scope: String
  let output: ProbeOutput

  var columns: [String]? { base.columns }

  func materialize(
    _ demand: CollectionDemand<ChangeRow>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID
  ) async throws -> CollectionSourceSession<ChangeRow, String> {
    output.emit(
      "path",
      [
        "scope": .string(scope), "path": .string("snapshot"),
        "materialization": .string(materializationID.rawValue),
      ])
    let session: CollectionSourceSession<ChangeRow, String>
    do {
      session = try await base.materialize(
        demand, identity: identity, materializationID: materializationID)
    } catch {
      if !(error is CancellationError) { output.failure(scope: scope, phase: "materialize", error) }
      throw error
    }
    let scope = scope
    let output = output
    return CollectionSourceSession(
      snapshot: session.snapshot,
      run: { apply in
        do { try await session.run(apply) } catch {
          if !(error is CancellationError) { output.failure(scope: scope, phase: "tail", error) }
          throw error
        }
      },
      stop: session.stop)
  }

  func resume(
    _ demand: CollectionDemand<ChangeRow>,
    identity: CollectionDemandIdentity,
    materializationID: CollectionMaterializationID,
    record: CollectionMaterializationRecord
  ) async throws -> CollectionResumeSession<ChangeRow, String>? {
    let resumed: CollectionResumeSession<ChangeRow, String>?
    do {
      resumed = try await base.resume(
        demand, identity: identity, materializationID: materializationID, record: record)
    } catch {
      if !(error is CancellationError) { output.failure(scope: scope, phase: "resume", error) }
      throw error
    }
    let fields: [String: JSONValue] = [
      "scope": .string(scope), "materialization": .string(materializationID.rawValue),
      "storedGeneration": .string(record.demand.scope.generation),
      "cursor": record.cursor.map { .string($0.offset) } ?? .null,
    ]
    guard let session = resumed else {
      output.emit("path", fields.merging(["path": .string("resume-declined")]) { $1 })
      return nil
    }
    output.emit("path", fields.merging(["path": .string("resume")]) { $1 })
    let scope = scope
    let output = output
    return CollectionResumeSession(
      cursor: session.cursor,
      sourceVersion: session.sourceVersion,
      run: { apply, caughtUp in
        do {
          try await session.run(
            apply,
            {
              output.emit("caught-up", ["scope": .string(scope)])
              await caughtUp()
            })
        } catch {
          if !(error is CancellationError) {
            output.failure(scope: scope, phase: "resumed-tail", error)
          }
          throw error
        }
      },
      stop: session.stop)
  }

  func cleanupAbandonedMaterialization(_ materializationID: CollectionMaterializationID)
    async throws
  {
    try await base.cleanupAbandonedMaterialization(materializationID)
  }
}

extension CollectionLoadState {
  var text: String {
    switch self {
    case .unavailable: "unavailable"
    case .cached: "cached"
    case .refreshing: "refreshing"
    case .live: "live"
    case .failed(.sourceUnavailable): "failed(sourceUnavailable)"
    case .failed(.storeUnavailable): "failed(storeUnavailable)"
    }
  }
}
