import ElectricCircuitsSwift
import Foundation
import LinearLiteGRDB

/// Client-side state for one ordered subset page. The server only tails the base predicate; this
/// value decides whether a change belongs to the loaded recent window and whether a strict page
/// needs a fresh query to refill its boundary.
public struct LinearLiteRecentSubsetWindow: Equatable, Sendable {
  public enum Action: Equatable, Sendable {
    case ignore
    case materialize(ChangeEnvelope)
    case reseed
  }

  private let limit: Int
  private var rows: [Int64: Issue] = [:]
  private var watermarks: [Int64: UInt64] = [:]
  private var snapshotLSN: UInt64?
  /// A page with exactly `limit` rows may have more rows behind its boundary. Once the query
  /// returns fewer rows, a delete or move-out can be applied directly because no hidden refill row
  /// exists at that snapshot. We conservatively mark a page full again after it grows to the limit.
  private var mayHaveHiddenRows = false

  public init(limit: Int) {
    self.limit = max(0, limit)
  }

  public var issues: [Issue] {
    rows.values.sorted(by: Self.precedes)
  }

  /// Replaces the loaded window with a query-back page and records its snapshot watermark.
  public mutating func seed(_ issues: [Issue], snapshotLSN: String?) {
    rows = Dictionary(uniqueKeysWithValues: issues.map { ($0.id, $0) })
    watermarks = [:]
    self.snapshotLSN = Self.parseLSN(snapshotLSN)
    for issue in rows.values {
      if let lsn = self.snapshotLSN { watermarks[issue.id] = lsn }
    }
    mayHaveHiddenRows = limit > 0 && rows.count >= limit
  }

  /// Merges one feed delta. A `.reseed` action means the caller must query and atomically install a
  /// replacement page; the caller should discard this candidate window and use the returned page
  /// as the new state.
  public mutating func merge(_ envelope: ChangeEnvelope) throws -> Action {
    guard let id = Int64(envelope.key) else {
      throw LinearLiteShapeMaterializerError.invalidDeleteKey(key: envelope.key)
    }
    let incomingLSN = Self.parseLSN(envelope.headers.lsn)
    guard Self.isFresh(incomingLSN, for: id, watermarks: watermarks, snapshotLSN: snapshotLSN)
    else {
      return .ignore
    }

    switch envelope.headers.operation {
    case .delete:
      record(incomingLSN, for: id)
      guard rows.removeValue(forKey: id) != nil else { return .ignore }
      return mayHaveHiddenRows ? .reseed : .materialize(envelope)

    case .insert, .update, .upsert:
      guard let value = envelope.value else {
        throw LinearLiteShapeMaterializerError.missingValue(key: envelope.key)
      }
      let issue: Issue
      do {
        issue = try Issue(changeRow: value)
      } catch {
        throw LinearLiteShapeMaterializerError.malformedValue(
          key: envelope.key, detail: String(describing: error))
      }
      guard issue.id == id else {
        throw LinearLiteShapeMaterializerError.keyMismatch(key: envelope.key, id: issue.id)
      }

      let wasPresent = rows[id] != nil
      let inside = isInWindow(issue)
      record(incomingLSN, for: id)
      guard inside else {
        guard wasPresent else { return .ignore }
        rows.removeValue(forKey: id)
        return mayHaveHiddenRows ? .reseed : .materialize(Self.deleteEnvelope(from: envelope))
      }

      rows[id] = issue
      if !wasPresent && rows.count >= limit { mayHaveHiddenRows = true }
      if mayHaveHiddenRows && !wasPresent { return .reseed }
      return .materialize(envelope)
    }
  }

  private func isInWindow(_ issue: Issue) -> Bool {
    guard limit > 0 else { return false }
    if rows.count < limit { return true }
    guard let boundary = rows.values.sorted(by: Self.precedes).last else { return true }
    return Self.notAfter(issue, boundary)
  }

  private mutating func record(_ lsn: UInt64?, for id: Int64) {
    if let lsn { watermarks[id] = lsn }
  }

  private static func precedes(_ lhs: Issue, _ rhs: Issue) -> Bool {
    if lhs.modified != rhs.modified { return lhs.modified > rhs.modified }
    return lhs.id > rhs.id
  }

  private static func notAfter(_ lhs: Issue, _ rhs: Issue) -> Bool {
    if lhs.modified != rhs.modified { return lhs.modified > rhs.modified }
    return lhs.id >= rhs.id
  }

  private static func isFresh(
    _ incoming: UInt64?,
    for id: Int64,
    watermarks: [Int64: UInt64],
    snapshotLSN: UInt64?
  ) -> Bool {
    guard let incoming else { return true }
    if let watermark = watermarks[id] { return incoming >= watermark }
    if let snapshotLSN { return incoming >= snapshotLSN }
    return true
  }

  private static func deleteEnvelope(from envelope: ChangeEnvelope) -> ChangeEnvelope {
    let headers = envelope.headers
    return ChangeEnvelope(
      type: envelope.type, key: envelope.key,
      headers: EnvelopeHeaders(
        operation: .delete, txid: headers.txid, offset: headers.offset, lsn: headers.lsn,
        seq: headers.seq, last: headers.last))
  }

  private static func parseLSN(_ value: String?) -> UInt64? {
    guard let value else { return nil }
    let parts = value.split(separator: "/", omittingEmptySubsequences: false)
    guard parts.count == 2,
      let high = UInt64(parts[0], radix: 16),
      let low = UInt64(parts[1], radix: 16)
    else { return nil }
    return (high << 32) | low
  }
}
