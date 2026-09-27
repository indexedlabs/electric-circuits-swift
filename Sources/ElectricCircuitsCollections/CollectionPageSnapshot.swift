import Foundation

/// Transaction visibility of one subset page. Snapshot xids and feed xids share a 32-bit space.
/// Persist this with a row version, including when the row is a tombstone.
public struct CollectionPageSnapshot: Codable, Equatable, Hashable, Sendable {
  public let lsn: UInt64
  public let horizon: UInt64
  public let xmin: UInt32
  public let xmax: UInt32
  public let xip: Set<UInt32>

  /// Missing or malformed positioning fields retain the old engine's LSN behavior.
  public init?(lsn: UInt64, snapshot: String?, horizon: String?) {
    guard let snapshot, let horizon, let parsedHorizon = Self.postgresLSN(horizon) else {
      return nil
    }
    let parts = snapshot.split(separator: ":", omittingEmptySubsequences: false)
    guard parts.count >= 2, let xmin = Self.xid32(String(parts[0])),
      let xmax = Self.xid32(String(parts[1]))
    else { return nil }
    var xip = Set<UInt32>()
    if parts.count > 2 {
      for text in parts[2].split(separator: ",") {
        guard let xid = Self.xid32(String(text)) else { return nil }
        xip.insert(xid)
      }
    }
    self.lsn = lsn
    self.horizon = parsedHorizon
    self.xmin = xmin
    self.xmax = xmax
    self.xip = xip
  }

  /// Decimal xid8 text modulo 2^32, without overflow or a floating-point conversion.
  public static func xid32(_ text: String) -> UInt32? {
    guard !text.isEmpty else { return nil }
    var result: UInt32 = 0
    for byte in text.utf8 {
      guard byte >= 48, byte <= 57 else { return nil }
      result = result &* 10 &+ UInt32(byte - 48)
    }
    return result
  }

  static func postgresLSN(_ text: String) -> UInt64? {
    let words = text.split(separator: "/", omittingEmptySubsequences: false)
    guard words.count == 2, let high = UInt32(words[0], radix: 16),
      let low = UInt32(words[1], radix: 16)
    else { return nil }
    return UInt64(high) << 32 | UInt64(low)
  }

  private static func precedes(_ a: UInt32, _ b: UInt32) -> Bool {
    Int32(bitPattern: a &- b) < 0
  }

  private func includes(transactionID: UInt32) -> Bool {
    if Self.precedes(transactionID, xmin) { return true }
    return Self.precedes(transactionID, xmax) && !xip.contains(transactionID)
  }

  /// Matches the TypeScript client's pageIncludes, including its missing-txid fallback.
  public func includes(lsn: UInt64, transactionID: UInt32?) -> Bool {
    guard lsn < horizon else { return false }
    guard let transactionID else { return lsn < self.lsn }
    return includes(transactionID: transactionID)
  }

  /// Separate materializations may finish page requests out of order. Compare visibility rather
  /// than response arrival or page LSN; a later snapshot cannot lose a previously visible xid.
  func includes(_ previous: Self) -> Bool {
    guard !Self.precedes(xmax, previous.xmax), !Self.precedes(xmin, previous.xmin) else {
      return false
    }
    return !xip.contains { previous.includes(transactionID: $0) }
  }
}
