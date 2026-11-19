import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation

/// The probe's report: one JSON object per line on stdout. Each line is written whole under a
/// lock, so scopes running concurrently never interleave, and stdout is a pipe's unbuffered
/// descriptor, so a driver reads an event as soon as it is written.
final class ProbeOutput: Sendable {
  private let lock = NSLock()

  func emit(_ event: String, _ fields: [String: JSONValue] = [:]) {
    var object = fields
    object["event"] = .string(event)
    object["at"] = .int(Int64((Date().timeIntervalSince1970 * 1000).rounded(.down)))
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys, .withoutEscapingSlashes]
    guard var data = try? encoder.encode(JSONValue.object(object)) else { return }
    data.append(0x0A)
    lock.withLock { FileHandle.standardOutput.write(data) }
  }

  /// A failure with the error as the SDK threw it: its description and its full type name.
  func failure(scope: String, phase: String, _ error: any Error) {
    var fields: [String: JSONValue] = [
      "scope": .string(scope), "phase": .string(phase),
      "error": .string(String(describing: error)),
      "type": .string(String(reflecting: type(of: error))),
    ]
    if let invalidated = error as? CollectionResumeInvalidated {
      fields["cause"] = .string(String(describing: invalidated.cause))
    }
    emit("failure", fields)
  }
}

enum ProbeJSON {
  /// Any `Encodable` as a `JSONValue`, for an event field.
  static func value<T: Encodable>(_ encodable: T) -> JSONValue {
    let encoder = JSONEncoder()
    encoder.outputFormatting = [.sortedKeys]
    guard let data = try? encoder.encode(encodable),
      let value = try? JSONDecoder().decode(JSONValue.self, from: data)
    else { return .null }
    return value
  }

  /// A response or request body as JSON, or nil when it is empty, too large to report or not
  /// JSON.
  static func body(_ data: Data?, limit: Int = 262_144) -> JSONValue? {
    guard let data, !data.isEmpty, data.count <= limit else { return nil }
    return try? JSONDecoder().decode(JSONValue.self, from: data)
  }

  /// A row key as the engine spells it on the stream: the key column's value as text.
  static func key(_ value: JSONValue?) -> String {
    switch value {
    case .string(let text): text
    case .int(let number): String(number)
    case .decimal(let number): "\(number)"
    case .number(let number): String(number)
    case .bool(let flag): String(flag)
    case .null, .array, .object, nil: ""
    }
  }
}
