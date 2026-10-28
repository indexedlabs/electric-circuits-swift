import Foundation

/// Response-local SSE framing. Only a blank line dispatches an event; dropping the parser at
/// EOF deliberately drops its unfinished event. Decode UTF-8 after a complete line so a chunk
/// can end in the middle of a code point, CRLF, field name, or JSON token.
struct SSEParser {
  struct Event: Equatable, Sendable {
    let name: String
    let data: Data
  }

  private(set) var maximumEventBytes: Int
  private let maximumEventBytesCeiling = 512 * 1024 * 1024
  private let dataField = Array("data:".utf8)
  private var line = Data()
  private var name = ""
  private var data = Data()
  private var hasData = false
  private var skipLF = false
  private var firstLine = true

  init(maximumEventBytes: Int) {
    self.maximumEventBytes = max(1, min(maximumEventBytes, maximumEventBytesCeiling))
  }

  mutating func append(_ byte: UInt8) throws -> Event? {
    if skipLF {
      skipLF = false
      if byte == 0x0A { return nil }
    }
    if byte == 0x0D || byte == 0x0A {
      skipLF = byte == 0x0D
      return try finishLine()
    }
    line.append(byte)
    // A data line is the largest temporary allocation in this parser. Check its decoded payload
    // as it arrives, while allowing the `data: ` field prefix and an exact-ceiling payload.
    if line.count >= dataField.count, line.prefix(dataField.count).elementsEqual(dataField) {
      let payloadStart = line.count > 5 && line[5] == 0x20 ? 6 : 5
      let payloadBytes = max(0, line.count - payloadStart)
      let projected = data.count + (hasData ? 1 : 0) + payloadBytes
      guard projected <= maximumEventBytesCeiling else {
        throw StreamError.responseTooLarge(
          limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
      }
    } else if line.count > maximumEventBytesCeiling + 6 {
      // Bound malformed or unknown fields too; they cannot become JSON data after this point.
      throw StreamError.responseTooLarge(
        limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
    }
    return nil
  }

  private mutating func finishLine() throws -> Event? {
    defer { line.removeAll(keepingCapacity: true) }
    if firstLine {
      firstLine = false
      if line.starts(with: [0xEF, 0xBB, 0xBF]) { line.removeFirst(3) }
    }
    if line.isEmpty {
      let event = hasData ? Event(name: name.isEmpty ? "message" : name, data: data) : nil
      name = ""
      data = Data()
      hasData = false
      return event
    }
    guard let text = String(data: line, encoding: .utf8) else {
      throw StreamError.decoding("Invalid UTF-8 in SSE event")
    }
    if text.hasPrefix(":") { return nil }
    let parts = text.split(separator: ":", maxSplits: 1, omittingEmptySubsequences: false)
    let field = parts[0]
    var value = parts.count == 2 ? parts[1] : Substring()
    if value.first == " " { value = value.dropFirst() }
    switch field {
    case "event": name = String(value)
    case "data":
      let additionalBytes = value.utf8.count + (hasData ? 1 : 0)
      let nextCount = data.count + additionalBytes
      while nextCount > maximumEventBytes && maximumEventBytes < maximumEventBytesCeiling {
        maximumEventBytes = min(maximumEventBytes * 2, maximumEventBytesCeiling)
      }
      guard nextCount <= maximumEventBytes else {
        throw StreamError.responseTooLarge(
          limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
      }
      if hasData { data.append(0x0A) }
      data.append(contentsOf: value.utf8)
      hasData = true
    default: break
    }
    return nil
  }
}
