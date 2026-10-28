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
  private var eventBytes = 0
  private var line = Data()
  private var name = ""
  private var data = Data()
  private var hasData = false
  private var skipLF = false
  private var firstLine = true

  init(maximumEventBytes: Int) {
    self.maximumEventBytes = maximumEventBytes
  }

  mutating func append(_ byte: UInt8) throws -> Event? {
    if skipLF {
      skipLF = false
      if byte == 0x0A { return nil }
    }
    eventBytes += 1
    if byte == 0x0D || byte == 0x0A {
      skipLF = byte == 0x0D
      return try finishLine()
    }
    line.append(byte)
    return nil
  }

  private mutating func finishLine() throws -> Event? {
    defer { line.removeAll(keepingCapacity: true) }
    if firstLine {
      firstLine = false
      if line.starts(with: [0xEF, 0xBB, 0xBF]) { line.removeFirst(3) }
    }
    if line.isEmpty {
      if data.count > maximumEventBytes {
        while data.count > maximumEventBytes && maximumEventBytes < maximumEventBytesCeiling {
          maximumEventBytes = min(maximumEventBytes * 2, maximumEventBytesCeiling)
        }
        guard data.count <= maximumEventBytes else {
          throw StreamError.responseTooLarge(
            limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
        }
      }
      let event = hasData ? Event(name: name.isEmpty ? "message" : name, data: data) : nil
      name = ""
      data = Data()
      hasData = false
      eventBytes = 0
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
      if hasData { data.append(0x0A) }
      data.append(contentsOf: value.utf8)
      hasData = true
    default: break
    }
    return nil
  }
}
