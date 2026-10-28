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
  private var lineFieldChecked = false
  private var dataLine = false

  init(maximumEventBytes: Int) {
    self.maximumEventBytes = max(1, min(maximumEventBytes, maximumEventBytesCeiling))
  }

  mutating func append(_ byte: UInt8) throws -> Event? {
    try append(Data([byte])).first
  }

  mutating func append(_ bytes: Data) throws -> [Event] {
    var events: [Event] = []
    var index = bytes.startIndex
    while index < bytes.endIndex {
      if skipLF {
        skipLF = false
        if bytes[index] == 0x0A {
          index = bytes.index(after: index)
          continue
        }
      }
      let newline = bytes[index...].firstIndex { $0 == 0x0D || $0 == 0x0A }
      let lineEnd = newline ?? bytes.endIndex
      if index < lineEnd {
        line.append(contentsOf: bytes[index..<lineEnd])
        if firstLine, line.count >= 3 {
          if line.starts(with: [0xEF, 0xBB, 0xBF]) { line.removeFirst(3) }
          firstLine = false
        }
        try checkLineLimit()
      }
      guard let newline else { break }
      skipLF = bytes[newline] == 0x0D
      if let event = try finishLine() { events.append(event) }
      index = bytes.index(after: newline)
    }
    return events
  }

  private mutating func checkLineLimit() throws {
    if !lineFieldChecked, line.count >= dataField.count {
      lineFieldChecked = true
      dataLine = line.starts(with: dataField)
    }
    if dataLine {
      let fieldEnd = line.index(line.startIndex, offsetBy: dataField.count)
      let payloadStart = line.count > dataField.count && line[fieldEnd] == 0x20
        ? dataField.count + 1 : dataField.count
      let payloadBytes = max(0, line.count - payloadStart)
      let projected = data.count + (hasData ? 1 : 0) + payloadBytes
      guard projected <= maximumEventBytesCeiling else {
        throw StreamError.responseTooLarge(
          limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
      }
    } else if line.count > maximumEventBytesCeiling + 6 {
      throw StreamError.responseTooLarge(
        limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
    }
  }

  private mutating func finishLine() throws -> Event? {
    defer {
      line.removeAll(keepingCapacity: line.count <= 64 * 1024)
      lineFieldChecked = false
      dataLine = false
    }
    if firstLine {
      firstLine = false
      if line.starts(with: [0xEF, 0xBB, 0xBF]) { line.removeFirst(3) }
    }
    if line.isEmpty {
      let event = hasData ? Event(name: name.isEmpty ? "message" : name, data: data) : nil
      name = ""
      data = Data()
      hasData = false
      lineFieldChecked = false
      dataLine = false
      return event
    }
    // Copy data bytes directly. JSONDecoder validates the resulting UTF-8 after the event is
    // complete; avoiding a temporary String keeps an at-ceiling event from being held three times.
    if line.starts(with: dataField) {
      let fieldEnd = line.index(line.startIndex, offsetBy: dataField.count)
      let valueStart = line.count > dataField.count && line[fieldEnd] == 0x20
        ? dataField.count + 1 : dataField.count
      let value = line.dropFirst(valueStart)
      let additionalBytes = value.count + (hasData ? 1 : 0)
      let nextCount = data.count + additionalBytes
      while nextCount > maximumEventBytes && maximumEventBytes < maximumEventBytesCeiling {
        maximumEventBytes = min(maximumEventBytes * 2, maximumEventBytesCeiling)
      }
      guard nextCount <= maximumEventBytes else {
        throw StreamError.responseTooLarge(
          limit: maximumEventBytesCeiling, observed: maximumEventBytesCeiling + 1)
      }
      if hasData { data.append(0x0A) }
      data.append(contentsOf: value)
      hasData = true
      lineFieldChecked = false
      dataLine = false
      return nil
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
    case "data": break
    default: break
    }
    lineFieldChecked = false
    dataLine = false
    return nil
  }
}
