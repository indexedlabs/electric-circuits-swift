import Foundation
import Testing

@testable import ElectricCircuitsSwift

private let deliveryURL = URL(string: "https://delivery.test/stream")!

private func envelopeJSON(_ key: String, commitTimestamp: Int64?) -> String {
  let commit = commitTimestamp.map { ",\"commit_ts\":\($0)" } ?? ""
  return
    "{\"type\":\"public.items\",\"key\":\"\(key)\",\"value\":{\"id\":\"\(key)\"},\"headers\":{\"operation\":\"upsert\",\"lsn\":\"0/10\"\(commit)}}"
}

/// Records the `LiveDelivery.current` each live apply runs under, and keeps the cursor so a
/// restarted reader resumes from it.
private actor DeliveryRecorder: ShapeMaterializer {
  private(set) var deliveries: [LiveDelivery?] = []
  private(set) var appliedDeliveries: [LiveDelivery?] = []
  private var cursor: StreamCursor?
  /// For each batch in turn, the envelope indices a source would accept; nil accepts all
  /// without calling `LiveDelivery.applying`.
  private var acceptance: [[Int]?]

  init(acceptance: [[Int]?] = []) { self.acceptance = acceptance }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch, expecting _: StreamCursor?, advancingTo cursor: StreamCursor
  ) async throws {
    deliveries.append(LiveDelivery.current)
    if !batch.isEmpty, !acceptance.isEmpty, let accepted = acceptance.removeFirst() {
      let narrowed = await LiveDelivery.applying(changesAt: accepted) { LiveDelivery.current }
      appliedDeliveries.append(narrowed)
    } else {
      appliedDeliveries.append(LiveDelivery.current)
    }
    self.cursor = cursor
  }
}

/// Serves scripted long-poll bodies, each with its own server time and next offset, then 404.
private actor LongPollDeliveryTransport: HTTPTransport {
  private var pages: [(body: String, serverTime: String?, next: String)]

  init(_ pages: [(body: String, serverTime: String?, next: String)]) { self.pages = pages }

  func append(_ page: (body: String, serverTime: String?, next: String)) { pages.append(page) }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    guard !pages.isEmpty else {
      return HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
    }
    let page = pages.removeFirst()
    var headers = ["Content-Type": "application/json", "stream-next-offset": page.next]
    headers[LiveDelivery.serverTimeHeader] = page.serverTime
    return HTTPResponse(
      data: Data(page.body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil, headerFields: headers)!)
  }
}

/// Serves scripted SSE responses as raw bytes, one chunk per element, then 404.
private actor SSEDeliveryTransport: HTTPTransport {
  private var responses: [[String]]

  init(_ responses: [[String]]) { self.responses = responses }

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    guard !responses.isEmpty else {
      try await onResponse(
        HTTPURLResponse(url: request.url!, statusCode: 404, httpVersion: nil, headerFields: nil)!)
      return
    }
    let chunks = responses.removeFirst()
    try await onResponse(
      HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!)
    for chunk in chunks { try await onBytes(Data(chunk.utf8)) }
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    Issue.record("Long-poll was unexpectedly used")
    return HTTPResponse(
      data: Data(),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 500, httpVersion: nil, headerFields: nil)!)
  }
}

@Test("An envelope's commit_ts decodes as commitTimestamp and encodes back")
func envelopeHeadersCarryCommitTimestamp() throws {
  let decoded = try JSONDecoder().decode(
    ChangeEnvelope.self, from: Data(envelopeJSON("a", commitTimestamp: 1_793_404_800_123).utf8))
  #expect(decoded.headers.commitTimestamp == 1_793_404_800_123)
  let encoded = String(decoding: try JSONEncoder().encode(decoded.headers), as: UTF8.self)
  #expect(encoded.contains("\"commit_ts\":1793404800123"))
  let unstamped = try JSONDecoder().decode(
    ChangeEnvelope.self, from: Data(envelopeJSON("b", commitTimestamp: nil).utf8))
  #expect(unstamped.headers.commitTimestamp == nil)
}

@Test("The SSE parser reads a data event's server-time and resets it per event")
func parserReadsServerTimePerEvent() throws {
  var parser = SSEParser(maximumEventBytes: 1024)
  let events = try parser.append(
    Data("event: data\ndata: []\nserver-time: 150\n\nevent: control\ndata: {}\n\n".utf8))
  #expect(events.map(\.name) == ["data", "control"])
  #expect(events.map(\.serverTime) == [150, nil])
}

@Test("A long-poll batch is applied under its delivery, first after a reader start")
func longPollBatchesCarryTheirDelivery() async throws {
  let transport = LongPollDeliveryTransport([
    // An offset-only advance applies nothing and doesn't use up the first-after-start flag.
    (body: "[]", serverTime: "140", next: "15"),
    (body: "[\(envelopeJSON("a", commitTimestamp: 100))]", serverTime: "150", next: "20"),
    (body: "[\(envelopeJSON("b", commitTimestamp: nil))]", serverTime: nil, next: "30"),
  ])
  let recorder = DeliveryRecorder()
  let reader = ShapeStreamReader(
    streamURL: deliveryURL, transport: transport, materializer: recorder)
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }
  // A retry restarts the reader's tail, so its first batch counts as after a reconnect again.
  await transport.append(
    (body: "[\(envelopeJSON("c", commitTimestamp: 200))]", serverTime: "250", next: "40"))
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }

  let recorded = await recorder.deliveries
  #expect(recorded.count == 4)
  #expect(recorded.first == .some(nil))
  let deliveries = recorded.compactMap { $0 }
  #expect(deliveries.count == 3)
  #expect(deliveries.allSatisfy { $0.mode == .longPoll })
  #expect(deliveries.map(\.serverSentAt) == [150, nil, 250])
  #expect(deliveries.map(\.firstAfterReconnect) == [true, false, true])
  #expect(
    deliveries.map(\.changes) == [
      [LiveDelivery.Change(table: "public.items", commitTimestamp: 100)],
      [LiveDelivery.Change(table: "public.items", commitTimestamp: nil)],
      [LiveDelivery.Change(table: "public.items", commitTimestamp: 200)],
    ])
  #expect(deliveries.allSatisfy { $0.receivedAt > 1_700_000_000_000 })
}

@Test("A batch a source drops entirely doesn't count as the first applied after a start")
func droppedBatchesLeaveTheFirstFlagForTheNextApplied() async throws {
  let transport = LongPollDeliveryTransport([
    (body: "[\(envelopeJSON("a", commitTimestamp: 100))]", serverTime: "150", next: "20"),
    (
      body: "[\(envelopeJSON("b", commitTimestamp: 110)),\(envelopeJSON("c", commitTimestamp: 120))]",
      serverTime: "160", next: "30"
    ),
    (body: "[\(envelopeJSON("d", commitTimestamp: 130))]", serverTime: "170", next: "40"),
  ])
  // The first batch is all in the snapshot; the second applies only its second envelope.
  let recorder = DeliveryRecorder(acceptance: [[], [1], nil])
  let reader = ShapeStreamReader(
    streamURL: deliveryURL, transport: transport, materializer: recorder)
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }
  let applied = await recorder.appliedDeliveries
  #expect(applied.count == 3)
  #expect(applied[0] == nil)
  #expect(applied[1]?.changes == [LiveDelivery.Change(table: "public.items", commitTimestamp: 120)])
  #expect(applied[1]?.firstAfterReconnect == true)
  #expect(applied[2]?.firstAfterReconnect == false)
}

@Test("An SSE batch is applied under its delivery; a routine reconnect is not a new start")
func sseBatchesCarryTheirDelivery() async throws {
  let transport = SSEDeliveryTransport([
    [
      "event: data\ndata: [\(envelopeJSON("a", commitTimestamp: 100))]\nserver-",
      "time: 150\n\nevent: control\ndata: {\"streamNextOffset\":\"20\"}\n\n",
      "event: data\ndata: [\(envelopeJSON("b", commitTimestamp: nil))]\nserver-time: 160\n\n",
      "event: control\ndata: {\"streamNextOffset\":\"30\"}\n\n",
      // Two frames fold into one batch; its send time is its last frame's, which has none.
      "event: data\ndata: [\(envelopeJSON("e", commitTimestamp: 200))]\nserver-time: 210\n\n",
      "event: data\ndata: [\(envelopeJSON("f", commitTimestamp: 220))]\n\n",
      "event: control\ndata: {\"streamNextOffset\":\"35\"}\n\n",
    ],
    [
      "event: data\ndata: [\(envelopeJSON("c", commitTimestamp: 300))]\n\n",
      "event: control\ndata: {\"streamNextOffset\":\"40\"}\n\n",
    ],
  ])
  let recorder = DeliveryRecorder()
  let reader = ShapeStreamReader(
    streamURL: deliveryURL, transport: transport, materializer: recorder,
    configuration: .init(liveReadMode: .sse))
  await #expect(throws: StreamError.terminal(path: "/stream", status: 404, reason: .notFound)) {
    try await reader.run()
  }

  let recorded = await recorder.deliveries
  #expect(recorded.count == 4)
  let deliveries = recorded.compactMap { $0 }
  #expect(deliveries.count == 4)
  #expect(deliveries.allSatisfy { $0.mode == .sse })
  #expect(deliveries.map(\.serverSentAt) == [150, 160, nil, nil])
  #expect(deliveries.map(\.firstAfterReconnect) == [true, false, false, false])
  #expect(
    deliveries.map { $0.changes.map(\.commitTimestamp) } == [[100], [nil], [200, 220], [300]])
  #expect(deliveries.allSatisfy { $0.receivedAt > 1_700_000_000_000 })
}
