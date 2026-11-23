import Foundation
import Testing

@testable import ElectricCircuitsSwift

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private let catchUpStream = URL(string: "https://streams.test/shape/s1")!

/// Answers stream reads from a script and ends the reader, as a cancellation does, once the
/// script runs out.
private actor CatchUpTransport: HTTPTransport {
  enum Reply: Sendable {
    /// A long-poll or catch-up answer.
    case json(String, status: Int = 200, next: String? = nil, upToDate: String? = nil)
    /// One SSE response: its events, after which the response ends.
    case sse([String])
  }

  private var replies: [Reply]
  private(set) var requests: [URLRequest] = []

  init(_ replies: [Reply]) { self.replies = replies }

  /// Every read's `live` parameter, nil for a read without one.
  var lives: [String?] { requests.map { Self.query($0, "live") } }
  var offsets: [String?] { requests.map { Self.query($0, "offset") } }

  private static func query(_ request: URLRequest, _ name: String) -> String? {
    request.url.flatMap { URLComponents(url: $0, resolvingAgainstBaseURL: false) }?
      .queryItems?.first { $0.name == name }?.value
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    guard case .json(let body, let status, let next, let upToDate)? = replies.first else {
      throw CancellationError()
    }
    replies.removeFirst()
    var headers: [String: String] = [:]
    if let next { headers["stream-next-offset"] = next }
    if let upToDate { headers["stream-up-to-date"] = upToDate }
    return HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
  }

  func stream(
    _ request: URLRequest,
    onResponse: @Sendable (HTTPURLResponse) async throws -> Void,
    onBytes: @Sendable (Data) async throws -> Void
  ) async throws {
    requests.append(request)
    guard case .sse(let events)? = replies.first else { throw CancellationError() }
    replies.removeFirst()
    try await onResponse(
      HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "text/event-stream"])!)
    for event in events { try await onBytes(Data(event.utf8)) }
  }
}

/// What the reader did, in order: each batch it had applied, and each head it reported.
private actor CatchUpLog: ShapeMaterializer {
  enum Event: Equatable, Sendable {
    case applied(String)
    case caughtUp(String)
  }

  private(set) var events: [Event] = []
  private var cursor: StreamCursor?

  init(cursor: StreamCursor) { self.cursor = cursor }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch, expecting expectedCursor: StreamCursor?,
    advancingTo next: StreamCursor
  ) async throws {
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(expected: expectedCursor, actual: cursor, advancingTo: next)
    }
    cursor = next
    events.append(.applied(next.offset))
  }

  func caughtUp(_ cursor: StreamCursor) { events.append(.caughtUp(cursor.offset)) }
}

private func change(_ key: String) -> String {
  #"[{"type":"public.items","key":"\#(key)","value":{"id":"\#(key)"},"#
    + #""headers":{"operation":"upsert","lsn":"0/\#(key)0"}}]"#
}

private func dataEvent(_ key: String) -> String { "event: data\ndata:\(change(key))\n\n" }

private func controlEvent(_ offset: String, upToDate: Bool? = nil) -> String {
  let flag = upToDate.map { #","upToDate":\#($0)"# } ?? ""
  return "event: control\ndata:{\"streamNextOffset\":\"\(offset)\"\(flag)}\n\n"
}

/// Runs a reader from offset `10` until the transport's script runs out.
private func runReader(
  _ transport: CatchUpTransport, log: CatchUpLog, mode: LiveReadMode = .longPoll,
  reportingHead: Bool = true
) async {
  let report: @Sendable (StreamCursor) async -> Void = { await log.caughtUp($0) }
  let reader = ShapeStreamReader(
    streamURL: catchUpStream, transport: transport, materializer: log,
    startingAt: StreamCursor(offset: "10"), configuration: .init(liveReadMode: mode),
    onCaughtUp: reportingHead ? report : nil)
  await #expect(throws: CancellationError.self) { try await reader.run() }
}

/// A reader asked to report the stream's head (`onCaughtUp`) reads to it without waiting there,
/// reports it once, after everything up to it is applied, and then tails as any reader does.
@Suite("Reading a stream to its head")
struct StreamCatchUpTests {
  @Test func aLongPollReaderAtTheHeadIsToldSoByAReadWithoutLive() async throws {
    let transport = CatchUpTransport([
      .json("[]", next: "10", upToDate: "true"),
      .json("", status: 204, next: "10", upToDate: "true"),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log)

    #expect(await log.events == [.caughtUp("10")])
    // The read that asks where the head is carries no `live`; the tail after it long-polls.
    #expect(await transport.lives == [nil, "long-poll", "long-poll"])
    #expect(await transport.offsets == ["10", "10", "10"])
    // Its answer says where the head is now, so it must not come from a cache.
    let first = try #require(await transport.requests.first)
    #expect(first.value(forHTTPHeaderField: "Cache-Control") == "no-cache")
    #expect(first.cachePolicy == .reloadIgnoringLocalCacheData)
    let second = await transport.requests[1]
    #expect(second.value(forHTTPHeaderField: "Cache-Control") == nil)
  }

  @Test func aLongPollReaderBehindReadsWithoutLiveUntilAnAnswerEndsAtTheHead() async {
    let transport = CatchUpTransport([
      .json(change("1"), next: "11"),
      .json(change("2"), next: "12", upToDate: "true"),
      .json(change("3"), next: "13", upToDate: "true"),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log)

    // Reported once, after the batch that ends at the head, and not after the first.
    #expect(
      await log.events
        == [.applied("11"), .applied("12"), .caughtUp("12"), .applied("13")])
    #expect(await transport.lives == [nil, nil, "long-poll", "long-poll"])
    #expect(await transport.offsets == ["10", "11", "12", "13"])
  }

  /// A server that does not mark its answers still answers a read without `live` at once, and
  /// an answer that brings nothing and moves nowhere has nothing after the offset.
  @Test func anEmptyUnmarkedAnswerAtTheSameOffsetIsTheHead() async {
    let transport = CatchUpTransport([
      .json(change("1"), next: "11"),
      .json("[]", next: "11"),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log)

    #expect(await log.events == [.applied("11"), .caughtUp("11")])
    #expect(await transport.lives == [nil, nil, "long-poll"])
  }

  /// An empty answer that names another offset moves the cursor; the head is where the next
  /// answer says it is.
  @Test func anEmptyAnswerThatMovesTheCursorIsCommittedBeforeTheHeadIsReported() async {
    let transport = CatchUpTransport([.json("[]", next: "11", upToDate: "1")])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log)

    #expect(await log.events == [.applied("11"), .caughtUp("11")])
    #expect(await transport.offsets == ["10", "11"])
  }

  @Test func aMarkThatSaysFalseIsNotTheHead() async {
    let transport = CatchUpTransport([
      .json(change("1"), next: "11", upToDate: "false"),
      .json(change("2"), next: "12", upToDate: "true"),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log)

    #expect(await log.events == [.applied("11"), .applied("12"), .caughtUp("12")])
  }

  /// Without `onCaughtUp` a reader is unchanged: every read is a live read.
  @Test func aReaderNotAskedForTheHeadOnlyLongPolls() async {
    let transport = CatchUpTransport([
      .json(change("1"), next: "11", upToDate: "true"),
      .json("", status: 204, next: "11", upToDate: "true"),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log, reportingHead: false)

    #expect(await log.events == [.applied("11")])
    #expect(await transport.lives == ["long-poll", "long-poll", "long-poll"])
    let first = await transport.requests[0]
    #expect(first.value(forHTTPHeaderField: "Cache-Control") == nil)
  }

  @Test func anSSEReaderAtTheHeadIsToldSoByTheOpeningControl() async {
    // The first control says so by naming the read's offset; the second also sets `upToDate`.
    for opening in [controlEvent("10"), controlEvent("10", upToDate: true)] {
      let transport = CatchUpTransport([.sse([opening, opening])])
      let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

      await runReader(transport, log: log, mode: .sse)

      #expect(await log.events == [.caughtUp("10")])
      #expect(await transport.lives == ["sse", "sse"])
    }
  }

  @Test func anSSEReaderBehindReportsTheHeadAfterTheBatchWhoseControlSaysSo() async {
    let transport = CatchUpTransport([
      .sse([
        dataEvent("1"), controlEvent("11"),
        dataEvent("2"), controlEvent("12", upToDate: true),
        dataEvent("3"), controlEvent("13", upToDate: true),
      ])
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log, mode: .sse)

    #expect(
      await log.events
        == [.applied("11"), .applied("12"), .caughtUp("12"), .applied("13")])
  }

  /// A control that ends a batch without `upToDate` leaves more to read. The control the
  /// stream repeats at that offset while idle is what says nothing follows.
  @Test func anSSEControlThatEndsABatchWithoutUpToDateIsNotTheHead() async {
    let transport = CatchUpTransport([
      .sse([dataEvent("1"), controlEvent("11", upToDate: false)]),
      .sse([controlEvent("11")]),
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))

    await runReader(transport, log: log, mode: .sse)

    #expect(await log.events == [.applied("11"), .caughtUp("11")])
    #expect(await transport.offsets == ["10", "11", "11"])
  }

  /// A reader that fell back from SSE still owes the head, and asks for it the long-poll way.
  @Test func aReaderThatFellBackFromSSEAsksForTheHeadWithoutLive() async {
    // No streaming support: the default `stream` reports it and the reader falls back.
    let transport = LongPollOnlyTransport([
      .json("[]", next: "10", upToDate: "true")
    ])
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))
    let reader = ShapeStreamReader(
      streamURL: catchUpStream, transport: transport, materializer: log,
      startingAt: StreamCursor(offset: "10"), configuration: .init(liveReadMode: .sse),
      onCaughtUp: { await log.caughtUp($0) })

    await #expect(throws: CancellationError.self) { try await reader.run() }

    #expect(await log.events == [.caughtUp("10")])
    #expect(await transport.lives == [nil, "long-poll"])
  }

  /// The subscription coordinator hands `onCaughtUp` to the reader of the claim it streams.
  @Test func aSubscriptionCoordinatorReportsItsStreamsHead() async throws {
    let transport = CoordinatedCatchUpTransport()
    let log = CatchUpLog(cursor: StreamCursor(offset: "10"))
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport, request: ShapeRequest(table: "public.items", subscription: "claim"),
      materializer: log, retryPolicy: ShapeSubscriptionRetryPolicy(maxRetries: 0),
      onCaughtUp: { await log.caughtUp($0) })

    try await coordinator.start()
    // The tail parks in a long-poll at the head once it has reported it.
    for _ in 0..<2_500 where await transport.lives.count < 2 {
      try await Task.sleep(for: .milliseconds(2))
    }

    #expect(await log.events == [.applied("11"), .caughtUp("11")])
    #expect(await transport.lives == [nil, "long-poll"])
    try await coordinator.stop()
  }
}

/// A transport without `stream`, as one written before SSE is.
private actor LongPollOnlyTransport: HTTPTransport {
  private let base: CatchUpTransport

  init(_ replies: [CatchUpTransport.Reply]) { base = CatchUpTransport(replies) }

  var lives: [String?] { get async { await base.lives } }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    try await base.send(request)
  }
}

/// A control plane with one stream that is one batch ahead of offset `10`.
private actor CoordinatedCatchUpTransport: HTTPTransport {
  private(set) var lives: [String?] = []
  private var parked: [CheckedContinuation<Void, Never>] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    func reply(_ body: String, status: Int = 200, headers: [String: String] = [:])
      -> HTTPResponse
    {
      HTTPResponse(
        data: Data(body.utf8),
        response: HTTPURLResponse(
          url: request.url!, statusCode: status, httpVersion: nil, headerFields: headers)!)
    }
    switch request.httpMethod {
    case "POST":
      return reply(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/shape/s1","#
          + #""streamUrl":"https://streams.test/shape/s1","subscription":"claim"}"#)
    case "DELETE":
      return reply("", status: 204)
    default:
      let query = URLComponents(url: request.url!, resolvingAgainstBaseURL: false)?.queryItems
      lives.append(query?.first { $0.name == "live" }?.value)
      if query?.first(where: { $0.name == "offset" })?.value == "10" {
        return reply(
          change("1"), headers: ["stream-next-offset": "11", "stream-up-to-date": "true"])
      }
      // At the head, a long-poll is held until the coordinator stops and cancels it.
      await withTaskCancellationHandler {
        await withCheckedContinuation { continuation in
          if Task.isCancelled { continuation.resume() } else { parked.append(continuation) }
        }
      } onCancel: {
        Task { await self.unpark() }
      }
      throw CancellationError()
    }
  }

  private func unpark() {
    for continuation in parked { continuation.resume() }
    parked.removeAll()
  }
}
