import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor ScriptedStreamTransport: HTTPTransport {
  private var responses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []

  init(responses: [HTTPResponse]) {
    self.responses = responses
  }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    try Task.checkCancellation()
    guard !responses.isEmpty else { throw CancellationError() }
    return responses.removeFirst()
  }
}

private actor BlockingMaterializer: ShapeMaterializer {
  private(set) var started = false
  private(set) var appliedCursor: StreamCursor?
  private var released = false

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    started = true
    while !released {
      try Task.checkCancellation()
      await Task.yield()
    }
    appliedCursor = cursor
  }

  func release() {
    released = true
  }
}

private actor CountingMaterializer: ShapeMaterializer {
  private(set) var applyCount = 0
  private let committedCursor: StreamCursor?

  init(committedCursor: StreamCursor? = nil) {
    self.committedCursor = committedCursor
  }

  func currentCursor() async throws -> StreamCursor? { committedCursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    applyCount += 1
  }
}

private actor SnapshotStrengtheningMaterializer: ShapeMaterializer {
  private var cursor: StreamCursor?
  private let firstCommittedCursor: StreamCursor
  private var applies = 0

  init(firstCommittedCursor: StreamCursor) {
    self.firstCommittedCursor = firstCommittedCursor
  }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo next: StreamCursor
  ) async throws {
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(expected: expectedCursor, actual: cursor, advancingTo: next)
    }
    applies += 1
    cursor = applies == 1 ? firstCommittedCursor : next
  }
}

private func streamResponse(
  _ body: String,
  status: Int = 200,
  headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://streams.test/shape/s1")!,
      statusCode: status,
      httpVersion: nil,
      headerFields: headers)!)
}

private func upsertDeleteFixture() throws -> Data {
  let fixtureURL = URL(fileURLWithPath: #filePath)
    .deletingLastPathComponent()
    .deletingLastPathComponent()
    .appendingPathComponent("Fixtures/shape-stream-upsert-delete.json")
  return try Data(contentsOf: fixtureURL)
}

@Suite("Durable stream materialization")
struct StreamMaterializationTests {
  @Test func fixtureDecodesRustEnvelopeAndPreservesTransactionMetadata() throws {
    let batch = try JSONDecoder().decode(ChangeBatch.self, from: upsertDeleteFixture())
    #expect(batch.envelopes.count == 2)
    #expect(batch.envelopes[0].headers.operation == .upsert)
    #expect(batch.envelopes[0].headers.txid == "txn-7")
    #expect(batch.envelopes[0].headers.lsn == "0/16B6A20")
    #expect(batch.envelopes[1].headers.operation == .delete)
    #expect(batch.envelopes[1].old?["title"] == .string("Removed"))

    let encoded = try JSONEncoder().encode(batch)
    let roundTrip = try JSONDecoder().decode(ChangeBatch.self, from: encoded)
    #expect(roundTrip == batch)
  }

  @Test func inMemoryMaterializerUpsertsDeletesAndAdvancesCursor() async throws {
    let materializer = InMemoryShapeMaterializer()
    let batch = try JSONDecoder().decode(ChangeBatch.self, from: upsertDeleteFixture())
    try await materializer.apply(
      batch, expecting: nil, advancingTo: StreamCursor(offset: "42", lsn: "0/16B6A20"))
    #expect(await materializer.rows()["42"]?["title"] == .string("First"))
    #expect(await materializer.rows()["41"] == nil)
    #expect(await materializer.cursor() == StreamCursor(offset: "42", lsn: "0/16B6A20"))
  }

  @Test func inMemoryMaterializerReplaysCommittedCheckpointIdempotently() async throws {
    let materializer = InMemoryShapeMaterializer()
    let row: ChangeRow = ["id": .int(1)]
    let batch = ChangeBatch([
      ChangeEnvelope(
        type: "public.items", key: "1", value: row,
        headers: EnvelopeHeaders(operation: .upsert))
    ])
    let cursor = StreamCursor(offset: "10")
    try await materializer.apply(batch, expecting: nil, advancingTo: cursor)
    try await materializer.apply(
      ChangeBatch([
        ChangeEnvelope(
          type: "public.items", key: "1",
          headers: EnvelopeHeaders(operation: .upsert))
      ]), expecting: cursor, advancingTo: cursor)

    #expect(await materializer.rows() == ["1": row])
    #expect(try await materializer.currentCursor() == cursor)
  }

  @Test func materializationScopeStorageKeyIncludesEveryIdentityComponent() {
    let first = MaterializationScope(
      principal: "ab", template: "c", subscription: "d", generation: "e")
    let second = MaterializationScope(
      principal: "a", template: "bc", subscription: "d", generation: "e")
    #expect(first.storageKey != second.storageKey)
    #expect(first.storageKey == "2:ab|1:c|1:d|1:e")
  }

  @Test func materializerRejectsDistinctCurrentCursorInsteadOfRegressing() async throws {
    let materializer = InMemoryShapeMaterializer()
    let committed = StreamCursor(offset: "10")
    try await materializer.apply(ChangeBatch(), expecting: nil, advancingTo: committed)
    await #expect(
      throws: StreamError.cursorConflict(
        expected: StreamCursor(offset: "9"), actual: committed,
        advancingTo: StreamCursor(offset: "11"))
    ) {
      try await materializer.apply(
        ChangeBatch(), expecting: StreamCursor(offset: "9"), advancingTo: StreamCursor(offset: "11")
      )
    }
    #expect(try await materializer.currentCursor() == committed)
  }

  @Test func idle204AdvancesAdvertisedOffsetWithoutRows() async throws {
    let transport = ScriptedStreamTransport(responses: [
      streamResponse("", status: 204, headers: ["stream-next-offset": "42"])
    ])
    let materializer = InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "41"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer,
      startingAt: StreamCursor(offset: "41"))

    let task = Task { try await reader.run() }
    while await transport.requests.count < 1 { await Task.yield() }
    while await materializer.cursor() == nil { await Task.yield() }
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(await materializer.cursor() == StreamCursor(offset: "42"))
    #expect(await transport.requests[0].url?.query?.contains("offset=41") == true)
    #expect(await transport.requests[0].url?.query?.contains("live=long-poll") == true)
  }

  @Test func idle204WithUnchangedOffsetDoesNotApply() async throws {
    let transport = ScriptedStreamTransport(responses: [
      streamResponse("", status: 204, headers: ["stream-next-offset": "41"]),
      streamResponse("gone", status: 404),
    ])
    let materializer = CountingMaterializer(committedCursor: StreamCursor(offset: "41"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer,
      startingAt: StreamCursor(offset: "41"))

    await #expect(
      throws: StreamError.terminal(path: "/shape/s1", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(await materializer.applyCount == 0)
    #expect(await transport.requests.count == 2)
    #expect(await transport.requests[1].url?.query?.contains("offset=41") == true)
  }

  @Test func emptyArrayAdvancesAdvertisedOffsetWithoutRows() async throws {
    let transport = ScriptedStreamTransport(responses: [
      streamResponse("[]", headers: ["stream-next-offset": "42"])
    ])
    let materializer = InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "41"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer,
      startingAt: StreamCursor(offset: "41"))

    let task = Task { try await reader.run() }
    while await transport.requests.count < 1 { await Task.yield() }
    while await materializer.cursor() == nil { await Task.yield() }
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(await materializer.rows().isEmpty)
    #expect(await materializer.cursor() == StreamCursor(offset: "42"))
  }

  @Test func emptyBodyAdvancesAdvertisedOffsetWithoutRows() async throws {
    let transport = ScriptedStreamTransport(responses: [
      streamResponse("", headers: ["stream-next-offset": "42"])
    ])
    let materializer = InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "41"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer,
      startingAt: StreamCursor(offset: "41"))

    let task = Task { try await reader.run() }
    while await transport.requests.count < 1 { await Task.yield() }
    while await materializer.cursor() == nil { await Task.yield() }
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
    #expect(await materializer.rows().isEmpty)
    #expect(await materializer.cursor() == StreamCursor(offset: "42"))
  }

  @Test func missingNextOffsetIsRejectedForNonemptyBatch() async throws {
    let body =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"
    let transport = ScriptedStreamTransport(responses: [streamResponse(body)])
    let materializer = InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "41"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer,
      startingAt: StreamCursor(offset: "41"))

    await #expect(throws: StreamError.missingNextOffset(path: "/shape/s1")) {
      try await reader.run()
    }
    #expect(await materializer.rows().isEmpty)
    #expect(await materializer.cursor() == StreamCursor(offset: "41"))
  }

  @Test func materializerFailureLeavesPersistedCursorUnchanged() async throws {
    let body =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"headers\":{\"operation\":\"upsert\"}}]"
    let prior = StreamCursor(offset: "41")
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(body, headers: ["stream-next-offset": "42"])
    ])
    let materializer = InMemoryShapeMaterializer(initialCursor: prior)
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer)

    await #expect(throws: StreamError.missingValue(key: "1")) {
      try await reader.run()
    }
    #expect(await materializer.cursor() == prior)
  }

  @Test func readerResumesFromPersistedCursorWhenStartingPointIsOmitted() async throws {
    let persisted = StreamCursor(offset: "73", lsn: "0/ABC")
    let transport = ScriptedStreamTransport(responses: [streamResponse("gone", status: 404)])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: InMemoryShapeMaterializer(initialCursor: persisted))

    await #expect(
      throws: StreamError.terminal(
        path: "/shape/s1", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(await transport.requests[0].url?.query?.contains("offset=73") == true)
  }

  @Test func mismatchedPublicStartingCursorFailsBeforeAnyHTTPPoll() async throws {
    let transport = ScriptedStreamTransport(responses: [])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: InMemoryShapeMaterializer(initialCursor: StreamCursor(offset: "10")),
      startingAt: StreamCursor(offset: "100"))

    await #expect(
      throws: StreamError.startingCursorMismatch(
        durable: StreamCursor(offset: "10"), requested: StreamCursor(offset: "100"))
    ) {
      try await reader.run()
    }
    #expect(await transport.requests.isEmpty)
  }

  @Test func cursorMovesOnlyAfterMaterializerReturns() async throws {
    let body =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(body, headers: ["stream-next-offset": "10"])
    ])
    let materializer = BlockingMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer)
    let task = Task { try await reader.run() }

    while await materializer.started == false { await Task.yield() }
    #expect(await materializer.appliedCursor == nil)
    await materializer.release()
    while await transport.requests.count < 2 { await Task.yield() }
    #expect(await materializer.appliedCursor == StreamCursor(offset: "10"))
    #expect(await transport.requests[1].url?.query?.contains("offset=10") == true)
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
  }

  @Test func sequentialBatchesAdvanceFromTheCommittedCursor() async throws {
    let first =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"
    let second =
      "[{\"type\":\"public.issues\",\"key\":\"2\",\"value\":{\"id\":2},\"headers\":{\"operation\":\"upsert\"}}]"
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(first, headers: ["stream-next-offset": "10"]),
      streamResponse(second, headers: ["stream-next-offset": "11"]),
      streamResponse("gone", status: 404),
    ])
    let materializer = InMemoryShapeMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: materializer)

    await #expect(
      throws: StreamError.terminal(
        path: "/shape/s1", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(await materializer.cursor() == StreamCursor(offset: "11"))
    #expect(await materializer.rows().count == 2)
    let requests = await transport.requests
    #expect(requests[0].url?.query?.contains("offset=-1") == true)
    #expect(requests[1].url?.query?.contains("offset=10") == true)
    #expect(requests[2].url?.query?.contains("offset=11") == true)
  }

  @Test func readerUsesSameOffsetCheckpointWithStrongerSnapshotLSN() async throws {
    let first =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\",\"lsn\":\"0/10\"}}]"
    let second =
      "[{\"type\":\"public.issues\",\"key\":\"2\",\"value\":{\"id\":2},\"headers\":{\"operation\":\"upsert\",\"lsn\":\"0/30\"}}]"
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(first, headers: ["stream-next-offset": "10"]),
      streamResponse(second, headers: ["stream-next-offset": "11"]),
      streamResponse("gone", status: 404),
    ])
    let materializer = SnapshotStrengtheningMaterializer(
      firstCommittedCursor: StreamCursor(offset: "10", lsn: "0/20"))
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: transport,
      materializer: materializer)

    await #expect(
      throws: StreamError.terminal(path: "/shape/s1", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(try await materializer.currentCursor() == StreamCursor(offset: "11", lsn: "0/30"))
    let requests = await transport.requests
    #expect(requests[1].url?.query?.contains("offset=10") == true)
  }

  @Test func readerRejectsMaterializerCheckpointWithDivergentOffset() async throws {
    let body =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\",\"lsn\":\"0/10\"}}]"
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(body, headers: ["stream-next-offset": "10"])
    ])
    let committed = StreamCursor(offset: "unexpected", lsn: "0/10")
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: transport,
      materializer: SnapshotStrengtheningMaterializer(firstCommittedCursor: committed))

    await #expect(
      throws: StreamError.committedCursorMismatch(
        result: StreamCursor(offset: "10", lsn: "0/10"), committed: committed)
    ) {
      try await reader.run()
    }
    #expect(await transport.requests.count == 1)
  }

  @Test func legacyMaterializerWithoutCursorRetainsResultCursorContinuation() async throws {
    let first =
      "[{\"type\":\"public.issues\",\"key\":\"1\",\"value\":{\"id\":1},\"headers\":{\"operation\":\"upsert\"}}]"
    let second =
      "[{\"type\":\"public.issues\",\"key\":\"2\",\"value\":{\"id\":2},\"headers\":{\"operation\":\"upsert\"}}]"
    let transport = ScriptedStreamTransport(responses: [
      streamResponse(first, headers: ["stream-next-offset": "10"]),
      streamResponse(second, headers: ["stream-next-offset": "11"]),
      streamResponse("gone", status: 404),
    ])
    let materializer = CountingMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!, transport: transport,
      materializer: materializer)

    await #expect(
      throws: StreamError.terminal(path: "/shape/s1", status: 404, reason: .notFound)
    ) {
      try await reader.run()
    }
    #expect(await materializer.applyCount == 2)
    #expect(await transport.requests[1].url?.query?.contains("offset=10") == true)
  }

  @Test(arguments: [404, 410])
  func missingStreamIsTerminal(status: Int) async throws {
    let transport = ScriptedStreamTransport(responses: [streamResponse("gone", status: status)])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: InMemoryShapeMaterializer())
    await #expect(
      throws: StreamError.terminal(
        path: "/shape/s1",
        status: status,
        reason: status == 404 ? .notFound : .gone)
    ) {
      try await reader.run()
    }
  }

  @Test func streamClosedHeaderIsTerminal() async throws {
    let transport = ScriptedStreamTransport(responses: [
      streamResponse("", status: 204, headers: ["stream-closed": "true"])
    ])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: InMemoryShapeMaterializer())
    await #expect(
      throws: StreamError.terminal(
        path: "/shape/s1", status: 204, reason: .closed)
    ) {
      try await reader.run()
    }
  }

  @Test func cancellationPropagatesFromLongPoll() async throws {
    let transport = ScriptedStreamTransport(responses: [])
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/shape/s1")!,
      transport: transport,
      materializer: InMemoryShapeMaterializer())
    let task = Task { try await reader.run() }
    while await transport.requests.isEmpty { await Task.yield() }
    task.cancel()
    await #expect(throws: CancellationError.self) { try await task.value }
  }
}
