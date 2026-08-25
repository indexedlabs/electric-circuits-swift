import Foundation
import Testing

@testable import ElectricCircuitsSwift

/// Scripted transport is intentionally restricted to malformed/fault cases that a conforming
/// native server cannot emit on demand. Real PostgreSQL/Axum coverage remains opt-in in the
/// repository's existing `Scripts/qualify-real-stack-pg18.sh` runner.
private actor NativeV1CorpusTransport: HTTPTransport {
  private var responses: [HTTPResponse]
  private(set) var requests: [URLRequest] = []

  init(_ responses: [HTTPResponse]) { self.responses = responses }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    guard !responses.isEmpty else { throw CancellationError() }
    return responses.removeFirst()
  }
}

private func corpusResponse(
  _ body: String = "", status: Int = 200, headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://native-v1.test/stream/s")!, statusCode: status,
      httpVersion: nil, headerFields: headers)!)
}

@Suite("Native v1 missing contract corpus")
struct NativeV1CorpusTests {
  @Test func subsetRowsPreserveNullAndOmittedFieldsAsDistinctValues() async throws {
    let transport = NativeV1CorpusTransport([
      corpusResponse(
        #"{"rows":[{"id":9007199254740993,"amount":12.34,"note":null},{"id":2}],"lsn":"0/ABC"}"#)
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://native-v1.test")!, transport: transport)

    let response = try await client.querySubset(SubsetQuery(table: "public.items"))
    let first = try #require(response.rows.first)
    let second = try #require(response.rows.last)
    guard case .object(let firstRow) = first, case .object(let secondRow) = second else {
      Issue.record("expected object rows")
      return
    }
    #expect(firstRow["id"] == .int(9_007_199_254_740_993))
    #expect(firstRow["amount"] == .decimal(Decimal(string: "12.34")!))
    #expect(firstRow["note"] == .null)
    #expect(secondRow["note"] == nil)
    #expect(response.lsn == "0/ABC")
  }

  @Test func unknownControlAndEnvelopeFieldsAreForwardCompatible() async throws {
    let control = NativeV1CorpusTransport([
      corpusResponse(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/shape/s1","streamUrl":"https://native-v1.test/stream/s1","futureServerField":{"epoch":2}}"#
      )
    ])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://native-v1.test")!, transport: control)
    let handle = try await client.createShape(
      ShapeRequest(table: "public.items", subscription: "claim"))
    #expect(handle.id == "s1")

    let stream = NativeV1CorpusTransport([
      corpusResponse(
        #"[{"type":"public.items","key":"bare-key","value":{"id":1,"futureColumn":"kept"},"headers":{"operation":"upsert","futureHeader":true},"futureEnvelope":true}]"#,
        headers: ["stream-next-offset": "2"]),
      corpusResponse("gone", status: 404),
    ])
    let materializer = InMemoryShapeMaterializer()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://native-v1.test/stream/s1")!, transport: stream,
      materializer: materializer)
    await #expect(
      throws: StreamError.terminal(path: "/stream/s1", status: 404, reason: .notFound)
    ) { try await reader.run() }
    #expect(await materializer.rows()["bare-key"]?["futureColumn"] == .string("kept"))
    #expect(await materializer.cursor() == StreamCursor(offset: "2"))
  }

  @Test func truncatedEnvelopeFailsClosedWithoutAdvancingCursor() async throws {
    let transport = NativeV1CorpusTransport([
      corpusResponse(
        #"[{"type":"public.items","key":"1","value":{"id":1},"headers":{"operation":"upsert"}}"#,
        headers: ["stream-next-offset": "42"])
    ])
    let prior = StreamCursor(offset: "41")
    let materializer = InMemoryShapeMaterializer(initialCursor: prior)
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://native-v1.test/stream/s")!, transport: transport,
      materializer: materializer, startingAt: prior)

    await #expect(throws: StreamError.self) { try await reader.run() }
    #expect(await materializer.cursor() == prior)
    #expect(await materializer.rows().isEmpty)
  }

  @Test func missingRequiredEnvelopeFieldFailsClosedWithoutAdvancingCursor() async throws {
    let transport = NativeV1CorpusTransport([
      corpusResponse(
        #"[{"type":"public.items","key":"1","value":{"id":1}}]"#,
        headers: ["stream-next-offset": "42"])
    ])
    let prior = StreamCursor(offset: "41")
    let materializer = InMemoryShapeMaterializer(initialCursor: prior)
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://native-v1.test/stream/s")!, transport: transport,
      materializer: materializer, startingAt: prior)

    await #expect(throws: StreamError.self) { try await reader.run() }
    #expect(await materializer.cursor() == prior)
  }

  @Test func headWithoutCursorHeaderIsMalformed() async throws {
    let transport = NativeV1CorpusTransport([corpusResponse()])
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://native-v1.test")!, transport: transport)
    let handle = ShapeHandle(
      response: ShapeResponse(
        shapeId: "s", table: "public.items", streamPath: "/stream/s",
        streamURL: URL(string: "https://native-v1.test/stream/s")!))

    await #expect(
      throws: ClientError.decoding("stream HEAD response missing stream-next-offset")
    ) { _ = try await client.streamCursor(for: handle) }
    #expect(await transport.requests.first?.httpMethod == "HEAD")
  }
}
