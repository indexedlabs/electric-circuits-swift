import Foundation
import Testing

@testable import ElectricCircuitsSwift

actor MockTransport: HTTPTransport {
  var requests: [URLRequest] = []
  var response: HTTPResponse
  init(response: HTTPResponse) { self.response = response }
  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return response
  }
}

private func response(_ body: String, status: Int = 200) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: URL(string: "https://engine.test")!, statusCode: status, httpVersion: nil,
      headerFields: nil)!)
}

@Suite("Electric Circuits native contract")
struct NativeClientTests {
  @Test func generatedClientIDsAreUniqueCanonicalUUID4Values() throws {
    let values = (0..<32).map { _ in ClientID() }
    #expect(Set(values).count == values.count)
    for value in values {
      let uuid = try #require(UUID(uuidString: value.rawValue))
      let bytes = uuid.uuid
      #expect((bytes.6 & 0xf0) == 0x40)
      #expect((bytes.8 & 0xc0) == 0x80)
      #expect(value.rawValue == value.rawValue.lowercased())
    }
  }

  @Test func clientIDCodableRejectsNonUUID4Values() throws {
    let encoded = try JSONEncoder().encode(ClientID())
    _ = try JSONDecoder().decode(ClientID.self, from: encoded)
    #expect(throws: (any Error).self) {
      try JSONDecoder().decode(
        ClientID.self, from: Data(#"\"00000000-0000-0000-0000-000000000000\""#.utf8))
    }
  }

  @Test func recursivePredicateRoundTripsRustShape() throws {
    let predicate: ElectricCircuitsSwift.Predicate = .and([
      .leaf(column: "age", op: .gte, value: .int(18)),
      .not(.isNull(column: "name", isNull: true)),
    ])
    let data = try JSONEncoder().encode(predicate)
    let encoded = String(decoding: data, as: UTF8.self)
    #expect(
      encoded.contains(#""and"#) && encoded.contains(#""col":"age"#)
        && encoded.contains(#""op":"gte"#) && encoded.contains(#""isNull":true"#))
    #expect(try JSONDecoder().decode(Predicate.self, from: data) == predicate)
  }

  @Test func shapeCreateUsesStableSubscriptionAndMapsHandle() async throws {
    let payload =
      #"{"shapeId":"s1","table":"public.items","streamPath":"/s/1","streamUrl":"https://streams.test/s/1","subscription":"claim-1","leaseSeconds":30}"#
      .data(using: .utf8)!
    let transport = MockTransport(
      response: HTTPResponse(
        data: payload,
        response: HTTPURLResponse(
          url: URL(string: "https://engine.test")!, statusCode: 200, httpVersion: nil,
          headerFields: nil)!))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let handle = try await client.createShape(
      ShapeRequest(table: "public.items", subscription: "claim-1"))
    #expect(handle.id == "s1")
    #expect(handle.stream.path == "/s/1")
    #expect(await transport.requests.first?.url?.path == "/v1/shapes")
    #expect(
      await transport.requests.first?.httpBody.flatMap { String(decoding: $0, as: UTF8.self) }?
        .contains("claim-1") == true)
  }

  @Test func shapeCreateRestoresOmittedStableSubscriptionAndRejectsMismatch() async throws {
    let omitted = MockTransport(
      response: response(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/s/1","streamUrl":"https://streams.test/s/1"}"#
      ))
    let omittedClient = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: omitted)
    let handle = try await omittedClient.createShape(
      ShapeRequest(table: "public.items", subscription: "stable-claim"))
    #expect(handle.subscription == "stable-claim")

    let mismatched = MockTransport(
      response: response(
        #"{"shapeId":"s1","table":"public.items","streamPath":"/s/1","streamUrl":"https://streams.test/s/1","subscription":"other"}"#
      ))
    let mismatchedClient = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: mismatched)
    await #expect(
      throws: ClientError.decoding("shape response subscription does not match request")
    ) {
      _ = try await mismatchedClient.createShape(
        ShapeRequest(table: "public.items", subscription: "stable-claim"))
    }
  }

  @Test func subsetQueryAndReleaseUseNativeRoutes() async throws {
    let subset = #"{"rows":[{"id":1}],"lsn":"0/10"}"#.data(using: .utf8)!
    let transport = MockTransport(
      response: HTTPResponse(
        data: subset,
        response: HTTPURLResponse(
          url: URL(string: "https://engine.test")!, statusCode: 200, httpVersion: nil,
          headerFields: nil)!))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let result = try await client.querySubset(SubsetQuery(table: "public.items", limit: 1))
    #expect(result.lsn == "0/10")
    let response = ShapeResponse(
      shapeId: "s", table: "public.items", streamPath: "/s",
      streamURL: URL(string: "https://streams.test/s")!, subscription: "claim", leaseSeconds: nil,
      state: nil, subscriptions: nil)
    try await client.releaseShape(ShapeHandle(response: response))
    let paths = await transport.requests.compactMap(\.url?.path)
    #expect(paths == ["/v1/subsets/query", "/v1/shapes/s"])
    #expect(await transport.requests.map(\.httpMethod) == ["POST", "DELETE"])
    #expect(await transport.requests.last?.url?.query == "subscription=claim")
  }

  @Test func streamCursorUsesHeadFrontierHeader() async throws {
    let transport = MockTransport(
      response: HTTPResponse(
        data: Data(),
        response: HTTPURLResponse(
          url: URL(string: "https://engine.test/v1/streams/s")!, statusCode: 200,
          httpVersion: nil, headerFields: ["stream-next-offset": "42"])!))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let response = ShapeResponse(
      shapeId: "s", table: "public.items", streamPath: "/v1/streams/s",
      streamURL: URL(string: "https://engine.test/v1/streams/s")!, subscription: "claim")

    #expect(
      try await client.streamCursor(for: ShapeHandle(response: response))
        == StreamCursor(offset: "42"))
    #expect(await transport.requests.first?.httpMethod == "HEAD")
  }

  @Test func subsetSnapshotReturnsMostRecentTenAndEncodesNativeRequest() async throws {
    struct IssueFixture: Sendable {
      let id: Int64
      let modified: Int64
      let status: String

      var row: JSONValue {
        .object([
          "id": .int(id),
          "modified": .int(modified),
          "status": .string(status),
        ])
      }
    }

    let allIssues: [IssueFixture] = [
      .init(id: 1, modified: 100, status: "open"),
      .init(id: 2, modified: 200, status: "open"),
      .init(id: 3, modified: 300, status: "closed"),
      .init(id: 4, modified: 400, status: "open"),
      .init(id: 5, modified: 500, status: "open"),
      .init(id: 6, modified: 600, status: "open"),
      .init(id: 7, modified: 700, status: "closed"),
      .init(id: 8, modified: 800, status: "open"),
      .init(id: 9, modified: 900, status: "open"),
      .init(id: 10, modified: 1_000, status: "open"),
      .init(id: 11, modified: 1_100, status: "open"),
      .init(id: 12, modified: 1_200, status: "open"),
      .init(id: 13, modified: 1_300, status: "open"),
      .init(id: 14, modified: 1_300, status: "open"),
      .init(id: 15, modified: 1_250, status: "closed"),
    ]
    let predicate: ElectricCircuitsSwift.Predicate = .leaf(
      column: "status", op: .eq, value: .string("open"))
    let request = SubsetQuery(
      table: "public.issues",
      where: predicate,
      columns: ["id", "modified", "status"],
      orderBy: SubsetOrderBy(column: "modified", descending: true),
      limit: 10)

    // The Rust endpoint accepts one order column and appends the primary key in the same
    // direction. This independently computes the expected page, including the modified=1300 tie.
    let expectedIssues =
      allIssues
      .filter { $0.status == "open" }
      .sorted {
        if $0.modified != $1.modified { return $0.modified > $1.modified }
        return $0.id > $1.id
      }
      .prefix(10)
    let expectedRows = expectedIssues.map(\.row)
    #expect(expectedIssues.prefix(2).map(\.id) == [14, 13])
    let payload = try JSONEncoder().encode(SubsetResponse(rows: expectedRows, lsn: "0/10"))
    let transport = MockTransport(response: response(String(decoding: payload, as: UTF8.self)))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)

    let result = try await client.querySubset(request)
    #expect(result.rows == expectedRows)
    #expect(result.rows.count == 10)
    #expect(result.lsn == "0/10")

    let body = try #require(await transport.requests.first?.httpBody)
    let encodedRequest = try JSONDecoder().decode(JSONValue.self, from: body)
    #expect(
      encodedRequest
        == .object([
          "table": .string("public.issues"),
          "where": .object([
            "col": .string("status"),
            "op": .string("eq"),
            "value": .string("open"),
          ]),
          "columns": .array([.string("id"), .string("modified"), .string("status")]),
          "orderBy": .object(["col": .string("modified"), "desc": .bool(true)]),
          "limit": .int(10),
        ]))
    #expect(await transport.requests.first?.url?.path == "/v1/subsets/query")
    #expect(await transport.requests.first?.httpMethod == "POST")
  }

  @Test func integerAndDecimalJSONValuesRemainExact() throws {
    let payload = Data(#"{"id":9007199254740993,"ratio":0.1}"#.utf8)
    let value = try JSONDecoder().decode(JSONValue.self, from: payload)
    guard case .object(let object) = value else {
      Issue.record("expected object")
      return
    }
    #expect(object["id"] == .int(9_007_199_254_740_993))
    #expect(object["ratio"] == .decimal(Decimal(string: "0.1")!))
    #expect(try JSONDecoder().decode(JSONValue.self, from: JSONEncoder().encode(value)) == value)
  }

  @Test func inPredicateRoundTripsWithBothNegationForms() throws {
    let subquery = PredicateSubquery(
      table: "public.projects", project: "id",
      where: .leaf(column: "archived", op: .eq, value: .bool(false)))
    for negated in [false, true] {
      let predicate = Predicate.in(column: "project_id", subquery: subquery, negated: negated)
      let data = try JSONEncoder().encode(predicate)
      let decoded = try JSONDecoder().decode(Predicate.self, from: data)
      #expect(decoded == predicate)
    }
  }

  @Test func aggregateUsesShapeResponseContract() async throws {
    let transport = MockTransport(
      response: response(
        #"{"shapeId":"a1","table":"public.items","streamPath":"/s/a1","streamUrl":"https://streams.test/s/a1","subscription":"claim"}"#
      ))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let handle = try await client.createAggregate(
      AggregateRequest(table: "public.items", function: .count, subscription: "claim"))
    #expect(handle.id == "a1")
    #expect(await transport.requests.first?.httpMethod == "POST")
    #expect(await transport.requests.first?.url?.path == "/v1/aggregates")
  }

  @Test func nonSuccessAndMalformedResponsesAreTypedErrors() async throws {
    let unauthorized = MockTransport(response: response("unauthorized", status: 401))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: unauthorized)
    await #expect(throws: ClientError.http(status: 401, message: "HTTP request failed")) {
      try await client.querySubset(SubsetQuery(table: "public.items"))
    }

    let malformed = MockTransport(response: response("not-json"))
    let malformedClient = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: malformed)
    await #expect(throws: ClientError.self) {
      try await malformedClient.querySubset(SubsetQuery(table: "public.items"))
    }
  }

  @Test func releaseTreatsAlreadyRetiredShapeAsIdempotent() async throws {
    let transport = MockTransport(response: response("gone", status: 404))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    let metadata = ShapeResponse(
      shapeId: "retired", table: "public.items", streamPath: "/s/retired",
      streamURL: URL(string: "https://streams.test/s/retired")!, subscription: "claim")
    try await client.releaseShape(ShapeHandle(response: metadata))
    #expect(await transport.requests.count == 1)
  }

  @Test func shapeIDsAreEscapedAsOnePathComponent() async throws {
    let transport = MockTransport(
      response: response(
        #"{"shapeId":"s","table":"public.items","streamPath":"/s","streamUrl":"https://streams.test/s"}"#
      ))
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://engine.test")!, transport: transport)
    _ = try await client.getShape(id: "x/y?z#fragment")
    let url = await transport.requests.first?.url
    #expect(url?.absoluteString == "https://engine.test/v1/shapes/x%2Fy%3Fz%23fragment")
    #expect(url?.query == nil)
  }

  @Test func inMemoryStoreIsAsyncAndDeterministic() async throws {
    let store = InMemoryElectricCircuitsStore(initialValues: ["b": Data([2]), "a": Data([1])])
    #expect(await store.keys() == ["a", "b"])
    let initial = try await store.read(forKey: "a")
    #expect(initial == Data([1]))
    try await store.write(Data([3]), forKey: "c")
    try await store.removeValue(forKey: "b")
    #expect(await store.keys() == ["a", "c"])
    let removed = try await store.read(forKey: "b")
    #expect(removed == nil)
  }
}
