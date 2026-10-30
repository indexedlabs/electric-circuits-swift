import Foundation
import Testing

@testable import ElectricCircuitsSwift

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

private actor GroupLoadTransport: HTTPTransport {
  private let body: String
  private(set) var requests: [URLRequest] = []

  init(body: String) { self.body = body }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return HTTPResponse(
      data: Data(body.utf8),
      response: HTTPURLResponse(
        url: request.url!, statusCode: 200, httpVersion: nil,
        headerFields: ["Content-Type": "application/json"])!)
  }
}

private let groupLoadAnswer = #"""
  {"groupId":"g-1","shapeId":"g-1","subscription":"scope-1","leaseSeconds":30,
   "streamPath":"/sync/v1/streams/tok","streamUrl":"https://gateway.test/sync/v1/streams/tok",
   "offset":"0000_0010","lsn":"0/10","snapshot":"100:110:105","horizon":"0/20",
   "rows":[{"table":"public.record","value":{"id":"1"}},
           {"table":"public.assignment","value":{"id":"1","record_id":"1"}}],
   "results":[{"key":"public.record","table":"public.record","shapeId":"s-1","subscription":"scope-1"},
              {"key":"public.assignment","table":"public.assignment","shapeId":"s-2","subscription":"scope-1:member:1"}]}
  """#

private func groupRequest() -> SubsetGroupRequest {
  SubsetGroupRequest(
    members: [
      SubsetGroupMember(
        table: "public.record", where: .leaf(column: "id", op: .eq, value: .string("1")),
        columns: ["id"], key: "public.record"),
      SubsetGroupMember(
        table: "public.assignment",
        where: .leaf(column: "record_id", op: .eq, value: .string("1")),
        key: "public.assignment"),
    ],
    subscription: "scope-1")
}

@Suite("Shape group load")
struct SubsetGroupLoadTests {
  @Test func groupLoadPostsEveryMemberAndMapsOneHandle() async throws {
    let transport = GroupLoadTransport(body: groupLoadAnswer)
    let client = ElectricCircuitsClient(
      baseURL: URL(string: "https://gateway.test/sync")!, transport: transport)

    let load = try await client.loadSubsetGroup(groupRequest())

    let requests = await transport.requests
    #expect(requests.count == 1)
    let request = try #require(requests.first)
    #expect(request.httpMethod == "POST")
    #expect(request.url?.path == "/sync/v1/subsets/load")
    let body = try #require(request.httpBody)
    let sent = try JSONDecoder().decode(SubsetGroupRequest.self, from: body)
    #expect(sent == groupRequest())

    #expect(load.handle.id == "g-1")
    #expect(load.groupId == load.shapeId)
    #expect(load.handle.subscription == "scope-1")
    #expect(load.handle.stream.url.absoluteString == "https://gateway.test/sync/v1/streams/tok")
    #expect(load.handle.response.leaseSeconds == 30)
    #expect(load.cursor == StreamCursor(offset: "0000_0010", lsn: "0/10"))
    #expect(load.snapshot == "100:110:105")
    #expect(load.horizon == "0/20")
    #expect(load.rows.map(\.table) == ["public.record", "public.assignment"])
    #expect(
      load.rows.map(\.value) == [
        .object(["id": .string("1")]),
        .object(["id": .string("1"), "record_id": .string("1")]),
      ])
    #expect(load.results.map(\.key) == ["public.record", "public.assignment"])
  }

  @Test func groupLoadRejectsAnAnswerThatDoesNotMatchTheRequest() async throws {
    let otherMembers = groupLoadAnswer.replacingOccurrences(
      of: #""key":"public.assignment""#, with: #""key":"public.tag""#)
    let foreignRow = groupLoadAnswer.replacingOccurrences(
      of: #"{"table":"public.assignment","value""#, with: #"{"table":"public.tag","value""#)
    let otherSubscription = groupLoadAnswer.replacingOccurrences(
      of: #""subscription":"scope-1","leaseSeconds""#,
      with: #""subscription":"scope-2","leaseSeconds""#)
    for body in [otherMembers, foreignRow, otherSubscription] {
      let client = ElectricCircuitsClient(
        baseURL: URL(string: "https://gateway.test/sync")!,
        transport: GroupLoadTransport(body: body))
      await #expect(throws: ClientError.self) {
        _ = try await client.loadSubsetGroup(groupRequest())
      }
    }
  }
}
