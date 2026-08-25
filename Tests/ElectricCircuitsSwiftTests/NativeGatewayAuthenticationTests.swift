import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor NativeAuthRequestRecorder {
  private var requests: [URLRequest] = []

  func reset() {
    requests.removeAll()
  }

  func record(_ request: URLRequest) {
    requests.append(request)
  }

  func first() -> URLRequest? {
    requests.first
  }
}

private final class NativeAuthURLProtocol: URLProtocol, @unchecked Sendable {
  static let recorder = NativeAuthRequestRecorder()

  override class func canInit(with request: URLRequest) -> Bool {
    request.url?.host == "engine.test"
  }

  override class func canonicalRequest(for request: URLRequest) -> URLRequest {
    request
  }

  override func startLoading() {
    let request = request
    Task { await Self.recorder.record(request) }
    guard let client, let url = request.url else { return }
    client.urlProtocol(
      self,
      didReceive: HTTPURLResponse(url: url, statusCode: 200, httpVersion: nil, headerFields: nil)!,
      cacheStoragePolicy: .notAllowed)
    client.urlProtocol(self, didLoad: Data("{}".utf8))
    client.urlProtocolDidFinishLoading(self)
  }

  override func stopLoading() {}
}

private func waitForAuthenticatedRequest() async throws -> URLRequest {
  for _ in 0..<10_000 {
    if let request = await NativeAuthURLProtocol.recorder.first() { return request }
    await Task.yield()
  }
  throw CancellationError()
}

@Suite("Native gateway authentication forwarding", .serialized)
struct NativeGatewayAuthenticationTests {
  @Test func transportForwardsCallerAndConfiguredCredentialsToTheNativeGateway() async throws {
    await NativeAuthURLProtocol.recorder.reset()
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [NativeAuthURLProtocol.self]
    let session = URLSession(configuration: configuration)
    let cookie = try #require(
      HTTPCookie(properties: [
        .domain: "engine.test", .path: "/", .name: "session", .value: "cookie-token",
        .secure: "TRUE",
      ]))
    let transport = URLSessionTransport(
      session: session,
      headers: ["Authorization": "Bearer gateway-token", "X-Principal": "configured"],
      cookies: [cookie])
    var request = URLRequest(url: URL(string: "https://engine.test/v1/subset-feeds")!)
    request.httpMethod = "POST"
    request.setValue("caller", forHTTPHeaderField: "X-Principal")

    _ = try await transport.send(request)
    let forwarded = try await waitForAuthenticatedRequest()
    #expect(forwarded.value(forHTTPHeaderField: "Authorization") == "Bearer gateway-token")
    #expect(forwarded.value(forHTTPHeaderField: "X-Principal") == "caller")
    #expect(forwarded.value(forHTTPHeaderField: "Cookie")?.contains("session=cookie-token") == true)
  }
}
