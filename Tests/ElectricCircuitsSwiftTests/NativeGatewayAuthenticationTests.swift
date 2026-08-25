import Foundation
import Testing

@testable import ElectricCircuitsSwift

private final class NativeAuthRequestRecorder: @unchecked Sendable {
  private let lock = NSLock()
  private var requests: [URLRequest] = []

  func reset() {
    lock.withLock { requests.removeAll() }
  }

  func record(_ request: URLRequest) {
    lock.withLock { requests.append(request) }
  }

  func first() -> URLRequest? {
    lock.withLock { requests.first }
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
    Self.recorder.record(request)
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

@Suite("Native gateway authentication forwarding", .serialized)
struct NativeGatewayAuthenticationTests {
  @Test func transportForwardsCallerAndConfiguredCredentialsToTheNativeGateway() async throws {
    NativeAuthURLProtocol.recorder.reset()
    let configuration = URLSessionConfiguration.ephemeral
    configuration.protocolClasses = [NativeAuthURLProtocol.self]
    let session = URLSession(configuration: configuration)
    defer { session.invalidateAndCancel() }
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
    let forwarded = try #require(NativeAuthURLProtocol.recorder.first())
    #expect(forwarded.value(forHTTPHeaderField: "Authorization") == "Bearer gateway-token")
    #expect(forwarded.value(forHTTPHeaderField: "X-Principal") == "caller")
    #expect(forwarded.value(forHTTPHeaderField: "Cookie")?.contains("session=cookie-token") == true)
  }
}
