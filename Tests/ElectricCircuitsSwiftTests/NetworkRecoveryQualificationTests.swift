import Foundation
import Testing

@testable import ElectricCircuitsSwift

private actor RecoveryReceiptGate {
  private var completed: Set<String> = []
  private var waiters: [String: [CheckedContinuation<Void, Never>]] = [:]

  func record(_ receipt: String) {
    guard completed.insert(receipt).inserted else { return }
    for waiter in waiters.removeValue(forKey: receipt) ?? [] { waiter.resume() }
  }

  func wait(for receipt: String) async {
    guard !completed.contains(receipt) else { return }
    await withCheckedContinuation { waiters[receipt, default: []].append($0) }
  }
}

private actor RecoveryMaterializer: ShapeMaterializer {
  private let receipts: RecoveryReceiptGate
  private var rows: [String: ChangeRow]
  private var cursor: StreamCursor?
  private(set) var applies = 0

  init(
    receipts: RecoveryReceiptGate,
    rows: [String: ChangeRow] = [:],
    cursor: StreamCursor? = nil
  ) {
    self.receipts = receipts
    self.rows = rows
    self.cursor = cursor
  }

  func currentCursor() async throws -> StreamCursor? { cursor }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo nextCursor: StreamCursor
  ) async throws {
    guard cursor == expectedCursor else {
      throw StreamError.cursorConflict(
        expected: expectedCursor, actual: cursor, advancingTo: nextCursor)
    }
    var nextRows = rows
    for envelope in batch.envelopes {
      switch envelope.headers.operation {
      case .delete:
        nextRows.removeValue(forKey: envelope.key)
      case .insert, .update, .upsert:
        guard let value = envelope.value else { throw StreamError.missingValue(key: envelope.key) }
        nextRows[envelope.key] = value
      }
    }
    rows = nextRows
    cursor = nextCursor
    applies += 1
    await receipts.record("provider-applied-\(nextCursor.offset)")
  }

  func snapshot() -> (rows: [String: ChangeRow], cursor: StreamCursor?, applies: Int) {
    (rows, cursor, applies)
  }
}

private actor RecoveryTelemetrySink: TelemetrySink {
  private var requests: [URLRequest] = []

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    return recoveryResponse(url: request.url!, status: 202)
  }

  func exportedBodies() -> [String] {
    requests.compactMap(\.httpBody).map { String(decoding: $0, as: UTF8.self) }
  }
}

/// A test-only caller-owned path gate. It intentionally does not report reachability into the
/// client: when unavailable it holds the in-flight request, and the caller decides when that
/// request may continue. This is the supported alternative to baking a reachability framework or
/// retry scheduler into the Foundation-only package.
private actor CallerOwnedPathGateTransport: HTTPTransport {
  private enum HeldPollState {
    case registered(waitsForAvailability: Bool)
    case waiting(waitsForAvailability: Bool, continuation: CheckedContinuation<Void, Never>)
    case resumed
    case cancelled
  }

  private let receipts: RecoveryReceiptGate
  private var isAvailable = false
  private var nextHeldPollID: UInt64 = 0
  private var heldPolls: [UInt64: HeldPollState] = [:]
  private var pollCountWaiters: [(Int, CheckedContinuation<Void, Never>)] = []
  private var releaseWaiters: [CheckedContinuation<Void, Never>] = []
  private var pauseFirstPollBeforePathHandlerInstallation = false
  private var firstPollPathHandlerInstallationPause: CheckedContinuation<Void, Never>?
  private var pauseFirstPollAfterPathResolution = false
  private var firstPollPathResolutionPause: CheckedContinuation<Void, Never>?
  private var batchSent = false
  private var pollCount = 0
  private var releaseCount = 0
  private var cancelledPollCount = 0
  private var requests: [URLRequest] = []

  init(receipts: RecoveryReceiptGate) { self.receipts = receipts }

  func send(_ request: URLRequest) async throws -> HTTPResponse {
    requests.append(request)
    switch request.httpMethod {
    case "POST":
      await receipts.record("claim-created")
      return recoveryShapeResponse(url: request.url!, id: "path-claim")
    case "DELETE":
      releaseCount += 1
      for waiter in releaseWaiters { waiter.resume() }
      releaseWaiters.removeAll()
      await receipts.record("claim-released")
      return recoveryResponse(url: request.url!, status: 404)
    case "GET":
      pollCount += 1
      resumePollCountWaiters()
      await receipts.record("poll-\(pollCount)-entered")
      if !isAvailable {
        await waitForPathGate(waitingForAvailability: true)
      }
      try Task.checkCancellation()
      if !batchSent {
        batchSent = true
        return recoveryResponse(
          url: request.url!, body: recoveryEvent,
          headers: ["stream-next-offset": "8"])
      }
      // A second long poll remains parked until lifecycle cancellation. It keeps the test's end
      // condition causal (the provider receipt), rather than using a sleep to race a hot idle loop.
      await waitForPathGate(waitingForAvailability: false)
      try Task.checkCancellation()
      throw CancellationError()
    default:
      throw CancellationError()
    }
  }

  func waitForPoll(count: Int) async {
    guard pollCount < count else { return }
    await withCheckedContinuation { pollCountWaiters.append((count, $0)) }
  }

  func becomeAvailable() {
    isAvailable = true
    var ready: [(UInt64, CheckedContinuation<Void, Never>)] = []
    for (id, state) in heldPolls {
      switch state {
      case .registered(waitsForAvailability: true):
        // The lifetime exists before the continuation is installed. The operation will observe
        // this terminal state and resume itself when it reaches the continuation boundary.
        heldPolls[id] = .resumed
      case .waiting(waitsForAvailability: true, let continuation):
        heldPolls[id] = .resumed
        ready.append((id, continuation))
      case .registered, .waiting, .resumed, .cancelled:
        break
      }
    }
    for (_, continuation) in ready {
      continuation.resume()
    }
  }

  func pauseFirstPollBeforePathHandlerInstallationForTesting() {
    pauseFirstPollBeforePathHandlerInstallation = true
  }

  func releaseFirstPollPathHandlerInstallationPauseForTesting() {
    firstPollPathHandlerInstallationPause?.resume()
    firstPollPathHandlerInstallationPause = nil
  }

  func pauseFirstPollAfterPathResolutionForTesting() {
    pauseFirstPollAfterPathResolution = true
  }

  func releaseFirstPollPathResolutionPauseForTesting() {
    firstPollPathResolutionPause?.resume()
    firstPollPathResolutionPause = nil
  }

  func waitForRelease() async {
    guard releaseCount == 0 else { return }
    await withCheckedContinuation { releaseWaiters.append($0) }
  }

  func requestSnapshot() -> [URLRequest] { requests }
  func cancellationSnapshot() -> Int { cancelledPollCount }
  func outstandingWaiterCount() -> Int {
    heldPolls.count
      + (firstPollPathHandlerInstallationPause == nil ? 0 : 1)
      + (firstPollPathResolutionPause == nil ? 0 : 1)
  }

  private func resumePollCountWaiters() {
    let ready = pollCountWaiters.filter { pollCount >= $0.0 }
    pollCountWaiters.removeAll { pollCount >= $0.0 }
    for (_, waiter) in ready { waiter.resume() }
  }

  private func waitForPathGate(waitingForAvailability: Bool) async {
    nextHeldPollID &+= 1
    let id = nextHeldPollID
    // Install the lifetime before registering the cancellation handler. A delayed Task created by
    // `onCancel` can only transition this exact ID; it can never recreate a removed marker.
    heldPolls[id] = .registered(waitsForAvailability: waitingForAvailability)
    defer { heldPolls.removeValue(forKey: id) }
    await pauseBeforePathHandlerInstallationIfNeeded(id: id)
    await withTaskCancellationHandler(
      operation: {
        await withCheckedContinuation { continuation in
          guard let state = heldPolls[id] else {
            continuation.resume()
            return
          }
          switch state {
          case .registered(let waitsForAvailability):
            if waitsForAvailability && isAvailable {
              heldPolls[id] = .resumed
              continuation.resume()
            } else {
              heldPolls[id] = .waiting(
                waitsForAvailability: waitsForAvailability, continuation: continuation)
            }
          case .resumed, .cancelled:
            continuation.resume()
          case .waiting:
            preconditionFailure("one path-gate operation installed two continuations")
          }
        }
        await pauseAfterPathResolutionIfNeeded(id: id)
      },
      onCancel: { Task { await self.cancelHeldPoll(id) } })
  }

  private func pauseBeforePathHandlerInstallationIfNeeded(id: UInt64) async {
    guard pollCount == 1, pauseFirstPollBeforePathHandlerInstallation else { return }
    pauseFirstPollBeforePathHandlerInstallation = false
    await receipts.record("poll-\(id)-lifetime-installed")
    await withCheckedContinuation { firstPollPathHandlerInstallationPause = $0 }
  }

  private func pauseAfterPathResolutionIfNeeded(id: UInt64) async {
    guard pollCount == 1, pauseFirstPollAfterPathResolution else { return }
    pauseFirstPollAfterPathResolution = false
    await receipts.record("poll-\(id)-availability-resolved")
    await withCheckedContinuation { firstPollPathResolutionPause = $0 }
  }

  private func cancelHeldPoll(_ id: UInt64) async {
    guard let state = heldPolls[id] else {
      await receipts.record("poll-\(id)-cancel-ignored")
      return
    }
    switch state {
    case .registered:
      heldPolls[id] = .cancelled
      cancelledPollCount += 1
    case .waiting(_, let continuation):
      heldPolls[id] = .cancelled
      cancelledPollCount += 1
      continuation.resume()
    case .resumed, .cancelled:
      await receipts.record("poll-\(id)-cancel-ignored")
    }
  }
}

private let recoveryEvent =
  """
  [
    {
      "type": "public.items",
      "key": "1",
      "value": { "id": 1, "title": "resumed" },
      "headers": { "operation": "upsert", "lsn": "0/8" }
    }
  ]
  """

private func recoveryResponse(
  url: URL,
  body: String = "",
  status: Int = 200,
  headers: [String: String] = [:]
) -> HTTPResponse {
  HTTPResponse(
    data: Data(body.utf8),
    response: HTTPURLResponse(
      url: url, statusCode: status, httpVersion: nil, headerFields: headers)!)
}

private func recoveryShapeResponse(url: URL, id: String) -> HTTPResponse {
  recoveryResponse(
    url: url,
    body: """
      {
        "shapeId": "\(id)",
        "table": "public.items",
        "streamPath": "/v1/streams/\(id)",
        "streamUrl": "https://streams.test/v1/streams/\(id)",
        "subscription": "claim-path"
      }
      """)
}

// The URLSession DNS operation and the caller-owned cancellation gates deliberately exercise
// process-global Foundation networking. Keep this small qualification suite ordered with itself;
// it remains independent of the package's unrelated suites.
@Suite("Native network recovery qualification", .serialized)
struct NetworkRecoveryQualificationTests {
  @Test func urlSessionDNSFailureIsTypedBoundedAndRedactedBeforeProviderMutation() async throws {
    let receipts = RecoveryReceiptGate()
    let provider = RecoveryMaterializer(
      receipts: receipts, rows: ["old": ["id": .int(7)]],
      cursor: StreamCursor(offset: "7", lsn: "0/7"))
    let transportSecret = "dns-header-secret"
    let cookieSecret = "dns-cookie-secret"
    let bodySecret = "dns-request-body-secret"
    let configuration = URLSessionConfiguration.ephemeral
    configuration.timeoutIntervalForRequest = 2
    configuration.timeoutIntervalForResource = 3
    let session = URLSession(configuration: configuration)
    defer { session.invalidateAndCancel() }
    let transport = URLSessionTransport(
      session: session,
      headers: ["Authorization": "Bearer \(transportSecret)"],
      cookieHeader: "session=\(cookieSecret)")
    let sink = RecoveryTelemetrySink()
    let telemetry = TelemetryReporter(
      configuration: .init(tracesEndpoint: URL(string: "https://collector.test/v1/traces")!),
      sink: sink)
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://electric-circuits-no-dns.invalid")!, transport: transport,
        telemetry: telemetry),
      transport: transport,
      request: ShapeRequest(table: "public.\(bodySecret)", subscription: bodySecret),
      materializer: provider,
      retryPolicy: .init(maxRetries: 0, jitterRatio: 0), telemetry: telemetry)

    do {
      _ = try await coordinator.start()
      Issue.record("expected reserved .invalid DNS host to fail")
    } catch let failure as ShapeSubscriptionFailure {
      guard
        case .retryExhausted(
          operation: "create", attempts: 1, cause: .transport(.url(let code))) = failure
      else {
        Issue.record("expected one typed transport attempt, got \(String(describing: failure))")
        return
      }
      #expect(code == URLError.cannotFindHost.rawValue)
      let diagnostic = String(describing: failure)
      #expect(!diagnostic.contains(transportSecret))
      #expect(!diagnostic.contains(cookieSecret))
      #expect(!diagnostic.contains(bodySecret))
      #expect(await coordinator.state == .failed(failure))
    }
    await telemetry.flush()
    let telemetryBody = await sink.exportedBodies().joined(separator: "\n")
    #expect(!telemetryBody.contains(transportSecret))
    #expect(!telemetryBody.contains(cookieSecret))
    #expect(!telemetryBody.contains(bodySecret))
    #expect(await provider.snapshot().cursor == StreamCursor(offset: "7", lsn: "0/7"))
    #expect(await provider.snapshot().rows == ["old": ["id": .int(7)]])
    #expect(await provider.snapshot().applies == 0)
    await telemetry.shutdown()
    session.invalidateAndCancel()
    await receipts.record("dns-session-invalidated")
  }

  @Test func callerOwnedPathGateAvoidsRetryChurnAndResumesTheExistingClaimAndCursor() async throws {
    let receipts = RecoveryReceiptGate()
    let provider = RecoveryMaterializer(
      receipts: receipts, cursor: StreamCursor(offset: "7", lsn: "0/7"))
    let transport = CallerOwnedPathGateTransport(receipts: receipts)
    let coordinator = ShapeSubscriptionCoordinator(
      client: ElectricCircuitsClient(
        baseURL: URL(string: "https://engine.test")!, transport: transport),
      transport: transport,
      request: ShapeRequest(table: "public.items", subscription: "claim-path"),
      materializer: provider,
      retryPolicy: .init(maxRetries: 8, jitterRatio: 0))

    let handle = try await coordinator.start()
    #expect(handle.id == "path-claim")
    await transport.waitForPoll(count: 1)
    await receipts.wait(for: "poll-1-entered")
    let whileUnavailable = await transport.requestSnapshot()
    #expect(whileUnavailable.filter { $0.httpMethod == "POST" }.count == 1)
    #expect(whileUnavailable.filter { $0.httpMethod == "GET" }.count == 1)
    #expect(
      URLComponents(
        url: try #require(whileUnavailable.first(where: { $0.httpMethod == "GET" })?.url),
        resolvingAgainstBaseURL: false)?
        .queryItems?.first(where: { $0.name == "offset" })?.value == "7")
    #expect(await provider.snapshot().cursor == StreamCursor(offset: "7", lsn: "0/7"))

    await transport.becomeAvailable()
    await receipts.wait(for: "provider-applied-8")
    await transport.waitForPoll(count: 2)
    await receipts.wait(for: "poll-2-entered")
    #expect(await provider.snapshot().cursor == StreamCursor(offset: "8", lsn: "0/8"))
    #expect(await provider.snapshot().applies == 1)
    let resumed = await transport.requestSnapshot()
    #expect(resumed.filter { $0.httpMethod == "POST" }.count == 1)
    let resumedPolls = resumed.filter { $0.httpMethod == "GET" }
    #expect(resumedPolls.count == 2)
    #expect(
      URLComponents(
        url: try #require(resumedPolls.last?.url), resolvingAgainstBaseURL: false)?
        .queryItems?.first(where: { $0.name == "offset" })?.value == "8")

    let stopped = Task { try await coordinator.stop() }
    await transport.waitForRelease()
    try await stopped.value
    await receipts.wait(for: "claim-released")
    #expect(await coordinator.state == .stopped)
    #expect(await transport.cancellationSnapshot() == 1)
    #expect(await transport.outstandingWaiterCount() == 0)
  }

  @Test func cancellationDuringHeldPathResponseLeavesDurableCursorUntouched() async throws {
    let receipts = RecoveryReceiptGate()
    let provider = RecoveryMaterializer(
      receipts: receipts, rows: ["old": ["id": .int(7)]],
      cursor: StreamCursor(offset: "7", lsn: "0/7"))
    let transport = CallerOwnedPathGateTransport(receipts: receipts)
    await transport.pauseFirstPollBeforePathHandlerInstallationForTesting()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/v1/streams/claim-path")!, transport: transport,
      materializer: provider)

    let read = Task { try await reader.run() }
    await transport.waitForPoll(count: 1)
    await receipts.wait(for: "poll-1-entered")
    await receipts.wait(for: "poll-1-lifetime-installed")
    read.cancel()
    await transport.releaseFirstPollPathHandlerInstallationPauseForTesting()
    await #expect(throws: CancellationError.self) { try await read.value }
    #expect(await transport.cancellationSnapshot() == 1)
    #expect(await transport.outstandingWaiterCount() == 0)
    #expect(await provider.snapshot().cursor == StreamCursor(offset: "7", lsn: "0/7"))
    #expect(await provider.snapshot().rows == ["old": ["id": .int(7)]])
    #expect(await provider.snapshot().applies == 0)
  }

  @Test func lateCancellationAfterAvailabilityResolutionLeavesNoPathGateState() async throws {
    let receipts = RecoveryReceiptGate()
    let provider = RecoveryMaterializer(
      receipts: receipts, cursor: StreamCursor(offset: "7", lsn: "0/7"))
    let transport = CallerOwnedPathGateTransport(receipts: receipts)
    await transport.pauseFirstPollAfterPathResolutionForTesting()
    let reader = ShapeStreamReader(
      streamURL: URL(string: "https://streams.test/v1/streams/claim-path")!, transport: transport,
      materializer: provider)

    let read = Task { try await reader.run() }
    await transport.waitForPoll(count: 1)
    await receipts.wait(for: "poll-1-entered")
    await transport.becomeAvailable()
    // The lifetime transitioned to `.resumed` before this named pause. Cancellation's delayed
    // actor hop must observe that completed ID and be ignored rather than leave a new marker.
    await receipts.wait(for: "poll-1-availability-resolved")
    read.cancel()
    await receipts.wait(for: "poll-1-cancel-ignored")
    await transport.releaseFirstPollPathResolutionPauseForTesting()
    await #expect(throws: CancellationError.self) { try await read.value }

    #expect(await transport.cancellationSnapshot() == 0)
    #expect(await transport.outstandingWaiterCount() == 0)
    #expect(await provider.snapshot().cursor == StreamCursor(offset: "7", lsn: "0/7"))
    #expect(await provider.snapshot().applies == 0)
  }
}
