import Combine
import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteGRDB

/// The intentionally small lifecycle surface exposed to the SwiftUI layer.
public enum LinearLiteConnectionState: Equatable, Sendable {
  case idle
  case connecting
  case snapshotReady
  case streaming
  case terminal(StreamTerminalReason)
  case failed(String)
  case stopped
}

/// Redacted, joined outcomes for the iOS host bridge. These deliberately do not expose transport,
/// provider, row, or credential diagnostics.
public enum LinearLiteSessionHostStartReceipt: Equatable, Sendable {
  case started(cursor: StreamCursor?)
  case unavailable(MaterializerAvailabilityError)
  case failed
  case cancelled
}

public enum LinearLiteSessionHostStopReceipt: Equatable, Sendable {
  case released
  case failed
}

/// Selects between the original live full-shape lifecycle and a bounded live recent subset feed.
public enum LinearLiteSyncMode: Equatable, Sendable {
  case fullShape
  /// A bounded live view. The predicate is data carried to both the snapshot query and its live
  /// base feed; it is deliberately not a LinearLite-specific filter switch.
  case recentSubset(limit: Int, where: ElectricCircuitsSwift.Predicate? = nil)
}

/// A coarse, user-facing milestone in one sync run. These events intentionally describe only
/// public client behavior; they are not an engine trace or a replacement for server telemetry.
public enum LinearLiteSyncEventKind: String, Equatable, Sendable {
  case syncRequested
  case shapeCreated
  case feedCreated
  case snapshotLoaded
  case streamStarted
  case liveBatchApplied
  case diagnosticTasksRequested
  case diagnosticTasksCreated
  case stopped
  case failed

  public var title: String {
    switch self {
    case .syncRequested: "Sync requested"
    case .shapeCreated: "Shape created"
    case .feedCreated: "Live subset feed created"
    case .snapshotLoaded: "Snapshot loaded"
    case .streamStarted: "Live stream started"
    case .liveBatchApplied: "Live batch applied"
    case .diagnosticTasksRequested: "Diagnostic tasks requested"
    case .diagnosticTasksCreated: "Diagnostic tasks inserted"
    case .stopped: "Sync stopped"
    case .failed: "Sync failed"
    }
  }
}

/// A deterministic presentation record for the Home screen's sync timeline.
public struct LinearLiteSyncEvent: Identifiable, Equatable, Sendable {
  public let id: Int
  public let kind: LinearLiteSyncEventKind
  public let elapsedMilliseconds: Int
  public let detail: String

  public init(
    id: Int, kind: LinearLiteSyncEventKind, elapsedMilliseconds: Int, detail: String = ""
  ) {
    self.id = id
    self.kind = kind
    self.elapsedMilliseconds = elapsedMilliseconds
    self.detail = detail
  }
}

/// One app session owns one local materializer. Full-shape mode retains the original live stream;
/// recent-subset mode maintains a bounded live top-10 view with client-side LSN positioning and
/// targeted query-backs only when a full page needs boundary refill.
@MainActor
public final class LinearLiteSession: ObservableObject {
  public static let issuesTable = "public.issues"
  public static let issueColumns = [
    "id", "client_id", "title", "description", "status", "priority", "username", "project_id",
    "created", "modified", "kanbanorder",
  ]

  public let client: ElectricCircuitsClient
  public let database: DatabaseQueue
  public let subscription: String
  public let transport: any HTTPTransport
  public let mode: LinearLiteSyncMode
  public let materializerAvailability: any MaterializerAvailabilityProbe
  /// Optional durable scope for an application that shares a database between concurrent views.
  /// The provider owns membership and cursor isolation; this session merely carries the immutable
  /// identity selected by its caller.
  public let materializationScope: MaterializationScope?

  @Published public private(set) var issues: [Issue] = []
  @Published public private(set) var connectionState: LinearLiteConnectionState = .idle
  @Published public private(set) var syncEvents: [LinearLiteSyncEvent] = []

  /// Whether a shape/reader is currently being established or streamed.
  public var isSyncActive: Bool {
    switch connectionState {
    case .connecting, .streaming: true
    case .idle, .snapshotReady, .terminal, .failed, .stopped: false
    }
  }

  /// Stable, deterministic presentation grouping used by the view and its tests.
  public var issuesByStatus: [(status: String, issues: [Issue])] {
    let grouped = Dictionary(grouping: issues, by: \.status)
    return grouped.keys.sorted().map { status in
      (
        status: status,
        issues: (grouped[status] ?? []).sorted {
          if $0.kanbanOrder != $1.kanbanOrder { return $0.kanbanOrder < $1.kanbanOrder }
          return $0.id < $1.id
        }
      )
    }
  }

  private var shapeHandle: ShapeHandle?
  private var materializer: LinearLiteShapeMaterializer?
  private var readerTask: Task<Void, Never>?
  private var stopping = false
  private var startInFlight = false
  private var lifecycleGeneration = 0
  private var syncStartedAt: Date?
  private var nextSyncEventID = 0
  private var diagnosticWriteStartedAt: Date?
  private var pendingDiagnosticTaskTitles: Set<String> = []
  @Published public private(set) var isDiagnosticWriteInFlight = false

  public init(
    client: ElectricCircuitsClient,
    database: DatabaseQueue,
    subscription: String,
    transport: any HTTPTransport,
    mode: LinearLiteSyncMode = .fullShape,
    materializationScope: MaterializationScope? = nil,
    materializerAvailability: any MaterializerAvailabilityProbe =
      AlwaysAvailableMaterializerAvailability()
  ) {
    self.client = client
    self.database = database
    self.subscription = subscription
    self.transport = transport
    self.mode = mode
    self.materializationScope = materializationScope
    self.materializerAvailability = materializerAvailability
  }

  deinit {
    readerTask?.cancel()
  }

  /// Creates and starts the one subscribed view for this session. Calling `start` again while active is a
  /// no-op. A terminal, failed, or stopped state is published only after its old shape is released, so it
  /// is immediately safe for a caller to start a fresh session.
  public func start() async {
    guard !startInFlight, readerTask == nil, shapeHandle == nil else { return }
    startInFlight = true
    stopping = false
    lifecycleGeneration += 1
    let generation = lifecycleGeneration
    beginSyncTimeline()
    connectionState = .connecting

    if case .recentSubset(let limit, let predicate) = mode {
      await startRecentSubset(limit: limit, predicate: predicate, generation: generation)
      startInFlight = false
      return
    }

    var createdHandle: ShapeHandle?
    do {
      try Task.checkCancellation()
      let request = ShapeRequest(
        table: Self.issuesTable,
        columns: Self.issueColumns,
        changesOnly: false,
        subscription: subscription)
      let handle = try await client.createShape(request)
      createdHandle = handle
      recordSyncEvent(.shapeCreated, detail: handle.id)
      try Task.checkCancellation()
      guard !stopping, lifecycleGeneration == generation else { throw CancellationError() }

      let provider = try makeMaterializer(defaultShapeID: handle.id)
      try Task.checkCancellation()
      guard !stopping, lifecycleGeneration == generation else { throw CancellationError() }
      try await refresh(provider: provider)
      recordSyncEvent(.snapshotLoaded, detail: "\(issues.count) issue(s)")
      try Task.checkCancellation()
      guard !stopping, lifecycleGeneration == generation else { throw CancellationError() }

      let observed = SessionMaterializer(provider: provider) { [weak self] batch in
        await self?.recordLiveBatchApplied(batch)
      }
      shapeHandle = handle
      materializer = provider
      connectionState = .streaming
      recordSyncEvent(.streamStarted)
      let reader = ShapeStreamReader(
        streamURL: handle.stream.url,
        transport: transport,
        materializer: observed)
      readerTask = Task { [weak self] in
        var finalState: LinearLiteConnectionState?
        var failureDetail: String?
        do {
          try await reader.run()
        } catch is CancellationError {
          guard let self, !self.stopping else { return }
          finalState = .stopped
        } catch let error as StreamError {
          switch error {
          case .terminal(_, _, let reason): finalState = .terminal(reason)
          default:
            let detail = String(describing: error)
            failureDetail = detail
            finalState = .failed(detail)
          }
        } catch {
          let detail = String(describing: error)
          failureDetail = detail
          finalState = .failed(detail)
        }
        await self?.finishReaderRun(
          generation: generation, finalState: finalState, failureDetail: failureDetail)
      }
    } catch {
      if let shapeHandle {
        try? await client.releaseShape(shapeHandle)
        self.shapeHandle = nil
      } else if let createdHandle {
        try? await client.releaseShape(createdHandle)
      }
      if Task.isCancelled || stopping || lifecycleGeneration != generation {
        if connectionState != .idle { connectionState = .stopped }
      } else {
        recordSyncEvent(.failed, detail: String(describing: error))
        connectionState = .failed(String(describing: error))
      }
    }
    startInFlight = false
  }

  /// Starts through the normal session path, then reports the cursor actually committed by the
  /// application provider. This is the only start surface consumed by the iOS host bridge.
  public func startForHostLifecycle() async -> LinearLiteSessionHostStartReceipt {
    do {
      let provider = try materializer ?? makeMaterializer(defaultShapeID: hostPreflightShapeID)
      _ = try await provider.currentCursor()
    } catch let availability as MaterializerAvailabilityError {
      return .unavailable(availability)
    } catch {
      return .failed
    }
    await start()
    switch connectionState {
    case .streaming:
      guard let materializer else { return .failed }
      do {
        return .started(cursor: try await materializer.currentCursor())
      } catch let availability as MaterializerAvailabilityError {
        return .unavailable(availability)
      } catch {
        return .failed
      }
    case .idle, .snapshotReady, .stopped:
      return .cancelled
    case .connecting, .terminal, .failed:
      return .failed
    }
  }

  /// Owns the session for the lifetime of an async UI task. The task remains suspended while the
  /// stream reader is active, and cancellation is translated into the same stop/release path used
  /// by an explicit caller.
  public func run() async {
    await start()
    let activeReader = readerTask
    guard let activeReader else { return }
    let runGeneration = lifecycleGeneration
    await withTaskCancellationHandler(
      operation: {
        await activeReader.value
        if Task.isCancelled {
          if lifecycleGeneration == runGeneration {
            await stop()
          }
        } else {
          _ = await finishRunPreservingState(generation: runGeneration)
        }
      },
      onCancel: {
        activeReader.cancel()
      })
  }

  private func finishReaderRun(
    generation: Int,
    finalState: LinearLiteConnectionState?,
    failureDetail: String?
  ) async {
    guard await finishRunPreservingState(generation: generation), let finalState else { return }
    if let failureDetail { recordSyncEvent(.failed, detail: failureDetail) }
    connectionState = finalState
  }

  private func finishRunPreservingState(generation: Int) async -> Bool {
    guard lifecycleGeneration == generation else { return false }
    stopping = true
    lifecycleGeneration += 1
    readerTask = nil
    materializer = nil
    let releasedHandle = shapeHandle
    shapeHandle = nil
    if let releasedHandle {
      try? await client.releaseShape(releasedHandle)
    }
    return lifecycleGeneration == generation + 1
  }

  /// Cancels the reader and releases the server-owned shape. Release is idempotent in the core
  /// client, so a terminal stream and a later stop remain safe.
  public func stop() async {
    _ = await stopForHostLifecycle()
  }

  /// Joined stop/release path for host lifecycle transitions. A failed release retains its handle
  /// so a later lifecycle stop can retry; it is never reported as a successful release.
  public func stopForHostLifecycle() async -> LinearLiteSessionHostStopReceipt {
    stopping = true
    lifecycleGeneration += 1
    let task = readerTask
    readerTask = nil
    task?.cancel()
    _ = await task?.result

    if let shapeHandle {
      do {
        try await client.releaseShape(shapeHandle)
      } catch {
        connectionState = .failed("Shape release failed")
        return .failed
      }
    }
    shapeHandle = nil
    materializer = nil
    if connectionState != .idle {
      recordSyncEvent(.stopped)
      connectionState = .stopped
    }
    return .released
  }

  public func refresh() async {
    if case .recentSubset = mode {
      guard !startInFlight else { return }
      await stop()
      await start()
      return
    }
    guard let materializer else { return }
    do {
      try await refresh(provider: materializer)
    } catch {
      connectionState = .failed(String(describing: error))
    }
  }

  /// Inserts two real Postgres rows through the engine's native diagnostic row-insert endpoint.
  ///
  /// This is intentionally an example-only control, not a package write API. It uses the first
  /// currently materialized issue's project and user so the rows satisfy LinearLite's foreign-key
  /// and visibility constraints. The feed remains the source of truth for when they reach GRDB.
  public func createTwoTimestampedTasks() async {
    guard !isDiagnosticWriteInFlight else { return }
    guard let template = issues.first else {
      recordSyncEvent(.failed, detail: "Sync at least one issue before creating tasks")
      return
    }

    let startedAt = Date()
    diagnosticWriteStartedAt = startedAt
    isDiagnosticWriteInFlight = true
    let stamp = ISO8601DateFormatter().string(from: startedAt)
    let writes = (1...2).map { (title: "Swift feed task \(stamp) #\($0)", clientID: ClientID()) }
    let titles = writes.map(\.title)
    pendingDiagnosticTaskTitles = Set(titles)
    recordSyncEvent(.diagnosticTasksRequested, detail: titles.joined(separator: " · "))

    do {
      for (index, write) in writes.enumerated() {
        let timestamp = Int64(startedAt.timeIntervalSince1970 * 1_000) + Int64(index)
        try await insertDiagnosticIssue(
          title: write.title,
          clientID: write.clientID,
          description: "Inserted by the iOS sync timing control at \(write.title)",
          username: template.username,
          projectID: template.projectID,
          timestamp: timestamp)
      }
      let elapsed = max(0, Int(Date().timeIntervalSince(startedAt) * 1_000))
      recordSyncEvent(
        .diagnosticTasksCreated,
        detail: "2 tasks inserted in \(elapsed) ms; waiting for the live feed")
    } catch {
      diagnosticWriteStartedAt = nil
      pendingDiagnosticTaskTitles.removeAll()
      let detail = String(describing: error)
      recordSyncEvent(.failed, detail: "Task insert failed: \(detail)")
    }
    isDiagnosticWriteInFlight = false
  }

  private func refresh(
    provider: LinearLiteShapeMaterializer,
    order: LinearLiteShapeMaterializer.IssueOrder = .id
  ) async throws {
    issues = try await provider.allIssuesIncludingOverlays(order: order)
  }

  private func startRecentSubset(
    limit: Int, predicate: ElectricCircuitsSwift.Predicate?, generation: Int
  ) async {
    guard limit > 0 else {
      let detail = "subset limit must be positive"
      recordSyncEvent(.failed, detail: detail)
      connectionState = .failed(detail)
      return
    }
    lifecycleGeneration = generation
    stopping = false
    var createdFeed: ShapeHandle?
    do {
      try Task.checkCancellation()
      let provider = try makeMaterializer(defaultShapeID: "recent-subset-\(subscription)")
      let subsetRequest = SubsetQuery(
        table: Self.issuesTable,
        where: predicate,
        columns: Self.issueColumns,
        orderBy: SubsetOrderBy(column: "modified", descending: true),
        limit: limit)
      let feedRequest = ShapeRequest(
        table: Self.issuesTable, where: predicate, columns: Self.issueColumns, changesOnly: true,
        subscription: subscription)
      let feed = try await client.createSubsetFeed(feedRequest)
      createdFeed = feed
      recordSyncEvent(.feedCreated, detail: feed.id)
      let frontier = try await client.streamCursor(for: feed)
      let response = try await client.querySubset(subsetRequest)
      guard !stopping, lifecycleGeneration == generation else { throw CancellationError() }
      let rows = try decodeSubsetRows(response.rows)
      let initialIssues = try rows.enumerated().map { index, row -> Issue in
        do { return try Issue(changeRow: row) } catch {
          throw LinearLiteShapeMaterializerError.malformedSnapshotRow(
            index: index, detail: String(describing: error))
        }
      }
      let cursor = StreamCursor(offset: frontier.offset, lsn: response.lsn)
      try await provider.replaceSnapshot(
        rows, expecting: try await provider.currentCursor(), advancingTo: cursor)
      try Task.checkCancellation()
      guard !stopping, lifecycleGeneration == generation else { throw CancellationError() }
      materializer = provider
      try await refresh(provider: provider, order: .modifiedDescending)
      recordSyncEvent(.snapshotLoaded, detail: "\(issues.count) recent issue(s)")
      let observed = RecentSubsetSessionMaterializer(
        provider: provider, client: client, request: subsetRequest, initialIssues: initialIssues,
        snapshotLSN: response.lsn, limit: limit
      ) { [weak self] batch in
        await self?.recordLiveBatchApplied(batch)
      }
      shapeHandle = feed
      connectionState = .streaming
      recordSyncEvent(.streamStarted, detail: "Live top 10")
      let reader = ShapeStreamReader(
        streamURL: feed.stream.url, transport: transport, materializer: observed,
        startingAt: cursor)
      readerTask = Task { [weak self] in
        var finalState: LinearLiteConnectionState?
        var failureDetail: String?
        do {
          try await reader.run()
        } catch is CancellationError {
          guard let self, !self.stopping else { return }
          finalState = .stopped
        } catch let error as StreamError {
          switch error {
          case .terminal(_, _, let reason): finalState = .terminal(reason)
          default:
            let detail = String(describing: error)
            failureDetail = detail
            finalState = .failed(detail)
          }
        } catch {
          let detail = String(describing: error)
          failureDetail = detail
          finalState = .failed(detail)
        }
        await self?.finishReaderRun(
          generation: generation, finalState: finalState, failureDetail: failureDetail)
      }
    } catch {
      if let createdFeed { try? await client.releaseShape(createdFeed) }
      if Task.isCancelled || stopping {
        connectionState = .stopped
      } else if lifecycleGeneration != generation {
        return
      } else {
        let detail = String(describing: error)
        recordSyncEvent(.failed, detail: detail)
        connectionState = .failed(detail)
      }
    }
  }

  private func makeMaterializer(defaultShapeID: String) throws -> LinearLiteShapeMaterializer {
    if let materializationScope {
      return try LinearLiteShapeMaterializer(
        database: database, scope: materializationScope, availability: materializerAvailability)
    }
    return try LinearLiteShapeMaterializer(
      database: database, shapeID: defaultShapeID, availability: materializerAvailability)
  }

  private var hostPreflightShapeID: String {
    switch mode {
    case .fullShape: return "host-preflight-\(subscription)"
    case .recentSubset: return "recent-subset-\(subscription)"
    }
  }

  private func decodeSubsetRows(_ values: [JSONValue]) throws -> [ChangeRow] {
    try values.enumerated().map { index, value in
      guard case .object(let row) = value else {
        throw LinearLiteShapeMaterializerError.malformedSnapshotRow(
          index: index, detail: "subset row is not an object")
      }
      return row
    }
  }

  private func beginSyncTimeline() {
    syncStartedAt = Date()
    nextSyncEventID = 0
    syncEvents = []
    diagnosticWriteStartedAt = nil
    pendingDiagnosticTaskTitles.removeAll()
    recordSyncEvent(.syncRequested)
  }

  private func recordSyncEvent(_ kind: LinearLiteSyncEventKind, detail: String = "") {
    let elapsed = syncStartedAt.map { max(0, Int(Date().timeIntervalSince($0) * 1_000)) } ?? 0
    let event = LinearLiteSyncEvent(
      id: nextSyncEventID, kind: kind, elapsedMilliseconds: elapsed, detail: detail)
    nextSyncEventID += 1
    syncEvents.append(event)
    if syncEvents.count > 50 { syncEvents.removeFirst(syncEvents.count - 50) }
  }

  private func recordLiveBatchApplied(_ batch: ChangeBatch) async {
    guard let materializer else { return }
    do {
      let order: LinearLiteShapeMaterializer.IssueOrder =
        if case .recentSubset = mode { .modifiedDescending } else { .id }
      try await refresh(provider: materializer, order: order)
    } catch {
      let detail = String(describing: error)
      recordSyncEvent(.failed, detail: detail)
      connectionState = .failed(detail)
      return
    }
    guard connectionState == .streaming else { return }
    var detail = "\(issues.count) issue(s)"
    if case .recentSubset = mode {
      detail += " · feed \(feedSummary(batch))"
    }
    let taskTitles = Set(
      batch.envelopes.compactMap { envelope -> String? in
        guard case .string(let title) = envelope.value?["title"] else { return nil }
        return title
      })
    let observedTasks = pendingDiagnosticTaskTitles.intersection(taskTitles)
    if !observedTasks.isEmpty, let startedAt = diagnosticWriteStartedAt {
      let elapsed = max(0, Int(Date().timeIntervalSince(startedAt) * 1_000))
      detail += " · \(observedTasks.count) task(s) reached GRDB in \(elapsed) ms"
      pendingDiagnosticTaskTitles.subtract(observedTasks)
      if pendingDiagnosticTaskTitles.isEmpty {
        diagnosticWriteStartedAt = nil
      }
    }
    recordSyncEvent(.liveBatchApplied, detail: detail)
  }

  private func feedSummary(_ batch: ChangeBatch) -> String {
    let counts = Dictionary(grouping: batch.envelopes, by: \.headers.operation)
      .map { "\($0.value.count) \($0.key.rawValue)" }
      .sorted()
    return counts.isEmpty ? "empty" : counts.joined(separator: ", ")
  }

  private func insertDiagnosticIssue(
    title: String,
    clientID: ClientID,
    description: String,
    username: String,
    projectID: Int64,
    timestamp: Int64
  ) async throws {
    let values: [String: JSONValue] = [
      "client_id": .string(clientID.rawValue),
      "title": .string(title),
      "description": .string(description),
      "status": .string("todo"),
      "priority": .string("high"),
      "username": .string(username),
      "project_id": .int(projectID),
      "created": .int(timestamp),
      "modified": .int(timestamp),
      "kanbanorder": .number(Double(timestamp)),
    ]
    let body = try JSONEncoder().encode(["columns": JSONValue.object(values)])
    let url = await client.baseURL
      .appendingPathComponent("table")
      .appendingPathComponent(Self.issuesTable)
      .appendingPathComponent("rows")
    var request = URLRequest(url: url)
    request.httpMethod = "POST"
    request.setValue("application/json", forHTTPHeaderField: "Accept")
    request.setValue("application/json", forHTTPHeaderField: "Content-Type")
    request.httpBody = body
    let response = try await transport.send(request)
    guard (200..<300).contains(response.response.statusCode) else {
      throw ClientError.http(
        status: response.response.statusCode,
        message: String(data: response.data, encoding: .utf8) ?? "HTTP error")
    }
  }
}

/// Forwards atomic materialization to GRDB and asks the main-actor session to refresh its
/// published snapshot only after the provider commits rows and cursor together.
private actor SessionMaterializer: ShapeMaterializer {
  let provider: LinearLiteShapeMaterializer
  let onApplied: @MainActor @Sendable (ChangeBatch) async -> Void

  init(
    provider: LinearLiteShapeMaterializer,
    onApplied: @escaping @MainActor @Sendable (ChangeBatch) async -> Void
  ) {
    self.provider = provider
    self.onApplied = onApplied
  }

  func currentCursor() async throws -> StreamCursor? {
    try await provider.currentCursor()
  }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    try await provider.apply(batch, expecting: expectedCursor, advancingTo: cursor)
    await onApplied(batch)
  }
}

/// Materializes a strict recent page. Ordinary in-window updates are applied directly; only a
/// membership change that can affect a full page triggers a bounded query to refill the boundary.
/// The server still owns only the base-predicate feed, never top-N state.
private actor RecentSubsetSessionMaterializer: ShapeMaterializer {
  let provider: LinearLiteShapeMaterializer
  let client: ElectricCircuitsClient
  let request: SubsetQuery
  let onApplied: @MainActor @Sendable (ChangeBatch) async -> Void
  private var window: LinearLiteRecentSubsetWindow

  init(
    provider: LinearLiteShapeMaterializer,
    client: ElectricCircuitsClient,
    request: SubsetQuery,
    initialIssues: [Issue],
    snapshotLSN: String,
    limit: Int,
    onApplied: @escaping @MainActor @Sendable (ChangeBatch) async -> Void
  ) {
    self.provider = provider
    self.client = client
    self.request = request
    self.onApplied = onApplied
    var window = LinearLiteRecentSubsetWindow(limit: limit)
    window.seed(initialIssues, snapshotLSN: snapshotLSN)
    self.window = window
  }

  func currentCursor() async throws -> StreamCursor? {
    try await provider.currentCursor()
  }

  func apply(
    _ batch: ChangeBatch,
    expecting expectedCursor: StreamCursor?,
    advancingTo cursor: StreamCursor
  ) async throws {
    if batch.isEmpty {
      try await provider.apply(batch, expecting: expectedCursor, advancingTo: cursor)
      return
    }
    var candidate = window
    var directEnvelopes: [ChangeEnvelope] = []
    var needsReseed = false
    for envelope in batch.envelopes {
      switch try candidate.merge(envelope) {
      case .ignore:
        break
      case .materialize(let materialize):
        directEnvelopes.append(materialize)
      case .reseed:
        needsReseed = true
      }
    }

    if needsReseed {
      let response = try await client.querySubset(request)
      let rows = try response.rows.enumerated().map { index, value -> ChangeRow in
        guard case .object(let row) = value else {
          throw LinearLiteShapeMaterializerError.malformedSnapshotRow(
            index: index, detail: "subset row is not an object")
        }
        return row
      }
      let snapshotCursor = StreamCursor(offset: cursor.offset, lsn: response.lsn)
      try await provider.replaceSnapshot(
        rows, expecting: expectedCursor, advancingTo: snapshotCursor)
      let issues = try rows.enumerated().map { index, row -> Issue in
        do { return try Issue(changeRow: row) } catch {
          throw LinearLiteShapeMaterializerError.malformedSnapshotRow(
            index: index, detail: String(describing: error))
        }
      }
      candidate.seed(issues, snapshotLSN: response.lsn)
    } else {
      try await provider.apply(
        ChangeBatch(directEnvelopes), expecting: expectedCursor, advancingTo: cursor)
    }
    window = candidate
    await onApplied(batch)
  }
}
