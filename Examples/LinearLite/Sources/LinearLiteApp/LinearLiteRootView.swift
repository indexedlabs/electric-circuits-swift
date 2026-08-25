import SwiftUI

private enum LinearLiteRootRoute: Hashable {
  case issues
}

/// App-level composition for the LinearLite demo. Sync is deliberately user-triggered so a fresh
/// install starts with an empty local cache and the Home screen can show each lifecycle milestone.
public struct LinearLiteRootView: View {
  private let session: LinearLiteSession
  @State private var path: [LinearLiteRootRoute] = []

  public init(session: LinearLiteSession) {
    self.session = session
  }

  public var body: some View {
    NavigationStack(path: $path) {
      LinearLiteHomeView(session: session)
        .navigationDestination(for: LinearLiteRootRoute.self) { route in
          switch route {
          case .issues:
            LinearLiteIssuesView(session: session)
          }
        }
    }
  }
}

/// Controls and compact diagnostics for the sync demo. The buttons exercise the same public
/// session lifecycle callers use; no privileged database or Postgres mutation endpoint is needed.
public struct LinearLiteHomeView: View {
  @ObservedObject private var session: LinearLiteSession

  public init(session: LinearLiteSession) {
    self.session = session
  }

  public var body: some View {
    ScrollView {
      VStack(alignment: .leading, spacing: 20) {
        Text(
          "Follow the 10 most recently modified Issues. Each live feed batch re-queries the bounded page so rows entering or leaving the top 10 refill correctly."
        )
        .font(.subheadline)
        .foregroundStyle(.secondary)

        controls
        statusCard

        NavigationLink(value: LinearLiteRootRoute.issues) {
          Label("Open Issues", systemImage: "list.bullet.rectangle")
            .frame(maxWidth: .infinity, minHeight: 44)
        }
        .buttonStyle(.borderedProminent)
        .contentShape(Rectangle())
        .accessibilityIdentifier("open-issues")

        timeline
      }
      .padding()
    }
    .navigationTitle("Sync Home")
  }

  private var statusCard: some View {
    HStack(spacing: 12) {
      Image(systemName: session.connectionState.systemImage)
        .font(.title2)
        .foregroundStyle(.tint)
        .frame(width: 32)
      VStack(alignment: .leading, spacing: 2) {
        Text("Status")
          .font(.subheadline.weight(.semibold))
          .foregroundStyle(.secondary)
        Text(statusDisplayName)
          .font(.body.weight(.semibold))
          .lineLimit(2)
      }
      Spacer(minLength: 8)
      VStack(alignment: .trailing, spacing: 2) {
        Text("\(session.issues.count)")
          .font(.title3.monospacedDigit().weight(.semibold))
        Text("issues")
          .font(.caption)
          .foregroundStyle(.secondary)
      }
    }
    .frame(maxWidth: .infinity, alignment: .leading)
    .padding(12)
    .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 14))
  }

  private var controls: some View {
    VStack(alignment: .leading, spacing: 10) {
      Text("Sync controls")
        .font(.headline)
      HStack(spacing: 10) {
        Button(session.mode == .fullShape ? "Start Sync" : "Start Recent 10") {
          Task { await session.start() }
        }
        .buttonStyle(.borderedProminent)
        .frame(maxWidth: .infinity, minHeight: 44)
        .accessibilityIdentifier("start-recent-10")
        .disabled(session.isSyncActive)

        if session.mode == .fullShape {
          Button("Stop") {
            Task { await session.stop() }
          }
          .buttonStyle(.bordered)
          .frame(maxWidth: .infinity, minHeight: 44)
          .disabled(!session.isSyncActive)
        }
      }

      Button(session.mode == .fullShape ? "Sync Again" : "Refresh Recent 10") {
        Task {
          if session.mode == .fullShape {
            await session.stop()
            await session.start()
          } else {
            await session.refresh()
          }
        }
      }
      .buttonStyle(.bordered)
      .frame(maxWidth: .infinity, minHeight: 44)
      .accessibilityIdentifier("refresh-recent-10")
      .disabled(session.connectionState == .connecting)

      Text(
        session.mode == .fullShape
          ? "Sync Again releases the current shape and creates a fresh snapshot/stream run."
          : "Refresh restarts the feed and atomically reseeds the top-10 view snapshot."
      )
      .font(.caption)
      .foregroundStyle(.secondary)

      Button {
        Task { await session.createTwoTimestampedTasks() }
      } label: {
        Label(
          session.isDiagnosticWriteInFlight
            ? "Creating 2 timestamped tasks…" : "Create 2 timestamped tasks",
          systemImage: session.isDiagnosticWriteInFlight ? "hourglass" : "plus.circle"
        )
        .frame(maxWidth: .infinity, minHeight: 44)
      }
      .buttonStyle(.borderedProminent)
      .tint(.orange)
      .accessibilityIdentifier("create-two-timestamped-tasks")
      .disabled(
        !session.isSyncActive || session.issues.isEmpty || session.isDiagnosticWriteInFlight)

      Text(
        "Inserts two Postgres tasks using the first visible project's user. The timeline shows the write acknowledgement and feed-to-GRDB latency."
      )
      .font(.caption)
      .foregroundStyle(.secondary)
    }
  }

  private var statusDisplayName: String {
    if case .recentSubset = session.mode, session.connectionState == .streaming {
      return "Streaming live top 10"
    }
    return session.connectionState.displayName
  }

  private var timeline: some View {
    VStack(alignment: .leading, spacing: 10) {
      HStack(alignment: .firstTextBaseline) {
        Text("Timing timeline")
          .font(.headline)
        Spacer()
        if !session.syncEvents.isEmpty {
          Text("\(session.syncEvents.count) events")
            .font(.caption)
            .foregroundStyle(.secondary)
        }
      }
      VStack(alignment: .leading, spacing: 0) {
        if session.syncEvents.isEmpty {
          Text(
            session.mode == .fullShape
              ? "Press Start Sync to record shape, snapshot, stream, and live-batch timings."
              : "Press Start Recent 10 to record feed creation, snapshot, and live top-10 timings."
          )
          .font(.subheadline)
          .foregroundStyle(.secondary)
          .padding(12)
        } else {
          ForEach(Array(session.syncEvents.enumerated()), id: \.element.id) { index, event in
            HStack(alignment: .top, spacing: 10) {
              Image(systemName: event.kind.systemImage)
                .foregroundStyle(event.kind.tint)
                .frame(width: 22)
              VStack(alignment: .leading, spacing: 2) {
                Text(event.kind.title)
                  .font(.subheadline.weight(.semibold))
                if !event.detail.isEmpty {
                  Text(event.detail)
                    .font(.caption)
                    .foregroundStyle(.secondary)
                }
              }
              Spacer(minLength: 8)
              Text("+\(event.elapsedMilliseconds) ms")
                .font(.caption.monospacedDigit())
                .foregroundStyle(.secondary)
            }
            .padding(.vertical, 8)
            if index < session.syncEvents.count - 1 {
              Divider().padding(.leading, 32)
            }
          }
        }
      }
      .padding(.horizontal, 12)
      .background(.thinMaterial, in: RoundedRectangle(cornerRadius: 14))
    }
  }
}

extension LinearLiteConnectionState {
  fileprivate var displayName: String {
    switch self {
    case .idle: "Idle — cache is empty"
    case .connecting: "Connecting…"
    case .snapshotReady: "Snapshot ready"
    case .streaming: "Streaming live changes"
    case .terminal(let reason): "Stream ended (\(reason.rawValue))"
    case .failed: "Failed — see timeline"
    case .stopped: "Stopped"
    }
  }

  fileprivate var systemImage: String {
    switch self {
    case .idle, .stopped: "pause.circle"
    case .snapshotReady: "checkmark.circle"
    case .connecting: "arrow.triangle.2.circlepath"
    case .streaming: "dot.radiowaves.left.and.right"
    case .terminal: "bolt.horizontal"
    case .failed: "exclamationmark.triangle"
    }
  }
}

extension LinearLiteSyncEventKind {
  fileprivate var systemImage: String {
    switch self {
    case .syncRequested: "play.circle"
    case .shapeCreated: "square.stack.3d.up"
    case .feedCreated: "dot.radiowaves.left.and.right"
    case .snapshotLoaded: "arrow.down.doc"
    case .streamStarted: "dot.radiowaves.left.and.right"
    case .liveBatchApplied: "arrow.triangle.2.circlepath"
    case .diagnosticTasksRequested: "paperplane"
    case .diagnosticTasksCreated: "checkmark.circle"
    case .stopped: "stop.circle"
    case .failed: "exclamationmark.triangle"
    }
  }

  fileprivate var tint: Color {
    switch self {
    case .failed: .red
    case .stopped: .secondary
    default: .accentColor
    }
  }
}
