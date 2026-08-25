import SwiftUI

/// A deliberately modest first screen: read-only issues grouped by status.
public struct LinearLiteIssuesView: View {
  @ObservedObject private var session: LinearLiteSession

  public init(session: LinearLiteSession) {
    self.session = session
  }

  public var body: some View {
    Group {
      switch session.connectionState {
      case .connecting:
        ProgressView("Connecting…")
      case .failed(let message):
        EmptyStateView(
          title: "Unable to load issues", systemImage: "exclamationmark.triangle", detail: message)
      case .terminal(let reason):
        EmptyStateView(
          title: "Issue stream ended", systemImage: "bolt.horizontal", detail: reason.rawValue)
      case .idle, .snapshotReady, .streaming, .stopped:
        if session.issuesByStatus.isEmpty {
          if session.connectionState == .streaming {
            ProgressView("Loading issues…")
          } else {
            EmptyStateView(title: "No issues", systemImage: "checkmark.circle")
          }
        } else {
          List {
            ForEach(session.issuesByStatus, id: \.status) { group in
              Section(group.status) {
                ForEach(group.issues, id: \.id) { issue in
                  VStack(alignment: .leading, spacing: 4) {
                    Text(issue.title).font(.headline)
                    Text("Priority \(issue.priority) · Project \(issue.projectID)")
                      .font(.subheadline)
                      .foregroundStyle(.secondary)
                  }
                }
              }
            }
          }
        }
      }
    }
    .navigationTitle("Issues")
  }
}

private struct EmptyStateView: View {
  let title: String
  let systemImage: String
  var detail: String?

  init(title: String, systemImage: String, detail: String? = nil) {
    self.title = title
    self.systemImage = systemImage
    self.detail = detail
  }

  var body: some View {
    VStack(spacing: 8) {
      Image(systemName: systemImage).font(.largeTitle).foregroundStyle(.secondary)
      Text(title).font(.headline)
      if let detail { Text(detail).font(.footnote).foregroundStyle(.secondary) }
    }
    .multilineTextAlignment(.center)
    .padding()
  }
}
