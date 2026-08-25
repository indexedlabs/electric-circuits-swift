import LinearLiteApp
import UIKit

enum LinearLiteSinglePrincipalHostError: Error {
  case accountSwitchUnsupported
}

/// iOS-only notification bridge. The reusable lifecycle state machine remains Foundation-only in
/// `LinearLiteApp`, where deterministic tests can inject events without UIApplication or scenes.
@MainActor
final class LinearLiteHostLifecycleAdapter {
  private let lifecycle: LinearLiteHostLifecycle
  private let notificationCenter: NotificationCenter
  private var observers: [NSObjectProtocol] = []

  init(
    lifecycle: LinearLiteHostLifecycle,
    notificationCenter: NotificationCenter = .default
  ) {
    self.lifecycle = lifecycle
    self.notificationCenter = notificationCenter
    observe()
  }

  /// Call during an explicit host teardown. App lifetime owns this adapter in the example host,
  /// so deinitialization does not cross UIKit's non-Sendable observer boundary.
  func invalidate() {
    for observer in observers { notificationCenter.removeObserver(observer) }
    observers.removeAll()
  }

  func launch() async {
    _ = await lifecycle.launch()
  }

  private func observe() {
    observers = [
      notificationCenter.addObserver(
        forName: UIApplication.protectedDataDidBecomeAvailableNotification, object: nil,
        queue: .main
      ) { [weak lifecycle] _ in
        Task { @MainActor in _ = await lifecycle?.protectedDataDidBecomeAvailable() }
      },
      notificationCenter.addObserver(
        forName: UIApplication.protectedDataWillBecomeUnavailableNotification, object: nil,
        queue: .main
      ) { [weak lifecycle] _ in
        Task { @MainActor in _ = await lifecycle?.protectedDataWillBecomeUnavailable() }
      },
      notificationCenter.addObserver(
        forName: UIScene.didActivateNotification, object: nil, queue: .main
      ) {
        [weak lifecycle] _ in
        Task { @MainActor in _ = await lifecycle?.sceneDidBecomeActive() }
      },
      notificationCenter.addObserver(
        forName: UIScene.willDeactivateNotification, object: nil, queue: .main
      ) { [weak lifecycle] _ in
        Task { @MainActor in _ = await lifecycle?.sceneDidBecomeInactive() }
      },
    ]
  }
}

@MainActor
final class LinearLiteSessionHostLifecycleSession: LinearLiteHostLifecycleSession {
  private let session: LinearLiteSession

  init(session: LinearLiteSession) {
    self.session = session
  }

  func start() async -> LinearLiteHostStartReceipt {
    switch await session.startForHostLifecycle() {
    case .started(let cursor): return .started(cursor: cursor)
    case .unavailable(let availability): return .unavailable(availability)
    case .failed: return .failed
    case .cancelled: return .cancelled
    }
  }

  func stop() async -> LinearLiteHostReleaseReceipt {
    switch await session.stopForHostLifecycle() {
    case .released: return .released(name: "linearlite-session")
    case .failed: return .failed
    }
  }
}
