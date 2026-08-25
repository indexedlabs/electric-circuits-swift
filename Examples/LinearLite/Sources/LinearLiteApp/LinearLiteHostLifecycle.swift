import ElectricCircuitsSwift
import Foundation

/// Redacted account identity used only to select a locally isolated materialization scope.
public struct LinearLiteHostPrincipal: Equatable, Hashable, Sendable {
  public let value: String

  public init(_ value: String) {
    precondition(!value.isEmpty)
    self.value = value
  }
}

/// The only start results a host needs to render or retry. It intentionally contains no provider,
/// row, cookie, header, or credential diagnostics.
public enum LinearLiteHostStartReceipt: Equatable, Sendable {
  case started(cursor: StreamCursor?)
  case unavailable(MaterializerAvailabilityError)
  case failed
  case cancelled
}

/// A named completion of the release path. Hosts may log the name for lifecycle audit without
/// exposing a shape identifier or any account data.
public enum LinearLiteHostReleaseReceipt: Equatable, Sendable {
  case released(name: String)
  case failed
}

/// The Foundation-only adapter boundary for an application-owned shape/coordinator lifecycle.
/// Authentication continues to belong to the injected transport beneath this seam.
@MainActor
public protocol LinearLiteHostLifecycleSession: AnyObject, Sendable {
  func start() async -> LinearLiteHostStartReceipt
  func stop() async -> LinearLiteHostReleaseReceipt
}

/// Public result of one lifecycle input. These are deliberately coarse and safe for UI state.
public enum LinearLiteHostLifecycleReceipt: Equatable, Sendable {
  case protectedDataUnavailable
  case started(principal: LinearLiteHostPrincipal, cursor: StreamCursor?)
  case failed(principal: LinearLiteHostPrincipal)
  case released(principal: LinearLiteHostPrincipal, name: String)
  case releaseFailed(principal: LinearLiteHostPrincipal)
  case purgeFailed(principal: LinearLiteHostPrincipal)
  case accountSwitched(from: LinearLiteHostPrincipal, to: LinearLiteHostPrincipal)
  case accountSwitchFailed(from: LinearLiteHostPrincipal, to: LinearLiteHostPrincipal)
  case ignored
}

/// Testable host state machine. UIKit/SwiftUI notification registration belongs in the iOS host;
/// this type receives deterministic lifecycle inputs and never owns credentials or storage.
@MainActor
public final class LinearLiteHostLifecycle {
  public typealias SessionFactory =
    @MainActor @Sendable (LinearLiteHostPrincipal) -> any LinearLiteHostLifecycleSession
  public typealias PrincipalPurger =
    @MainActor @Sendable (LinearLiteHostPrincipal) async throws -> Void

  private let makeSession: SessionFactory
  private let purgePrincipal: PrincipalPurger
  private var principal: LinearLiteHostPrincipal
  private var protectedDataAvailable: Bool
  private var session: (any LinearLiteHostLifecycleSession)?
  private var generation: UInt64 = 0
  private var startTask: Task<LinearLiteHostStartReceipt, Never>?
  private var stopTask: Task<LinearLiteHostReleaseReceipt, Never>?
  private var stopGeneration: UInt64?
  private var accountTransitionActive = false
  private var accountTransitionWaiters: [CheckedContinuation<Void, Never>] = []

  public init(
    principal: LinearLiteHostPrincipal,
    protectedDataAvailable: Bool,
    makeSession: @escaping SessionFactory,
    purgePrincipal: @escaping PrincipalPurger
  ) {
    self.principal = principal
    self.protectedDataAvailable = protectedDataAvailable
    self.makeSession = makeSession
    self.purgePrincipal = purgePrincipal
  }

  public func launch() async -> LinearLiteHostLifecycleReceipt {
    await startIfPermitted()
  }

  /// The UIKit protected-data-available notification retries the same durable session identity.
  public func protectedDataDidBecomeAvailable() async -> LinearLiteHostLifecycleReceipt {
    protectedDataAvailable = true
    return await startIfPermitted()
  }

  /// A host may report the complementary notification before it stops its scene.
  public func protectedDataWillBecomeUnavailable() async -> LinearLiteHostLifecycleReceipt {
    protectedDataAvailable = false
    return await stopCurrentSession()
  }

  public func sceneDidBecomeActive() async -> LinearLiteHostLifecycleReceipt {
    await startIfPermitted()
  }

  public func sceneDidBecomeInactive() async -> LinearLiteHostLifecycleReceipt {
    await stopCurrentSession()
  }

  /// Logout never adopts another identity implicitly. It releases the old claim before purging
  /// every old-principal local artifact, then leaves the bridge idle for caller-owned auth.
  public func logout() async -> LinearLiteHostLifecycleReceipt {
    guard await acquireAccountTransition() else { return .purgeFailed(principal: principal) }
    defer { releaseAccountTransition() }
    let old = principal
    let release = await stopCurrentSession()
    guard case .released = release else { return release }
    do {
      try Task.checkCancellation()
      try await purgePrincipal(old)
    } catch {
      return .purgeFailed(principal: old)
    }
    return release
  }

  public func switchAccount(to next: LinearLiteHostPrincipal) async
    -> LinearLiteHostLifecycleReceipt
  {
    guard await acquireAccountTransition() else {
      return .accountSwitchFailed(from: principal, to: next)
    }
    defer { releaseAccountTransition() }
    guard next != principal else { return .ignored }
    let old = principal
    let release = await stopCurrentSession()
    guard case .released = release else { return release }
    do {
      try Task.checkCancellation()
      try await purgePrincipal(old)
      try Task.checkCancellation()
    } catch {
      return .accountSwitchFailed(from: old, to: next)
    }
    principal = next
    session = nil
    if case .started = await startIfPermitted(allowAccountTransition: true) {
      return .accountSwitched(from: old, to: next)
    }
    return .accountSwitchFailed(from: old, to: next)
  }

  private func startIfPermitted(
    allowAccountTransition: Bool = false
  ) async -> LinearLiteHostLifecycleReceipt {
    guard !accountTransitionActive || allowAccountTransition else { return .ignored }
    guard protectedDataAvailable else { return .protectedDataUnavailable }
    if let stopTask {
      let release = await stopTask.value
      settleStopIfNeeded(release)
    }
    guard protectedDataAvailable else { return .protectedDataUnavailable }
    if let startTask {
      let joinedTask = startTask
      let joinedGeneration = generation
      return receipt(from: await joinedTask.value, generation: joinedGeneration)
    }

    let activePrincipal = principal
    let activeSession = session ?? makeSession(activePrincipal)
    session = activeSession
    generation &+= 1
    let activeGeneration = generation
    let task = Task { @MainActor [activeSession] in await activeSession.start() }
    startTask = task
    let result = await task.value
    guard generation == activeGeneration else { return .ignored }
    startTask = nil
    return receipt(from: result, generation: activeGeneration)
  }

  private func receipt(
    from result: LinearLiteHostStartReceipt, generation expectedGeneration: UInt64
  ) -> LinearLiteHostLifecycleReceipt {
    guard generation == expectedGeneration else { return .ignored }
    switch result {
    case .started(let cursor): return .started(principal: principal, cursor: cursor)
    case .unavailable(.protectedDataUnavailable): return .protectedDataUnavailable
    case .unavailable, .cancelled: return .ignored
    case .failed: return .failed(principal: principal)
    }
  }

  private func stopCurrentSession() async -> LinearLiteHostLifecycleReceipt {
    if let stopTask {
      return releaseReceipt(await stopTask.value, principal: principal)
    }
    guard let activeSession = session else { return .ignored }
    generation &+= 1
    let thisStopGeneration = generation
    let starting = startTask
    starting?.cancel()
    let task = Task { @MainActor [activeSession, starting] in
      _ = await starting?.value
      return await activeSession.stop()
    }
    stopTask = task
    stopGeneration = thisStopGeneration
    let release = await task.value
    if stopGeneration == thisStopGeneration { settleStopIfNeeded(release) }
    return releaseReceipt(release, principal: principal)
  }

  private func settleStopIfNeeded(_ release: LinearLiteHostReleaseReceipt) {
    guard stopTask != nil else { return }
    stopTask = nil
    stopGeneration = nil
    startTask = nil
    if case .released = release { session = nil }
  }

  private func releaseReceipt(
    _ release: LinearLiteHostReleaseReceipt, principal: LinearLiteHostPrincipal
  ) -> LinearLiteHostLifecycleReceipt {
    switch release {
    case .released(let name): return .released(principal: principal, name: name)
    case .failed: return .releaseFailed(principal: principal)
    }
  }

  private func acquireAccountTransition() async -> Bool {
    while accountTransitionActive {
      await withCheckedContinuation { accountTransitionWaiters.append($0) }
    }
    guard !Task.isCancelled else { return false }
    accountTransitionActive = true
    return true
  }

  private func releaseAccountTransition() {
    accountTransitionActive = false
    let waiters = accountTransitionWaiters
    accountTransitionWaiters.removeAll()
    for waiter in waiters { waiter.resume() }
  }
}
