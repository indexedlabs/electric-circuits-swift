import LinearLiteApp
import SwiftUI

@main
@MainActor
struct LinearLiteHostApp: App {
  private let session: LinearLiteSession
  private let lifecycleAdapter: LinearLiteHostLifecycleAdapter

  init() {
    do {
      let configuration = try LinearLiteHostConfiguration.load()
      let session = try LinearLiteHostFactory.makeSession(configuration: configuration)
      self.session = session
      let hostSession = LinearLiteSessionHostLifecycleSession(session: session)
      lifecycleAdapter = LinearLiteHostLifecycleAdapter(
        lifecycle: LinearLiteHostLifecycle(
          principal: LinearLiteHostPrincipal("linearlite-host"),
          protectedDataAvailable: UIApplication.shared.isProtectedDataAvailable,
          makeSession: { _ in hostSession },
          // This demo has one fixed, caller-owned identity. It does not expose account switching
          // or pretend it can purge an account it never selected.
          purgePrincipal: { _ in throw LinearLiteSinglePrincipalHostError.accountSwitchUnsupported }
        ))
    } catch {
      fatalError("Unable to initialize LinearLite host: \(error)")
    }
  }

  var body: some Scene {
    WindowGroup {
      LinearLiteRootView(session: session)
        .task { await lifecycleAdapter.launch() }
    }
  }
}
