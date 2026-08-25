import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteApp
import Testing

@Suite("LinearLite host configuration")
struct LinearLiteHostConfigurationTests {
  @Test @MainActor func environmentOverridesDefaultsAndFactoryUsesInjectedDatabase() throws {
    let defaults = UserDefaults(suiteName: "LinearLiteHostConfigurationTests")!
    defaults.set("https://defaults.example", forKey: LinearLiteHostConfiguration.baseURLKey)
    defer { defaults.removePersistentDomain(forName: "LinearLiteHostConfigurationTests") }

    let configuration = try LinearLiteHostConfiguration.load(
      environment: [LinearLiteHostConfiguration.baseURLKey: "https://environment.example/base"],
      userDefaults: defaults)
    #expect(configuration.baseURL == URL(string: "https://environment.example/base"))

    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("linearlite-host-test-\(UUID().uuidString).sqlite")
    defer { try? FileManager.default.removeItem(at: databaseURL) }
    let session = try LinearLiteHostFactory.makeSession(
      configuration: configuration,
      databaseURL: databaseURL,
      urlSession: URLSession(configuration: .ephemeral),
      headers: ["X-Test": "host"],
      cookieHeader: "session=test")

    #expect(session.subscription == LinearLiteHostConfiguration.defaultSubscription)
    #expect(session.mode == .recentSubset(limit: 10))
    #expect(session.database.path == databaseURL.path)
    let transport = try #require(session.transport as? URLSessionTransport)
    #expect(transport.headers == ["X-Test": "host"])
    #expect(transport.cookieHeader == "session=test")
  }

  @Test func invalidURLIsRejectedWithoutNetwork() {
    #expect(throws: LinearLiteHostConfigurationError.invalidBaseURL("file:///tmp/linear-lite")) {
      try LinearLiteHostConfiguration(baseURL: URL(string: "file:///tmp/linear-lite")!)
    }
  }

  @Test func invalidConfiguredURLFailsClosed() {
    #expect(throws: LinearLiteHostConfigurationError.invalidBaseURL("file:///tmp/linear-lite")) {
      try LinearLiteHostConfiguration.load(
        environment: [LinearLiteHostConfiguration.baseURLKey: "file:///tmp/linear-lite"])
    }
  }

  @Test @MainActor func durableHostDatabaseUsesWALAndForeignKeyPolicy() throws {
    let databaseURL = FileManager.default.temporaryDirectory
      .appendingPathComponent("linearlite-host-policy-\(UUID().uuidString).sqlite")
    defer {
      for suffix in ["", "-wal", "-shm"] {
        try? FileManager.default.removeItem(atPath: databaseURL.path + suffix)
      }
    }

    _ = try LinearLiteHostFactory.makeSession(
      configuration: LinearLiteHostConfiguration(), databaseURL: databaseURL,
      urlSession: URLSession(configuration: .ephemeral))
    let inspection = try DatabaseQueue(path: databaseURL.path)
    let policy = try inspection.read { db in
      (
        try String.fetchOne(db, sql: "PRAGMA journal_mode"),
        try Bool.fetchOne(db, sql: "PRAGMA foreign_keys")
      )
    }
    #expect(policy.0 == "wal")
    #expect(policy.1 == true)
  }
}
