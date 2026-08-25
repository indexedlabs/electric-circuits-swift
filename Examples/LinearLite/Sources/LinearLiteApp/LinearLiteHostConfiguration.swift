import ElectricCircuitsSwift
import Foundation
import GRDB
import LinearLiteGRDB

/// Configuration and construction helpers used by the runnable LinearLite iOS host.
///
/// The host deliberately has no authentication policy. Cookies, headers, and any credential
/// refresh remain owned by the injected `HTTPTransport`/server boundary.
public struct LinearLiteHostConfiguration: Equatable, Sendable {
  public static let baseURLKey = "ELECTRIC_CIRCUITS_BASE_URL"
  public static let defaultBaseURL = URL(string: "http://127.0.0.1:3000")!
  public static let defaultSubscription = "linearlite-ios"

  public let baseURL: URL
  public let subscription: String

  public init(
    baseURL: URL = Self.defaultBaseURL,
    subscription: String = Self.defaultSubscription
  ) throws {
    guard let scheme = baseURL.scheme?.lowercased(), ["http", "https"].contains(scheme),
      baseURL.host != nil, baseURL.query == nil, baseURL.fragment == nil
    else {
      throw LinearLiteHostConfigurationError.invalidBaseURL(baseURL.absoluteString)
    }
    guard !subscription.isEmpty else {
      throw LinearLiteHostConfigurationError.emptySubscription
    }
    self.baseURL = baseURL
    self.subscription = subscription
  }

  /// Loads the base URL from the process environment first, then UserDefaults, and finally the
  /// loopback development default. The environment key is intentionally the same string as the
  /// UserDefaults key so `-ELECTRIC_CIRCUITS_BASE_URL https://...` also works in a scheme.
  public static func load(
    environment: [String: String] = ProcessInfo.processInfo.environment,
    userDefaults: UserDefaults = .standard
  ) throws -> Self {
    let configured = environment[baseURLKey] ?? userDefaults.string(forKey: baseURLKey)
    guard let configured else { return try Self(baseURL: defaultBaseURL) }
    let value = configured.trimmingCharacters(in: .whitespacesAndNewlines)
    guard !value.isEmpty else { return try Self(baseURL: defaultBaseURL) }
    guard let url = URL(string: value) else {
      throw LinearLiteHostConfigurationError.invalidBaseURL(value)
    }
    return try Self(baseURL: url)
  }
}

public enum LinearLiteHostConfigurationError: Error, Equatable, Sendable {
  case invalidBaseURL(String)
  case emptySubscription
}

/// Builds the one database/session pair owned by the host application.
public enum LinearLiteHostFactory {
  public static let databaseFileName = "linearlite.sqlite"

  public static func appSupportDatabaseURL(fileManager: FileManager = .default) throws -> URL {
    let support = try fileManager.url(
      for: .applicationSupportDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
    let directory = support.appendingPathComponent("LinearLite", isDirectory: true)
    try fileManager.createDirectory(at: directory, withIntermediateDirectories: true)
    return directory.appendingPathComponent(databaseFileName)
  }

  @MainActor
  public static func makeSession(
    configuration: LinearLiteHostConfiguration,
    databaseURL: URL? = nil,
    fileManager: FileManager = .default,
    urlSession: URLSession = .shared,
    headers: [String: String] = [:],
    cookieHeader: String? = nil
  ) throws -> LinearLiteSession {
    let url = try databaseURL ?? appSupportDatabaseURL(fileManager: fileManager)
    let database = try DatabaseQueue(
      path: url.path, configuration: LinearLiteShapeMaterializer.databaseConfiguration)
    let transport = URLSessionTransport(
      session: urlSession, headers: headers, cookieHeader: cookieHeader)
    let client = ElectricCircuitsClient(baseURL: configuration.baseURL, transport: transport)
    return LinearLiteSession(
      client: client,
      database: database,
      subscription: configuration.subscription,
      transport: transport,
      mode: .recentSubset(limit: 10))
  }
}
