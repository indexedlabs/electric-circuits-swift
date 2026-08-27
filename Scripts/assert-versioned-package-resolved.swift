#!/usr/bin/env swift
import Foundation

private struct PackageResolved: Decodable {
  let pins: [Pin]

  struct Pin: Decodable {
    let identity: String
    let location: String
    let state: State
  }

  struct State: Decodable {
    let revision: String
    let version: String?
  }
}

private enum ResolvedPinError: Error, CustomStringConvertible {
  case invalidSourceLocation(String)
  case missingPin(identity: String)
  case duplicatePins(identity: String, count: Int)
  case wrongLocation(expected: String, actual: String)
  case wrongVersion(expected: String, actual: String?)
  case wrongRevision(expected: String, actual: String)

  var description: String {
    switch self {
    case .invalidSourceLocation(let location):
      return "versioned-host: source location is not a local file URL or path: \(location)"
    case .missingPin(let identity):
      return "versioned-host: Package.resolved has no pin with identity \(identity)"
    case .duplicatePins(let identity, let count):
      return "versioned-host: Package.resolved has \(count) pins with identity \(identity)"
    case .wrongLocation(let expected, let actual):
      return "versioned-host: pin location mismatch (expected \(expected), got \(actual))"
    case .wrongVersion(let expected, let actual):
      return "versioned-host: pin version mismatch (expected \(expected), got \(actual ?? "nil"))"
    case .wrongRevision(let expected, let actual):
      return "versioned-host: pin revision mismatch (expected \(expected), got \(actual))"
    }
  }
}

private func canonicalSourceLocation(_ location: String) throws -> String {
  let path: String
  if location.hasPrefix("file://") {
    guard let url = URL(string: location), url.isFileURL else {
      throw ResolvedPinError.invalidSourceLocation(location)
    }
    path = url.path
  } else if location.hasPrefix("/") {
    path = location
  } else {
    throw ResolvedPinError.invalidSourceLocation(location)
  }

  return URL(fileURLWithPath: path).standardizedFileURL.resolvingSymlinksInPath().path
}

private func assertPin(
  _ package: PackageResolved,
  identity: String,
  location: String,
  version: String,
  revision: String
) throws {
  let pins = package.pins.filter { $0.identity == identity }
  guard let pin = pins.first else { throw ResolvedPinError.missingPin(identity: identity) }
  guard pins.count == 1 else {
    throw ResolvedPinError.duplicatePins(identity: identity, count: pins.count)
  }

  let expectedLocation = try canonicalSourceLocation(location)
  let actualLocation = try canonicalSourceLocation(pin.location)
  guard actualLocation == expectedLocation else {
    throw ResolvedPinError.wrongLocation(expected: expectedLocation, actual: actualLocation)
  }
  guard pin.state.version == version else {
    throw ResolvedPinError.wrongVersion(expected: version, actual: pin.state.version)
  }
  guard pin.state.revision == revision else {
    throw ResolvedPinError.wrongRevision(expected: revision, actual: pin.state.revision)
  }
}

private func selfTest() throws {
  let identity = "electric-circuits-swift"
  let location = "/tmp/electric-circuits-swift-release"
  let version = "0.2.1"
  let revision = "0123456789abcdef0123456789abcdef01234567"
  let valid = PackageResolved(
    pins: [.init(identity: identity, location: location, state: .init(revision: revision, version: version))])
  try assertPin(valid, identity: identity, location: location, version: version, revision: revision)

  let invalidPins = [
    PackageResolved.Pin(identity: "another-package", location: location, state: .init(revision: revision, version: version)),
    .init(identity: identity, location: "/tmp/other-release", state: .init(revision: revision, version: version)),
    .init(identity: identity, location: location, state: .init(revision: revision, version: "0.1.1")),
    .init(identity: identity, location: location, state: .init(revision: "different-revision", version: version)),
  ]
  for pin in invalidPins {
    do {
      try assertPin(.init(pins: [pin]), identity: identity, location: location, version: version, revision: revision)
      throw NSError(domain: "versioned-host", code: 1, userInfo: [
        NSLocalizedDescriptionKey: "versioned-host: resolved-pin self-test accepted an invalid pin",
      ])
    } catch is ResolvedPinError {
      continue
    }
  }
}

do {
  if CommandLine.arguments.dropFirst().elementsEqual(["--self-test"]) {
    try selfTest()
    print("versioned-host: resolved-pin parser self-test passed")
  } else {
    guard CommandLine.arguments.count == 6 else {
      throw NSError(domain: "versioned-host", code: 64, userInfo: [
        NSLocalizedDescriptionKey: "usage: assert-versioned-package-resolved.swift <Package.resolved> <identity> <location> <version> <revision>",
      ])
    }
    let arguments = CommandLine.arguments
    let data = try Data(contentsOf: URL(fileURLWithPath: arguments[1]))
    let package = try JSONDecoder().decode(PackageResolved.self, from: data)
    try assertPin(package, identity: arguments[2], location: arguments[3], version: arguments[4], revision: arguments[5])
    print("versioned-host: Package.resolved pin is exact for \(arguments[2])")
  }
} catch {
  fputs("\(error)\n", stderr)
  exit(1)
}
