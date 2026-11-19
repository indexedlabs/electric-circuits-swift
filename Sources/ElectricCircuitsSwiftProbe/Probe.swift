import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

/// Drives the SDK's real collection stack against an engine URL and reports what it does.
///
/// A single-table scope is a `CollectionCoordinator` over `CircuitsSubsetSource`; a group scope
/// is `CircuitsSubsetGroupSource` driven the way its documentation describes. Both write to one
/// file-backed store, so a new process started on the same `--store` picks each scope up from
/// what the last process committed. Nothing here re-implements the protocol: the probe only
/// builds the SDK's own types, counts their requests and prints their state.
///
/// ```
/// swift run ElectricCircuitsSwiftProbe --engine http://127.0.0.1:3000 --store /tmp/probe.json \
///   --scope '{"name":"mine","table":"items","key":"id","where":{"col":"owner","op":"eq","value":"a"}}' \
///   --scope '{"name":"pair","group":[{"table":"items","key":"id"},{"table":"notes","key":"id"}]}'
/// ```
///
/// Options: `--generation <text>` (default: a new one per process, as an app that names its
/// generation from a launch-scoped session does), `--principal <text>`, `--live long-poll|sse`
/// (default: `ECS_SWIFT_PROBE_LIVE`, else long-poll), `--stream-origin <url>` (send stream
/// requests to this origin instead of the one the engine returned), `--scopes-file <path>` (a
/// JSON array of scopes).
///
/// Commands, one JSON object per line on stdin: `{"cmd":"rows"}`, `{"cmd":"counters"}`,
/// `{"cmd":"position"}`, `{"cmd":"acquire","scope":…}`, `{"cmd":"refresh","scope":…}`,
/// `{"cmd":"release","scope":…}`, `{"cmd":"sync","id":…}` and `{"cmd":"stop","release":true}`.
/// `acquire` and `refresh` take `"mode":"snapshot"` to read the source again instead of
/// continuing from the stored cursor (`CollectionLoadMode`); the default is `"resume"`.
/// `stop` with `"release":false` exits without releasing any claim, as a killed app does. End
/// of input and `SIGTERM` stop with a release.
///
/// Events, one JSON object per line on stdout: `started`, `state`, `path` (snapshot, resume or
/// resume-declined), `snapshot`, `change`, `batch`, `caught-up`, `failure`, `request` (every
/// network request, with a stream read's envelopes as the engine sent them), `rows`, `counters`,
/// `position`, `sync` and `stopped`.
@main
struct ElectricCircuitsSwiftProbe {
  static func main() async {
    let output = ProbeOutput()
    do {
      let configuration = try ProbeConfiguration(
        arguments: Array(CommandLine.arguments.dropFirst()))
      let probe = try await Probe(configuration: configuration, output: output)
      // A terminating signal stops the probe the way end of input does, releasing its claims.
      signal(SIGTERM, SIG_IGN)
      let terminate = DispatchSource.makeSignalSource(signal: SIGTERM, queue: .global())
      terminate.setEventHandler { Task { await probe.stop(release: true) } }
      terminate.resume()
      await probe.start()
      for try await line in FileHandle.standardInput.bytes.lines {
        guard let data = line.data(using: .utf8),
          let command = try? JSONDecoder().decode(ProbeCommand.self, from: data)
        else {
          output.emit("error", ["message": .string("unreadable command"), "line": .string(line)])
          continue
        }
        await probe.handle(command)
      }
      await probe.stop(release: true)
    } catch {
      output.emit("error", ["message": .string(String(describing: error))])
      FileHandle.standardError.write(Data("probe: \(error)\n".utf8))
      exit(64)
    }
  }
}

enum ProbeUsageError: Error, Equatable {
  case missing(String)
  case invalid(String)
}

struct ProbeConfiguration: Sendable {
  var engine: URL
  var store: URL
  var generation = UUID().uuidString.lowercased()
  var principal = "probe"
  var live = LiveReadMode.longPoll
  var streamOrigin: URL?
  var scopes: [ProbeScopeSpec] = []

  init(arguments: [String]) throws {
    var engine: URL?
    var store: URL?
    var index = 0
    func value(_ name: String) throws -> String {
      index += 1
      guard index < arguments.count else { throw ProbeUsageError.missing(name) }
      return arguments[index]
    }
    var generation: String?
    var principal: String?
    var live =
      ProcessInfo.processInfo.environment["ECS_SWIFT_PROBE_LIVE"].flatMap(LiveReadMode.init)
      ?? .longPoll
    var streamOrigin: URL?
    var scopes: [ProbeScopeSpec] = []
    while index < arguments.count {
      let name = arguments[index]
      switch name {
      case "--engine":
        engine = URL(string: try value(name))
      case "--store":
        store = URL(fileURLWithPath: try value(name))
      case "--generation":
        generation = try value(name)
      case "--principal":
        principal = try value(name)
      case "--live":
        guard let mode = LiveReadMode(rawValue: try value(name)) else {
          throw ProbeUsageError.invalid(name)
        }
        live = mode
      case "--stream-origin":
        streamOrigin = URL(string: try value(name))
      case "--scope":
        scopes.append(
          try JSONDecoder().decode(ProbeScopeSpec.self, from: Data(try value(name).utf8)))
      case "--scopes-file":
        scopes += try JSONDecoder().decode(
          [ProbeScopeSpec].self, from: Data(contentsOf: URL(fileURLWithPath: try value(name))))
      default:
        throw ProbeUsageError.invalid(name)
      }
      index += 1
    }
    guard let engine else { throw ProbeUsageError.missing("--engine") }
    guard let store else { throw ProbeUsageError.missing("--store") }
    guard !scopes.isEmpty else { throw ProbeUsageError.missing("--scope") }
    guard Set(scopes.map(\.name)).count == scopes.count else {
      throw ProbeUsageError.invalid("--scope names must be unique")
    }
    for scope in scopes where scope.single == nil && scope.group == nil {
      throw ProbeUsageError.invalid("scope '\(scope.name)' needs table and key, or group")
    }
    self.engine = engine
    self.store = store
    if let generation { self.generation = generation }
    if let principal { self.principal = principal }
    self.live = live
    self.streamOrigin = streamOrigin
    self.scopes = scopes
  }
}

struct ProbeCommand: Decodable, Sendable {
  let cmd: String
  let scope: String?
  let id: String?
  let release: Bool?
  let mode: String?

  /// The caller's policy for `acquire` and `refresh`; nil when `mode` names neither.
  var loadMode: CollectionLoadMode? {
    switch mode {
    case nil, "resume": .resume
    case "snapshot": .snapshot
    default: nil
    }
  }
}

actor Probe {
  private let configuration: ProbeConfiguration
  private let output: ProbeOutput
  private let log = ProbeRequestLog()
  private let store: ProbeStore
  private let scope: CollectionScope
  private var singles: [String: ProbeSingleScope] = [:]
  private var groups: [String: ProbeGroupScope] = [:]
  private var stopping = false

  init(configuration: ProbeConfiguration, output: ProbeOutput) async throws {
    self.configuration = configuration
    self.output = output
    scope = CollectionScope(
      principal: configuration.principal, authorization: "probe-v1",
      generation: configuration.generation)
    var keyColumns: [String: String] = [:]
    var groupNames: [String: String] = [:]
    for spec in configuration.scopes {
      if let single = spec.single {
        keyColumns[spec.name] = single.key
      } else {
        groupNames[ProbeGroupScope.materializationID(for: spec.name).rawValue] = spec.name
      }
    }
    store = try ProbeStore(
      url: configuration.store, keyColumns: keyColumns, groupNames: groupNames, output: output)
    for spec in configuration.scopes {
      let transport = ProbeTransport(
        base: URLSessionTransport(), scope: spec.name, log: log, output: output,
        streamOrigin: configuration.streamOrigin)
      let live = configuration.live
      let client = ElectricCircuitsClient(
        baseURL: configuration.engine, transport: transport,
        configuration: .init(liveReadMode: live))
      if let single = spec.single {
        singles[spec.name] = ProbeSingleScope(
          name: spec.name, member: single, client: client, transport: transport, scope: scope,
          store: store, output: output)
      } else if let members = spec.group {
        groups[spec.name] = ProbeGroupScope(
          spec: spec, members: members, client: client, transport: transport, scope: scope,
          store: store, output: output)
      }
    }
  }

  func start() async {
    var stored: [String: JSONValue] = [:]
    for (name, single) in singles { stored[name] = .object(await single.position()) }
    for (name, group) in groups { stored[name] = .object(await group.position()) }
    output.emit(
      "started",
      [
        "pid": .int(Int64(ProcessInfo.processInfo.processIdentifier)),
        "engine": .string(configuration.engine.absoluteString),
        "generation": .string(configuration.generation),
        "live": .string(configuration.live.rawValue),
        "scopes": .array(configuration.scopes.map { .string($0.name) }),
        "stored": .object(stored),
      ])
    for spec in configuration.scopes {
      await singles[spec.name]?.acquire()
      await groups[spec.name]?.acquire()
    }
  }

  func handle(_ command: ProbeCommand) async {
    guard !stopping else { return }
    let names = command.scope.map { [$0] } ?? configuration.scopes.map(\.name)
    let id: [String: JSONValue] = command.id.map { ["id": .string($0)] } ?? [:]
    switch command.cmd {
    case "rows":
      for name in names {
        if let single = singles[name] {
          output.emit(
            "rows", id.merging(["scope": .string(name), "rows": await single.rows()]) { $1 })
        } else if let group = groups[name] {
          output.emit(
            "rows", id.merging(["scope": .string(name), "tables": await group.rows()]) { $1 })
        }
      }
    case "counters":
      await emitCounters(id)
    case "position":
      for name in names {
        var position: [String: JSONValue] = [:]
        if let single = singles[name] {
          position = await single.position()
        } else if let group = groups[name] {
          position = await group.position()
        }
        output.emit(
          "position", id.merging(position) { $1 }.merging(["scope": .string(name)]) { $1 })
      }
    case "acquire", "refresh":
      guard let mode = command.loadMode else {
        output.emit("error", ["message": .string("unknown mode '\(command.mode ?? "")'")])
        return
      }
      for name in names {
        if command.cmd == "acquire" {
          await singles[name]?.acquire(mode)
          await groups[name]?.acquire(mode)
        } else {
          await singles[name]?.refresh(mode)
          await groups[name]?.refresh(mode)
        }
      }
    case "release":
      for name in names {
        await singles[name]?.release()
        await groups[name]?.release()
      }
    case "sync":
      output.emit("sync", id)
    case "stop":
      await stop(release: command.release ?? true)
    default:
      output.emit("error", ["message": .string("unknown command '\(command.cmd)'")])
    }
  }

  private func emitCounters(_ extra: [String: JSONValue] = [:]) async {
    let counters = await log.counters().mapValues { counts in
      JSONValue.object(counts.mapValues { .int(Int64($0)) })
    }
    output.emit("counters", extra.merging(["counters": .object(counters)]) { $1 })
  }

  /// Ends the process. With `release`, every lease and group session is released first, which
  /// sends the SDK's claim releases; without it the process exits holding its claims.
  func stop(release: Bool) async {
    guard !stopping else { return }
    stopping = true
    if release {
      for single in singles.values { await single.release() }
      for group in groups.values { await group.release() }
    }
    await emitCounters()
    output.emit("stopped", ["released": .bool(release)])
    exit(0)
  }
}

/// One single-table scope: the SDK's coordinator, source and one lease.
actor ProbeSingleScope {
  private let name: String
  private let output: ProbeOutput
  private let store: ProbeStore
  private let demand: CollectionDemand<ChangeRow>
  private let identity: CollectionDemandIdentity
  private let coordinator: CollectionCoordinator<ChangeRow, String, ProbeSource, ProbeStore>
  private var lease: CollectionLease?

  init(
    name: String, member: ProbeMemberSpec, client: ElectricCircuitsClient,
    transport: ProbeTransport, scope: CollectionScope, store: ProbeStore, output: ProbeOutput
  ) {
    self.name = name
    self.output = output
    self.store = store
    let keyColumn = member.key
    let definition = CollectionDefinition<ChangeRow, String>(
      id: CollectionID(rawValue: name), key: { ProbeJSON.key($0[keyColumn]) })
    let source = ProbeSource(
      base: CircuitsSubsetSource<ChangeRow, String>(
        client: client, transport: transport, table: member.table, columns: member.columns,
        decodeRow: { $0 }, decodeKey: { $0 }),
      scope: name, output: output)
    demand = CollectionDemand(
      unsafePredicateIdentity: member.predicateIdentity, sourcePredicate: member.where)
    identity = demand.identity(for: definition, scope: scope, columns: source.columns)
    coordinator = CollectionCoordinator(
      definition: definition, scope: scope, source: source, store: store)
  }

  /// Takes a lease on the demand. A scope that already holds one takes another and then gives
  /// the first back, which is how a second consumer's request restarts a failed demand.
  func acquire(_ mode: CollectionLoadMode = .resume) async {
    let previous = lease
    let next = await coordinator.acquire(demand, mode: mode)
    lease = next
    let name = name
    let output = output
    Task {
      for await state in next.stateUpdates {
        output.emit("state", ["scope": .string(name), "state": .string(state.text)])
      }
    }
    if let previous { try? await previous.release() }
  }

  func refresh(_ mode: CollectionLoadMode = .resume) async {
    await lease?.refresh(mode)
  }

  func release() async {
    guard let lease else { return }
    do {
      try await lease.release()
      self.lease = nil
      output.emit("state", ["scope": .string(name), "state": .string("released")])
    } catch {
      output.failure(scope: name, phase: "release", error)
    }
  }

  func rows() async -> JSONValue {
    ProbeRows.sorted(await store.rows(for: identity))
  }

  func position() async -> [String: JSONValue] {
    await store.position(for: identity)
  }
}

/// One group scope: the SDK's group source and group store, driven as the source's
/// documentation describes: resume a stored group when the source can, otherwise materialize,
/// replace the snapshot, then run the tail.
actor ProbeGroupScope {
  private let name: String
  private let output: ProbeOutput
  private let store: ProbeStore
  private let source: CircuitsSubsetGroupSource
  private let demand: CollectionGroupDemand
  private let identity: CollectionGroupDemandIdentity
  private let scope: CollectionScope
  private let materializationID: CollectionMaterializationID
  private let collections: [(table: String, collection: CollectionID)]
  private var session: Task<Void, Never>?
  private var stop: (@Sendable () async throws -> Void)?
  private var isLive = false

  static func materializationID(for name: String) -> CollectionMaterializationID {
    CollectionMaterializationID(rawValue: "probe-group-\(name)")
  }

  init(
    spec: ProbeScopeSpec, members: [ProbeMemberSpec], client: ElectricCircuitsClient,
    transport: ProbeTransport, scope: CollectionScope, store: ProbeStore, output: ProbeOutput
  ) {
    name = spec.name
    self.output = output
    self.store = store
    self.scope = scope
    source = CircuitsSubsetGroupSource(client: client, transport: transport)
    collections = members.map { ($0.table, spec.collection(of: $0)) }
    demand = CollectionGroupDemand(
      members: members.map { member in
        let keyColumn = member.key
        return CollectionGroupMember(
          definition: CollectionDefinition<ChangeRow, String>(
            id: spec.collection(of: member), key: { ProbeJSON.key($0[keyColumn]) }),
          table: member.table, unsafePredicateIdentity: member.predicateIdentity,
          sourcePredicate: member.where, columns: member.columns, decodeRow: { $0 },
          decodeKey: { $0 })
      })
    identity = demand.identity(scope: scope)
    materializationID = Self.materializationID(for: spec.name)
  }

  func acquire(_ mode: CollectionLoadMode = .resume) {
    guard session == nil else { return }
    session = Task { await self.run(mode) }
  }

  func refresh(_ mode: CollectionLoadMode = .resume) async {
    await release()
    acquire(mode)
  }

  func release() async {
    guard let session else { return }
    do { try await stop?() } catch { output.failure(scope: name, phase: "release", error) }
    session.cancel()
    await session.value
    self.session = nil
    stop = nil
    isLive = false
    state("released")
  }

  private func state(_ text: String) {
    output.emit("state", ["scope": .string(name), "state": .string(text)])
  }

  private func run(_ mode: CollectionLoadMode) async {
    let id = materializationID
    let store = store
    var phase = "resume"
    do {
      // A stored group continues from its cursor when the source can prove the stream is the
      // same; otherwise, and when the resumed stream refuses the cursor, it loads a snapshot.
      // `-DPROBE_WITHOUT_GROUP_RESUME` builds the probe against an SDK before group resume
      // (0.15.1 and earlier), for comparing the two on the same engine.
      #if !PROBE_WITHOUT_GROUP_RESUME
        if let record = try await store.groupMaterialization(id) {
          state("cached")
          let fields: [String: JSONValue] = [
            "scope": .string(name), "materialization": .string(id.rawValue),
            "storedGeneration": .string(record.demand.scope.generation),
            "cursor": record.cursor.map { .string($0.offset) } ?? .null,
          ]
          if let resumed = try await source.resume(
            demand, identity: identity, materializationID: id, record: record, mode: mode)
          {
            output.emit("path", fields.merging(["path": .string("resume")]) { $1 })
            stop = resumed.stop
            phase = "resumed-tail"
            do {
              // The stored rows are current when the session says so, not at the first batch.
              try await resumed.run(
                { batch in try await store.applyGroup(batch, to: id) },
                { await self.caughtUp() })
              state("cached")
              return
            } catch let invalidated as CollectionResumeInvalidated {
              output.failure(scope: name, phase: phase, invalidated)
            }
          } else if mode == .resume {
            output.emit("path", fields.merging(["path": .string("resume-declined")]) { $1 })
          }
        }
      #endif
      phase = "materialize"
      isLive = false
      state("refreshing")
      output.emit(
        "path",
        [
          "scope": .string(name), "path": .string("snapshot"),
          "materialization": .string(id.rawValue),
        ])
      let session = try await source.materialize(
        demand, identity: identity, materializationID: id)
      stop = session.stop
      phase = "store"
      do {
        try await store.replaceGroupSnapshot(
          session.snapshot, materializationID: id, demand: identity)
      } catch {
        try? await session.stop()
        throw error
      }
      live()
      phase = "tail"
      try await session.run { batch in try await store.applyGroup(batch, to: id) }
      state("cached")
    } catch is CancellationError {
      return
    } catch {
      output.failure(scope: name, phase: phase, error)
      state("failed")
      session = nil
      stop = nil
      isLive = false
    }
  }

  /// The resumed tail has read from the stored cursor to the stream's head.
  private func caughtUp() {
    output.emit("caught-up", ["scope": .string(name)])
    live()
  }

  private func live() {
    guard !isLive else { return }
    isLive = true
    state("live")
  }

  func rows() async -> JSONValue {
    var tables: [String: JSONValue] = [:]
    for (table, collection) in collections {
      tables[table] = ProbeRows.sorted(
        await store.groupRows(materializationID, collection: collection, scope: scope))
    }
    return .object(tables)
  }

  func position() async -> [String: JSONValue] {
    await store.groupPosition(materializationID)
  }
}

enum ProbeRows {
  /// Rows in key order: numeric keys by value, others as text.
  static func sorted(_ rows: [String: ChangeRow]) -> JSONValue {
    let keys = rows.keys.sorted { left, right in
      if let a = Int64(left), let b = Int64(right) { return a < b }
      return left < right
    }
    return .array(keys.compactMap { rows[$0].map(JSONValue.object) })
  }
}
