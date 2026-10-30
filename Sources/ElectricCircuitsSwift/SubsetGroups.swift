import Foundation

#if canImport(FoundationNetworking)
  import FoundationNetworking
#endif

// MARK: - Shape group load DTOs

/// One table of a shape group: the single-table predicate the engine would accept on its own.
/// `key` names the member in the answer and defaults to its table on the server.
public struct SubsetGroupMember: Codable, Equatable, Sendable {
  public var key: String?
  public var table: String
  public var `where`: Predicate?
  public var columns: [String]?

  public init(
    table: String, where predicate: Predicate? = nil, columns: [String]? = nil, key: String? = nil
  ) {
    self.key = key
    self.table = table
    self.where = predicate
    self.columns = columns
  }
}

/// A group load: one subscription claim and every member, sent in one request. Retrying the same
/// request value renews the group instead of conflicting with itself.
public struct SubsetGroupRequest: Codable, Equatable, Sendable {
  public var subscription: String?
  public var members: [SubsetGroupMember]

  public init(members: [SubsetGroupMember], subscription: String? = UUID().uuidString) {
    self.subscription = subscription
    self.members = members
  }
}

/// One snapshot row of a group, tagged with its member's table so equal keys in two tables stay
/// distinct.
public struct SubsetGroupRow: Codable, Equatable, Sendable {
  public var table: String
  public var value: JSONValue

  public init(table: String, value: JSONValue) {
    self.table = table
    self.value = value
  }
}

/// One member of a loaded group, in request order. Its `shapeId` is diagnostic only: renew and
/// release go through the group's handle.
public struct SubsetGroupMemberResult: Codable, Equatable, Sendable {
  public var key: String
  public var table: String
  public var shapeId: String?
  public var subscription: String?

  public init(key: String, table: String, shapeId: String? = nil, subscription: String? = nil) {
    self.key = key
    self.table = table
    self.shapeId = shapeId
    self.subscription = subscription
  }
}

/// The answer to a group load: one handle and stream for the whole group, one snapshot position,
/// flat table-tagged rows and the members in request order.
public struct SubsetGroupResponse: Codable, Equatable, Sendable {
  public var groupId: String?
  /// The group's one shape id, equal to `groupId`; renew and release use it.
  public var shapeId: String
  public var subscription: String?
  public var leaseSeconds: Int?
  public var streamPath: String
  public var streamURL: URL
  /// The group stream's frontier at the snapshot; the live tail starts here.
  public var offset: String
  public var lsn: String
  public var snapshot: String?
  public var horizon: String?
  public var rows: [SubsetGroupRow]
  public var results: [SubsetGroupMemberResult]

  public init(
    shapeId: String, streamPath: String, streamURL: URL, offset: String, lsn: String,
    rows: [SubsetGroupRow], results: [SubsetGroupMemberResult], groupId: String? = nil,
    subscription: String? = nil, leaseSeconds: Int? = nil, snapshot: String? = nil,
    horizon: String? = nil
  ) {
    self.groupId = groupId
    self.shapeId = shapeId
    self.subscription = subscription
    self.leaseSeconds = leaseSeconds
    self.streamPath = streamPath
    self.streamURL = streamURL
    self.offset = offset
    self.lsn = lsn
    self.snapshot = snapshot
    self.horizon = horizon
    self.rows = rows
    self.results = results
  }

  enum CodingKeys: String, CodingKey {
    case groupId, shapeId, subscription, leaseSeconds, streamPath
    case streamURL = "streamUrl"
    case offset, lsn, snapshot, horizon, rows, results
  }

  /// The group's one claim, as a shape handle the subscription coordinator renews, reads and
  /// releases. A group has no single table, so `table` lists the members' tables in order.
  public var handle: ShapeHandle {
    ShapeHandle(
      response: ShapeResponse(
        shapeId: shapeId, table: results.map(\.table).joined(separator: ","),
        streamPath: streamPath, streamURL: streamURL, subscription: subscription,
        leaseSeconds: leaseSeconds))
  }

  /// The stream position the snapshot was read at: the live tail resumes from `offset`.
  public var cursor: StreamCursor { StreamCursor(offset: offset, lsn: lsn) }
}

extension ElectricCircuitsClient {
  /// Loads several tables as one shape group: one request naming the subscription and every
  /// member, answered with one handle, one stream and snapshot rows tagged by table. The answer
  /// must list exactly the requested members in order and tag every row with one of their tables;
  /// anything else fails the whole load, because a group succeeds or fails as one.
  public func loadSubsetGroup(_ request: SubsetGroupRequest) async throws -> SubsetGroupResponse {
    var response: SubsetGroupResponse = try await sendGroupLoad(request)
    if let subscription = request.subscription {
      if let echoed = response.subscription, echoed != subscription {
        throw ClientError.decoding("group response subscription does not match request")
      }
      // As with a shape create, the claim is the client's ownership identity for renew and
      // release, so the handle keeps it even when the server omits the echo.
      response.subscription = subscription
    }
    guard response.results.count == request.members.count else {
      throw ClientError.decoding("group response members do not match request")
    }
    // A named member must come back under its name. An unnamed member's key defaults to the
    // server's spelling of its table, which may add the schema, so only its position is checked.
    for (member, result) in zip(request.members, response.results) {
      if let key = member.key, result.key != key {
        throw ClientError.decoding("group response members do not match request")
      }
    }
    // The server may spell a table with its schema; a row is accepted under either spelling.
    let tables = Set(request.members.map(\.table) + response.results.map(\.table))
    guard response.rows.allSatisfy({ tables.contains($0.table) }) else {
      throw ClientError.decoding("group response row names a table outside the group")
    }
    return response
  }
}

/// The claim fields of a group load answer. Decoding skips `rows`, so a renewal never builds the
/// group's snapshot rows only to throw them away.
private struct SubsetGroupClaim: Decodable {
  var shapeId: String
  var subscription: String?
  var leaseSeconds: Int?
  var streamPath: String
  var streamURL: URL
  var results: [SubsetGroupMemberResult]

  enum CodingKeys: String, CodingKey {
    case shapeId, subscription, leaseSeconds, streamPath
    case streamURL = "streamUrl"
    case results
  }
}

extension ElectricCircuitsClient {
  /// Creates or renews a group's claim with the same request `loadSubsetGroup(_:)` sends, keeping
  /// only its handle. The coordinator uses it for start and lease renewal, where the snapshot rows
  /// the answer still carries are not wanted.
  func claimSubsetGroup(_ request: SubsetGroupRequest) async throws -> ShapeHandle {
    let claim: SubsetGroupClaim = try await sendGroupLoad(request)
    guard claim.results.count == request.members.count else {
      throw ClientError.decoding("group response members do not match request")
    }
    for (member, result) in zip(request.members, claim.results) {
      if let key = member.key, result.key != key {
        throw ClientError.decoding("group response members do not match request")
      }
    }
    var subscription = claim.subscription
    if let requested = request.subscription {
      if let echoed = subscription, echoed != requested {
        throw ClientError.decoding("group response subscription does not match request")
      }
      subscription = requested
    }
    return ShapeHandle(
      response: ShapeResponse(
        shapeId: claim.shapeId, table: claim.results.map(\.table).joined(separator: ","),
        streamPath: claim.streamPath, streamURL: claim.streamURL, subscription: subscription,
        leaseSeconds: claim.leaseSeconds))
  }
}
