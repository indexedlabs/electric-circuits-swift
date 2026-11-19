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
  /// The identity of the claim this load granted, as `ShapeResponse.claim`.
  public var claim: String?

  public init(
    shapeId: String, streamPath: String, streamURL: URL, offset: String, lsn: String,
    rows: [SubsetGroupRow], results: [SubsetGroupMemberResult], groupId: String? = nil,
    subscription: String? = nil, leaseSeconds: Int? = nil, snapshot: String? = nil,
    horizon: String? = nil, claim: String? = nil
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
    self.claim = claim
  }

  enum CodingKeys: String, CodingKey {
    case groupId, shapeId, subscription, leaseSeconds, streamPath
    case streamURL = "streamUrl"
    case offset, lsn, snapshot, horizon, rows, results, claim
  }

  /// The group's one claim, as a shape handle the subscription coordinator renews, reads and
  /// releases. A group has no single table, so `table` lists the members' tables in order.
  public var handle: ShapeHandle {
    ShapeHandle(
      response: ShapeResponse(
        shapeId: shapeId, table: results.map(\.table).joined(separator: ","),
        streamPath: streamPath, streamURL: streamURL, subscription: subscription,
        leaseSeconds: leaseSeconds, claim: claim))
  }

  /// The stream position the snapshot was read at: the live tail resumes from `offset`.
  public var cursor: StreamCursor { StreamCursor(offset: offset, lsn: lsn) }
}

extension ElectricCircuitsClient {
  /// Loads several tables as one shape group: one request naming the subscription and every
  /// member, answered with one handle, one stream and snapshot rows tagged by table. The answer
  /// must list exactly the requested members in order and tag every row with one of their tables;
  /// anything else fails the whole load, because a group succeeds or fails as one.
  ///
  /// `claim` is the identity of the claim the caller already holds under the subscription, as
  /// for `createShape(_:holding:)`.
  public func loadSubsetGroup(_ request: SubsetGroupRequest, holding claim: String? = nil)
    async throws -> SubsetGroupResponse
  {
    var response: SubsetGroupResponse = try await sendGroupLoad(request, claim: claim)
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

/// The answer to a group re-join: a load's answer less the snapshot. The engine read no rows for
/// it, so it has no `rows`, `lsn`, `snapshot` or `horizon`.
public struct SubsetGroupRejoinResponse: Codable, Equatable, Sendable {
  public var groupId: String?
  /// The group's one shape id, equal to `groupId`; renew and release use it.
  public var shapeId: String
  public var subscription: String?
  public var leaseSeconds: Int?
  public var streamPath: String
  public var streamURL: URL
  /// The group stream's tail when the claim was taken. A reader that resumes continues from the
  /// offset it stored, not from this one.
  public var offset: String?
  public var results: [SubsetGroupMemberResult]
  /// The identity of the claim this re-join granted or renewed, as `ShapeResponse.claim`.
  public var claim: String?

  public init(
    shapeId: String, streamPath: String, streamURL: URL, results: [SubsetGroupMemberResult],
    groupId: String? = nil, subscription: String? = nil, leaseSeconds: Int? = nil,
    offset: String? = nil, claim: String? = nil
  ) {
    self.groupId = groupId
    self.shapeId = shapeId
    self.subscription = subscription
    self.leaseSeconds = leaseSeconds
    self.streamPath = streamPath
    self.streamURL = streamURL
    self.offset = offset
    self.results = results
    self.claim = claim
  }

  enum CodingKeys: String, CodingKey {
    case groupId, shapeId, subscription, leaseSeconds, streamPath
    case streamURL = "streamUrl"
    case offset, results, claim
  }

  /// The group's one claim, as `SubsetGroupResponse.handle` spells it.
  public var handle: ShapeHandle {
    ShapeHandle(
      response: ShapeResponse(
        shapeId: shapeId, table: results.map(\.table).joined(separator: ","),
        streamPath: streamPath, streamURL: streamURL, subscription: subscription,
        leaseSeconds: leaseSeconds, claim: claim))
  }
}

/// What `POST /v1/subsets/rejoin` found for a subscription and its members
/// (`Contracts/native-v1-contract-v1.md`, "Group re-join").
public enum SubsetGroupRejoin: Equatable, Sendable {
  /// The subscription's group, claimed again or renewed. The engine read no rows.
  case joined(SubsetGroupRejoinResponse)
  /// The engine holds no group for this subscription and exactly these members (`412`): it was
  /// never loaded, was retired, or was loaded with other members. Nothing was created. Load it.
  case noSuchGroup
  /// The server has no re-join route (`404` or `405`): an engine or gateway from before it
  /// existed. Claim and renew with `loadSubsetGroup(_:)`, as before.
  case unavailable
}

extension SubsetGroupRejoin {
  /// The status that says "no such group". No other native route answers it.
  public static let noSuchGroupStatus = 412
}

extension ElectricCircuitsClient {
  /// Claims or renews the group `request.subscription` already holds for exactly
  /// `request.members`, without the engine reading a row: the body `loadSubsetGroup(_:)` sends,
  /// answered with the load's answer less its snapshot. It never creates a group. A session that
  /// kept its rows and its stream offset resumes with it, and every lease renewal uses it.
  ///
  /// The answer is checked against the request as a load's is. `503` and the other transient
  /// statuses throw `ClientError.retryableHTTP`, and every other refusal `ClientError.http`,
  /// as for a load.
  ///
  /// `claim` is the identity of the claim the caller already holds under the subscription: a
  /// session's renewal names it and keeps it. A session that holds none, such as one that
  /// resumes a stored group in a new process, names none and is answered a claim of its own, so
  /// a release an earlier session sent cannot release it.
  public func rejoinSubsetGroup(_ request: SubsetGroupRequest, holding claim: String? = nil)
    async throws -> SubsetGroupRejoin
  {
    var answer: SubsetGroupRejoinResponse
    do {
      answer = try await sendGroupRejoin(request, claim: claim)
    } catch ClientError.http(status: SubsetGroupRejoin.noSuchGroupStatus, message: _) {
      return .noSuchGroup
    } catch ClientError.http(status: let status, message: _) where status == 404 || status == 405 {
      return .unavailable
    }
    guard answer.results.count == request.members.count else {
      throw ClientError.decoding("group response members do not match request")
    }
    for (member, result) in zip(request.members, answer.results) {
      if let key = member.key, result.key != key {
        throw ClientError.decoding("group response members do not match request")
      }
    }
    if let requested = request.subscription {
      if let echoed = answer.subscription, echoed != requested {
        throw ClientError.decoding("group response subscription does not match request")
      }
      answer.subscription = requested
    }
    return .joined(answer)
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
  var claim: String?

  enum CodingKeys: String, CodingKey {
    case shapeId, subscription, leaseSeconds, streamPath
    case streamURL = "streamUrl"
    case results, claim
  }
}

extension ElectricCircuitsClient {
  /// Creates or renews a group's claim with the same request `loadSubsetGroup(_:)` sends, keeping
  /// only its handle. The coordinator uses it to start a group no setup load claimed, and to
  /// renew on a server without the re-join route, where the snapshot rows the answer still
  /// carries are not wanted.
  func claimSubsetGroup(_ request: SubsetGroupRequest, holding held: String? = nil) async throws
    -> ShapeHandle
  {
    let claim: SubsetGroupClaim = try await sendGroupLoad(request, claim: held)
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
        leaseSeconds: claim.leaseSeconds, claim: claim.claim))
  }
}
