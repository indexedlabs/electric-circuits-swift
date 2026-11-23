import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

/// What the SDK does with the live envelopes a real engine wrote. The envelopes are the fixture,
/// byte for byte as the engine's streams carried them; only the transport around them is scripted.
///
/// The ordering rule these tests pin: a live change is positioned by `headers.lsn`, and an
/// envelope without one is refused, never applied as if it were ordered. The engine at the
/// fixture's commit writes no `lsn` on a feed whose predicate has a subquery.
@Suite("Engine contract: live envelopes")
struct EngineLiveEnvelopeContractTests {


  @Test func aPlainFeedsEnvelopesApplyInPlaceAtTheirCommitLSN() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let lease = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })

    let next = await engine.append("s1", fixture.plain.body)

    #expect(
      await contractEventually {
        try await store.materialization(for: contractIdentity("launch-1"))?.cursor?.offset == next
      })
    // Row 1 updated in place, row 4 inserted and deleted again, row 2 untouched.
    #expect(
      await store.rows() == [
        1: ContractItem(id: 1, title: "one-b"), 2: ContractItem(id: 2, title: "two"),
      ])
    let record = try #require(try await store.materialization(for: contractIdentity("launch-1")))
    #expect(record.cursor == StreamCursor(offset: next, lsn: "0/1BF2F48"))
    #expect(record.sourceVersion.rawValue == "0/1BF2F48")
    #expect(record.sourceVersion.order == 0x1BF_2F48)
    #expect(await lease.state() == .live)
    #expect(await engine.count(.snapshotQuery) == 1)
    try await lease.release()
  }

  /// The refused batch reaches the caller as the source's own error, with the missing position
  /// in it, and nothing of the batch is applied.
  @Test func anEnvelopeWithoutAnLSNFailsTheFeedAndAppliesNothing() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let source = contractSource(engine)
    let identity = contractIdentity("launch-1")
    let id = CollectionMaterializationID(rawValue: "copy-1")
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)

    let session = try await source.materialize(
      listOneItems, identity: identity, materializationID: id)
    try await store.replaceSnapshot(session.snapshot, materializationID: id, demand: identity)
    await engine.append("s1", fixture.subquery.body)

    await #expect(throws: CircuitsSubsetSourceError.invalidLiveSourceVersion(nil)) {
      try await session.run { batch in try await store.apply(batch, to: id) }
    }

    #expect(
      await store.rows() == [
        1: ContractItem(id: 1, title: "one"), 2: ContractItem(id: 2, title: "two"),
      ])
    let record = try #require(try await store.materialization(for: identity))
    #expect(record.cursor == session.snapshot.cursor)
    #expect(record.cursor?.offset == ContractEngine.start)
    // The failed session released its feed once and read the stream once.
    #expect(await engine.count(.release) == 1)
    #expect(await engine.count(.streamRead) == 1)
  }

  /// A batch the source refused sits at the stored cursor, and reading from that cursor again
  /// returns it again. So the attempt after the failure takes a snapshot, whose frontier is past
  /// the batch, instead of resuming into it.
  @Test func theAttemptAfterARefusedEnvelopeSnapshotsInsteadOfResumingIntoIt() async throws {
    let fixture = try EngineEnvelopeFixture.load()
    let engine = ContractEngine(snapshots: [contractSnapshot, laterContractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let lease = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })

    let afterRefused = await engine.append("s1", fixture.subquery.body)
    #expect(await contractEventually { await lease.state() == .failed(.sourceUnavailable) })
    // Rows and the lease are kept; the coordinator starts nothing on its own.
    #expect(await store.rows()[1] == ContractItem(id: 1, title: "one"))
    #expect(await engine.count(.snapshotQuery) == 1)
    #expect(await engine.count(.release) == 1)
    let settled = await engine.requests.count

    await lease.refresh()

    #expect(await contractEventually { await lease.state() == .live })
    #expect(await engine.count(.snapshotQuery) == 2)
    #expect(await store.rows()[1] == ContractItem(id: 1, title: "one-b"))
    // One feed create, the frontier past the refused batch, one snapshot query, then the tail
    // from that frontier: the refused batch is never read again. (A snapshot is live when it
    // commits, before its tail has sent its first read.)
    #expect(await contractEventually { await engine.kinds(from: settled).count >= 4 })
    #expect(
      Array(await engine.kinds(from: settled).prefix(4))
        == [.feedCreate, .frontier, .snapshotQuery, .streamRead])
    #expect(
      await engine.requests(of: .streamRead).map(\.offset) == [ContractEngine.start, afterRefused])
    let record = try #require(try await store.materialization(for: contractIdentity("launch-1")))
    #expect(record.cursor?.offset == afterRefused)
    try await lease.release()
  }

}

/// A snapshot that left transaction 768 in progress: `767` and `769` are visible to it, `768` and
/// everything from `770` on are not. Its horizon is above its LSN.
private let overlapSnapshot =
  #"{"rows":[{"id":1,"list_id":1,"title":"one"},{"id":2,"list_id":1,"title":"two"}],"#
  + #""lsn":"0/1BF2C98","snapshot":"767:770:768","horizon":"0/1BF2D80"}"#

/// One `items` envelope in the engine's form, with the position under test.
private func positioned(_ id: Int, _ title: String, lsn: String, txid: String) -> String {
  #"{"type":"public.items","key":"\#(id)","value":{"id":\#(id),"list_id":1,"title":"\#(title)"},"#
    + #""headers":{"operation":"upsert","txid":"\#(txid)","lsn":"\#(lsn)","commit_ts":1794939392095}}"#
}

/// One `items` delete in the engine's form: the key, no value, and the position under test. From
/// electric-circuits `e10c4bd` (#70) a changes-only feed sends one for a row that leaves its scope
/// even when the client holds it only from its snapshot, and from `a248886` (#71) a subquery feed's
/// delete carries `headers.lsn` like every other envelope.
private func positionedDelete(_ id: Int, lsn: String, txid: String) -> String {
  #"{"type":"public.items","key":"\#(id)","#
    + #""headers":{"operation":"delete","txid":"\#(txid)","lsn":"\#(lsn)","commit_ts":1794939392095}}"#
}

private func batch(_ envelopes: String...) -> String {
  "[" + envelopes.joined(separator: ",") + "]"
}

/// The rules the engine states for an envelope's position (electric-circuits
/// `docs/ARCHITECTURE.md`, "Native envelope positions", from `a248886`, #71), as the SDK applies
/// them. The engine at the fixture's commit does not yet position a subquery feed's envelopes, so
/// these envelopes are written to the contract's form rather than captured.
@Suite("Engine contract: envelope positions")
struct EngineEnvelopePositionTests {
  private func rows(_ store: InMemoryCollectionStore<ContractItem, Int64>) async -> [Int64: String]
  {
    await store.rows().mapValues(\.title)
  }

  private func cursorReaches(
    _ offset: String, _ store: InMemoryCollectionStore<ContractItem, Int64>,
    generation: String = "launch-1"
  ) async -> Bool {
    await contractEventually {
      try await store.materialization(for: contractIdentity(generation))?.cursor?.offset == offset
    }
  }

  /// Rule 2: a snapshot is consulted for a key no envelope has been applied to since, and it
  /// skips only what it already includes.
  @Test func anEnvelopeTheSnapshotIncludesIsSkippedAndOneItLacksIsApplied() async throws {
    let engine = ContractEngine(snapshots: [overlapSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let lease = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })

    let next = await engine.append(
      "s1",
      batch(
        // Visible to the snapshot and below its horizon: the snapshot's row already says this.
        positioned(2, "in the snapshot", lsn: "0/1BF2C50", txid: "769"),
        // In progress when the snapshot was taken: below its LSN, and not in it.
        positioned(1, "committed after the snapshot", lsn: "0/1BF2C60", txid: "768")))

    #expect(await cursorReaches(next, store))
    #expect(await rows(store) == [1: "committed after the snapshot", 2: "two"])
    try await lease.release()
  }

  /// Rule 2's limit: once an envelope has been applied to a key, the next one for it is applied
  /// whatever the snapshot saw. A flip's verdict names the transaction that caused it, which the
  /// snapshot can include, and still follows a newer change to the row at that change's position.
  @Test func aVerdictForAKeyTheStreamAlreadyChangedIsApplied() async throws {
    let engine = ContractEngine(snapshots: [overlapSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let lease = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })

    await engine.append(
      "s1", batch(positioned(1, "committed after the snapshot", lsn: "0/1BF2C60", txid: "768")))
    let next = await engine.append(
      "s1", batch(positioned(1, "the stream's last word", lsn: "0/1BF2C60", txid: "769")))

    #expect(await cursorReaches(next, store))
    #expect(await rows(store)[1] == "the stream's last word")
    try await lease.release()
  }

  /// The same two envelopes with a relaunch between them. The resumed tail cannot know which keys
  /// the stream has changed since the snapshot, so once its cursor has left the snapshot's offset
  /// it skips nothing on the snapshot's account. The store still keeps an envelope the snapshot
  /// covers from replacing a row the snapshot wrote.
  @Test func aResumedTailAppliesAVerdictForAKeyAnEarlierLaunchChanged() async throws {
    let engine = ContractEngine(snapshots: [overlapSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let first = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await first.state() == .live })
    let applied = await engine.append(
      "s1", batch(positioned(1, "committed after the snapshot", lsn: "0/1BF2C60", txid: "768")))
    #expect(await cursorReaches(applied, store))
    try await first.release()
    // Its position is the snapshot's still: the applied change committed below the snapshot's LSN.
    #expect(
      try await store.materialization(for: contractIdentity("launch-1"))?.sourceVersion.rawValue
        == "0/1BF2C98")

    let next = await engine.append(
      "s1",
      batch(
        positioned(1, "the stream's last word", lsn: "0/1BF2C60", txid: "769"),
        positioned(2, "in the snapshot", lsn: "0/1BF2C60", txid: "769")))
    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await cursorReaches(next, store, generation: "launch-2"))
    #expect(await rows(store) == [1: "the stream's last word", 2: "two"])
    #expect(await engine.count(.snapshotQuery) == 1)
    try await lease.release()
  }

  /// A resumed tail whose cursor is still the snapshot's offset has applied nothing, so the
  /// snapshot's visibility, which the fence carries, gates it exactly as the first tail's would.
  @Test func aResumedTailAtTheSnapshotsOffsetStillSkipsWhatTheSnapshotIncludes() async throws {
    let engine = ContractEngine(snapshots: [overlapSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let first = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await first.state() == .live })
    #expect(await contractEventually { await engine.count(.streamRead) == 1 })
    try await first.release()

    let next = await engine.append(
      "s1",
      batch(
        positioned(2, "in the snapshot", lsn: "0/1BF2C50", txid: "769"),
        positioned(1, "committed after the snapshot", lsn: "0/1BF2C60", txid: "768")))
    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await cursorReaches(next, store, generation: "launch-2"))
    #expect(await rows(store) == [1: "committed after the snapshot", 2: "two"])
    // Only the envelope the snapshot lacked became a change.
    #expect(
      try await store.materialization(for: contractIdentity("launch-2"))?.sourceVersion.rawValue
        == "0/1BF2C98")
    try await lease.release()
  }

  /// A row the client holds only from its snapshot leaves the scope: the feed never delivered it,
  /// and the engine still sends its delete (electric-circuits #70), positioned (#71). Rule 2
  /// consults the snapshot for that key, which lacks the transaction, so the delete applies, on
  /// the first tail and on a tail resumed by a later launch.
  @Test func aDeleteForARowHeldOnlyFromTheSnapshotRemovesIt() async throws {
    let engine = ContractEngine(snapshots: [overlapSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let first = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await first.state() == .live })

    // In progress when the snapshot was taken, below its LSN: the snapshot still holds row 2.
    let removed = await engine.append(
      "s1", batch(positionedDelete(2, lsn: "0/1BF2C60", txid: "768")))
    #expect(await cursorReaches(removed, store))
    #expect(await rows(store) == [1: "one"])
    try await first.release()

    // A later launch resumes from the stored cursor and removes the other snapshot row the same
    // way, past the snapshot's horizon.
    let next = await engine.append(
      "s1", batch(positionedDelete(1, lsn: "0/1BF2E00", txid: "771")))
    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await cursorReaches(next, store, generation: "launch-2"))
    #expect(await rows(store) == [:])
    #expect(await engine.count(.snapshotQuery) == 1)
    try await lease.release()
  }

  /// Rule 1: between envelopes a position only rejects a replay. Equal positions are ordinary and
  /// offset order decides among them; a lower one is a replay of something already applied.
  @Test func equalPositionsApplyInOffsetOrderAndALowerOneIsAReplay() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let lease = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await lease.state() == .live })

    await engine.append(
      "s1",
      batch(
        positioned(1, "first", lsn: "0/1BF2E00", txid: "771"),
        positioned(1, "second, same transaction", lsn: "0/1BF2E00", txid: "771")))
    await engine.append(
      "s1", batch(positioned(1, "raised verdict", lsn: "0/1BF2E00", txid: "770")))
    let next = await engine.append(
      "s1", batch(positioned(1, "replayed", lsn: "0/1BF2D90", txid: "769")))

    #expect(await cursorReaches(next, store))
    #expect(await rows(store)[1] == "raised verdict")
    #expect(
      try await store.materialization(for: contractIdentity("launch-1"))?.sourceVersion.rawValue
        == "0/1BF2E00")
    try await lease.release()
  }

  /// After a relaunch the last position applied is the stored version: a restarted engine's
  /// replays, at their own lower positions, are rejected, and the stream's next change applies.
  @Test func aResumedTailRejectsAReplayBelowTheStoredPosition() async throws {
    let engine = ContractEngine(snapshots: [contractSnapshot])
    let store = InMemoryCollectionStore<ContractItem, Int64>(key: \.id)
    let first = await contractLaunch(engine, store: store, generation: "launch-1")
      .acquire(listOneItems)
    #expect(await contractEventually { await first.state() == .live })
    let applied = await engine.append(
      "s1", batch(positioned(1, "applied", lsn: "0/1BF2E00", txid: "771")))
    #expect(await cursorReaches(applied, store))
    try await first.release()

    let next = await engine.append(
      "s1",
      batch(
        positioned(1, "replayed", lsn: "0/1BF2D90", txid: "770"),
        positioned(2, "next change", lsn: "0/1BF2E00", txid: "772")))
    let lease = await contractLaunch(engine, store: store, generation: "launch-2")
      .acquire(listOneItems)

    #expect(await cursorReaches(next, store, generation: "launch-2"))
    #expect(await rows(store) == [1: "applied", 2: "next change"])
    try await lease.release()
  }

}
