import ElectricCircuitsCollections
import ElectricCircuitsSwift
import Foundation
import Testing

@Suite("Subset page transaction visibility")
struct CollectionPageSnapshotTests {
  @Test func decodeOptionalFieldsAndLegacyResponse() throws {
    let decoder = JSONDecoder()
    let page = try decoder.decode(
      SubsetResponse.self,
      from: Data(#"{"rows":[],"lsn":"0/100","snapshot":"100:110:105","horizon":"0/180"}"#.utf8))
    #expect(page.snapshot == "100:110:105")
    #expect(page.horizon == "0/180")
    let old = try decoder.decode(
      SubsetResponse.self, from: Data(#"{"rows":[],"lsn":"0/100"}"#.utf8))
    #expect(old.snapshot == nil)
    #expect(old.horizon == nil)
  }

  @Test func visibilityAndHorizon() throws {
    let page = try #require(
      CollectionPageSnapshot(lsn: 0x100, snapshot: "100:110:105", horizon: "0/180"))
    #expect(!page.includes(lsn: 0xFF, transactionID: 105))
    #expect(page.includes(lsn: 0x120, transactionID: 104))
    #expect(page.includes(lsn: 0x17F, transactionID: 99))
    #expect(!page.includes(lsn: 0x180, transactionID: 99))
    #expect(!page.includes(lsn: 0x181, transactionID: 99))
    #expect(!page.includes(lsn: 0x90, transactionID: 110))
    #expect(page.includes(lsn: 0x90, transactionID: nil))
    #expect(!page.includes(lsn: 0x100, transactionID: nil))
  }

  @Test func wraparoundAndDecimalMasking() throws {
    let page = try #require(
      CollectionPageSnapshot(
        lsn: 0x100, snapshot: "4294967290:4294967300:4294967299", horizon: "0/180"))
    #expect(page.includes(lsn: 0x90, transactionID: 4_294_967_294))
    #expect(page.includes(lsn: 0x90, transactionID: 2))
    #expect(!page.includes(lsn: 0x90, transactionID: 3))
    #expect(!page.includes(lsn: 0x90, transactionID: 5))
    #expect(CollectionPageSnapshot.xid32("4294967299") == 3)
    #expect(CollectionPageSnapshot.xid32("18446744073709551619") == 3)
    for invalid in ["", "-1", "+1", "1.0", " 1", "١"] {
      #expect(CollectionPageSnapshot.xid32(invalid) == nil)
    }
  }

  @Test func malformedPositioningFallsBack() {
    #expect(CollectionPageSnapshot(lsn: 1, snapshot: nil, horizon: nil) == nil)
    #expect(CollectionPageSnapshot(lsn: 1, snapshot: "1:2:", horizon: "garbage") == nil)
    #expect(CollectionPageSnapshot(lsn: 1, snapshot: "1:x:", horizon: "0/180") == nil)
    #expect(CollectionPageSnapshot(lsn: 1, snapshot: "1:2:bad", horizon: "0/180") == nil)
  }

  @Test func rowVersionsRoundTripAndProtectAcrossMaterializations() throws {
    let page = try #require(
      CollectionPageSnapshot(lsn: 0x100, snapshot: "100:110:105", horizon: "0/180"))
    let pageVersion = CollectionSourceVersion(rawValue: "0/100", order: 0x100, snapshot: page)
    let live = CollectionSourceVersion(rawValue: "0/FF", order: 0xFF, transactionID: 105)
    #expect(live.supersedes(pageVersion))
    #expect(!pageVersion.supersedes(live))
    let visible = CollectionSourceVersion(rawValue: "0/120", order: 0x120, transactionID: 104)
    #expect(!visible.supersedes(pageVersion))
    #expect(pageVersion.supersedes(visible))
    let newerPage = try #require(
      CollectionPageSnapshot(lsn: 0x100, snapshot: "106:110:", horizon: "0/180"))
    let newer = CollectionSourceVersion(rawValue: "0/100", order: 0x100, snapshot: newerPage)
    #expect(newer.supersedes(live))
    #expect(newer.supersedes(pageVersion))
    #expect(!pageVersion.supersedes(newer))
    for version in [pageVersion, live] {
      let decoded = try JSONDecoder().decode(
        CollectionSourceVersion.self, from: JSONEncoder().encode(version))
      #expect(decoded.snapshot == version.snapshot)
      #expect(decoded.transactionID == version.transactionID)
    }
    let legacy = try JSONDecoder().decode(
      CollectionSourceVersion.self, from: Data(#"{"rawValue":"0/100","order":256}"#.utf8))
    #expect(legacy.snapshot == nil)
    #expect(legacy.transactionID == nil)
    #expect(!live.supersedes(legacy))
  }
}
