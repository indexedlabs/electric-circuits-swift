# Releasing ElectricCircuitsSwift

## 0.7.0 contract

`0.7.0` is the release target for the `ElectricCircuitsSwift` and
`ElectricCircuitsCollections` library products. Their supported profile is Swift tools 6.0, iOS 16+,
and macOS 13+; the transport product's wire surface is the checked-in native `/v1` contract corpus.
Both products remain Foundation-only. GRDB and the SwiftUI application are optional LinearLite
example dependencies, not core dependencies.

Use the [SemVer policy](../Policies/SEMVER.md) to classify later changes. In particular, retain the
documented source-compatibility promise within a supported release line and do not imply binary
module stability.

The 0.7.0 minor release adds `CollectionCoordinator.resumeAndDrainStaleRevalidation()` for a
cleanup-only lifecycle handoff with an explicit readiness result. Retained refused-release leases
continue to occupy scheduler slots even after their predecessor marks disappear. It preserves
source compatibility with 0.6.0 and introduces no store protocol, persisted schema, or HTTP changes.

## Consumer installation

```swift
dependencies: [
  .package(url: "https://github.com/indexedlabs/electric-circuits-swift.git", from: "0.7.0"),
]
```

Add the `ElectricCircuitsSwift` product to targets that use the client and the
`ElectricCircuitsCollections` product to targets that use collection coordination. Applications that
use the optional LinearLite provider resolve its separate package and GRDB dependency themselves.

## Before publishing

Run the release gates from the exact candidate commit:

```sh
Scripts/quality.sh
Scripts/qualify-versioned-linearlite-host.sh
```

The versioned-host qualifier keeps normal development local: it creates a throwaway local
source-control clone, tags only that clone as `0.7.0`, rewrites a copied LinearLite manifest to use
`.package(url: ..., exact: "0.7.0")`, asserts SwiftPM's resolved version and location, runs the
LinearLite tests, and builds the real unsigned generic iOS Simulator host. It creates no canonical
tag and leaves no generated package dependency in this repository.

After review approves the exact candidate and its release evidence, the authorized publisher may:

```sh
git tag -a 0.7.0 <candidate-sha> -m 'ElectricCircuitsSwift 0.7.0'
git push origin 0.7.0
# Create the matching GitHub release from CHANGELOG.md through the approved release workflow.
```

Do not substitute a branch or unpinned revision for the release tag, and do not publish if either
gate is not green on the candidate commit.
