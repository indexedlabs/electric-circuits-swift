# Releasing ElectricCircuitsSwift

## 0.1.0 contract

`0.1.0` is the first public source release of the `ElectricCircuitsSwift` library product. Its
supported profile is Swift tools 6.0, iOS 16+, and macOS 13+; its wire surface is the checked-in
native `/v1` contract corpus. The core remains Foundation-only. GRDB and the SwiftUI application
are optional LinearLite example dependencies, not core dependencies.

Use the [SemVer policy](../Policies/SEMVER.md) to classify later changes. In particular, retain the
documented source-compatibility promise within a supported release line and do not imply binary
module stability.

## Consumer installation

```swift
dependencies: [
  .package(url: "https://github.com/indexedlabs/electric-circuits-swift.git", from: "0.1.0"),
]
```

Add the `ElectricCircuitsSwift` product to the target that uses the client. Applications that use
the optional LinearLite provider resolve its separate package and GRDB dependency themselves.

## Before publishing

Run the release gates from the exact candidate commit:

```sh
Scripts/quality.sh
Scripts/qualify-versioned-linearlite-host.sh
```

The versioned-host qualifier keeps normal development local: it creates a throwaway local
source-control clone, tags only that clone as `0.1.0`, rewrites a copied LinearLite manifest to use
`.package(url: ..., exact: "0.1.0")`, asserts SwiftPM's resolved version and location, runs the
LinearLite tests, and builds the real unsigned generic iOS Simulator host. It creates no canonical
tag and leaves no generated package dependency in this repository.

After review approves the exact candidate and its release evidence, the authorized publisher may:

```sh
git tag -a 0.1.0 <candidate-sha> -m 'ElectricCircuitsSwift 0.1.0'
git push origin 0.1.0
# Create the matching GitHub release from CHANGELOG.md through the approved release workflow.
```

Do not substitute a branch or unpinned revision for the release tag, and do not publish if either
gate is not green on the candidate commit.
