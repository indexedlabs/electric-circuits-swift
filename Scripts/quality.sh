#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-swift-quality.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

cd "$root"
Scripts/check-package-quality.sh
Scripts/check-package-quality-negative-fixtures.sh

swift build -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
swift test -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
swift package show-dependencies

(
  cd Examples/LinearLite
  # A defaulted public API addition is source-compatible, but an existing local `.build` can still
  # contain objects linked against an older module symbol. CI must prove a fresh example build.
  swift package clean
  swift build -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
  swift test -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors
  swift package show-dependencies
)

Scripts/check-swift-format.sh

symbol_graphs="$work_dir/symbol-graphs"
mkdir -p "$symbol_graphs"
swift build -Xswiftc -emit-symbol-graph -Xswiftc -emit-symbol-graph-dir -Xswiftc "$symbol_graphs"
xcrun docc convert Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc \
  --additional-symbol-graph-dir "$symbol_graphs" --output-path "$work_dir/ElectricCircuitsSwift.doccarchive" \
  --fallback-display-name ElectricCircuitsSwift \
  --fallback-bundle-identifier com.electriccircuits.swift --fallback-bundle-version 0.1.0 \
  --warnings-as-errors

Scripts/generate-linear-lite-host-project.sh

xcodebuild -project Examples/LinearLite/iOS/LinearLiteHost.xcodeproj -scheme LinearLiteHost \
  -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' -configuration Debug \
  -derivedDataPath "$work_dir/DerivedData" CODE_SIGNING_ALLOWED=NO CODE_SIGNING_REQUIRED=NO \
  CODE_SIGN_IDENTITY= build
