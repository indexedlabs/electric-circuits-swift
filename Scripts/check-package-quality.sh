#!/usr/bin/env bash
set -euo pipefail

root=${1:-$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)}

require_file() {
  local path=$1
  if [[ ! -f "$root/$path" ]]; then
    printf 'package-quality: missing required file: %s\n' "$path" >&2
    return 1
  fi
}

require_text() {
  local path=$1
  local text=$2
  if ! grep -Fq -- "$text" "$root/$path"; then
    printf 'package-quality: %s must contain: %s\n' "$path" "$text" >&2
    return 1
  fi
}

required_files=(
  Package.swift
  Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/ElectricCircuitsSwift.md
  Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Transport.md
  Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Materialization.md
  Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Lifecycle-and-errors.md
  Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Telemetry.md
  Policies/SUPPORT.md
  Policies/SEMVER.md
  CHANGELOG.md
  Docs/RELEASING.md
  .github/workflows/swift.yml
  Scripts/quality.sh
  Scripts/check-swift-format.sh
  Scripts/install-xcodegen.sh
  Scripts/generate-linear-lite-host-project.sh
  Scripts/assert-versioned-package-resolved.swift
  Scripts/qualify-versioned-linearlite-host.sh
  Examples/LinearLite/iOS/project.yml
  Examples/LinearLite/iOS/LinearLiteHost.xcodeproj/project.pbxproj
)

for path in "${required_files[@]}"; do require_file "$path"; done

require_text Package.swift '// swift-tools-version: 6.0'
require_text Package.swift '.iOS(.v16)'
require_text Package.swift '.macOS(.v13)'
require_text Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/ElectricCircuitsSwift.md \
  '@Metadata'
require_text Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Transport.md 'HTTPTransport'
require_text Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Materialization.md 'ShapeMaterializer'
require_text Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Lifecycle-and-errors.md \
  'ShapeSubscriptionCoordinator'
require_text Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc/Telemetry.md 'TelemetryConfiguration'
require_text Policies/SUPPORT.md 'iOS 16'
require_text Policies/SUPPORT.md 'macOS 13'
require_text Policies/SUPPORT.md 'ElectricCircuitsCollections'
require_text Policies/SEMVER.md 'major'
require_text Policies/SEMVER.md 'minor'
require_text Policies/SEMVER.md 'patch'
require_text CHANGELOG.md '0.2.1'
require_text Docs/RELEASING.md '.package(url:'
require_text Docs/RELEASING.md 'exact: "0.2.1"'
require_text Docs/RELEASING.md 'ElectricCircuitsCollections'
require_text .github/workflows/swift.yml \
  'actions/checkout@d23441a48e516b6c34aea4fa41551a30e30af803 # v6.1.0'
require_text .github/workflows/swift.yml 'macos-15'
require_text .github/workflows/swift.yml 'Xcode_26.3.app'
require_text Scripts/quality.sh 'docc convert'
require_text Scripts/quality.sh 'CODE_SIGNING_ALLOWED=NO'
require_text Scripts/generate-linear-lite-host-project.sh 'cmp -s'
require_text Scripts/assert-versioned-package-resolved.swift 'JSONDecoder'
require_text Scripts/qualify-versioned-linearlite-host.sh 'git clone --quiet --no-local'
require_text Scripts/qualify-versioned-linearlite-host.sh 'exact: "0.2.1"'

printf 'package-quality: manifest is complete\n'
