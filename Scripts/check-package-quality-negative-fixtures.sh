#!/usr/bin/env bash
set -euo pipefail

root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
checker="$root/Scripts/check-package-quality.sh"
fixture=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-package-quality-fixture.XXXXXX")
trap 'rm -rf "$fixture"' EXIT

mkdir -p "$fixture/Sources/ElectricCircuitsSwift" "$fixture/Examples/LinearLite/iOS/LinearLiteHost.xcodeproj"
mkdir -p "$fixture/.github/workflows"
cp "$root/Package.swift" "$fixture/Package.swift"
ditto "$root/Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc" \
  "$fixture/Sources/ElectricCircuitsSwift/ElectricCircuitsSwift.docc"
ditto "$root/Policies" "$fixture/Policies"
ditto "$root/Scripts" "$fixture/Scripts"
cp "$root/.github/workflows/swift.yml" "$fixture/.github/workflows/swift.yml"
cp "$root/Examples/LinearLite/iOS/project.yml" "$fixture/Examples/LinearLite/iOS/project.yml"
cp "$root/Examples/LinearLite/iOS/LinearLiteHost.xcodeproj/project.pbxproj" \
  "$fixture/Examples/LinearLite/iOS/LinearLiteHost.xcodeproj/project.pbxproj"

rm "$fixture/Policies/SEMVER.md"
if "$checker" "$fixture" >/dev/null 2>&1; then
  printf 'negative package-quality fixture unexpectedly passed\n' >&2
  exit 1
fi

cp "$root/Policies/SEMVER.md" "$fixture/Policies/SEMVER.md"
"$checker" "$fixture"
printf 'package-quality negative fixture rejected a missing SemVer policy\n'
