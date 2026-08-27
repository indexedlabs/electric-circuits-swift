#!/usr/bin/env bash
set -euo pipefail

# Proves the published-package boundary without changing the development fixture's local-path
# dependency. The only 0.2.0 tag created here belongs to a throwaway clone and is deleted with it.
root=$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)
version=0.2.0
work_dir=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-swift-versioned-host.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT

release_repo="$work_dir/electric-circuits-swift"
consumer_root="$work_dir/consumer"
consumer_package="$consumer_root/LinearLite"
derived_data="$work_dir/DerivedData"
source_revision=$(git -C "$root" rev-parse HEAD)

require_clean_committed_candidate() {
  if [[ -n $(git -C "$root" status --porcelain) ]]; then
    printf 'versioned-host: release candidate must be a clean committed HEAD\n' >&2
    return 1
  fi
  git -C "$root" cat-file -e "$source_revision^{commit}"
}

require_clean_committed_candidate
swift "$root/Scripts/assert-versioned-package-resolved.swift" --self-test

# --no-local prevents shared object storage and makes this a real source-control consumer input.
git clone --quiet --no-local "$root" "$release_repo"
release_repo=$(cd "$release_repo" && pwd -P)
clone_revision=$(git -C "$release_repo" rev-parse HEAD)
if [[ "$clone_revision" != "$source_revision" ]]; then
  printf 'versioned-host: throwaway clone HEAD did not match candidate %s\n' "$source_revision" >&2
  exit 1
fi
# A normal clone inherits published tags from the source repository. The qualification tag must
# identify this candidate inside the throwaway clone, so remove only the clone-local inherited tag
# before recreating it below. The source repository is never mutated.
git -C "$release_repo" tag --delete "$version" >/dev/null 2>&1 || true
git -C "$release_repo" tag "$version" "$source_revision"

tag_revision=$(git -C "$release_repo" rev-parse "$version^{commit}")
if [[ "$tag_revision" != "$source_revision" ]]; then
  printf 'versioned-host: temporary %s tag did not resolve to %s\n' "$version" "$source_revision" >&2
  exit 1
fi

ditto "$root/Examples/LinearLite" "$consumer_package"
# `ditto` preserves ignored SwiftPM state when a developer has built the example locally. Its module
# cache records absolute paths, so it must not cross the isolated-consumer boundary.
rm -rf "$consumer_package/.build" "$consumer_package/.swiftpm" "$consumer_package/Package.resolved"

manifest="$consumer_package/Package.swift"
RELEASE_REPOSITORY_URL="file://$release_repo" perl -0pi -e '
  BEGIN {
    $old = q{.package(name: "electric-circuits-swift", path: "../.."),};
    $new = qq{.package(url: "$ENV{RELEASE_REPOSITORY_URL}", exact: "0.2.0"),};
  }
  s/\Q$old\E/$new/ or die "versioned-host: LinearLite package no longer has the expected development dependency\n";
' "$manifest"

cd "$consumer_package"
swift package resolve

resolved=.swiftpm/Package.resolved
if [[ ! -f "$resolved" ]]; then
  resolved=Package.resolved
fi
if [[ ! -f "$resolved" ]]; then
  printf 'versioned-host: SwiftPM did not write Package.resolved\n' >&2
  exit 1
fi
swift "$root/Scripts/assert-versioned-package-resolved.swift" "$resolved" \
  electric-circuits-swift "$release_repo" "$version" "$tag_revision"

swift test -Xswiftc -strict-concurrency=complete -Xswiftc -warnings-as-errors

xcodebuild -project iOS/LinearLiteHost.xcodeproj -scheme LinearLiteHost \
  -sdk iphonesimulator -destination 'generic/platform=iOS Simulator' -configuration Debug \
  -derivedDataPath "$derived_data" CODE_SIGNING_ALLOWED=NO CODE_SIGNING_REQUIRED=NO \
  CODE_SIGN_IDENTITY= build

printf 'versioned-host: LinearLite resolved ElectricCircuitsSwift %s at %s and built the iOS host\n' \
  "$version" "$source_revision"
