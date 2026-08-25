#!/usr/bin/env bash
set -euo pipefail

version=2.45.3
archive_sha256=0c90f4d28ca57335f9fa78cf5bf6dabfe20a232036dabe36de2eef79cb7c0878
download_url="https://github.com/yonaskolb/XcodeGen/releases/download/${version}/xcodegen.zip"

usage() {
  printf 'usage: %s --bin-dir DIRECTORY\n' "$0" >&2
  exit 64
}

[[ ${1:-} == --bin-dir && -n ${2:-} && $# == 2 ]] || usage
bin_dir=$2
mkdir -p "$bin_dir"

if [[ -x "$bin_dir/xcodegen" ]]; then
  "$bin_dir/xcodegen" --version
  exit 0
fi

work_dir=$(mktemp -d "${TMPDIR:-/tmp}/electric-circuits-xcodegen.XXXXXX")
trap 'rm -rf "$work_dir"' EXIT
archive="$work_dir/xcodegen.zip"

curl --fail --location --retry 3 --silent --show-error "$download_url" --output "$archive"
actual_sha256=$(shasum -a 256 "$archive" | awk '{print $1}')
if [[ "$actual_sha256" != "$archive_sha256" ]]; then
  printf 'xcodegen checksum mismatch: expected %s, got %s\n' "$archive_sha256" "$actual_sha256" >&2
  exit 65
fi

unzip -q "$archive" -d "$work_dir/unpacked"
candidate=$(find "$work_dir/unpacked" -type f -name xcodegen -perm -u+x -print -quit)
if [[ -z "$candidate" ]]; then
  printf 'xcodegen archive did not contain an executable\n' >&2
  exit 65
fi

install -m 755 "$candidate" "$bin_dir/xcodegen"
"$bin_dir/xcodegen" --version
